# app.py
import streamlit as st
import requests, zipfile, io, xml.etree.ElementTree as ET, pandas as pd, time
from datetime import datetime

# ─── 기본 설정 ─────────────────────────────────────────────────────────────────
REPRT_CODE      = '11011'    # 사업보고서 코드
SLEEP_SEC       = 0.0        # API 호출 간 대기(초)
STATUS_INTERVAL = 50         # 몇 건마다 진행 상태 표시
# ────────────────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="DART Executive Monitor", layout="wide")
st.title("📊 DART 임원 ‘주요경력’ 모니터링 서비스 (상장/비상장)")

# 1) 사용자 입력 UI
api_key  = st.text_input("🔑 DART API Key", type="password").strip()
kw_input = st.text_input("🔍 검색할 키워드 (쉼표로 구분)", value="이촌,삼정,안진,삼성,LG,현대,삼일")
listing  = st.multiselect("🏷️ 회사 구분", ["상장사","비상장사"], default=["상장사"])
current_year = datetime.now().year
start_year, end_year = st.slider(
    "📅 사업연도 범위",
    min_value=2000, max_value=current_year,
    value=(current_year-1, current_year),
    step=1
)
run_button = st.button("▶️ 모니터링 시작")

# 세션 초기화
if 'results' not in st.session_state:
    st.session_state['results'] = []

# ─── 2) corpCode.xml 한 번만 내려받아 캐시 ────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_corp_list(key):
    url  = 'https://opendart.fss.or.kr/api/corpCode.xml'
    resp = requests.get(url, params={'crtfc_key': key}, timeout=30)
    resp.raise_for_status()
    content = resp.content
    if not content.startswith(b'PK'):
        err = resp.text.replace('\n',' ')[:200]
        st.error(f"❌ corpCode.xml이 ZIP이 아닙니다.\n{err}")
        return []
    zf   = zipfile.ZipFile(io.BytesIO(content))
    xml  = zf.open(zf.namelist()[0]).read()
    root = ET.fromstring(xml)
    out  = []
    for e in root.findall('list'):
        sc = (e.findtext('stock_code') or '').strip()
        out.append({
            'corp_code':  e.findtext('corp_code'),
            'corp_name':  e.findtext('corp_name'),
            'stock_code': sc
        })
    return out

# ─── 3) 임원현황 API 호출 함수 ───────────────────────────────────────────────
def fetch_execs(key, corp_code, year):
    url = 'https://opendart.fss.or.kr/api/exctvSttus.json'
    params = {
        'crtfc_key':  key,
        'corp_code':  corp_code,
        'bsns_year':  str(year),
        'reprt_code': REPRT_CODE
    }
    resp = requests.get(url, params=params, timeout=20)
    data = resp.json()
    return data.get('list', [])

# ─── 4) 모니터링 실행 ────────────────────────────────────────────────────────
if run_button:
    if not api_key:
        st.warning("API Key를 입력해 주세요.")
    else:
        keywords = [w.strip() for w in kw_input.split(",") if w.strip()]

        with st.spinner("1) 회사 목록 다운로드 중…"):
            corps = load_corp_list(api_key)

        # 4‑A) 상장/비상장 필터
        targets = []
        for c in corps:
            is_listed = bool(c['stock_code'])
            if (is_listed   and "상장사"   in listing) or \
               (not is_listed and "비상장사" in listing):
                targets.append(c)

        total = len(targets)
        st.write(f"✅ 대상 회사: **{total:,}** 개, 사업연도: {start_year} ~ {end_year}")

        progress = st.progress(0)
        results  = []

        years = list(range(start_year, end_year + 1))
        for idx, corp in enumerate(targets, 1):
            for y in years:
                rows = fetch_execs(api_key, corp['corp_code'], y)
                for r in rows:
                    mc = r.get('main_career','')
                    matched = [kw for kw in keywords if kw in mc]
                    if matched:
                        results.append({
                            '회사명':           corp['corp_name'],
                            '종목코드':         corp['stock_code'] or "비상장",
                            '임원이름':         r.get('nm',''),
                            '직위':             r.get('ofcps',''),
                            '주요경력':         mc,
                            'matched_keywords': ",".join(matched),
                            'source':           str(y)
                        })
                if SLEEP_SEC:
                    time.sleep(SLEEP_SEC)
            if idx % STATUS_INTERVAL == 0 or idx == total:
                progress.progress(idx / total)

        # 결과를 세션에 저장
        st.session_state['results'] = results

# ─── 5) 결과 표시 & 다운로드 ────────────────────────────────────────────────
if st.session_state['results']:
    df = pd.DataFrame(
        st.session_state['results'],
        columns=['회사명','종목코드','임원이름','직위','주요경력','matched_keywords','source']
    )
    st.success(f"총 **{len(df):,}**건 매칭 완료")
    st.dataframe(df)

    # Excel(.xlsx) 다운로드
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    st.download_button(
        "📥 XLSX 다운로드",
        data=buf.getvalue(),
        file_name="dart_execs.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
