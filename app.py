# app.py
import streamlit as st
import requests, zipfile, io, xml.etree.ElementTree as ET, pandas as pd, time
from datetime import datetime

# ─── 기본 설정 ─────────────────────────────────────────────────────────────────
REPRT_CODE      = '11011'    # 정기보고서 코드
SLEEP_SEC       = 0.0        # API 호출 간 대기(초)
STATUS_INTERVAL = 50         # 몇 건마다 진행 상태 표시
# ────────────────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="DART Executive Monitor", layout="wide")
st.title("📊 DART 임원 ‘주요경력’ 모니터링 서비스")

# 1) 사용자 입력 UI
api_key    = st.text_input("🔑 DART API Key", type="password")
kw_input   = st.text_input("🔍 검색할 키워드 (쉼표로 구분)",
                           value="이촌,삼정,안진,삼성,LG,현대,삼일")
markets    = st.multiselect("🏷️ 상장사 구분",
                            ["KOSPI","KOSDAQ","KONEX"],
                            default=["KOSPI","KOSDAQ","KONEX"])
# 2) 사업연도 범위 선택
current_year = datetime.now().year
start_year, end_year = st.slider(
    "📅 사업연도 범위",
    min_value=2000, max_value=current_year,
    value=(current_year-1, current_year),
    step=1
)
run_button = st.button("▶️ 모니터링 시작")


def fetch_corp_list(crtfc_key):
    url  = 'https://opendart.fss.or.kr/api/corpCode.xml'
    resp = requests.get(url, params={'crtfc_key': crtfc_key}, timeout=30)
    resp.raise_for_status()
    content = resp.content
    if not content.startswith(b'PK'):
        st.error("corpCode.xml이 ZIP이 아닙니다. API Key를 확인하세요.")
        return []
    zf   = zipfile.ZipFile(io.BytesIO(content))
    xml  = zf.open(zf.namelist()[0]).read()
    root = ET.fromstring(xml)
    out  = []
    for e in root.findall('list'):
        sc = (e.findtext('stock_code') or '').strip()
        if sc:
            out.append({
                'corp_code':  e.findtext('corp_code'),
                'corp_name':  e.findtext('corp_name'),
                'stock_code': sc
            })
    return out


def filter_by_market(corps, markets):
    def in_market(code, m):
        if not code.isdigit():
            return False
        c = int(code)
        if m == "KOSPI":   return 1 <= c < 100000
        if m == "KOSDAQ":  return 100000 <= c < 200000
        if m == "KONEX":   return c >= 900000
        return False
    return [c for c in corps if any(in_market(c['stock_code'], m) for m in markets)]


def fetch_execs(crtfc_key, corp_code, bsns_year):
    url = 'https://opendart.fss.or.kr/api/exctvSttus.json'
    params = {
        'crtfc_key':  crtfc_key,
        'corp_code':  corp_code,
        'bsns_year':  str(bsns_year),
        'reprt_code': REPRT_CODE
    }
    resp = requests.get(url, params=params, timeout=20)
    data = resp.json()
    return data.get('list', [])


if run_button:
    if not api_key:
        st.warning("API Key를 입력해 주세요.")
    else:
        keywords = [w.strip() for w in kw_input.split(",") if w.strip()]
        with st.spinner("1) 상장회사 목록 다운로드 중…"):
            corps = fetch_corp_list(api_key)
        corps       = filter_by_market(corps, markets)
        total_corps = len(corps)
        st.write(f"✅ 대상 회사: **{total_corps:,}** 개, 사업연도: {start_year} ~ {end_year}")

        progress = st.progress(0)
        results  = []

        years = list(range(start_year, end_year + 1))
        for idx, corp in enumerate(corps, 1):
            for y in years:
                rows = fetch_execs(api_key, corp['corp_code'], y)
                for r in rows:
                    mc = r.get('main_career', '')
                    matched = [kw for kw in keywords if kw in mc]
                    if matched:
                        results.append({
                            '회사명':           corp['corp_name'],
                            '종목코드':         corp['stock_code'],
                            '임원이름':         r.get('nm',''),
                            '직위':             r.get('ofcps',''),
                            '주요경력':         mc,
                            'matched_keywords': ",".join(matched),
                            'source':           str(y)
                        })
                if SLEEP_SEC:
                    time.sleep(SLEEP_SEC)

            if idx % STATUS_INTERVAL == 0 or idx == total_corps:
                progress.progress(idx / total_corps)

        if results:
            df = pd.DataFrame(results, columns=[
                '회사명', '종목코드', '임원이름', '직위',
                '주요경력', 'matched_keywords', 'source'
            ])
            st.success(f"총 **{len(df):,}**건 매칭 완료")
            st.dataframe(df)

            # ── CSV 다운로드 (CP949) ──────────────────────────────────────────────
            csv_cp949 = df.to_csv(index=False, encoding='cp949', errors='replace')
            st.download_button(
                "📥 CSV 다운로드 (CP949)",
                data=csv_cp949,
                file_name="dart_execs_cp949.csv",
                mime="text/csv"
            )

            # ── Excel(xlsx) 다운로드 ─────────────────────────────────────────────
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Sheet1')
            st.download_button(
                "📥 XLSX 다운로드",
                data=output.getvalue(),
                file_name="dart_execs.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        else:
            st.info("키워드에 매칭된 임원이 없습니다.")
