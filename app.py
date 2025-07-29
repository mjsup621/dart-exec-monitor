# app.py
import streamlit as st
import requests, zipfile, io, xml.etree.ElementTree as ET, pandas as pd, time
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# ─── 설정 ─────────────────────────────────────────────────────────────────────
SLEEP_SEC       = 0.0    # API 호출 간 대기(초)
STATUS_INTERVAL = 50     # 진행바 갱신 간격
# ────────────────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="DART Executive Monitor", layout="wide")
st.title("📊 DART 임원 ‘주요경력’ 모니터링 서비스")

# ─── 1) 입력 섹션 ──────────────────────────────────────────────────────────────
api_key  = st.text_input("🔑 DART API Key", type="password").strip()
kw_input = st.text_input("🔍 검색할 키워드 (쉼표로 구분)", value="이촌,삼정,안진")

REPORT_CHOICES = {
    "11013": "1분기보고서",
    "11012": "반기보고서",
    "11014": "3분기보고서",
    "11011": "사업보고서(연간)"
}
selected_reports = st.multiselect(
    "📑 보고서 종류 선택",
    options=list(REPORT_CHOICES.keys()),
    format_func=lambda c: f"{REPORT_CHOICES[c]} ({c})",
    default=["11011"]
)

listing = st.multiselect(
    "🏷️ 회사 구분",
    ["상장사", "비상장사"],
    default=["상장사"]
)

current_year = datetime.now().year
start_year, end_year = st.slider(
    "📅 사업연도 범위",
    min_value=2000, max_value=current_year,
    value=(current_year - 1, current_year), step=1
)

run_button = st.button("▶️ 모니터링 시작")

# ─── 2) HTTP 세션 + Retry 설정 ─────────────────────────────────────────────────
session = requests.Session()
retries = Retry(total=2, backoff_factor=1, status_forcelist=[500,502,503,504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# ─── 3) corpCode.xml 로드 (캐시) ───────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_corp_list(key):
    resp = session.get(
        "https://opendart.fss.or.kr/api/corpCode.xml",
        params={"crtfc_key": key}, timeout=60
    )
    resp.raise_for_status()
    content = resp.content
    if not content.startswith(b"PK"):
        raise ValueError("ZIP 파싱 실패")
    zf = zipfile.ZipFile(io.BytesIO(content))
    xml = zf.open(zf.namelist()[0]).read()
    root = ET.fromstring(xml)
    out = []
    for e in root.findall("list"):
        out.append({
            "corp_code":  e.findtext("corp_code"),
            "corp_name":  e.findtext("corp_name"),
            "stock_code": (e.findtext("stock_code") or "").strip()
        })
    return out

# ─── 4) 임원현황 API 호출 함수 ────────────────────────────────────────────────
def fetch_execs(key, corp_code, year, rpt_code):
    resp = session.get(
        "https://opendart.fss.or.kr/api/exctvSttus.json",
        params={
            "crtfc_key":  key,
            "corp_code":  corp_code,
            "bsns_year":  str(year),
            "reprt_code": rpt_code
        }, timeout=60
    )
    resp.raise_for_status()
    return resp.json().get("list") or []

# ─── 5) 실행 및 결과 처리 ──────────────────────────────────────────────────────
if run_button:
    if not api_key:
        st.warning("API Key를 입력해 주세요.")
        st.stop()

    keywords = [w.strip() for w in kw_input.split(",") if w.strip()]

    with st.spinner("1) 회사 목록 다운로드 중…"):
        try:
            corps = load_corp_list(api_key)
        except Exception as e:
            st.error(f"회사 목록 로드 실패: {e}")
            st.stop()

    # 상장/비상장 필터
    targets = [
        c for c in corps
        if ((c["stock_code"] and "상장사" in listing) or
            (not c["stock_code"] and "비상장사" in listing))
    ]

    total_tasks = len(targets) * len(range(start_year, end_year+1)) * len(selected_reports)
    st.write(f"✅ 대상 회사: **{len(targets):,}** 개, 연도: {start_year}~{end_year}, 보고서: {len(selected_reports)}종류")
    progress = st.progress(0)

    results = []
    call_count = 0
    error_msg = None

    # 주요 루프: 호출 실패 시 중단
    for corp in targets:
        if error_msg: break
        for y in range(start_year, end_year+1):
            if error_msg: break
            for rpt in selected_reports:
                try:
                    rows = fetch_execs(api_key, corp["corp_code"], y, rpt)
                    call_count += 1
                except Exception as e:
                    error_msg = str(e)
                    break

                # 매칭 로직
                for r in rows:
                    mc = r.get("main_career","")
                    matched = [kw for kw in keywords if kw in mc]
                    if matched:
                        results.append({
                            "회사명":           corp["corp_name"],
                            "종목코드":         corp["stock_code"] or "비상장",
                            "사업연도":         y,
                            "보고서종류":       REPORT_CHOICES[rpt],
                            "임원이름":         r.get("nm",""),
                            "직위":             r.get("ofcps",""),
                            "주요경력":         mc,
                            "matched_keywords": ",".join(matched),
                            "source":           f"{y}-{rpt}"
                        })

                progress.progress(call_count / total_tasks)

    # 호출 현황 요약
    st.write(f"🔄 총 API 호출 시도: {call_count:,}회")
    if error_msg:
        st.error(f"❗ 호출 중 오류 발생 후 중단: {call_count}회 호출, 오류: {error_msg}")

    # 결과 DataFrame & 다운로드
    df = pd.DataFrame(
        results,
        columns=[
            "회사명","종목코드","사업연도","보고서종류",
            "임원이름","직위","주요경력","matched_keywords","source"
        ]
    )
    if df.empty:
        st.info("🔍 매칭된 결과물이 없습니다.")
    else:
        st.success(f"총 **{len(df):,}**건 매칭 완료")
    st.dataframe(df)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    st.download_button(
        "📥 XLSX 다운로드",
        data=buf.getvalue(),
        file_name="dart_execs_partial.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
