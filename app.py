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

# 1) 사용자 입력
api_key  = st.text_input("🔑 DART API Key", type="password").strip()
kw_input = st.text_input("🔍 검색할 키워드 (쉼표로 구분)", value="이촌,삼정,안진,삼성,LG,현대,삼일")

# 2) 보고서 종류
REPORT_CHOICES = {
    "11013": "1분기보고서",
    "11012": "반기보고서",
    "11014": "3분기보고서",
    "11011": "사업보고서(연간)",
}
selected_reports = st.multiselect(
    "📑 보고서 종류 선택",
    options=list(REPORT_CHOICES.keys()),
    format_func=lambda c: f"{REPORT_CHOICES[c]} ({c})",
    default=["11011"]
)

# 3) 상장/비상장
listing = st.multiselect("🏷️ 회사 구분", ["상장사", "비상장사"], default=["상장사"])

# 4) 사업연도 범위
current_year = datetime.now().year
start_year, end_year = st.slider(
    "📅 사업연도 범위",
    min_value=2000, max_value=current_year,
    value=(current_year - 1, current_year),
    step=1
)

run_button = st.button("▶️ 모니터링 시작")

# 5) 세션 초기화
if "results" not in st.session_state:
    st.session_state["results"] = []

# 6) requests.Session + Retry 설정 (corpCode.xml도 재시도)
session = requests.Session()
retries = Retry(total=2, backoff_factor=1, status_forcelist=[500,502,503,504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# 7) corpCode.xml 호출 (예외 처리 포함)
@st.cache_data(show_spinner=False)
def load_corp_list(key):
    try:
        resp = session.get(
            "https://opendart.fss.or.kr/api/corpCode.xml",
            params={"crtfc_key": key},
            timeout=60
        )
        resp.raise_for_status()
    except requests.exceptions.ReadTimeout:
        st.error("⏱️ corpCode.xml 요청 시간 초과 – 네트워크 상태를 확인해주세요.")
        return []
    except Exception as e:
        st.error(f"⚠️ corpCode.xml 호출 오류: {e}")
        return []

    content = resp.content
    if not content.startswith(b"PK"):
        err = resp.text.replace("\n", " ")[:200]
        st.error(f"❌ ZIP 파싱 실패: API 응답: {err}")
        return []

    zf = zipfile.ZipFile(io.BytesIO(content))
    xml = zf.open(zf.namelist()[0]).read()
    root = ET.fromstring(xml)
    out = []
    for e in root.findall("list"):
        out.append({
            "corp_code": e.findtext("corp_code"),
            "corp_name": e.findtext("corp_name"),
            "stock_code": (e.findtext("stock_code") or "").strip()
        })
    return out

# 8) 임원현황 API 호출 (예외 처리 포함)
def fetch_execs(key, corp_code, year, rpt_code):
    try:
        resp = session.get(
            "https://opendart.fss.or.kr/api/exctvSttus.json",
            params={
                "crtfc_key":  key,
                "corp_code":  corp_code,
                "bsns_year":  str(year),
                "reprt_code": rpt_code
            },
            timeout=60
        )
        resp.raise_for_status()
        return resp.json().get("list") or []
    except requests.exceptions.ReadTimeout:
        st.warning(f"⏱️ 요청 시간 초과: {corp_code} / {year} / {REPORT_CHOICES[rpt_code]}")
        return []
    except Exception as e:
        st.error(f"⚠️ API 호출 오류 ({corp_code} / {year}): {e}")
        return []

# 9) 모니터링 실행
if run_button:
    if not api_key:
        st.warning("API Key를 입력해 주세요.")
    else:
        keywords = [w.strip() for w in kw_input.split(",") if w.strip()]

        with st.spinner("1) 회사 목록 다운로드 중…"):
            corps = load_corp_list(api_key)
        if not corps:
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
        cnt = 0

        for corp in targets:
            for y in range(start_year, end_year+1):
                for rpt in selected_reports:
                    rows = fetch_execs(api_key, corp["corp_code"], y, rpt)
                    for r in rows:
                        mc = r.get("main_career", "")
                        matched = [kw for kw in keywords if kw in mc]
                        if matched:
                            results.append({
                                "회사명":           corp["corp_name"],
                                "종목코드":         corp["stock_code"] or "비상장",
                                "사업연도":         y,
                                "보고서종류":       REPORT_CHOICES[rpt],
                                "임원이름":         r.get("nm", ""),
                                "직위":             r.get("ofcps", ""),
                                "주요경력":         mc,
                                "matched_keywords": ",".join(matched),
                                "source":           f"{y}-{rpt}"
                            })
                    cnt += 1
                    if SLEEP_SEC:
                        time.sleep(SLEEP_SEC)
                    progress.progress(cnt / total_tasks)

        st.session_state["results"] = results

# 10) 결과 표시 & 다운로드
if st.session_state["results"]:
    df = pd.DataFrame(
        st.session_state["results"],
        columns=[
            "회사명","종목코드","사업연도","보고서종류",
            "임원이름","직위","주요경력","matched_keywords","source"
        ]
    )
    st.success(f"총 **{len(df):,}**건 매칭 완료")
    st.dataframe(df)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    st.download_button(
        "📥 XLSX 다운로드",
        data=buf.getvalue(),
        file_name="dart_execs.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )# app.py
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

# 1) 사용자 입력
api_key  = st.text_input("🔑 DART API Key", type="password").strip()
kw_input = st.text_input("🔍 검색할 키워드 (쉼표로 구분)", value="이촌,삼정,안진,삼성,LG,현대,삼일")

# 2) 보고서 종류
REPORT_CHOICES = {
    "11013": "1분기보고서",
    "11012": "반기보고서",
    "11014": "3분기보고서",
    "11011": "사업보고서(연간)",
}
selected_reports = st.multiselect(
    "📑 보고서 종류 선택",
    options=list(REPORT_CHOICES.keys()),
    format_func=lambda c: f"{REPORT_CHOICES[c]} ({c})",
    default=["11011"]
)

# 3) 상장/비상장
listing = st.multiselect("🏷️ 회사 구분", ["상장사", "비상장사"], default=["상장사"])

# 4) 사업연도 범위
current_year = datetime.now().year
start_year, end_year = st.slider(
    "📅 사업연도 범위",
    min_value=2000, max_value=current_year,
    value=(current_year - 1, current_year),
    step=1
)

run_button = st.button("▶️ 모니터링 시작")

# 5) 세션 초기화
if "results" not in st.session_state:
    st.session_state["results"] = []

# 6) requests.Session + Retry 설정 (corpCode.xml도 재시도)
session = requests.Session()
retries = Retry(total=2, backoff_factor=1, status_forcelist=[500,502,503,504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# 7) corpCode.xml 호출 (예외 처리 포함)
@st.cache_data(show_spinner=False)
def load_corp_list(key):
    try:
        resp = session.get(
            "https://opendart.fss.or.kr/api/corpCode.xml",
            params={"crtfc_key": key},
            timeout=60
        )
        resp.raise_for_status()
    except requests.exceptions.ReadTimeout:
        st.error("⏱️ corpCode.xml 요청 시간 초과 – 네트워크 상태를 확인해주세요.")
        return []
    except Exception as e:
        st.error(f"⚠️ corpCode.xml 호출 오류: {e}")
        return []

    content = resp.content
    if not content.startswith(b"PK"):
        err = resp.text.replace("\n", " ")[:200]
        st.error(f"❌ ZIP 파싱 실패: API 응답: {err}")
        return []

    zf = zipfile.ZipFile(io.BytesIO(content))
    xml = zf.open(zf.namelist()[0]).read()
    root = ET.fromstring(xml)
    out = []
    for e in root.findall("list"):
        out.append({
            "corp_code": e.findtext("corp_code"),
            "corp_name": e.findtext("corp_name"),
            "stock_code": (e.findtext("stock_code") or "").strip()
        })
    return out

# 8) 임원현황 API 호출 (예외 처리 포함)
def fetch_execs(key, corp_code, year, rpt_code):
    try:
        resp = session.get(
            "https://opendart.fss.or.kr/api/exctvSttus.json",
            params={
                "crtfc_key":  key,
                "corp_code":  corp_code,
                "bsns_year":  str(year),
                "reprt_code": rpt_code
            },
            timeout=60
        )
        resp.raise_for_status()
        return resp.json().get("list") or []
    except requests.exceptions.ReadTimeout:
        st.warning(f"⏱️ 요청 시간 초과: {corp_code} / {year} / {REPORT_CHOICES[rpt_code]}")
        return []
    except Exception as e:
        st.error(f"⚠️ API 호출 오류 ({corp_code} / {year}): {e}")
        return []

# 9) 모니터링 실행
if run_button:
    if not api_key:
        st.warning("API Key를 입력해 주세요.")
    else:
        keywords = [w.strip() for w in kw_input.split(",") if w.strip()]

        with st.spinner("1) 회사 목록 다운로드 중…"):
            corps = load_corp_list(api_key)
        if not corps:
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
        cnt = 0

        for corp in targets:
            for y in range(start_year, end_year+1):
                for rpt in selected_reports:
                    rows = fetch_execs(api_key, corp["corp_code"], y, rpt)
                    for r in rows:
                        mc = r.get("main_career", "")
                        matched = [kw for kw in keywords if kw in mc]
                        if matched:
                            results.append({
                                "회사명":           corp["corp_name"],
                                "종목코드":         corp["stock_code"] or "비상장",
                                "사업연도":         y,
                                "보고서종류":       REPORT_CHOICES[rpt],
                                "임원이름":         r.get("nm", ""),
                                "직위":             r.get("ofcps", ""),
                                "주요경력":         mc,
                                "matched_keywords": ",".join(matched),
                                "source":           f"{y}-{rpt}"
                            })
                    cnt += 1
                    if SLEEP_SEC:
                        time.sleep(SLEEP_SEC)
                    progress.progress(cnt / total_tasks)

        st.session_state["results"] = results

# 10) 결과 표시 & 다운로드
if st.session_state["results"]:
    df = pd.DataFrame(
        st.session_state["results"],
        columns=[
            "회사명","종목코드","사업연도","보고서종류",
            "임원이름","직위","주요경력","matched_keywords","source"
        ]
    )
    st.success(f"총 **{len(df):,}**건 매칭 완료")
    st.dataframe(df)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    st.download_button(
        "📥 XLSX 다운로드",
        data=buf.getvalue(),
        file_name="dart_execs.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                       )# app.py
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

# 1) 사용자 입력
api_key  = st.text_input("🔑 DART API Key", type="password").strip()
kw_input = st.text_input("🔍 검색할 키워드 (쉼표로 구분)", value="이촌,삼정,안진,삼성,LG,현대,삼일")

# 2) 보고서 종류
REPORT_CHOICES = {
    "11013": "1분기보고서",
    "11012": "반기보고서",
    "11014": "3분기보고서",
    "11011": "사업보고서(연간)",
}
selected_reports = st.multiselect(
    "📑 보고서 종류 선택",
    options=list(REPORT_CHOICES.keys()),
    format_func=lambda c: f"{REPORT_CHOICES[c]} ({c})",
    default=["11011"]
)

# 3) 상장/비상장
listing = st.multiselect("🏷️ 회사 구분", ["상장사", "비상장사"], default=["상장사"])

# 4) 사업연도 범위
current_year = datetime.now().year
start_year, end_year = st.slider(
    "📅 사업연도 범위",
    min_value=2000, max_value=current_year,
    value=(current_year - 1, current_year),
    step=1
)

run_button = st.button("▶️ 모니터링 시작")

# 5) 세션 초기화
if "results" not in st.session_state:
    st.session_state["results"] = []

# 6) requests.Session + Retry 설정 (corpCode.xml도 재시도)
session = requests.Session()
retries = Retry(total=2, backoff_factor=1, status_forcelist=[500,502,503,504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# 7) corpCode.xml 호출 (예외 처리 포함)
@st.cache_data(show_spinner=False)
def load_corp_list(key):
    try:
        resp = session.get(
            "https://opendart.fss.or.kr/api/corpCode.xml",
            params={"crtfc_key": key},
            timeout=60
        )
        resp.raise_for_status()
    except requests.exceptions.ReadTimeout:
        st.error("⏱️ corpCode.xml 요청 시간 초과 – 네트워크 상태를 확인해주세요.")
        return []
    except Exception as e:
        st.error(f"⚠️ corpCode.xml 호출 오류: {e}")
        return []

    content = resp.content
    if not content.startswith(b"PK"):
        err = resp.text.replace("\n", " ")[:200]
        st.error(f"❌ ZIP 파싱 실패: API 응답: {err}")
        return []

    zf = zipfile.ZipFile(io.BytesIO(content))
    xml = zf.open(zf.namelist()[0]).read()
    root = ET.fromstring(xml)
    out = []
    for e in root.findall("list"):
        out.append({
            "corp_code": e.findtext("corp_code"),
            "corp_name": e.findtext("corp_name"),
            "stock_code": (e.findtext("stock_code") or "").strip()
        })
    return out

# 8) 임원현황 API 호출 (예외 처리 포함)
def fetch_execs(key, corp_code, year, rpt_code):
    try:
        resp = session.get(
            "https://opendart.fss.or.kr/api/exctvSttus.json",
            params={
                "crtfc_key":  key,
                "corp_code":  corp_code,
                "bsns_year":  str(year),
                "reprt_code": rpt_code
            },
            timeout=60
        )
        resp.raise_for_status()
        return resp.json().get("list") or []
    except requests.exceptions.ReadTimeout:
        st.warning(f"⏱️ 요청 시간 초과: {corp_code} / {year} / {REPORT_CHOICES[rpt_code]}")
        return []
    except Exception as e:
        st.error(f"⚠️ API 호출 오류 ({corp_code} / {year}): {e}")
        return []

# 9) 모니터링 실행
if run_button:
    if not api_key:
        st.warning("API Key를 입력해 주세요.")
    else:
        keywords = [w.strip() for w in kw_input.split(",") if w.strip()]

        with st.spinner("1) 회사 목록 다운로드 중…"):
            corps = load_corp_list(api_key)
        if not corps:
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
        cnt = 0

        for corp in targets:
            for y in range(start_year, end_year+1):
                for rpt in selected_reports:
                    rows = fetch_execs(api_key, corp["corp_code"], y, rpt)
                    for r in rows:
                        mc = r.get("main_career", "")
                        matched = [kw for kw in keywords if kw in mc]
                        if matched:
                            results.append({
                                "회사명":           corp["corp_name"],
                                "종목코드":         corp["stock_code"] or "비상장",
                                "사업연도":         y,
                                "보고서종류":       REPORT_CHOICES[rpt],
                                "임원이름":         r.get("nm", ""),
                                "직위":             r.get("ofcps", ""),
                                "주요경력":         mc,
                                "matched_keywords": ",".join(matched),
                                "source":           f"{y}-{rpt}"
                            })
                    cnt += 1
                    if SLEEP_SEC:
                        time.sleep(SLEEP_SEC)
                    progress.progress(cnt / total_tasks)

        st.session_state["results"] = results

# 10) 결과 표시 & 다운로드
if st.session_state["results"]:
    df = pd.DataFrame(
        st.session_state["results"],
        columns=[
            "회사명","종목코드","사업연도","보고서종류",
            "임원이름","직위","주요경력","matched_keywords","source"
        ]
    )
    st.success(f"총 **{len(df):,}**건 매칭 완료")
    st.dataframe(df)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    st.download_button(
        "📥 XLSX 다운로드",
        data=buf.getvalue(),
        file_name="dart_execs.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
