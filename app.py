import streamlit as st
import requests, zipfile, io, xml.etree.ElementTree as ET, pandas as pd, json
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import gspread
from google.oauth2.service_account import Credentials

# ─── 구글 시트 인증: service_account.json 대신 secrets에서 불러오기 ──────────────
service_account_info = json.loads(st.secrets["SERVICE_ACCOUNT_JSON"])
creds = Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)
SPREADSHEET_ID = "1hT_PaNZvsBqVfxQXCNgIXSVcpsDGJf474yWQfYmeJ7o"
sh = gc.open_by_key(SPREADSHEET_ID)
jobs_ws = sh.worksheet("DART_Jobs")
prog_ws = sh.worksheet("DART_Progress")

# ─── Streamlit UI ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="DART 임원 주요경력 모니터링", layout="wide")
st.title("📊 DART 임원 ‘주요경력’ 모니터링 서비스")

api_options = [
    ("API 1", "eeb883965e882026589154074cddfc695330693c"),
    ("API 2", "1290bb1ec7879cba0e9f9b350ac97bb5d38ec176"),
    ("API 3", "5e75506d60b4ab3f325168019bcacf364cf4937e"),
    ("API 4", "6c64f7efdea057881deb91bbf3aaa5cb8b03d394"),
    ("API 5", "d9f0d92fbdc3a2205e49c66c1e24a442fa8c6fe8"),
]
colA, colB = st.columns([2,4])
api_select = colA.radio("🗝️ DART API KEY 선택", [x[0] for x in api_options])
api_key_default = dict(api_options)[api_select]
api_key_input = colB.text_input("🔑 DART API Key 직접 입력 (선택)", value=api_key_default)
api_keys = [k.strip() for k in api_key_input.split(",") if k.strip()]
corp_key = api_keys[0]  # corpCode.xml용

recipient = st.text_input("📧 결과 수신 이메일", value="")
keywords  = st.text_input("🔍 키워드 (쉼표로 구분)", "이촌,삼정,안진")
REPORTS = {
    "11013":"1분기보고서","11012":"반기보고서",
    "11014":"3분기보고서","11011":"사업보고서(연간)"
}
sel_reports = st.multiselect(
    "📑 보고서 종류",
    options=list(REPORTS.keys()),
    format_func=lambda c: f"{REPORTS[c]} ({c})",
    default=["11011"]
)
listing = st.multiselect("🏷️ 회사 구분", ["상장사","비상장사"], default=["상장사"])
cy = datetime.now().year
start_y, end_y = st.slider("📅 사업연도 범위", 2000, cy, (cy-1, cy))
col1, col2 = st.columns(2)
run, stop = col1.button("▶️ 모니터링 시작"), col2.button("⏹️ 중지")

# ─── HTTP 세션 + Retry ──────────────────────────────────────────────────────────
session = requests.Session()
session.mount("https://", HTTPAdapter(
    max_retries=Retry(total=2, backoff_factor=1, status_forcelist=[500,502,503,504])
))

# ─── corpCode.xml 로드 (캐시) ────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_corp_list(key):
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    try:
        resp = session.get(url, params={"crtfc_key": key}, timeout=30)
        resp.raise_for_status()
        if not resp.content.startswith(b"PK"):
            err = ET.fromstring(resp.content).findtext("message", default="알 수 없는 오류")
            return None, err
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        xml = zf.open(zf.namelist()[0]).read()
        root = ET.fromstring(xml)
        out = []
        for e in root.findall("list"):
            out.append({
                "corp_code":  e.findtext("corp_code"),
                "corp_name":  e.findtext("corp_name"),
                "stock_code": (e.findtext("stock_code") or "").strip()
            })
        return out, None
    except Exception as e:
        return None, str(e)

# ─── 임원현황 API 호출 ───────────────────────────────────────────────────────────
def fetch_execs(key, corp_code, year, rpt):
    try:
        payload = {
            "crtfc_key": key,
            "corp_code": corp_code,
            "bsns_year": str(year),
            "reprt_code": rpt
        }
        data = session.get(
            "https://opendart.fss.or.kr/api/exctvSttus.json",
            params=payload, timeout=20
        ).json()
        if data.get("status") != "000":
            return [], data.get("message")
        return data.get("list", []), None
    except Exception as e:
        return [], str(e)

# ─── 모니터링 제어 상태 ──────────────────────────────────────────────────────────
if "running" not in st.session_state:
    st.session_state.running = False

if run:
    if not (recipient and keywords and sel_reports):
        st.warning("이메일·키워드·보고서를 모두 입력하세요.")
    else:
        st.session_state.running = True

if stop:
    st.session_state.running = False
    st.warning("중지되었습니다.")

# ─── 모니터링 수행 ──────────────────────────────────────────────────────────────
if st.session_state.running:
    job_id, ts0 = datetime.now().strftime("%Y%m%d-%H%M%S"), datetime.now().isoformat()
    jobs_ws.append_row([job_id, recipient, ts0, "running"])

    with st.spinner("회사 목록 로드 중…"):
        corps, corp_err = load_corp_list(corp_key)
        if not corps:
            st.session_state.running = False
            st.error(f"회사 목록 로드 실패: {corp_err}")
            jobs_ws.append_row([job_id, recipient, datetime.now().isoformat(), "failed"])
            st.stop()

    kws = [w.strip() for w in keywords.split(",") if w.strip()]
    all_c = [
        c for c in corps
        if ((c["stock_code"] and "상장사" in listing)
            or (not c["stock_code"] and "비상장사" in listing))
    ]
    targets = [
        (c, y, r)
        for c in all_c
        for y in range(start_y, end_y+1)
        for r in sel_reports
    ]
    N = len(targets)
    st.success(f"✅ 총 호출 대상: **{N:,}**건")
    prog = st.progress(0)

    results = []
    for i, (corp, y, rpt) in enumerate(targets,1):
        if not st.session_state.running:
            break
        rows, err = fetch_execs(corp_key, corp["corp_code"], y, rpt)
        if err:
            st.warning(f"{y}-{rpt} {corp['corp_name']} 오류: {err}")
            continue
        for r in rows:
            mc = r.get("main_career","")
            if any(k in mc for k in kws):
                results.append({
                    "회사명":     corp["corp_name"],
                    "종목코드":   corp["stock_code"] or "비상장",
                    "사업연도":   y,
                    "보고서종류": REPORTS[rpt],
                    "임원이름":   r.get("nm",""),
                    "직위":       r.get("ofcps",""),
                    "주요경력":   mc,
                    "매칭키워드": ",".join([k for k in kws if k in mc])
                })
        prog.progress(i/N)
    prog.progress(1.0)

    # 진행 요약
    ts1 = datetime.now().isoformat()
    prog_ws.append_row([
        job_id, N, f"{start_y}-{end_y}",
        ",".join(REPORTS[r] for r in sel_reports),
        ts1, len(results)
    ])
    status = "completed" if st.session_state.running else "stopped"
    r = jobs_ws.find(job_id, in_column=1)
    jobs_ws.update_cell(r.row, 4, status)

    df = pd.DataFrame(results)
    if df.empty:
        st.info("🔍 매칭된 결과물이 없습니다. 메일이 발송되지 않습니다.")
    else:
        st.success(f"총 **{len(df):,}**건 매칭 완료")
        st.dataframe(df)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w,index=False,sheet_name="Sheet1")

        # 이메일 발송
        from_email = st.secrets["smtp"]["sender_email"]
        from_pwd   = st.secrets["smtp"]["sender_password"]
        # send_email() 함수는 아래에서 따로 정의(구현)해줘야 함.
        send_email(
            to_email=recipient,
            subject=f"[DART Monitor {job_id}] 결과",
            body=(f"작업ID: {job_id}\n시작: {ts0}\n종료: {ts1}\n총 호출: {N:,}회\n매칭: {len(results):,}건"),
            attachment_bytes=buf.getvalue(),
            filename=f"dart_results_{job_id}.xlsx"
        )
        st.info(f"결과를 {recipient} 로 발송했습니다.")
        st.download_button(
            "📥 XLSX 다운로드",
            data=buf.getvalue(),
            file_name=f"dart_results_{job_id}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ─── 이메일 발송 함수 (SMTP, Gmail 등) ─────────────────────────────
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

def send_email(to_email, subject, body, attachment_bytes, filename):
    from_email = st.secrets["smtp"]["sender_email"]
    from_pwd   = st.secrets["smtp"]["sender_password"]
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, "plain"))
    part = MIMEApplication(attachment_bytes)
    part.add_header('Content-Disposition', 'attachment', filename=filename)
    msg.attach(part)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(from_email, from_pwd)
        server.send_message(msg)
