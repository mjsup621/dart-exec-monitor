import streamlit as st
import json, requests, zipfile, io, xml.etree.ElementTree as ET, pandas as pd
from datetime import datetime, timedelta
import pytz
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import gspread
from google.oauth2.service_account import Credentials

# --- 한국시간 함수 ---
def now_kst():
    return datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Asia/Seoul'))

def kst_iso():
    return now_kst().isoformat(timespec="seconds")

def kst_id():
    return now_kst().strftime("%Y%m%d-%H%M%S")

# --- Google Sheets 인증 ---
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

# --- UI/STYLE ---
st.set_page_config(page_title="DART 임원 주요경력 모니터링", layout="wide", initial_sidebar_state="collapsed")
st.markdown("""
    <style>
    .stApp { background: #f9fbfd; font-family: 'SF Pro Display', 'Pretendard', 'Inter', sans-serif; }
    .top-title {margin-bottom:2rem;}
    .api-group {display:flex; gap:8px; margin-bottom:1rem;}
    .api-label {padding:8px 16px; border-radius:10px; background:#fff; border:2px solid #d2e3fc; font-weight:600;}
    .api-label.checked {background:#007aff; color:#fff; border:none;}
    .api-direct {padding:8px; border-radius:10px; background:#f6f8fa; border:1.5px solid #d2e3fc; margin-bottom:2rem; font-size:15px;}
    .stProgress > div > div {background:#007aff;}
    .stButton button {background:#007aff; color:#fff; border-radius:10px; font-weight:500;}
    .stTextInput input {font-size:17px;}
    .stMarkdown, .stDataFrame, .stMultiselect, .stSlider {font-size:17px;}
    </style>
""", unsafe_allow_html=True)

st.markdown("""
<div class='top-title'>
    <span style='font-size:2.2rem;font-weight:700;color:#222;letter-spacing:-2px;'>DART 임원 <span style='color:#007aff'>‘주요경력’</span> 모니터링 서비스</span>
    <div style="font-size:1rem; color:#666; font-weight:500; margin:0.8rem 0 0.5rem;">
        🔑 DART API Key (프리셋 복수선택 + 직접입력 추가/수정)
    </div>
</div>
""", unsafe_allow_html=True)

# --- API KEY 프리셋 체크박스 + 직접입력 (한줄로)
preset_keys = [
    ("API 1", "eeb883965e882026589154074cddfc695330693c"),
    ("API 2", "1290bb1ec7879cba0e9f9b350ac97bb5d38ec176"),
    ("API 3", "5e75506d60b4ab3f325168019bcacf364cf4937e"),
    ("API 4", "6c64f7efdea057881deb91bbf3aaa5cb8b03d394"),
    ("API 5", "d9f0d92fbdc3a2205e49c66c1e24a442fa8c6fe8"),
    ("API 6", "c38b1fdef8960f694f56a50cf4e52d5c25fd5675"),
]
cols = st.columns(len(preset_keys))
preset_checked = []
for i, (name, val) in enumerate(preset_keys):
    checked = cols[i].checkbox(name, value=True, key=f"api_preset_{i}")
    if checked:
        preset_checked.append(val)

api_direct = st.text_area(
    "API Key 직접 입력 (여러 개 입력 가능, 쉼표 또는 줄바꿈)",
    value="",
    placeholder="API Key를 복수로 입력 (예: key1, key2, ...)",
    height=38
)
direct_keys = [k.strip() for line in api_direct.splitlines() for k in line.split(",") if k.strip()]
api_keys = preset_checked + direct_keys
if not api_keys:
    st.warning("하나 이상의 API Key를 선택하거나 입력하세요.")
    st.stop()

# --- 파라미터 입력 ---
recipient = st.text_input("결과 수신 이메일 (필수)", value="", placeholder="your@email.com")
keywords  = st.text_input("키워드 (쉼표구분)", "이촌,삼정,안진")
REPORTS = {
    "11013":"1분기보고서","11012":"반기보고서",
    "11014":"3분기보고서","11011":"사업보고서(연간)"
}
sel_reports = st.multiselect(
    "보고서 종류", options=list(REPORTS.keys()),
    format_func=lambda c: f"{REPORTS[c]} ({c})",
    default=["11011"]
)
listing = st.multiselect("회사 구분", ["상장사","비상장사"], default=["상장사"])
cy = now_kst().year
start_y, end_y = st.slider("사업연도 범위", 2000, cy, (cy-1, cy), label_visibility="visible")
col1, col2 = st.columns(2)
run, stop = col1.button("▶️ 모니터링 시작", use_container_width=True), col2.button("⏹️ 중지", use_container_width=True)

# --- 이메일 UX 검증 ---
import re
def is_valid_email(addr):
    return bool(re.match(r"[^@]+@[^@]+\.[^@]+", addr))
if run and not recipient:
    st.warning("결과 수신 이메일을 입력하세요.")
    st.session_state.email_focus = True
    st.stop()
elif run and not is_valid_email(recipient):
    st.warning("올바른 이메일 주소 형식이 아닙니다.")
    st.session_state.email_focus = True
    st.stop()
else:
    st.session_state.email_focus = False

if "running" not in st.session_state:
    st.session_state.running = False

if run and is_valid_email(recipient):
    st.session_state.running = True

if stop:
    st.session_state.running = False
    st.info("중지되었습니다.")

# --- HTTP 세션 ---
session = requests.Session()
session.mount("https://", HTTPAdapter(
    max_retries=Retry(total=2, backoff_factor=1, status_forcelist=[500,502,503,504])
))

# --- corpCode.xml 캐싱 ---
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

# --- 임원현황 API 호출 ---
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

# --- UI 상태 표시 영역 ---
prog_placeholder = st.empty()
status_placeholder = st.empty()
err_placeholder = st.empty()

# --- 모니터링 수행 ---
if st.session_state.running:
    job_id, ts0 = kst_id(), kst_iso()
    jobs_ws.append_row([job_id, recipient, ts0, "running"])
    with st.spinner("회사 목록 로드 중…"):
        corps, corp_err = load_corp_list(api_keys[0])
        if not corps:
            st.session_state.running = False
            err_placeholder.error(f"회사 목록 로드 실패: {corp_err}")
            jobs_ws.append_row([job_id, recipient, kst_iso(), "failed"])
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
    st.success(f"총 호출 대상: {N:,}건")
    results = []
    key_idx = 0
    for i, (corp, y, rpt) in enumerate(targets, 1):
        if not st.session_state.running:
            break
        key = api_keys[key_idx % len(api_keys)]
        rows, err = fetch_execs(key, corp["corp_code"], y, rpt)
        key_idx += 1
        if err:
            err_placeholder.warning(f"{corp['corp_name']} {y}-{REPORTS[rpt]}: {err}", icon="⚠️")
            # 사용한도 초과 시도 바로 다음키로 (계속)
            continue
        # 상태 한줄
        status_placeholder.markdown(
            f"<span style='color:#666;font-size:17px;'>"
            f"[{i}/{N}] <b style='color:#007aff'>{corp['corp_name']}</b> · {y}년 · {REPORTS[rpt]} 검색 중…"
            f"</span>", unsafe_allow_html=True
        )
        for r in rows:
            mc = r.get("main_career", "")
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
        prog_placeholder.progress(i/N)
    prog_placeholder.progress(1.0)
    status_placeholder.success("✅ 전체 조회 완료!")

    # --- 결과 표시/저장 ---
    ts1 = kst_iso()
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
        st.success(f"총 {len(df):,}건 매칭 완료")
        st.dataframe(df, use_container_width=True)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name="Sheet1")
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

# --- 이메일 발송 함수 ---
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
