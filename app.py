import streamlit as st
import requests, zipfile, io, xml.etree.ElementTree as ET, pandas as pd, json
from datetime import datetime
from pytz import timezone
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import gspread
from google.oauth2.service_account import Credentials
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

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

KST = timezone('Asia/Seoul')

# --- Apple 스타일 (UI/폰트/버튼 등) ---
st.set_page_config(page_title="DART 임원 모니터링", layout="wide")
st.markdown("""
<style>
.stApp {background: #f8f9fa !important; font-family:'SF Pro Display','Apple SD Gothic Neo','Pretendard',sans-serif;}
h1, h2, h3, h4, .stRadio, .stButton button, .stTextInput input {font-weight:600;}
.stProgress > div > div {background-color:#007aff!important;}
.api-label {font-weight:600; color:#111; font-size:17px; margin-bottom:8px;}
.stDataFrame {border-radius:18px;}
.job-badge {display:inline-block;background:#007aff;color:#fff;border-radius:8px;padding:0 7px;}
</style>
""", unsafe_allow_html=True)

st.markdown("<h2 style='font-size:2.3rem;margin-bottom:0.7em;'><b>DART 임원 <span style='color:#007aff'>‘주요경력’</span> 모니터링 서비스</b></h2>", unsafe_allow_html=True)

# --- API KEY (3개씩 2줄 라디오+직접입력 우선) ---
api_presets = [
    ("API 1", "eeb883965e882026589154074cddfc695330693c"),
    ("API 2", "1290bb1ec7879cba0e9f9b350ac97bb5d38ec176"),
    ("API 3", "5e75506d60b4ab3f325168019bcacf364cf4937e"),
    ("API 4", "6c64f7efdea057881deb91bbf3aaa5cb8b03d394"),
    ("API 5", "d9f0d92fbdc3a2205e49c66c1e24a442fa8c6fe8"),
    ("API 6", "c38b1fdef8960f694f56a50cf4e52d5c25fd5675"),
]
api_labels = [x[0] for x in api_presets]
api_keys_list = [x[1] for x in api_presets]

# ---- 프리셋 3개씩 2줄 라디오 버튼 ----
col_api_left, col_api_right = st.columns([1,3])
with col_api_left:
    st.markdown("<div class='api-label'>프리셋 API KEY<br>(한 번에 하나 선택)</div>", unsafe_allow_html=True)
    col_api_row1, col_api_row2 = st.columns(2)
    with col_api_row1:
        selected1 = st.radio("", api_labels[:3], index=0, key="api_preset_row1")
    with col_api_row2:
        selected2 = st.radio("", api_labels[3:], key="api_preset_row2")

    # 두 줄 중 선택된 API 가져오기 (단, 직접입력 있으면 무시됨)
    selected_preset = selected1 if selected1 != api_labels[0] else selected2
    api_key_selected = dict(api_presets)[selected_preset] if selected_preset in dict(api_presets) else api_presets[0][1]

with col_api_right:
    st.markdown("<div class='api-label'>API Key 직접 입력<br><span style='font-size:13px;color:#888;'>(값 입력시 프리셋 무시, 한 개만 적용)</span></div>", unsafe_allow_html=True)
    api_key_input = st.text_area(
        "", value="", height=40, placeholder="복사/붙여넣기 (한 개만 적용)"
    )
api_keys = [k.strip() for k in api_key_input.replace(",", "\n").splitlines() if k.strip()]
corp_key = api_keys[0] if api_keys else api_key_selected

# ---- 검색 폼 ----
def focus_email():
    js = """<script>
    setTimeout(function() {
        let email=document.querySelectorAll('input[type="text"]')[0];
        if(email){email.focus();}
    },300);</script>"""
    st.markdown(js, unsafe_allow_html=True)

def is_valid_email(email):
    return "@" in email and "." in email and len(email) > 6

recipient = st.text_input("📧 결과 수신 이메일 (필수)", value="", key="email_input")
if st.session_state.get("email_required") and not is_valid_email(recipient):
    st.warning("유효한 이메일 주소를 입력하세요.", icon="⚠️")
    focus_email()

keywords = st.text_input("🔍 키워드 (쉼표 구분)", "이촌,삼정,안진")
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
cy = datetime.now(KST).year
start_y, end_y = st.slider("사업연도 범위", 2000, cy, (cy-1, cy))

# ---- 이어받기/복구 UI ----
jobs_data = jobs_ws.get_all_records()
unfinished = [r for r in jobs_data if r["status"] in ("stopped","failed")][-1:]  # 최근 1개
if unfinished:
    rj = unfinished[0]
    st.markdown(
        f"<div style='background:#eef6fe;border-radius:9px;padding:12px 16px 8px 16px;margin-bottom:5px;'>"
        f"🔄 <b>미완료(중단) 작업 이어받기:</b> "
        f"<span class='job-badge'>{rj['job_id']}</span> "
        f"({rj.get('user_email','')}, {rj.get('start_time','')})"
        f"</div>",
        unsafe_allow_html=True
    )
    if st.button("▶️ 이어서 복구/재시작", key="resume_btn"):
        st.session_state.resume_job = rj["job_id"]

# ---- 컨트롤 버튼/진행상태 ----
col1, col2 = st.columns(2)
run = col1.button("▶️ 모니터링 시작", use_container_width=True)
stop = col2.button("⏹️ 중지", use_container_width=True)

if run:
    if not is_valid_email(recipient):
        st.session_state.email_required = True
        focus_email()
        st.stop()
    else:
        st.session_state.running = True
        st.session_state.email_required = False
        st.session_state.progress = 0
        st.session_state.start_time = datetime.now(KST)
        st.session_state.results = []

if stop:
    st.session_state.running = False

# ---- HTTP 세션+Retry ----
session = requests.Session()
session.mount("https://", HTTPAdapter(
    max_retries=Retry(total=2, backoff_factor=1, status_forcelist=[500,502,503,504])
))

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

# ---- 진행률 바/진행상태 ----
prog_placeholder = st.empty()
status_placeholder = st.empty()

# ---- 모니터링 수행 (Main) ----
if st.session_state.get("running", False):
    job_id = datetime.now(KST).strftime("%Y%m%d-%H%M%S")
    ts0 = datetime.now(KST).isoformat()
    jobs_ws.append_row([job_id, recipient, ts0, "running"])

    with st.spinner("회사 목록 로드 중…"):
        corps, corp_err = load_corp_list(corp_key)
        if not corps:
            st.session_state.running = False
            st.error(f"회사 목록 로드 실패: {corp_err}")
            jobs_ws.append_row([job_id, recipient, datetime.now(KST).isoformat(), "failed"])
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
    start_time = datetime.now()
    for i, (corp, y, rpt) in enumerate(targets, 1):
        if not st.session_state.get("running", False):
            break
        rows, err = fetch_execs(corp_key, corp["corp_code"], y, rpt)
        elapsed = (datetime.now() - start_time).total_seconds()
        speed = i / elapsed if elapsed else 1
        eta = int((N-i) / speed) if speed > 0 else 0
        prog_placeholder.progress(i/N, text=f"{i:,}/{N:,} ({i/N*100:.0f}%) · 예상 남은 시간 {eta//60}분 {eta%60}초")
        status_placeholder.markdown(
            f"<span style='color:#222;font-size:17px;font-weight:600;'>"
            f"{corp['corp_name']} · {y}년 · {REPORTS[rpt]}</span>", unsafe_allow_html=True
        )
        if err:
            continue
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
    st.session_state.running = False
    prog_placeholder.progress(1.0, text=f"전체 조회 완료!")

    # --- 결과 표시/다운로드/메일발송 ---
    ts1 = datetime.now(KST).isoformat()
    prog_ws.append_row([job_id, N, f"{start_y}-{end_y}", ",".join(REPORTS[r] for r in sel_reports), ts1, len(results)])
    status = "completed" if i == N else "stopped"
    r = jobs_ws.find(job_id, in_column=1)
    jobs_ws.update_cell(r.row, 4, status)

    df = pd.DataFrame(results)
    if df.empty:
        st.info("🔍 매칭 결과 없음. 메일 미발송.")
    else:
        st.success(f"총 {len(df):,}건 매칭 완료")
        st.dataframe(df, use_container_width=True)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name="Sheet1")

        st.download_button(
            "📥 XLSX 다운로드", data=buf.getvalue(),
            file_name=f"dart_results_{job_id}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        if st.button("📧 결과 메일 발송"):
            send_email(
                to_email=recipient,
                subject=f"[DART Monitor {job_id}] 결과",
                body=(f"작업ID: {job_id}\n시작: {ts0}\n종료: {ts1}\n총 호출: {N:,}회\n매칭: {len(results):,}건"),
                attachment_bytes=buf.getvalue(),
                filename=f"dart_results_{job_id}.xlsx"
            )
            st.success(f"결과를 {recipient} 로 발송했습니다.")

# ---- 이메일 발송 함수 ----
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
