import streamlit as st
import json, io, os
import requests, zipfile, xml.etree.ElementTree as ET, pandas as pd
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import gspread
from google.oauth2.service_account import Credentials
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# ------------------ 스타일 (애플 UI 감성) ------------------
st.set_page_config(page_title="DART 임원 주요경력 모니터링", layout="wide")
st.markdown("""
<style>
.stApp {background: #f8f9fa !important; font-family: 'SF Pro Display','Apple SD Gothic Neo','Pretendard','Inter',sans-serif;}
.stButton button, .stTextInput input, .stRadio > div, .stDownloadButton button {border-radius: 12px !important;}
.stProgress > div > div {background-color: #007aff !important;}
.stTextInput input {font-size: 17px !important;}
.stMarkdown, .stDataFrame, .stMultiselect, .stSlider {font-size: 17px;}
.job-badge {background:#e6f0ff;color:#0057b7;font-weight:600;border-radius:8px;padding:2px 10px;display:inline-block;margin-right:6px;}
</style>
""", unsafe_allow_html=True)
st.markdown("## <span style='color:#222;font-weight:600;'>DART 임원 <span style='color:#007aff'>‘주요경력’</span> 모니터링 서비스</span>", unsafe_allow_html=True)

# ------------------ 구글 시트 인증 ------------------
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

# ------------------ API Key 입력 (프리셋 + 직접) ------------------
preset_keys = [
    ("API 1", "eeb883965e882026589154074cddfc695330693c"),
    ("API 2", "1290bb1ec7879cba0e9f9b350ac97bb5d38ec176"),
    ("API 3", "5e75506d60b4ab3f325168019bcacf364cf4937e"),
    ("API 4", "6c64f7efdea057881deb91bbf3aaa5cb8b03d394"),
    ("API 5", "d9f0d92fbdc3a2205e49c66c1e24a442fa8c6fe8"),
]
st.markdown("#### 🔑 DART API Key (프리셋 복수선택 + 직접입력 추가/수정)")
cols = st.columns(6)
api_presets = [cols[i].checkbox(f"{name}", key=f"api_{i}", value=(i in [0, 4])) for i, (name, _) in enumerate(preset_keys)]
selected_presets = [preset_keys[i][1] for i, checked in enumerate(api_presets) if checked]
api_keys_input = cols[5].text_area("API Key 직접 입력 (쉼표/줄바꿈 가능)", value="", height=38, key="api_input")
direct_keys = [k.strip() for line in api_keys_input.splitlines() for k in line.split(",") if k.strip()]
api_keys = selected_presets + direct_keys
api_keys = list(dict.fromkeys(api_keys))  # 중복제거, 순서유지

if not api_keys:
    st.warning("최소 1개 이상의 DART API KEY가 필요합니다.")
    st.stop()

# ------------------ 입력 파라미터 ------------------
recipient = st.text_input("📧 결과 수신 이메일 (필수)", value="")
keywords  = st.text_input("🔍 키워드 (쉼표구분)", "이촌,삼정,안진")
REPORTS = {"11013":"1분기보고서","11012":"반기보고서","11014":"3분기보고서","11011":"사업보고서(연간)"}
sel_reports = st.multiselect(
    "📑 보고서 종류", options=list(REPORTS.keys()),
    format_func=lambda c: f"{REPORTS[c]} ({c})", default=["11011"]
)
listing = st.multiselect("🏷️ 회사 구분", ["상장사","비상장사"], default=["상장사"])
cy = datetime.now().year
start_y, end_y = st.slider("📅 사업연도 범위", 2000, cy, (cy-1, cy))
col1, col2 = st.columns(2)
run, stop = col1.button("▶️ 모니터링 시작", use_container_width=True), col2.button("⏹️ 중지", use_container_width=True)

# --- 이메일 형식 검증 ---
def is_valid_email(email):
    return "@" in email and "." in email and len(email) >= 6
if run and not recipient:
    st.warning("결과 수신 이메일을 입력해 주세요.")
    st.session_state['focus_email'] = True
    st.stop()
if run and not is_valid_email(recipient):
    st.warning("정상 이메일을 입력해 주세요. (예: user@domain.com)")
    st.session_state['focus_email'] = True
    st.stop()

# (최신 Streamlit: email 입력에 focus 줄 수 있음)
if st.session_state.get('focus_email'):
    st.components.v1.html("""
        <script>
        setTimeout(function(){
            const inp = window.parent.document.querySelector('input[type="text"]');
            if(inp) { inp.focus(); }
        }, 200);
        </script>
    """, height=0)
    st.session_state['focus_email'] = False

# ------------------ HTTP 세션 + Retry ------------------
session = requests.Session()
session.mount("https://", HTTPAdapter(
    max_retries=Retry(total=2, backoff_factor=1, status_forcelist=[500,502,503,504])
))

# ------------------ corpCode.xml 캐싱 ------------------
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

# ------------------ 임원현황 API 호출 (키 순환) ------------------
key_idx, call_cnt = 0, 0
def get_next_key():
    global key_idx, call_cnt
    if call_cnt and call_cnt % 20000 == 0:
        key_idx = (key_idx + 1) % len(api_keys)
    return api_keys[key_idx]
def fetch_execs(key, corp_code, year, rpt):
    try:
        payload = {
            "crtfc_key": key,
            "corp_code": corp_code,
            "bsns_year": str(year),
            "reprt_code": rpt
        }
        resp = session.get(
            "https://opendart.fss.or.kr/api/exctvSttus.json",
            params=payload, timeout=20
        )
        if resp.status_code == 429 or (resp.headers.get("X-RateLimit-Remaining") == "0"):
            return [], "API 사용한도 초과"
        data = resp.json()
        if data.get("status") == "020":
            return [], "API 사용한도 초과"
        if data.get("status") != "000":
            return [], data.get("message")
        return data.get("list", []), None
    except Exception as e:
        return [], str(e)

# ------------------ 상태 관리 ------------------
if "running" not in st.session_state:
    st.session_state.running = False
if "results" not in st.session_state:
    st.session_state.results = []
if "progress_idx" not in st.session_state:
    st.session_state.progress_idx = 0
if "targets" not in st.session_state:
    st.session_state.targets = []

# ---- "이어서 복구" 기능용, 이전 미완료 작업 불러오기 ----
restore_job = None
recent_failed = []
try:
    all_jobs = jobs_ws.get_all_records()
    for row in reversed(all_jobs):
        if row.get("status") == "stopped":
            recent_failed.append(row)
except: pass
if recent_failed:
    rj = recent_failed[0]
    st.info(f"🔄 미완료(중단) 작업 이어받기: <span class='job-badge'>{rj['job_id']}</span> ({rj.get('user_email','')}, {rj.get('start_time','')})", unsafe_allow_html=True)
    if st.button("▶️ 이어서 복구/재시작"):
        st.session_state.running = True
        # targets 다시 세팅 (전체 → progress_idx 부터)
        st.session_state.progress_idx = int(rj.get("processed_count", 0))
        # results 재세팅 (기존 결과 복원)
        # (예시: 로컬/DB/S3에 임시 저장된 DataFrame을 불러오는 방식도 가능. 여기선 간단화)
        st.session_state.results = []
        st.experimental_rerun()

# ------------------ 모니터링 루프 ------------------
prog_placeholder = st.empty()
status_placeholder = st.empty()
dl_placeholder = st.empty()

def save_progress(results, N, job_id, ts0, ts1, recipient, status):
    prog_ws.append_row([job_id, N, ts0, ts1, len(results), status])
    r = jobs_ws.find(job_id, in_column=1)
    jobs_ws.update_cell(r.row, 4, status)

def show_download_ui(results, recipient, job_id, ts0, ts1, N):
    df = pd.DataFrame(results)
    if not df.empty:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name="Sheet1")
        dl_placeholder.download_button(
            "📥 지금까지 결과 다운로드",
            data=buf.getvalue(),
            file_name=f"dart_results_{job_id}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        # 이메일 발송
        if st.button("✉️ 지금까지 결과 메일 발송"):
            send_email(
                to_email=recipient,
                subject=f"[DART Monitor {job_id}] 중간결과",
                body=(f"작업ID: {job_id}\n시작: {ts0}\n종료: {ts1}\n총 호출: {N:,}회\n매칭: {len(results):,}건"),
                attachment_bytes=buf.getvalue(),
                filename=f"dart_results_{job_id}.xlsx"
            )
            st.success(f"결과를 {recipient} 로 발송했습니다.")

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

if run or st.session_state.running:
    if not st.session_state.running:
        st.session_state.running = True
        st.session_state.results = []
        st.session_state.progress_idx = 0
        st.session_state.targets = []
    job_id, ts0 = datetime.now().strftime("%Y%m%d-%H%M%S"), datetime.now().isoformat()
    if not st.session_state.targets:
        with st.spinner("회사 목록 로드 중…"):
            corps, corp_err = load_corp_list(api_keys[0])
            if not corps:
                st.session_state.running = False
                st.error(f"회사 목록 로드 실패: {corp_err}")
                st.stop()
        kws = [w.strip() for w in keywords.split(",") if w.strip()]
        all_c = [
            c for c in corps
            if ((c["stock_code"] and "상장사" in listing)
                or (not c["stock_code"] and "비상장사" in listing))
        ]
        st.session_state.targets = [
            (c, y, r)
            for c in all_c
            for y in range(start_y, end_y+1)
            for r in sel_reports
        ]
    targets = st.session_state.targets
    N = len(targets)
    st.success(f"총 호출 대상: {N:,}건")
    prog_bar = prog_placeholder.progress(st.session_state.progress_idx / N if N else 0)
    for i in range(st.session_state.progress_idx, N):
        corp, y, rpt = targets[i]
        key = get_next_key()
        rows, err = fetch_execs(key, corp["corp_code"], y, rpt)
        if err:
            if "사용한도" in err:
                st.warning(f"API({key[:7]+'..'}) 사용한도 소진! 다음 API로 교체, 혹은 중지/이어받기", icon="⛔")
                show_download_ui(st.session_state.results, recipient, job_id, ts0, datetime.now().isoformat(), N)
                save_progress(st.session_state.results, N, job_id, ts0, datetime.now().isoformat(), recipient, "stopped")
                st.session_state.running = False
                st.session_state.progress_idx = i
                break
            else:
                status_placeholder.warning(f"{corp['corp_name']} {y}-{REPORTS[rpt]}: {err}", icon="⚠️")
                continue
        status_placeholder.markdown(
            f"<span style='color:#666;font-size:17px;'>"
            f"[{i+1}/{N}] <b style='color:#007aff'>{corp['corp_name']}</b> · {y}년 · {REPORTS[rpt]} 검색 중…"
            f"</span>", unsafe_allow_html=True
        )
        for r in rows:
            mc = r.get("main_career", "")
            if any(k in mc for k in kws):
                st.session_state.results.append({
                    "회사명":     corp["corp_name"],
                    "종목코드":   corp["stock_code"] or "비상장",
                    "사업연도":   y,
                    "보고서종류": REPORTS[rpt],
                    "임원이름":   r.get("nm",""),
                    "직위":       r.get("ofcps",""),
                    "주요경력":   mc,
                    "매칭키워드": ",".join([k for k in kws if k in mc])
                })
        prog_bar.progress((i+1)/N)
        st.session_state.progress_idx = i+1
    # 작업 완료
    ts1 = datetime.now().isoformat()
    save_progress(st.session_state.results, N, job_id, ts0, ts1, recipient, "completed" if st.session_state.running else "stopped")
    if st.session_state.running:
        show_download_ui(st.session_state.results, recipient, job_id, ts0, ts1, N)
        st.success("✅ 전체 조회 완료!")
        st.session_state.running = False
        st.session_state.progress_idx = 0
        st.session_state.targets = []
    else:
        st.info("🔴 작업이 중지되었습니다. 지금까지의 결과를 다운로드/메일발송 하실 수 있습니다.")

# (끝) - 전체 복사해서 사용 가능

