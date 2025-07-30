import streamlit as st
import json
import requests, zipfile, io, xml.etree.ElementTree as ET, pandas as pd
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import gspread
from google.oauth2.service_account import Credentials
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# -------------------- 1. Apple 스타일 심플 CSS (라운드, 블루, 폰트 등) --------------------
st.markdown("""
    <style>
    body, .stApp { font-family: 'SF Pro', 'Apple SD Gothic Neo', 'sans-serif' !important; }
    .css-18e3th9 { background-color: #f8fafd; }
    .stButton>button { border-radius: 12px; background: #1d72ff; color: #fff; font-weight: bold; height: 45px; font-size: 18px;}
    .stButton>button:active, .stButton>button:focus { background: #0056d6 !important;}
    .stProgress>div>div>div>div { background: linear-gradient(90deg, #63b3ff, #1d72ff 80%); height: 16px !important; border-radius: 10px;}
    .stTextInput>div>input, .stSelectbox>div>div { border-radius: 8px;}
    .block-container { padding-top: 24px; }
    .big-label { font-size: 1.4rem; font-weight: 700; color: #1d2939; letter-spacing: -1px;}
    .small-label { color: #748092; font-size: 1rem;}
    .status-bar { margin-top:12px; margin-bottom:20px; padding:16px; border-radius:14px; background:#e9f2ff; font-weight:600; color:#1d72ff; font-size:1.2rem; box-shadow:0 2px 10px #c3e0ff40;}
    .info-bar { background: #f7fafc; color: #2563eb; font-size:1.05rem; border-radius:8px; padding: 8px 20px;}
    </style>
""", unsafe_allow_html=True)

# -------------------- 2. 구글시트 인증 & 시트 열기 --------------------
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

# -------------------- 3. API KEY 프리셋 & 입력 UI --------------------
st.title("📊 DART 임원 ‘주요경력’ 모니터링")
st.markdown('<div class="big-label">애플 스타일 대용량 자동화 모니터링</div>', unsafe_allow_html=True)

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
corp_key = api_keys[0]

recipient = st.text_input("📧 결과 수신 이메일", value="")
keywords = st.text_input("🔍 키워드 (쉼표로 구분)", "이촌,삼정,안진")
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

# -------------------- 4. 작업 이어하기/복구 (구글시트 조회) --------------------
unfinished_jobs = jobs_ws.get_all_records()
unfinished = [j for j in unfinished_jobs if j["status"] in ["running", "interrupted"]]
if unfinished:
    st.markdown("#### ⏪ <b>이전 미완료 작업 이어하기</b>", unsafe_allow_html=True)
    for uj in unfinished:
        st.write(f"🔄 Job: {uj['job_id']} | {uj['user_email']} | {uj['start_time']} | 상태:{uj['status']}")
    if st.button("▶️ 미완료 작업 이어받기"):
        # 최근 미완료 Job의 파라미터를 복구 (예시)
        last_job = unfinished[-1]
        # 여기에 파라미터 복원/이어받기 로직 구현 (최신 상태 동기화 필요)
        st.session_state["resume_job_id"] = last_job["job_id"]
        st.session_state["running"] = True

# -------------------- 5. 실행/중지 버튼 --------------------
col1, col2 = st.columns(2)
run = col1.button("▶️ 모니터링 시작")
stop = col2.button("⏹️ 중지")
resume_job_id = st.session_state.get("resume_job_id", None)

# -------------------- 6. HTTP 세션 + Retry + 회사목록 캐시 --------------------
session = requests.Session()
session.mount("https://", HTTPAdapter(
    max_retries=Retry(total=2, backoff_factor=1, status_forcelist=[500,502,503,504])
))
@st.cache_data(ttl=3600, show_spinner="회사목록 로딩/캐시 중…")
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

# -------------------- 7. 임원현황 API 호출 --------------------
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

# -------------------- 8. 메일발송 함수 --------------------
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

# -------------------- 9. 진행상태 & 복구 세션 --------------------
if "running" not in st.session_state:
    st.session_state.running = False
if "results" not in st.session_state:
    st.session_state.results = []
if "job_id" not in st.session_state:
    st.session_state.job_id = None

# -------------------- 10. 실행/중지/이어받기 논리 --------------------
if run or resume_job_id:
    st.session_state.running = True
    if not resume_job_id:
        st.session_state.job_id = datetime.now().strftime("%Y%m%d-%H%M%S")
        jobs_ws.append_row([st.session_state.job_id, recipient, datetime.now().isoformat(), "running", ""])
        st.session_state.results = []
    else:
        st.session_state.job_id = resume_job_id
        # Progress Sheet에서 마지막 처리된 인덱스 찾아서 이어서 처리(아래 구현 참조)
else:
    if stop:
        st.session_state.running = False

# -------------------- 11. 모니터링/자동화 루프 (진행률, 복구, 중단) --------------------
if st.session_state.running:
    job_id = st.session_state.job_id
    # 회사목록 캐시 활용
    corps, corp_err = load_corp_list(corp_key)
    if not corps:
        st.error(f"회사 목록 로드 실패: {corp_err}")
        jobs_ws.append_row([job_id, recipient, datetime.now().isoformat(), "failed", ""])
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
    st.markdown(f'<div class="status-bar">총 호출 대상: <b>{N:,}건</b></div>', unsafe_allow_html=True)

    # 복구: 마지막 처리 인덱스(Progress 시트) 확인
    last_idx = 0
    if resume_job_id:
        all_logs = prog_ws.get_all_records()
        progresses = [i for i, row in enumerate(all_logs) if row['job_id'] == job_id]
        if progresses:
            last_idx = max(progresses) + 1

    prog = st.progress(last_idx / N)
    status_area = st.empty()
    results = st.session_state.results if last_idx > 0 else []
    stopped = False

    for i, (corp, y, rpt) in enumerate(targets[last_idx:], last_idx+1):
        if not st.session_state.running:
            stopped = True
            break
        rows, err = fetch_execs(corp_key, corp["corp_code"], y, rpt)
        matched_count = 0
        if err:
            status_area.info(f"⚠️ {corp['corp_name']} {y}-{REPORTS[rpt]}: {err}")
        else:
            for r in rows:
                mc = r.get("main_career","")
                if any(k in mc for k in kws):
                    matched_count += 1
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
            prog_ws.append_row([job_id, corp["corp_code"], y, rpt, datetime.now().isoformat(), matched_count, str(rows)[:1000]])
        prog.progress(i/N)
        status_area.markdown(f"<span class='info-bar'>[{i}/{N}] <b style='color:#1d72ff'>{corp['corp_name']}</b> · {y}년 · {REPORTS[rpt]} 검색중...</span>", unsafe_allow_html=True)
        st.session_state.results = results  # 중간중간 저장 (복구/이어받기)

    # -- 작업 끝 or 중지시 결과 처리
    if not stopped or stop:
        ts1 = datetime.now().isoformat()
        status_area.markdown(f"<span class='info-bar'>작업 완료! 결과를 메일 및 다운로드로 제공합니다.</span>", unsafe_allow_html=True)
        jobs_ws.append_row([job_id, recipient, ts1, "completed", "메일발송"])
        df = pd.DataFrame(results)
        if df.empty:
            st.info("🔍 매칭 결과 없음")
        else:
            st.success(f"총 **{len(df):,}**건 매칭 완료")
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as w:
                df.to_excel(w,index=False,sheet_name="Sheet1")
            send_email(
                to_email=recipient,
                subject=f"[DART Monitor {job_id}] 결과",
                body=(f"작업ID: {job_id}\n종료: {ts1}\n총 호출: {N:,}회\n매칭: {len(results):,}건"),
                attachment_bytes=buf.getvalue(),
                filename=f"dart_results_{job_id}.xlsx"
            )
            st.download_button(
                "📥 XLSX 다운로드",
                data=buf.getvalue(),
                file_name=f"dart_results_{job_id}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        st.session_state.running = False
        st.session_state.resume_job_id = None
        st.session_state.results = []

    elif stopped:  # 중간중지시 즉시 다운로드/메일
        ts1 = datetime.now().isoformat()
        status_area.markdown(f"<span class='info-bar'>⏹️ 중지됨! 지금까지 결과를 저장합니다.</span>", unsafe_allow_html=True)
        jobs_ws.append_row([job_id, recipient, ts1, "interrupted", "메일발송"])
        df = pd.DataFrame(results)
        if not df.empty:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as w:
                df.to_excel(w,index=False,sheet_name="Sheet1")
            send_email(
                to_email=recipient,
                subject=f"[DART Monitor {job_id}] (중간중지) 결과",
                body=(f"작업ID: {job_id}\n중단: {ts1}\n총 호출: {N:,}회\n매칭: {len(results):,}건"),
                attachment_bytes=buf.getvalue(),
                filename=f"dart_results_{job_id}_partial.xlsx"
            )
            st.download_button(
                "📥 지금까지 XLSX 다운로드",
                data=buf.getvalue(),
                file_name=f"dart_results_{job_id}_partial.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        st.session_state.running = False

