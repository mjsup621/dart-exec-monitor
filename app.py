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
import time

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

# --- API 호출량 관리 ---
def get_api_usage_info():
    """API별 호출 가능량 정보 반환 (24시간마다 리셋)"""
    current_time = datetime.now(KST)
    today = current_time.strftime("%Y%m%d")
    
    if 'api_usage_date' not in st.session_state or st.session_state.api_usage_date != today:
        # 새로운 날짜면 모든 API 호출량 리셋
        st.session_state.api_usage_date = today
        st.session_state.api_usage = {
            "eeb883965e882026589154074cddfc695330693c": 20000,
            "1290bb1ec7879cba0e9f9b350ac97bb5d38ec176": 20000,
            "5e75506d60b4ab3f325168019bcacf364cf4937e": 20000,
            "6c64f7efdea057881deb91bbf3aaa5cb8b03d394": 20000,
            "d9f0d92fbdc3a2205e49c66c1e24a442fa8c6fe8": 20000,
            "c38b1fdef8960f694f56a50cf4e52d5c25fd5675": 20000,
        }
    
    return st.session_state.api_usage

def update_api_usage(api_key, used_count=1):
    """API 호출량 업데이트"""
    usage_info = get_api_usage_info()
    if api_key in usage_info:
        usage_info[api_key] = max(0, usage_info[api_key] - used_count)

# --- API 호출량 관리 ---
def get_api_usage_info():
    """API별 호출 가능량 정보 반환 (24시간마다 리셋)"""
    current_time = datetime.now(KST)
    today = current_time.strftime("%Y%m%d")
    
    if 'api_usage_date' not in st.session_state or st.session_state.api_usage_date != today:
        # 새로운 날짜면 모든 API 호출량 리셋
        st.session_state.api_usage_date = today
        st.session_state.api_usage = {
            "eeb883965e882026589154074cddfc695330693c": 20000,
            "1290bb1ec7879cba0e9f9b350ac97bb5d38ec176": 20000,
            "5e75506d60b4ab3f325168019bcacf364cf4937e": 20000,
            "6c64f7efdea057881deb91bbf3aaa5cb8b03d394": 20000,
            "d9f0d92fbdc3a2205e49c66c1e24a442fa8c6fe8": 20000,
            "c38b1fdef8960f694f56a50cf4e52d5c25fd5675": 20000,
        }
    
    return st.session_state.api_usage

def update_api_usage(api_key, used_count=1):
    """API 호출량 업데이트"""
    usage_info = get_api_usage_info()
    if api_key in usage_info:
        usage_info[api_key] = max(0, usage_info[api_key] - used_count)

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
.api-limit-warning {background:#fff3cd;border:1px solid #ffeaa7;border-radius:8px;padding:12px;margin:10px 0;}
.success-box {background:#d1edff;border-radius:8px;padding:12px;margin:10px 0;}
.progress-container {background:#f8f9fa;border-radius:8px;padding:12px;margin:10px 0;border:1px solid #e9ecef;}
.stProgress > div > div > div {font-size: 16px !important; font-weight: 600 !important; color: #333 !important;}
</style>
""", unsafe_allow_html=True)

st.markdown("<h2 style='font-size:2.3rem;margin-bottom:0.7em;'><b>DART 임원 <span style='color:#007aff'>'주요경력'</span> 모니터링 서비스</b></h2>", unsafe_allow_html=True)

# --- 최근 사용 API 관리 함수 ---
def get_recent_apis():
    """최근 사용한 API 키 3개 가져오기"""
    if 'recent_apis' not in st.session_state:
        st.session_state.recent_apis = []
    return st.session_state.recent_apis[:3]

def add_recent_api(api_key):
    """최근 사용 API에 추가 (중복 제거, 최대 3개)"""
    if 'recent_apis' not in st.session_state:
        st.session_state.recent_apis = []
    
    if api_key in st.session_state.recent_apis:
        st.session_state.recent_apis.remove(api_key)
    
    st.session_state.recent_apis.insert(0, api_key)
    st.session_state.recent_apis = st.session_state.recent_apis[:3]

# --- API KEY (프리셋 + 최근 사용 + 직접입력) ---
api_usage_info = get_api_usage_info()

api_presets = [
    ("API 1", "eeb883965e882026589154074cddfc695330693c"),
    ("API 2", "1290bb1ec7879cba0e9f9b350ac97bb5d38ec176"),
    ("API 3", "5e75506d60b4ab3f325168019bcacf364cf4937e"),
    ("API 4", "6c64f7efdea057881deb91bbf3aaa5cb8b03d394"),
    ("API 5", "d9f0d92fbdc3a2205e49c66c1e24a442fa8c6fe8"),
    ("API 6", "c38b1fdef8960f694f56a50cf4e52d5c25fd5675"),
]

# API 프리셋에 호출 가능량 표시
api_labels_with_usage = []
for name, key in api_presets:
    remaining = api_usage_info.get(key, 20000)
    api_labels_with_usage.append(f"{name}(호출가능: {remaining:,})")

# 최근 사용 API 표시
recent_apis = get_recent_apis()
if recent_apis:
    st.markdown("**🕐 최근 사용 API** (참고용)")
    for i, api in enumerate(recent_apis, 1):
        st.markdown(f"&nbsp;&nbsp;최근 {i}: `{api[:8]}...{api[-8:]}`")

api_keys_list = [x[1] for x in api_presets]

# ---- 프리셋 API 키 선택 (한 개만 선택 가능) ----
col_api_left, col_api_right = st.columns([1,3])
with col_api_left:
    st.markdown("<div class='api-label'>프리셋 API KEY<br>(한 개만 선택)</div>", unsafe_allow_html=True)
    
    # 단일 라디오 버튼으로 모든 API 옵션 표시
    selected_preset = st.radio(
        "", 
        options=api_labels_with_usage, 
        index=0, 
        key="api_preset_single"
    )
    
    # 선택된 API 키 추출
    selected_index = api_labels_with_usage.index(selected_preset)
    api_key_selected = api_presets[selected_index][1]

with col_api_right:
    st.markdown("<div class='api-label'>API Key 직접 입력<br><span style='font-size:13px;color:#888;'>(입력 시 프리셋 무시됨)</span></div>", unsafe_allow_html=True)
    api_key_input = st.text_area(
        "", value="", height=40, placeholder="API 키를 직접 입력하세요 (한 개만)"
    )

# API 키 최종 결정 로직: 직접 입력이 있으면 프리셋 무시
api_keys = [k.strip() for k in api_key_input.replace(",", "\n").splitlines() if k.strip()]
if api_keys:
    corp_key = api_keys[0]  # 직접 입력된 첫 번째 키 사용
    st.info(f"✅ 직접 입력 API 사용: `{corp_key[:8]}...{corp_key[-8:]}`")
else:
    corp_key = api_key_selected  # 프리셋에서 선택된 키 사용
    st.info(f"✅ 프리셋 API 사용: **{api_presets[selected_index][0]}** (`{corp_key[:8]}...{corp_key[-8:]}`)")

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
    # 날짜/시간 형식 변경
    start_time_str = rj.get('start_time', '')
    if start_time_str:
        try:
            # ISO 형식을 파싱하여 원하는 형식으로 변환
            dt = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = KST.localize(dt)
            else:
                dt = dt.astimezone(KST)
            formatted_date = dt.strftime('%Y-%m-%d')
            formatted_time = dt.strftime('%H:%M')
            display_time = f"{formatted_date}, {formatted_time}"
        except:
            display_time = start_time_str
    else:
        display_time = "시간 정보 없음"
    
    st.markdown(
        f"<div style='background:#eef6fe;border-radius:9px;padding:12px 16px 8px 16px;margin-bottom:5px;'>"
        f"🔄 <b>미완료(중단) 작업 이어받기:</b> "
        f"<span class='job-badge'>{rj['job_id']}</span> "
        f"({rj.get('user_email','')}, {display_time})"
        f"</div>",
        unsafe_allow_html=True
    )
    if st.button("▶️ 이어서 복구/재시작", key="resume_btn"):
        st.session_state.resume_job_id = rj["job_id"]
        st.session_state.resume_data = rj
        
        # 이어받기 데이터에 진행 상황 추가
        if 'resume_progress' in st.session_state:
            rj['resume_progress'] = st.session_state.resume_progress
        if 'resume_results' in st.session_state:
            rj['resume_results'] = st.session_state.resume_results
            
        st.success(f"작업 {rj['job_id']} 복구 준비 완료!")

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
        st.session_state.api_call_count = 0
        # 최근 사용 API에 추가
        add_recent_api(corp_key)

if stop:
    st.session_state.running = False

# ---- HTTP 세션+Retry ----
session = requests.Session()
session.mount("https://", HTTPAdapter(
    max_retries=Retry(total=2, backoff_factor=1, status_forcelist=[500,502,503,504])
))

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

def check_api_limit_error(data):
    """API 한도 초과 에러 체크"""
    if isinstance(data, dict):
        status = data.get("status")
        message = data.get("message", "")
        # API 한도 초과 관련 에러 코드들
        if status in ["020", "021"] or "한도" in message or "limit" in message.lower():
            return True
    return False

def fetch_execs(key, corp_code, year, rpt):
    try:
        payload = {
            "crtfc_key": key,
            "corp_code": corp_code,
            "bsns_year": str(year),
            "reprt_code": rpt
        }
        
        # API 호출 카운트 증가
        if 'api_call_count' not in st.session_state:
            st.session_state.api_call_count = 0
        st.session_state.api_call_count += 1
        
        # API 사용량 업데이트
        update_api_usage(key)
        
        response = session.get(
            "https://opendart.fss.or.kr/api/exctvSttus.json",
            params=payload, timeout=20
        )
        
        data = response.json()
        
        # API 한도 초과 체크
        if check_api_limit_error(data):
            return [], "API_LIMIT_EXCEEDED"
        
        if data.get("status") != "000":
            return [], data.get("message")
            
        return data.get("list", []), None
        
    except Exception as e:
        return [], str(e)

# ---- 이메일 발송 함수 ----
def send_email(to_email, subject, body, attachment_bytes=None, filename=None):
    try:
        from_email = st.secrets["smtp"]["sender_email"]
        from_pwd   = st.secrets["smtp"]["sender_password"]
        
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, "plain", 'utf-8'))
        
        if attachment_bytes and filename:
            part = MIMEApplication(attachment_bytes)
            part.add_header('Content-Disposition', 'attachment', filename=filename)
            msg.attach(part)
        
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(from_email, from_pwd)
            server.send_message(msg)
        
        return True, "메일 발송 성공"
    except Exception as e:
        return False, f"메일 발송 실패: {str(e)}"

# ---- 이전 결과 표시 (새 작업 시작 전에도 보여주기) ----
if 'monitoring_results' in st.session_state and st.session_state.monitoring_results:
    st.markdown("---")
    st.markdown("### 📊 이전 검색 결과")
    
    prev_df = pd.DataFrame(st.session_state.monitoring_results)
    st.success(f"💾 저장된 결과: {len(prev_df):,}건 (작업ID: {st.session_state.get('current_job_id', 'Unknown')})")
    st.dataframe(prev_df, use_container_width=True)
    
    # 이전 결과 다운로드 버튼 (항상 사용 가능)
    prev_buf = io.BytesIO()
    with pd.ExcelWriter(prev_buf, engine="openpyxl") as w:
        prev_df.to_excel(w, index=False, sheet_name="DART_Results")
    prev_excel_data = prev_buf.getvalue()
    
    col_download, col_clear = st.columns([1, 1])
    with col_download:
        st.download_button(
            "📥 저장된 결과 다운로드", 
            data=prev_excel_data,
            file_name=f"dart_results_{st.session_state.get('current_job_id', 'saved')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_saved_results"
        )
    with col_clear:
        if st.button("🗑️ 저장된 결과 삭제", key="clear_saved_results"):
            st.session_state.monitoring_results = []
            if 'current_job_id' in st.session_state:
                del st.session_state.current_job_id
            st.success("저장된 결과가 삭제되었습니다.")
            st.rerun()

# ---- 진행률 바/진행상태 (모니터링 시작 시에만 표시) ----
prog_placeholder = st.empty()
status_placeholder = st.empty()
api_status_placeholder = st.empty()

# ---- 모니터링 수행 (Main) ----
if st.session_state.get("running", False) or st.session_state.get("resume_job_id"):
    
    # 이어받기 모드인지 확인
    is_resume = bool(st.session_state.get("resume_job_id"))
    
    if is_resume:
        job_id = st.session_state.resume_job_id
        st.info(f"🔄 작업 {job_id} 이어받기 시작...")
        # 기존 작업 상태를 running으로 변경
        job_row = jobs_ws.find(job_id, in_column=1)
        if job_row:
            jobs_ws.update_cell(job_row.row, 4, "running")
    else:
        job_id = datetime.now(KST).strftime("%Y%m%d-%H%M%S")
        ts0 = datetime.now(KST).isoformat()
        jobs_ws.append_row([job_id, recipient, ts0, "running"])

    with st.spinner("회사 목록 로드 중…"):
        corps, corp_err = load_corp_list(corp_key)
        if not corps:
            st.session_state.running = False
            st.error(f"회사 목록 로드 실패: {corp_err}")
            if not is_resume:
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
    
    # 결과를 세션 상태에 저장 (다운로드 후에도 유지)
    if 'monitoring_results' not in st.session_state:
        st.session_state.monitoring_results = []
    if 'current_job_id' not in st.session_state:
        st.session_state.current_job_id = job_id
    
    # 이어받기 모드인 경우 이전 결과 복원
    if is_resume and st.session_state.get("resume_data") and 'resume_results' in st.session_state.resume_data:
        st.session_state.monitoring_results = st.session_state.resume_data['resume_results']
        results = st.session_state.monitoring_results.copy()
    else:
        # 새 작업 시작 시 이전 결과 초기화
        if st.session_state.current_job_id != job_id:
            st.session_state.monitoring_results = []
            st.session_state.current_job_id = job_id
        results = st.session_state.monitoring_results.copy()
    start_time = datetime.now()
    api_limit_hit = False
    
    # 진행률 초기화
    st.session_state.total_count = N
    st.session_state.current_count = 0
    
    # 진행률바 표시 시작 (API 호출 횟수 포함)
    prog_placeholder.markdown("<div class='progress-container'>", unsafe_allow_html=True)
    prog_placeholder.markdown("**📊 진행 상황**")
    
    # 초기 진행률바 표시 (0%부터 시작)
    prog_placeholder.progress(0, text=f"📊 API 호출: 0/20,000 | 진행: 0/{N:,} (0%) | 남은시간: 계산 중...")
    
    # 이어받기 모드인 경우 시작 인덱스 설정
    start_index = 0
    if is_resume and st.session_state.get("resume_data"):
        # 이어받기 데이터에서 이전 진행 상황 복원
        resume_data = st.session_state.resume_data
        if 'resume_progress' in resume_data:
            start_index = int(resume_data.get('resume_progress', 0))
            st.session_state.current_count = start_index
            st.session_state.progress = start_index / N if N > 0 else 0
    
    for i, (corp, y, rpt) in enumerate(targets[start_index:], start_index + 1):
        # 중지 버튼 체크 (이어받기 모드에서도 작동하도록)
        if not st.session_state.get("running", False):
            break
        
        rows, err = fetch_execs(corp_key, corp["corp_code"], y, rpt)
        
        # API 한도 초과 감지
        if err == "API_LIMIT_EXCEEDED":
            api_limit_hit = True
            st.session_state.running = False
            
            # 현재까지의 진행 상황을 세션에 저장 (이어받기용)
            st.session_state.resume_progress = i - 1
            st.session_state.resume_results = results.copy()
            
            # 현재까지의 진행 상황 저장
            prog_ws.append_row([
                job_id, f"{i-1}/{N}", f"{start_y}-{end_y}", 
                ",".join(REPORTS[r] for r in sel_reports), 
                datetime.now(KST).isoformat(), len(results)
            ])
            
            # 작업 상태를 stopped로 변경
            job_row = jobs_ws.find(job_id, in_column=1)
            if job_row:
                jobs_ws.update_cell(job_row.row, 4, "stopped")
            
            st.error("🚫 API 일일 한도(20,000회) 초과! 다른 API 키를 선택하여 이어받기를 진행하세요.")
            st.markdown(
                "<div class='api-limit-warning'>"
                f"⚠️ <b>API 한도 초과 안내</b><br>"
                f"• 현재까지 처리: {i-1:,}/{N:,}건<br>"
                f"• 매칭된 결과: {len(results):,}건<br>"
                f"• 다른 API 키로 변경 후 '이어받기' 버튼을 클릭하세요."
                "</div>", 
                unsafe_allow_html=True
            )
            break
        
        if err and err != "API_LIMIT_EXCEEDED":
            continue
            
        # 진행률 및 상태 업데이트 (매번 업데이트)
        st.session_state.current_count = i
        st.session_state.progress = i / N
        
        elapsed = (datetime.now() - start_time).total_seconds()
        speed = i / elapsed if elapsed > 0 else 1
        eta = int((N-i) / speed) if speed > 0 else 0
        
        # 진행률바 업데이트 (매번 즉시 업데이트)
        prog_placeholder.progress(
            st.session_state.progress, 
            text=f"📊 API 호출: {st.session_state.get('api_call_count', 0):,}/20,000 | 진행: {i:,}/{N:,} ({st.session_state.progress*100:.0f}%) | 남은시간: {eta//60}분 {eta%60}초"
        )
        
        status_placeholder.markdown(
            f"<span style='color:#222;font-size:17px;font-weight:600;'>"
            f"{corp['corp_name']} · {y}년 · {REPORTS[rpt]}</span>", 
            unsafe_allow_html=True
        )
        
        # 결과 수집 및 세션 상태에 저장
        for r in rows:
            mc = r.get("main_career", "")
            if any(k in mc for k in kws):
                new_result = {
                    "회사명":     corp["corp_name"],
                    "종목코드":   corp["stock_code"] or "비상장",
                    "사업연도":   y,
                    "보고서종류": REPORTS[rpt],
                    "임원이름":   r.get("nm",""),
                    "직위":       r.get("ofcps",""),
                    "주요경력":   mc,
                    "매칭키워드": ",".join([k for k in kws if k in mc])
                }
                results.append(new_result)
                st.session_state.monitoring_results.append(new_result)
        
        # 잠시 쉬기 (API 호출 제한 준수, 진행률바 업데이트를 위해 짧게)
        time.sleep(0.05)
    
    # API 한도 초과가 아닌 경우에만 완료 처리
    if not api_limit_hit:
        st.session_state.running = False
        st.session_state.progress = 1.0
        
        # 진행률바 완료 표시
        prog_placeholder.progress(1.0, text="✅ 전체 조회 완료!")
        
        # 완료 상태 업데이트
        ts1 = datetime.now(KST).isoformat()
        prog_ws.append_row([
            job_id, N, f"{start_y}-{end_y}", 
            ",".join(REPORTS[r] for r in sel_reports), 
            ts1, len(results)
        ])
        
        status = "completed"
        job_row = jobs_ws.find(job_id, in_column=1)
        if job_row:
            jobs_ws.update_cell(job_row.row, 4, status)

    # --- 결과 처리 (완료 또는 중단 모두) ---
    # 최종 결과는 세션에서 가져오기
    final_results = st.session_state.monitoring_results
    df = pd.DataFrame(final_results)
    
    if df.empty and not api_limit_hit:
        st.info("🔍 매칭 결과 없음.")
        # 빈 결과도 메일로 알림
        email_subject = f"[DART] {start_y}-{end_y}년 {','.join(REPORTS[r] for r in sel_reports)} 모니터링 결과 (결과 없음)"
        email_body = f"""
작업ID: {job_id}
시작시간: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}
검색 키워드: {keywords}
검색 범위: {start_y}-{end_y}년
보고서 종류: {', '.join(REPORTS[r] for r in sel_reports)}
총 호출 건수: {st.session_state.get('api_call_count', 0):,}회
매칭 결과: 0건

검색 조건에 맞는 결과가 없습니다.
"""
        success, msg = send_email(recipient, email_subject, email_body)
        if success:
            st.success(f"결과 없음 알림을 {recipient}로 발송했습니다.")
        else:
            st.error(f"메일 발송 실패: {msg}")
            
    elif len(df) > 0:
        st.success(f"총 {len(df):,}건 매칭 완료")
        st.dataframe(df, use_container_width=True)
        
        # Excel 파일 생성
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name="DART_Results")
        excel_data = buf.getvalue()
        
        # 다운로드 버튼 (결과 리셋 방지)
        st.download_button(
            "📥 XLSX 다운로드", 
            data=excel_data,
            file_name=f"dart_results_{job_id}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"download_{job_id}"
        )
        
        # **핵심: 자동 메일 발송**
        email_subject = f"[DART] {start_y}-{end_y}년 {','.join(REPORTS[r] for r in sel_reports)} 모니터링 결과"
        
        status_text = "완료" if not api_limit_hit else "일시중단 (API 한도 초과)"
        
        email_body = f"""
작업ID: {job_id}
작업 상태: {status_text}
시작시간: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}
검색 키워드: {keywords}
검색 범위: {start_y}-{end_y}년
보고서 종류: {', '.join(REPORTS[r] for r in sel_reports)}
총 호출 건수: {st.session_state.get('api_call_count', 0):,}회
매칭 결과: {len(results):,}건

{'첨부된 Excel 파일을 확인하세요.' if len(results) > 0 else ''}
{'API 한도 초과로 작업이 중단되었습니다. 다른 API 키로 이어받기를 진행하세요.' if api_limit_hit else ''}
"""
        
        # 자동 메일 발송
        success, msg = send_email(
            to_email=recipient,
            subject=email_subject,
            body=email_body,
            attachment_bytes=excel_data,
            filename=f"dart_results_{job_id}.xlsx"
        )
        
        if success:
            st.markdown(
                f"<div class='success-box'>"
                f"✅ <b>결과가 자동으로 {recipient}에게 발송되었습니다!</b><br>"
                f"📧 제목: {email_subject}"
                f"</div>", 
                unsafe_allow_html=True
            )
        else:
            st.error(f"❌ 자동 메일 발송 실패: {msg}")
            
            # 수동 발송 버튼 제공
            if st.button("📧 수동 메일 발송 재시도", key=f"manual_send_{job_id}"):
                success2, msg2 = send_email(
                    to_email=recipient,
                    subject=email_subject,
                    body=email_body,
                    attachment_bytes=excel_data,
                    filename=f"dart_results_{job_id}.xlsx"
                )
                if success2:
                    st.success(f"수동 메일 발송 성공: {recipient}")
                else:
                    st.error(f"수동 메일 발송도 실패: {msg2}")

    # 세션 정리
    if 'resume_job_id' in st.session_state:
        del st.session_state.resume_job_id
    if 'resume_data' in st.session_state:
        del st.session_state.resume_data
