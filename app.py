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
try:
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
    GSHEET_CONNECTED = True
except Exception as e:
    st.error(f"Google Sheets 연결에 실패했습니다. 관리자에게 문의하세요. 오류: {e}")
    GSHEET_CONNECTED = False


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
.api-limit-warning {background:#fff3cd;border:1px solid #ffeaa7;border-radius:8px;padding:12px;margin:10px 0;}
.success-box {background:#d1edff;border-radius:8px;padding:12px;margin:10px 0;}
</style>
""", unsafe_allow_html=True)

st.markdown("<h2 style='font-size:2.3rem;margin-bottom:0.7em;'><b>DART 임원 <span style='color:#007aff'>'주요경력'</span> 모니터링 서비스</b></h2>", unsafe_allow_html=True)

# --- 최근 사용 API 관리 함수 ---
def get_recent_apis():
    if 'recent_apis' not in st.session_state:
        st.session_state.recent_apis = []
    return st.session_state.recent_apis[:3]

def add_recent_api(api_key):
    if 'recent_apis' not in st.session_state:
        st.session_state.recent_apis = []
    if api_key in st.session_state.recent_apis:
        st.session_state.recent_apis.remove(api_key)
    st.session_state.recent_apis.insert(0, api_key)
    st.session_state.recent_apis = st.session_state.recent_apis[:3]

# --- API KEY (프리셋 + 최근 사용 + 직접입력) ---
api_presets = [
    ("API 1", "eeb883965e882026589154074cddfc695330693c"),
    ("API 2", "1290bb1ec7879cba0e9f9b350ac97bb5d38ec176"),
    ("API 3", "5e75506d60b4ab3f325168019bcacf364cf4937e"),
    ("API 4", "6c64f7efdea057881deb91bbf3aaa5cb8b03d394"),
    ("API 5", "d9f0d92fbdc3a2205e49c66c1e24a442fa8c6fe8"),
    ("API 6", "c38b1fdef8960f694f56a50cf4e52d5c25fd5675"),
]

# (수정사항 2) API 남은 호출량 추적 및 표시
def update_and_get_api_labels(presets):
    """API 사용량 확인 및 라디오 버튼 레이블 생성 (매일 자정 리셋)"""
    if 'api_usage' not in st.session_state:
        st.session_state.api_usage = {}

    today = datetime.now(KST).strftime('%Y-%m-%d')
    formatted_labels = []

    for name, key in presets:
        # 오늘 날짜와 다르면 카운트 리셋
        if st.session_state.api_usage.get(key, {}).get('date') != today:
            st.session_state.api_usage[key] = {'date': today, 'count': 0}

        usage_count = st.session_state.api_usage.get(key, {}).get('count', 0)
        remaining = 20000 - usage_count
        formatted_labels.append(f"{name} (남은 호출: {remaining:,})")

    return formatted_labels

recent_apis = get_recent_apis()
if recent_apis:
    st.markdown("**🕐 최근 사용 API** (참고용)")
    for i, api in enumerate(recent_apis, 1):
        st.markdown(f"&nbsp;&nbsp;최근 {i}: `{api[:8]}...{api[-8:]}`")

api_labels = [x[0] for x in api_presets]
api_keys_list = [x[1] for x in api_presets]
# 동적으로 포맷팅된 레이블 생성
formatted_api_labels = update_and_get_api_labels(api_presets)

col_api_left, col_api_right = st.columns([1, 3])
with col_api_left:
    st.markdown("<div class='api-label'>프리셋 API KEY<br>(한 개만 선택)</div>", unsafe_allow_html=True)
    selected_preset_label = st.radio(
        "",
        options=formatted_api_labels, # 포맷팅된 레이블 사용
        index=0,
        key="api_preset_single"
    )
    # 선택된 레이블로부터 원래 API 이름과 키를 찾음
    selected_index = formatted_api_labels.index(selected_preset_label)
    selected_preset_name = api_labels[selected_index]
    api_key_selected = api_keys_list[selected_index]

with col_api_right:
    st.markdown("<div class='api-label'>API Key 직접 입력<br><span style='font-size:13px;color:#888;'>(입력 시 프리셋 무시됨)</span></div>", unsafe_allow_html=True)
    api_key_input = st.text_area(
        "", value="", height=40, placeholder="API 키를 직접 입력하세요 (한 개만)"
    )

api_keys = [k.strip() for k in api_key_input.replace(",", "\n").splitlines() if k.strip()]
if api_keys:
    corp_key = api_keys[0]
    st.info(f"✅ 직접 입력 API 사용: `{corp_key[:8]}...{corp_key[-8:]}`")
else:
    corp_key = api_key_selected
    st.info(f"✅ 프리셋 API 사용: **{selected_preset_name}** (`{corp_key[:8]}...{corp_key[-8:]}`)")

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
    "11013": "1분기보고서", "11012": "반기보고서",
    "11014": "3분기보고서", "11011": "사업보고서(연간)"
}
sel_reports = st.multiselect(
    "보고서 종류", options=list(REPORTS.keys()),
    format_func=lambda c: f"{REPORTS[c]} ({c})",
    default=["11011"]
)
listing = st.multiselect("회사 구분", ["상장사", "비상장사"], default=["상장사"])
cy = datetime.now(KST).year
start_y, end_y = st.slider("사업연도 범위", 2000, cy, (cy - 1, cy))

# ---- 이어받기/복구 UI ----
if GSHEET_CONNECTED:
    jobs_data = jobs_ws.get_all_records()
    unfinished = [r for r in jobs_data if r["status"] in ("stopped", "failed")][-1:]

    if unfinished:
        rj = unfinished[0]
        # (수정사항 1) 시간 포맷 변경
        formatted_time = "시간정보 없음"
        try:
            iso_time_str = rj.get('start_time', '')
            if iso_time_str:
                dt_object = datetime.fromisoformat(iso_time_str.replace("Z", "+00:00"))
                formatted_time = dt_object.strftime('%Y-%m-%d, %H:%M')
        except (ValueError, TypeError):
            formatted_time = rj.get('start_time', '') # 파싱 실패 시 원본 표시

        st.markdown(
            f"<div style='background:#eef6fe;border-radius:9px;padding:12px 16px 8px 16px;margin-bottom:5px;'>"
            f"🔄 <b>미완료(중단) 작업 이어받기:</b> "
            f"<span class='job-badge'>{rj['job_id']}</span> "
            f"({rj.get('user_email','')}, {formatted_time})"
            f"</div>",
            unsafe_allow_html=True
        )
        if st.button("▶️ 이어서 복구/재시작", key="resume_btn"):
            st.session_state.resume_job_id = rj["job_id"]
            st.session_state.resume_data = rj
            st.success(f"작업 {rj['job_id']} 복구 준비 완료!")

# ---- 컨트롤 버튼 ----
col1, col2 = st.columns(2)
run = col1.button("▶️ 모니터링 시작", use_container_width=True)
stop = col2.button("⏹️ 중지", use_container_width=True)

# (수정사항 3) 진행률 바 상시 표시를 위한 세션 상태 및 UI 위치 조정
if 'progress_value' not in st.session_state:
    st.session_state.progress_value = 0.0
if 'progress_text' not in st.session_state:
    st.session_state.progress_text = "대기 중"
if 'job_completed' not in st.session_state:
    st.session_state.job_completed = False

prog_placeholder = st.empty()
status_placeholder = st.empty()
api_status_placeholder = st.empty()

# 항상 마지막 상태의 진행률 바를 표시
prog_placeholder.progress(st.session_state.progress_value, text=st.session_state.progress_text)

if run:
    if not is_valid_email(recipient):
        st.session_state.email_required = True
        st.rerun()
    else:
        st.session_state.running = True
        st.session_state.email_required = False
        st.session_state.start_time = datetime.now(KST)
        st.session_state.monitoring_results = [] # 새 작업 시작 시 결과 초기화
        st.session_state.api_call_count = 0
        st.session_state.job_completed = False
        st.session_state.progress_value = 0.0
        st.session_state.progress_text = "초기화 및 회사 목록 로드 중..."
        add_recent_api(corp_key)
        # UI 업데이트를 위해 rerun
        st.rerun()

if stop:
    st.session_state.running = False
    st.warning("작업이 사용자에 의해 중지되었습니다.")
    st.session_state.progress_text = "사용자 중지"
    st.rerun()

# ---- HTTP 세션+Retry ----
session = requests.Session()
session.mount("https://", HTTPAdapter(
    max_retries=Retry(total=2, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
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
        out = [{"corp_code": e.findtext("corp_code"), "corp_name": e.findtext("corp_name"), "stock_code": (e.findtext("stock_code") or "").strip()} for e in root.findall("list")]
        return out, None
    except Exception as e:
        return None, str(e)

def check_api_limit_error(data):
    if isinstance(data, dict):
        status = data.get("status")
        message = data.get("message", "")
        if status in ["020", "021"] or "한도" in message or "limit" in message.lower():
            return True
    return False

def fetch_execs(key, corp_code, year, rpt):
    try:
        payload = {"crtfc_key": key, "corp_code": corp_code, "bsns_year": str(year), "reprt_code": rpt}
        
        # (수정사항 2) API 키별 사용량 업데이트
        today = datetime.now(KST).strftime('%Y-%m-%d')
        if st.session_state.api_usage.get(key, {}).get('date') != today:
             st.session_state.api_usage[key] = {'date': today, 'count': 0}
        st.session_state.api_usage[key]['count'] += 1
        st.session_state.api_call_count += 1

        response = session.get("https://opendart.fss.or.kr/api/exctvSttus.json", params=payload, timeout=20)
        data = response.json()
        
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
        from_pwd = st.secrets["smtp"]["sender_password"]
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

# ---- 모니터링 수행 (Main) ----
if st.session_state.get("running", False) or st.session_state.get("resume_job_id"):
    is_resume = bool(st.session_state.get("resume_job_id"))
    
    if is_resume:
        job_id = st.session_state.resume_job_id
        st.info(f"🔄 작업 {job_id} 이어받기 시작...")
        if GSHEET_CONNECTED:
            job_row = jobs_ws.find(job_id, in_column=1)
            if job_row: jobs_ws.update_cell(job_row.row, 4, "running")
    else:
        job_id = datetime.now(KST).strftime("%Y%m%d-%H%M%S")
        ts0 = datetime.now(KST).isoformat()
        if GSHEET_CONNECTED:
            jobs_ws.append_row([job_id, recipient, ts0, "running"])

    st.session_state.current_job_id = job_id
    
    corps, corp_err = load_corp_list(corp_key)
    if not corps:
        st.session_state.running = False
        st.error(f"회사 목록 로드 실패: {corp_err}")
        if GSHEET_CONNECTED and not is_resume:
            jobs_ws.append_row([job_id, recipient, datetime.now(KST).isoformat(), "failed"])
        st.stop()

    kws = [w.strip() for w in keywords.split(",") if w.strip()]
    all_c = [c for c in corps if (c["stock_code"] and "상장사" in listing) or (not c["stock_code"] and "비상장사" in listing)]
    targets = [(c, y, r) for c in all_c for y in range(start_y, end_y + 1) for r in sel_reports]
    N = len(targets)
    
    start_time = datetime.now()
    api_limit_hit = False

    for i, (corp, y, rpt) in enumerate(targets, 1):
        if not st.session_state.get("running", False) and not is_resume:
            status_placeholder.warning("작업이 중지되었습니다.")
            if GSHEET_CONNECTED:
                 job_row = jobs_ws.find(job_id, in_column=1)
                 if job_row: jobs_ws.update_cell(job_row.row, 4, "stopped")
            break
        
        rows, err = fetch_execs(corp_key, corp["corp_code"], y, rpt)
        
        if err == "API_LIMIT_EXCEEDED":
            api_limit_hit = True
            st.session_state.running = False
            st.error("🚫 API 일일 한도(20,000회) 초과! 다른 API 키를 선택하여 이어받기를 진행하세요.")
            if GSHEET_CONNECTED:
                job_row = jobs_ws.find(job_id, in_column=1)
                if job_row: jobs_ws.update_cell(job_row.row, 4, "stopped")
            break
        
        if err: continue

        elapsed = (datetime.now() - start_time).total_seconds()
        speed = i / elapsed if elapsed > 0 else 1
        eta = int((N - i) / speed) if speed > 0 else 0
        
        st.session_state.progress_value = i / N
        st.session_state.progress_text = f"{i:,}/{N:,} ({i/N*100:.0f}%) · 예상 남은 시간 {eta//60}분 {eta%60}초"
        prog_placeholder.progress(st.session_state.progress_value, text=st.session_state.progress_text)
        status_placeholder.markdown(f"<span style='color:#222;font-size:17px;font-weight:600;'>{corp['corp_name']} · {y}년 · {REPORTS[rpt]}</span>", unsafe_allow_html=True)
        
        # API 호출 횟수 표시
        api_status_placeholder.markdown(f"📊 API 호출 횟수: {st.session_state.api_call_count:,} / 20,000", unsafe_allow_html=True)
        
        for r in rows:
            mc = r.get("main_career", "")
            if any(k in mc for k in kws):
                new_result = {
                    "회사명": corp["corp_name"], "종목코드": corp["stock_code"] or "비상장", "사업연도": y,
                    "보고서종류": REPORTS[rpt], "임원이름": r.get("nm", ""), "직위": r.get("ofcps", ""),
                    "주요경력": mc, "매칭키워드": ",".join([k for k in kws if k in mc])
                }
                st.session_state.monitoring_results.append(new_result)
        time.sleep(0.1)
    
    # 작업 완료/중단 후 최종 처리
    st.session_state.running = False
    st.session_state.job_completed = True # 작업이 끝났음을 표시
    
    if not api_limit_hit and not stop:
        st.session_state.progress_value = 1.0
        st.session_state.progress_text = f"✅ 전체 조회 완료! (총 {N:,}건)"
        if GSHEET_CONNECTED:
            job_row = jobs_ws.find(job_id, in_column=1)
            if job_row: jobs_ws.update_cell(job_row.row, 4, "completed")
    
    # 세션 정리
    if 'resume_job_id' in st.session_state: del st.session_state.resume_job_id
    if 'resume_data' in st.session_state: del st.session_state.resume_data
    
    # 결과 표시를 위해 rerun
    st.rerun()

# (수정사항 4) 결과 표시 및 다운로드/삭제 로직을 모니터링 로직과 분리
if 'monitoring_results' in st.session_state:
    st.markdown("---")
    st.markdown("### 📊 모니터링 결과")

    results_list = st.session_state.monitoring_results
    df = pd.DataFrame(results_list)

    if df.empty:
        st.info(f"💾 조회된 결과 없음 (작업ID: {st.session_state.get('current_job_id', 'N/A')})")
    else:
        st.success(f"💾 조회된 결과: {len(df):,}건 (작업ID: {st.session_state.get('current_job_id', 'N/A')})")
        st.dataframe(df, use_container_width=True)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name="DART_Results")
        excel_data = buf.getvalue()

        st.download_button(
            "📥 결과 다운로드 (XLSX)",
            data=excel_data,
            file_name=f"dart_results_{st.session_state.get('current_job_id', 'saved')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_final_results"
        )
    
    if st.button("🗑️ 화면 결과 삭제 및 초기화", key="clear_results"):
        st.session_state.monitoring_results = []
        st.session_state.progress_value = 0.0
        st.session_state.progress_text = "대기 중"
        st.session_state.job_completed = False
        if 'current_job_id' in st.session_state:
            del st.session_state.current_job_id
        st.success("화면의 결과가 삭제되었습니다. 새 작업을 시작할 수 있습니다.")
        st.rerun()

# 작업 완료 시점에 이메일 발송 (한 번만 실행되도록 job_completed 플래그 사용)
if st.session_state.get("job_completed"):
    email_subject = f"[DART] 모니터링 결과 ({st.session_state.get('current_job_id', '')})"
    status_text = "완료" if st.session_state.progress_value == 1.0 else "중단"
    results_list = st.session_state.monitoring_results
    
    email_body = f"""
작업ID: {st.session_state.get('current_job_id', 'N/A')}
작업 상태: {status_text}
검색 키워드: {keywords}
검색 범위: {start_y}-{end_y}년
보고서 종류: {', '.join(REPORTS.get(r, r) for r in sel_reports)}
총 API 호출 건수: {st.session_state.get('api_call_count', 0):,}회
매칭 결과: {len(results_list):,}건

{ '첨부된 Excel 파일을 확인하세요.' if results_list else '검색 조건에 맞는 결과가 없습니다.'}
"""
    excel_data_for_email = None
    if results_list:
        buf_email = io.BytesIO()
        with pd.ExcelWriter(buf_email, engine="openpyxl") as w:
            pd.DataFrame(results_list).to_excel(w, index=False, sheet_name="DART_Results")
        excel_data_for_email = buf_email.getvalue()

    with st.spinner("결과 이메일 발송 중..."):
        success, msg = send_email(
            to_email=recipient,
            subject=email_subject,
            body=email_body,
            attachment_bytes=excel_data_for_email,
            filename=f"dart_results_{st.session_state.get('current_job_id')}.xlsx" if excel_data_for_email else None
        )

    if success:
        st.markdown(f"<div class='success-box'>✅ <b>결과가 {recipient}에게 성공적으로 발송되었습니다!</b></div>", unsafe_allow_html=True)
    else:
        st.error(f"❌ 자동 메일 발송 실패: {msg}")

    # 이메일 발송 후 플래그를 리셋하여 재발송 방지
    st.session_state.job_completed = False
