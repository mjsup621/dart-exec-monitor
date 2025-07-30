import streamlit as st
import requests, zipfile, io, xml.etree.ElementTree as ET, pandas as pd, os, pickle
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import gspread
from google.oauth2.service_account import Credentials
import smtplib
from email.message import EmailMessage

# ─── Google Sheets 세팅 ───────────────────────────
SPREADSHEET_ID = "1hT_PaNZvsBqVfxQXCNgIXSVcpsDGJf474yWQfYmeJ7o"
creds = Credentials.from_service_account_file(
    "service_account.json",
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)
jobs_ws = sh.worksheet("DART_Jobs")
prog_ws = sh.worksheet("DART_Progress")
# ────────────────────────────────────────────────

st.set_page_config(page_title="DART Exec Monitor", layout="wide")
st.title("📊 DART 임원 ‘주요경력’ 모니터링 서비스")

# ─── API Key 프리셋 및 입력/관리 ────────────────
api_presets = {
    "1번 (pompougnac)": "eeb883965e882026589154074cddfc695330693c",
    "2번 (nimirahal)":   "1290bb1ec7879cba0e9f9b350ac97bb5d38ec176",
    "3번 (good_mater)":  "5e75506d60b4ab3f325168019bcacf364cf4937e",
    "4번 (v-__-v)":      "6c64f7efdea057881deb91bbf3aaa5cb8b03d394",
    "5번 (2realfire)":   "d9f0d92fbdc3a2205e49c66c1e24a442fa8c6fe8"
}
api_labels = list(api_presets.keys()) + ["직접 입력"]

st.subheader("🔑 DART API Key 관리")
selected_keys = st.multiselect(
    "사용할 API Key를 순서대로 선택 (한도 소진 시 자동 전환)",
    api_labels,
    default=[api_labels[0]]
)
api_keys = []
for label in selected_keys:
    if label == "직접 입력":
        input_val = st.text_input("직접 입력할 API Key", key="direct_input").strip()
        if input_val:
            api_keys.append(input_val)
    else:
        api_keys.append(api_presets[label])
if api_keys:
    current_key = api_keys[0]
    st.success(f"현재 사용중: `{current_key[:6]}...{current_key[-6:]}`")
    if len(api_keys) > 1:
        st.info(f"남은 키 개수: {len(api_keys)-1}개")
else:
    st.error("최소 1개 이상의 API Key를 선택하거나 직접 입력해 주세요.")
    st.stop()
auto_rotate = st.checkbox("API Key 자동 전환 (한도 도달시 순차 소진)", value=True)
st.divider()

# ─── 나머지 사용자 입력 ───────────────────────
recipient  = st.text_input("📧 결과 수신 이메일").strip()
keywords   = st.text_input("🔍 키워드 (쉼표로 구분)", "이촌,삼정,안진")
REPORTS    = {
    "11013":"1분기보고서","11012":"반기보고서",
    "11014":"3분기보고서","11011":"사업보고서(연간)"
}
sel_reports = st.multiselect(
    "📑 보고서 종류",
    options=list(REPORTS.keys()),
    format_func=lambda c: f"{REPORTS[c]} ({c})",
    default=["11011"]
)
listing    = st.multiselect("🏷️ 회사 구분", ["상장사","비상장사"], default=["상장사"])
cy         = datetime.now().year
start_y, end_y = st.slider("📅 사업연도 범위", 2000, cy, (cy-1, cy))
col1, col2  = st.columns(2)
run, stop  = col1.button("▶️ 모니터링 시작"), col2.button("⏹️ 중지")

# ─── HTTP 세션 + Retry ──────────────────────────
session = requests.Session()
session.mount("https://", HTTPAdapter(
    max_retries=Retry(total=3, backoff_factor=1, status_forcelist=[500,502,503,504])
))

# ─── corpCode.xml 캐싱/로컬 저장 ────────────────
def load_corp_list_cached(key):
    cache_file = "corp_list.pickle"
    # 1) 캐시파일 있으면 우선 사용(1일 이내)
    if os.path.exists(cache_file):
        mtime = os.path.getmtime(cache_file)
        if (datetime.now() - datetime.fromtimestamp(mtime)).total_seconds() < 24*3600:
            try:
                with open(cache_file, "rb") as f:
                    return pickle.load(f)
            except Exception:
                pass
    # 2) 없으면 새로 다운로드
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    try:
        resp = session.get(url, params={"crtfc_key": key}, timeout=30)
        resp.raise_for_status()
        content = resp.content
        if not content.startswith(b"PK"):
            err = ET.fromstring(content).findtext("message", default="알 수 없는 오류")
            st.error(f"❌ 회사목록 오류: {err}")
            return []
        zf = zipfile.ZipFile(io.BytesIO(content))
        xml = zf.open(zf.namelist()[0]).read()
        root = ET.fromstring(xml)
        corp_list = [
            {
                "corp_code":  e.findtext("corp_code"),
                "corp_name":  e.findtext("corp_name"),
                "stock_code": (e.findtext("stock_code") or "").strip()
            }
            for e in root.findall("list")
        ]
        with open(cache_file, "wb") as f:
            pickle.dump(corp_list, f)
        return corp_list
    except Exception as e:
        st.error(f"회사 목록 로드 실패: {e}")
        return []
# ────────────────────────────────────────────────

# ─── 임원현황 API 호출 (Key 순차/자동) ──────────
call_cnt, key_idx = 0, 0
def get_next_key():
    global key_idx, call_cnt
    if auto_rotate and call_cnt and call_cnt % 20000 == 0:
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
        data = session.get(
            "https://opendart.fss.or.kr/api/exctvSttus.json",
            params=payload, timeout=20
        ).json()
        if data.get("status") != "000":
            return []
        return data.get("list", [])
    except Exception as e:
        return []
# ────────────────────────────────────────────────

# ─── 모니터링 제어/상태 ────────────────────────
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

# ─── 모니터링 수행 ──────────────────────────────
if st.session_state.running:
    job_id, ts0 = datetime.now().strftime("%Y%m%d-%H%M%S"), datetime.now().isoformat()
    jobs_ws.append_row([job_id, recipient, ts0, "running"])
    # 회사목록 로드
    with st.spinner("회사 목록 로드 중…"):
        corps = load_corp_list_cached(api_keys[0])
        if not corps:
            st.session_state.running = False
            st.error("모니터링 에러: 회사 목록 로드 실패")
            st.stop()
    # 대상 필터링
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
    st.success(f"✅ 총 호출 대상: {N:,}건")
    prog = st.progress(0)

    results = []
    call_cnt = 0
    key_idx = 0
    for i, (corp, y, rpt) in enumerate(targets,1):
        if not st.session_state.running:
            break
        key = get_next_key()
        rows = fetch_execs(key, corp["corp_code"], y, rpt)
        call_cnt += 1
        for r in rows or []:
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
    ts1 = datetime.now().isoformat()
    # 프로그레스 시트에 요약만 한줄
    prog_ws.append_row([
        job_id, N, f"{start_y}-{end_y}",
        ",".join(REPORTS[r] for r in sel_reports),
        ts1, len(results)
    ])
    # Jobs 시트 상태 업데이트
    status = "completed" if call_cnt==N else "stopped"
    r = jobs_ws.find(job_id, in_column=1)
    jobs_ws.update_cell(r.row,4,status)
    # 결과 출력/저장
    df = pd.DataFrame(results)
    st.divider()
    st.subheader("🔍 매칭된 결과물")
    if df.empty:
        st.info("매칭된 결과물이 없습니다.")
    else:
        st.success(f"총 {len(df):,}건 매칭 완료")
        st.dataframe(df)
    # 엑셀 다운로드
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w,index=False,sheet_name="Sheet1")
    st.download_button(
        "📥 XLSX 다운로드",
        data=buf.getvalue(),
        file_name=f"dart_results_{job_id}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    # 결과가 있을 때만 이메일 발송
    if not df.empty:
        from_email = st.secrets["smtp"]["sender_email"]
        from_pwd   = st.secrets["smtp"]["sender_password"]
        # 간단 이메일 발송 함수
        def send_email(to_email, subject, body, attachment_bytes, filename):
            msg = EmailMessage()
            msg["Subject"] = subject
            msg["From"] = from_email
            msg["To"] = to_email
            msg.set_content(body)
            msg.add_attachment(attachment_bytes, maintype="application",
                              subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                              filename=filename)
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                smtp.login(from_email, from_pwd)
                smtp.send_message(msg)
        send_email(
            to_email=recipient,
            subject=f"[DART Monitor {job_id}] 결과",
            body=(f"작업ID: {job_id}\n시작: {ts0}\n종료: {ts1}\n총 호출: {call_cnt:,}회\n매칭: {len(results):,}건"),
            attachment_bytes=buf.getvalue(),
            filename=f"dart_results_{job_id}.xlsx"
        )
        st.info(f"결과를 {recipient} 로 발송했습니다.")
