import os
import re
import json
import time
import csv
import socket
import shutil
import random
import logging
import tempfile
import threading
import struct
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from urllib3.connection import HTTPConnection

import requests
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =============================================================================
# 0) 사용자 설정
# =============================================================================
MAX_THREADS = 1  

ENABLE_WINDOW_SIZE = True
WINDOW_WIDTH = 600
WINDOW_HEIGHT = 400
ENABLE_WINDOW_JITTER = False
WINDOW_JITTER_RANGE = 80
ENABLE_WINDOW_POSITION = True
WINDOW_POS_X = 50
WINDOW_POS_Y = 400
ENABLE_BLOCK_CHECK = False 
CHECK_INTERVAL_SECONDS = 60*30
MAX_PAGES = 10

TASKS = [
    {"keyword": "올빼미티비", "domain": "https://www.tvda.co.kr/?srt=1"},
]

MAX_PROXIES_PER_TASK = 30
REFRESH_PROXIES_EACH_CYCLE = True
RUN_HEADLESS = False
PAGELOAD_TIMEOUT_SEC = 60*2
ELEM_WAIT_SEC = 30

# 🔒 스텔스 모드 설정 (신규 추가)
ENABLE_STEALTH = True  # 스텔스 기능 활성화 여부
RANDOM_DELAY_MIN = 2.0  # 액션 간 최소 대기 시간(초)
RANDOM_DELAY_MAX = 5.0  # 액션 간 최대 대기 시간(초)
ENABLE_MOUSE_MOVEMENT = True  # 마우스 움직임 시뮬레이션
SCROLL_BEHAVIOR = True  # 스크롤 시뮬레이션

OUT_DIR = os.path.abspath("./naver_monitor_out")
LOG_FILE = os.path.join(OUT_DIR, "monitor.log")
RESULT_JSONL = os.path.join(OUT_DIR, "results.jsonl")
RESULT_CSV = os.path.join(OUT_DIR, "results.csv")
WINDOW_STATE_FILE = os.path.join(OUT_DIR, "window_states.json")  # 창 상태 저장 파일
STOP_EVENT = threading.Event()
FILE_LOCK = threading.Lock()
WINDOW_STATE_LOCK = threading.Lock()  # 창 상태 파일 접근용 락 

# =============================================================================
# 1) 프록시 설정 (기본 유지)
# =============================================================================
ALL_SOURCES = [
    ("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt", "http", False),
    ("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt", "socks5", False),
    ("https://raw.githubusercontent.com/victorgeel/proxy-list-update/main/proxies/http.txt", "http", False),
    ("https://raw.githubusercontent.com/victorgeel/proxy-list-update/main/proxies/socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/victorgeel/proxy-list-update/main/proxies/socks5.txt", "socks5", False),
    ("https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt", "http", False),
    ("https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt", "socks5", False),
]

SOURCES_KR = [
    # 1. ProxyScrape (국가 필터 지원 - 가장 가용성 높음)
    #("https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=KR&ssl=all&anonymity=all", "http", False),
    #("https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks4&timeout=10000&country=KR", "socks4", False),
    #("https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks5&timeout=10000&country=KR", "socks5", False),

    # 2. Geonode (API 방식 - 한국 IP 데이터 정제가 잘 됨)
    ("https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&country=KR&anonymityLevel=elite", "http", True),

    # 3. Spys.me (신뢰도 높은 텍스트 기반 리스트)
    #("https://spys.me/proxy.txt", "http", False), 

    # 4. Proxifly (국가별 목록 지원)
    #("https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/countries/KR/data.txt", "http", False),
]

#ALL_SOURCES = SOURCES_KR
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

linger_option = (socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
HTTPConnection.default_socket_options = HTTPConnection.default_socket_options + [linger_option]

# =============================================================================
# 2) 데이터 모델 및 유틸 (로그 설정 강화)
# =============================================================================
@dataclass
class ProxyInfo:
    protocol: str
    address: str
    source: str

@dataclass
class RunResult:
    ts: str
    keyword: str
    target_url: str
    proxy_protocol: Optional[str]
    proxy_address: Optional[str]
    proxy_source: Optional[str]
    found: bool
    found_page: Optional[int]
    found_rank_on_page: Optional[int]
    found_href: Optional[str]
    clicked_ok: bool
    final_url: Optional[str]
    error: Optional[str]
    note: Optional[str]

def setup_logging() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # 로그 포맷에 쓰레드 이름을 명시하여 어떤 키워드 작업인지 구분 가능하게 함
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s", datefmt="%H:%M:%S")
    
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    
    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(sh)

# =============================================================================
# 2-1) 🔒 스텔스 유틸리티 함수 (신규 추가)
# =============================================================================
def load_window_state(slot_id: str) -> Optional[Dict]:
    """
    슬롯별 창 위치/크기 상태 로드
    """
    try:
        with WINDOW_STATE_LOCK:
            if os.path.exists(WINDOW_STATE_FILE):
                with open(WINDOW_STATE_FILE, 'r', encoding='utf-8') as f:
                    states = json.load(f)
                    return states.get(slot_id)
    except Exception as e:
        logging.warning(f"⚠️ 창 상태 로드 실패 (슬롯 {slot_id}): {e}")
    return None

def save_window_state(slot_id: str, x: int, y: int, width: int, height: int) -> None:
    """
    슬롯별 창 위치/크기 상태 저장
    """
    try:
        with WINDOW_STATE_LOCK:
            states = {}
            if os.path.exists(WINDOW_STATE_FILE):
                with open(WINDOW_STATE_FILE, 'r', encoding='utf-8') as f:
                    states = json.load(f)
            
            states[slot_id] = {
                'x': x,
                'y': y,
                'width': width,
                'height': height
            }
            
            with open(WINDOW_STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(states, f, indent=2)
            
            logging.info(f"💾 창 상태 저장 완료 (슬롯 {slot_id}): 위치({x},{y}) 크기({width}x{height})")
    except Exception as e:
        logging.warning(f"⚠️ 창 상태 저장 실패 (슬롯 {slot_id}): {e}")

def random_delay(min_sec: float = None, max_sec: float = None) -> None:
    """
    인간처럼 보이기 위한 랜덤 딜레이
    """
    if not ENABLE_STEALTH:
        return
    min_val = min_sec if min_sec is not None else RANDOM_DELAY_MIN
    max_val = max_sec if max_sec is not None else RANDOM_DELAY_MAX
    delay = random.uniform(min_val, max_val)
    time.sleep(delay)

def simulate_human_typing(element, text: str) -> None:
    """
    사람처럼 한 글자씩 타이핑하는 효과
    """
    if not ENABLE_STEALTH:
        element.send_keys(text)
        return
    
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.05, 0.15))  # 글자당 50~150ms 딜레이

def simulate_scroll(driver, scroll_count: int = 3) -> None:
    """
    페이지를 천천히 스크롤하여 인간 행동 시뮬레이션
    """
    if not ENABLE_STEALTH or not SCROLL_BEHAVIOR:
        return
    
    for _ in range(scroll_count):
        scroll_amount = random.randint(200, 500)
        driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
        time.sleep(random.uniform(0.3, 0.8))

def get_random_user_agent() -> str:
    """
    랜덤 User-Agent 생성 (다양한 브라우저 버전)
    """
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    ]
    return random.choice(user_agents)

def inject_stealth_scripts(driver) -> None:
    """
    🔒 고급 스텔스 스크립트 주입
    - webdriver 속성 숨김
    - navigator 속성 조작
    - 자동화 감지 우회
    """
    if not ENABLE_STEALTH:
        return
    
    # 1. webdriver 속성 제거
    stealth_js = """
    // webdriver 속성 숨기기
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined
    });
    
    // Chrome 관련 속성 추가
    window.chrome = {
        runtime: {}
    };
    
    // Permissions API 조작
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
            Promise.resolve({ state: Notification.permission }) :
            originalQuery(parameters)
    );
    
    // Plugin 배열 수정
    Object.defineProperty(navigator, 'plugins', {
        get: () => [1, 2, 3, 4, 5]
    });
    
    // Languages 설정
    Object.defineProperty(navigator, 'languages', {
        get: () => ['ko-KR', 'ko', 'en-US', 'en']
    });
    
    // WebGL Vendor 정보 수정
    const getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {
        if (parameter === 37445) {
            return 'Intel Inc.';
        }
        if (parameter === 37446) {
            return 'Intel Iris OpenGL Engine';
        }
        return getParameter.apply(this, [parameter]);
    };
    
    // 자동화 감지 메서드 덮어쓰기
    window.navigator.chrome = {
        runtime: {},
    };
    
    // console.debug 숨김 (Selenium 흔적 제거)
    const originalDebug = console.debug;
    console.debug = function() {};
    """
    
    try:
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': stealth_js
        })
        logging.info("🔒 스텔스 스크립트 주입 완료")
    except Exception as e:
        logging.warning(f"⚠️ 스텔스 스크립트 주입 실패 (undetected-chromedriver가 일부 처리): {e}")

# =============================================================================
# 3) 프록시 수집 및 검증 (기존 유지)
# =============================================================================
def fetch_all_proxies() -> List[ProxyInfo]:
    logging.info("📄 [수집] 프록시 수집 시작 (텍스트/JSON 프로토콜 매칭 최적화)")
    raw_list = []
    
    for url, default_proto, _ in ALL_SOURCES:
        if STOP_EVENT.is_set(): break
        try:
            resp = requests.get(url, timeout=20, headers=HEADERS)
            if resp.status_code != 200: continue
            
            content = resp.text.strip()
            count = 0
            
            # 1️⃣ JSON 형식 (Geonode 등)
            if content.startswith('{') or content.startswith('['):
                try:
                    data = resp.json()
                    items = data.get('data', []) if isinstance(data, dict) else data
                    for item in items:
                        if isinstance(item, dict) and 'ip' in item and 'port' in item:
                            addr = f"{item['ip']}:{item['port']}"
                            # JSON 내 protocols 우선 확인, 없으면 소스 정의 기본값(default_proto) 사용
                            actual_proto = default_proto
                            if 'protocols' in item and item['protocols']:
                                actual_proto = item['protocols'][0].lower()
                            
                            raw_list.append(ProxyInfo(protocol=actual_proto, address=addr, source=urlparse(url).netloc))
                            count += 1
                except: pass
            
            # 2️⃣ 일반 텍스트 형식 (프로토콜 정보가 없는 경우 대응)
            else:
                for line in content.splitlines():
                    line = line.strip()
                    if not line or line.startswith('#'): continue
                    
                    # 주소만 있는 경우(1.2.3.4:80)를 대비해 무조건 default_proto 적용
                    addr = line.split("://")[-1] if "://" in line else line
                    if ":" in addr:
                        # 텍스트 목록은 소스 리스트 옆에 적어둔 프로토콜(http, socks5 등)을 강제 부여
                        raw_list.append(ProxyInfo(protocol=default_proto, address=addr, source=urlparse(url).netloc))
                        count += 1
            
            if count > 0:
                logging.info(f"📥 [수집] {urlparse(url).netloc:20s} | {count:4d}개 ({default_proto})")
                
        except Exception as e:
            logging.error(f"⚠️ [실패] {urlparse(url).netloc}: {e}")
            
    # 중복 제거
    uniq = {(p.protocol, p.address): p for p in raw_list}
    proxies = list(uniq.values())
    logging.info(f"📊 [최종] 총 {len(proxies)}개의 고유 프록시 로드 완료")
    return proxies

def tcp_quick_check(addr: str, timeout: float = 2.0) -> bool:
    try:
        host, port_s = addr.split(":", 1)
        port = int(port_s)
        with socket.create_connection((host, port), timeout=timeout): return True
    except Exception: return False

# =============================================================================
# 4) 브라우저 드라이버 생성 (🔒 스텔스 강화)
# =============================================================================
def make_driver(proxy: Optional[ProxyInfo], slot_id: str = "0") -> Tuple[uc.Chrome, str]:
    profile_dir = tempfile.mkdtemp(prefix="naver_mon_profile_")
    options = uc.ChromeOptions()
    
    # 기본 옵션
    options.add_argument(f"--user-data-dir={profile_dir}")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--lang=ko-KR")
    
    # 🔒 스텔스 옵션 추가
    if ENABLE_STEALTH:
        options.add_argument("--disable-blink-features=AutomationControlled")  # 자동화 감지 비활성화
        options.add_argument("--disable-web-security")  # CORS 우회 (선택)
        options.add_argument("--disable-features=IsolateOrigins,site-per-process")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument("--profile-directory=Default")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--disable-gpu")  # GPU 가속 비활성화
        
        # 랜덤 User-Agent 설정
        user_agent = get_random_user_agent()
        options.add_argument(f"--user-agent={user_agent}")
        logging.info(f"🎭 랜덤 User-Agent 적용: {user_agent[:50]}...")
    
    if RUN_HEADLESS: 
        options.add_argument("--headless=new")
    
    # 프록시 설정
    if proxy:
        proxy_str = f"{proxy.protocol}://{proxy.address}"
        logging.info(f"🌐 [드라이버 생성] 프록시 적용: {proxy_str} (출처: {proxy.source})")
        options.add_argument(f"--proxy-server={proxy_str}")
    else:
        logging.info("🌐 [드라이버 생성] 프록시 미사용 (Direct 연결)")

    # 드라이버 생성
    driver = uc.Chrome(options=options, use_subprocess=True)
    driver.set_page_load_timeout(PAGELOAD_TIMEOUT_SEC)
    
    # 🔒 스텔스 스크립트 주입
    if ENABLE_STEALTH:
        inject_stealth_scripts(driver)
    
    # 📍 저장된 창 상태 로드 및 적용
    saved_state = load_window_state(slot_id)
    
    try:
        if saved_state:
            # 저장된 위치와 크기가 있으면 복원
            logging.info(f"📍 슬롯 {slot_id}: 저장된 창 상태 복원 중... 위치({saved_state['x']},{saved_state['y']}) 크기({saved_state['width']}x{saved_state['height']})")
            driver.set_window_size(saved_state['width'], saved_state['height'])
            driver.set_window_position(saved_state['x'], saved_state['y'])
        else:
            # 저장된 상태가 없으면 기본 설정 적용
            if ENABLE_WINDOW_SIZE:
                w, h = WINDOW_WIDTH, WINDOW_HEIGHT
                if ENABLE_WINDOW_JITTER:
                    w += random.randint(-WINDOW_JITTER_RANGE, WINDOW_JITTER_RANGE)
                    h += random.randint(-WINDOW_JITTER_RANGE, WINDOW_JITTER_RANGE)
                driver.set_window_size(max(300, w), max(300, h))
            if ENABLE_WINDOW_POSITION:
                driver.set_window_position(WINDOW_POS_X, WINDOW_POS_Y)
            logging.info(f"📍 슬롯 {slot_id}: 기본 창 설정 적용")
    except Exception as e:
        logging.warning(f"⚠️ 창 설정 적용 실패: {e}")
    
    return driver, profile_dir

def update_query_param(url: str, **kwargs) -> str:
    u = urlparse(url)
    q = parse_qs(u.query)
    for k, v in kwargs.items(): q[str(k)] = [str(v)]
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))

# =============================================================================
# 5) 작업 로직 (🔒 스텔스 행동 패턴 추가)
# =============================================================================
def thread_worker(task: Dict, proxy: ProxyInfo, slot_id: str = "0"):
    keyword, target_url = task["keyword"], task["domain"]
    logging.info(f"▶️ 작업 시작 | 슬롯: {slot_id} | 키워드: [{keyword}] | 프록시: {proxy.address}")
    
    driver, profile_dir = None, ""
    rr = RunResult(datetime.now().isoformat(timespec="seconds"), keyword, target_url, proxy.protocol, proxy.address, proxy.source, False, None, None, None, False, None, None, None)
    
    try:
        # 1. TCP 체크
        if not tcp_quick_check(proxy.address):
            logging.warning(f"❌ TCP 연결 실패: {proxy.address}")
            rr.error = "TCP_CONNECT_FAIL"
        else:
            logging.info(f"🌐 브라우저 실행 중... (슬롯 {slot_id}, 프록시 {proxy.address})")
            driver, profile_dir = make_driver(proxy, slot_id)
            
            # 🔒 초기 딜레이 (봇 감지 회피)
            random_delay(1.0, 2.0)
            
            logging.info(f"🔍 네이버 접속 및 키워드 검색: [{keyword}]")
            driver.get("https://www.naver.com/")
            WebDriverWait(driver, ELEM_WAIT_SEC).until(lambda d: d.execute_script("return document.readyState") in ("interactive", "complete"))
            
            # 🔒 페이지 로딩 후 자연스러운 대기
            random_delay(1.5, 3.0)
            
            # 🔒 스크롤 시뮬레이션
            simulate_scroll(driver, scroll_count=2)
            
            box = WebDriverWait(driver, ELEM_WAIT_SEC).until(EC.presence_of_element_located((By.NAME, "query")))
            box.clear()
            
            # 🔒 인간처럼 타이핑
            simulate_human_typing(box, keyword)
            
            # 🔒 엔터 전 짧은 대기
            random_delay(0.5, 1.0)
            box.send_keys(Keys.ENTER)
            
            WebDriverWait(driver, ELEM_WAIT_SEC).until(lambda d: "search.naver.com" in (d.current_url or ""))
            results_url = driver.current_url
            
            # 🔒 검색 결과 로딩 대기
            random_delay(2.0, 4.0)
            
            # 2. 페이지 순회
            for page in range(1, MAX_PAGES + 1):
                if STOP_EVENT.is_set(): break
                
                logging.info(f"📄 페이지 탐색 중... ({page}/{MAX_PAGES} page)")
                driver.get(update_query_param(results_url, start=1 + (page - 1) * 10))
                
                # 🔒 페이지 로딩 대기
                random_delay(2.0, 3.5)
                
                # 🔒 스크롤 시뮬레이션
                simulate_scroll(driver, scroll_count=3)
                
                # 타겟 탐색
                found_data = None
                anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")
                for idx, a in enumerate(anchors, 1):
                    try:
                        href = a.get_attribute("href") or ""
                        # URL 정규화 및 비교
                        if href and target_url:
                            h_can = urlunparse((urlparse(href).scheme, urlparse(href).netloc, urlparse(href).path or "/", "", "", ""))
                            t_can = urlunparse((urlparse(target_url).scheme, urlparse(target_url).netloc, urlparse(target_url).path or "/", "", "", ""))
                            if h_can.lower() == t_can.lower():
                                found_data = (idx, href)
                                break
                    except: continue
                
                if found_data:
                    rank, href = found_data
                    logging.info(f"✨ 타겟 발견! | {page}페이지 {rank}위 | URL: {href[:50]}...")
                    rr.found, rr.found_page, rr.found_rank_on_page, rr.found_href = True, page, rank, href
                    
                    # 🔒 클릭 전 자연스러운 대기
                    random_delay(1.0, 2.5)
                    
                    logging.info("🖱️ 타겟 링크 클릭 및 검증 중...")
                    driver.get(href)
                    WebDriverWait(driver, 10).until(lambda d: d.execute_script("return document.readyState") in ("interactive", "complete"))
                    
                    # 🔒 페이지 도착 후 대기
                    random_delay(2.0, 3.0)
                    
                    final_url = driver.current_url
                    # 최종 URL 검증
                    h_final = urlunparse((urlparse(final_url).scheme, urlparse(final_url).netloc, urlparse(final_url).path or "/", "", "", ""))
                    t_can = urlunparse((urlparse(target_url).scheme, urlparse(target_url).netloc, urlparse(target_url).path or "/", "", "", ""))
                    
                    if h_final.lower() == t_can.lower():
                        logging.info(f"✅ 클릭 성공: 최종 목적지 확인됨 ({h_final})")
                        rr.clicked_ok, rr.final_url = True, final_url
                    else:
                        logging.error(f"⚠️ 목적지 불일치 | 현재: {h_final}")
                        rr.clicked_ok, rr.final_url, rr.note = False, final_url, "FINAL_URL_NOT_MATCH"
                    break
                
                # 🔒 다음 페이지로 넘어가기 전 대기
                if page < MAX_PAGES:
                    random_delay(1.5, 3.0)
            
            if not rr.found and not rr.error:
                logging.info(f"🔎 탐색 종료: {MAX_PAGES}페이지 내에 타겟이 없습니다.")
                rr.error = "NOT_FOUND_IN_PAGES"

    except Exception as e:
        logging.error(f"💥 예외 발생: {str(e)[:100]}")
        rr.error = str(e)[:160]
    finally:
        # 📍 창 상태 저장 (드라이버 종료 전)
        if driver:
            try:
                pos = driver.get_window_position()
                size = driver.get_window_size()
                save_window_state(slot_id, pos['x'], pos['y'], size['width'], size['height'])
            except Exception as e:
                logging.warning(f"⚠️ 창 상태 추출 실패 (슬롯 {slot_id}): {e}")
            
            try: driver.quit()
            except: pass
        
        if profile_dir: 
            try: shutil.rmtree(profile_dir, ignore_errors=True)
            except: pass
        
        # 락을 사용한 결과 저장
        with FILE_LOCK:
            with open(RESULT_JSONL, "a", encoding="utf-8") as f:
                f.write(json.dumps(asdict(rr), ensure_ascii=False) + "\n")
            is_new = not os.path.exists(RESULT_CSV)
            with open(RESULT_CSV, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if is_new: w.writerow(["ts", "keyword", "target_url", "proxy_protocol", "proxy_address", "proxy_source", "found", "found_page", "found_rank_on_page", "found_href", "clicked_ok", "final_url", "error", "note"])
                w.writerow([rr.ts, rr.keyword, rr.target_url, rr.proxy_protocol, rr.proxy_address, rr.proxy_source, rr.found, rr.found_page, rr.found_rank_on_page, rr.found_href, rr.clicked_ok, rr.final_url, rr.error, rr.note])
        
        status = "성공" if rr.found else f"실패({rr.error})"
        logging.info(f"🏁 작업 종료 | 슬롯: {slot_id} | 키워드: [{keyword}] | 결과: {status}")

# =============================================================================
# 6) 메인 루프
# =============================================================================
def main_loop() -> None:
    setup_logging()
    logging.info("==================================================")
    logging.info("🚀 Naver Exposure Monitor 시작")
    logging.info(f"⚙️ 설정: 쓰레드 슬롯 {MAX_THREADS}개 / 탐색 {MAX_PAGES}페이지")
    if ENABLE_STEALTH:
        logging.info(f"🔒 스텔스 모드: 활성화 (딜레이: {RANDOM_DELAY_MIN}~{RANDOM_DELAY_MAX}초)")
    logging.info("==================================================")
    
    proxies_cache = []
    active_threads: List[threading.Thread] = []
    
    try:
        while not STOP_EVENT.is_set():
            if REFRESH_PROXIES_EACH_CYCLE or not proxies_cache: 
                proxies_cache = fetch_all_proxies()
            
            if not proxies_cache:
                logging.warning("⚠️ 사용 가능한 프록시가 없습니다. 60초 후 재시도합니다.")
                time.sleep(60); continue

            for task in TASKS:
                for idx, proxy in enumerate(proxies_cache):
                    if STOP_EVENT.is_set(): break
                    
                    # 슬롯 대기 로깅
                    if len(active_threads) >= MAX_THREADS:
                        logging.info(f"⏳ 슬롯 가득 찼음 ({len(active_threads)}/{MAX_THREADS}). 빈 슬롯 대기 중...")
                    
                    while len(active_threads) >= MAX_THREADS:
                        active_threads = [t for t in active_threads if t.is_alive()]
                        time.sleep(1)
                    
                    # 📍 현재 사용 가능한 슬롯 ID 계산 (0부터 MAX_THREADS-1까지)
                    used_slots = set()
                    for t in active_threads:
                        if t.is_alive() and '-slot' in t.name:
                            slot_part = t.name.split('-slot')[-1]
                            try:
                                used_slots.add(int(slot_part))
                            except: pass
                    
                    # 사용 가능한 첫 번째 슬롯 찾기
                    available_slot = None
                    for slot_num in range(MAX_THREADS):
                        if slot_num not in used_slots:
                            available_slot = slot_num
                            break
                    
                    if available_slot is None:
                        available_slot = len(active_threads)  # 폴백
                    
                    slot_id = str(available_slot)
                    
                    # 새 쓰레드 생성 및 실행 (쓰레드 이름에 슬롯 ID 포함)
                    t_name = f"{task['keyword']}-{idx}-slot{slot_id}"
                    t = threading.Thread(target=thread_worker, args=(task, proxy, slot_id), name=t_name, daemon=True)
                    active_threads.append(t)
                    t.start()
                    logging.info(f"➕ 새 쓰레드 할당: [{t_name}] | 남은 슬롯: {MAX_THREADS - len(active_threads)}")

            logging.info("⏳ 모든 작업 투입 완료. 활성 쓰레드 종료 대기 중...")
            while any(t.is_alive() for t in active_threads):
                active_threads = [t for t in active_threads if t.is_alive()]
                time.sleep(2)
                
            logging.info(f"✅ 사이클 완료. {CHECK_INTERVAL_SECONDS}초 대기 후 재시작합니다.")
            time.sleep(CHECK_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        STOP_EVENT.set()
        logging.info("🛑 사용자 중단 신호 감지. 프로그램을 종료합니다.")

if __name__ == "__main__":
    main_loop()