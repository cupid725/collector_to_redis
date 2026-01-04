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
from pathlib import Path

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
WINDOW_WIDTH = 1000
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

# 전역 변수: 내 공인 IP 저장용
MY_PUBLIC_IP = None

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
    ("https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&country=KR&anonymityLevel=elite", "http", True),
]

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
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s", datefmt="%H:%M:%S")
    
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    
    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(sh)

# =============================================================================
# 2-1) 🔒 추가된 IP 확인 및 스텔스 유틸리티 함수
# =============================================================================
def get_my_actual_ip():
    """실행 시점의 내 실제 공인 IP 확인"""
    try:
        res = requests.get("https://api.ipify.org", timeout=10)
        return res.text.strip()
    except:
        return None

def is_proxy_leaking_my_ip(proxy: ProxyInfo, my_ip: str):
    """프록시가 내 IP를 유출하고 있는지(내 IP가 반환되는지) 확인"""
    if not my_ip: return False # 내 IP를 모르면 체크 불가하므로 패스
    try:
        proxies = {
            "http": f"{proxy.protocol}://{proxy.address}",
            "https": f"{proxy.protocol}://{proxy.address}"
        }
        res = requests.get("https://api.ipify.org", proxies=proxies, timeout=10)
        return res.text.strip() == my_ip
    except:
        return False

def load_window_state(slot_id: str) -> Optional[Dict]:
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
    try:
        with WINDOW_STATE_LOCK:
            states = {}
            if os.path.exists(WINDOW_STATE_FILE):
                with open(WINDOW_STATE_FILE, 'r', encoding='utf-8') as f:
                    states = json.load(f)
            states[slot_id] = {'x': x, 'y': y, 'width': width, 'height': height}
            with open(WINDOW_STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(states, f, indent=2)
            logging.info(f"💾 창 상태 저장 완료 (슬롯 {slot_id}): 위치({x},{y}) 크기({width}x{height})")
    except Exception as e:
        logging.warning(f"⚠️ 창 상태 저장 실패 (슬롯 {slot_id}): {e}")

def random_delay(min_sec: float = None, max_sec: float = None) -> None:
    if not ENABLE_STEALTH: return
    min_val = min_sec if min_sec is not None else RANDOM_DELAY_MIN
    max_val = max_sec if max_sec is not None else RANDOM_DELAY_MAX
    time.sleep(random.uniform(min_val, max_val))

def simulate_human_typing(element, text: str) -> None:
    if not ENABLE_STEALTH:
        element.send_keys(text)
        return
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(0.05, 0.15))

def simulate_scroll(driver, scroll_count: int = 3) -> None:
    if not ENABLE_STEALTH or not SCROLL_BEHAVIOR: return
    for _ in range(scroll_count):
        scroll_amount = random.randint(200, 500)
        driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
        time.sleep(random.uniform(0.3, 0.8))

def get_random_user_agent() -> str:
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]
    return random.choice(user_agents)

def inject_stealth_scripts(driver) -> None:
    if not ENABLE_STEALTH: return
    stealth_js = """
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    window.chrome = { runtime: {} };
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
            Promise.resolve({ state: Notification.permission }) :
            originalQuery(parameters)
    );
    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, 'languages', { get: () => ['ko-KR', 'ko', 'en-US', 'en'] });
    """
    try:
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {'source': stealth_js})
        logging.info("🔒 스텔스 스크립트 주입 완료")
    except Exception as e:
        logging.warning(f"⚠️ 스텔스 스크립트 주입 실패: {e}")

# =============================================================================
# 3) 프록시 수집 및 검증 (기존 유지)
# =============================================================================
def fetch_all_proxies() -> List[ProxyInfo]:
    logging.info("📄 [수집] 프록시 수집 시작")
    raw_list = []
    for url, default_proto, _ in ALL_SOURCES:
        if STOP_EVENT.is_set(): break
        try:
            resp = requests.get(url, timeout=20, headers=HEADERS)
            if resp.status_code != 200: continue
            content = resp.text.strip()
            count = 0
            if content.startswith('{') or content.startswith('['):
                try:
                    data = resp.json()
                    items = data.get('data', []) if isinstance(data, dict) else data
                    for item in items:
                        if isinstance(item, dict) and 'ip' in item and 'port' in item:
                            addr = f"{item['ip']}:{item['port']}"
                            actual_proto = item['protocols'][0].lower() if 'protocols' in item and item['protocols'] else default_proto
                            raw_list.append(ProxyInfo(protocol=actual_proto, address=addr, source=urlparse(url).netloc))
                            count += 1
                except: pass
            else:
                for line in content.splitlines():
                    line = line.strip()
                    if not line or line.startswith('#'): continue
                    addr = line.split("://")[-1] if "://" in line else line
                    if ":" in addr:
                        raw_list.append(ProxyInfo(protocol=default_proto, address=addr, source=urlparse(url).netloc))
                        count += 1
            if count > 0:
                logging.info(f"📥 [수집] {urlparse(url).netloc:20s} | {count:4d}개 ({default_proto})")
        except Exception as e:
            logging.error(f"⚠️ [실패] {urlparse(url).netloc}: {e}")
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
# 4) 브라우저 드라이버 생성 (기존 유지)
# =============================================================================
def make_driver(proxy: Optional[ProxyInfo], slot_id: str = "0") -> Tuple[uc.Chrome, str]:
    tmp_root = Path(__file__).resolve().parent / "_tmp_profiles"
    tmp_root.mkdir(parents=True, exist_ok=True)
    profile_dir = tempfile.mkdtemp(prefix=f"naver_mon_profile_", dir=str(tmp_root))

    driver = None
    try:
        options = uc.ChromeOptions()
        
        options.add_argument(f"--user-data-dir={profile_dir}")
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--lang=ko-KR")
        options.set_capability("pageLoadStrategy", "eager")   # ✅ 핵심
        
        if ENABLE_STEALTH:
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-web-security")
            options.add_argument("--disable-features=IsolateOrigins,site-per-process")
            options.add_argument("--disable-infobars")
            options.add_argument("--disable-extensions")
            options.add_argument("--profile-directory=Default")
            options.add_argument("--ignore-certificate-errors")
            options.add_argument("--disable-gpu")
            user_agent = get_random_user_agent()
            options.add_argument(f"--user-agent={user_agent}")
            logging.info(f"🎭 랜덤 User-Agent 적용: {user_agent[:50]}.")

        if RUN_HEADLESS:
            options.add_argument("--headless=new")

        if proxy:
            proxy_str = f"{proxy.protocol}://{proxy.address}"
            logging.info(f"🌐 [드라이버 생성] 프록시 적용: {proxy_str} (출처: {proxy.source})")
            options.add_argument(f"--proxy-server={proxy_str}")

        driver = uc.Chrome(options=options, use_subprocess=True)
        driver.set_page_load_timeout(PAGELOAD_TIMEOUT_SEC)

        if ENABLE_STEALTH:
            inject_stealth_scripts(driver)

        saved_state = load_window_state(slot_id)
        try:
            if saved_state:
                driver.set_window_size(saved_state['width'], saved_state['height'])
                driver.set_window_position(saved_state['x'], saved_state['y'])
            else:
                if ENABLE_WINDOW_SIZE:
                    w, h = WINDOW_WIDTH, WINDOW_HEIGHT
                    if ENABLE_WINDOW_JITTER:
                        w += random.randint(-WINDOW_JITTER_RANGE, WINDOW_JITTER_RANGE)
                        h += random.randint(-WINDOW_JITTER_RANGE, WINDOW_JITTER_RANGE)
                    driver.set_window_size(max(300, w), max(300, h))
                if ENABLE_WINDOW_POSITION:
                    driver.set_window_position(WINDOW_POS_X, WINDOW_POS_Y)
        except Exception as e:
            logging.warning(f"⚠️ 창 설정 적용 실패: {e}")

        return driver, profile_dir

    except Exception as e:
        logging.error(f"🛑 make_driver 예외 → 프로필 정리 시도: {e}")

        # 드라이버가 일부라도 떴으면 닫기
        if driver:
            try:
                driver.quit()
            except:
                pass
            time.sleep(0.2)

        # 프로필 삭제(재시도)
        for i in range(10):
            try:
                if profile_dir and os.path.exists(profile_dir):
                    shutil.rmtree(profile_dir)
                break
            except Exception as e2:
                logging.warning(f"⚠️ 프로필 삭제 실패(try {i+1}/10): {profile_dir} | {e2}")
                time.sleep(0.3 * (i + 1))

        raise


def make_driver_old(proxy: Optional[ProxyInfo], slot_id: str = "0") -> Tuple[uc.Chrome, str]:
    tmp_root = Path(__file__).resolve().parent / "_tmp_profiles"
    tmp_root.mkdir(parents=True, exist_ok=True)
    profile_dir = tempfile.mkdtemp(prefix=f"naver_mon_profile_", dir=str(tmp_root))
    
    options = uc.ChromeOptions()
    options.add_argument(f"--user-data-dir={profile_dir}")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--lang=ko-KR")
    
    if ENABLE_STEALTH:
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-web-security")
        options.add_argument("--disable-features=IsolateOrigins,site-per-process")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument("--profile-directory=Default")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--disable-gpu")
        user_agent = get_random_user_agent()
        options.add_argument(f"--user-agent={user_agent}")
        logging.info(f"🎭 랜덤 User-Agent 적용: {user_agent[:50]}...")
    
    if RUN_HEADLESS: options.add_argument("--headless=new")
    if proxy:
        proxy_str = f"{proxy.protocol}://{proxy.address}"
        logging.info(f"🌐 [드라이버 생성] 프록시 적용: {proxy_str} (출처: {proxy.source})")
        options.add_argument(f"--proxy-server={proxy_str}")

    #driver = uc.Chrome(options=options, use_subprocess=True)
    try:
        driver = uc.Chrome(options=options, use_subprocess=True)
    except Exception:
        # 드라이버 생성 단계에서 예외가 발생하면 profile_dir이 누수되지 않도록 즉시 정리
        try:
            shutil.rmtree(profile_dir, ignore_errors=True)
        except Exception:
            pass
    raise
    driver.set_page_load_timeout(PAGELOAD_TIMEOUT_SEC)
    
    if ENABLE_STEALTH: inject_stealth_scripts(driver)
    
    saved_state = load_window_state(slot_id)
    try:
        if saved_state:
            driver.set_window_size(saved_state['width'], saved_state['height'])
            driver.set_window_position(saved_state['x'], saved_state['y'])
        else:
            if ENABLE_WINDOW_SIZE:
                w, h = WINDOW_WIDTH, WINDOW_HEIGHT
                if ENABLE_WINDOW_JITTER:
                    w += random.randint(-WINDOW_JITTER_RANGE, WINDOW_JITTER_RANGE)
                    h += random.randint(-WINDOW_JITTER_RANGE, WINDOW_JITTER_RANGE)
                driver.set_window_size(max(300, w), max(300, h))
            if ENABLE_WINDOW_POSITION:
                driver.set_window_position(WINDOW_POS_X, WINDOW_POS_Y)
    except Exception as e:
        logging.warning(f"⚠️ 창 설정 적용 실패: {e}")
    return driver, profile_dir

def update_query_param(url: str, **kwargs) -> str:
    u = urlparse(url)
    q = parse_qs(u.query)
    for k, v in kwargs.items(): q[str(k)] = [str(v)]
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))

def simulate_natural_scroll(driver, min_actions: int = 6, max_actions: int = 12) -> None:
    """
    자연스러운 읽기 행동처럼:
    - 아래로 여러 번 스크롤
    - 잠깐 멈춰서 읽는 듯 대기
    - 위로 조금 되돌아가는 스크롤
    """
    if not ENABLE_STEALTH or not SCROLL_BEHAVIOR:
        return

    try:
        scroll_h = driver.execute_script(
            "return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight) || 0;"
        )
        view_h = driver.execute_script("return window.innerHeight || 0;")
        if not scroll_h or not view_h:
            return
        if scroll_h <= view_h + 80:
            return  # 스크롤할 게 거의 없음
    except Exception:
        return

    actions = random.randint(min_actions, max_actions)
    down_actions = max(2, int(actions * random.uniform(0.6, 0.8)))
    up_actions = max(1, actions - down_actions)

    # 아래로 스크롤 (부드럽게)
    for _ in range(down_actions):
        step = random.randint(int(view_h * 0.25), int(view_h * 0.95))
        try:
            driver.execute_script(
                "window.scrollBy({top: arguments[0], left: 0, behavior: 'smooth'});",
                step,
            )
        except Exception:
            driver.execute_script("window.scrollBy(0, arguments[0]);", step)
        time.sleep(random.uniform(0.4, 1.2))

        # 중간중간 '읽는' 멈춤
        if random.random() < 0.25:
            time.sleep(random.uniform(0.7, 1.8))

    # 잠깐 머무름
    time.sleep(random.uniform(1.0, 2.5))

    # 위로 조금 되돌리기
    for _ in range(up_actions):
        step = random.randint(int(view_h * 0.15), int(view_h * 0.75))
        try:
            driver.execute_script(
                "window.scrollBy({top: -arguments[0], left: 0, behavior: 'smooth'});",
                step,
            )
        except Exception:
            driver.execute_script("window.scrollBy(0, -arguments[0]);", step)
        time.sleep(random.uniform(0.35, 1.0))

    # 마지막에 아주 미세한 흔들림(가끔)
    if random.random() < 0.5:
        jiggle = random.randint(-120, 120)
        driver.execute_script("window.scrollBy(0, arguments[0]);", jiggle)
        time.sleep(random.uniform(0.2, 0.6))

from selenium.webdriver.common.action_chains import ActionChains
def wait_and_mouse_click_live_more(driver, timeout=60):
    
    try :
        sel = (By.CSS_SELECTOR, "li a[href='/live-more']")

        # 1) 클릭 가능 상태까지 대기
        elem = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(sel))

        # 2) 화면 중앙으로 스크롤(가끔 오버레이/고정헤더 때문에 필요)
        driver.execute_script(
            "arguments[0].scrollIntoView({block:'center', inline:'center'});",
            elem
        )

        # 3) “마우스로” 이동 후 클릭
        ActionChains(driver).move_to_element(elem).pause(0.2).click(elem).perform()
        logging.info("📥 [동작 성공] /live-more") 
    except :
        logging.info("📥 [동작 실패] /live-more") 
        return False
    
    return True

# =============================================================================
# 5) 작업 로직 (IP 노출 필터링 기능 통합)
# =============================================================================
def thread_worker(task: Dict, proxy: ProxyInfo, slot_id: str = "0"):
    keyword, target_url = task["keyword"], task["domain"]
    logging.info(f"▶️ 작업 시작 | 슬롯: {slot_id} | 키워드: [{keyword}] | 프록시: {proxy.address}")

    driver, profile_dir = None, ""
    rr = RunResult(
        datetime.now().isoformat(timespec="seconds"),
        keyword, target_url,
        proxy.protocol, proxy.address, proxy.source,
        False, None, None, None,
        False, None, None, None
    )

    try:
        # 1. TCP 체크 및 내 IP 유출 검사
        if not tcp_quick_check(proxy.address):
            logging.warning(f"❌ TCP 연결 실패: {proxy.address}")
            rr.error = "TCP_CONNECT_FAIL"

        elif is_proxy_leaking_my_ip(proxy, MY_PUBLIC_IP):
            logging.warning(f"❌ 프록시 거부 (내 공인 IP 노출됨): {proxy.address}")
            rr.error = "IP_LEAK_DETECTED"

        else:
            logging.info(f"🌐 브라우저 실행 중. (슬롯 {slot_id})")
            driver, profile_dir = make_driver(proxy, slot_id)

            random_delay(1.0, 2.0)
            logging.info(f"🔍 네이버 접속 및 키워드 검색: [{keyword}]")
            driver.get("https://www.naver.com/")
            #WebDriverWait(driver, ELEM_WAIT_SEC).until(
            #    lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
            #)
            WebDriverWait(driver, ELEM_WAIT_SEC).until(
                lambda d: d.execute_script("return document.readyState") != "loading"
            )

            random_delay(1.5, 3.0)
            simulate_scroll(driver, scroll_count=2)

            box = WebDriverWait(driver, ELEM_WAIT_SEC).until(
                EC.presence_of_element_located((By.NAME, "query"))
            )
            box.clear()
            simulate_human_typing(box, keyword)
            random_delay(0.5, 1.0)
            box.send_keys(Keys.ENTER)

            WebDriverWait(driver, ELEM_WAIT_SEC).until(
                lambda d: "search.naver.com" in (d.current_url or "")
            )
            results_url = driver.current_url
            random_delay(2.0, 4.0)

            for page in range(1, MAX_PAGES + 1):
                if STOP_EVENT.is_set():
                    break

                logging.info(f"📄 페이지 탐색 중. ({page}/{MAX_PAGES} page)")
                driver.get(update_query_param(results_url, start=1 + (page - 1) * 10))
                random_delay(2.0, 3.5)
                simulate_scroll(driver, scroll_count=3)

                found_data = None
                anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")

                # target canonical은 "가장 마지막 계산값"을 그대로 쓰지 않도록 밖에서 관리
                t_can = urlunparse((
                    urlparse(target_url).scheme,
                    urlparse(target_url).netloc,
                    urlparse(target_url).path or "/",
                    "", "", ""
                )) if target_url else None

                for idx, a in enumerate(anchors, 1):
                    try:
                        href = a.get_attribute("href") or ""
                        if href and target_url:
                            h_can = urlunparse((
                                urlparse(href).scheme,
                                urlparse(href).netloc,
                                urlparse(href).path or "/",
                                "", "", ""
                            ))

                            if h_can.lower() == t_can.lower():
                                # ✅ element까지 같이 저장 (실제 클릭)
                                found_data = (idx, href, a)
                                break
                    except:
                        continue

                if found_data:
                    rank, href, elem = found_data
                    rr.found, rr.found_page, rr.found_rank_on_page, rr.found_href = True, page, rank, href
                    random_delay(1.0, 2.5)

                    # ===== (너가 요구한 클릭 로그 스니펫 그대로) =====
                    handles_before = driver.window_handles
                    url_before = driver.current_url

                    # 클릭이 가려져서 안먹는 케이스 줄이기
                    try:
                        driver.execute_script(
                            "arguments[0].scrollIntoView({block:'center', inline:'center'});",
                            elem
                        )
                    except:
                        pass
                    random_delay(0.3, 0.8)

                    elem.click()
                    logging.info(f"[Slot-{slot_id}] ✅ elem.click() executed")

                    # 클릭 결과 확인(새탭/이동 여부)
                    time.sleep(0.2)
                    handles_after = driver.window_handles
                    url_after = driver.current_url

                    logging.info(
                        f"[Slot-{slot_id}] 🔎 after click | handles: {len(handles_before)}→{len(handles_after)} | url: {url_before} → {url_after}"
                    )
                    # ==============================================

                    # 새 탭이면 전환했다가, 작업 끝나면 닫고 부모로 복귀
                    parent_handle = driver.current_window_handle
                    child_handle = None
                    try:
                        new_handles = [h for h in handles_after if h not in handles_before]
                        if new_handles:
                            child_handle = new_handles[-1]
                            driver.switch_to.window(child_handle)
                    except:
                        child_handle = None

                    # 클릭 후 실제 로딩 대기
                    #try:#

                    #    #WebDriverWait(driver, 10).until(
                    #    #    lambda d: d.execute_script("return document.readyState") != "loading"
                    #    #)
                    #    WebDriverWait(driver, 10).until(
                    #        EC.visibility_of_element_located((By.CSS_SELECTOR, "li a[href='/live-more']"))
                    #    )    
                    #except:
                    #    pass
                    # ✅ 로딩 끝나면 자연스러운 스크롤 다운/업
                    #random_delay(30.0, 60.0)
                    #simulate_natural_scroll(driver)
                    #random_delay(300.0, 360.0)
                    random_delay(30.0, 60.0)
                    
                    if not wait_and_mouse_click_live_more(driver):
                        rr.clicked_ok = False
                        rr.note = "LIVE_MORE_CLICK_FAILED"
                        rr.error = "LIVE_MORE_CLICK_FAILED"
                        return  # ✅ 즉시 finally로 감
                    random_delay(30.0, 60.0)
                    simulate_natural_scroll(driver)
                    random_delay(300.0, 360.0)

                    final_url = driver.current_url
                    h_final = urlunparse((
                        urlparse(final_url).scheme,
                        urlparse(final_url).netloc,
                        urlparse(final_url).path or "/",
                        "", "", ""
                    ))

                    if t_can and h_final.lower() == t_can.lower():
                        rr.clicked_ok, rr.final_url = True, final_url
                    else:
                        rr.clicked_ok, rr.final_url, rr.note = False, final_url, "FINAL_URL_NOT_MATCH"

                    # 자식 탭은 닫고 부모로 복귀
                    if child_handle:
                        try:
                            driver.close()
                        except:
                            pass
                        try:
                            driver.switch_to.window(parent_handle)
                        except:
                            pass

                    break

                if page < MAX_PAGES:
                    random_delay(1.5, 3.0)

            if not rr.found and not rr.error:
                rr.error = "NOT_FOUND_IN_PAGES"

    except Exception as e:
        logging.error(f"💥 예외 발생: {str(e)[:100]}")
        rr.error = str(e)[:160]

    finally:
        # 창 상태 저장 + 드라이버 종료
        if driver:
            try:
                pos = driver.get_window_position()
                size = driver.get_window_size()
                save_window_state(slot_id, pos['x'], pos['y'], size['width'], size['height'])
            except:
                pass

            try:
                driver.quit()
            except:
                pass

            # quit 직후 파일락 완화
            time.sleep(0.3)

        # ✅ 프로필 디렉 삭제: ignore_errors 제거 + 재시도 + 실패 로그
        if profile_dir:
            def _onerror(func, path, exc_info):
                try:
                    os.chmod(path, 0o777)
                    func(path)
                except:
                    pass

            deleted = False
            for i in range(10):
                try:
                    if os.path.exists(profile_dir):
                        shutil.rmtree(profile_dir, onerror=_onerror)
                    if not os.path.exists(profile_dir):
                        deleted = True
                        break
                except Exception as e2:
                    logging.warning(f"⚠️ 프로필 삭제 실패(try {i+1}/10): {profile_dir} | {e2}")
                time.sleep(0.3 * (i + 1))

            if not deleted and os.path.exists(profile_dir):
                logging.error(f"🛑 프로필 디렉 최종 삭제 실패: {profile_dir}")

        # 결과 저장(원본 그대로)
        with FILE_LOCK:
            with open(RESULT_JSONL, "a", encoding="utf-8") as f:
                f.write(json.dumps(asdict(rr), ensure_ascii=False) + "\n")
            is_new = not os.path.exists(RESULT_CSV)
            with open(RESULT_CSV, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if is_new:
                    w.writerow([
                        "ts", "keyword", "target_url", "proxy_protocol", "proxy_address", "proxy_source",
                        "found", "found_page", "found_rank_on_page", "found_href",
                        "clicked_ok", "final_url", "error", "note"
                    ])
                w.writerow([
                    rr.ts, rr.keyword, rr.target_url, rr.proxy_protocol, rr.proxy_address, rr.proxy_source,
                    rr.found, rr.found_page, rr.found_rank_on_page, rr.found_href,
                    rr.clicked_ok, rr.final_url, rr.error, rr.note
                ])

        logging.info(f"🏁 작업 종료 | 슬롯: {slot_id} | 결과: {'성공' if rr.found else '실패'}")


def thread_worker_old(task: Dict, proxy: ProxyInfo, slot_id: str = "0"):
    keyword, target_url = task["keyword"], task["domain"]
    logging.info(f"▶️ 작업 시작 | 슬롯: {slot_id} | 키워드: [{keyword}] | 프록시: {proxy.address}")
    
    driver, profile_dir = None, ""
    rr = RunResult(datetime.now().isoformat(timespec="seconds"), keyword, target_url, proxy.protocol, proxy.address, proxy.source, False, None, None, None, False, None, None, None)
    
    try:
        # 1. TCP 체크 및 내 IP 유출 검사
        if not tcp_quick_check(proxy.address):
            logging.warning(f"❌ TCP 연결 실패: {proxy.address}")
            rr.error = "TCP_CONNECT_FAIL"
        elif is_proxy_leaking_my_ip(proxy, MY_PUBLIC_IP):
            logging.warning(f"❌ 프록시 거부 (내 공인 IP 노출됨): {proxy.address}")
            rr.error = "IP_LEAK_DETECTED"
        else:
            logging.info(f"🌐 브라우저 실행 중... (슬롯 {slot_id})")
            driver, profile_dir = make_driver(proxy, slot_id)
            
            random_delay(1.0, 2.0)
            logging.info(f"🔍 네이버 접속 및 키워드 검색: [{keyword}]")
            driver.get("https://www.naver.com/")
            WebDriverWait(driver, ELEM_WAIT_SEC).until(lambda d: d.execute_script("return document.readyState") in ("interactive", "complete"))
            
            random_delay(1.5, 3.0)
            simulate_scroll(driver, scroll_count=2)
            
            box = WebDriverWait(driver, ELEM_WAIT_SEC).until(EC.presence_of_element_located((By.NAME, "query")))
            box.clear()
            simulate_human_typing(box, keyword)
            random_delay(0.5, 1.0)
            box.send_keys(Keys.ENTER)
            
            WebDriverWait(driver, ELEM_WAIT_SEC).until(lambda d: "search.naver.com" in (d.current_url or ""))
            results_url = driver.current_url
            random_delay(2.0, 4.0)
            
            for page in range(1, MAX_PAGES + 1):
                if STOP_EVENT.is_set(): break
                logging.info(f"📄 페이지 탐색 중... ({page}/{MAX_PAGES} page)")
                driver.get(update_query_param(results_url, start=1 + (page - 1) * 10))
                random_delay(2.0, 3.5)
                simulate_scroll(driver, scroll_count=3)
                
                found_data = None
                anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")
                for idx, a in enumerate(anchors, 1):
                    try:
                        href = a.get_attribute("href") or ""
                        if href and target_url:
                            h_can = urlunparse((urlparse(href).scheme, urlparse(href).netloc, urlparse(href).path or "/", "", "", ""))
                            t_can = urlunparse((urlparse(target_url).scheme, urlparse(target_url).netloc, urlparse(target_url).path or "/", "", "", ""))
                            if h_can.lower() == t_can.lower():
                                found_data = (idx, href)
                                break
                    except: continue
                
                if found_data:
                    rank, href = found_data
                    rr.found, rr.found_page, rr.found_rank_on_page, rr.found_href = True, page, rank, href
                    random_delay(1.0, 2.5)
                    driver.get(href)
                    WebDriverWait(driver, 10).until(lambda d: d.execute_script("return document.readyState") in ("interactive", "complete"))
                    random_delay(2.0, 3.0)
                    
                    final_url = driver.current_url
                    h_final = urlunparse((urlparse(final_url).scheme, urlparse(final_url).netloc, urlparse(final_url).path or "/", "", "", ""))
                    if h_final.lower() == t_can.lower():
                        rr.clicked_ok, rr.final_url = True, final_url
                    else:
                        rr.clicked_ok, rr.final_url, rr.note = False, final_url, "FINAL_URL_NOT_MATCH"
                    break
                
                if page < MAX_PAGES: random_delay(1.5, 3.0)
            
            if not rr.found and not rr.error:
                rr.error = "NOT_FOUND_IN_PAGES"

    except Exception as e:
        logging.error(f"💥 예외 발생: {str(e)[:100]}")
        rr.error = str(e)[:160]
    finally:
        if driver:
            try:
                pos = driver.get_window_position()
                size = driver.get_window_size()
                save_window_state(slot_id, pos['x'], pos['y'], size['width'], size['height'])
            except: pass
            try: driver.quit()
            except: pass
        if profile_dir: shutil.rmtree(profile_dir, ignore_errors=True)
        
        with FILE_LOCK:
            with open(RESULT_JSONL, "a", encoding="utf-8") as f:
                f.write(json.dumps(asdict(rr), ensure_ascii=False) + "\n")
            is_new = not os.path.exists(RESULT_CSV)
            with open(RESULT_CSV, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if is_new: w.writerow(["ts", "keyword", "target_url", "proxy_protocol", "proxy_address", "proxy_source", "found", "found_page", "found_rank_on_page", "found_href", "clicked_ok", "final_url", "error", "note"])
                w.writerow([rr.ts, rr.keyword, rr.target_url, rr.proxy_protocol, rr.proxy_address, rr.proxy_source, rr.found, rr.found_page, rr.found_rank_on_page, rr.found_href, rr.clicked_ok, rr.final_url, rr.error, rr.note])
        logging.info(f"🏁 작업 종료 | 슬롯: {slot_id} | 결과: {'성공' if rr.found else '실패'}")

# =============================================================================
# 6) 메인 루프
# =============================================================================


def main_loop() -> None:
    global MY_PUBLIC_IP
    setup_logging()
    logging.info("==================================================")
    logging.info("🚀 Naver Exposure Monitor 시작")
    
    # 시작 시 내 실제 공인 IP를 먼저 확인
    MY_PUBLIC_IP = get_my_actual_ip()
    logging.info(f"🏠 내 공인 IP: {MY_PUBLIC_IP}")
    
    logging.info(f"⚙️ 설정: 쓰레드 슬롯 {MAX_THREADS}개 / 탐색 {MAX_PAGES}페이지")
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
                    while len(active_threads) >= MAX_THREADS:
                        active_threads = [t for t in active_threads if t.is_alive()]
                        time.sleep(1)
                    
                    used_slots = set()
                    for t in active_threads:
                        if t.is_alive() and '-slot' in t.name:
                            try: used_slots.add(int(t.name.split('-slot')[-1]))
                            except: pass
                    
                    available_slot = None
                    for slot_num in range(MAX_THREADS):
                        if slot_num not in used_slots:
                            available_slot = slot_num
                            break
                    if available_slot is None: available_slot = 0
                    
                    slot_id = str(available_slot)
                    t_name = f"{task['keyword']}-{idx}-slot{slot_id}"
                    t = threading.Thread(target=thread_worker, args=(task, proxy, slot_id), name=t_name, daemon=True)
                    active_threads.append(t)
                    t.start()
                    logging.info(f"➕ 새 쓰레드 할당: [{t_name}]")

            while any(t.is_alive() for t in active_threads):
                active_threads = [t for t in active_threads if t.is_alive()]
                time.sleep(2)
            logging.info(f"✅ 사이클 완료. {CHECK_INTERVAL_SECONDS}초 대기...")
            time.sleep(CHECK_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        STOP_EVENT.set()
        logging.info("🛑 프로그램 종료")

if __name__ == "__main__":
    main_loop()