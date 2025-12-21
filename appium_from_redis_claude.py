import random
import threading
import time
import json
import os
from typing import Dict, Any, Optional
from urllib.parse import urlparse

# 외부 라이브러리
import numpy as np  # pip install numpy
import redis        # pip install redis

from appium import webdriver as appium_webdriver
from appium.options.android import UiAutomator2Options
from appium.webdriver.common.appiumby import AppiumBy

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    NoSuchElementException,
    InvalidSessionIdException,
    NoSuchWindowException,
)


# 드라이버 생성 시 동시 접근 방지용 Lock
driver_creation_lock = threading.Lock()

# 모든 스레드에 중단 신호를 보내기 위한 전역 Event
stop_event = threading.Event()

# ===================== Redis 설정 =====================
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None

# Lease 방식 키
REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_LEASE = "proxies:lease"
REDIS_HASH_FAIL  = "proxies:fail"
REDIS_ZSET_USED  = "proxies:used_recent"

def get_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )

# --------------------- Lease Lua (원자적) ---------------------
_LUA_CLAIM = r"""
local alive = KEYS[1]
local lease = KEYS[2]
local now = tonumber(ARGV[1])
local lease_sec = tonumber(ARGV[2])
local reclaim_limit = tonumber(ARGV[3])
local sample_k = tonumber(ARGV[4])
local rand_int = tonumber(ARGV[5])

local expired = redis.call('ZRANGEBYSCORE', lease, '-inf', now, 'LIMIT', 0, reclaim_limit)
for i, m in ipairs(expired) do
  redis.call('ZREM', lease, m)
  redis.call('ZADD', alive, 0, m)
end

local cands = redis.call('ZRANGEBYSCORE', alive, '-inf', now, 'LIMIT', 0, sample_k)
if (not cands) or (#cands == 0) then
  return nil
end

local idx = (rand_int % #cands) + 1
local m = cands[idx]

redis.call('ZREM', alive, m)
redis.call('ZADD', lease, now + lease_sec, m)
return m
"""

_LUA_RELEASE = r"""
local alive = KEYS[1]
local lease = KEYS[2]
local member = ARGV[1]
local next_time = tonumber(ARGV[2])

redis.call('ZREM', lease, member)
redis.call('ZADD', alive, next_time, member)
return 1
"""

_LUA_BAN = r"""
local alive = KEYS[1]
local lease = KEYS[2]
local member = ARGV[1]
redis.call('ZREM', alive, member)
redis.call('ZREM', lease, member)
return 1
"""

def claim_proxy(
    r: redis.Redis,
    lease_seconds: int,
    reclaim_limit: int = 200,
    sample_k: int = 50,
) -> Optional[str]:
    now = int(time.time())
    rand_int = random.randint(0, 2_147_483_647)
    try:
        member = r.eval(
            _LUA_CLAIM,
            2,
            REDIS_ZSET_ALIVE,
            REDIS_ZSET_LEASE,
            now,
            int(lease_seconds),
            int(reclaim_limit),
            int(sample_k),
            int(rand_int),
        )
    except redis.RedisError as e:
        print(f"[REDIS] claim_proxy 실패: {e}")
        return None

    if not member:
        return None
    if "://" not in member:
        return None
    return member

def release_proxy(r: redis.Redis, member: str, cooldown_seconds: int = 0) -> None:
    next_time = int(time.time()) + max(0, int(cooldown_seconds))
    try:
        r.eval(_LUA_RELEASE, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, member, next_time)
    except redis.RedisError as e:
        print(f"[REDIS] release_proxy 실패: {e}")

def ban_proxy(r: redis.Redis, member: str) -> None:
    try:
        r.eval(_LUA_BAN, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, member)
    except redis.RedisError as e:
        print(f"[REDIS] ban_proxy 실패: {e}")

def inc_fail(r: redis.Redis, member: str) -> int:
    try:
        return int(r.hincrby(REDIS_HASH_FAIL, member, 1))
    except redis.RedisError:
        return 1

def reset_fail(r: redis.Redis, member: str) -> None:
    try:
        r.hdel(REDIS_HASH_FAIL, member)
    except redis.RedisError:
        pass

def log_proxy_used(r: redis.Redis, member: str) -> None:
    try:
        r.zadd(REDIS_ZSET_USED, {member: time.time()})
    except redis.RedisError:
        pass

# ===================== REGION_PROFILES: JSON에서 로드 =====================

def load_region_profiles(json_path: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    if json_path is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        json_path = os.path.join(base_dir, "region_profiles.json")

    if not os.path.exists(json_path):
        raise FileNotFoundError(f"region_profiles.json 파일을 찾을 수 없습니다: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict) or not data:
        raise ValueError("region_profiles.json 내용이 비어있거나 형식이 올바르지 않습니다.")

    return data

try:
    REGION_PROFILES: Dict[str, Dict[str, Any]] = load_region_profiles()
    print(f"[INIT] region_profiles.json 로드 완료. 지역 수: {len(REGION_PROFILES)}")
except Exception as e:
    print(f"[INIT] ❌ REGION_PROFILES 로드 실패: {e}")
    REGION_PROFILES = {}

# ===================== 공통 설정 =====================
TARGET_URL = "https://www.youtube.com/shorts/mcy0JKTavW4?feature=share"  # 첫눈
TARGET_URL1 = "https://youtube.com/shorts/-vVnZoVtnFk?feature=share"  # 크리스마스
TARGET_URL = "https://www.youtube.com/shorts/u7sO-mNEpT4?feature=share"  # 크리스마스 2

COMMAND_TIMEOUT = 300
LOAD_TIMEOUT = COMMAND_TIMEOUT
ENSURE_TIMEOUT = 420
BROWSE_MAX_SECONDS = ENSURE_TIMEOUT
STAY_DURATION = 300

NUM_BROWSERS = 1  # Nox 에뮬레이터 2개

WAIT_WHEN_NO_PROXY_SECONDS = 60

# Lease 운영 파라미터
LEASE_SECONDS = max(120, int(ENSURE_TIMEOUT + STAY_DURATION + 120))

COOLDOWN_SUCCESS = 0
COOLDOWN_FAIL_BASE = 30
COOLDOWN_FAIL_JITTER = 60
MAX_FAIL = 5

# ===================== Appium 서버 설정 (Nox 2개) =====================
APPIUM_CONFIGS = [
    {
        "appium_server": "http://127.0.0.1:4723",
        "device_name": "127.0.0.1:62001",
        "platform_version": None,  # None으로 설정하면 자동 감지
    },
    {
        "appium_server": "http://127.0.0.1:4724",
        "device_name": "127.0.0.1:62025",
        "platform_version": None,  # None으로 설정하면 자동 감지
    },
]

# ===================== 사람처럼 행동하는 유틸 =====================
def human_sleep(min_sec=0.5, max_sec=2.0, mu=None, sigma=None):
    if mu is None:
        mu = (min_sec + max_sec) / 2
    if sigma is None:
        sigma = (max_sec - min_sec) / 4
    sleep_time = random.gauss(mu, sigma)
    sleep_time = max(min_sec, min(sleep_time, max_sec))
    time.sleep(sleep_time)

def human_scroll(driver):
    """Appium에서 스크롤 (스와이프)"""
    try:
        size = driver.get_window_size()
        start_x = size['width'] // 2
        start_y = int(size['height'] * 0.7)
        end_y = int(size['height'] * 0.3)
        
        # 여러 번 작은 스크롤
        for _ in range(random.randint(2, 4)):
            driver.swipe(start_x, start_y, start_x, end_y, random.randint(300, 600))
            time.sleep(random.uniform(0.3, 0.8))
            
        # 가끔 역방향 스크롤
        if random.random() < 0.3:
            driver.swipe(start_x, end_y, start_x, start_y, random.randint(200, 400))
            
    except Exception as e:
        print(f"   [Scroll] 스크롤 실패: {e}")

def human_tap(driver):
    """화면 랜덤 위치 탭"""
    try:
        size = driver.get_window_size()
        x = random.randint(100, size['width'] - 100)
        y = random.randint(100, size['height'] - 100)
        driver.tap([(x, y)], random.randint(50, 150))
        time.sleep(random.uniform(0.2, 0.5))
    except Exception:
        pass

# ===================== Proxy 정규화 =====================
def normalize_proxy_for_android(proxy: Optional[str]) -> tuple:
    """
    Redis member('proto://ip:port')를 Android proxy 설정용으로 파싱
    Returns: (host, port, type) or (None, None, None)
    """
    if not proxy:
        return None, None, None
    
    p = proxy.strip()
    
    # https:// -> http://
    if p.startswith("https://"):
        p = "http://" + p[len("https://"):]
    
    # socks:// -> socks5://
    if p.startswith("socks://"):
        p = "socks5://" + p[len("socks://"):]
    
    # proto://host:port 파싱
    try:
        if "://" in p:
            proto, rest = p.split("://", 1)
            if ":" in rest:
                host, port = rest.rsplit(":", 1)
                return host, int(port), proto.lower()
    except Exception:
        pass
    
    return None, None, None

def get_available_browser(device_name: str) -> str:
    """설치된 브라우저 확인"""
    import subprocess
    
    try:
        # 설치된 패키지 확인
        result = subprocess.run(
            ["adb", "-s", device_name, "shell", "pm", "list", "packages"],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        packages = result.stdout.lower()
        
        # Chrome 우선
        if "com.android.chrome" in packages:
            return "Chrome"
        # Chromium
        elif "org.chromium" in packages:
            return "chromium-browser"
        # 기본 브라우저
        else:
            return "Browser"
            
    except Exception as e:
        print(f"   ⚠️ 브라우저 확인 실패: {e}, 기본값(Browser) 사용")
        return "Browser"
# ===================== Appium 드라이버 생성 =====================
def normalize_locale_for_android(locale: str) -> str:
    """
    Android locale 형식으로 정규화: 단순히 하이픈을 언더스코어로 변경
    "ja-JP" -> "ja_JP", "ko-KR" -> "ko_KR"
    """
    if not locale:
        return "en_US"
    
    return locale.replace("-", "_")

def create_appium_driver(
    profile: Dict[str, Any],
    proxy: Optional[str],
    appium_config: Dict[str, str],
    thread_id: int = 0
):
    """
    Appium 드라이버 생성 (Nox 에뮬레이터용)
    """
    options = UiAutomator2Options()
    
    # 기본 설정
    options.platform_name = "Android"
    options.device_name = appium_config["device_name"]
    
    if appium_config.get("platform_version"):
        options.platform_version = appium_config["platform_version"]
    
    options.automation_name = "UiAutomator2"
    
    # Chrome 브라우저 설정
    options.browser_name = "Chrome"
    
    # ✅ Chromedriver 자동 다운로드 설정
    options.set_capability("appium:chromedriverAutodownload", True)
    
    # ✅ Chrome 옵션 설정
    chrome_options = {
        "androidPackage": "com.android.chrome",
        "w3c": False,  # 일부 호환성 이슈 방지
    }
    
    # User-Agent 및 자동화 우회 옵션
    chrome_args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-dev-shm-usage",
        "--no-sandbox",
        "--disable-gpu",
        "--disable-notifications",
    ]
    
    if "user_agents" in profile:
        ua = random.choice(profile["user_agents"])
        chrome_args.append(f"--user-agent={ua}")
        print(f"[Driver-{thread_id}] 🎭 User-Agent: {ua[:80]}...")
    
    # Proxy 설정
    proxy_host, proxy_port, proxy_type = normalize_proxy_for_android(proxy)
    if proxy_host and proxy_port:
        if proxy_type == "http":
            proxy_str = f"{proxy_host}:{proxy_port}"
            chrome_args.append(f"--proxy-server={proxy_str}")
            print(f"[Driver-{thread_id}] 🔧 Proxy: {proxy_str}")
    
    chrome_options["args"] = chrome_args
    options.set_capability("goog:chromeOptions", chrome_options)
    
    options.no_reset = True  # 세션 유지
    options.full_reset = False
    
    with driver_creation_lock:
        try:
            print(f"[Driver-{thread_id}] ⏳ Chrome 드라이버 초기화 중 (자동 다운로드)...")
            
            driver = appium_webdriver.Remote(
                appium_config["appium_server"],
                options=options
            )
            driver.implicitly_wait(10)
            print(f"[Driver-{thread_id}] ✅ Appium 드라이버 생성 완료")
            
        except Exception as e:
            print(f"[ERR] Appium 드라이버 생성 실패: {e}")
            print(f"[ERR] Chrome 버전: 138.0.7204.179")
            print(f"[ERR] Chromedriver 자동 다운로드가 실패했을 수 있습니다.")
            return None
    
    # 자동화 감지 우회
    try:
        driver.execute_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            window.chrome = { runtime: {}, loadTimes: function() {}, csi: function() {}, app: {} };
        """)
        print(f"[Driver-{thread_id}] ✅ 자동화 우회 스크립트 주입 완료")
    except Exception as e:
        print(f"[Driver-{thread_id}] ⚠️ 스크립트 주입 실패: {e}")
    
    return driver

def create_appium_driver_old(
    profile: Dict[str, Any],
    proxy: Optional[str],
    appium_config: Dict[str, str],
    thread_id: int = 0
):
    """
    Appium 드라이버 생성 (Nox 에뮬레이터용)
    """
    options = UiAutomator2Options()
    
    options.platform_name = "Android"
    options.device_name = appium_config["device_name"]
    
    if appium_config.get("platform_version"):
        options.platform_version = appium_config["platform_version"]
    
    options.automation_name = "UiAutomator2"
    
    # ✅ 자동으로 사용 가능한 브라우저 선택
    browser = get_available_browser(appium_config["device_name"])
    options.browser_name = browser
    print(f"[Driver-{thread_id}] 🌐 브라우저: {browser}")
    
    options.no_reset = False
    options.full_reset = False
    
    # User-Agent
    if "user_agents" in profile:
        ua = random.choice(profile["user_agents"])
        ua = "Mozilla/5.0 (Linux; Android 13; SM-G998N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36"
        options.set_capability("chromedriverArgs", [f"--user-agent={ua}"])
        print(f"[Driver-{thread_id}] 🎭 User-Agent: {ua[:80]}...")
    
    # ✅ Locale 정규화 - 단순하게!
    # ✅ Locale/Language capability는 세션 생성 실패(지원되지 않는 locale 조합) 원인이 될 수 있어 제거.
    # 필요하면 아래 로그만 참고(웹 언어는 UA/Accept-Language로 처리).
    raw_locale = profile.get("locale", "en-US")
    locale = raw_locale.replace("-", "_")  # "ja-JP" -> "ja_JP"
    language = locale.split("_")[0]        # "ja_JP" -> "ja"
    print(f"[Driver-{thread_id}] 🌍 Locale(로그만): {raw_locale} -> {locale}, Language: {language}")

    
    # Proxy 설정
    proxy_host, proxy_port, proxy_type = normalize_proxy_for_android(proxy)
    ################
    proxy_type = "socks5"
    proxy_host = "192.252.208.67"
    proxy_port = "14287"
    ##################    
    if proxy_host and proxy_port:
        if proxy_type == "http":
            proxy_str = f"{proxy_host}:{proxy_port}"
            options.set_capability("proxy", {
                "proxyType": "manual",
                "httpProxy": proxy_str,
                "sslProxy": proxy_str,
            })
            print(f"[Driver-{thread_id}] 🔧 Proxy: {proxy_str}")
        elif proxy_type.startswith("socks"):
            options.set_capability("proxy", {
                "proxyType": "manual",
                "socksProxy": f"{proxy_host}:{proxy_port}",
                "socksVersion": 5,
            })
            print(f"[Driver-{thread_id}] 🔧 Proxy (SOCKS5): {proxy_host}:{proxy_port}")
    
    # Chrome 옵션
    chrome_options = {
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-gpu",
        ],
        "prefs": {
            "profile.default_content_setting_values.notifications": 2,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
        }
    }
    options.set_capability("chromeOptions", chrome_options)
    
    with driver_creation_lock:
        try:
            driver = appium_webdriver.Remote(
                appium_config["appium_server"],
                options=options
            )
            driver.implicitly_wait(10)
            print(f"[Driver-{thread_id}] ✅ Appium 드라이버 생성 완료")
            
        except Exception as e:
            print(f"[ERR] Appium 드라이버 생성 실패: {e}")
            return None
    
    # 자동화 감지 우회
    try:
        driver.execute_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            window.chrome = { runtime: {}, loadTimes: function() {}, csi: function() {}, app: {} };
        """)
        print(f"[Driver-{thread_id}] ✅ 자동화 우회 스크립트 주입 완료")
    except Exception as e:
        print(f"[Driver-{thread_id}] ⚠️ 스크립트 주입 실패: {e}")
    
    return driver



# ===================== 페이지 로딩/에러 감지 =====================
def _page_really_ready(driver):
    try:
        ready = driver.execute_script("return document.readyState") == "complete"
        if not ready:
            return False

        is_error = driver.execute_script("""
            const href = window.location.href || '';
            const text = document.body ? document.body.innerText : '';
            
            if (href.startsWith('chrome-error://')) return true;
            if (text.includes('ERR_TIMED_OUT') || text.includes('ERR_CONNECTION_TIMED_OUT')) return true;
            if (text.includes("This site can't be reached")) return true;
            
            return false;
        """)
        
        return not is_error
    except Exception:
        return False

def ensure_page_ready(driver, timeout=120):
    try:
        WebDriverWait(driver, timeout).until(_page_really_ready)
        return True
    except (TimeoutException, WebDriverException):
        return False

# ===================== 유튜브 동의 페이지 처리 =====================
def click_youtube_consent_accept_all(driver, timeout=8):
    try:
        url = driver.current_url
        host = urlparse(url).hostname or ""
        if "consent.youtube.com" not in host:
            return False

        btn = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable(
                (AppiumBy.CSS_SELECTOR, "form[action='https://consent.youtube.com/save'] button[jsname='b3VHJd']")
            )
        )
        btn.click()
        print("[Consent] ✅ 유튜브 동의 '모두 수락' 버튼 자동 클릭 완료")
        return True

    except (TimeoutException, NoSuchElementException):
        return False
    except Exception as e:
        print(f"[Consent] ⚠️ 예외 발생: {e}")
        return False

def is_driver_alive(driver) -> bool:
    try:
        driver.current_url
        return True
    except (InvalidSessionIdException, NoSuchWindowException, WebDriverException):
        return False

def smart_wait(driver, stop_event, timeout: float, index: int, check_interval: float = 0.5) -> bool:
    end = time.time() + max(0.0, float(timeout))

    while True:
        if stop_event.is_set():
            return False

        if not is_driver_alive(driver):
            print(f"[Bot-{index}] 🛑 세션 종료 감지 -> 대기 중단")
            return False

        remaining = end - time.time()
        if remaining <= 0:
            return True

        stop_event.wait(timeout=min(check_interval, remaining))

# ===================== 메인 워커 =====================
def monitor_service(
    url: str,
    proxy_member: str,
    index: int,
    appium_config: Dict[str, str],
    stop_event: threading.Event,
    redis_client: Optional[redis.Redis] = None,
):
    driver = None
    session_ok = False

    try:
        if not REGION_PROFILES:
            print(f"[Bot-{index}] ❌ REGION_PROFILES가 비어 있습니다.")
            return

        region = random.choice(list(REGION_PROFILES.keys()))
        profile = REGION_PROFILES[region]

        print(f"\n[Bot-{index}] 🌍 Profile: {region} ({profile['timezone']})")
        print(f"[Bot-{index}] 🧩 Proxy(leased): {proxy_member}")
        print(f"[Bot-{index}] 📱 Device: {appium_config['device_name']}")

        if stop_event.is_set():
            print(f"[Bot-{index}] 🛑 시작 전 중단 신호 수신. 종료.")
            return

        driver = create_appium_driver(profile, proxy_member, appium_config, index)
        if not driver:
            print(f"[Bot-{index}] ❌ 드라이버 생성 실패.")
            return

        # 타겟 페이지 접속
        print(f"[Bot-{index}] 접속 요청: {url}")
        browse_start = time.time()
        hard_deadline = browse_start + BROWSE_MAX_SECONDS

        pre_nav_delay = random.uniform(1.0, 3.0)
        print(f"[Bot-{index}] ⏳ 접속 전 {pre_nav_delay:.1f}초 대기...")
        time.sleep(pre_nav_delay)

        try:
            driver.get(url)
            click_youtube_consent_accept_all(driver)

        except TimeoutException:
            print(f"[Bot-{index}] ⚠️ Get 요청 타임아웃")

        remaining_for_load = hard_deadline - time.time()
        if remaining_for_load <= 0:
            print(f"[Bot-{index}] ⏰ 브라우징 최대 시간 도달. 세션 종료.")
            return

        if not ensure_page_ready(driver, timeout=min(ENSURE_TIMEOUT, max(5, remaining_for_load))):
            print(f"[Bot-{index}] ❌ 페이지 로딩 실패로 종료.")
            return

        session_ok = True

        remaining = hard_deadline - time.time()
        if remaining <= 0:
            print(f"[Bot-{index}] ⏰ 브라우징 최대 시간 도달. 세션 종료.")
            return

        reaction_time = min(random.uniform(0.8, 2.5), remaining)
        if reaction_time > 0:
            print(f"[Bot-{index}] ✅ 로딩 완료. 인지 반응 대기: {reaction_time:.2f}초")
            stop_event.wait(timeout=reaction_time)

        if stop_event.is_set():
            print(f"[Bot-{index}] 🛑 중단 신호. 종료.")
            return

        remaining = hard_deadline - time.time()
        if remaining <= 0:
            print(f"[Bot-{index}] ⏰ 브라우징 최대 시간 도달. 세션 종료.")
            return

        stay_time = max(10, random.gauss(STAY_DURATION, 10))
        stay_time = min(stay_time, remaining)

        action_offset = 15.0

        if stay_time <= action_offset:
            print(f"[Bot-{index}] 체류 시작 (총 {stay_time:.1f}초, 즉시 휴먼 이벤트 실행)")
            human_scroll(driver)
            human_tap(driver)
            if not smart_wait(driver, stop_event, stay_time, index):
                return
        else:
            pre_wait = stay_time - action_offset
            print(f"[Bot-{index}] 체류 시작 (총 {stay_time:.1f}초, {pre_wait:.1f}초 후 휴먼 이벤트)")
            if not smart_wait(driver, stop_event, pre_wait, index):
                return
            if stop_event.is_set():
                return
            
            human_scroll(driver)
            human_tap(driver)
            
            remaining2 = hard_deadline - time.time()
            tail = min(action_offset, max(0, remaining2))
            if tail > 0:
                if not smart_wait(driver, stop_event, tail, index):
                    return

        print(f"[Bot-{index}] 모니터링 정상 종료.")

    except Exception as e:
        print(f"[Bot-{index}] 🛑 오류 발생: {e.__class__.__name__}: {e}")

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

        if redis_client and proxy_member:
            if session_ok:
                reset_fail(redis_client, proxy_member)
                release_proxy(redis_client, proxy_member, cooldown_seconds=COOLDOWN_SUCCESS)
                print(f"[Bot-{index}] 🔓 proxy released (ok): {proxy_member}")
            else:
                fails = inc_fail(redis_client, proxy_member)
                if fails >= MAX_FAIL:
                    ban_proxy(redis_client, proxy_member)
                    print(f"[Bot-{index}] ⛔ proxy banned (fails={fails}): {proxy_member}")
                else:
                    cooldown = COOLDOWN_FAIL_BASE + random.randint(0, max(0, COOLDOWN_FAIL_JITTER))
                    release_proxy(redis_client, proxy_member, cooldown_seconds=cooldown)
                    print(f"[Bot-{index}] 🔓 proxy released (fail={fails}, cooldown={cooldown}s): {proxy_member}")

# ===================== 메인 (워커 스케줄러) =====================
if __name__ == "__main__":
    print(f"=== 🛡️ Appium 기반 Nox Monitor Started (TARGET_URL: {TARGET_URL}) ===")
    print(f"[MAIN] 📱 Nox 에뮬레이터 수: {NUM_BROWSERS}")

    if not REGION_PROFILES:
        print("[MAIN] ❌ REGION_PROFILES가 비어 있습니다. region_profiles.json 상태를 확인하세요.")
        exit(1)

    if len(APPIUM_CONFIGS) < NUM_BROWSERS:
        print(f"[MAIN] ⚠️ APPIUM_CONFIGS({len(APPIUM_CONFIGS)})가 NUM_BROWSERS({NUM_BROWSERS})보다 적습니다.")
        exit(1)

    r = get_redis()

    threads: list[threading.Thread] = []
    worker_index = 0

    try:
        while not stop_event.is_set():
            # 1) 죽은 스레드 정리
            alive_threads = [t for t in threads if t.is_alive()]
            if len(alive_threads) != len(threads):
                print(f"[MAIN] 🔄 스레드 정리: {len(threads)} → {len(alive_threads)} alive")
            threads = alive_threads

            capacity = max(0, NUM_BROWSERS - len(threads))

            # 2) 여유 슬롯만큼 새 워커 생성 시도
            no_proxy_available = False
            for slot in range(capacity):
                if stop_event.is_set():
                    break

                proxy_member = claim_proxy(r, lease_seconds=LEASE_SECONDS, reclaim_limit=200, sample_k=50)
                if not proxy_member:
                    no_proxy_available = True
                    print("[MAIN] ⚠️ 사용할 프록시가 없습니다. collector가 채울 때까지 대기.")
                    break

                log_proxy_used(r, proxy_member)

                idx = worker_index
                worker_index += 1
                
                # Nox 에뮬레이터 슬롯 할당 (순환)
                appium_config = APPIUM_CONFIGS[slot % len(APPIUM_CONFIGS)]
                
                # URL 번갈아가며 사용
                url = TARGET_URL if (idx % 2 == 0) else TARGET_URL1

                print(f"[MAIN] ▶ 새 워커 Bot-{idx} 시작, 프록시: {proxy_member}, Device: {appium_config['device_name']}")
                t = threading.Thread(
                    target=monitor_service,
                    args=(url, proxy_member, idx, appium_config, stop_event, r),
                )
                t.start()
                threads.append(t)

                time.sleep(random.uniform(5, 15))

            # 3) 프록시도 없고, 돌고 있는 스레드도 없으면 → 길게 대기
            if no_proxy_available and not threads:
                print(f"[MAIN] ⚠️ 프록시 없음 + 활성 워커 0 ⇒ {WAIT_WHEN_NO_PROXY_SECONDS}초 대기 후 재시도.")
                for _ in range(WAIT_WHEN_NO_PROXY_SECONDS):
                    if stop_event.is_set():
                        break
                    time.sleep(1)
            else:
                time.sleep(2)

    except KeyboardInterrupt:
        print("\n[MAIN] Ctrl+C (KeyboardInterrupt) 수신. Graceful Shutdown 시작.")
        stop_event.set()

    finally:
        for t in threads:
            if t.is_alive():
                t.join(timeout=10)

        print("\n=== ✅ 모든 작업 완료 및 정리 완료 ===")