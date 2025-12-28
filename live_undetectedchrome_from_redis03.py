import random
import threading
import time
import tempfile
import os
import shutil
import json
import gc
import psutil
from typing import Dict, Any, Optional
from live_human_events import HumanEvent

# 외부 라이브러리
import numpy as np
import redis
import undetected_chromedriver as uc

from selenium.webdriver.common.by import By
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

# ===================== 성능 모니터링 추가 =====================
class ResourceMonitor:
    """시스템 리소스 모니터링 클래스"""
    
    @staticmethod
    def get_process_info():
        """현재 프로세스의 메모리/CPU 사용량"""
        try:
            process = psutil.Process()
            return {
                'memory_mb': process.memory_info().rss / 1024 / 1024,
                'cpu_percent': process.cpu_percent(interval=0.1),
                'num_threads': process.num_threads(),
            }
        except:
            return None
    
    @staticmethod
    def check_resource_limits():
        """리소스 임계값 체크"""
        try:
            memory = psutil.virtual_memory()
            cpu = psutil.cpu_percent(interval=0.1)
            
            # 메모리 80% 이상 또는 CPU 90% 이상이면 경고
            if memory.percent > 80 or cpu > 90:
                return False, f"⚠️ 리소스 부족: RAM {memory.percent:.1f}%, CPU {cpu:.1f}%"
            return True, None
        except:
            return True, None

# ===================== 전역 설정 =====================
driver_creation_lock = threading.Lock()
stop_event = threading.Event()

# Redis 설정
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None

REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_LEASE = "proxies:lease"
REDIS_HASH_FAIL = "proxies:fail"
REDIS_ZSET_USED = "proxies:used_recent"

def get_redis() -> redis.Redis:
    """Redis 연결 (connection pool 사용)"""
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
        max_connections=20,  # ✅ connection pool 크기 제한
        socket_keepalive=True,
        socket_keepalive_options={
            1: 1,  # TCP_KEEPIDLE
            2: 1,  # TCP_KEEPINTVL
            3: 3,  # TCP_KEEPCNT
        },
    )

# Lua 스크립트들
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

def claim_proxy(r: redis.Redis, lease_seconds: int, reclaim_limit: int = 200, sample_k: int = 50) -> Optional[str]:
    now = int(time.time())
    rand_int = random.randint(0, 2_147_483_647)
    try:
        member = r.eval(_LUA_CLAIM, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, now, int(lease_seconds), int(reclaim_limit), int(sample_k), int(rand_int))
    except redis.RedisError as e:
        print(f"[REDIS] claim_proxy 실패: {e}")
        return None

    if not member or "://" not in member:
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

# ===================== Region Profiles =====================
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
TARGET_URL = "https://youtube.com/shorts/eewyMV23vXg?feature=share" #새해인사
TARGET_URL1 = "https://youtube.com/shorts/eewyMV23vXg?feature=share"    #크리스마스 2
TARGET_URL = "https://youtube.com/shorts/eewyMV23vXg?feature=share"    

COMMAND_TIMEOUT = 180  # ✅ 300 -> 180 감소
LOAD_TIMEOUT = COMMAND_TIMEOUT
ENSURE_TIMEOUT = 240  # ✅ 420 -> 240 감소
BROWSE_MAX_SECONDS = ENSURE_TIMEOUT
STAY_DURATION = 90  # ✅ 120 -> 90 감소
WINDOW_WIDTH = 800
WINDOW_HEIGHT = 700
NUM_BROWSERS = 2
HEADLESS = False

HUMAN_EVENT_BEFORE_END_SECONDS = 30
WAIT_WHEN_NO_PROXY_SECONDS = 60

SCREEN_WIDTH = WINDOW_WIDTH * NUM_BROWSERS + 40 * (NUM_BROWSERS - 1) - 200
SCREEN_HEIGHT = WINDOW_HEIGHT + 100 - 200

LEASE_SECONDS = max(120, int(ENSURE_TIMEOUT + STAY_DURATION + 60))  # ✅ 여유 시간 감소

COOLDOWN_SUCCESS = 0
COOLDOWN_FAIL_BASE = 30
COOLDOWN_FAIL_JITTER = 60
MAX_FAIL = 5

# ✅ 리소스 관리 설정
MAX_MEMORY_MB = 2000  # 슬롯당 최대 메모리 (MB)
CLEANUP_INTERVAL = 300  # 5분마다 정리
RESOURCE_CHECK_INTERVAL = 30  # 30초마다 리소스 체크

# ===================== 브라우저 데이터 초기화 =====================
def reset_browser_data_in_session(driver):
    try:
        current_url = driver.current_url
        if not current_url or current_url == "data:,":
            try:
                driver.get("about:blank")
            except:
                print("   [Reset] ⚠️ about:blank 이동 실패, 초기화 스킵")
                return False

        try:
            driver.delete_all_cookies()
        except WebDriverException:
            pass

        try:
            driver.execute_script("window.localStorage.clear();")
        except WebDriverException:
            pass

        try:
            driver.execute_script("window.sessionStorage.clear();")
        except WebDriverException:
            pass

        print("   [Reset] 🧹 쿠키, 로컬/세션 스토리지를 세션 내에서 초기화했습니다.")
        return True

    except Exception as e:
        print(f"   [Reset] ⚠️ 데이터 초기화 중 예외 발생: {e.__class__.__name__}")
        return False

# ===================== Proxy 정규화 =====================
def normalize_proxy_for_chrome(proxy: Optional[str]) -> Optional[str]:
    if not proxy:
        return proxy
    p = proxy.strip()

    if p.startswith("https://"):
        return "http://" + p[len("https://"):]

    if p.startswith("socks://"):
        return "socks5://" + p[len("socks://"):]

    return p

# ===================== 창 위치 계산 =====================
def calculate_window_position(slot_index: int, total_slots: int = NUM_BROWSERS):
    if total_slots <= 3:
        cols, rows = total_slots, 1
    elif total_slots <= 4:
        cols, rows = 2, 2
    elif total_slots <= 6:
        cols, rows = 3, 2
    else:
        cols = 3
        rows = (total_slots + 2) // 3
    
    window_width = SCREEN_WIDTH // cols
    window_height = SCREEN_HEIGHT // rows
    row = slot_index // cols
    col = slot_index % cols
    
    return {
        'x': col * window_width,
        'y': row * window_height,
        'width': window_width,
        'height': window_height
    }

# ===================== Driver 생성 (최적화) =====================
def create_undetected_driver(profile: Dict[str, Any], proxy: Optional[str], slot_index: int = 0):
    """
    ✅ 최적화된 드라이버 생성
    - 불필요한 기능 비활성화
    - 메모리 사용량 감소
    """
    options = uc.ChromeOptions()

    temp_dir = tempfile.mkdtemp(prefix=f"monitor_slot_{slot_index}_")
    options.add_argument(f"--user-data-dir={temp_dir}")
    
    if "user_agents" in profile:
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
        options.add_argument(f"--user-agent={ua}")
    
    options.add_argument(f"--timezone-id={profile['timezone']}")
    options.add_argument(f"--lang={profile['locale']}")

    prefs = {
        "profile.default_content_setting_values.notifications": 2,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
        "webrtc.ip_handling_policy": "disable_non_proxied_udp",
        "webrtc.multiple_routes_enabled": False,
        "webrtc.nonproxied_udp_enabled": False,
        "intl.accept_languages": random.choice(profile["accept_languages"]),
        # ✅ 성능 최적화 옵션 추가
        "profile.default_content_setting_values.images": 2,  # 이미지 차단 (선택적)
        "profile.managed_default_content_settings.media_stream": 2,  # 미디어 스트림 차단
    }
    options.add_experimental_option("prefs", prefs)
    
    # ✅ 성능 최적화 옵션들
    options.add_argument("--disable-quic")
    options.add_argument("--disable-features=NetworkService,NetworkServiceInProcess")
    options.add_argument("--disable-gpu")  # GPU 비활성화
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-sync")
    options.add_argument("--metrics-recording-only")
    options.add_argument("--mute-audio")  # 오디오 음소거
    options.add_argument("--no-default-browser-check")
    options.add_argument("--no-first-run")
    options.add_argument("--disable-hang-monitor")
    options.add_argument("--disable-prompt-on-repost")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-ipc-flooding-protection")
    options.add_argument("--disable-client-side-phishing-detection")
    options.add_argument("--disable-component-update")
    options.add_argument("--disable-domain-reliability")
    
    # ✅ 메모리 관리
    options.add_argument(f"--max-old-space-size={MAX_MEMORY_MB}")
    options.add_argument("--js-flags=--max-old-space-size=512")

    options.add_argument("--homepage=about:blank")
    options.add_argument("about:blank")

    if HEADLESS:
        options.add_argument("--headless=new")
    
    if proxy:
        proxy_for_chrome = normalize_proxy_for_chrome(proxy)
        options.add_argument(f"--proxy-server={proxy_for_chrome}")

    options.add_argument("--disable-blink-features=AutomationControlled")
    
    pos = calculate_window_position(slot_index)
    options.add_argument(f"--window-position={pos['x']},{pos['y']}")
    options.add_argument(f"--window-size={pos['width']},{pos['height']}")
    
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")

    with driver_creation_lock:
        try:
            driver = uc.Chrome(
                options=options,
                use_subprocess=True,
                command_executor_process_timeout=COMMAND_TIMEOUT,
            )
            driver.command_executor.set_timeout(COMMAND_TIMEOUT)
            driver.set_page_load_timeout(LOAD_TIMEOUT)
            
            driver.set_window_size(
                pos['width'] + random.randint(-50, 50),
                pos['height'] + random.randint(-50, 50),
            )

        except Exception as e:
            print(f"[ERR] Driver creation failed (Slot-{slot_index}): {e}")
            try:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
            except:
                pass
            return None, None

    # CDP 스크립트 주입
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                    Object.defineProperty(navigator, 'languages', { get: () => ['ko-KR', 'ko', 'en-US', 'en'] });
                    
                    window.chrome = { 
                        runtime: {},
                        loadTimes: function() {},
                        csi: function() {},
                        app: {}
                    };
                    
                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ?
                            Promise.resolve({ state: Notification.permission }) :
                            originalQuery(parameters)
                    );
                    
                    console.debug = () => {};
                """
            },
        )
        print(f"[Driver-Slot{slot_index}] ✅ 자동화 감지 우회 스크립트 주입 완료")
        
    except Exception as e:
        print(f"[Driver-Slot{slot_index}] ⚠️ CDP 스크립트 주입 실패: {e}")

    return driver, temp_dir

# ===================== 페이지 로딩/에러 감지 =====================
def _page_really_ready(driver):
    ready = driver.execute_script("return document.readyState") == "complete"
    if not ready:
        return False

    bodies = driver.find_elements(By.TAG_NAME, "body")
    if not bodies or not any(b.is_displayed() for b in bodies):
        return False

    is_error = driver.execute_script(
        """
        const href  = window.location.href || '';
        const text  = document.body ? document.body.innerText : '';

        if (href.startsWith('chrome-error://')) return true;
        if (text.includes('ERR_TIMED_OUT') || text.includes('ERR_CONNECTION_TIMED_OUT')) return true;
        if (text.includes("This site can't be reached")) return true;
        if (text.includes("사이트에 연결할 수 없음") || text.includes("사이트에 접속할 수 없습니다")) return true;

        return false;
    """
    )
    if is_error:
        return False
    return True

def ensure_page_ready(driver, timeout=120):
    try:
        WebDriverWait(driver, timeout).until(_page_really_ready)
        return True
    except (TimeoutException, WebDriverException):
        return False

# ===================== YouTube 동의 페이지 =====================
from urllib.parse import urlparse

def click_youtube_consent_accept_all(driver, timeout=8):
    try:
        url = driver.current_url
        host = urlparse(url).hostname or ""
        if "consent.youtube.com" not in host:
            return False

        forms = driver.find_elements(By.CSS_SELECTOR, "form[action='https://consent.youtube.com/save']")
        if not forms:
            return False

        btn = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "form[action='https://consent.youtube.com/save'] button[jsname='b3VHJd']")
            )
        )
        btn.click()
        print("[Consent] ✅ 유튜브 동의 '모두 수락' 버튼 자동 클릭 완료")
        return True

    except (TimeoutException, NoSuchElementException):
        return False
    except Exception as e:
        print(f"[Consent] ⚠ 예외 발생: {e}")
        return False

def is_driver_alive(driver) -> bool:
    try:
        handles = driver.window_handles
        if not handles:
            return False
        driver.execute_script("return 1;")
        return True
    except (InvalidSessionIdException, NoSuchWindowException, WebDriverException):
        return False

def smart_wait(driver, stop_event, timeout: float, index: int, check_interval: float = 0.5) -> bool:
    end = time.time() + max(0.0, float(timeout))

    while True:
        if stop_event.is_set():
            return False

        if not is_driver_alive(driver):
            print(f"[Bot-{index}] 🛑 브라우저/세션 종료 감지 -> 대기 중단")
            return False

        remaining = end - time.time()
        if remaining <= 0:
            return True

        stop_event.wait(timeout=min(check_interval, remaining))

CHROME_ERROR_URL_PREFIXES = ("chrome-error://", "chrome://error")
ERROR_TEXT_MARKERS = ("This site can't be reached", "ERR_TIMED_OUT", "net::ERR_")

def _page_looks_like_error(driver) -> bool:
    try:
        cur = (driver.current_url or "").lower()
        if any(cur.startswith(p) for p in CHROME_ERROR_URL_PREFIXES):
            return True
    except Exception:
        pass

    try:
        body = driver.find_element(By.TAG_NAME, "body")
        txt = (body.text or "")
        if any(m in txt for m in ERROR_TEXT_MARKERS):
            return True
    except Exception:
        pass

    return False

def safe_get(driver, url: str, index: int, page_load_timeout: float = 30.0) -> bool:
    try:
        driver.set_page_load_timeout(page_load_timeout)
    except Exception:
        pass

    try:
        driver.get(url)
    except TimeoutException:
        print(f"[Bot-{index}] ⚠️ pageLoadTimeout 발생")
        return False
    except WebDriverException as e:
        msg = str(e)
        if "net::ERR_" in msg or "timeout" in msg.lower():
            print(f"[Bot-{index}] ⚠️ WebDriverException: {msg[:160]}")
            return False
        return False

    if _page_looks_like_error(driver):
        print(f"[Bot-{index}] ⚠️ 에러 페이지 감지")
        return False

    return True

# ===================== 메인 워커 (최적화) =====================
def monitor_service(
    url: str,
    proxy_member: str,
    slot_index: int,
    stop_event: threading.Event,
    redis_client: Optional[redis.Redis] = None,
):
    """
    ✅ 최적화된 워커 함수
    - 리소스 모니터링 추가
    - 메모리 관리 강화
    """
    driver = None
    temp_dir = None
    session_ok = False
    start_time = time.time()

    try:
        # ✅ 리소스 체크
        resource_ok, msg = ResourceMonitor.check_resource_limits()
        if not resource_ok:
            print(f"[Slot-{slot_index}] {msg}")
            return

        if not REGION_PROFILES:
            print(f"[Slot-{slot_index}] ❌ REGION_PROFILES가 비어 있습니다.")
            return

        region = random.choice(list(REGION_PROFILES.keys()))
        profile = REGION_PROFILES[region]

        print(f"\n[Slot-{slot_index}] 🌍 Profile: {region}")
        print(f"[Slot-{slot_index}] 🧩 Proxy: {proxy_member}")

        if stop_event.is_set():
            return
        
        driver, temp_dir = create_undetected_driver(profile, proxy_member, slot_index)
        if not driver:
            print(f"[Slot-{slot_index}] ❌ 드라이버 생성 실패")
            return

        # 초기 페이지
        try:
            driver.get("about:blank")
        except Exception as e:
            print(f"[Slot-{slot_index}] ⚠️ 초기 페이지 로드 실패: {e}")
            return

        reset_browser_data_in_session(driver)

        # Referer 설정
        referer = random.choice(profile["referers"])
        try:
            driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": {"Referer": referer}})
        except Exception:
            pass

        # 랜덤 대기
        pre_nav_delay = random.uniform(1.0, 2.0)  # ✅ 3.0 -> 2.0 감소
        time.sleep(pre_nav_delay)

        # 타겟 페이지 접속
        print(f"[Slot-{slot_index}] 접속 요청: {url}")
        browse_start = time.time()
        hard_deadline = browse_start + BROWSE_MAX_SECONDS

        try:
            driver.get(url)
            click_youtube_consent_accept_all(driver)
        except TimeoutException:
            print(f"[Slot-{slot_index}] ⚠️ Get 요청 타임아웃")

        remaining_for_load = hard_deadline - time.time()
        if remaining_for_load <= 0:
            return

        if not ensure_page_ready(driver, timeout=min(ENSURE_TIMEOUT, max(5, remaining_for_load))):
            print(f"[Slot-{slot_index}] ❌ 페이지 로딩 실패")
            return

        session_ok = True

        remaining = hard_deadline - time.time()
        if remaining <= 0:
            return

        if stop_event.is_set():
            return

        stay_time = max(10, random.gauss(STAY_DURATION, 10))
        stay_time = min(stay_time, remaining)

        human_event_timing = min(HUMAN_EVENT_BEFORE_END_SECONDS, stay_time - HUMAN_EVENT_BEFORE_END_SECONDS)
        human_event = HumanEvent(driver)

        if human_event_timing <= 5:
            print(f"[Slot-{slot_index}] 체류 시작 (즉시 휴먼 이벤트)")
            human_event.execute_random_action()
            if not smart_wait(driver, stop_event, 10, slot_index):
                return
        else:
            print(f"[Slot-{slot_index}] 체류 시작 (총 {stay_time:.1f}초)")
            
            if not smart_wait(driver, stop_event, human_event_timing, slot_index):
                return
            if stop_event.is_set():
                return

            human_event.execute_random_action()
            
            if not smart_wait(driver, stop_event, 20, slot_index):
                return

        # ✅ 세션 정보 출력
        elapsed = time.time() - start_time
        info = ResourceMonitor.get_process_info()
        if info:
            print(f"[Slot-{slot_index}] 📊 세션 완료: {elapsed:.1f}초, 메모리: {info['memory_mb']:.1f}MB")

    except Exception as e:
        print(f"[Slot-{slot_index}] 🛑 오류 발생: {e.__class__.__name__}: {e}")

    finally:
        # ✅ 드라이버 정리
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
            
            # ✅ 명시적 메모리 정리
            driver = None
            gc.collect()

        time.sleep(2)

        # ✅ 임시 디렉토리 정리
        if temp_dir and os.path.exists(temp_dir):
            for attempt in range(3):
                try:
                    shutil.rmtree(temp_dir)
                    break
                except PermissionError:
                    if attempt < 2:
                        print(f"[Slot-{slot_index}] ⚠️ 삭제 재시도 {attempt + 1}/3")
                        time.sleep(2)
                    else:
                        print(f"[Slot-{slot_index}] ⚠️ 임시 디렉토리 삭제 최종 실패")
                except Exception as e:
                    print(f"[Slot-{slot_index}] ⚠️ 임시 디렉토리 삭제 실패: {e}")
                    break

        # ✅ Redis 프록시 반환
        if redis_client and proxy_member:
            if session_ok:
                reset_fail(redis_client, proxy_member)
                release_proxy(redis_client, proxy_member, cooldown_seconds=COOLDOWN_SUCCESS)
                print(f"[Slot-{slot_index}] 🔓 proxy released (ok): {proxy_member}")
            else:
                fails = inc_fail(redis_client, proxy_member)
                if fails >= MAX_FAIL:
                    ban_proxy(redis_client, proxy_member)
                    print(f"[Slot-{slot_index}] ⛔ proxy banned (fails={fails}): {proxy_member}")
                else:
                    cooldown = COOLDOWN_FAIL_BASE + random.randint(0, max(0, COOLDOWN_FAIL_JITTER))
                    release_proxy(redis_client, proxy_member, cooldown_seconds=cooldown)
                    print(f"[Slot-{slot_index}] 🔓 proxy released (fail={fails}, cooldown={cooldown}s): {proxy_member}")

# ===================== 임시 디렉토리 정리 =====================
def cleanup_temp_dirs():
    """전역 임시 디렉토리 정리"""
    print("\n🧹 남은 임시 파일 확인 중...")
    cleaned = 0
    failed = 0
    try:
        temp_base = tempfile.gettempdir()
        for item in os.listdir(temp_base):
            if item.startswith("monitor_slot_"):
                path = os.path.join(temp_base, item)
                try:
                    if os.path.isdir(path):
                        def remove_readonly(func, path, exc_info):
                            os.chmod(path, 0o777)
                            func(path)
                        shutil.rmtree(path, onerror=remove_readonly)
                        cleaned += 1
                except Exception:
                    failed += 1
                    pass
    except Exception:
        pass

    if cleaned > 0:
        print(f"   ✅ {cleaned}개 디렉토리 정리 완료")
    if failed > 0:
        print(f"   ⚠️ {failed}개 디렉토리 정리 실패 (재부팅 후 수동 삭제 권장)")
    if cleaned == 0 and failed == 0:
        print(f"   ✅ 정리할 항목 없음")

import atexit
atexit.register(cleanup_temp_dirs)

# ===================== 리소스 모니터링 스레드 =====================
def resource_monitor_thread(stop_event: threading.Event):
    """
    ✅ 주기적으로 시스템 리소스를 체크하는 백그라운드 스레드
    """
    print("[Monitor] 📊 리소스 모니터링 스레드 시작")
    
    while not stop_event.is_set():
        try:
            info = ResourceMonitor.get_process_info()
            if info:
                # 메모리가 과도하게 높으면 경고
                if info['memory_mb'] > 3000:  # 3GB 이상
                    print(f"[Monitor] ⚠️ 높은 메모리 사용: {info['memory_mb']:.1f}MB, 스레드: {info['num_threads']}")
                    # 강제 가비지 컬렉션
                    gc.collect()
            
            resource_ok, msg = ResourceMonitor.check_resource_limits()
            if not resource_ok:
                print(f"[Monitor] {msg}")
                # 심각한 경우 전체 중단도 고려 가능
                # stop_event.set()
            
        except Exception as e:
            print(f"[Monitor] 리소스 체크 오류: {e}")
        
        # 30초마다 체크
        stop_event.wait(timeout=RESOURCE_CHECK_INTERVAL)
    
    print("[Monitor] 📊 리소스 모니터링 스레드 종료")

# ===================== 메인 (슬롯 스케줄러) =====================
if __name__ == "__main__":
    print(f"=== 🛡️ Redis 기반 Stealth Monitor Started (최적화 버전) ===")
    print(f"=== 🎯 TARGET_URL: {TARGET_URL} ===")

    if not REGION_PROFILES:
        print("[MAIN] ❌ REGION_PROFILES가 비어 있습니다. region_profiles.json 상태를 확인하세요.")
        exit(1)

    # ✅ Redis 연결 (connection pool 포함)
    r = get_redis()
    
    # ✅ 초기 리소스 상태 출력
    info = ResourceMonitor.get_process_info()
    if info:
        print(f"[MAIN] 📊 초기 상태: 메모리 {info['memory_mb']:.1f}MB, CPU {info['cpu_percent']:.1f}%")

    # ✅ 슬롯 기반 관리
    active_slots: Dict[int, threading.Thread] = {}
    
    # ✅ 리소스 모니터링 스레드 시작
    monitor_thread = threading.Thread(
        target=resource_monitor_thread,
        args=(stop_event,),
        daemon=True,
        name="ResourceMonitor"
    )
    monitor_thread.start()

    try:
        iteration = 0
        last_cleanup = time.time()
        
        while not stop_event.is_set():
            iteration += 1
            
            # ✅ 주기적 메모리 정리 (5분마다)
            if time.time() - last_cleanup > CLEANUP_INTERVAL:
                print(f"\n[MAIN] 🧹 주기적 메모리 정리 실행 (iteration: {iteration})")
                gc.collect()
                last_cleanup = time.time()
                
                # 리소스 상태 출력
                info = ResourceMonitor.get_process_info()
                if info:
                    print(f"[MAIN] 📊 현재 상태: 메모리 {info['memory_mb']:.1f}MB, 스레드 {info['num_threads']}개")
            
            # 1) 종료된 스레드 정리
            for slot in list(active_slots.keys()):
                if not active_slots[slot].is_alive():
                    del active_slots[slot]
                    print(f"[MAIN] 🔄 슬롯-{slot} 정리 완료 (스레드 종료)")
                    # ✅ 슬롯 종료 후 메모리 정리
                    gc.collect()

            # 2) 빈 슬롯 채우기
            for slot in range(NUM_BROWSERS):
                if slot not in active_slots and not stop_event.is_set():
                    # ✅ 리소스 체크
                    resource_ok, msg = ResourceMonitor.check_resource_limits()
                    if not resource_ok:
                        print(f"[MAIN] {msg} - 새 슬롯 생성 대기")
                        time.sleep(30)
                        break
                    
                    # 프록시 가져오기
                    proxy_member = claim_proxy(r, lease_seconds=LEASE_SECONDS, reclaim_limit=200, sample_k=50)
                    if not proxy_member:
                        print(f"[MAIN] ⚠️ 사용 가능한 프록시 없음, 대기 중...")
                        time.sleep(WAIT_WHEN_NO_PROXY_SECONDS)
                        break

                    log_proxy_used(r, proxy_member)

                    # URL 선택
                    url = TARGET_URL if slot % 2 == 0 else TARGET_URL1

                    print(f"[MAIN] ▶ 슬롯-{slot} 시작, 프록시: {proxy_member}")
                    
                    # 스레드 생성
                    t = threading.Thread(
                        target=monitor_service,
                        args=(url, proxy_member, slot, stop_event, r),
                        daemon=True,
                        name=f"Slot-{slot}"
                    )
                    t.start()
                    active_slots[slot] = t

                    # 슬롯 생성 간격
                    time.sleep(random.uniform(5, 15))

            # 3) 메인 루프 대기
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n[MAIN] Ctrl+C (KeyboardInterrupt) 수신. Graceful Shutdown 시작.")
        stop_event.set()

    finally:
        # ✅ 모든 슬롯의 스레드 종료 대기
        print(f"\n[MAIN] 🛑 모든 슬롯 종료 대기 중... (활성 슬롯: {len(active_slots)}개)")
        for slot, t in active_slots.items():
            if t.is_alive():
                print(f"[MAIN] ⏳ 슬롯-{slot} 종료 대기...")
                t.join(timeout=10)

        # ✅ 리소스 모니터 스레드 종료 대기
        if monitor_thread.is_alive():
            print(f"[MAIN] ⏳ 리소스 모니터 스레드 종료 대기...")
            monitor_thread.join(timeout=5)

        # ✅ 최종 정리
        cleanup_temp_dirs()
        
        # ✅ Redis 연결 정리
        try:
            r.close()
        except:
            pass
        
        # ✅ 최종 메모리 정리
        gc.collect()
        
        # ✅ 최종 리소스 상태
        info = ResourceMonitor.get_process_info()
        if info:
            print(f"[MAIN] 📊 최종 상태: 메모리 {info['memory_mb']:.1f}MB")

        print("\n=== ✅ 모든 작업 완료 및 정리 완료 ===")
        print(f"=== 🏁 슬롯 기반 모니터 종료 ===")