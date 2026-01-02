import random
import threading
import time
import tempfile
from pathlib import Path
import os
import shutil
import json
from typing import Dict, Any, Optional
from live_human_events import HumanEvent, HumanEventMobile

# 외부 라이브러리
import numpy as np  # pip install numpy
import redis        # pip install redis
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
REDIS_ZSET_ALIVE = "proxies:alive"        # collector가 넣는 풀 (score는 next_available_epoch 권장. 0이면 즉시 사용 가능)
REDIS_ZSET_LEASE = "proxies:lease"        # client가 임대 중인 프록시 (score는 lease_expire_epoch)
REDIS_HASH_FAIL  = "proxies:fail"         # 실패 카운트 (선택)

# (옵션) 최근 사용 기록용
REDIS_ZSET_USED  = "proxies:used_recent"  # timestamp score로 기록

def get_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,  # member를 str로 다루기
    )

# --------------------- Lease Lua (원자적) ---------------------
# claim:
#  1) 만료된 lease를 alive로 회수
#  2) alive에서 (score<=now) 인 후보 중 앞쪽 sample_k개를 가져와 랜덤 1개 선택
#  3) alive -> lease로 이동
_LUA_CLAIM = r"""
local alive = KEYS[1]
local lease = KEYS[2]
local now = tonumber(ARGV[1])
local lease_sec = tonumber(ARGV[2])
local reclaim_limit = tonumber(ARGV[3])
local sample_k = tonumber(ARGV[4])
local rand_int = tonumber(ARGV[5])

-- 1) 만료된 lease 회수
local expired = redis.call('ZRANGEBYSCORE', lease, '-inf', now, 'LIMIT', 0, reclaim_limit)
for i, m in ipairs(expired) do
  redis.call('ZREM', lease, m)
  redis.call('ZADD', alive, 0, m)
end

-- 2) 사용 가능한 후보들 중 앞쪽 sample_k개
local cands = redis.call('ZRANGEBYSCORE', alive, '-inf', now, 'LIMIT', 0, sample_k)
if (not cands) or (#cands == 0) then
  return nil
end

-- 3) 랜덤 1개 선택 (rand_int를 이용해 결정)
local idx = (rand_int % #cands) + 1
local m = cands[idx]

redis.call('ZREM', alive, m)
redis.call('ZADD', lease, now + lease_sec, m)
return m
"""

# release: lease -> alive 로 이동, score = next_time(epoch)
_LUA_RELEASE = r"""
local alive = KEYS[1]
local lease = KEYS[2]
local member = ARGV[1]
local next_time = tonumber(ARGV[2])

redis.call('ZREM', lease, member)
redis.call('ZADD', alive, next_time, member)
return 1
"""

# ban: alive/lease 모두에서 제거
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
    """alive에서 프록시 1개를 임대(claim). 반환: 'proto://ip:port' or None"""
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
    """임대된 프록시를 alive로 반납(release)."""
    next_time = int(time.time()) + max(0, int(cooldown_seconds))
    try:
        r.eval(_LUA_RELEASE, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, member, next_time)
    except redis.RedisError as e:
        print(f"[REDIS] release_proxy 실패: {e}")

def ban_proxy(r: redis.Redis, member: str) -> None:
    """문제 프록시를 풀에서 제거(ban)."""
    try:
        r.eval(_LUA_BAN, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, member)
    except redis.RedisError as e:
        print(f"[REDIS] ban_proxy 실패: {e}")

def inc_fail(r: redis.Redis, member: str) -> int:
    """실패 카운트 +1"""
    try:
        return int(r.hincrby(REDIS_HASH_FAIL, member, 1))
    except redis.RedisError:
        return 1

def reset_fail(r: redis.Redis, member: str) -> None:
    """실패 카운트 초기화"""
    try:
        r.hdel(REDIS_HASH_FAIL, member)
    except redis.RedisError:
        pass

def log_proxy_used(r: redis.Redis, member: str) -> None:
    """최근 사용 기록만 남김(풀에서는 제거하지 않음)."""
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
TARGET_URL = "https://www.youtube.com/shorts/mcy0JKTavW4?feature=share" #첫눈

TARGET_URL1 = "https://youtube.com/shorts/-vVnZoVtnFk?si=d7zi4TVY49jGdSyM" #크리스마스
TARGET_URL = "https://youtube.com/shorts/u7sO-mNEpT4?si=-niEKY13Q38Nqq4W" #크리스마스 2

TARGET_URL = "https://youtube.com/shorts/eewyMV23vXg?si=vtn1a6WMt0bDcDac" #새해인사
TARGET_URL1 = "https://youtube.com/shorts/eewyMV23vXg?si=vtn1a6WMt0bDcDac" #새해인사

#TARGET_URL = "https://www.youtube.com/shorts/i2Z4NaSqCYc?feature=share" #테스트용
#TARGET_URL1 = "https://www.youtube.com/shorts/i2Z4NaSqCYc?feature=share" #테스트용


COMMAND_TIMEOUT = 300
LOAD_TIMEOUT = COMMAND_TIMEOUT
ENSURE_TIMEOUT = 420
BROWSE_MAX_SECONDS = ENSURE_TIMEOUT
STAY_DURATION = 120
WINDOW_WIDTH = 800
WINDOW_HEIGHT = 700
NUM_BROWSERS = 2
HEADLESS = False

HUMAN_EVENT_BEFORE_END_SECONDS = 30

WAIT_WHEN_NO_PROXY_SECONDS = 60

# ✅ 화면 크기 설정 (슬롯 배치용)
SCREEN_WIDTH = WINDOW_WIDTH * NUM_BROWSERS + 40 * (NUM_BROWSERS - 1) - 200 # 창 간격 40px 고려
SCREEN_HEIGHT = WINDOW_HEIGHT + 100  - 200 # 상단 여유 공간

# ---- Lease 운영 파라미터 (필요시 네가 조정) ----
LEASE_SECONDS = max(120, int(ENSURE_TIMEOUT + STAY_DURATION + 120))

COOLDOWN_SUCCESS = 0
COOLDOWN_FAIL_BASE = 30
COOLDOWN_FAIL_JITTER = 60
MAX_FAIL = 5

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

# ===================== undetected_chromedriver 생성 =====================
# ===================== Proxy 정규화 =====================
def normalize_proxy_for_chrome(proxy: Optional[str]) -> Optional[str]:
    """Redis member('proto://ip:port')를 Chrome이 잘 먹는 형태로 보정"""
    if not proxy:
        return proxy
    p = proxy.strip()

    # 흔한 케이스: https://ip:port (리스트 명칭일 뿐, 실제 프록시는 http CONNECT인 경우가 대부분)
    if p.startswith("https://"):
        return "http://" + p[len("https://") :]

    # 사용자가 가끔 쓰는 socks:// 형태 → socks5:// 로 보정
    if p.startswith("socks://"):
        return "socks5://" + p[len("socks://") :]

    return p

# ✅ 슬롯별 창 위치 계산 함수 (Playwright 버전과 동일)
def calculate_window_position(slot_index: int, width: int, height: int, total_slots: int = NUM_BROWSERS):
    """
    각 슬롯의 실제 크기를 고려한 창 위치 계산
    """
    if total_slots <= 3:
        cols, rows = total_slots, 1
    elif total_slots <= 4:
        cols, rows = 2, 2
    elif total_slots <= 6:
        cols, rows = 3, 2
    else:
        cols = 3
        rows = (total_slots + 2) // 3
    
    # ✅ 최대 크기 기준으로 그리드 계산 (여유 공간 확보)
    max_width = 450  # 모바일 최대 너비 + 여유
    max_height = 950  # 모바일 최대 높이 + 여유
    
    row = slot_index // cols
    col = slot_index % cols
    
    return {
        'x': col * max_width,
        'y': row * max_height,
        'width': width,   # 실제 디바이스 크기
        'height': height
    }
# ===================== 모바일 디바이스 정보 로드 =====================
from playwright.sync_api import sync_playwright
def load_mobile_devices():
    """Playwright의 디바이스 목록을 가져와서 모바일 기기만 필터링"""
    with sync_playwright() as p:
        devices = p.devices
        # 모바일 디바이스만 필터링 (iPhone, iPad, Pixel, Galaxy 등)
        mobile_devices = {
            name: info for name, info in devices.items()
            if any(keyword in name for keyword in ['iPhone', 'iPad', 'Pixel', 'Galaxy', 'Nexus'])
        }
    return mobile_devices

# 전역 변수로 로드
try:
    MOBILE_DEVICES = load_mobile_devices()
    print(f"[INIT] 모바일 디바이스 로드 완료. 디바이스 수: {len(MOBILE_DEVICES)}")
except Exception as e:
    print(f"[INIT] ⚠️ 모바일 디바이스 로드 실패: {e}")
    MOBILE_DEVICES = {}
    
def create_undetected_driver(profile: Dict[str, Any], proxy: Optional[str], slot_index: int = 0):
    """
    향상된 스텔스 드라이버 생성 (모바일 기기 에뮬레이션)
    ✅ slot_index 사용: 슬롯별 고유 temp_dir 및 창 위치
    ✅ Playwright 디바이스 정보로 실제 모바일 기기 에뮬레이션
    Returns: (driver, temp_dir) 튜플
    """
    options = uc.ChromeOptions()

    # ✅ 슬롯별 고유 temp_dir
    tmp_root = Path(__file__).resolve().parent / "_tmp_profiles"
    tmp_root.mkdir(parents=True, exist_ok=True)
    temp_dir = tempfile.mkdtemp(prefix=f"monitor_slot_{slot_index}_", dir=str(tmp_root))
    options.add_argument(f"--user-data-dir={temp_dir}")
    
    # ✅ 랜덤 모바일 디바이스 선택
    mobile_width = 412  # 기본값
    mobile_height = 915
    device_scale_factor = 3.0
    is_mobile = True
    
    if MOBILE_DEVICES:
        device_name = random.choice(list(MOBILE_DEVICES.keys()))
        device = MOBILE_DEVICES[device_name]
        
        # User-Agent (모바일)
        ua = device['user_agent']
        options.add_argument(f"--user-agent={ua}")
        
        # 디바이스의 실제 화면 크기
        viewport = device['viewport']
        mobile_width = viewport['width']
        mobile_height = viewport['height']
        device_scale_factor = device.get('device_scale_factor', 3.0)
        is_mobile = device.get('is_mobile', True)
        
        print(f"[Driver-Slot{slot_index}] 📱 Mobile Device: {device_name}")
        print(f"[Driver-Slot{slot_index}] 🎭 User-Agent: {ua[:80]}...")
        print(f"[Driver-Slot{slot_index}] 📐 Screen Size: {mobile_width}x{mobile_height}")
        
    else:
        # fallback: 다양한 모바일 크기 중 랜덤 선택
        print(f"[Driver-Slot{slot_index}] ⚠️ MOBILE_DEVICES 없음, fallback 모바일 설정 사용")
        
        common_mobile_configs = [
            {
                'size': (360, 640),
                'ua': 'Mozilla/5.0 (Linux; Android 11; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36',
                'scale': 3.0
            },
            {
                'size': (375, 667),
                'ua': 'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1',
                'scale': 2.0
            },
            {
                'size': (390, 844),
                'ua': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1',
                'scale': 3.0
            },
            {
                'size': (412, 915),
                'ua': 'Mozilla/5.0 (Linux; Android 13; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36',
                'scale': 2.625
            },
            {
                'size': (414, 896),
                'ua': 'Mozilla/5.0 (iPhone; CPU iPhone OS 15_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Mobile/15E148 Safari/604.1',
                'scale': 3.0
            },
            {
                'size': (393, 873),
                'ua': 'Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36',
                'scale': 2.75
            },
        ]
        
        config = random.choice(common_mobile_configs)
        mobile_width, mobile_height = config['size']
        device_scale_factor = config['scale']
        ua = config['ua']
        options.add_argument(f"--user-agent={ua}")
        
        print(f"[Driver-Slot{slot_index}] 📐 Fallback Size: {mobile_width}x{mobile_height}")
        print(f"[Driver-Slot{slot_index}] 🎭 Fallback UA: {ua[:80]}...")
    
    options.add_argument(f"--timezone-id={profile['timezone']}")
    options.add_argument(f"--lang={profile['locale']}")

    prefs = {
        "profile.default_content_setting_values.notifications": 2,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
        # ✅ WebRTC 강화 차단
        "webrtc.ip_handling_policy": "disable_non_proxied_udp",
        "webrtc.multiple_routes_enabled": False,
        "webrtc.nonproxied_udp_enabled": False,
        "webrtc.udp.max_packet_size": 0,
        "intl.accept_languages": random.choice(profile["accept_languages"]),
    }
    options.add_experimental_option("prefs", prefs)
    
    options.add_argument("--disable-quic")
    options.add_argument("--disable-features=NetworkService,NetworkServiceInProcess")

    # Startup 설정
    options.add_argument("--homepage=about:blank")
    options.add_argument("about:blank")

    if HEADLESS:
        options.add_argument("--headless=new")
    #proxy = None
    if proxy:
        options.add_argument(f"--proxy-server={proxy}")
        #proxy_for_chrome = normalize_proxy_for_chrome(proxy)
        #if proxy_for_chrome != proxy:
        #    print(f"[Proxy] 🔧 normalize: {proxy}  →  {proxy_for_chrome}")
        #options.add_argument(f"--proxy-server={proxy_for_chrome}")

    # ✅ 자동화 감지 우회 옵션 강화
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-first-run")
    
    # ✅ 모바일 화면 크기를 고려한 창 위치 계산
    pos = calculate_window_position(slot_index, mobile_width, mobile_height)
    options.add_argument(f"--window-position={pos['x']},{pos['y']}")
    options.add_argument(f"--window-size={mobile_width},{mobile_height}")
    
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
            
            # ✅ 모바일 화면 크기 설정 (약간의 랜덤 변화)
            driver.set_window_size(
                mobile_width + random.randint(-5, 5),
                mobile_height + random.randint(-10, 10),
            )

        except Exception as e:
            print(f"[ERR] Driver creation failed (Slot-{slot_index}): {e}")
            try:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
            except:
                pass
            return None, None

    # ✅ CDP 명령으로 모바일 에뮬레이션 활성화
    try:
        driver.execute_cdp_cmd("Emulation.setDeviceMetricsOverride", {
            "width": mobile_width,
            "height": mobile_height,
            "deviceScaleFactor": device_scale_factor,
            "mobile": is_mobile,
            "screenOrientation": {
                "type": "portraitPrimary",
                "angle": 0
            }
        })
        
        # 터치 이벤트 활성화
        driver.execute_cdp_cmd("Emulation.setTouchEmulationEnabled", {
            "enabled": True,
            "maxTouchPoints": 5
        })
        
        print(f"[Driver-Slot{slot_index}] ✅ 모바일 에뮬레이션 활성화 완료")
        
    except Exception as e:
        print(f"[Driver-Slot{slot_index}] ⚠️ 모바일 에뮬레이션 설정 실패: {e}")

    # ✅ CDP 명령으로 강력한 자동화 감지 우회
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                    // Navigator 속성 재정의
                    Object.defineProperty(navigator, 'webdriver', { 
                        get: () => undefined 
                    });
                    
                    Object.defineProperty(navigator, 'plugins', { 
                        get: () => [1, 2, 3, 4, 5] 
                    });
                    
                    Object.defineProperty(navigator, 'languages', { 
                        get: () => ['ko-KR', 'ko', 'en-US', 'en'] 
                    });
                    
                    // ✅ 모바일 기기 특성 추가
                    Object.defineProperty(navigator, 'maxTouchPoints', {
                        get: () => 5
                    });
                    
                    Object.defineProperty(navigator, 'platform', {
                        get: () => {
                            const platforms = ['Linux armv8l', 'Linux armv7l', 'iPhone'];
                            return platforms[Math.floor(Math.random() * platforms.length)];
                        }
                    });
                    
                    Object.defineProperty(navigator, 'hardwareConcurrency', {
                        get: () => {
                            const cores = [4, 6, 8];
                            return cores[Math.floor(Math.random() * cores.length)];
                        }
                    });
                    
                    // Chrome 객체 추가 (자동화 도구 아님을 위장)
                    window.chrome = { 
                        runtime: {},
                        loadTimes: function() {},
                        csi: function() {},
                        app: {}
                    };
                    
                    // Permissions 쿼리 오버라이드
                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ?
                            Promise.resolve({ state: Notification.permission }) :
                            originalQuery(parameters)
                    );
                    
                    // WebGL Vendor 정보 랜덤화 (핑거프린트 방지)
                    const getParameter = WebGLRenderingContext.prototype.getParameter;
                    WebGLRenderingContext.prototype.getParameter = function(parameter) {
                        if (parameter === 37445) {
                            const vendors = ['Google Inc.', 'ARM', 'Qualcomm'];
                            return vendors[Math.floor(Math.random() * vendors.length)];
                        }
                        if (parameter === 37446) {
                            const renderers = [
                                'Adreno (TM) 640',
                                'Mali-G78',
                                'Apple A15 GPU'
                            ];
                            return renderers[Math.floor(Math.random() * renderers.length)];
                        }
                        return getParameter.apply(this, [parameter]);
                    };
                    
                    // Canvas Fingerprinting 방지
                    const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
                    HTMLCanvasElement.prototype.toDataURL = function() {
                        if (Math.random() < 0.1) {
                            const context = this.getContext('2d');
                            if (context) {
                                context.fillStyle = 'rgba(' + 
                                    Math.floor(Math.random()*255) + ',' +
                                    Math.floor(Math.random()*255) + ',' +
                                    Math.floor(Math.random()*255) + ',0.01)';
                                context.fillRect(0, 0, 1, 1);
                            }
                        }
                        return originalToDataURL.apply(this, arguments);
                    };
                    
                    // console.debug 숨기기
                    console.debug = () => {};
                    
                    // ✅ 터치 이벤트 지원 추가
                    if (!('ontouchstart' in window)) {
                        window.ontouchstart = null;
                        document.ontouchstart = null;
                    }
                """
            },
        )
        print(f"[Driver-Slot{slot_index}] ✅ 자동화 감지 우회 스크립트 주입 완료")
        
    except Exception as e:
        print(f"[Driver-Slot{slot_index}] ⚠️ CDP 스크립트 주입 실패: {e}")

    # ✅ 네트워크 조건 시뮬레이션 (사람처럼 보이게)
    try:
        # 모바일 네트워크는 데스크톱보다 느림
        #driver.execute_cdp_cmd('Network.enable', {})
        #driver.execute_cdp_cmd('Network.emulateNetworkConditions', {
        #    'offline': False,
        #    'downloadThroughput': random.uniform(0.5, 1.5) * 1024 * 1024,  # 0.5-1.5 Mbps (모바일 4G)
        #    'uploadThroughput': random.uniform(200, 500) * 1024,  # 200-500 Kbps
        #    'latency': random.randint(50, 200),  # 50-200ms (모바일 레이턴시)
        #})
        print(f"[Driver-Slot{slot_index}] 🌐 모바일 네트워크 조건 시뮬레이션 활성화")
    except Exception as e:
        print(f"[Driver-Slot{slot_index}] ⚠️ 네트워크 시뮬레이션 실패: {e}")

    return driver, temp_dir



# ===================== 프록시 품질 테스트 함수 (선택적 사용) =====================
def test_proxy_quality(driver, thread_id: int = 0):
    """
    프록시 IP 및 감지 여부 확인 (디버깅용)
    실제 운영시에는 호출하지 않는 것을 권장 (시간 소요)
    """
    try:
        print(f"[Bot-{thread_id}] 🔍 프록시 품질 테스트 시작...")
        
        # 1. 현재 IP 확인
        driver.get("https://api.ipify.org?format=json")
        time.sleep(2)
        try:
            body = driver.find_element(By.TAG_NAME, "body").text
            print(f"[Bot-{thread_id}] 📍 Current IP: {body}")
        except:
            pass
        
        # 2. WebRTC 누수 확인 (간단 버전)
        driver.execute_script("""
            var myPeerConnection = window.RTCPeerConnection || window.mozRTCPeerConnection || window.webkitRTCPeerConnection;
            if (myPeerConnection) {
                console.log('WebRTC is available');
            } else {
                console.log('WebRTC is blocked');
            }
        """)
        
        print(f"[Bot-{thread_id}] ✅ 프록시 품질 테스트 완료")
        
    except Exception as e:
        print(f"[Bot-{thread_id}] ⚠️ 프록시 테스트 실패: {e}")

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
        const title = document.title || '';
        const text  = document.body ? document.body.innerText : '';

        if (href.startsWith('chrome-error://')) return true;

        if (text.includes('ERR_TIMED_OUT') ||
            text.includes('ERR_CONNECTION_TIMED_OUT')) return true;

        if (text.includes("This site can't be reached")) return true;

        if (text.includes("사이트에 연결할 수 없음") ||
            text.includes("사이트에 접속할 수 없습니다")) return true;

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

# ===================== 유튜브 동의 페이지 처리 =====================
from urllib.parse import urlparse

def click_youtube_consent_accept_all(driver, timeout=8):
    try:
        url = driver.current_url
        host = urlparse(url).hostname or ""
        if "consent.youtube.com" not in host:
            print(f"[Consent]  동의 페이지가 아닌 것으로 판단 → 스킵({host})")
            return False

        forms = driver.find_elements(
            By.CSS_SELECTOR,
            "form[action='https://consent.youtube.com/save']",
        )
        if not forms:
            print("[Consent] save 폼이 없어 동의 페이지가 아닌 것으로 판단 → 스킵")
            return False

        btn = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable(
                (
                    By.CSS_SELECTOR,
                    "form[action='https://consent.youtube.com/save'] button[jsname='b3VHJd']",
                )
            )
        )
        btn.click()
        print("[Consent] ✅ 유튜브 동의 '모두 수락' 버튼 자동 클릭 완료")
        return True

    except (TimeoutException, NoSuchElementException):
        print("[Consent] ⚠ 동의 버튼을 찾지 못함 (구조 변경/언어 이슈?)")
        return False
    except Exception as e:
        print(f"[Consent] ⚠ 예외 발생: {e}")
        return False

def is_driver_alive(driver) -> bool:
    """
    사용자가 창을 닫았거나(윈도우 핸들 없음),
    세션이 죽었거나(InvalidSessionId 등),
    크롬이 강종된 경우를 최대한 빨리 감지.
    """
    try:
        handles = driver.window_handles  # 창 닫히면 [] 또는 예외
        if not handles:
            return False

        # 세션/렌더러 죽었는지 가볍게 한번 찔러보기
        driver.execute_script("return 1;")
        return True
    except (InvalidSessionIdException, NoSuchWindowException, WebDriverException):
        return False


def smart_wait(driver, stop_event, timeout: float, index: int, check_interval: float = 0.5) -> bool:
    """
    timeout 동안 대기하되, check_interval마다 stop_event/브라우저 생존을 체크.
    - True: 정상적으로 timeout까지 기다림
    - False: stop_event 또는 브라우저 종료 감지로 조기 중단
    """
    end = time.time() + max(0.0, float(timeout))

    while True:
        if stop_event.is_set():
            return False

        if not is_driver_alive(driver):
            print(f"[Bot-{index}] 🛑 브라우저/세션 종료 감지 -> 대기 중단")
            # 다른 쓰레드도 같이 멈추게 하고 싶으면 아래를 켜도 됨
            # stop_event.set()
            return False

        remaining = end - time.time()
        if remaining <= 0:
            return True

        stop_event.wait(timeout=min(check_interval, remaining))

CHROME_ERROR_URL_PREFIXES = (
    "chrome-error://",        # 크로미움 에러 페이지
    "chrome://error",         # 일부 케이스
)

ERROR_TEXT_MARKERS = (
    "This site can't be reached",
    "This site can't be reached",
    "ERR_TIMED_OUT",
    "net::ERR_",
    "Connect to network",
)

def _page_looks_like_error(driver) -> bool:
    # 1) chrome 자체 에러 페이지 URL
    try:
        cur = (driver.current_url or "").lower()
        if any(cur.startswith(p) for p in CHROME_ERROR_URL_PREFIXES):
            return True
    except Exception:
        pass

    # 2) 화면 텍스트로 감지 (가장 확실)
    try:
        body = driver.find_element(By.TAG_NAME, "body")
        txt = (body.text or "")
        if any(m in txt for m in ERROR_TEXT_MARKERS):
            return True
    except Exception:
        pass

    # 3) page_source로 추가 감지 (body.text가 비는 경우 대비)
    try:
        src = driver.page_source or ""
        if any(m in src for m in ERROR_TEXT_MARKERS):
            return True
    except Exception:
        pass

    # 4) 프록시 서버가 뿜는 에러 감지 (일부 프록시에서 connectivitycheck.gstatic.com으로 리다이렉트하는 경우)
    try:
        url = driver.current_url or ""
        host = urlparse(url).hostname or ""
        if "connectivitycheck.gstatic.com" == host:
            return True
    except Exception:
        pass

    return False


def safe_get(driver, url: str, index: int, page_load_timeout: float = 30.0) -> bool:
    """
    True면 '정상 페이지'로 간주, False면 접속 실패/타임아웃/에러페이지.
    """
    try:
        driver.set_page_load_timeout(page_load_timeout)
    except Exception:
        pass

    try:
        driver.get(url)
    except TimeoutException:
        print(f"[Bot-{index}] ⚠️ pageLoadTimeout 발생 (driver.get)")
        return False
    except WebDriverException as e:
        msg = str(e)
        # net::ERR_* 류는 대부분 여기로 옴
        if "net::ERR_" in msg or "ERR_TIMED_OUT" in msg or "timeout" in msg.lower():
            print(f"[Bot-{index}] ⚠️ WebDriverException (네트워크/타임아웃): {msg[:160]}")
            return False
        # 그 외는 그대로 실패 처리(원하면 raise)
        print(f"[Bot-{index}] ⚠️ WebDriverException: {msg[:160]}")
        return False

    # 예외가 안 나도 에러 페이지일 수 있음
    if _page_looks_like_error(driver):
        print(f"[Bot-{index}] ⚠️ 에러 페이지 감지 (ERR_TIMED_OUT 등)")
        return False

    return True

def get_and_error_if_new_tab(driver, url, *, max_wait=2.0, poll=0.05, close_new=True):
    before_handles = set(driver.window_handles)
    before_current = driver.current_window_handle if before_handles else None

    driver.get(url)

    deadline = time.time() + max_wait
    new_infos = []

    while time.time() < deadline:
        after_handles = set(driver.window_handles)

        # 1) 새 탭/창 생김
        diff = list(after_handles - before_handles)
        if diff:
            for h in diff:
                info = {"handle": h, "url": None}
                try:
                    driver.switch_to.window(h)
                    info["url"] = driver.current_url
                    if close_new:
                        driver.close()
                except WebDriverException:
                    pass
                new_infos.append(info)

            # 원래 탭으로 복귀
            try:
                if before_current and before_current in driver.window_handles:
                    driver.switch_to.window(before_current)
                elif driver.window_handles:
                    driver.switch_to.window(driver.window_handles[0])
            except WebDriverException:
                pass

            raise RuntimeError(f"Unexpected new tab/window opened during get(): {new_infos}")

        # 2) (드물지만) 원래 탭이 사라진 경우도 비정상으로 볼 수 있음
        if before_current and before_current not in after_handles:
            raise RuntimeError("Original tab disappeared after get().")

        time.sleep(poll)

    return True
        
# ===================== 메인 워커 =====================
def monitor_service(
    url: str,
    proxy_member: str,
    slot_index: int,  # ✅ index -> slot_index로 변경
    stop_event: threading.Event,
    redis_client: Optional[redis.Redis] = None,
):
    """
    ✅ 슬롯 기반 워커 함수
    - slot_index: 고정된 슬롯 번호 (0 ~ NUM_BROWSERS-1)
    """
    driver = None
    temp_dir = None
    session_ok = False

    try:
        if not REGION_PROFILES:
            print(f"[Slot-{slot_index}] ❌ REGION_PROFILES가 비어 있습니다.")
            return

        region = random.choice(list(REGION_PROFILES.keys()))
        profile = REGION_PROFILES[region]

        print(f"\n[Slot-{slot_index}] 🌍 Profile: {region} ({profile['timezone']})")
        print(f"[Slot-{slot_index}] 🧩 Proxy(leased): {proxy_member}")
        print(f"[Slot-{slot_index}] 🧩 Proxy(chrome): {normalize_proxy_for_chrome(proxy_member)}")

        if stop_event.is_set():
            print(f"[Slot-{slot_index}] 🛑 시작 전 중단 신호 수신. 종료.")
            return
        
        # ✅ slot_index를 전달하여 슬롯별 창 위치 설정
        driver, temp_dir = create_undetected_driver(profile, proxy_member, slot_index)
        if not driver:
            print(f"[Slot-{slot_index}] ❌ 드라이버 생성 실패.")
            return

        # 디버그: 브라우저 초기 상태
        try:
            print(f"[Slot-{slot_index}] (debug) initial url={driver.current_url} title={driver.title!r}")
        except Exception:
            pass

        # ✅ 창 위치는 이미 create_undetected_driver에서 설정됨
        print(f"[Slot-{slot_index}] 🪟 창 위치는 드라이버 생성 시 슬롯별로 자동 설정됨")

        # 초기 페이지
        try:
            driver.get("about:blank")
            print(f"[Slot-{slot_index}] 초기 페이지(about:blank) 로드 완료")
        except Exception as e:
            print(f"[Slot-{slot_index}] ⚠️ 초기 페이지 로드 실패: {e}")
            return

        reset_browser_data_in_session(driver)

        # ✅ Referer 설정 (region_profiles.json에서)
        referer = random.choice(profile["referers"])
        try:
            driver.execute_cdp_cmd(
                "Network.setExtraHTTPHeaders", {"headers": {"Referer": referer}}
            )
            print(f"[Slot-{slot_index}] 🔗 Referer: {referer}")
        except Exception as e:
            print(f"[Slot-{slot_index}] ⚠️ Referer 설정 실패: {e}")

        # ✅ 랜덤 대기 후 타겟 페이지 접속 (더 사람처럼)
        pre_nav_delay = random.uniform(1.0, 3.0)
        print(f"[Slot-{slot_index}] ⏳ 접속 전 {pre_nav_delay:.1f}초 대기...")
        time.sleep(pre_nav_delay)

        # 타겟 페이지 접속
        print(f"[Slot-{slot_index}] 접속 요청: {url}")
        browse_start = time.time()
        hard_deadline = browse_start + BROWSE_MAX_SECONDS

        try:

            #driver.get(url)
            try:
                get_and_error_if_new_tab(driver, url, max_wait=5.0, close_new=True)
            except RuntimeError as e:
                print(f"[Slot-{slot_index}] ⚠️[ERR] 새 탭/창 자동 오픈 감지:{e}")
                return

            clicked = click_youtube_consent_accept_all(driver)

            if not clicked:
                try:
                    WebDriverWait(driver, 5).until(
                        lambda d: "consent.youtube.com" in d.current_url
                    )
                    click_youtube_consent_accept_all(driver)
                except TimeoutException:
                    pass
        except TimeoutException:
            print(f"[Slot-{slot_index}] ⚠️ Get 요청 타임아웃. 로딩 상태 확인 시도.")

        if _page_looks_like_error(driver) :
            print(f"[Slot-{slot_index}] ⏰ 에러페이지로 의심. 세션 종료.")
            return
        
        remaining_for_load = hard_deadline - time.time()
        if remaining_for_load <= 0:
            print(f"[Slot-{slot_index}] ⏰ 브라우징 최대 시간({BROWSE_MAX_SECONDS}초) 도달(로딩 대기 중). 세션 종료.")
            return

        if not ensure_page_ready(driver, timeout=min(ENSURE_TIMEOUT, max(5, remaining_for_load))):
            print(f"[Slot-{slot_index}] ❌ 페이지 로딩 실패로 종료.")
            return

        session_ok = True

        remaining = hard_deadline - time.time()
        if remaining <= 0:
            print(f"[Slot-{slot_index}] ⏰ 브라우징 최대 시간({BROWSE_MAX_SECONDS}초) 도달(로딩 직후). 세션 종료.")
            return

        if stop_event.is_set():
            print(f"[Slot-{slot_index}] 🛑 인지 대기 중 중단 신호. 종료.")
            return

        remaining = hard_deadline - time.time()
        if remaining <= 0:
            print(f"[Slot-{slot_index}] ⏰ 브라우징 최대 시간({BROWSE_MAX_SECONDS}초) 도달(체류 전). 세션 종료.")
            return

        stay_time = max(10, random.gauss(STAY_DURATION, 10))
        stay_time = min(stay_time, remaining)

        # ✅ 휴먼 이벤트 타이밍 계산: 세션 종료 HUMAN_EVENT_BEFORE_END_SECONDS초 전
        human_event_timing = min(HUMAN_EVENT_BEFORE_END_SECONDS, stay_time - HUMAN_EVENT_BEFORE_END_SECONDS)
        
        human_event = HumanEventMobile(driver)

        if human_event_timing <= 5:
            # 체류 시간이 너무 짧으면 즉시 실행
            print(f"[Slot-{slot_index}] 체류 시작 (이 {stay_time:.1f}초, 즉시 휴먼 이벤트 실행)")
            human_event.execute_random_action()

            # ✅ 휴먼 이벤트 후: 남은 시간과 무관하게 10초 대기 후 종료
            print(f"[Slot-{slot_index}] ⏳ 휴먼 이벤트 후 10초 대기...")
            if not smart_wait(driver, stop_event, 10, slot_index):
                return
            print(f"[Slot-{slot_index}] 모니터링 정상 종료.")
            return
        else:
            # 계산된 시점에 휴먼 이벤트 실행
            after_event_wait = stay_time - human_event_timing

            print(f"[Slot-{slot_index}] 체류 시작 (이 {stay_time:.1f}초: 대기 {human_event_timing:.1f}초 → 휴먼 이벤트 → 마무리 {after_event_wait:.1f}초)")

            # 휴먼 이벤트 전 대기
            if not smart_wait(driver, stop_event, human_event_timing, slot_index):
                return
            if stop_event.is_set():
                return

            human_event.execute_random_action()

            # ✅ 휴먼 이벤트 후: 남은 시간과 무관하게 10초 대기 후 종료
            print(f"[Slot-{slot_index}] ⏳ 휴먼 이벤트 후 20초 대기...")
            if not smart_wait(driver, stop_event, 20, slot_index):
                return
            print(f"[Slot-{slot_index}] 모니터링 정상 종료.")
            return


    except Exception as e:
        print(f"[Slot-{slot_index}] 🛑 오류 발생: {e.__class__.__name__}: {e}")

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

        time.sleep(2)

        if temp_dir and os.path.exists(temp_dir):
            for attempt in range(3):
                try:
                    shutil.rmtree(temp_dir)
                    print(f"[Slot-{slot_index}] 🧹 임시 디렉토리 삭제 완료: {temp_dir}")
                    break
                except PermissionError:
                    if attempt < 2:
                        print(f"[Slot-{slot_index}] ⚠️ 삭제 재시도 {attempt + 1}/3 (파일 사용 중)")
                        time.sleep(2)
                    else:
                        print(f"[Slot-{slot_index}] ⚠️ 임시 디렉토리 삭제 최종 실패")
                except Exception as e:
                    print(f"[Slot-{slot_index}] ⚠️ 임시 디렉토리 삭제 실패: {e}")
                    break

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

# ===================== 임시 디렉토리 정리 (전역, 예비용) =====================
def cleanup_temp_dirs():
    print("\n🧹 남은 임시 파일 확인 중...")
    cleaned = 0
    failed = 0
    try:
        temp_base = tempfile.gettempdir()
        for item in os.listdir(temp_base):
            if item.startswith("monitor_slot_"):  # ✅ prefix 변경
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

# ===================== 메인 (슬롯 스케줄러) =====================
if __name__ == "__main__":
    print(f"=== 🛡️ Redis 기반 Stealth Monitor Started (TARGET_URL: {TARGET_URL}) ===")

    if not REGION_PROFILES:
        print("[MAIN] ❌ REGION_PROFILES가 비어 있습니다. region_profiles.json 상태를 확인하세요.")
        exit(1)

    r = get_redis()

    # ✅ 슬롯 기반 관리: {슬롯번호: 쓰레드객체}
    active_slots: Dict[int, threading.Thread] = {}

    try:
        while not stop_event.is_set():
            # 1) 종료된 스레드 정리
            for slot in list(active_slots.keys()):
                if not active_slots[slot].is_alive():
                    del active_slots[slot]
                    print(f"[MAIN] 🔄 슬롯-{slot} 정리 완료 (스레드 종료)")

            # 2) 빈 슬롯 채우기
            for slot in range(NUM_BROWSERS):
                if slot not in active_slots and not stop_event.is_set():
                    # 프록시 가져오기
                    proxy_member = claim_proxy(r, lease_seconds=LEASE_SECONDS, reclaim_limit=200, sample_k=50)
                    if not proxy_member:
                        print(f"[MAIN] ⚠️ 사용 가능한 프록시 없음, 대기 중...")
                        time.sleep(WAIT_WHEN_NO_PROXY_SECONDS)
                        break

                    log_proxy_used(r, proxy_member)

                    # URL 선택 (슬롯 번호에 따라)
                    url = TARGET_URL if slot % 2 == 0 else TARGET_URL1

                    print(f"[MAIN] ▶ 슬롯-{slot} 시작, 프록시(leased): {proxy_member}")
                    
                    # ✅ 스레드 생성 시 slot_index 전달
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
        # 모든 슬롯의 스레드 종료 대기
        print(f"\n[MAIN] 🛑 모든 슬롯 종료 대기 중... (활성 슬롯: {len(active_slots)}개)")
        for slot, t in active_slots.items():
            if t.is_alive():
                print(f"[MAIN] ⏳ 슬롯-{slot} 종료 대기...")
                t.join(timeout=10)

        cleanup_temp_dirs()
        print("\n=== ✅ 모든 작업 완료 및 정리 완료 ===")
        print(f"=== 🏁 슬롯 기반 모니터 종료 ===")