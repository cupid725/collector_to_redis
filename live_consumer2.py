import random
import threading
import time
import tempfile
import os
import shutil
import json
from typing import Dict, Any, Optional

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

from urllib.parse import urlparse
import atexit


# 드라이버 생성 시 동시 접근 방지용 Lock
driver_creation_lock = threading.Lock()

# 모든 스레드에 중단 신호를 보내기 위한 전역 Event
stop_event = threading.Event()

# ===================== Redis 설정 =====================
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None

# ✅ collector가 넣는 풀(SET)
REDIS_POOL_KEY = "proxies:pool"

# consumer 정책
MAX_GHOST_RETRY_PER_GET = 500     # 유령(키 TTL 만료) 연속으로 이만큼 나오면 포기
WAIT_WHEN_NO_PROXY_SECONDS = 60   # pool 비었을 때 메인 루프에서 기다릴 시간
DELETE_KEY_AFTER_USE = True       # ✅ 사용 후 DEL proxy (선택)

def get_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_connect_timeout=10,
        socket_timeout=30,
        retry_on_timeout=True,
    )

def try_pop_valid_proxy(r: redis.Redis, ghost_limit: int = MAX_GHOST_RETRY_PER_GET) -> Optional[str]:
    """
    ✅ consumer 방식:
      proxy = SPOP proxies:pool
      if not EXISTS proxy: continue  (유령 버림)
    pool이 비면 None 반환
    """
    ghost = 0
    while not stop_event.is_set():
        proxy = r.spop(REDIS_POOL_KEY)
        if not proxy:
            return None

        # TTL 만료로 key가 없으면 유령 -> 버리고 다음
        if not r.exists(proxy):
            ghost += 1
            if ghost % 50 == 0:
                print(f"[REDIS] ⚠️ ghost popped={ghost} (expired key)")
            if ghost >= ghost_limit:
                print(f"[REDIS] ⚠️ ghost_limit 도달({ghost_limit}). 잠시 후 재시도 권장.")
                return None
            continue

        # 최소 검증
        if "://" not in proxy:
            print(f"[REDIS] ⚠️ invalid proxy format: {proxy!r}")
            continue

        return proxy

    return None


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
TARGET_URL = "https://www.youtube.com/shorts/u7sO-mNEpT4?feature=share"  # 크리스마스 2
TARGET_URL1 = "https://youtube.com/shorts/-vVnZoVtnFk?feature=share"     # 크리스마스

COMMAND_TIMEOUT = 300
LOAD_TIMEOUT = COMMAND_TIMEOUT
ENSURE_TIMEOUT = 420
BROWSE_MAX_SECONDS = ENSURE_TIMEOUT
STAY_DURATION = 300

WINDOW_WIDTH = 800
WINDOW_HEIGHT = 700
NUM_BROWSERS = 2
HEADLESS = False


# ===================== 사람처럼 행동하는 유틸 =====================
def human_sleep(min_sec=0.5, max_sec=2.0, mu=None, sigma=None):
    if mu is None:
        mu = (min_sec + max_sec) / 2
    if sigma is None:
        sigma = (max_sec - min_sec) / 4
    sleep_time = random.gauss(mu, sigma)
    sleep_time = max(min_sec, min(sleep_time, max_sec))
    time.sleep(sleep_time)

def get_bezier_curve(start, end, control_points, num_points=20):
    points = []
    for t in np.linspace(0, 1, num_points):
        x = (1 - t) ** 2 * start[0] + 2 * (1 - t) * t * control_points[0] + t ** 2 * end[0]
        y = (1 - t) ** 2 * start[1] + 2 * (1 - t) * t * control_points[1] + t ** 2 * end[1]
        points.append((x, y))
    return points

def human_mouse_move(driver, start_el=None, end_el=None):
    try:
        action = ActionChains(driver)
        window_size = driver.get_window_size()
        start_x = random.randint(10, window_size['width'] // 2)
        start_y = random.randint(10, window_size['height'] // 2)

        if end_el:
            loc = end_el.location
            size = end_el.size
            end_x = loc['x'] + random.randint(0, size['width'])
            end_y = loc['y'] + random.randint(0, size['height'])
        else:
            end_x = random.randint(100, window_size['width'] - 100)
            end_y = random.randint(100, window_size['height'] - 100)

        control_x = random.randint(min(start_x, end_x), max(start_x, end_x))
        control_y = random.randint(min(start_y, end_y), max(start_y, end_y)) + random.randint(-200, 200)

        _ = get_bezier_curve((start_x, start_y), (end_x, end_y), (control_x, control_y))

        move_duration = random.uniform(0.3, 0.8)
        time.sleep(move_duration)

        if end_el:
            action.move_to_element(end_el).perform()
        else:
            action.move_by_offset(random.randint(-5, 5), random.randint(-5, 5)).perform()
    except Exception:
        pass

def human_scroll(driver):
    try:
        scroll_height = driver.execute_script("return document.body.scrollHeight")
        if not scroll_height:
            return

        current_pos = driver.execute_script("return window.pageYOffset;")
        target_pos = random.randint(int(scroll_height * 0.3), int(scroll_height * 0.8))

        while current_pos < target_pos:
            step = random.randint(50, 150)
            current_pos += step
            driver.execute_script(f"window.scrollTo(0, {current_pos});")
            time.sleep(random.uniform(0.02, 0.1))

        if random.random() < 0.5:
            driver.execute_script(f"window.scrollBy(0, -{random.randint(50, 200)});")
    except Exception:
        pass

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

# ===================== undetected_chromedriver 생성 =====================
def create_undetected_driver(profile: Dict[str, Any], proxy: Optional[str], thread_id: int = 0):
    """
    향상된 스텔스 드라이버 생성 (region_profiles.json의 user_agents 활용)
    Returns: (driver, temp_dir) 튜플
    """
    options = uc.ChromeOptions()

    temp_dir = tempfile.mkdtemp(prefix=f"monitor_profile_{thread_id}_")
    options.add_argument(f"--user-data-dir={temp_dir}")

    # ✅ User-Agent 설정 (region_profiles.json에서)
    if "user_agents" in profile:
        ua = random.choice(profile["user_agents"])
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
        options.add_argument(f"--user-agent={ua}")
        print(f"[Driver-{thread_id}] 🎭 User-Agent: {ua[:80]}...")

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

    if proxy:
        proxy_for_chrome = normalize_proxy_for_chrome(proxy)
        if proxy_for_chrome != proxy:
            print(f"[Proxy] 🔧 normalize: {proxy}  →  {proxy_for_chrome}")
        options.add_argument(f"--proxy-server={proxy_for_chrome}")

    # ✅ 자동화 감지 우회 옵션 강화
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-first-run")
    options.add_argument(f"--window-size={WINDOW_WIDTH},{WINDOW_HEIGHT}")
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
                WINDOW_WIDTH + random.randint(-100, 100),
                WINDOW_HEIGHT + random.randint(-100, 100),
            )
        except Exception as e:
            print(f"[ERR] Driver creation failed: {e}")
            try:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
            except:
                pass
            return None, None

    # ✅ CDP 명령으로 강력한 자동화 감지 우회
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                    Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
                    Object.defineProperty(navigator, 'languages', { get: () => ['ko-KR','ko','en-US','en'] });

                    window.chrome = { runtime: {}, loadTimes: function(){}, csi: function(){}, app: {} };

                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ?
                            Promise.resolve({ state: Notification.permission }) :
                            originalQuery(parameters)
                    );

                    const getParameter = WebGLRenderingContext.prototype.getParameter;
                    WebGLRenderingContext.prototype.getParameter = function(parameter) {
                        if (parameter === 37445) {
                            const vendors = ['Intel Inc.', 'Google Inc.', 'Mozilla'];
                            return vendors[Math.floor(Math.random() * vendors.length)];
                        }
                        if (parameter === 37446) {
                            const renderers = [
                                'Intel Iris OpenGL Engine',
                                'ANGLE (Intel, Intel(R) HD Graphics 630 Direct3D11 vs_5_0 ps_5_0)',
                                'Mesa DRI Intel(R) HD Graphics'
                            ];
                            return renderers[Math.floor(Math.random() * renderers.length)];
                        }
                        return getParameter.apply(this, [parameter]);
                    };

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

                    console.debug = () => {};
                """
            },
        )
        print(f"[Driver-{thread_id}] ✅ 자동화 감지 우회 스크립트 주입 완료")
    except Exception as e:
        print(f"[Driver-{thread_id}] ⚠️ CDP 스크립트 주입 실패: {e}")

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
def click_youtube_consent_accept_all(driver, timeout=8):
    try:
        url = driver.current_url
        host = urlparse(url).hostname or ""
        if "consent.youtube.com" not in host:
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

# ===================== 메인 워커 =====================
def monitor_service(
    url: str,
    proxy_member: str,
    index: int,
    stop_event: threading.Event,
    redis_client: Optional[redis.Redis] = None,
):
    driver = None
    temp_dir = None
    session_ok = False

    try:
        if not REGION_PROFILES:
            print(f"[Bot-{index}] ❌ REGION_PROFILES가 비어 있습니다.")
            return

        region = random.choice(list(REGION_PROFILES.keys()))
        profile = REGION_PROFILES[region]

        print(f"\n[Bot-{index}] 🌍 Profile: {region} ({profile['timezone']})")
        print(f"[Bot-{index}] 🧩 Proxy(pop): {proxy_member}")
        print(f"[Bot-{index}] 🧩 Proxy(chrome): {normalize_proxy_for_chrome(proxy_member)}")

        if stop_event.is_set():
            print(f"[Bot-{index}] 🛑 시작 전 중단 신호 수신. 종료.")
            return

        driver, temp_dir = create_undetected_driver(profile, proxy_member, index)
        if not driver:
            print(f"[Bot-{index}] ❌ 드라이버 생성 실패.")
            return

        # 디버그: 브라우저 초기 상태
        try:
            print(f"[Bot-{index}] (debug) initial url={driver.current_url} title={driver.title!r}")
        except Exception:
            pass

        # 창 위치 설정
        try:
            slot = index % max(1, NUM_BROWSERS)
            base_x = 50
            base_y = 50
            gap_x = WINDOW_WIDTH + 40
            x = base_x + slot * gap_x
            y = base_y
            if not HEADLESS:
                driver.set_window_position(x, y)
                print(f"[Bot-{index}] 🪟 창 위치 설정: ({x}, {y}) [slot {slot}]")
        except Exception as e:
            print(f"[Bot-{index}] ⚠️ 창 위치 설정 실패: {e}")

        # 초기 페이지
        try:
            driver.get("about:blank")
            print(f"[Bot-{index}] 초기 페이지(about:blank) 로드 완료")
        except Exception as e:
            print(f"[Bot-{index}] ⚠️ 초기 페이지 로드 실패: {e}")
            return

        reset_browser_data_in_session(driver)

        # ✅ Referer 설정 (region_profiles.json에서)
        referer = random.choice(profile["referers"])
        try:
            driver.execute_cdp_cmd(
                "Network.setExtraHTTPHeaders", {"headers": {"Referer": referer}}
            )
            print(f"[Bot-{index}] 🔗 Referer: {referer}")
        except Exception as e:
            print(f"[Bot-{index}] ⚠️ Referer 설정 실패: {e}")

        # ✅ 랜덤 대기 후 타겟 페이지 접속
        pre_nav_delay = random.uniform(1.0, 3.0)
        print(f"[Bot-{index}] ⏳ 접속 전 {pre_nav_delay:.1f}초 대기...")
        time.sleep(pre_nav_delay)

        print(f"[Bot-{index}] 접속 요청: {url}")
        browse_start = time.time()
        hard_deadline = browse_start + BROWSE_MAX_SECONDS

        try:
            driver.get(url)

            clicked = click_youtube_consent_accept_all(driver)
            if not clicked:
                try:
                    WebDriverWait(driver, 5).until(lambda d: "consent.youtube.com" in d.current_url)
                    click_youtube_consent_accept_all(driver)
                except TimeoutException:
                    pass

        except TimeoutException:
            print(f"[Bot-{index}] ⚠️ Get 요청 타임아웃. 로딩 상태 확인 시도.")

        remaining_for_load = hard_deadline - time.time()
        if remaining_for_load <= 0:
            print(f"[Bot-{index}] ⏰ 브라우징 최대 시간({BROWSE_MAX_SECONDS}초) 도달(로딩 대기 중). 세션 종료.")
            return

        if not ensure_page_ready(driver, timeout=min(ENSURE_TIMEOUT, max(5, remaining_for_load))):
            print(f"[Bot-{index}] ❌ 페이지 로딩 실패로 종료.")
            return

        session_ok = True

        remaining = hard_deadline - time.time()
        if remaining <= 0:
            print(f"[Bot-{index}] ⏰ 브라우징 최대 시간({BROWSE_MAX_SECONDS}초) 도달(로딩 직후). 세션 종료.")
            return

        reaction_time = min(random.uniform(0.8, 2.5), remaining)
        if reaction_time > 0:
            print(f"[Bot-{index}] ✅ 로딩 완료. 인지 반응 대기: {reaction_time:.2f}초 (남은 상한: {remaining:.1f}초)")
            stop_event.wait(timeout=reaction_time)

        if stop_event.is_set():
            print(f"[Bot-{index}] 🛑 인지 대기 중 중단 신호. 종료.")
            return

        remaining = hard_deadline - time.time()
        if remaining <= 0:
            print(f"[Bot-{index}] ⏰ 브라우징 최대 시간({BROWSE_MAX_SECONDS}초) 도달(체류 전). 세션 종료.")
            return

        stay_time = max(10, random.gauss(STAY_DURATION, 10))
        stay_time = min(stay_time, remaining)

        action_offset = 15.0

        if stay_time <= action_offset:
            print(f"[Bot-{index}] 체류 시작 (총 {stay_time:.1f}초, 즉시 휴먼 이벤트 실행 후 대기)")
            try:
                body = driver.find_element(By.TAG_NAME, "body")
                human_mouse_move(driver, end_el=body)
            except Exception:
                pass
            human_scroll(driver)
            if not smart_wait(driver, stop_event, stay_time, index):
                return
        else:
            pre_wait = stay_time - action_offset
            print(f"[Bot-{index}] 체류 시작 (총 {stay_time:.1f}초, {pre_wait:.1f}초 후 휴먼 이벤트 실행, 이후 15초 유지)")
            if not smart_wait(driver, stop_event, pre_wait, index):
                return
            if stop_event.is_set():
                return
            try:
                body = driver.find_element(By.TAG_NAME, "body")
                human_mouse_move(driver, end_el=body)
            except Exception:
                pass
            human_scroll(driver)
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

        time.sleep(2)

        if temp_dir and os.path.exists(temp_dir):
            for attempt in range(3):
                try:
                    shutil.rmtree(temp_dir)
                    print(f"[Bot-{index}] 🧹 임시 디렉토리 삭제 완료: {temp_dir}")
                    break
                except PermissionError:
                    if attempt < 2:
                        print(f"[Bot-{index}] ⚠️ 삭제 재시도 {attempt + 1}/3 (파일 사용 중)")
                        time.sleep(2)
                    else:
                        print(f"[Bot-{index}] ⚠️ 임시 디렉토리 삭제 최종 실패")
                except Exception as e:
                    print(f"[Bot-{index}] ⚠️ 임시 디렉토리 삭제 실패: {e}")
                    break

        # ✅ consumer 정책: 사용 후 (선택) DEL proxy
        if redis_client and proxy_member and DELETE_KEY_AFTER_USE:
            try:
                redis_client.delete(proxy_member)
                print(f"[Bot-{index}] 🧹 DEL proxy key(after use): {proxy_member}")
            except redis.RedisError as e:
                print(f"[Bot-{index}] ⚠️ DEL proxy key 실패: {e}")


# ===================== 임시 디렉토리 정리 (전역, 예비용) =====================
def cleanup_temp_dirs():
    print("\n🧹 남은 임시 파일 확인 중...")
    cleaned = 0
    failed = 0
    try:
        temp_base = tempfile.gettempdir()
        for item in os.listdir(temp_base):
            if item.startswith("monitor_profile_"):
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

atexit.register(cleanup_temp_dirs)

# ===================== 메인 (워커 스케줄러) =====================
if __name__ == "__main__":
    print(f"=== 🛡️ Redis 기반 Stealth Monitor Started (TARGET_URL: {TARGET_URL}) ===")

    if not REGION_PROFILES:
        print("[MAIN] ❌ REGION_PROFILES가 비어 있습니다. region_profiles.json 상태를 확인하세요.")
        exit(1)

    r = get_redis()

    threads: list[threading.Thread] = []
    worker_index = 0
    cycle = 0

    try:
        while not stop_event.is_set():
            cycle += 1

            # 1) 죽은 스레드 정리
            alive_threads = [t for t in threads if t.is_alive()]
            if len(alive_threads) != len(threads):
                print(f"[MAIN] 🔄 스레드 정리: {len(threads)} → {len(alive_threads)} alive")
            threads = alive_threads

            capacity = max(0, NUM_BROWSERS - len(threads))

            # 2) 여유 슬롯만큼 새 워커 생성 시도
            no_proxy_available = False
            for _ in range(capacity):
                if stop_event.is_set():
                    break

                proxy_member = try_pop_valid_proxy(r)
                if not proxy_member:
                    no_proxy_available = True
                    print("[MAIN] ⚠️ 사용할 프록시가 없습니다(SPOP 결과 없음 또는 유령 과다). collector가 채울 때까지 대기.")
                    break

                idx = worker_index
                worker_index += 1

                # URL 교차
                url = TARGET_URL
                if (idx % 2):
                    url = TARGET_URL1

                print(f"[MAIN] ▶ 새 워커 Bot-{idx} 시작, 프록시(pop): {proxy_member}")
                t = threading.Thread(
                    target=monitor_service,
                    args=(url, proxy_member, idx, stop_event, r),
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

        cleanup_temp_dirs()
        print("\n=== ✅ 모든 작업 완료 및 정리 완료 ===")
