import random
import threading
import time
import tempfile
import os
import shutil
import json
import atexit
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

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
)

# ==================== 전역 설정 ====================
driver_creation_lock = threading.Lock()
temp_dirs = []
stop_event = threading.Event()

# ==================== Redis 설정 ====================
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None

REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_USED = "proxies:used_recent"
REDIS_ZSET_FAILED = "proxies:failed_recent"

PROXY_REUSE_COOLDOWN_MINUTES = 30
PROXY_FAILURE_PENALTY_MINUTES = 60

def get_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )

# ==================== 프록시 선택 ====================
# (기존 pick_proxy, mark_proxy_used, cleanup_old_proxy_records 함수 그대로 유지)
# ... [이전 코드와 동일한 부분 생략] ...

def pick_proxy(
    r: redis.Redis,
    prefer_protocol: Optional[str] = None,
    prefer_countries: Optional[List[str]] = None,
    top_n: int = 100,
) -> Optional[Dict[str, str]]:
    # 기존 함수 그대로
    members = r.zrange(REDIS_ZSET_ALIVE, 0, top_n - 1, withscores=True)
    if not members:
        return None

    now = time.time()
    used_cutoff = now - (PROXY_REUSE_COOLDOWN_MINUTES * 60)
    failed_cutoff = now - (PROXY_FAILURE_PENALTY_MINUTES * 60)
    
    recently_used = set(r.zrangebyscore(REDIS_ZSET_USED, used_cutoff, now))
    recently_failed = set(r.zrangebyscore(REDIS_ZSET_FAILED, failed_cutoff, now))
    
    excluded = recently_used | recently_failed

    candidates = []
    for member, latency in members:
        if "://" not in member:
            continue
        if member in excluded:
            continue
        
        protocol, address = member.split("://", 1)
        
        if prefer_protocol and protocol != prefer_protocol:
            continue
        
        proxy_key = f"proxy:{protocol}:{address}"
        proxy_info = r.hgetall(proxy_key)
        
        if not proxy_info or proxy_info.get("status") != "alive":
            continue
        
        countries = proxy_info.get("countries", "Unknown")
        
        if prefer_countries:
            country_match = any(
                country.upper() in countries.upper() 
                for country in prefer_countries
            )
            if not country_match:
                continue
        
        candidates.append({
            "member": member,
            "protocol": protocol,
            "address": address,
            "latency": latency,
            "countries": countries,
            "proxy_type": proxy_info.get("proxy_type", "Unknown"),
        })
    
    if not candidates:
        return None
    
    candidates.sort(key=lambda x: x["latency"])
    
    top_20_percent = max(1, len(candidates) // 5)
    if random.random() < 0.7 and len(candidates) > top_20_percent:
        selected = random.choice(candidates[:top_20_percent])
    else:
        selected = random.choice(candidates)
    
    return selected

def mark_proxy_used(r: redis.Redis, member: str, success: bool = True):
    now_ts = time.time()
    pipe = r.pipeline()
    
    if success:
        pipe.zadd(REDIS_ZSET_USED, {member: now_ts})
        print(f"[Proxy] ✅ 사용 완료: {member}")
    else:
        pipe.zadd(REDIS_ZSET_FAILED, {member: now_ts})
        pipe.zrem(REDIS_ZSET_ALIVE, member)
        print(f"[Proxy] ❌ 실패 기록: {member}")
    
    try:
        pipe.execute()
    except redis.RedisError as e:
        print(f"[Redis] 프록시 상태 업데이트 실패: {e}")

def cleanup_old_proxy_records(r: redis.Redis):
    try:
        now = time.time()
        used_cutoff = now - (PROXY_REUSE_COOLDOWN_MINUTES * 2 * 60)
        failed_cutoff = now - (PROXY_FAILURE_PENALTY_MINUTES * 2 * 60)
        
        removed_used = r.zremrangebyscore(REDIS_ZSET_USED, 0, used_cutoff)
        removed_failed = r.zremrangebyscore(REDIS_ZSET_FAILED, 0, failed_cutoff)
        
        if removed_used > 0 or removed_failed > 0:
            print(f"[Redis] 오래된 프록시 기록 정리: used={removed_used}, failed={removed_failed}")
    except redis.RedisError as e:
        print(f"[Redis] 정리 실패: {e}")

# ==================== Region Profiles ====================
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

# ==================== 공통 설정 ====================
TARGET_URL = "https://www.example.com"
TARGET_URL = "https://youtube.com/shorts/-vVnZoVtnFk?feature=share"
TARGET_URL = "https://abrahamjuliot.github.io/creepjs/"
COMMAND_TIMEOUT = 300
LOAD_TIMEOUT = COMMAND_TIMEOUT
ENSURE_TIMEOUT = 300
BROWSE_MAX_SECONDS = ENSURE_TIMEOUT
STAY_DURATION = 600
NUM_BROWSERS = 1
HEADLESS = False

WAIT_WHEN_NO_PROXY_SECONDS = 60
PREFER_COUNTRIES: Optional[List[str]] = None

# ==================== Human-like 유틸 ====================
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

        path = get_bezier_curve((start_x, start_y), (end_x, end_y), (control_x, control_y))

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

# ==================== 브라우저 데이터 초기화 ====================
def reset_browser_data_in_session(driver):
    try:
        current_url = driver.current_url
        if not current_url or current_url == "data:,":
            try:
                driver.get("about:blank")
            except:
                print("   [Reset] ⚠️ about:blank 이동 실패")
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

        print("   [Reset] 🧹 세션 데이터 초기화 완료")
        return True

    except Exception as e:
        print(f"   [Reset] ⚠️ 데이터 초기화 실패: {e.__class__.__name__}")
        return False

# ==================== 드라이버 생성 ====================
def _strip_qvalues(lang_header: str) -> str:
    # "en-US,en;q=0.9" -> "en-US,en"
    parts = []
    for token in (lang_header or "").split(","):
        token = token.strip()
        if not token:
            continue
        parts.append(token.split(";", 1)[0].strip())
    return ",".join([p for p in parts if p])

def _pick_desktop_windows_ua(profile: Dict[str, Any]) -> Optional[str]:
    # A안: Windows 데스크탑 Chrome 계열만 허용 (모바일/사파리/안드로이드 제외)
    uas = profile.get("user_agents") or []
    filtered = []
    for ua in uas:
        if ("Windows NT" in ua) and ("Android" not in ua) and ("iPhone" not in ua) and ("iPad" not in ua):
            filtered.append(ua)
    return random.choice(filtered) if filtered else None

def create_undetected_driver(profile: Dict[str, Any], proxy: Optional[str], thread_id: int = 0):
    options = uc.ChromeOptions()

    temp_dir = tempfile.mkdtemp(prefix=f"monitor_profile_{thread_id}_")
    temp_dirs.append(temp_dir)
    options.add_argument(f"--user-data-dir={temp_dir}")

    # ✅ 지역 프로필 값
    locale = profile.get("locale", "en-US")
    tz = profile.get("timezone", "UTC")

    # UI 언어(크롬 UI/JS navigator.language에 영향)
    options.add_argument(f"--lang={locale}")

    # Accept-Language(HTTP 헤더) - q값 제거해서 깔끔하게
    accept_lang_raw = random.choice(profile.get("accept_languages", [locale]))
    accept_lang = _strip_qvalues(accept_lang_raw) or locale

    # ✅ A안: UA는 '가능하면 건드리지 않는 게' 가장 정합성이 좋음
    # 그래도 프로필 기반으로 돌리고 싶으면 "Windows 데스크탑 UA"만 제한해서 사용
    ua = _pick_desktop_windows_ua(profile)
    if ua:
        options.add_argument(f"--user-agent={ua}")

    # 해상도 랜덤 선택
    resolutions = profile.get("resolutions", ["800x700", "1024x768", "1280x800", "1366x768", "1920x1080"])
    chosen_res = random.choice(resolutions)
    width, height = map(int, chosen_res.split('x'))
    options.add_argument(f"--window-size={width},{height}")

    prefs = {
        "profile.default_content_setting_values.notifications": 2,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
        # WebRTC 누출 최소화(지금처럼 유지)
        "webrtc.ip_handling_policy": "disable_non_proxied_udp",
        "webrtc.multiple_routes_enabled": False,
        "webrtc.nonproxied_udp_enabled": False,
        # ✅ Accept-Language 정합
        "intl.accept_languages": accept_lang,
    }
    options.add_experimental_option("prefs", prefs)

    if HEADLESS:
        options.add_argument("--headless=new")
    if proxy:
        options.add_argument(f"--proxy-server={proxy}")

    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-first-run")
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
            driver.set_window_size(width, height)
        except Exception as e:
            print(f"[ERR] 드라이버 생성 실패: {e}")
            return None

    # ✅ 타임존/로케일은 CDP로 확실히 적용
    try:
        driver.execute_cdp_cmd("Emulation.setTimezoneOverride", {"timezoneId": tz})
    except Exception:
        pass

    try:
        driver.execute_cdp_cmd("Emulation.setLocaleOverride", {"locale": locale})
    except Exception:
        pass

    # ✅ 최소한의 webdriver 흔적만 (plugins/languages/canvas/audio/hw 스푸핑 제거!)
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"},
        )
    except Exception:
        pass

    return driver


# ==================== 페이지 로딩 검증 ====================
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
        if (text.includes('ERR_TIMED_OUT')) return true;
        if (text.includes('ERR_CONNECTION_TIMED_OUT')) return true;
        if (text.includes("This site can't be reached")) return true;
        if (text.includes("사이트에 연결할 수 없음")) return true;

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

# ==================== 메인 워커 ====================
def monitor_service(
    url: str,
    proxy_info: Dict[str, str],
    index: int,
    stop_event: threading.Event,
    redis_client: Optional[redis.Redis] = None,
):
    driver = None
    proxy_success = False
    start_time = datetime.now()

    try:
        if not REGION_PROFILES:
            print(f"[Bot-{index}] ❌ REGION_PROFILES가 비어있습니다.")
            return

        region = random.choice(list(REGION_PROFILES.keys()))
        profile = REGION_PROFILES[region]

        proxy_member = proxy_info["member"]
        latency = proxy_info["latency"]
        countries = proxy_info["countries"]
        proxy_type = proxy_info["proxy_type"]

        proxy_member = None
        print(f"\n{'='*60}")
        print(f"[Bot-{index}] 🚀 세션 시작")
        print(f"  Profile: {region} ({profile['timezone']})")
        print(f"  Proxy: {proxy_member}")
        print(f"  Latency: {latency:.1f}ms | Type: {proxy_type}")
        print(f"  Country: {countries}")
        print(f"  Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")

        if stop_event.is_set():
            return

        driver = create_undetected_driver(profile, proxy_member, index)
        if not driver:
            print(f"[Bot-{index}] ❌ 드라이버 생성 실패")
            return

        try:
            slot = index % max(1, NUM_BROWSERS)
            x = 50 + slot * (1200 + 40)
            y = 50
            if not HEADLESS:
                driver.set_window_position(x, y)
        except Exception as e:
            print(f"[Bot-{index}] ⚠ 창 위치 설정 실패: {e}")

        if stop_event.is_set():
            return

        driver.get("about:blank")
        reset_browser_data_in_session(driver)

        # Referer + 추가 헤더
        referer = random.choice(profile.get("referers", ["https://www.google.com/"]))
        accept_encoding = random.choice(["gzip, deflate, br", "gzip, deflate", "br"])
        extra_headers = {
            "Referer": referer,
            "Accept-Encoding": accept_encoding,
            "Connection": random.choice(["keep-alive", "close"])
        }
        try:
            driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": extra_headers})
        except Exception:
            pass

        print(f"[Bot-{index}] 🌐 접속 시도: {url}")
        browse_start = time.time()
        hard_deadline = browse_start + BROWSE_MAX_SECONDS

        try:
            driver.get(url)
        except TimeoutException:
            print(f"[Bot-{index}] ⚠️ Get 요청 타임아웃")

        remaining = hard_deadline - time.time()
        if remaining <= 0 or not ensure_page_ready(driver, timeout=min(ENSURE_TIMEOUT, max(5, remaining))):
            print(f"[Bot-{index}] ❌ 페이지 로딩 실패")
            return

        proxy_success = True

        # 체류 행동 강화
        stay_time = random.gauss(STAY_DURATION, 80)
        stay_time = max(30, min(stay_time, hard_deadline - time.time()))

        action_count = random.randint(1, 3)
        action_interval = stay_time / (action_count + 1)

        for i in range(action_count):
            if stop_event.is_set() or time.time() >= hard_deadline:
                break
            stop_event.wait(action_interval)
            try:
                body = driver.find_element(By.TAG_NAME, "body")
                human_mouse_move(driver, end_el=body)
                human_sleep(0.5, 2.0)
                human_scroll(driver)
            except Exception:
                pass

        remaining_wait = hard_deadline - time.time()
        if remaining_wait > 0:
            stop_event.wait(remaining_wait)

        duration = (datetime.now() - start_time).total_seconds()
        print(f"[Bot-{index}] ✅ 모니터링 완료 (소요: {duration:.1f}초)")

    except Exception as e:
        print(f"[Bot-{index}] 🛑 오류: {e.__class__.__name__}: {e}")

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        
        if redis_client:
            mark_proxy_used(redis_client, proxy_info["member"], success=proxy_success)

# ==================== 임시 디렉토리 정리 및 통계 ====================
# (기존 cleanup_temp_dirs, SessionStats, 메인 루프 부분 그대로 유지)

def cleanup_temp_dirs():
    print("\n🧹 임시 파일 정리 중...")
    count = 0
    for d in temp_dirs:
        try:
            if os.path.exists(d):
                shutil.rmtree(d)
                count += 1
        except Exception as e:
            print(f"   ⚠️ 정리 실패: {d} - {e}")
    if count > 0:
        print(f"   ✅ {count}개 디렉토리 정리 완료")

atexit.register(cleanup_temp_dirs)

class SessionStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.total_sessions = 0
        self.successful_sessions = 0
        self.failed_sessions = 0
        self.start_time = datetime.now()
    
    def record_session(self, success: bool):
        with self.lock:
            self.total_sessions += 1
            if success:
                self.successful_sessions += 1
            else:
                self.failed_sessions += 1
    
    def print_stats(self):
        with self.lock:
            runtime = (datetime.now() - self.start_time).total_seconds()
            success_rate = (self.successful_sessions / max(1, self.total_sessions)) * 100
            
            print(f"\n{'='*60}")
            print(f"📊 세션 통계")
            print(f"{'='*60}")
            print(f"  실행 시간: {runtime/60:.1f}분")
            print(f"  총 세션: {self.total_sessions}")
            print(f"  성공: {self.successful_sessions} ({success_rate:.1f}%)")
            print(f"  실패: {self.failed_sessions}")
            print(f"{'='*60}\n")

if __name__ == "__main__":
    print(f"{'='*60}")
    print(f"🛡️ 개선된 Redis 기반 Stealth Monitor (Fingerprint 강화 버전)")
    print(f"{'='*60}")
    print(f"Target: {TARGET_URL}")
    print(f"동시 브라우저: {NUM_BROWSERS}")
    print(f"{'='*60}\n")

    if not REGION_PROFILES:
        print("[MAIN] ❌ REGION_PROFILES가 비어있습니다.")
        exit(1)

    r = get_redis()
    stats = SessionStats()
    threads: List[threading.Thread] = []
    worker_index = 0
    last_cleanup = time.time()

    try:
        while not stop_event.is_set():
            if time.time() - last_cleanup > 600:
                cleanup_old_proxy_records(r)
                last_cleanup = time.time()
                stats.print_stats()

            alive_threads = [t for t in threads if t.is_alive()]
            if len(alive_threads) != len(threads):
                print(f"[MAIN] 🔄 스레드 정리: {len(threads)} → {len(alive_threads)}")
            threads = alive_threads

            capacity = max(0, NUM_BROWSERS - len(threads))

            no_proxy_available = False
            for _ in range(capacity):
                if stop_event.is_set():
                    break

                proxy_info = pick_proxy(r, prefer_countries=PREFER_COUNTRIES, top_n=100)
                
                if not proxy_info:
                    no_proxy_available = True
                    print("[MAIN] ⚠️ 사용 가능한 프록시 없음")
                    break

                idx = worker_index
                worker_index += 1

                t = threading.Thread(
                    target=monitor_service,
                    args=(TARGET_URL, proxy_info, idx, stop_event, r),
                )
                t.start()
                threads.append(t)
                time.sleep(random.uniform(5, 15))

            if no_proxy_available and not threads:
                print(f"[MAIN] 💤 프록시 없음. {WAIT_WHEN_NO_PROXY_SECONDS}초 대기")
                for _ in range(WAIT_WHEN_NO_PROXY_SECONDS):
                    if stop_event.is_set():
                        break
                    time.sleep(1)
            else:
                time.sleep(2)

    except KeyboardInterrupt:
        print("\n[MAIN] ⚠️ Ctrl+C 감지. 종료 중...")
        stop_event.set()

    finally:
        print("\n[MAIN] 종료 처리 중...")
        for i, t in enumerate(threads):
            if t.is_alive():
                t.join(timeout=10)
        stats.print_stats()
        cleanup_temp_dirs()
        print("\n" + "="*60)
        print("✅ 모든 작업 완료")
        print("="*60)