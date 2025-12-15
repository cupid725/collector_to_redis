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
)

# 🔒 드라이버 생성 시 동시 접근 방지용 Lock
driver_creation_lock = threading.Lock()
temp_dirs = []  # 생성된 임시 디렉토리 목록

# 🔥 모든 스레드에 중단 신호를 보내기 위한 전역 Event
stop_event = threading.Event()

# ===================== Redis 설정 =====================
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None

REDIS_ZSET_ALIVE = "proxies:alive"        # collector_redis에서 넣는 풀
REDIS_ZSET_USED  = "proxies:used_recent"  # 이번/최근에 소비된 프록시 기록용

def get_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )

def pick_proxy(
    r: redis.Redis,
    prefer_protocol: Optional[str] = None,
    top_n: int = 50,
) -> Optional[str]:
    """
    Redis ZSET 'proxies:alive' 에서 프록시 하나 선택.
    - member 형식: "http://1.2.3.4:8080" or "socks5://5.6.7.8:1080"
    - prefer_protocol: "http" 또는 "socks5" 선호 가능
    반환: "protocol://ip:port" 또는 None
    """
    members = r.zrange(REDIS_ZSET_ALIVE, 0, top_n - 1)
    if not members:
        return None

    if prefer_protocol:
        filtered = []
        for m in members:
            if "://" not in m:
                continue
            proto, _ = m.split("://", 1)
            if proto == prefer_protocol:
                filtered.append(m)
        if filtered:
            members = filtered

    candidates = [m for m in members if "://" in m]
    if not candidates:
        return None

    return random.choice(candidates)

def mark_proxy_used(r: redis.Redis, member: str):
    """
    선택된 프록시를 '사용 완료'로 처리:
    - proxies:alive 에서 제거(ZREM)
    - proxies:used_recent 에 timestamp score로 기록
    """
    now_ts = time.time()
    pipe = r.pipeline()
    pipe.zrem(REDIS_ZSET_ALIVE, member)
    pipe.zadd(REDIS_ZSET_USED, {member: now_ts})
    try:
        pipe.execute()
    except redis.RedisError as e:
        print(f"[REDIS] mark_proxy_used 실패: {e}")

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
TARGET_URL = "https://www.youtube.com/shorts/mcy0JKTavW4?feature=share"
TARGET_URL = "https://youtube.com/shorts/-vVnZoVtnFk?feature=share"

COMMAND_TIMEOUT = 300
LOAD_TIMEOUT = COMMAND_TIMEOUT
ENSURE_TIMEOUT = 300
BROWSE_MAX_SECONDS = ENSURE_TIMEOUT  # 브라우징 시작 후 최대 허용 시간(초)
STAY_DURATION = 600                  # 체류 시간 평균(초)
WINDOW_WIDTH = 800
WINDOW_HEIGHT = 700
NUM_BROWSERS = 2                     # 동시에 띄울 브라우저 최대 개수
HEADLESS = False

WAIT_WHEN_NO_PROXY_SECONDS = 60      # 프록시 없고 스레드도 없을 때 재시도 전 대기

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
    """자연스러운 스크롤 (필요하면 사용)"""
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

# ===================== undetected_chromedriver 생성 =====================
def create_undetected_driver(profile: Dict[str, Any], proxy: Optional[str], thread_id: int = 0):
    options = uc.ChromeOptions()

    temp_dir = tempfile.mkdtemp(prefix=f"monitor_profile_{thread_id}_")
    temp_dirs.append(temp_dir)
    options.add_argument(f"--user-data-dir={temp_dir}")
    options.add_argument(f"--timezone-id={profile['timezone']}")

    ua = random.choice(profile["user_agents"])
    options.add_argument(f"--user-agent={ua}")
    options.add_argument(f"--lang={profile['locale']}")

    prefs = {
        "profile.default_content_setting_values.notifications": 2,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
        "webrtc.ip_handling_policy": "disable_non_proxied_udp",
        "webrtc.multiple_routes_enabled": False,
        "webrtc.nonproxied_udp_enabled": False,
        "intl.accept_languages": random.choice(profile["accept_languages"]),
    }
    options.add_experimental_option("prefs", prefs)

    if HEADLESS:
        options.add_argument("--headless=new")
    if proxy:
        options.add_argument(f"--proxy-server={proxy}")

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
            driver.set_window_size(WINDOW_WIDTH, WINDOW_HEIGHT)
        except Exception as e:
            print(f"[ERR] Driver creation failed: {e}")
            return None

    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                """
            },
        )
    except Exception:
        pass

    return driver

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

        if (text.includes("This site can’t be reached")) return true;

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

def print_proxy_ip_and_country(driver, index: int):
    return

# ===================== 유튜브 로그인/로봇확인 interstitial 감지 =====================
def is_youtube_interstitial_login(driver, index: int) -> bool:
    """
    - yt-player-interstitial-renderer 안에
      accounts.google.com/ServiceLogin?service=youtube 로 가는 링크가 있으면
      → 로그인/로봇확인 interstitial로 간주
    - 언어 독립적으로 동작
    """
    try:
        result = driver.execute_script(
            """
            try {
              const href = window.location.href || "";

              // 완전한 로그인 페이지로 튄 케이스
              if (href.includes("accounts.google.com/ServiceLogin") &&
                  href.includes("service=youtube")) {
                return true;
              }

              // 재생 영역 위에 뜨는 interstitial 레이어
              const link = document.querySelector(
                'yt-player-interstitial-renderer a[href*="accounts.google.com/ServiceLogin"][href*="service=youtube"]'
              );
              if (link) return true;

              const link2 = document.querySelector(
                '.yt-player-interstitial-renderer a[href*="accounts.google.com/ServiceLogin"][href*="service=youtube"]'
              );
              if (link2) return true;

              return false;
            } catch (e) {
              return false;
            }
            """
        )
    except WebDriverException:
        result = False

    if result:
        print(f"[Bot-{index}] 🔐 유튜브 interstitial 로그인/로봇확인 레이어 감지 → 세션 종료.")
    return bool(result)

# ===================== 메인 워커 =====================
def monitor_service(
    url: str,
    proxy_member: str,
    index: int,
    stop_event: threading.Event,
    redis_client: Optional[redis.Redis] = None,
):
    driver = None

    try:
        if not REGION_PROFILES:
            print(f"[Bot-{index}] ❌ REGION_PROFILES가 비어 있습니다. region_profiles.json 로드를 확인하세요.")
            return

        region = random.choice(list(REGION_PROFILES.keys()))
        profile = REGION_PROFILES[region]

        print(f"\n[Bot-{index}] 🌐 Profile: {region} ({profile['timezone']})")
        print(f"[Bot-{index}] 🧩 Proxy: {proxy_member}")

        if stop_event.is_set():
            print(f"[Bot-{index}] 🛑 시작 전 중단 신호 수신. 종료.")
            return

        proxy_for_chrome = proxy_member

        driver = create_undetected_driver(profile, proxy_for_chrome, index)
        if not driver:
            print(f"[Bot-{index}] ❌ 드라이버 생성 실패.")
            return

        # 🔸 창 위치 슬롯별로 배치 (겹치지 않게)
        try:
            slot = index % max(1, NUM_BROWSERS)
            base_x = 50
            base_y = 50
            gap_x = WINDOW_WIDTH + 40  # 창 너비 + 간격
            x = base_x + slot * gap_x
            y = base_y
            if not HEADLESS:
                driver.set_window_position(x, y)
                print(f"[Bot-{index}] 🪟 창 위치 설정: ({x}, {y}) [slot {slot}]")
        except Exception as e:
            print(f"[Bot-{index}] ⚠ 창 위치 설정 실패: {e}")

        if stop_event.is_set():
            print(f"[Bot-{index}] 🛑 드라이버 생성 후 중단 신호 수신. 종료.")
            return

        # 초기 페이지
        try:
            driver.get("about:blank")
            print(f"[Bot-{index}] 초기 페이지(about:blank) 로드 완료")
        except Exception as e:
            print(f"[Bot-{index}] ⚠️ 초기 페이지 로드 실패: {e}")
            return

        reset_browser_data_in_session(driver)

        # Referer 설정
        referer = random.choice(profile["referers"])
        try:
            driver.execute_cdp_cmd(
                "Network.setExtraHTTPHeaders", {"headers": {"Referer": referer}}
            )
            print(f"[Bot-{index}] Referer: {referer}")
        except Exception as e:
            print(f"[Bot-{index}] ⚠ Referer 설정 실패: {e}")

        print_proxy_ip_and_country(driver, index)

        # 타겟 페이지 접속
        print(f"[Bot-{index}] 접속 요청: {url}")
        browse_start = time.time()
        hard_deadline = browse_start + BROWSE_MAX_SECONDS

        try:
            driver.get(url)
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
            print(f"[Bot-{index}] ⚠️ Get 요청 타임아웃. 로딩 상태 확인 시도.")

        # 브라우징 최대 시간 내에서만 페이지 로딩을 기다림
        remaining_for_load = hard_deadline - time.time()
        if remaining_for_load <= 0:
            print(f"[Bot-{index}] ⏰ 브라우징 최대 시간({BROWSE_MAX_SECONDS}초) 도달(로딩 대기 중). 세션 종료.")
            return

        if not ensure_page_ready(driver, timeout=min(ENSURE_TIMEOUT, max(5, remaining_for_load))):
            print(f"[Bot-{index}] ❌ 페이지 로딩 실패로 종료.")
            return

        # 로그인/로봇확인 interstitial 감지
        #if is_youtube_interstitial_login(driver, index):
        #    return

        # 로딩 후에도 브라우징 최대 시간을 넘기지 않도록 남은 시간 계산
        remaining = hard_deadline - time.time()
        if remaining <= 0:
            print(f"[Bot-{index}] ⏰ 브라우징 최대 시간({BROWSE_MAX_SECONDS}초) 도달(로딩 직후). 세션 종료.")
            return

        reaction_time = random.uniform(0.8, 2.5)
        reaction_time = min(reaction_time, remaining)
        if reaction_time <= 0:
            print(f"[Bot-{index}] ⏰ 브라우징 최대 시간({BROWSE_MAX_SECONDS}초)로 인지 대기 생략.")
        else:
            print(f"[Bot-{index}] ✅ 로딩 완료. 인지 반응 대기: {reaction_time:.2f}초 (남은 상한: {remaining:.1f}초)")
            stop_event.wait(timeout=reaction_time)
        if stop_event.is_set():
            print(f"[Bot-{index}] 🛑 인지 대기 중 중단 신호. 종료.")
            return

        # === 여기부터 체류 + 종료 10초 전 휴먼 이벤트 ===
        remaining = hard_deadline - time.time()
        if remaining <= 0:
            print(f"[Bot-{index}] ⏰ 브라우징 최대 시간({BROWSE_MAX_SECONDS}초) 도달(체류 전). 세션 종료.")
            return

        stay_time = random.gauss(STAY_DURATION, 10)
        stay_time = max(10, stay_time)
        if stay_time > remaining:
            stay_time = remaining

        action_offset = 10.0  # 종료 10초 전에 휴먼 이벤트 실행

        if stay_time <= action_offset:
            print(
                f"[Bot-{index}] 체류 시작 (총 {stay_time:.1f}초, 즉시 휴먼 이벤트 실행 후 대기)"
            )
            try:
                body = driver.find_element(By.TAG_NAME, "body")
                human_mouse_move(driver, end_el=body)
            except Exception:
                pass
            human_scroll(driver)

            was_interrupted = stop_event.wait(timeout=stay_time)
            if was_interrupted:
                print(f"[Bot-{index}] 🛑 메인 프로세스 중단 신호 수신. 체류 중단.")
                return
        else:
            pre_wait = stay_time - action_offset
            print(
                f"[Bot-{index}] 체류 시작 (총 {stay_time:.1f}초, "
                f"{pre_wait:.1f}초 후 휴먼 이벤트 실행, 이후 10초 유지)"
            )
            was_interrupted = stop_event.wait(timeout=pre_wait)
            if was_interrupted or stop_event.is_set():
                print(f"[Bot-{index}] 🛑 휴먼 이벤트 전 중단 신호. 종료.")
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
                was_interrupted = stop_event.wait(timeout=tail)
                if was_interrupted:
                    print(f"[Bot-{index}] 🛑 휴먼 이벤트 이후 중단 신호. 종료.")
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

# ===================== 임시 디렉토리 정리 =====================
def cleanup_temp_dirs():
    print("\n🧹 임시 파일 정리 중...")
    for d in temp_dirs:
        try:
            shutil.rmtree(d)
        except Exception:
            pass
    print("   [Cleanup] 완료.")

# ===================== 메인 (워커 스케줄러) =====================
if __name__ == "__main__":
    print(f"=== 🛡️ Redis 기반 Stealth Monitor Started (TARGET_URL: {TARGET_URL}) ===")

    if not REGION_PROFILES:
        print("[MAIN] ❌ REGION_PROFILES가 비어 있습니다. region_profiles.json 상태를 확인하세요.")
        exit(1)

    r = get_redis()

    threads: list[threading.Thread] = []
    worker_index = 0  # Bot-0, Bot-1, Bot-2... 이런 식으로 순번만 올림
    cycle = 0

    try:
        while not stop_event.is_set():
            cycle += 1

            # 1) 죽은 스레드 정리
            alive_threads = []
            for t in threads:
                if t.is_alive():
                    alive_threads.append(t)
            if len(alive_threads) != len(threads):
                print(f"[MAIN] 🔁 스레드 정리: {len(threads)} → {len(alive_threads)} alive")
            threads = alive_threads

            current_alive = len(threads)
            capacity = max(0, NUM_BROWSERS - current_alive)

            # 2) 여유 슬롯만큼 새 워커 생성 시도
            no_proxy_available = False
            for _ in range(capacity):
                if stop_event.is_set():
                    break

                proxy_member = pick_proxy(
                    r,
                    prefer_protocol=None,
                    top_n=50,
                )
                if not proxy_member:
                    no_proxy_available = True
                    print("[MAIN] ⚠️ 사용할 프록시가 더 이상 없습니다. (지금은 새 워커 생성 불가)")
                    break

                mark_proxy_used(r, proxy_member)

                idx = worker_index
                worker_index += 1

                print(f"[MAIN] ▶ 새 워커 Bot-{idx} 시작, 프록시: {proxy_member}")
                t = threading.Thread(
                    target=monitor_service,
                    args=(TARGET_URL, proxy_member, idx, stop_event, r),
                )
                t.start()
                threads.append(t)

                # 스폰 간 약간 랜덤 딜레이
                time.sleep(random.uniform(5, 15))

            # 3) 프록시도 없고, 돌고 있는 스레드도 없으면 → 길게 대기
            if no_proxy_available and not threads:
                print(f"[MAIN] ⚠️ 프록시 없음 + 활성 워커 0 ⇒ {WAIT_WHEN_NO_PROXY_SECONDS}초 대기 후 재시도.")
                for _ in range(WAIT_WHEN_NO_PROXY_SECONDS):
                    if stop_event.is_set():
                        break
                    time.sleep(1)
            else:
                # 짧게 쉬면서 루프를 계속 돌림
                time.sleep(2)

    except KeyboardInterrupt:
        print("\n[MAIN] Ctrl+C (KeyboardInterrupt) 수신. Graceful Shutdown 시작.")
        stop_event.set()

    finally:
        # 남은 스레드 정리
        for t in threads:
            if t.is_alive():
                t.join(timeout=10)

        cleanup_temp_dirs()
        print("\n=== ✅ 모든 작업 완료 및 정리 완료 ===")
