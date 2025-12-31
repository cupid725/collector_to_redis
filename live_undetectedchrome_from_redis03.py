import random
import threading
import time
import json
from typing import Dict, Any, Optional
from live_human_events import HumanEvent
from stealth_browser import StealthBrowser  # ✅ 새로운 브라우저 클래스 임포트

# 외부 라이브러리
import redis

from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
)

# 전역 중단 이벤트
stop_event = threading.Event()

# ===================== Redis 설정 =====================
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None

# Lease 방식 키
REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_LEASE = "proxies:lease"
REDIS_HASH_FAIL = "proxies:fail"
REDIS_ZSET_USED = "proxies:used_recent"

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

-- 3) 랜덤 1개 선택
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
    """alive에서 프록시 1개를 임대(claim)"""
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
    """임대된 프록시를 alive로 반납(release)"""
    next_time = int(time.time()) + max(0, int(cooldown_seconds))
    try:
        r.eval(_LUA_RELEASE, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, member, next_time)
    except redis.RedisError as e:
        print(f"[REDIS] release_proxy 실패: {e}")

def ban_proxy(r: redis.Redis, member: str) -> None:
    """문제 프록시를 풀에서 제거(ban)"""
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
    """최근 사용 기록만 남김"""
    try:
        r.zadd(REDIS_ZSET_USED, {member: time.time()})
    except redis.RedisError:
        pass

# ===================== REGION_PROFILES 로드 =====================
def load_region_profiles(json_path: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    import os
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
TARGET_URL1 = "https://www.youtube.com/shorts/-vVnZoVtnFk?feature=share"  # 크리스마스
TARGET_URL = "https://www.youtube.com/shorts/u7sO-mNEpT4?feature=share"  # 크리스마스 2

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

# 화면 크기 설정 (슬롯 배치용)
SCREEN_WIDTH = WINDOW_WIDTH * NUM_BROWSERS + 40 * (NUM_BROWSERS - 1) - 200
SCREEN_HEIGHT = WINDOW_HEIGHT + 100 - 200

# Lease 운영 파라미터
LEASE_SECONDS = max(120, int(ENSURE_TIMEOUT + STAY_DURATION + 120))
COOLDOWN_SUCCESS = 0
COOLDOWN_FAIL_BASE = 30
COOLDOWN_FAIL_JITTER = 60
MAX_FAIL = 5

# ===================== 유틸리티 함수 =====================
def is_driver_alive(driver) -> bool:
    """드라이버 세션 생존 확인 (StealthBrowser.is_alive() 사용 가능)"""
    try:
        handles = driver.window_handles
        if not handles:
            return False
        driver.execute_script("return 1;")
        return True
    except Exception:
        return False

def smart_wait(driver, stop_event, timeout: float, index: int, check_interval: float = 0.5) -> bool:
    """
    timeout 동안 대기하되, check_interval마다 stop_event/브라우저 생존을 체크
    - True: 정상적으로 timeout까지 기다림
    - False: stop_event 또는 브라우저 종료 감지로 조기 중단
    """
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

def get_and_error_if_new_tab(driver, url, *, max_wait=2.0, poll=0.05, close_new=True):
    """새 탭/창이 열리면 에러 발생"""
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

# ===================== 메인 워커 (리팩토링) =====================
def monitor_service(
    url: str,
    proxy_member: str,
    slot_index: int,
    stop_event: threading.Event,
    redis_client: Optional[redis.Redis] = None,
):
    """
    ✅ StealthBrowser 클래스를 사용한 슬롯 기반 워커 함수
    """
    session_ok = False
    browser = None

    try:
        if not REGION_PROFILES:
            print(f"[Slot-{slot_index}] ❌ REGION_PROFILES가 비어 있습니다.")
            return

        region = random.choice(list(REGION_PROFILES.keys()))
        profile = REGION_PROFILES[region]

        print(f"\n[Slot-{slot_index}] 🌍 Profile: {region} ({profile['timezone']})")
        print(f"[Slot-{slot_index}] 🧩 Proxy(leased): {proxy_member}")

        if stop_event.is_set():
            print(f"[Slot-{slot_index}] 🛑 시작 전 중단 신호 수신. 종료.")
            return

        # ✅ StealthBrowser 인스턴스 생성
        browser = StealthBrowser(
            profile=profile,
            proxy=proxy_member,
            slot_index=slot_index,
            headless=HEADLESS,
            command_timeout=COMMAND_TIMEOUT,
            load_timeout=LOAD_TIMEOUT,
            window_width=WINDOW_WIDTH,
            window_height=WINDOW_HEIGHT,
            screen_width=SCREEN_WIDTH,
            screen_height=SCREEN_HEIGHT,
            total_slots=NUM_BROWSERS,
        )

        # 드라이버 생성
        driver, temp_dir = browser.create_driver()
        if not driver:
            print(f"[Slot-{slot_index}] ❌ 드라이버 생성 실패.")
            return

        # 초기 페이지
        try:
            driver.get("about:blank")
            print(f"[Slot-{slot_index}] 초기 페이지(about:blank) 로드 완료")
        except Exception as e:
            print(f"[Slot-{slot_index}] ⚠️ 초기 페이지 로드 실패: {e}")
            return

        # 브라우저 데이터 초기화
        browser.reset_browser_data()

        # Referer 설정
        referer = random.choice(profile["referers"])
        browser.set_referer(referer)

        # 랜덤 대기 후 타겟 페이지 접속
        pre_nav_delay = random.uniform(1.0, 3.0)
        print(f"[Slot-{slot_index}] ⏳ 접속 전 {pre_nav_delay:.1f}초 대기...")
        time.sleep(pre_nav_delay)

        # 타겟 페이지 접속
        print(f"[Slot-{slot_index}] 접속 요청: {url}")
        browse_start = time.time()
        hard_deadline = browse_start + BROWSE_MAX_SECONDS

        try:
            try:
                get_and_error_if_new_tab(driver, url, max_wait=2.0, close_new=True)
            except RuntimeError as e:
                print(f"[Slot-{slot_index}] ⚠️[ERR] 새 탭/창 자동 오픈 감지: {e}")
                return

            # 유튜브 동의 페이지 처리
            clicked = browser.click_youtube_consent()
            if not clicked:
                try:
                    from selenium.webdriver.support.ui import WebDriverWait
                    WebDriverWait(driver, 5).until(
                        lambda d: "consent.youtube.com" in d.current_url
                    )
                    browser.click_youtube_consent()
                except TimeoutException:
                    pass

        except TimeoutException:
            print(f"[Slot-{slot_index}] ⚠️ Get 요청 타임아웃. 로딩 상태 확인 시도.")

        # 에러 페이지 확인
        if browser.page_looks_like_error():
            print(f"[Slot-{slot_index}] ⏰ 에러페이지로 의심. 세션 종료.")
            return

        remaining_for_load = hard_deadline - time.time()
        if remaining_for_load <= 0:
            print(f"[Slot-{slot_index}] ⏰ 브라우징 최대 시간({BROWSE_MAX_SECONDS}초) 도달(로딩 대기 중). 세션 종료.")
            return

        # 페이지 로딩 완료 대기
        if not browser.ensure_page_ready(timeout=min(ENSURE_TIMEOUT, max(5, remaining_for_load))):
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

        # 체류 시간 계산
        remaining = hard_deadline - time.time()
        if remaining <= 0:
            print(f"[Slot-{slot_index}] ⏰ 브라우징 최대 시간({BROWSE_MAX_SECONDS}초) 도달(체류 전). 세션 종료.")
            return

        stay_time = max(10, random.gauss(STAY_DURATION, 10))
        stay_time = min(stay_time, remaining)

        # 휴먼 이벤트 타이밍 계산
        human_event_timing = min(HUMAN_EVENT_BEFORE_END_SECONDS, stay_time - HUMAN_EVENT_BEFORE_END_SECONDS)

        human_event = HumanEvent(driver)

        if human_event_timing <= 5:
            print(f"[Slot-{slot_index}] 체류 시작 (총 {stay_time:.1f}초, 즉시 휴먼 이벤트 실행)")
            human_event.execute_random_action()

            print(f"[Slot-{slot_index}] ⏳ 휴먼 이벤트 후 10초 대기...")
            if not smart_wait(driver, stop_event, 10, slot_index):
                return
            print(f"[Slot-{slot_index}] 모니터링 정상 종료.")
            return
        else:
            after_event_wait = stay_time - human_event_timing

            print(f"[Slot-{slot_index}] 체류 시작 (총 {stay_time:.1f}초: 대기 {human_event_timing:.1f}초 → 휴먼 이벤트 → 마무리 {after_event_wait:.1f}초)")

            if not smart_wait(driver, stop_event, human_event_timing, slot_index):
                return
            if stop_event.is_set():
                return

            human_event.execute_random_action()

            print(f"[Slot-{slot_index}] ⏳ 휴먼 이벤트 후 20초 대기...")
            if not smart_wait(driver, stop_event, 20, slot_index):
                return
            print(f"[Slot-{slot_index}] 모니터링 정상 종료.")
            return

    except Exception as e:
        print(f"[Slot-{slot_index}] 🛑 오류 발생: {e.__class__.__name__}: {e}")

    finally:
        # ✅ StealthBrowser의 close() 메서드로 정리
        if browser:
            browser.close()

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
    """남은 임시 파일 정리"""
    import tempfile
    import os
    import shutil
    
    print("\n🧹 남은 임시 파일 확인 중...")
    cleaned = 0
    failed = 0
    try:
        temp_base = tempfile.gettempdir()
        for item in os.listdir(temp_base):
            if item.startswith("stealth_browser_"):
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

    # 슬롯 기반 관리: {슬롯번호: 쓰레드객체}
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