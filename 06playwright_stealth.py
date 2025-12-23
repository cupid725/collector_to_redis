import random
import threading
import time
import json
import redis
import os
import sys
import signal
from playwright.sync_api import sync_playwright

# ===================== 1. 설정 및 데이터 로드 =====================

# 지역별 설정 로드 (locale, timezone 등)
REGION_PROFILES = {}
try:
    if os.path.exists('region_profiles.json'):
        with open('region_profiles.json', 'r', encoding='utf-8') as f:
            REGION_PROFILES = json.load(f)
        print(f"✅ 지역 프로필 로드 완료 ({len(REGION_PROFILES)}개 지역)")
    else:
        print("⚠️ region_profiles.json 파일이 없습니다. 기본 설정(en-US)을 사용합니다.")
except Exception as e:
    print(f"❌ 지역 프로필 로드 실패: {e}")

# 유입 경로(Referer) 목록
REFERERS = [
    "https://www.google.com/",
    "https://www.facebook.com/",
    "https://twitter.com/",
    "https://t.co/",
    "https://www.instagram.com/",
    "https://www.bing.com/",
    "https://duckduckgo.com/"
]

TARGET_URL = "https://www.youtube.com/shorts/u7sO-mNEpT4?feature=share"
TARGET_URL1 = "https://youtube.com/shorts/-vVnZoVtnFk?feature=share"

NUM_BROWSERS = 3 
MOBILE_DEVICES_LIST = [
    'Pixel 5', 'Pixel 4', 'iPhone 13', 'iPhone 12', 'iPhone 11', 'iPhone SE'
]

REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_LEASE = "proxies:lease"

# 쿨타임 설정
SUCCESS_COOL_DOWN = 3600  # 1시간
FAILURE_PENALTY = 86400   # 24시간

# ===================== 2. 유틸리티 및 스텔스 로직 =====================

def get_redis():
    return redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

def apply_stealth_and_custom(page, config):
    """라이브러리 호환성 및 수동 우회 로직 통합"""
    try:
        from playwright_stealth import Stealth
        # 인자 오류 방지를 위한 계층적 시도
        try:
            stealth_obj = Stealth(nav_webdriver=True)
        except:
            try: stealth_obj = Stealth()
            except: stealth_obj = None

        if stealth_obj and hasattr(stealth_obj, 'apply'):
            stealth_obj.apply(page)
        
        # 수동 속성 주입 (언어 설정 및 웹드라이버 숨기기)
        page.add_init_script(f"""
            Object.defineProperty(navigator, 'webdriver', {{ get: () => undefined }});
            Object.defineProperty(navigator, 'languages', {{ get: () => ['{config.get("locale", "en-US")}', 'en'] }});
            Object.defineProperty(navigator, 'maxTouchPoints', {{ get: () => 5 }});
        """)
    except:
        pass

def calculate_window_position(index):
    width, height = 640, 800
    x = (index % 3) * 650
    y = (index // 3) * 850
    return {'x': x, 'y': y, 'width': width, 'height': height}

# ===================== 3. 메인 워커 (Referer & Region 반영) =====================

def monitor_service(url, proxy_url, index, stop_event, r):
    success = False
    # 지역 프로필 랜덤 선택 (또는 IP 분석 기반 가능)
    region_key = random.choice(list(REGION_PROFILES.keys())) if REGION_PROFILES else "US"
    config = REGION_PROFILES.get(region_key, {"locale": "en-US", "timezone": "America/New_York"})
    referer = random.choice(REFERERS)
    device_name = random.choice(MOBILE_DEVICES_LIST)

    print(f"[Bot-{index}] 🚀 시작 | 📱 {device_name} | 🌍 {region_key} | 🔗 {proxy_url}")
    print(f"   [INFO] Referer: {referer}")

    from playwright.sync_api import sync_playwright
    playwright_mgr = sync_playwright().start()
    browser = None
    
    try:
        device_info = playwright_mgr.devices[device_name]
        pos = calculate_window_position(index)
        
        browser = playwright_mgr.chromium.launch(
            headless=False,
            proxy={"server": proxy_url},
            args=[
                f"--window-position={pos['x']},{pos['y']}",
                "--disable-blink-features=AutomationControlled"
            ]
        )

        context = browser.new_context(
            **device_info,
            locale=config['locale'],
            timezone_id=config['timezone']
        )
        page = context.new_page()
        apply_stealth_and_custom(page, config)

        # Referer를 적용하여 페이지 이동
        #page.goto(url, referer=referer, wait_until="commit", timeout=60000)
        page.goto(
            url, 
            referer=referer, 
            wait_until="domcontentloaded", # 'commit'보다 조금 더 일찍 성공으로 간주
            timeout=120000                 # 120초로 확장
        )
        
        # 시청 로직
        watch_duration = random.uniform(240, 300)
        elapsed = 0
        last_v_time = 0
        
        while elapsed < watch_duration and not stop_event.is_set():
            time.sleep(5)
            elapsed += 5
            try:
                # 비디오 상태 체크
                status = page.evaluate("() => { const v = document.querySelector('video'); return v ? {t: v.currentTime, p: v.paused} : null; }")
                if status:
                    is_playing = not status['p'] and status['t'] > last_v_time
                    print(f"[Bot-{index}] {'▶️' if is_playing else '⏸️'} {elapsed:.0f}/{watch_duration:.0f}s")
                    last_v_time = status['t']
                else:
                    # 영상이 없으면(차단 등) 즉시 종료
                    break
            except: break
        
        if elapsed >= watch_duration:
            success = True
            print(f"[Bot-{index}] ✅ 시청 완료")

    except Exception as e:
        print(f"[Bot-{index}] 🛑 에러: {str(e)[:100]}")
    finally:
        try:
            if browser: browser.close()
            playwright_mgr.stop()
        except: pass
        
        # Redis 쿨타임/페널티 적용
        if r and proxy_url:
            r.zrem(REDIS_ZSET_LEASE, proxy_url)
            score = int(time.time()) + (SUCCESS_COOL_DOWN if success else FAILURE_PENALTY)
            r.zadd(REDIS_ZSET_ALIVE, {proxy_url: score})
            print(f"[Bot-{index}] 🕒 쿨타임/페널티 적용 완료")

# ===================== 4. 메인 제어 루프 (LUA) =====================

_LUA_CLAIM = r"""
local alive, lease = KEYS[1], KEYS[2]
local now, l_sec = tonumber(ARGV[1]), tonumber(ARGV[2])
local expired = redis.call('ZRANGEBYSCORE', lease, '-inf', now)
for _, m in ipairs(expired) do
    redis.call('ZREM', lease, m)
    redis.call('ZADD', alive, 0, m)
end
local cands = redis.call('ZRANGEBYSCORE', alive, '-inf', now, 'LIMIT', 0, 1)
if #cands == 0 then return nil end
local target = cands[1]
redis.call('ZREM', alive, target)
redis.call('ZADD', lease, now + l_sec, target)
return target
"""

def main():
    r = get_redis()
    active_slots = {}
    stop_event = threading.Event()

    def signal_handler(sig, frame):
        stop_event.set()
        print("\n🛑 중단 요청... 모든 브라우저를 닫습니다.")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    print("=" * 60)
    print("📡 Playwright Multi-Proxy Bot v3.0")
    print("📡 Region, Referer, Stealth 통합 버전")
    print("=" * 60)

    try:
        while not stop_event.is_set():
            # 종료된 스레드 정리
            for s in list(active_slots.keys()):
                if not active_slots[s].is_alive():
                    del active_slots[s]
            
            # 빈 슬롯 채우기
            if len(active_slots) < NUM_BROWSERS:
                for s in range(NUM_BROWSERS):
                    if s not in active_slots:
                        # Redis에서 쿨타임 안 걸린 IP 가져오기
                        proxy = r.eval(_LUA_CLAIM, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, int(time.time()), 600)
                        if proxy:
                            url = TARGET_URL if s % 2 == 0 else TARGET_URL1
                            t = threading.Thread(target=monitor_service, args=(url, proxy, s, stop_event, r), daemon=True)
                            t.start()
                            active_slots[s] = t
                            break
            time.sleep(2)
    except Exception as e:
        print(f"메인 루프 에러: {e}")

if __name__ == "__main__":
    main()