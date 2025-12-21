import random
import threading
import time
import json
import redis
from playwright.sync_api import sync_playwright
from undetected_playwright import Tarnished  # undetected-playwright

# ===================== 1. 설정 및 타임아웃 변수 =====================
# === 타겟 URL 설정 ===
TARGET_URL = "https://www.youtube.com/shorts/u7sO-mNEpT4?feature=share"  # 크리스마스 2
TARGET_URL1 = "https://youtube.com/shorts/-vVnZoVtnFk?feature=share"  # 크리스마스


TARGET_URL = "https://youtube.com/shorts/8tat5aSyW4Q?feature=share"  # 크리스마스 2
TARGET_URL1 = "https://youtube.com/shorts/8tat5aSyW4Q?feature=share"  # 크리스마스

NUM_BROWSERS = 3

REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_LEASE = "proxies:lease"

SCREEN_WIDTH = 1920
SCREEN_HEIGHT = 1080

BROWSER_LAUNCH_TIMEOUT = 60
PAGE_LOAD_TIMEOUT = 90
CONTEXT_DEFAULT_TIMEOUT = 90

PAGE_LOAD_MAX_RETRIES = 3
PAGE_LOAD_RETRY_DELAY_MIN = 3
PAGE_LOAD_RETRY_DELAY_MAX = 7

YOUTUBE_INIT_DELAY_MIN = 3
YOUTUBE_INIT_DELAY_MAX = 6

HUMAN_MOUSE_MOVE_DELAY_MIN = 0.1
HUMAN_MOUSE_MOVE_DELAY_MAX = 0.3
HUMAN_CLICK_DELAY_MIN = 0.5
HUMAN_CLICK_DELAY_MAX = 1.5
HUMAN_SCROLL_DELAY_MIN = 0.5
HUMAN_SCROLL_DELAY_MAX = 1.2

VIDEO_WATCH_TIME_MIN = 240
VIDEO_WATCH_TIME_MAX = 300
VIDEO_STATUS_CHECK_INTERVAL = 5

PROXY_PENALTY_TIME = 60
PROXY_LEASE_TIME = 600

MAIN_LOOP_SLOT_CHECK_DELAY = 5
MAIN_LOOP_ITERATION_DELAY = 2

THREAD_JOIN_TIMEOUT = 10

TEST_MODE_WATCH_TIME = 30

# ===================== 화면 배치 함수 =====================

def calculate_window_position(index, total_browsers):
    if total_browsers <= 3:
        cols, rows = total_browsers, 1
    elif total_browsers <= 4:
        cols, rows = 2, 2
    elif total_browsers <= 6:
        cols, rows = 3, 2
    else:
        cols = 3
        rows = (total_browsers + 2) // 3
    
    window_width = SCREEN_WIDTH // cols
    window_height = SCREEN_HEIGHT // rows
    
    row = index // cols
    col = index % cols
    
    x = col * window_width
    y = row * window_height
    
    return {'x': x, 'y': y, 'width': window_width, 'height': window_height}

# JSON 프로필 로드
with open('region_profiles.json', 'r', encoding='utf-8') as f:
    REGION_PROFILES = json.load(f)

stop_event = threading.Event()

def get_redis():
    return redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

# ===================== 모바일 행동 시뮬레이션 =====================

def simulate_mobile_behavior(page):
    try:
        for _ in range(random.randint(2, 4)):
            scroll_amount = random.randint(30, 150)
            page.evaluate(f"window.scrollBy(0, {scroll_amount})")
            time.sleep(random.uniform(HUMAN_SCROLL_DELAY_MIN, HUMAN_SCROLL_DELAY_MAX))
        
        viewport = page.viewport_size
        if viewport:
            for _ in range(random.randint(2, 4)):
                x = random.randint(50, viewport['width'] - 50)
                y = random.randint(50, viewport['height'] - 50)
                page.mouse.move(x, y)
                time.sleep(random.uniform(HUMAN_MOUSE_MOVE_DELAY_MIN, HUMAN_MOUSE_MOVE_DELAY_MAX))
            
            if random.random() > 0.5:
                x = random.randint(100, viewport['width'] - 100)
                y = random.randint(100, viewport['height'] - 100)
                page.mouse.click(x, y)
                time.sleep(random.uniform(HUMAN_CLICK_DELAY_MIN, HUMAN_CLICK_DELAY_MAX))
    except Exception as e:
        print(f"   ⚠️ 행동 시뮬레이션 경고: {e}")

# ===================== 워커 함수 =====================

def monitor_service(url, proxy_url, index, stop_event, r):
    success = False
    region_name = random.choice(list(REGION_PROFILES.keys()))
    profile = REGION_PROFILES[region_name]
    
    print(f"[Bot-{index}] 🌍 지역: {region_name} | 프록시: {proxy_url}")

    browser = None
    try:
        if stop_event.is_set():
            return
        
        window_pos = calculate_window_position(index, NUM_BROWSERS)
        print(f"[Bot-{index}] 📐 창: {window_pos['width']}x{window_pos['height']} at ({window_pos['x']},{window_pos['y']})")
            
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,
                proxy={"server": proxy_url} if proxy_url else None,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    f"--window-position={window_pos['x']},{window_pos['y']}",
                    f"--window-size={window_pos['width']},{window_pos['height']}",
                    "--autoplay-policy=no-user-gesture-required",
                    "--disable-features=ImprovedCookieControls,LazyFrameLoading,GlobalMediaControls",
                    "--allow-running-insecure-content",
                    "--disable-infobars",
                    "--no-sandbox",
                    "--disable-web-security",
                ],
                timeout=BROWSER_LAUNCH_TIMEOUT * 1000
            )

            device_info = p.devices['Galaxy S9+']
            context = browser.new_context(
                viewport=device_info['viewport'],
                device_scale_factor=device_info['device_scale_factor'],
                is_mobile=device_info['is_mobile'],
                has_touch=device_info['has_touch'],
                user_agent=random.choice(profile['user_agents']),
                locale=profile['locale'],
                timezone_id=profile['timezone'],
                extra_http_headers={
                    "Accept-Language": profile['accept_languages'][0],
                    "DNT": "1"
                },
                permissions=["geolocation"],
                geolocation={"latitude": 37.5665, "longitude": 126.9780}
            )
            
            Tarnished.apply_stealth(context)
            print(f"[Bot-{index}] 🛡️ undetected-playwright stealth 적용")

            context.set_default_timeout(CONTEXT_DEFAULT_TIMEOUT * 1000)
            context.set_default_navigation_timeout(PAGE_LOAD_TIMEOUT * 1000)

            page = context.new_page()

            page.add_init_script("""
                Object.defineProperty(document, 'hidden', {get: () => false});
                Object.defineProperty(document, 'visibilityState', {get: () => 'visible'});
            """)

            chosen_referer = random.choice(profile['referers'])
            print(f"[Bot-{index}] 🔗 리퍼러: {chosen_referer}")
            
            retry_count = 0
            page_loaded = False
            
            while retry_count < PAGE_LOAD_MAX_RETRIES and not page_loaded and not stop_event.is_set():
                try:
                    print(f"[Bot-{index}] 🔄 로딩 {retry_count + 1}/{PAGE_LOAD_MAX_RETRIES}...")
                    page.goto(url, referer=chosen_referer, wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT * 1000)
                    page_loaded = True
                    print(f"[Bot-{index}] ✅ 로딩 성공")
                except Exception as e:
                    retry_count += 1
                    if retry_count < PAGE_LOAD_MAX_RETRIES:
                        wait_time = random.uniform(PAGE_LOAD_RETRY_DELAY_MIN, PAGE_LOAD_RETRY_DELAY_MAX)
                        print(f"[Bot-{index}] ⚠️ 재시도 대기 {wait_time:.1f}초...")
                        time.sleep(wait_time)
                    else:
                        raise e
            
            if stop_event.is_set():
                return

            # 비디오 요소 대기
            try:
                page.wait_for_selector('video', timeout=30000)
                print(f"[Bot-{index}] 🎥 video 요소 발견")
            except:
                print(f"[Bot-{index}] ⚠️ video 요소 대기 실패")

            init_wait = random.uniform(YOUTUBE_INIT_DELAY_MIN, YOUTUBE_INIT_DELAY_MAX)
            print(f"[Bot-{index}] ⏳ 초기화 대기 {init_wait:.1f}초...")
            time.sleep(init_wait)

            viewport = page.viewport_size
            if viewport:
                cx, cy = viewport['width'] // 2, viewport['height'] // 2
                page.mouse.click(cx, cy, click_count=2, delay=200)
                time.sleep(0.8)
                page.mouse.click(cx, cy + 100)
                time.sleep(0.8)
                page.keyboard.press("Space")
                time.sleep(0.5)

            # 강제 재생 (가장 확실)
            play_result = page.evaluate("""() => {
                const video = document.querySelector('video');
                if (!video) return {success: false, reason: 'no video'};
                video.muted = true;
                const promise = video.play();
                if (promise !== undefined) {
                    promise.then(() => {
                        setTimeout(() => {
                            video.muted = false;
                            video.volume = 0.5;
                        }, 3000);
                    }).catch(() => {});
                }
                return {success: true, currentTime: video.currentTime};
            }""")

            if play_result.get('success', False):
                print(f"[Bot-{index}] ▶️ 강제 자동재생 성공")
            else:
                print(f"[Bot-{index}] ⚠️ 자동재생 실패 - 추가 클릭")
                if viewport:
                    page.mouse.click(cx, cy)

            time.sleep(2)
            if not stop_event.is_set():
                simulate_mobile_behavior(page)

            watch_duration = random.uniform(VIDEO_WATCH_TIME_MIN, VIDEO_WATCH_TIME_MAX)
            print(f"[Bot-{index}] ⏱️ 시청 시작: {watch_duration:.0f}초")

            elapsed = 0
            last_time = 0
            
            while elapsed < watch_duration and not stop_event.is_set():
                time.sleep(min(VIDEO_STATUS_CHECK_INTERVAL, watch_duration - elapsed))
                elapsed += VIDEO_STATUS_CHECK_INTERVAL
                
                try:
                    status = page.evaluate("""() => {
                        const v = document.querySelector('video');
                        return v ? {time: v.currentTime, paused: v.paused} : null;
                    }""")
                    
                    if status:
                        is_playing = not status['paused'] and status['time'] > last_time
                        icon = "▶️" if is_playing else "⏸️"
                        print(f"[Bot-{index}] {icon} {elapsed:.0f}초 - 영상:{status['time']:.1f}초")
                        last_time = status['time']
                        
                        if status['paused'] and elapsed < watch_duration / 2 and viewport:
                            print(f"[Bot-{index}] 🔄 재생 재시도")
                            page.mouse.click(viewport['width'] // 2, viewport['height'] // 2)
                except:
                    pass
            
            if random.random() > 0.5:
                simulate_mobile_behavior(page)
            
            success = True
            print(f"[Bot-{index}] ✅ 완료 - {elapsed:.0f}초 시청")

    except Exception as e:
        print(f"[Bot-{index}] 🛑 에러: {e}")
    finally:
        try:
            if browser:
                browser.close()
        except:
            pass
            
        if r and proxy_url:
            penalty = 0 if success else PROXY_PENALTY_TIME
            r.zrem(REDIS_ZSET_LEASE, proxy_url)
            r.zadd(REDIS_ZSET_ALIVE, {proxy_url: int(time.time()) + penalty})

# ===================== 테스트 함수 =====================

def test_without_proxy(url, region_name="korea"):
    print(f"\n{'='*60}\n🧪 테스트 모드\n{'='*60}\n")
    profile = REGION_PROFILES.get(region_name, REGION_PROFILES["korea"])
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            device_info = p.devices['Galaxy S9+']
            context = browser.new_context(
                viewport=device_info['viewport'],
                device_scale_factor=device_info['device_scale_factor'],
                is_mobile=device_info['is_mobile'],
                has_touch=device_info['has_touch'],
                user_agent=random.choice(profile['user_agents']),
                locale=profile['locale'],
                timezone_id=profile['timezone']
            )
            Tarnished.apply_stealth(context)
            print("[TEST] 🛡️ stealth 적용")
            
            page = context.new_page()
            page.goto(url, wait_until="networkidle")
            print("[TEST] ✅ 로딩 완료")
            
            page.wait_for_selector('video', timeout=30000)
            page.evaluate("""() => { const v = document.querySelector('video'); if(v){v.muted=true; v.play();} }""")
            print("[TEST] ▶️ 강제 재생 시도")
            
            for i in range(TEST_MODE_WATCH_TIME):
                time.sleep(1)
                if (i + 1) % 5 == 0:
                    status = page.evaluate("""() => {
                        const v = document.querySelector('video');
                        return v ? {time: v.currentTime, paused: v.paused} : null;
                    }""")
                    if status:
                        icon = "▶️" if not status['paused'] else "⏸️"
                        print(f"[TEST] {icon} {i+1}초 - 영상:{status['time']:.1f}초")
            
            print("\n[TEST] ✅ 테스트 완료")
            time.sleep(15)
            browser.close()
    except Exception as e:
        print(f"[TEST] 🛑 에러: {e}")

# ===================== 메인 루프 =====================

_LUA_CLAIM = r"""
local alive = KEYS[1]
local lease = KEYS[2]
local now = tonumber(ARGV[1])
local lease_sec = tonumber(ARGV[2])
local expired = redis.call('ZRANGEBYSCORE', lease, '-inf', now, 'LIMIT', 0, 100)
for i, m in ipairs(expired) do
    redis.call('ZREM', lease, m)
    redis.call('ZADD', alive, 0, m)
end
local cands = redis.call('ZRANGEBYSCORE', alive, '-inf', now, 'LIMIT', 0, 50)
if (not cands) or (#cands == 0) then return nil end
local m = cands[math.random(#cands)]
redis.call('ZREM', alive, m)
redis.call('ZADD', lease, now + lease_sec, m)
return m
"""

if __name__ == "__main__":
    import sys
    import signal
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        region = sys.argv[2] if len(sys.argv) > 2 else "korea"
        test_without_proxy(TARGET_URL, region)
        sys.exit(0)
    
    print(f"\n{'='*60}")
    print(f"🚀 YouTube Shorts 자동 시청 봇 (자동재생 강화 버전)")
    print(f"{'='*60}\n")
    
    r = get_redis()
    active_slots = {}
    
    def signal_handler(signum, frame):
        print(f"\n{'='*60}\n🛑 종료 중...\n{'='*60}")
        stop_event.set()
        
    signal.signal(signal.SIGINT, signal_handler)

    try:
        while not stop_event.is_set():
            for slot in list(active_slots.keys()):
                if not active_slots[slot].is_alive():
                    print(f"[Main] 🔄 슬롯-{slot} 재사용 가능")
                    del active_slots[slot]
            
            if len(active_slots) < NUM_BROWSERS:
                for slot in range(NUM_BROWSERS):
                    if slot not in active_slots:
                        proxy = r.eval(_LUA_CLAIM, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, int(time.time()), PROXY_LEASE_TIME)
                        if proxy:
                            url = TARGET_URL if (slot % 2 == 0) else TARGET_URL1
                            t = threading.Thread(
                                target=monitor_service, 
                                args=(url, proxy, slot, stop_event, r),
                                daemon=True,
                                name=f"Bot-{slot}"
                            )
                            t.start()
                            active_slots[slot] = t
                            print(f"[Main] ✅ 슬롯-{slot} 시작 ({len(active_slots)}/{NUM_BROWSERS})")
                            break
                time.sleep(MAIN_LOOP_SLOT_CHECK_DELAY)
            time.sleep(MAIN_LOOP_ITERATION_DELAY)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        print(f"\n⏳ 정리 중...")
        for slot, t in active_slots.items():
            if t.is_alive():
                t.join(timeout=THREAD_JOIN_TIMEOUT)
                status = "정상" if not t.is_alive() else "강제"
                print(f"   {'✅' if not t.is_alive() else '⚠️'} 슬롯-{slot} {status} 종료")
        print(f"\n{'='*60}\n✅ 종료 완료\n{'='*60}\n")