import random
import threading
import time
import json
import redis
from playwright.sync_api import sync_playwright

# ===================== 1. 설정 및 타임아웃 변수 =====================

TARGET_URL = "https://www.youtube.com/shorts/u7sO-mNEpT4?feature=share"  # 크리스마스 2
TARGET_URL1 = "https://youtube.com/shorts/-vVnZoVtnFk?feature=share"  # 크리스마스

# === 브라우저 설정 ===
NUM_BROWSERS = 3 

# === [추가] 랜덤으로 선택될 모바일 기기 리스트 ===
# Playwright p.devices에 정의된 정확한 이름을 사용합니다.
MOBILE_DEVICES_LIST = [
    'Galaxy S9+', 'Galaxy S8', 'Pixel 5', 'Pixel 4', 
    'iPhone 13', 'iPhone 12', 'iPhone 11', 'iPhone XR', 'iPhone SE'
]

# === Redis 설정 ===
REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_LEASE = "proxies:lease"

SCREEN_WIDTH = 1920
SCREEN_HEIGHT = 1080

# 타임아웃 설정 (기존과 동일)
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

# ===================== 화면 배치 함수 (기존 유지) =====================

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
    
    return {
        'x': col * window_width, 'y': row * window_height,
        'width': window_width, 'height': window_height
    }

# JSON 프로필 로드
with open('region_profiles.json', 'r', encoding='utf-8') as f:
    REGION_PROFILES = json.load(f)

stop_event = threading.Event()

def get_redis():
    return redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

# ===================== 2. 모바일 행동 및 Stealth 함수 (완전 복구) =====================

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

def inject_mobile_properties(page, platform_name='Linux armv8l'):
    """기존의 WebGL 노이즈 및 Stealth 로직 100% 복구"""
    page.add_init_script(f"""
        Object.defineProperty(navigator, 'webdriver', {{get: () => undefined}});
        
        window.navigator.chrome = {{
            runtime: {{}}, loadTimes: function() {{}}, csi: function() {{}}, app: {{}}
        }};
        
        Object.defineProperty(navigator, 'languages', {{get: () => ['ko-KR', 'ko', 'en-US', 'en']}});
        Object.defineProperty(navigator, 'maxTouchPoints', {{get: () => 5}});
        Object.defineProperty(navigator, 'platform', {{get: () => '{platform_name}'}});
        
        // WebGL 노이즈 주입 부분 (복구)
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(param) {{
            if (param === 37445) return 'ARM';
            if (param === 37446) return 'Mali-G72';
            return getParameter.apply(this, arguments);
        }};
        
        // Canvas Fingerprinting 방어 부분 (복구)
        const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function(type) {{
            if (type === 'image/png' && this.width === 280 && this.height === 60) {{
                const context = this.getContext('2d');
                const imageData = context.getImageData(0, 0, this.width, this.height);
                for (let i = 0; i < imageData.data.length; i += 4) {{
                    imageData.data[i] += Math.floor(Math.random() * 3) - 1;
                }}
                context.putImageData(imageData, 0, 0);
            }}
            return originalToDataURL.apply(this, arguments);
        }};

        Object.defineProperty(document, 'hidden', {{get: () => false}});
        Object.defineProperty(document, 'visibilityState', {{get: () => 'visible'}});
    """)

# ===================== 3. 워커 함수 (YouTube 시청 봇) =====================

def monitor_service(url, proxy_url, index, stop_event, r):
    """YouTube Shorts 자동 시청 봇 (중복 인자 에러 수정 완료)"""
    success = False
    region_name = random.choice(list(REGION_PROFILES.keys()))
    profile = REGION_PROFILES[region_name]
    
    # 랜덤 기기 선택
    selected_device_name = random.choice(MOBILE_DEVICES_LIST)
    
    print(f"[Bot-{index}] 🌍 {region_name} | 📱 {selected_device_name} | 🔗 {proxy_url}")

    browser = None
    try:
        if stop_event.is_set():
            return
        
        window_pos = calculate_window_position(index, NUM_BROWSERS)
            
        with sync_playwright() as p:
            # 1. 기기 정보 가져오기 (딕셔너리 복사본 생성)
            device_info = dict(p.devices[selected_device_name])
            
            # 2. device_info 안에 있는 user_agent를 꺼내옵니다.
            # 이렇게 하면 **device_info를 사용할 때 user_agent가 중복되지 않습니다.
            device_agent = device_info.pop('user_agent', None)
            
            browser = p.chromium.launch(
                headless=False,
                proxy={"server": proxy_url} if proxy_url else None,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    f"--window-position={window_pos['x']},{window_pos['y']}",
                    f"--window-size={window_pos['width']},{window_pos['height']}",
                    "--autoplay-policy=no-user-gesture-required",
                    "--exclude-switches=enable-automation",
                    "--disable-infobars"
                ],
                timeout=BROWSER_LAUNCH_TIMEOUT * 1000
            )

            # 3. 브라우저 컨텍스트 생성 (중복 인자 방지 처리)
            context = browser.new_context(
                **device_info,           # user_agent가 제거된 나머지 기기 설정
                user_agent=device_agent, # 꺼내온 기기 고유 에이전트 주입
                locale=profile['locale'],
                timezone_id=profile['timezone'],
                extra_http_headers={
                    "Accept-Language": profile['accept_languages'][0],
                    "DNT": "1"
                },
                permissions=["geolocation"]
            )
            
            context.set_default_timeout(CONTEXT_DEFAULT_TIMEOUT * 1000)
            page = context.new_page()

            # 4. Stealth 및 WebGL/Canvas 노이즈 주입 (기존 로직)
            platform = 'iPhone' if 'iPhone' in selected_device_name else 'Linux armv8l'
            inject_mobile_properties(page, platform)

            # 5. 페이지 이동 및 로딩 (재시도 로직 포함)
            chosen_referer = random.choice(profile['referers'])
            retry_count = 0
            page_loaded = False
            while retry_count < PAGE_LOAD_MAX_RETRIES and not page_loaded and not stop_event.is_set():
                try:
                    page.goto(url, referer=chosen_referer, wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT * 1000)
                    page_loaded = True
                except Exception:
                    retry_count += 1
                    time.sleep(random.uniform(PAGE_LOAD_RETRY_DELAY_MIN, PAGE_LOAD_RETRY_DELAY_MAX))
            
            if not page_loaded:
                raise Exception("페이지 로딩 실패")

            time.sleep(random.uniform(YOUTUBE_INIT_DELAY_MIN, YOUTUBE_INIT_DELAY_MAX))
            
            # 중앙 클릭 재생
            v_size = page.viewport_size
            if v_size:
                page.mouse.click(v_size['width'] // 2, v_size['height'] // 2)
            
            # 6. 시청 모니터링 루프
            watch_duration = random.uniform(VIDEO_WATCH_TIME_MIN, VIDEO_WATCH_TIME_MAX)
            elapsed = 0
            last_video_time = 0
            
            while elapsed < watch_duration and not stop_event.is_set():
                time.sleep(VIDEO_STATUS_CHECK_INTERVAL)
                elapsed += VIDEO_STATUS_CHECK_INTERVAL
                
                try:
                    status = page.evaluate("""() => {
                        const v = document.querySelector('video');
                        return v ? {time: v.currentTime, paused: v.paused} : null;
                    }""")
                    
                    if status:
                        is_playing = not status['paused'] and status['time'] > last_video_time
                        icon = "▶️" if is_playing else "⏸️"
                        print(f"[Bot-{index}] {icon} {elapsed:.0f}/{watch_duration:.0f}초 (영상:{status['time']:.1f}초)")
                        last_video_time = status['time']
                    
                    if elapsed % 60 == 0:
                        simulate_mobile_behavior(page)
                except:
                    pass
            
            success = True
            print(f"[Bot-{index}] ✅ 시청 완료")

    except Exception as e:
        print(f"[Bot-{index}] 🛑 에러 발생: {e}")
    finally:
        if browser:
            try: browser.close()
            except: pass
        
        if r and proxy_url:
            r.zrem(REDIS_ZSET_LEASE, proxy_url)
            penalty = 0 if success else PROXY_PENALTY_TIME
            r.zadd(REDIS_ZSET_ALIVE, {proxy_url: int(time.time()) + penalty})
            
            
# ===================== 5. 메인 루프 (기존과 동일) =====================

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
    import signal
    r = get_redis()
    active_slots = {}
    
    def signal_handler(signum, frame):
        stop_event.set()
    signal.signal(signal.SIGINT, signal_handler)

    print(f"🚀 YouTube 봇 시작 (슬롯: {NUM_BROWSERS})")

    try:
        while not stop_event.is_set():
            for slot in list(active_slots.keys()):
                if not active_slots[slot].is_alive():
                    del active_slots[slot]
            
            if len(active_slots) < NUM_BROWSERS:
                for slot in range(NUM_BROWSERS):
                    if slot not in active_slots:
                        proxy = r.eval(_LUA_CLAIM, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, int(time.time()), PROXY_LEASE_TIME)
                        if proxy:
                            url = TARGET_URL if (slot % 2 == 0) else TARGET_URL1
                            t = threading.Thread(target=monitor_service, args=(url, proxy, slot, stop_event, r), daemon=True)
                            t.start()
                            active_slots[slot] = t
                            print(f"[Main] ✅ 슬롯-{slot} 활성화")
                            break
                time.sleep(MAIN_LOOP_SLOT_CHECK_DELAY)
            time.sleep(MAIN_LOOP_ITERATION_DELAY)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        for t in active_slots.values(): t.join(timeout=5)