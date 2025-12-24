import random
import threading
import time
import json
import redis
import os
import sys
import signal
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from urllib.parse import urlparse

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

# 유입 경로(Referer) 목록 - 지역별로 다양화
REFERERS = [
    "https://www.google.com/",
    "https://www.facebook.com/",
    "https://twitter.com/",
    "https://t.co/",
    "https://www.instagram.com/",
    "https://www.bing.com/",
    "https://duckduckgo.com/",
    "https://www.reddit.com/",
    "https://news.ycombinator.com/",
]

TARGET_URL = "https://www.youtube.com/shorts/u7sO-mNEpT4?feature=share"
TARGET_URL1 = "https://youtube.com/shorts/-vVnZoVtnFk?feature=share"

NUM_BROWSERS = 3 
MOBILE_DEVICES_LIST = [
    'Pixel 5', 'Pixel 4', 'iPhone 13', 'iPhone 12', 'iPhone 11', 'iPhone SE'
]

REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_LEASE = "proxies:lease"

# ✅ 개선: 쿨타임 설정 합리화
SUCCESS_COOL_DOWN = 0      # 성공 시 즉시 재사용 가능
FAILURE_PENALTY = 300      # 실패 시 5분 페널티 (24시간은 너무 김)

# ✅ 추가: 타임아웃 설정
BROWSER_LAUNCH_TIMEOUT = 60000   # 60초
PAGE_LOAD_TIMEOUT = 120000       # 120초
CONTEXT_TIMEOUT = 90000          # 90초
VIDEO_WATCH_MIN = 240
VIDEO_WATCH_MAX = 300
VIDEO_CHECK_INTERVAL = 5
MAX_VIDEO_CHECK_ERRORS = 3       # 연속 3번 실패하면 종료
CONSENT_READ_TIME_MIN = 12
CONSENT_READ_TIME_MAX = 25

# ✅ 추가: 화면 크기 설정
SCREEN_WIDTH = 1920
SCREEN_HEIGHT = 1080

# ===================== 2. 유틸리티 및 스텔스 로직 =====================

def get_redis():
    return redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

def apply_stealth_and_custom(page, config, device_name):
    """
    ✅ 개선: 강화된 스텔스 로직
    - Playwright 흔적 제거
    - 랜덤 WebGL GPU
    - Canvas 노이즈
    - Chrome API 구현
    """
    page.add_init_script(f"""
        // ===== 1. Playwright 탐지 완전 제거 =====
        delete window.__playwright;
        delete window.playwright;
        delete window.__pw_manual;
        delete window.__PW_inspect;
        
        Object.defineProperty(navigator, 'webdriver', {{get: () => undefined}});
        
        // ===== 2. Chrome Runtime API =====
        window.chrome = {{
            runtime: {{
                connect: function() {{}},
                sendMessage: function() {{}},
            }},
            loadTimes: function() {{ 
                return {{
                    commitLoadTime: Date.now() / 1000 - Math.random() * 2,
                    connectionInfo: "http/2",
                    finishLoadTime: Date.now() / 1000 - Math.random() * 0.5,
                    firstPaintTime: Date.now() / 1000 - Math.random() * 1.5,
                }}
            }},
            csi: function() {{ 
                return {{
                    startE: Date.now() - Math.random() * 3000,
                    onloadT: Date.now() - Math.random() * 1000,
                    pageT: Date.now() - Math.random() * 2000,
                }}
            }},
        }};
        
        // ===== 3. 랜덤 WebGL GPU =====
        const gpuList = [
            ['ARM', 'Mali-G72'], ['ARM', 'Mali-G76'], ['ARM', 'Mali-G77'],
            ['Qualcomm', 'Adreno (TM) 640'], ['Qualcomm', 'Adreno (TM) 650'],
            ['Apple', 'Apple GPU'], ['Apple', 'Apple A14 GPU'],
        ];
        const randomGPU = gpuList[Math.floor(Math.random() * gpuList.length)];
        
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(param) {{
            if (param === 37445) return randomGPU[0];
            if (param === 37446) return randomGPU[1];
            return getParameter.apply(this, arguments);
        }};
        
        // ===== 4. Canvas Fingerprinting 방어 =====
        const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function(type) {{
            if (this.width > 0 && this.height > 0) {{
                try {{
                    const context = this.getContext('2d');
                    if (context) {{
                        const imageData = context.getImageData(0, 0, this.width, this.height);
                        for (let i = 0; i < imageData.data.length; i += 40) {{
                            imageData.data[i] = Math.min(255, Math.max(0, imageData.data[i] + Math.floor(Math.random() * 5) - 2));
                        }}
                        context.putImageData(imageData, 0, 0);
                    }}
                }} catch(e) {{}}
            }}
            return originalToDataURL.apply(this, arguments);
        }};
        
        // ===== 5. 기타 속성 =====
        Object.defineProperty(navigator, 'languages', {{
            get: () => ['{config.get("locale", "en-US")}', 'en']
        }});
        
        Object.defineProperty(navigator, 'maxTouchPoints', {{
            get: () => 5
        }});
        
        // 배터리 API
        if (navigator.getBattery) {{
            navigator.getBattery = function() {{
                return Promise.resolve({{
                    charging: Math.random() > 0.5,
                    chargingTime: Infinity,
                    dischargingTime: Math.random() * 20000 + 10000,
                    level: Math.random() * 0.5 + 0.3,
                }});
            }};
        }}
        
        // 페이지 가시성
        Object.defineProperty(document, 'hidden', {{get: () => false}});
        Object.defineProperty(document, 'visibilityState', {{get: () => 'visible'}});
    """)

def calculate_window_position(index, total_browsers=NUM_BROWSERS):
    """✅ 개선: 화면 배치 최적화"""
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
        'x': col * window_width,
        'y': row * window_height,
        'width': window_width,
        'height': window_height
    }

def handle_youtube_consent(page, timeout=10000):
    """
    ✅ 추가: 유튜브 동의 페이지 처리
    실제 사용자처럼 읽는 시간 추가
    """
    try:
        url = page.url
        host = urlparse(url).hostname or ""
        
        if "consent.youtube.com" not in host:
            return False

        # 실제 사용자처럼 동의 페이지를 읽는 시간
        read_time = random.uniform(CONSENT_READ_TIME_MIN, CONSENT_READ_TIME_MAX)
        print(f"   [Consent] 📖 동의 페이지 읽는 중... ({read_time:.1f}초)")
        time.sleep(read_time)
        
        # '모두 수락' 버튼 찾기
        consent_button = page.locator("form[action='https://consent.youtube.com/save'] button[jsname='b3VHJd']")
        
        if consent_button.count() > 0:
            # 버튼 위치로 마우스 이동 (자연스럽게)
            box = consent_button.bounding_box()
            if box:
                page.mouse.move(
                    box['x'] + box['width'] / 2, 
                    box['y'] + box['height'] / 2,
                    steps=random.randint(5, 10)
                )
                time.sleep(random.uniform(0.3, 0.8))
            
            consent_button.click()
            print("   [Consent] ✅ 유튜브 동의 '모두 수락' 클릭 완료")
            
            page.wait_for_load_state("networkidle", timeout=timeout)
            return True
        
        return False
    except Exception as e:
        print(f"   [Consent] ⚠ 처리 중 예외 발생: {e}")
        return False

def simulate_mobile_behavior(page):
    """
    ✅ 추가: 자연스러운 모바일 행동 시뮬레이션
    """
    try:
        viewport = page.viewport_size
        if not viewport:
            return
        
        # 스크롤
        scroll_count = random.randint(1, 3)
        for _ in range(scroll_count):
            scroll_amount = random.randint(50, 200)
            page.evaluate(f"window.scrollBy({{top: {scroll_amount}, behavior: 'smooth'}})")
            time.sleep(random.uniform(0.5, 1.5))
        
        # 랜덤 터치
        if random.random() > 0.6:
            x = random.randint(100, viewport['width'] - 100)
            y = random.randint(100, viewport['height'] - 100)
            page.mouse.click(x, y)
            time.sleep(random.uniform(0.5, 1.0))
        
    except Exception:
        pass

# ===================== 3. 메인 워커 (개선됨) =====================

def monitor_service(url, proxy_url, index, stop_event, r):
    """
    ✅ 대폭 개선된 워커 함수
    - 에러 핸들링 강화
    - 연속 에러 카운팅
    - 타임아웃 관리
    - Consent 처리
    """
    success = False
    region_key = random.choice(list(REGION_PROFILES.keys())) if REGION_PROFILES else "US"
    config = REGION_PROFILES.get(region_key, {"locale": "en-US", "timezone": "America/New_York"})
    referer = random.choice(REFERERS)
    device_name = random.choice(MOBILE_DEVICES_LIST)

    print(f"[Bot-{index}] 🚀 시작")
    print(f"   📱 Device: {device_name}")
    print(f"   🌍 Region: {region_key} ({config.get('locale')})")
    print(f"   🔗 Proxy: {proxy_url}")
    print(f"   🔗 Referer: {referer}")

    playwright_mgr = None
    browser = None
    
    try:
        if stop_event.is_set():
            return
        
        playwright_mgr = sync_playwright().start()
        device_info = dict(playwright_mgr.devices[device_name])
        device_agent = device_info.pop('user_agent', None)
        
        pos = calculate_window_position(index)
        
        # ✅ 개선: 브라우저 옵션 강화
        browser = playwright_mgr.chromium.launch(
            headless=False,
            proxy={"server": proxy_url} if proxy_url else None,
            args=[
                f"--window-position={pos['x']},{pos['y']}",
                f"--window-size={pos['width']},{pos['height']}",
                "--disable-blink-features=AutomationControlled",
                "--exclude-switches=enable-automation",
                "--disable-infobars",
                "--autoplay-policy=no-user-gesture-required",
            ],
            timeout=BROWSER_LAUNCH_TIMEOUT
        )

        # ✅ 개선: 컨텍스트 설정
        context = browser.new_context(
            **device_info,
            user_agent=device_agent,
            locale=config['locale'],
            timezone_id=config['timezone'],
            extra_http_headers={
                "Accept-Language": config.get('locale', 'en-US'),
            }
        )
        
        context.set_default_timeout(CONTEXT_TIMEOUT)
        page = context.new_page()
        
        # ✅ 개선: 스텔스 적용
        apply_stealth_and_custom(page, config, device_name)

        # ✅ 개선: 페이지 로딩 재시도 로직
        page_loaded = False
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries and not page_loaded and not stop_event.is_set():
            try:
                print(f"   [Bot-{index}] 🌐 페이지 로딩 시도 {retry_count + 1}/{max_retries}...")
                page.goto(
                    url, 
                    referer=referer, 
                    wait_until="domcontentloaded",
                    timeout=PAGE_LOAD_TIMEOUT
                )
                page_loaded = True
            except PlaywrightTimeoutError:
                retry_count += 1
                if retry_count < max_retries:
                    print(f"   [Bot-{index}] ⚠️ 타임아웃, 재시도 중...")
                    time.sleep(random.uniform(3, 7))
            except Exception as e:
                print(f"   [Bot-{index}] ❌ 로딩 실패: {str(e)[:100]}")
                break
        
        if not page_loaded:
            raise Exception("페이지 로딩 최종 실패")
        
        # ✅ 개선: Consent 처리
        time.sleep(random.uniform(3, 6))
        handle_youtube_consent(page)
        
        # 초기화 대기
        time.sleep(random.uniform(3, 6))
        
        # 중앙 클릭으로 재생 트리거
        v_size = page.viewport_size
        if v_size:
            center_x = v_size['width'] // 2 + random.randint(-50, 50)
            center_y = v_size['height'] // 2 + random.randint(-50, 50)
            page.mouse.move(center_x, center_y, steps=random.randint(5, 10))
            time.sleep(random.uniform(0.2, 0.5))
            page.mouse.click(center_x, center_y)
        
        # ✅ 개선: 시청 로직 (연속 에러 카운팅)
        watch_duration = random.uniform(VIDEO_WATCH_MIN, VIDEO_WATCH_MAX)
        elapsed = 0
        last_v_time = 0
        consecutive_errors = 0
        behavior_interval = random.randint(20, 40)
        
        print(f"   [Bot-{index}] 🎬 시청 시작 (목표: {watch_duration:.0f}초)")
        
        while elapsed < watch_duration and not stop_event.is_set():
            time.sleep(VIDEO_CHECK_INTERVAL)
            elapsed += VIDEO_CHECK_INTERVAL
            
            try:
                status = page.evaluate("""() => {
                    const v = document.querySelector('video');
                    return v ? {t: v.currentTime, p: v.paused} : null;
                }""")
                
                if status:
                    is_playing = not status['p'] and status['t'] > last_v_time
                    icon = "▶️" if is_playing else "⏸️"
                    print(f"   [Bot-{index}] {icon} {elapsed:.0f}/{watch_duration:.0f}초 (영상:{status['t']:.1f}초)")
                    last_v_time = status['t']
                    consecutive_errors = 0  # 성공하면 리셋
                else:
                    consecutive_errors += 1
                    print(f"   [Bot-{index}] ⚠️ 영상 상태 없음 (에러: {consecutive_errors}/{MAX_VIDEO_CHECK_ERRORS})")
                
                # ✅ 개선: 연속 에러 체크
                if consecutive_errors >= MAX_VIDEO_CHECK_ERRORS:
                    print(f"   [Bot-{index}] 🛑 상태 체크 연속 실패 {MAX_VIDEO_CHECK_ERRORS}번 → 작업 종료")
                    break
                
                # 행동 시뮬레이션
                if elapsed % behavior_interval == 0:
                    simulate_mobile_behavior(page)
                    behavior_interval = random.randint(20, 40)
                    
            except Exception as e:
                consecutive_errors += 1
                print(f"   [Bot-{index}] ⚠️ 상태 체크 오류 (에러: {consecutive_errors}/{MAX_VIDEO_CHECK_ERRORS}): {e}")
                
                if consecutive_errors >= MAX_VIDEO_CHECK_ERRORS:
                    print(f"   [Bot-{index}] 🛑 상태 체크 연속 실패 → 작업 종료")
                    break
        
        # ✅ 개선: 성공 조건
        if elapsed >= watch_duration and consecutive_errors < MAX_VIDEO_CHECK_ERRORS:
            success = True
            print(f"   [Bot-{index}] ✅ 시청 성공 완료")
        else:
            print(f"   [Bot-{index}] ⚠️ 시청 미완료")

    except Exception as e:
        print(f"   [Bot-{index}] 🛑 에러 발생: {str(e)[:100]}")
    finally:
        try:
            if browser:
                browser.close()
            if playwright_mgr:
                playwright_mgr.stop()
        except:
            pass
        
        # ✅ 개선: Redis 처리 (score=0으로 즉시 반환 또는 짧은 페널티)
        if r and proxy_url:
            r.zrem(REDIS_ZSET_LEASE, proxy_url)
            
            if success:
                # 성공 시 즉시 재사용 가능
                r.zadd(REDIS_ZSET_ALIVE, {proxy_url: 0})
                print(f"   [Bot-{index}] ✅ 프록시 반환 (성공, score=0)")
            else:
                # 실패 시 짧은 페널티 또는 반환 안함
                if FAILURE_PENALTY > 0:
                    score = int(time.time()) + FAILURE_PENALTY
                    r.zadd(REDIS_ZSET_ALIVE, {proxy_url: score})
                    print(f"   [Bot-{index}] ⚠️ 프록시 반환 (실패, {FAILURE_PENALTY}초 페널티)")
                else:
                    print(f"   [Bot-{index}] ⚠️ 프록시 실패, Collector 재테스트 대기")

# ===================== 4. 메인 제어 루프 =====================

_LUA_CLAIM = r"""
local alive, lease = KEYS[1], KEYS[2]
local now, l_sec = tonumber(ARGV[1]), tonumber(ARGV[2])
local expired = redis.call('ZRANGEBYSCORE', lease, '-inf', now, 'LIMIT', 0, 100)
for _, m in ipairs(expired) do
    redis.call('ZREM', lease, m)
    redis.call('ZADD', alive, 0, m)
end
local cands = redis.call('ZRANGEBYSCORE', alive, '-inf', now, 'LIMIT', 0, 50)
if #cands == 0 then return nil end
local target = cands[math.random(#cands)]
redis.call('ZREM', alive, target)
redis.call('ZADD', lease, now + l_sec, target)
return target
"""

def main():
    r = get_redis()
    active_slots = {}
    stop_event = threading.Event()

    def signal_handler(sig, frame):
        print("\n🛑 중단 요청... 모든 브라우저를 닫습니다.")
        stop_event.set()
        time.sleep(2)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    print("=" * 80)
    print("📡 Playwright Multi-Proxy Bot v4.0 (개선됨)")
    print("=" * 80)
    print(f"📱 슬롯: {NUM_BROWSERS}개")
    print(f"⏱️  시청 시간: {VIDEO_WATCH_MIN}-{VIDEO_WATCH_MAX}초")
    print(f"🎭 Stealth: 강화된 탐지 회피 (Playwright 흔적 제거, 랜덤 GPU)")
    print(f"🤖 행동: 자연스러운 스크롤, 터치, 클릭 패턴")
    print(f"🔍 안전장치: 상태 체크 {MAX_VIDEO_CHECK_ERRORS}회 연속 실패 시 자동 종료")
    print(f"🕒 쿨타임: 성공={SUCCESS_COOL_DOWN}초, 실패={FAILURE_PENALTY}초")
    print("=" * 80)

    try:
        while not stop_event.is_set():
            # 종료된 스레드 정리
            for s in list(active_slots.keys()):
                if not active_slots[s].is_alive():
                    del active_slots[s]
                    print(f"[Main] 🔄 슬롯-{s} 정리 완료")
            
            # 빈 슬롯 채우기
            if len(active_slots) < NUM_BROWSERS:
                for s in range(NUM_BROWSERS):
                    if s not in active_slots:
                        # Redis에서 프록시 가져오기
                        proxy = r.eval(_LUA_CLAIM, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, int(time.time()), 600)
                        if proxy:
                            url = TARGET_URL if s % 2 == 0 else TARGET_URL1
                            t = threading.Thread(
                                target=monitor_service, 
                                args=(url, proxy, s, stop_event, r), 
                                daemon=True
                            )
                            t.start()
                            active_slots[s] = t
                            print(f"[Main] ✅ 슬롯-{s} 활성화")
                            break
                        else:
                            print(f"[Main] ⚠️ 사용 가능한 프록시 없음, 대기 중...")
                            time.sleep(10)
                            break
            
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n🛑 KeyboardInterrupt 감지")
    except Exception as e:
        print(f"메인 루프 에러: {e}")
    finally:
        stop_event.set()
        print("\n🛑 종료 중... 모든 스레드 대기")
        for t in active_slots.values():
            t.join(timeout=10)
        print("✅ 모든 봇 종료 완료")

if __name__ == "__main__":
    main()