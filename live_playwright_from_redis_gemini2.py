import random
import threading
import time
import json
import redis
from playwright.sync_api import sync_playwright
from urllib.parse import urlparse

# ===================== 1. 설정 및 타임아웃 변수 =====================

TARGET_URL = "https://www.youtube.com/shorts/u7sO-mNEpT4?feature=share"  # 크리스마스 2
TARGET_URL1 = "https://youtube.com/shorts/-vVnZoVtnFk?feature=share"  # 크리스마스

# === 브라우저 설정 ===
NUM_BROWSERS = 3 

# === 랜덤으로 선택될 모바일 기기 리스트 ===
MOBILE_DEVICES_LIST = [
    'Galaxy S9+', 'Galaxy S8', 'Pixel 5', 'Pixel 4', 
    'iPhone 13', 'iPhone 12', 'iPhone 11', 'iPhone XR', 'iPhone SE'
]

# === Redis 설정 ===
REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_LEASE = "proxies:lease"

SCREEN_WIDTH = 1920
SCREEN_HEIGHT = 1080

# 타임아웃 설정 (개선됨)
BROWSER_LAUNCH_TIMEOUT = 60
PAGE_LOAD_TIMEOUT = 90
CONTEXT_DEFAULT_TIMEOUT = 90
PAGE_LOAD_MAX_RETRIES = 3
PAGE_LOAD_RETRY_DELAY_MIN = 3
PAGE_LOAD_RETRY_DELAY_MAX = 7
YOUTUBE_INIT_DELAY_MIN = 3
YOUTUBE_INIT_DELAY_MAX = 6
HUMAN_MOUSE_MOVE_DELAY_MIN = 0.1
HUMAN_MOUSE_MOVE_DELAY_MAX = 0.4
HUMAN_CLICK_DELAY_MIN = 0.5
HUMAN_CLICK_DELAY_MAX = 1.8
HUMAN_SCROLL_DELAY_MIN = 0.3
HUMAN_SCROLL_DELAY_MAX = 1.8
VIDEO_WATCH_TIME_MIN = 240  # 원본대로 4분 (프록시 느릴 수 있음)
VIDEO_WATCH_TIME_MAX = 300  # 원본대로 5분
VIDEO_STATUS_CHECK_INTERVAL = 5
MAX_STATUS_CHECK_ERRORS = 3  # 상태 체크 연속 실패 3번이면 종료
PROXY_PENALTY_TIME = 60
PROXY_LEASE_TIME_MIN = 540  # 9분
PROXY_LEASE_TIME_MAX = 660  # 11분 (랜덤화)
MAIN_LOOP_SLOT_CHECK_DELAY = 5
MAIN_LOOP_ITERATION_DELAY = 2
THREAD_JOIN_TIMEOUT = 10
CONSENT_READ_TIME_MIN = 12  # 동의 페이지 읽는 시간
CONSENT_READ_TIME_MAX = 25

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

# ===================== 2. 개선된 Stealth 함수 =====================

def inject_mobile_properties(page, platform_name='Linux armv8l'):
    """
    강화된 탐지 회피 스크립트
    - Playwright 흔적 제거
    - 랜덤 WebGL GPU
    - 전체 Canvas 노이즈
    - 향상된 Chrome API
    """
    page.add_init_script(f"""
        // ===== 1. Playwright 탐지 완전 제거 =====
        delete window.__playwright;
        delete window.playwright;
        delete window.__pw_manual;
        delete window.__PW_inspect;
        
        Object.defineProperty(navigator, 'webdriver', {{get: () => undefined}});
        
        // ===== 2. 정교한 Chrome Runtime API =====
        window.chrome = {{
            runtime: {{
                OnInstalledReason: {{
                    CHROME_UPDATE: "chrome_update",
                    INSTALL: "install",
                    SHARED_MODULE_UPDATE: "shared_module_update",
                    UPDATE: "update",
                }},
                OnRestartRequiredReason: {{
                    APP_UPDATE: "app_update",
                    OS_UPDATE: "os_update",
                    PERIODIC: "periodic",
                }},
                PlatformArch: {{
                    ARM: "arm",
                    ARM64: "arm64",
                    X86_32: "x86-32",
                    X86_64: "x86-64",
                }},
                PlatformOs: {{
                    ANDROID: "android",
                    LINUX: "linux",
                    MAC: "mac",
                    WIN: "win",
                }},
                connect: function() {{}},
                sendMessage: function() {{}},
            }},
            loadTimes: function() {{ 
                return {{
                    commitLoadTime: Date.now() / 1000 - Math.random() * 2,
                    connectionInfo: "http/2",
                    finishDocumentLoadTime: Date.now() / 1000 - Math.random(),
                    finishLoadTime: Date.now() / 1000 - Math.random() * 0.5,
                    firstPaintAfterLoadTime: 0,
                    firstPaintTime: Date.now() / 1000 - Math.random() * 1.5,
                    navigationType: "Other",
                    npnNegotiatedProtocol: "h2",
                    requestTime: Date.now() / 1000 - Math.random() * 3,
                    startLoadTime: Date.now() / 1000 - Math.random() * 2.5,
                    wasAlternateProtocolAvailable: false,
                    wasFetchedViaSpdy: true,
                    wasNpnNegotiated: true,
                }}
            }},
            csi: function() {{ 
                return {{
                    startE: Date.now() - Math.random() * 3000,
                    onloadT: Date.now() - Math.random() * 1000,
                    pageT: Date.now() - Math.random() * 2000,
                    tran: 15
                }}
            }},
            app: {{
                isInstalled: false,
                InstallState: {{
                    DISABLED: "disabled",
                    INSTALLED: "installed",
                    NOT_INSTALLED: "not_installed"
                }},
                RunningState: {{
                    CANNOT_RUN: "cannot_run",
                    READY_TO_RUN: "ready_to_run",
                    RUNNING: "running"
                }}
            }}
        }};
        
        // ===== 3. 랜덤 WebGL GPU (매번 다른 GPU) =====
        const gpuList = [
            ['ARM', 'Mali-G72'], ['ARM', 'Mali-G76'], ['ARM', 'Mali-G77'],
            ['ARM', 'Mali-G78'], ['ARM', 'Mali-G710'],
            ['Qualcomm', 'Adreno (TM) 640'], ['Qualcomm', 'Adreno (TM) 650'],
            ['Qualcomm', 'Adreno (TM) 660'], ['Qualcomm', 'Adreno (TM) 730'],
            ['Apple', 'Apple GPU'], ['Apple', 'Apple A14 GPU'], ['Apple', 'Apple A15 GPU'],
            ['PowerVR', 'PowerVR Rogue GE8320']
        ];
        const randomGPU = gpuList[Math.floor(Math.random() * gpuList.length)];
        
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(param) {{
            if (param === 37445) return randomGPU[0];  // UNMASKED_VENDOR_WEBGL
            if (param === 37446) return randomGPU[1];  // UNMASKED_RENDERER_WEBGL
            return getParameter.apply(this, arguments);
        }};
        
        // WebGL2도 동일하게
        if (window.WebGL2RenderingContext) {{
            const getParameter2 = WebGL2RenderingContext.prototype.getParameter;
            WebGL2RenderingContext.prototype.getParameter = function(param) {{
                if (param === 37445) return randomGPU[0];
                if (param === 37446) return randomGPU[1];
                return getParameter2.apply(this, arguments);
            }};
        }}
        
        // ===== 4. Canvas Fingerprinting 방어 (모든 크기) =====
        const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function(type) {{
            if (this.width > 0 && this.height > 0) {{
                try {{
                    const context = this.getContext('2d');
                    if (context) {{
                        const imageData = context.getImageData(0, 0, this.width, this.height);
                        // 10픽셀마다 미세한 노이즈 추가 (눈에 안 보임)
                        for (let i = 0; i < imageData.data.length; i += 40) {{
                            imageData.data[i] = Math.min(255, Math.max(0, imageData.data[i] + Math.floor(Math.random() * 5) - 2));
                        }}
                        context.putImageData(imageData, 0, 0);
                    }}
                }} catch(e) {{}}
            }}
            return originalToDataURL.apply(this, arguments);
        }};
        
        // ===== 5. 기타 탐지 회피 =====
        Object.defineProperty(navigator, 'languages', {{
            get: () => ['ko-KR', 'ko', 'en-US', 'en']
        }});
        
        Object.defineProperty(navigator, 'maxTouchPoints', {{
            get: () => 5
        }});
        
        Object.defineProperty(navigator, 'platform', {{
            get: () => '{platform_name}'
        }});
        
        // 배터리 API (모바일에서 자연스럽게)
        if (navigator.getBattery) {{
            const originalGetBattery = navigator.getBattery;
            navigator.getBattery = function() {{
                return Promise.resolve({{
                    charging: Math.random() > 0.5,
                    chargingTime: Infinity,
                    dischargingTime: Math.random() * 20000 + 10000,
                    level: Math.random() * 0.5 + 0.3,
                    addEventListener: function() {{}},
                    removeEventListener: function() {{}},
                    dispatchEvent: function() {{}}
                }});
            }};
        }}
        
        // 페이지 가시성 항상 visible
        Object.defineProperty(document, 'hidden', {{get: () => false}});
        Object.defineProperty(document, 'visibilityState', {{get: () => 'visible'}});
        
        // Permissions API (랜덤하게)
        if (navigator.permissions && navigator.permissions.query) {{
            const originalQuery = navigator.permissions.query;
            navigator.permissions.query = function(params) {{
                if (params.name === 'notifications') {{
                    return Promise.resolve({{state: Math.random() > 0.7 ? 'granted' : 'denied', addEventListener: function(){{}}, removeEventListener: function(){{}}}});
                }}
                return originalQuery.apply(this, arguments);
            }};
        }}
        
        // Connection API (모바일 네트워크)
        if (navigator.connection) {{
            Object.defineProperty(navigator.connection, 'effectiveType', {{
                get: () => ['4g', '4g', '3g'][Math.floor(Math.random() * 3)]
            }});
            Object.defineProperty(navigator.connection, 'downlink', {{
                get: () => Math.random() * 10 + 1
            }});
            Object.defineProperty(navigator.connection, 'rtt', {{
                get: () => Math.random() * 100 + 50
            }});
        }}
    """)

# ===================== 3. 개선된 자연스러운 행동 시뮬레이션 =====================

def simulate_mobile_behavior(page):
    """
    더욱 자연스러운 모바일 사용자 행동
    - 가속도를 가진 스크롤
    - 랜덤한 멈춤
    - 스크롤 백
    - 가변적인 터치/클릭
    """
    try:
        viewport = page.viewport_size
        if not viewport:
            return
        
        # === 1. 자연스러운 스크롤 (1-5회, 가변적) ===
        scroll_count = random.randint(1, 5)
        total_scroll = 0
        
        for i in range(scroll_count):
            # 가속도: 처음엔 천천히, 중간에 빠르게, 끝에 천천히
            if i == 0:
                acceleration = random.uniform(0.5, 0.8)  # 시작은 느리게
            elif i == scroll_count - 1:
                acceleration = random.uniform(0.6, 0.9)  # 끝도 느리게
            else:
                acceleration = random.uniform(1.0, 1.3)  # 중간은 빠르게
            
            base_scroll = random.randint(40, 250)
            scroll_amount = int(base_scroll * acceleration)
            
            page.evaluate(f"window.scrollBy({{top: {scroll_amount}, behavior: 'smooth'}})")
            total_scroll += scroll_amount
            
            # 가변적인 딜레이 (때때로 길게 멈춤)
            if random.random() > 0.7:
                time.sleep(random.uniform(1.5, 3.0))  # 읽는 시간
            else:
                time.sleep(random.uniform(HUMAN_SCROLL_DELAY_MIN, HUMAN_SCROLL_DELAY_MAX))
        
        # === 2. 스크롤 백 (실제 사용자는 가끔 위로 올림) ===
        if random.random() > 0.5 and total_scroll > 100:
            back_scroll = random.randint(30, min(150, total_scroll // 2))
            page.evaluate(f"window.scrollBy({{top: -{back_scroll}, behavior: 'smooth'}})")
            time.sleep(random.uniform(0.5, 1.2))
        
        # === 3. 랜덤 터치 이동 (손가락 움직임) ===
        touch_count = random.randint(1, 3)
        for _ in range(touch_count):
            x = random.randint(50, viewport['width'] - 50)
            y = random.randint(50, viewport['height'] - 50)
            
            # 부드러운 이동 (여러 단계로)
            steps = random.randint(3, 8)
            page.mouse.move(x, y, steps=steps)
            time.sleep(random.uniform(HUMAN_MOUSE_MOVE_DELAY_MIN, HUMAN_MOUSE_MOVE_DELAY_MAX))
        
        # === 4. 가끔 화면 터치 (클릭) ===
        if random.random() > 0.6:
            x = random.randint(100, viewport['width'] - 100)
            y = random.randint(100, viewport['height'] - 100)
            page.mouse.click(x, y)
            time.sleep(random.uniform(HUMAN_CLICK_DELAY_MIN, HUMAN_CLICK_DELAY_MAX))
        
    except Exception as e:
        print(f"   ⚠️ 행동 시뮬레이션 경고: {e}")

def handle_youtube_consent(page, timeout=10000):
    """
    유튜브 쿠키/개인정보 동의 페이지 처리
    개선: 실제 사용자처럼 읽는 시간 추가
    """
    try:
        url = page.url
        host = urlparse(url).hostname or ""
        
        if "consent.youtube.com" not in host:
            return False

        # 실제 사용자처럼 동의 페이지를 읽는 시간
        read_time = random.uniform(CONSENT_READ_TIME_MIN, CONSENT_READ_TIME_MAX)
        print(f"[Consent] 📖 동의 페이지 읽는 중... ({read_time:.1f}초)")
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
            print("[Consent] ✅ 유튜브 동의 '모두 수락' 클릭 완료")
            
            # 클릭 후 리다이렉트 대기
            page.wait_for_load_state("networkidle", timeout=timeout)
            return True
        
        print("[Consent] save 폼을 찾지 못함 → 스킵")
        return False
    except Exception as e:
        print(f"[Consent] ⚠ 처리 중 예외 발생: {e}")
        return False

# ===================== 4. 개선된 헤더 생성 =====================

def get_random_headers(profile):
    """
    더 자연스러운 HTTP 헤더
    - DNT는 5%만 사용
    - Sec-CH-UA 추가
    """
    headers = {
        "Accept-Language": profile['accept_languages'][0],
    }
    
    # DNT는 실제로 5% 미만만 사용
    if random.random() < 0.05:
        headers["DNT"] = "1"
    
    return headers

# ===================== 5. 워커 함수 (YouTube 시청 봇) =====================

def monitor_service(url, proxy_url, index, stop_event, r):
    """
    개선된 YouTube Shorts 자동 시청 봇
    - 강화된 Stealth
    - 자연스러운 행동 패턴
    - Shorts에 맞는 짧은 시청 시간
    """
    success = False
    region_name = random.choice(list(REGION_PROFILES.keys()))
    profile = REGION_PROFILES[region_name]
    
    selected_device_name = random.choice(MOBILE_DEVICES_LIST)
    
    print(f"[Bot-{index}] 🌍 {region_name} | 📱 {selected_device_name} | 🔗 {proxy_url}")

    browser = None
    try:
        if stop_event.is_set():
            return
        
        window_pos = calculate_window_position(index, NUM_BROWSERS)
            
        with sync_playwright() as p:
            # 1. 기기 프리셋 로드
            device_info = dict(p.devices[selected_device_name])
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
                    "--disable-infobars",
                    "--disable-dev-shm-usage",
                    "--no-first-run",
                    "--no-default-browser-check",
                ],
                timeout=BROWSER_LAUNCH_TIMEOUT * 1000
            )

            # 2. 브라우저 컨텍스트 생성 (permission 제거)
            context = browser.new_context(
                **device_info,
                user_agent=device_agent,
                locale=profile['locale'],
                timezone_id=profile['timezone'],
                extra_http_headers=get_random_headers(profile),
                # permissions 제거 (불필요하고 의심스러움)
            )
            
            context.set_default_timeout(CONTEXT_DEFAULT_TIMEOUT * 1000)
            page = context.new_page()

            # 3. 강화된 Stealth 주입
            platform = 'iPhone' if 'iPhone' in selected_device_name else 'Linux armv8l'
            inject_mobile_properties(page, platform)

            # 4. 페이지 이동
            chosen_referer = random.choice(profile['referers'])
            retry_count = 0
            page_loaded = False
            
            while retry_count < PAGE_LOAD_MAX_RETRIES and not page_loaded and not stop_event.is_set():
                try:
                    page.goto(url, referer=chosen_referer, wait_until="commit", timeout=PAGE_LOAD_TIMEOUT * 1000)
                    page_loaded = True
                except Exception as e:
                    retry_count += 1
                    print(f"[Bot-{index}] ⚠️ 로딩 재시도 ({retry_count}/{PAGE_LOAD_MAX_RETRIES}): {e}")
                    time.sleep(random.uniform(PAGE_LOAD_RETRY_DELAY_MIN, PAGE_LOAD_RETRY_DELAY_MAX))
            
            if not page_loaded:
                raise Exception("페이지 로딩 최종 실패")

            # 5. Consent 처리 (자연스러운 딜레이 포함)
            time.sleep(random.uniform(3, 6))
            handle_youtube_consent(page)

            # 유튜브 초기화 대기
            time.sleep(random.uniform(YOUTUBE_INIT_DELAY_MIN, YOUTUBE_INIT_DELAY_MAX))
            
            # 중앙 클릭으로 재생 트리거 (자연스럽게)
            v_size = page.viewport_size
            if v_size:
                # 정확히 중앙이 아닌 약간 랜덤하게
                center_x = v_size['width'] // 2 + random.randint(-50, 50)
                center_y = v_size['height'] // 2 + random.randint(-50, 50)
                page.mouse.move(center_x, center_y, steps=random.randint(5, 10))
                time.sleep(random.uniform(0.2, 0.5))
                page.mouse.click(center_x, center_y)
            
            # 6. 시청 모니터링 (상태 체크 에러 카운팅 추가)
            watch_duration = random.uniform(VIDEO_WATCH_TIME_MIN, VIDEO_WATCH_TIME_MAX)
            elapsed = 0
            last_video_time = 0
            behavior_interval = random.randint(20, 40)  # 20-40초마다 행동
            consecutive_errors = 0  # 연속 에러 카운터
            
            print(f"[Bot-{index}] 🎬 시청 시작 (목표: {watch_duration:.0f}초)")
            
            while elapsed < watch_duration and not stop_event.is_set():
                time.sleep(VIDEO_STATUS_CHECK_INTERVAL)
                elapsed += VIDEO_STATUS_CHECK_INTERVAL
                
                try:
                    # 영상 상태 체크
                    status = page.evaluate("""() => {
                        const v = document.querySelector('video');
                        return v ? {time: v.currentTime, paused: v.paused} : null;
                    }""")
                    
                    if status:
                        is_playing = not status['paused'] and status['time'] > last_video_time
                        icon = "▶️" if is_playing else "⏸️"
                        print(f"[Bot-{index}] {icon} {elapsed:.0f}/{watch_duration:.0f}초 (영상:{status['time']:.1f}초)")
                        last_video_time = status['time']
                        consecutive_errors = 0  # 성공하면 에러 카운터 리셋
                    else:
                        consecutive_errors += 1
                        print(f"[Bot-{index}] ⚠️ 영상 상태 없음 (에러: {consecutive_errors}/{MAX_STATUS_CHECK_ERRORS})")
                    
                    # 연속 에러 3번이면 종료
                    if consecutive_errors >= MAX_STATUS_CHECK_ERRORS:
                        print(f"[Bot-{index}] 🛑 상태 체크 연속 실패 {MAX_STATUS_CHECK_ERRORS}번 → 작업 종료")
                        break
                    
                    # 랜덤한 간격으로 행동 수행
                    if elapsed % behavior_interval == 0:
                        simulate_mobile_behavior(page)
                        behavior_interval = random.randint(20, 40)  # 다음 간격도 랜덤
                        
                except Exception as e:
                    consecutive_errors += 1
                    print(f"[Bot-{index}] ⚠️ 상태 체크 오류 (에러: {consecutive_errors}/{MAX_STATUS_CHECK_ERRORS}): {e}")
                    
                    # 연속 에러 3번이면 종료
                    if consecutive_errors >= MAX_STATUS_CHECK_ERRORS:
                        print(f"[Bot-{index}] 🛑 상태 체크 연속 실패 {MAX_STATUS_CHECK_ERRORS}번 → 작업 종료")
                        break
            
            # elapsed가 watch_duration에 도달했고 에러가 없었으면 성공
            if elapsed >= watch_duration and consecutive_errors < MAX_STATUS_CHECK_ERRORS:
                success = True
                print(f"[Bot-{index}] ✅ 시청 성공 완료")
            else:
                print(f"[Bot-{index}] ⚠️ 시청 미완료 (경과: {elapsed:.0f}초, 목표: {watch_duration:.0f}초)")

    except Exception as e:
        print(f"[Bot-{index}] 🛑 에러 발생: {e}")
    finally:
        # 7. 리소스 정리 및 Redis 상태 업데이트
        if browser:
            try:
                browser.close()
            except:
                pass
        
        if r and proxy_url:
            r.zrem(REDIS_ZSET_LEASE, proxy_url)
            penalty = 0 if success else PROXY_PENALTY_TIME
            r.zadd(REDIS_ZSET_ALIVE, {proxy_url: int(time.time()) + penalty})

# ===================== 6. 메인 루프 =====================

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

    print("=" * 80)
    print("🚀 개선된 YouTube Shorts 시청 봇")
    print("=" * 80)
    print(f"📱 슬롯: {NUM_BROWSERS}개")
    print(f"⏱️  시청 시간: {VIDEO_WATCH_TIME_MIN}-{VIDEO_WATCH_TIME_MAX}초 (4-5분, 프록시 느린 경우 대비)")
    print(f"🎭 Stealth: 강화된 탐지 회피 (랜덤 GPU, Canvas 노이즈, Playwright 흔적 제거)")
    print(f"🤖 행동: 자연스러운 스크롤, 터치, 클릭 패턴")
    print(f"🔍 안전장치: 상태 체크 {MAX_STATUS_CHECK_ERRORS}회 연속 실패 시 자동 종료")
    print("=" * 80)

    try:
        while not stop_event.is_set():
            for slot in list(active_slots.keys()):
                if not active_slots[slot].is_alive():
                    del active_slots[slot]
            
            if len(active_slots) < NUM_BROWSERS:
                for slot in range(NUM_BROWSERS):
                    if slot not in active_slots:
                        # 랜덤 lease 시간
                        lease_time = random.randint(PROXY_LEASE_TIME_MIN, PROXY_LEASE_TIME_MAX)
                        proxy = r.eval(_LUA_CLAIM, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, int(time.time()), lease_time)
                        if proxy:
                            url = TARGET_URL if (slot % 2 == 0) else TARGET_URL1
                            t = threading.Thread(target=monitor_service, args=(url, proxy, slot, stop_event, r), daemon=True)
                            t.start()
                            active_slots[slot] = t
                            print(f"[Main] ✅ 슬롯-{slot} 활성화 (lease: {lease_time}초)")
                            break
                time.sleep(MAIN_LOOP_SLOT_CHECK_DELAY)
            time.sleep(MAIN_LOOP_ITERATION_DELAY)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        print("\n🛑 종료 중...")
        for t in active_slots.values(): 
            t.join(timeout=THREAD_JOIN_TIMEOUT)
        print("✅ 모든 봇 종료 완료")