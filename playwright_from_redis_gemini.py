import random
import threading
import time
import json
import redis
from playwright.sync_api import sync_playwright
# playwright-stealth 대신 수동으로 stealth 기능 구현

# ===================== 1. 설정 및 데이터 로드 =====================
TARGET_URL = "https://www.youtube.com/shorts/5y-_oaunCCQ?feature=share"
TARGET_URL1 = "https://youtube.com/shorts/-vVnZoVtnFk?feature=share"
TARGET_URL1 = "https://youtube.com/shorts/-vVnZoVtnFk?feature=share" #크리스마스
TARGET_URL = "https://www.youtube.com/shorts/u7sO-mNEpT4?feature=share" #크리스마스 2

NUM_BROWSERS = 1
REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_LEASE = "proxies:lease"

# 화면 레이아웃 설정
SCREEN_WIDTH = 1920  # 모니터 전체 너비 (필요시 수정)
SCREEN_HEIGHT = 1080  # 모니터 전체 높이 (필요시 수정)

def calculate_window_position(index, total_browsers):
    """브라우저 인덱스에 따라 창 위치와 크기 계산"""
    # 그리드 레이아웃 계산 (예: 3개면 1x3, 4개면 2x2)
    if total_browsers <= 3:
        cols = total_browsers
        rows = 1
    elif total_browsers <= 4:
        cols = 2
        rows = 2
    elif total_browsers <= 6:
        cols = 3
        rows = 2
    else:
        cols = 3
        rows = (total_browsers + 2) // 3
    
    # 각 창의 크기
    window_width = SCREEN_WIDTH // cols
    window_height = SCREEN_HEIGHT // rows
    
    # 현재 인덱스의 위치
    row = index // cols
    col = index % cols
    
    # 위치 계산
    x = col * window_width
    y = row * window_height
    
    # 약간의 여백 추가 (타이틀바 고려)
    padding = 0
    
    return {
        'x': x + padding,
        'y': y + padding,
        'width': window_width - (padding * 2),
        'height': window_height - (padding * 2)
    }

# JSON 프로필 로드
with open('region_profiles.json', 'r', encoding='utf-8') as f:
    REGION_PROFILES = json.load(f)

stop_event = threading.Event()

def get_redis():
    return redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

# ===================== 2. 모바일 행동 시뮬레이션 함수 =====================

def simulate_mobile_behavior(page):
    """실제 모바일 사용자처럼 행동 시뮬레이션"""
    try:
        # 1. 랜덤 스크롤 (모바일 스와이프 느낌) - 더 자연스럽게
        for _ in range(random.randint(2, 4)):
            scroll_amount = random.randint(30, 150)
            page.evaluate(f"window.scrollBy(0, {scroll_amount})")
            time.sleep(random.uniform(0.5, 1.2))
        
        # 2. 마우스 움직임 (사람처럼)
        viewport = page.viewport_size
        if viewport:
            # 여러 지점으로 마우스 이동
            for _ in range(random.randint(2, 4)):
                x = random.randint(50, viewport['width'] - 50)
                y = random.randint(50, viewport['height'] - 50)
                page.mouse.move(x, y)
                time.sleep(random.uniform(0.1, 0.3))
            
            # 랜덤 클릭
            if random.random() > 0.5:
                x = random.randint(100, viewport['width'] - 100)
                y = random.randint(100, viewport['height'] - 100)
                page.mouse.click(x, y)
                time.sleep(random.uniform(0.5, 1.5))
        
    except Exception as e:
        print(f"   ⚠️  행동 시뮬레이션 중 경고: {e}")

def inject_mobile_properties(page):
    """모바일 환경 JavaScript 속성 주입 + Stealth 기능"""
    page.add_init_script("""
        // ========== Stealth 기능 (자동화 탐지 방지) ==========
        
        // 1. webdriver 속성 제거 (가장 중요)
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
        
        // 2. Chrome 객체 추가
        window.navigator.chrome = {
            runtime: {},
            loadTimes: function() {},
            csi: function() {},
            app: {}
        };
        
        // 3. Permissions API 수정
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({state: Notification.permission}) :
                originalQuery(parameters)
        );
        
        // 4. Plugins 배열 추가 (실제 Chrome과 유사하게)
        Object.defineProperty(navigator, 'plugins', {
            get: () => [
                {
                    0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format"},
                    description: "Portable Document Format",
                    filename: "internal-pdf-viewer",
                    length: 1,
                    name: "Chrome PDF Plugin"
                },
                {
                    0: {type: "application/pdf", suffixes: "pdf", description: "Portable Document Format"},
                    description: "Portable Document Format", 
                    filename: "mhjfbmdgcfjbbpaeojofohoefgiehjai",
                    length: 1,
                    name: "Chrome PDF Viewer"
                },
                {
                    0: {type: "application/x-nacl", suffixes: "", description: "Native Client Executable"},
                    1: {type: "application/x-pnacl", suffixes: "", description: "Portable Native Client Executable"},
                    description: "",
                    filename: "internal-nacl-plugin",
                    length: 2,
                    name: "Native Client"
                }
            ]
        });
        
        // 5. Languages 일관성
        Object.defineProperty(navigator, 'languages', {
            get: () => ['ko-KR', 'ko', 'en-US', 'en']
        });
        
        // 6. Hardware Concurrency (CPU 코어 수)
        Object.defineProperty(navigator, 'hardwareConcurrency', {
            get: () => 8
        });
        
        // 7. Device Memory
        Object.defineProperty(navigator, 'deviceMemory', {
            get: () => 8
        });
        
        // ========== 모바일 환경 속성 ==========
        
        // 8. 터치 이벤트 지원 강화
        Object.defineProperty(navigator, 'maxTouchPoints', {
            get: () => 5
        });
        
        // 9. 모바일 플랫폼 정보
        Object.defineProperty(navigator, 'platform', {
            get: () => 'Linux armv8l'
        });
        
        // 10. 배터리 API
        navigator.getBattery = () => Promise.resolve({
            charging: Math.random() > 0.5,
            chargingTime: 0,
            dischargingTime: Infinity,
            level: Math.random() * 0.5 + 0.3
        });
        
        // 11. 네트워크 정보
        Object.defineProperty(navigator, 'connection', {
            get: () => ({
                effectiveType: ['4g', '3g'][Math.floor(Math.random() * 2)],
                downlink: Math.random() * 10 + 1,
                rtt: Math.random() * 100 + 50,
                saveData: false
            })
        });
        
        // 12. WebGL 모바일 특성
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(param) {
            if (param === 37445) return 'ARM';
            if (param === 37446) return 'Mali-G72';
            return getParameter.apply(this, arguments);
        };
        
        // 13. 자동화 감지 우회 - 더 강력하게
        delete Object.getPrototypeOf(navigator).webdriver;
        
        // 14. iframe 체크 우회
        Object.defineProperty(window, 'outerWidth', {
            get: () => window.innerWidth
        });
        Object.defineProperty(window, 'outerHeight', {
            get: () => window.innerHeight
        });
        
        // 15. toString 메서드 재정의 (탐지 우회)
        const toStringProxy = new Proxy(Function.prototype.toString, {
            apply: function(target, thisArg, args) {
                if (thisArg === WebGLRenderingContext.prototype.getParameter) {
                    return 'function getParameter() { [native code] }';
                }
                return target.apply(thisArg, args);
            }
        });
        Function.prototype.toString = toStringProxy;
        
        // 16. Canvas Fingerprinting 방어
        const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function(type) {
            if (type === 'image/png' && this.width === 280 && this.height === 60) {
                // reCAPTCHA 캔버스 크기 - 약간의 노이즈 추가
                const context = this.getContext('2d');
                const imageData = context.getImageData(0, 0, this.width, this.height);
                for (let i = 0; i < imageData.data.length; i += 4) {
                    imageData.data[i] += Math.floor(Math.random() * 3) - 1;
                }
                context.putImageData(imageData, 0, 0);
            }
            return originalToDataURL.apply(this, arguments);
        };
        
        // 17. 마우스 이벤트 타이밍 (사람처럼)
        let lastMouseMove = Date.now();
        document.addEventListener('mousemove', function() {
            lastMouseMove = Date.now();
        }, true);
    """)

# ===================== 3. 워커 함수 (강화된 모바일 시뮬레이션) =====================

def monitor_service(url, proxy_url, index, stop_event, r):
    success = False
    region_name = random.choice(list(REGION_PROFILES.keys()))
    profile = REGION_PROFILES[region_name]
    
    print(f"[Bot-{index}] 🌍 지역: {region_name} | 프록시: {proxy_url}")

    browser = None
    try:
        # 종료 신호 체크
        if stop_event.is_set():
            print(f"[Bot-{index}] 🛑 종료 신호 감지, 시작 취소")
            return
        
        # 창 위치 계산
        window_pos = calculate_window_position(index, NUM_BROWSERS)
        print(f"[Bot-{index}] 📐 창 위치: x={window_pos['x']}, y={window_pos['y']}, {window_pos['width']}x{window_pos['height']}")
            
        with sync_playwright() as p:
            # 1. 브라우저 실행 (창 위치 지정)
            browser = p.chromium.launch(
                headless=False,
                proxy={"server": proxy_url} if proxy_url else None,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=IsolateOrigins,site-per-process",
                    f"--window-position={window_pos['x']},{window_pos['y']}",
                    f"--window-size={window_pos['width']},{window_pos['height']}",
                    "--autoplay-policy=no-user-gesture-required",
                    "--disable-web-security",
                    # 추가 봇 탐지 우회 옵션
                    "--disable-blink-features=AutomationControlled",
                    "--exclude-switches=enable-automation",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-accelerated-2d-canvas",
                    "--disable-gpu",
                    "--start-maximized",
                    "--disable-infobars",
                    "--disable-extensions",
                ],
                timeout=60000
            )

            # 2. 모바일 기기 프로필 (Galaxy S9+)
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
                    "DNT": "1",
                    "Upgrade-Insecure-Requests": "1"
                },
                permissions=["geolocation"],
                geolocation={"latitude": 37.5665, "longitude": 126.9780}
            )
            
            # 컨텍스트 타임아웃 설정 (프록시용)
            context.set_default_timeout(90000)
            context.set_default_navigation_timeout(90000)

            page = context.new_page()

            # 종료 신호 체크
            if stop_event.is_set():
                print(f"[Bot-{index}] 🛑 종료 신호 감지, 브라우저 닫기")
                browser.close()
                return

            # 4. Stealth + 모바일 JavaScript 속성 주입
            inject_mobile_properties(page)
            
            # 자동재생 정책 추가
            page.add_init_script("""
                Object.defineProperty(document, 'hidden', {
                    get: () => false
                });
                Object.defineProperty(document, 'visibilityState', {
                    get: () => 'visible'
                });
            """)

            # 5. Referer와 함께 페이지 이동 (재시도 로직)
            chosen_referer = random.choice(profile['referers'])
            print(f"[Bot-{index}] 🔗 리퍼러: {chosen_referer}")
            
            max_retries = 3
            retry_count = 0
            page_loaded = False
            
            while retry_count < max_retries and not page_loaded and not stop_event.is_set():
                try:
                    print(f"[Bot-{index}] 🔄 페이지 로딩 시도 {retry_count + 1}/{max_retries}...")
                    page.goto(
                        url, 
                        referer=chosen_referer, 
                        wait_until="networkidle",  # 네트워크 안정화까지 대기
                        timeout=90000
                    )
                    page_loaded = True
                    print(f"[Bot-{index}] ✅ 페이지 로딩 성공")
                except Exception as goto_error:
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_before_retry = random.uniform(3, 7)
                        print(f"[Bot-{index}] ⚠️  로딩 실패, {wait_before_retry:.1f}초 후 재시도... ({goto_error})")
                        time.sleep(wait_before_retry)
                    else:
                        raise goto_error
            
            # 종료 신호 체크
            if stop_event.is_set():
                print(f"[Bot-{index}] 🛑 종료 신호 감지, 브라우저 닫기")
                browser.close()
                return
            
            # 6. YouTube Shorts 로딩 대기
            print(f"[Bot-{index}] ⏳ YouTube Shorts 초기화 대기...")
            time.sleep(random.uniform(3, 6))  # 랜덤 대기로 더 자연스럽게
            
            # 6-1. 사람처럼 마우스 움직임 추가
            viewport = page.viewport_size
            if viewport:
                for _ in range(random.randint(3, 6)):
                    x = random.randint(50, viewport['width'] - 50)
                    y = random.randint(50, viewport['height'] - 50)
                    page.mouse.move(x, y)
                    time.sleep(random.uniform(0.1, 0.3))
            
            # 7. 비디오 재생 상태 확인 및 강제 재생
            video_status = page.evaluate("""() => {
                const video = document.querySelector('video');
                if (!video) return {found: false};
                
                // 음소거 해제 및 재생
                video.muted = false;
                video.volume = 0.5;
                
                try {
                    video.play().catch(e => console.log('Play error:', e));
                } catch(e) {}
                
                return {
                    found: true,
                    paused: video.paused,
                    currentTime: video.currentTime,
                    readyState: video.readyState,
                    src: video.src || video.currentSrc || 'no src'
                };
            }""")
            
            if video_status['found']:
                status_icon = "⏸️" if video_status['paused'] else "▶️"
                print(f"[Bot-{index}] {status_icon} 비디오 발견 - 재생:{not video_status['paused']}, 준비:{video_status['readyState']}/4")
            else:
                print(f"[Bot-{index}] ❌ 비디오 요소 없음 - 페이지 문제 가능성")
            
            # 8. 화면 중앙 클릭 (재생 트리거)
            viewport = page.viewport_size
            if viewport:
                center_x = viewport['width'] // 2
                center_y = viewport['height'] // 2
                try:
                    page.mouse.click(center_x, center_y)
                    time.sleep(1)
                    page.mouse.click(center_x, center_y)  # 재클릭
                    print(f"[Bot-{index}] 🖱️  화면 클릭으로 재생 시도")
                except:
                    pass
            
            time.sleep(2)

            # 9. 모바일 행동 시뮬레이션
            if not stop_event.is_set():
                simulate_mobile_behavior(page)

            # 10. 시청 시뮬레이션 (종료 신호 체크하면서)
            wait_time = random.uniform(180, 220)
            print(f"[Bot-{index}] ⏱️  {wait_time:.1f}초 시청 시뮬레이션...")
            
            # 5초마다 재생 상태 체크
            elapsed = 0
            check_interval = 5
            last_time = 0
            
            while elapsed < wait_time and not stop_event.is_set():
                time.sleep(min(check_interval, wait_time - elapsed))
                elapsed += check_interval
                
                # 재생 상태 확인
                try:
                    current_status = page.evaluate("""() => {
                        const video = document.querySelector('video');
                        if (!video) return null;
                        return {
                            time: video.currentTime,
                            paused: video.paused
                        };
                    }""")
                    
                    if current_status:
                        is_playing = not current_status['paused'] and current_status['time'] > last_time
                        icon = "▶️" if is_playing else "⏸️"
                        print(f"[Bot-{index}] {icon} {elapsed:.0f}초 경과 - 영상: {current_status['time']:.1f}초")
                        last_time = current_status['time']
                        
                        # 재생이 안되고 있으면 다시 클릭 시도
                        if current_status['paused'] and elapsed < wait_time / 2:
                            print(f"[Bot-{index}] 🔄 일시정지 감지, 재생 재시도")
                            if viewport:
                                page.mouse.click(viewport['width'] // 2, viewport['height'] // 2)
                except:
                    pass
            
            if stop_event.is_set():
                print(f"[Bot-{index}] 🛑 종료 신호 감지, 시청 중단")
                browser.close()
                return
            
            # 11. 추가 인터랙션
            if random.random() > 0.5 and not stop_event.is_set():
                simulate_mobile_behavior(page)
            
            success = True
            browser.close()
            print(f"[Bot-{index}] ✅ 완료 ({region_name})")

    except Exception as e:
        print(f"[Bot-{index}] 🛑 에러: {e}")
        if "ERR_TIMED_OUT" in str(e):
            print(f"[Bot-{index}] 💀 프록시 타임아웃: {proxy_url}")
        elif "ERR_PROXY_CONNECTION_FAILED" in str(e):
            print(f"[Bot-{index}] 💀 프록시 연결 실패: {proxy_url}")
        elif "ERR_TUNNEL_CONNECTION_FAILED" in str(e):
            print(f"[Bot-{index}] 💀 프록시 터널 실패: {proxy_url}")
    finally:
        # 브라우저 강제 종료
        try:
            if browser:
                browser.close()
        except:
            pass
            
        if r and proxy_url:
            # 성공 시 즉시 반납, 실패 시 60초 대기 후 재사용
            penalty_time = 0 if success else 60
            r.zrem(REDIS_ZSET_LEASE, proxy_url)
            r.zadd(REDIS_ZSET_ALIVE, {proxy_url: int(time.time()) + penalty_time})
            if not success:
                print(f"[Bot-{index}] ⏳ 프록시 {penalty_time}초 페널티 부여")

# ===================== 4. 프록시 없이 테스트 함수 =====================

def test_without_proxy(url, region_name="korea"):
    """프록시 없이 직접 연결 테스트 - 영상 재생 확인용"""
    print(f"\n{'='*60}")
    print(f"🧪 테스트 모드: 프록시 없이 직접 연결")
    print(f"{'='*60}\n")
    
    profile = REGION_PROFILES.get(region_name, REGION_PROFILES["korea"])
    print(f"[TEST] 🌍 지역 설정: {region_name}")
    
    try:
        with sync_playwright() as p:
            # 브라우저 실행 (프록시 없음)
            browser = p.chromium.launch(
                headless=False,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--autoplay-policy=no-user-gesture-required",  # 자동재생 허용
                    "--disable-web-security",  # CORS 우회
                ]
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
                    "DNT": "1",
                    "Upgrade-Insecure-Requests": "1"
                },
                permissions=["geolocation"],
                geolocation={"latitude": 37.5665, "longitude": 126.9780}
            )
            
            context.set_default_timeout(30000)
            page = context.new_page()
            
            # Stealth 적용
            inject_mobile_properties(page)
            
            # 자동재생 정책 추가 설정
            page.add_init_script("""
                // 자동재생 허용
                Object.defineProperty(document, 'hidden', {
                    get: () => false
                });
                Object.defineProperty(document, 'visibilityState', {
                    get: () => 'visible'
                });
            """)
            
            chosen_referer = random.choice(profile['referers'])
            print(f"[TEST] 🔗 리퍼러: {chosen_referer}")
            print(f"[TEST] 🎬 URL 접속 중: {url}")
            
            # 페이지 이동
            page.goto(url, referer=chosen_referer, wait_until="networkidle")
            print(f"[TEST] ✅ 페이지 로딩 완료")
            
            # 초기 대기 (YouTube Shorts 로딩 시간)
            print(f"[TEST] ⏳ YouTube Shorts 초기화 대기 (5초)...")
            time.sleep(5)
            
            # 영상 재생 상태 체크
            print(f"[TEST] 🔍 영상 재생 상태 확인 중...")
            
            # YouTube Shorts 비디오 요소 확인 및 강제 재생
            video_check = page.evaluate("""() => {
                const video = document.querySelector('video');
                if (!video) {
                    return {found: false, message: '비디오 요소 없음'};
                }
                
                // 음소거 해제 및 볼륨 설정
                video.muted = false;
                video.volume = 0.5;
                
                // 재생 시도
                let playResult = 'not attempted';
                try {
                    video.play().then(() => {
                        console.log('Video play succeeded');
                    }).catch(e => {
                        console.log('Video play failed:', e);
                    });
                    playResult = 'attempted';
                } catch (e) {
                    playResult = 'error: ' + e.message;
                }
                
                return {
                    found: true,
                    paused: video.paused,
                    currentTime: video.currentTime,
                    duration: video.duration,
                    readyState: video.readyState,
                    muted: video.muted,
                    volume: video.volume,
                    playResult: playResult,
                    src: video.src || video.currentSrc || 'no src'
                };
            }""")
            
            if video_check['found']:
                print(f"[TEST] 📹 비디오 요소 발견!")
                print(f"       - 재생 중: {'아니오 ❌' if video_check['paused'] else '예 ✅'}")
                print(f"       - 현재 시간: {video_check['currentTime']:.2f}초")
                print(f"       - 전체 길이: {video_check.get('duration', 'N/A')}")
                print(f"       - 준비 상태: {video_check['readyState']}/4")
                print(f"       - 음소거: {'예' if video_check['muted'] else '아니오'}")
                print(f"       - 볼륨: {video_check['volume']}")
                print(f"       - 재생 시도: {video_check['playResult']}")
                print(f"       - 소스: {video_check['src'][:80]}...")
                
            else:
                print(f"[TEST] ❌ 비디오 요소를 찾을 수 없음: {video_check.get('message', 'Unknown')}")
                print(f"[TEST] 🔍 페이지 구조 분석 중...")
                
                # 페이지에 있는 모든 요소 확인
                elements = page.evaluate("""() => {
                    return {
                        videos: document.querySelectorAll('video').length,
                        iframes: document.querySelectorAll('iframe').length,
                        shortsPlayer: document.querySelector('#shorts-player') ? 'found' : 'not found',
                        ytdApp: document.querySelector('ytd-app') ? 'found' : 'not found'
                    };
                }""")
                print(f"       - Video 태그: {elements['videos']}개")
                print(f"       - Iframe: {elements['iframes']}개")
                print(f"       - Shorts Player: {elements['shortsPlayer']}")
                print(f"       - YTD App: {elements['ytdApp']}")
            
            # 화면 중앙 클릭 (YouTube Shorts는 클릭으로 재생/일시정지)
            print(f"[TEST] 🖱️  화면 중앙 클릭 시도...")
            viewport = page.viewport_size
            if viewport:
                center_x = viewport['width'] // 2
                center_y = viewport['height'] // 2
                page.mouse.click(center_x, center_y)
                time.sleep(1)
                
                # 재클릭 (일시정지 -> 재생)
                page.mouse.click(center_x, center_y)
                time.sleep(2)
            
            # 모바일 터치 시뮬레이션
            print(f"[TEST] 📱 모바일 터치 시뮬레이션...")
            try:
                page.evaluate("""() => {
                    const video = document.querySelector('video');
                    if (video) {
                        // 간단한 클릭 이벤트로 대체 (터치 대신)
                        const clickEvent = new MouseEvent('click', {
                            bubbles: true,
                            cancelable: true,
                            view: window
                        });
                        video.dispatchEvent(clickEvent);
                        
                        // 강제 재생
                        video.play().catch(e => console.log('Touch play error:', e));
                    }
                }""")
            except Exception as touch_error:
                print(f"[TEST] ⚠️  터치 시뮬레이션 스킵: {touch_error}")
            
            time.sleep(2)
            
            # 재생 상태 재확인
            video_check2 = page.evaluate("""() => {
                const video = document.querySelector('video');
                if (!video) return {found: false};
                return {
                    found: true,
                    paused: video.paused,
                    currentTime: video.currentTime,
                    playbackRate: video.playbackRate,
                    networkState: video.networkState,
                    error: video.error ? video.error.message : null
                };
            }""")
            
            if video_check2['found']:
                print(f"\n[TEST] 🔄 인터랙션 후 상태:")
                print(f"       - 재생 중: {'아니오 ❌' if video_check2['paused'] else '예 ✅'}")
                print(f"       - 현재 시간: {video_check2['currentTime']:.2f}초")
                print(f"       - 재생 속도: {video_check2['playbackRate']}x")
                print(f"       - 네트워크 상태: {video_check2['networkState']}")
                if video_check2['error']:
                    print(f"       - 에러: {video_check2['error']}")
            
            # 시청 시뮬레이션
            wait_time = 30
            print(f"\n[TEST] ⏱️  {wait_time}초 시청 테스트")
            print(f"[TEST] 💡 브라우저 창에서 직접 확인하세요")
            print(f"[TEST] 💡 수동으로 재생 버튼을 눌러보세요")
            print(f"[TEST] 💡 Ctrl+C로 언제든 종료 가능\n")
            
            for i in range(wait_time):
                time.sleep(1)
                if (i + 1) % 5 == 0:
                    # 5초마다 재생 위치 확인
                    status = page.evaluate("""() => {
                        const video = document.querySelector('video');
                        if (!video) return null;
                        return {
                            time: video.currentTime,
                            paused: video.paused,
                            buffered: video.buffered.length > 0 ? video.buffered.end(0) : 0
                        };
                    }""")
                    
                    if status:
                        icon = "▶️" if not status['paused'] else "⏸️"
                        print(f"[TEST] {icon} {i+1}초 경과 - 영상: {status['time']:.2f}초 / 버퍼: {status['buffered']:.2f}초")
                    else:
                        print(f"[TEST] ⏱️  {i+1}초 경과 - 비디오 없음")
            
            print(f"\n[TEST] ✅ 테스트 완료 (브라우저를 15초 후 닫습니다)")
            print(f"[TEST] 💡 영상이 재생되지 않았다면:")
            print(f"       1. YouTube 로그인 필요 여부 확인")
            print(f"       2. 지역 제한 확인")
            print(f"       3. 연령 제한 확인")
            print(f"       4. 브라우저 콘솔 에러 확인 (F12)")
            time.sleep(15)
            browser.close()
            
    except KeyboardInterrupt:
        print(f"\n[TEST] ⏹️  사용자가 테스트 중단")
        try:
            browser.close()
        except:
            pass
    except Exception as e:
        print(f"[TEST] 🛑 에러: {e}")
        import traceback
        traceback.print_exc()

# ===================== 5. 메인 루프 =====================

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
    
    # 테스트 모드 체크
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # 사용법: python playwright_from_redis_gemini.py test
        # 또는:   python playwright_from_redis_gemini.py test japan
        region = sys.argv[2] if len(sys.argv) > 2 else "korea"
        test_url = TARGET_URL  # 기본 URL
        
        print(f"\n🧪 테스트 모드로 실행")
        print(f"   URL: {test_url}")
        print(f"   지역: {region}")
        print(f"\n💡 사용 가능한 지역: {', '.join(REGION_PROFILES.keys())}")
        print(f"💡 다른 URL 테스트: 코드에서 test_url 변경\n")
        
        test_without_proxy(test_url, region)
        sys.exit(0)
    
    # 일반 모드 (프록시 사용)
    print(f"\n{'='*60}")
    print(f"🚀 일반 모드: Redis 프록시 풀 사용")
    print(f"   동시 브라우저: {NUM_BROWSERS}개")
    print(f"   화면 레이아웃: {SCREEN_WIDTH}x{SCREEN_HEIGHT}")
    print(f"   타겟 URL 1: {TARGET_URL}")
    print(f"   타겟 URL 2: {TARGET_URL1}")
    print(f"{'='*60}\n")
    print(f"💡 화면 크기 변경: 코드 상단의 SCREEN_WIDTH, SCREEN_HEIGHT 수정")
    print(f"💡 테스트 모드 실행: python {sys.argv[0]} test")
    print(f"💡 Ctrl+C로 종료\n")
    
    r = get_redis()
    threads = []
    worker_index = 0
    active_slots = {}  # 슬롯별 스레드 추적 {슬롯번호: 스레드}
    
    # 시그널 핸들러 등록 (Ctrl+C)
    def signal_handler(signum, frame):
        print(f"\n\n{'='*60}")
        print(f"🛑 종료 신호 수신 (Ctrl+C)")
        print(f"{'='*60}")
        stop_event.set()
        print(f"⏳ 실행 중인 봇들 종료 대기 중... (최대 10초)")
        print(f"   - 활성 스레드: {len([t for t in threads if t.is_alive()])}개")
        
    signal.signal(signal.SIGINT, signal_handler)

    try:
        while not stop_event.is_set():
            # 종료된 스레드 정리 및 슬롯 확인
            for slot in list(active_slots.keys()):
                if not active_slots[slot].is_alive():
                    print(f"[Main] 🔄 슬롯-{slot} 비었음, 재사용 가능")
                    del active_slots[slot]
            
            # 빈 슬롯 찾기
            if len(active_slots) < NUM_BROWSERS:
                # 0부터 NUM_BROWSERS-1 중 비어있는 슬롯 찾기
                for slot in range(NUM_BROWSERS):
                    if slot not in active_slots:
                        # 프록시 할당
                        proxy = r.eval(_LUA_CLAIM, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, int(time.time()), 600)
                        if proxy:
                            url = TARGET_URL if (slot % 2 == 0) else TARGET_URL1
                            t = threading.Thread(
                                target=monitor_service, 
                                args=(url, proxy, slot, stop_event, r),  # slot 번호 사용
                                daemon=True,
                                name=f"Bot-{slot}"
                            )
                            t.start()
                            active_slots[slot] = t
                            print(f"[Main] ✅ 슬롯-{slot} 시작 (전체 {len(active_slots)}/{NUM_BROWSERS})")
                            break  # 한 번에 하나씩만 시작
                
                time.sleep(5)
            time.sleep(2)
    except KeyboardInterrupt:
        # 시그널 핸들러가 처리
        pass
    finally:
        if not stop_event.is_set():
            stop_event.set()
        
        print(f"\n⏳ 스레드 정리 중...")
        # 모든 스레드가 종료될 때까지 대기 (최대 10초)
        for slot, t in active_slots.items():
            if t.is_alive():
                t.join(timeout=10)
                if t.is_alive():
                    print(f"   ⚠️  슬롯-{slot} 스레드가 아직 실행 중 (강제 종료됨)")
                else:
                    print(f"   ✅ 슬롯-{slot} 정상 종료")
        
        print(f"\n{'='*60}")
        print(f"✅ 모든 봇 종료 완료")
        print(f"{'='*60}\n")