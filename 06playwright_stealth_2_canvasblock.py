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

# CanvasBlocker 확장 프로그램 경로 설정
# ⚠️ 주의: 경로에 한글이 있으면 안 됨! (Redis 키 이름 제약)
CANVASBLOCKER_PATH = os.path.join(os.getcwd(), "canvasblocker")

# 또는 절대 영문 경로 사용 (한글 경로 문제 회피)
# CANVASBLOCKER_PATH = "C:/extensions/canvasblocker"  # Windows
# CANVASBLOCKER_PATH = "/home/user/extensions/canvasblocker"  # Linux

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

# 검색 키워드 목록 - 랜덤으로 선택
SEARCH_KEYWORDS = [
    "mr redpanda",
    "funny cat videos",
    "music 2024",
    "cooking tutorial",
    "travel vlog",
    "gaming highlights",
    "workout routine",
    "tech review",
    "comedy skits",
    "educational content",
    "art tutorial",
    "science experiments",
    "movie trailers",
    "asmr sounds",
    "podcast clips",
    "cat",
    "puppy",
    "baby",
    "happy",
    "red panda",
]

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
TARGET_URL = "https://youtube.com/shorts/eewyMV23vXg?feature=share" #새해인사
TARGET_URL1 = "https://youtube.com/shorts/eewyMV23vXg?feature=share" #새해인사

NUM_BROWSERS = 1 
MOBILE_DEVICES_LIST = []

REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_LEASE = "proxies:lease"

# ✅ 개선: 쿨타임 설정 합리화
SUCCESS_COOL_DOWN = 3600*6      # 성공 시 6시간뒤 재사용 가능
FAILURE_PENALTY = 3600      # 실패 시 1시간 페널티 (24시간은 너무 김)

# ✅ 추가: 타임아웃 설정
BROWSER_LAUNCH_TIMEOUT = 60000   # 60초
PAGE_LOAD_TIMEOUT = 120000       # 120초
CONTEXT_TIMEOUT = 90000          # 90초
VIDEO_WATCH_MIN = 180
VIDEO_WATCH_MAX = 300
TIME_BEFORE_END_FOR_SPECIAL_BEHAVIOR = 240  # 나중에 재생시간이 결정되면 그때 다시 조정
   
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
    ✅ 간소화된 스텔스 로직 - YouTube 호환성 최우선
    CanvasBlocker를 사용하므로 Canvas 관련 코드 제거
    """
    page.add_init_script(f"""
        // ===== 1. 기본 WebDriver 탐지 방어 =====
        Object.defineProperty(navigator, 'webdriver', {{
            get: () => undefined
        }});
        
        // ===== 2. 간단한 Playwright 마커 제거 =====
        try {{
            delete window.__playwright;
            delete window.playwright;
        }} catch(e) {{}}
        
        console.log('✅ 기본 스텔스 활성화');
    """)

def apply_enhanced_stealth(page, config, device_name):
    """강화된 스텔스 로직 - CanvasBlocker 사용으로 Canvas 코드 제거"""
    
    stealth_scripts = [
        # 1. Playwright 마커 강화 제거 (추가)
        """
        // ===== Playwright 마커 완전 제거 =====
        (function() {
            const markers = [
                'playwright', '__playwright', '__pw', '__playwright_bound_',
                '__playwright_script__', '__playwright_evaluation_script__',
                '__playwright_mutation_observer__',
                'cdc_adoQpoasnfa76pfcZLmcfl', 'cdc_adoQpoasnfa76pfcZLmcfl_JSON',
                'cdc_adoQpoasnfa76pfcZLmcfl_Array', 'cdc_adoQpoasnfa76pfcZLmcfl_Object',
                'cdc_adoQpoasnfa76pfcZLmcfl_Promise', 'cdc_adoQpoasnfa76pfcZLmcfl_Symbol',
                'document.$cdc_asdjflasutopfhvcZLmcfl_'
            ];
            
            markers.forEach(marker => {
                try { delete window[marker]; } catch(e) {}
                try { delete document[marker]; } catch(e) {}
            });
            
            // 속성 재정의로 접근 차단
            Object.defineProperty(window, 'playwright', {
                get: () => undefined,
                set: (val) => val,
                configurable: false
            });
            
            Object.defineProperty(window, '__playwright', {
                get: () => undefined,
                set: (val) => val,
                configurable: false
            });
            
            // navigator.webdriver 완전 은닉
            Object.defineProperty(navigator, 'webdriver', {
                get: () => false,
                configurable: false,
                enumerable: false
            });
            
            // userAgent에서 Playwright/Headless 문자열 제거
            const originalUA = Object.getOwnPropertyDescriptor(navigator, 'userAgent');
            Object.defineProperty(navigator, 'userAgent', {
                get: () => {
                    const ua = originalUA ? originalUA.get() : '';
                    return ua
                        .replace(/Playwright\\/[\\d\\.]+/g, '')
                        .replace('HeadlessChrome', 'Chrome')
                        .replace(/\\(playwright\\)/g, '')
                        .trim();
                },
                configurable: true,
                enumerable: true
            });
        })();
        """,
        
        # 2. 기본 WebDriver 마스킹
        """
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        window.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'languages', { get: () => ['%s', '%s'] });
        """ % (config.get('locale', 'en-US'), 'en-US'),
        
        # 3. Permissions 스푸핑
        """
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );
        """,
        
        # 4. 플러그인 스푸핑
        """
        Object.defineProperty(navigator, 'plugins', {
            get: () => [
                { 
                    name: 'Chrome PDF Viewer', 
                    filename: 'internal-pdf-viewer',
                    description: 'Portable Document Format',
                    length: 1
                }
            ]
        });
        """,
        
        # 5. WebGL 스푸핑
        """
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) return 'Intel Inc.';
            if (parameter === 37446) return 'Intel Iris OpenGL Engine';
            return getParameter(parameter);
        };
        """,
        
        # 6. Canvas 방어는 CanvasBlocker 확장 프로그램이 처리하므로 제거
        
        # 7. Function.toString() 오버라이드 (추가)
        """
        // Playwright 함수 문자열 감지 방지
        const originalToString = Function.prototype.toString;
        Function.prototype.toString = function() {
            const str = originalToString.call(this);
            return str
                .replace(/__playwright_[a-zA-Z0-9_]+/g, '')
                .replace(/playwrightBinding/g, '')
                .replace(/\\[native code\\].*playwright.*/gi, '[native code]');
        };
        """,
        
        # 8. console.log 필터링 (추가)
        """
        // 콘솔 로그에서 Playwright 관련 내용 숨기기
        const originalLog = console.log;
        console.log = function(...args) {
            const filteredArgs = args.map(arg => {
                if (typeof arg === 'string') {
                    return arg.replace(/playwright|__pw|cdc_/gi, '[REDACTED]');
                }
                return arg;
            });
            originalLog.apply(console, filteredArgs);
        };
        """
    ]
    
    # 모든 스텔스 스크립트 적용
    for idx, script in enumerate(stealth_scripts):
        try:
            page.add_init_script(script)
            print(f"   [Stealth-{idx+1}] ✅ 적용 완료")
        except Exception as e:
            print(f"   [Stealth-{idx+1}] ⚠️ 적용 실패: {e}")
            
                       
def apply_ultimate_stealth(page, config, device_name):
    """
    ✅ 최고 수준 스텔스: 라이브러리 + 커스텀 스크립트 조합
    CanvasBlocker를 사용하므로 Canvas 관련 코드 제거
    """
    print(f"   [Stealth] 🛡️ 최고 수준 스텔스 적용 중...")
    
    # 1. playwright-stealth 라이브러리 적용
    try:
        from playwright_stealth import stealth_sync as stealth
        stealth(page)
        print(f"   [Stealth] ✅ playwright-stealth 라이브러리 적용 완료")
    except ImportError:
        # stealth_sync가 없으면 기본 stealth 시도
        try:
            from playwright.sync_api import sync_playwright
            import playwright_stealth
            playwright_stealth.stealth_sync(page)
            print(f"   [Stealth] ✅ playwright-stealth 적용 완료")
        except:
            print(f"   [Stealth] ⚠️ playwright-stealth 미설치 또는 호환 안됨")
    except Exception as e:
        print(f"   [Stealth] ⚠️ 라이브러리 적용 실패: {e}")
    
    # 2. 추가 커스텀 강화 (라이브러리가 놓친 부분 보완)
    apply_enhanced_stealth(page, config, device_name)
    
    print(f"   [Stealth] ✅ 최고 수준 스텔스 적용 완료")
                
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

def handle_youtube_consent(page, index, timeout=15000):
    """
    ✅ 수정: 유튜브 동의 페이지 처리 - 재시도 로직 추가
    """
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            current_url = page.url
            host = urlparse(current_url).hostname or ""
            
            print(f"   [Bot-{index}] [Consent-{retry_count+1}] 현재 URL: {current_url[:80]}")
            
            if "consent.youtube.com" not in host:
                print(f"   [Bot-{index}] [Consent] ✅ 동의 페이지 아님, 계속 진행")
                return True
            
            read_time = random.uniform(CONSENT_READ_TIME_MIN, CONSENT_READ_TIME_MAX)
            print(f"   [Bot-{index}] [Consent] 📖 동의 페이지 읽는 중... ({read_time:.1f}초)")
            time.sleep(read_time)
            
            # 다양한 버튼 셀렉터 시도
            button_selectors = [
                "form[action='https://consent.youtube.com/save'] button[jsname='b3VHJd']",
                "button[aria-label*='Accept all']",
                "button[aria-label*='모두 수락']",
                "button:has-text('Accept all')",
                "button:has-text('모두 수락')",
                ".eom-buttons button:nth-child(2)",
            ]
            
            button_clicked = False
            
            for selector in button_selectors:
                try:
                    consent_button = page.locator(selector).first
                    
                    if consent_button.count() > 0:
                        print(f"   [Bot-{index}] [Consent] 🎯 버튼 발견: {selector[:50]}")
                        
                        consent_button.wait_for(state="visible", timeout=5000)
                        
                        box = consent_button.bounding_box()
                        if box:
                            page.mouse.move(
                                box['x'] + box['width'] / 2, 
                                box['y'] + box['height'] / 2,
                                steps=random.randint(5, 10)
                            )
                            time.sleep(random.uniform(0.3, 0.8))
                        
                        consent_button.click()
                        print(f"   [Bot-{index}] [Consent] ✅ 버튼 클릭 완료")
                        button_clicked = True
                        
                        time.sleep(3)
                        
                        new_url = page.url
                        if "consent.youtube.com" not in new_url:
                            print(f"   [Bot-{index}] [Consent] ✅ 동의 완료, 페이지 이동됨")
                            page.wait_for_load_state("networkidle", timeout=timeout)
                            return True
                        else:
                            print(f"   [Bot-{index}] [Consent] ⚠️ 클릭했으나 페이지 이동 안됨, 재시도...")
                            break
                            
                except Exception as e:
                    print(f"   [Bot-{index}] [Consent] ⚠️ 셀렉터 {selector[:30]} 실패: {e}")
                    continue
            
            if not button_clicked:
                print(f"   [Bot-{index}] [Consent] ⚠️ 버튼을 찾지 못함, 재시도...")
            
            retry_count += 1
            time.sleep(2)
            
        except Exception as e:
            print(f"   [Bot-{index}] [Consent] ⚠️ 처리 중 예외: {e}")
            retry_count += 1
            time.sleep(2)
    
    print(f"   [Bot-{index}] [Consent] ❌ {max_retries}번 시도 후 실패")
    return False

def try_play_video(page, index):
    """
    ✅ 새로운 함수: 비디오 재생 시도 (여러 방법 사용)
    """
    print(f"   [Bot-{index}] 🎬 비디오 재생 시도 중...")
    
    # ✅ 중요: 비디오 요소가 로드될 때까지 대기
    try:
        print(f"   [Bot-{index}] ⏳ 비디오 요소 로딩 대기 중...")
        page.wait_for_selector('video', timeout=30000, state='attached')
        print(f"   [Bot-{index}] ✅ 비디오 요소 발견")
        time.sleep(3)  # 추가 안정화 시간
    except Exception as e:
        print(f"   [Bot-{index}] ❌ 비디오 요소 로딩 실패: {e}")
        return False
    
    # 방법 1: 화면 클릭
    try:
        v_size = page.viewport_size
        if v_size:
            click_x = v_size['width'] // 2
            click_y = v_size['height'] // 2 + 100
            page.mouse.move(click_x, click_y, steps=random.randint(5, 10))
            time.sleep(random.uniform(0.5, 1.0))
            page.mouse.click(click_x, click_y)
            print(f"   [Bot-{index}] 🖱️ 화면 클릭 완료")
            time.sleep(2)
    except Exception as e:
        print(f"   [Bot-{index}] ⚠️ 화면 클릭 실패: {e}")
    
    # 방법 2: 스페이스바
    try:
        page.keyboard.press(" ")
        print(f"   [Bot-{index}] ␣ 스페이스바 재생 시도")
        time.sleep(2)
    except Exception as e:
        print(f"   [Bot-{index}] ⚠️ 스페이스바 실패: {e}")
    
    # 방법 3: JavaScript play()
    try:
        play_result = page.evaluate("""() => {
            const videos = document.querySelectorAll('video');
            if (videos.length > 0) {
                const video = videos[0];
                return video.play()
                    .then(() => ({success: true, time: video.currentTime, paused: video.paused}))
                    .catch(e => ({success: false, error: e.message}));
            }
            return {success: false, error: 'No video found'};
        }""")
        
        if play_result and play_result.get('success'):
            print(f"   [Bot-{index}] ▶️ JavaScript 재생 성공")
        else:
            print(f"   [Bot-{index}] ⚠️ JavaScript 재생 실패: {play_result.get('error', '알 수 없음')}")
        
        time.sleep(2)
    except Exception as e:
        print(f"   [Bot-{index}] ⚠️ JavaScript 재생 오류: {e}")
    
    # 방법 4: 재생 상태 확인
    try:
        status = page.evaluate("""() => {
            const v = document.querySelector('video');
            if (v) {
                if (v.paused) {
                    v.play().catch(e => console.error('Play failed:', e));
                }
                return {
                    currentTime: v.currentTime,
                    paused: v.paused,
                    duration: v.duration,
                    readyState: v.readyState
                };
            }
            return null;
        }""")
        
        if status:
            is_playing = not status['paused'] and status['currentTime'] > 0
            print(f"   [Bot-{index}] 📊 재생 상태: {'▶️재생중' if is_playing else '⏸️정지'} " +
                  f"(시간: {status['currentTime']:.1f}/{status['duration']:.1f}초)")
            return is_playing
        else:
            print(f"   [Bot-{index}] ⚠️ 비디오 요소를 찾을 수 없음")
            return False
            
    except Exception as e:
        print(f"   [Bot-{index}] ⚠️ 재생 상태 확인 실패: {e}")
        return False
    
def simulate_mobile_behavior(page, is_near_end=False, search_keyword=None):
    """
    ✅ 개선: 자연스러운 모바일 행동 시뮬레이션
    - is_near_end: True일 경우 시청 종료 직전 특별 행동 실행
    - search_keyword: 검색할 키워드 (None이면 랜덤 선택)
    """
    try:
        viewport = page.viewport_size
        if not viewport:
            return False
        
        # 시청 종료 직전 특별 행동 (예: 종료 1분 전)
        if is_near_end:
            print("   [Behavior] 🏁 시청 종료 직전 - 탐색 모드 시작")
            
            # ✅ 수정: 검색 키워드 선택 (파라미터가 없으면 랜덤 선택)
            if search_keyword is None:
                search_keyword = random.choice(SEARCH_KEYWORDS)
            print(f"   [Behavior] 🔍 선택된 검색어: '{search_keyword}'")
            
            # 1. 홈 버튼 찾아서 클릭
            try:
                print("   [Behavior] 🔍 홈 버튼 찾는 중...")
                
                # 홈 버튼 선택자들 (제공된 HTML 구조 기반)
                home_button_selectors = [
                    # 제공된 HTML 구조에 맞는 선택자
                    "button[role='link'][aria-label*='YouTube 홈']",
                    "button[role='link'][aria-label*='YouTube Home']",
                    "button.logo-in-player-endpoint",
                    "button[key='logo']",
                    "c3-icon#home-icon",
                    "#home-icon",
                    "button:has(c3-icon#home-icon)",
                    
                    # 일반적인 YouTube 홈 버튼 선택자
                    "a#logo",
                    "ytd-topbar-logo-renderer a",
                    "ytd-masthead a",
                    "[href='/'][aria-label*='YouTube']",
                    "button[aria-label*='홈']",
                    "button[aria-label*='Home']",
                    
                    # 위치 기반 선택 (왼쪽 상단)
                    "button:left-of(:text('YouTube'))",
                    ":near(:text('YouTube'), 50) button",
                ]
                
                home_button_clicked = False
                
                for selector in home_button_selectors:
                    try:
                        home_button = page.locator(selector).first
                        if home_button.count() > 0:
                            print(f"   [Behavior] 🏠 홈 버튼 발견: {selector}")
                            
                            # 버튼 정보 확인
                            box = home_button.bounding_box()
                            if box:
                                print(f"   [Behavior] 📍 홈 버튼 위치: ({box['x']:.0f}, {box['y']:.0f})")
                            
                            # 자연스러운 마우스 이동
                            if box:
                                page.mouse.move(
                                    box['x'] + box['width']/2,
                                    box['y'] + box['height']/2,
                                    steps=random.randint(8, 12)
                                )
                                time.sleep(random.uniform(0.3, 0.7))
                            
                            # 클릭
                            home_button.click()
                            print("   [Behavior] ✅ 홈 버튼 클릭 완료")
                            home_button_clicked = True
                            
                            # 홈 페이지 로딩 대기
                            wait_time = random.uniform(2, 4)
                            print(f"   [Behavior] ⏳ 홈 페이지 로딩 대기 ({wait_time:.1f}초)")
                            time.sleep(wait_time)
                            break
                            
                    except Exception as e:
                        print(f"   [Behavior] ⚠️ 홈 버튼 {selector} 실패: {e}")
                        continue
                
                # 홈 버튼을 찾지 못한 경우
                if not home_button_clicked:
                    print("   [Behavior] ⚠️ 홈 버튼을 찾지 못함, 대체 방법 시도")
                    
                    # 대체 방법 1: 왼쪽 상단의 첫 번째 버튼 클릭
                    try:
                        # 왼쪽 상단 영역의 버튼 찾기
                        top_left_buttons = page.locator("button, a").filter(
                            lambda el: el.bounding_box()['x'] < 200 and el.bounding_box()['y'] < 100
                        )
                        
                        if top_left_buttons.count() > 0:
                            top_left_buttons.first.click()
                            print("   [Behavior] 🔘 왼쪽 상단 첫 번째 버튼 클릭")
                            home_button_clicked = True
                            time.sleep(random.uniform(2, 3))
                    except:
                        pass
                    
                    # 대체 방법 2: 키보드 단축키
                    if not home_button_clicked:
                        try:
                            page.keyboard.press("Shift+H")  # YouTube 홈 단축키
                            print("   [Behavior] ⌨️ Shift+H 단축키로 홈 이동")
                            home_button_clicked = True
                            time.sleep(random.uniform(2, 3))
                        except:
                            pass
                    
                    # 대체 방법 3: 직접 URL 이동 (최후의 수단)
                    if not home_button_clicked:
                        try:
                            current_url = page.url
                            if "youtube.com" in current_url:
                                page.goto("https://www.youtube.com/", wait_until="domcontentloaded")
                                print("   [Behavior] 🌐 YouTube 홈으로 직접 이동")
                                home_button_clicked = True
                                time.sleep(random.uniform(2, 3))
                        except:
                            pass
                
                if home_button_clicked:
                    print("   [Behavior] ✅ 홈 이동 완료")
                else:
                    print("   [Behavior] ⚠️ 홈 이동 실패, 계속 진행")
                    
            except Exception as e:
                print(f"   [Behavior] ⚠️ 홈 이동 과정 실패: {e}")
            
            # 2. 검색창 찾기 및 검색
            try:
                print("   [Behavior] 🔍 검색창 찾는 중...")
                
                # 검색 버튼 클릭
                search_button_selectors = [
                    "button[aria-label='Search YouTube']",
                    "button.icon-button.topbar-menu-button-avatar-button",
                    "button[aria-label*='Search'][aria-label*='YouTube']",
                ]
                
                search_button_clicked = False
                for selector in search_button_selectors:
                    try:
                        search_button = page.locator(selector).first
                        if search_button.count() > 0:
                            search_button.click()
                            print(f"   [Behavior] ✅ 검색 버튼 클릭: {selector}")
                            search_button_clicked = True
                            time.sleep(random.uniform(1, 2))
                            break
                    except:
                        continue
                
                # 검색창 찾기
                search_box = None
                search_selectors = [
                    "input#search",
                    "#search-input input",
                    "ytd-searchbox input",
                    "input[type='search']",
                    "input[name='search_query']",
                ]
                
                for selector in search_selectors:
                    try:
                        search_box = page.locator(selector).first
                        if search_box.count() > 0:
                            print(f"   [Behavior] 🔍 검색창 발견: {selector}")
                            break
                    except:
                        continue
                
                if search_box and search_box.count() > 0:
                    # 검색창 클릭
                    try:
                        search_box.click()
                        time.sleep(random.uniform(0.5, 1.0))
                    except:
                        pass
                    
                    # 검색어 입력
                    print(f"   [Behavior] ⌨️ '{search_keyword}' 입력 중...")
                    
                    try:
                        # 기존 내용 지우기
                        search_box.fill("")
                        time.sleep(0.3)
                        
                        # 타이핑
                        search_box.type(search_keyword, delay=random.uniform(50, 100))
                        print("   [Behavior] ✅ 검색어 입력 완료")
                        
                        # 엔터 키
                        time.sleep(random.uniform(0.3, 0.6))
                        page.keyboard.press("Enter")
                        print("   [Behavior] ↵ 검색 실행")
                        
                        # 검색 결과 로딩 대기 (대기시간 연장)
                        wait_time = random.uniform(5, 8)  # 3-6초에서 5-8초로 연장
                        print(f"   [Behavior] ⏳ 검색 결과 로딩 대기 ({wait_time:.1f}초)")
                        time.sleep(wait_time)
                        
                        # 3. 검색 결과 클릭
                        try:
                            print("   [Behavior] 🔍 검색 결과 찾는 중...")
                            
                            # YouTube 검색 결과 페이지의 다양한 구조 시도
                            result_methods = [
                                lambda: page.locator("ytd-video-renderer"),
                                lambda: page.locator("a#video-title"),
                                lambda: page.locator("a#thumbnail"),
                                lambda: page.locator("a[href*='/watch?v=']"),
                                lambda: page.locator("#contents ytd-video-renderer"),
                                lambda: page.locator("ytd-video-renderer, ytd-rich-item-renderer, ytd-playlist-renderer"),
                            ]
                            
                            video_results = None
                            best_count = 0
                            best_method = None
                            
                            # 모든 방법 시도하고 가장 많은 결과를 가진 방법 선택
                            for method_idx, method in enumerate(result_methods):
                                try:
                                    results = method()
                                    count = results.count()
                                    if count > best_count:
                                        best_count = count
                                        video_results = results
                                        best_method = method_idx
                                        print(f"   [Behavior] 🔍 방법 {method_idx+1}: {count}개 결과 발견")
                                except Exception as e:
                                    print(f"   [Behavior] ⚠️ 방법 {method_idx+1} 실패: {e}")
                            
                            if video_results and best_count > 0:
                                print(f"   [Behavior] 🎬 최종: 방법 {best_method+1} 선택 ({best_count}개 결과)")
                                
                                # 클릭할 개수 결정
                                available = min(10, best_count)
                                click_count = min(random.randint(1, 3), available)
                                
                                if click_count > 0:
                                    print(f"   [Behavior] 🎯 검색 결과 {click_count}개 클릭 예정")
                                    
                                    # 랜덤 인덱스 선택 (앞쪽 결과 위주)
                                    indices = random.sample(range(min(8, available)), click_count)
                                    
                                    for i, idx in enumerate(indices):
                                        try:
                                            result = video_results.nth(idx)
                                            if result.count() > 0:
                                                # 클릭
                                                result.click()
                                                print(f"   [Behavior] 👆 검색 결과 {idx+1}번 클릭 ({i+1}/{click_count})")
                                                
                                                # 짧은 시청 (대기시간 연장)
                                                watch_time = random.uniform(8, 15)
                                                print(f"   [Behavior] ⏱️ 짧은 시청 ({watch_time:.1f}초)")
                                                time.sleep(watch_time)
                                                
                                                # 마지막이 아니면 뒤로 가기 (대기시간 연장)
                                                if i < len(indices) - 1:
                                                    page.go_back()
                                                    wait_time = random.uniform(3, 5)  # 2-3초에서 3-5초로 연장
                                                    print(f"   [Behavior] ↩️ 뒤로 가기 ({wait_time:.1f}초 대기)")
                                                    time.sleep(wait_time)
                                        except Exception as e:
                                            print(f"   [Behavior] ⚠️ 결과 {idx} 클릭 실패: {e}")
                                            continue
                                    
                                    print("   [Behavior] ✅ 검색 결과 클릭 완료")
                                else:
                                    print("   [Behavior] ⚠️ 클릭할 결과 없음")
                            else:
                                print("   [Behavior] ⚠️ 검색 결과를 찾을 수 없음")
                                
                        except Exception as e:
                            print(f"   [Behavior] ⚠️ 검색 결과 처리 실패: {e}")
                    except Exception as e:
                        print(f"   [Behavior] ⚠️ 검색 입력 실패: {e}")
                        
                else:
                    print("   [Behavior] ⚠️ 검색창을 찾을 수 없음")
                    
            except Exception as e:
                print(f"   [Behavior] ⚠️ 검색 과정 실패: {e}")
            
            print("   [Behavior] 🏁 탐색 모드 종료")
            
            # ✅ 수정: 특별 행동 후 3초 대기
            print("   [Behavior] ⏳ 특별 행동 완료 후 3초 대기")
            time.sleep(3)
            
            # ✅ 수정: 특별 행동 후 브라우저 종료를 위한 플래그 반환
            return True  # True 반환하여 monitor_service에서 종료하도록 신호
        
        # 일반 행동
        scroll_count = random.randint(1, 3)
        for _ in range(scroll_count):
            scroll_amount = random.randint(50, 200)
            page.evaluate(f"window.scrollBy({{top: {scroll_amount}, behavior: 'smooth'}})")
            time.sleep(random.uniform(0.5, 1.5))
        
        if random.random() > 0.6:
            x = random.randint(100, viewport['width'] - 100)
            y = random.randint(100, viewport['height'] - 100)
            
            page.mouse.move(x, y, steps=random.randint(3, 7))
            time.sleep(random.uniform(0.2, 0.5))
            
            page.mouse.click(x, y)
            time.sleep(random.uniform(0.5, 1.0))
        
        # 일반 행동은 False 반환 (종료 안함)
        return False
            
    except Exception as e:
        print(f"   [Behavior] ⚠️ 행동 시뮬레이션 오류: {e}")
        return False

def detect_bot_suspicion_by_link(page):
    """지정된 봇 확인 링크가 있는지 검사"""
    try:
        # 감지할 링크 패턴들
        target_link_patterns = [
            "https://support.google.com/youtube/answer/3037019",
            "/answer/3037019",
            "3037019",
            "#zippy=%2Ccheck-that-youre-signed-into-youtube",
            "answer/3037019#zippy"
        ]
        
        # 페이지의 모든 링크 검사
        all_links = page.locator("a[href]")
        link_count = all_links.count()
        
        print(f"   [Link Check] 페이지 내 링크 수: {link_count}")
        
        # 모든 링크 순회 (성능을 위해 최대 100개만)
        for i in range(min(link_count, 100)):
            try:
                href = all_links.nth(i).get_attribute("href")
                if href:
                    href_lower = href.lower()
                    
                    # 각 패턴과 비교
                    for pattern in target_link_patterns:
                        if pattern in href_lower:
                            print(f"   [Link Check] ✅ 발견: {href[:100]}...")
                            print(f"   [Link Check] ✅ 패턴 매칭: {pattern}")
                            return True
            except:
                continue
        
        print(f"   [Link Check] ❌ 타겟 링크 없음")
        return False
        
    except Exception as e:
        print(f"   [Link Check] ⚠️ 오류: {e}")
        return False
    
# ===================== 3. 메인 워커 (개선됨) =====================

def monitor_service(url, proxy_url, index, stop_event, r):
    """
    ✅ 안정적인 워커 함수 - 재생 문제 해결 + CanvasBlocker 적용
    """
    success = False
    region_key = random.choice(list(REGION_PROFILES.keys())) if REGION_PROFILES else "US"
    config = REGION_PROFILES.get(region_key, {"locale": "en-US", "timezone": "America/New_York"})
    referer = random.choice(REFERERS)
    device_name = random.choice(MOBILE_DEVICES_LIST)
    
    # ✅ 추가: 각 봇별로 랜덤 검색 키워드 선택
    search_keyword = random.choice(SEARCH_KEYWORDS)

    print(f"[Bot-{index}] 🚀 시작")
    print(f"   📱 Device: {device_name}")
    print(f"   🌍 Region: {region_key} ({config.get('locale')})")
    print(f"   🔗 Proxy: {proxy_url}")
    print(f"   🔗 Referer: {referer}")
    print(f"   🔍 Search Keyword: '{search_keyword}'")

    # ✅ 추가: 특별 행동 관련 변수
    special_behavior_done = False
    should_close_after_special_behavior = False

    playwright_mgr = None
    browser = None
    
    try:
        if stop_event.is_set():
            return
        
        playwright_mgr = sync_playwright().start()
        device_info = dict(playwright_mgr.devices[device_name])
        device_agent = device_info.pop('user_agent', None)
        
        pos = calculate_window_position(index)
        
        # ✅ CanvasBlocker 확장 프로그램 로드 - 수정된 방식
        browser_args = [
            f"--window-position={pos['x']},{pos['y']}",
            f"--window-size={pos['width']},{pos['height']}",
            "--disable-blink-features=AutomationControlled",
            "--autoplay-policy=no-user-gesture-required",
            "--disable-features=IsolateOrigins,site-per-process",
            "--disable-site-isolation-trials",
        ]
        
        # ✅ 개선: 확장 프로그램 로드 전 경로 검증
        extension_loaded = False
        if os.path.exists(CANVASBLOCKER_PATH):
            # 절대 경로로 변환 (한글 경로 처리)
            abs_extension_path = os.path.abspath(CANVASBLOCKER_PATH)
            
            # Windows 경로 구분자 변환 (Chromium 호환)
            abs_extension_path = abs_extension_path.replace('\\', '/')
            
            manifest_path = os.path.join(CANVASBLOCKER_PATH, 'manifest.json')
            
            if os.path.exists(manifest_path):
                try:
                    # manifest.json 유효성 검사
                    with open(manifest_path, 'r', encoding='utf-8') as f:
                        manifest = json.load(f)
                        ext_name = manifest.get('name', 'Unknown')
                        ext_version = manifest.get('version', '?')
                        manifest_version = manifest.get('manifest_version', 2)
                        
                    print(f"   [Bot-{index}] 📦 확장 프로그램 발견:")
                    print(f"   [Bot-{index}]    - 이름: {ext_name}")
                    print(f"   [Bot-{index}]    - 버전: {ext_version}")
                    print(f"   [Bot-{index}]    - Manifest: v{manifest_version}")
                    print(f"   [Bot-{index}]    - 경로: {abs_extension_path}")
                    
                    # Playwright는 Manifest V2만 지원
                    if manifest_version == 3:
                        print(f"   [Bot-{index}] ⚠️ 경고: Manifest V3는 Playwright에서 불안정할 수 있음")
                        print(f"   [Bot-{index}]    - Manifest V2 버전 사용을 권장합니다")
                        print(f"   [Bot-{index}]    - 대안: 수동 Canvas 노이즈가 자동 적용됩니다")
                        # V3는 로드 시도하지만 작동 안 할 가능성 높음
                    
                    # 확장 프로그램 로드
                    browser_args.extend([
                        f"--disable-extensions-except={abs_extension_path}",
                        f"--load-extension={abs_extension_path}",
                    ])
                    extension_loaded = True
                    
                    if manifest_version == 2:
                        print(f"   [Bot-{index}] ✅ 확장 프로그램 로드 설정 완료 (V2 - 호환)")
                    else:
                        print(f"   [Bot-{index}] ⚠️ 확장 프로그램 로드 설정 완료 (V3 - 비호환 가능)")
                    
                except json.JSONDecodeError as e:
                    print(f"   [Bot-{index}] ❌ manifest.json 파싱 오류: {e}")
                except Exception as e:
                    print(f"   [Bot-{index}] ❌ 확장 프로그램 로드 실패: {e}")
            else:
                print(f"   [Bot-{index}] ❌ manifest.json 없음: {manifest_path}")
        else:
            print(f"   [Bot-{index}] ⚠️ CanvasBlocker 경로 없음: {CANVASBLOCKER_PATH}")
        
        # ✅ 브라우저 실행 옵션 수정 - user-data-dir 추가
        launch_options = {
            "headless": False,
            "args": browser_args,
            "timeout": BROWSER_LAUNCH_TIMEOUT
        }
        
        # proxy 설정
        if proxy_url:
            launch_options["proxy"] = {"server": proxy_url}
        
        # ✅ 확장 프로그램을 위한 persistent context 사용 (선택사항)
        # 일반 브라우저처럼 확장 프로그램이 제대로 작동하도록
        if extension_loaded:
            # chromium.launch_persistent_context를 사용하면 확장 프로그램이 더 잘 작동
            # 하지만 여기서는 일단 기본 launch 유지
            pass
        
        browser = playwright_mgr.chromium.launch(**launch_options)

        # ✅ 안정적인 컨텍스트 설정
        context = browser.new_context(
            **device_info,
            user_agent=device_agent,
            locale=config['locale'],
            timezone_id=config['timezone'],
            permissions=['camera', 'microphone'],
            extra_http_headers={
                "Accept-Language": config.get('locale', 'en-US'),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Connection": "keep-alive",
            }
        )
        
        context.set_default_timeout(CONTEXT_TIMEOUT)
        page = context.new_page()
        
        # ✅ 확장 프로그램 로딩 대기
        if extension_loaded:
            print(f"   [Bot-{index}] ⏳ 확장 프로그램 초기화 대기 중...")
            time.sleep(3)  # 확장 프로그램이 완전히 로드될 때까지 대기
            
            # 확장 프로그램 페이지 확인
            try:
                all_pages = context.pages
                print(f"   [Bot-{index}] 📄 현재 페이지 수: {len(all_pages)}")
                for idx, p in enumerate(all_pages):
                    url = p.url
                    if 'chrome-extension://' in url:
                        print(f"   [Bot-{index}]    - 페이지 {idx}: 확장 프로그램 감지 ✅")
                    else:
                        print(f"   [Bot-{index}]    - 페이지 {idx}: {url[:50]}")
            except Exception as e:
                print(f"   [Bot-{index}] ⚠️ 페이지 확인 실패: {e}")
        
        # ✅ 스텔스 적용
        apply_ultimate_stealth(page, config, device_name)
        
        # ✅ CanvasBlocker 사전 테스트 (간단한 HTML로)
        if os.path.exists(CANVASBLOCKER_PATH):
            try:
                print(f"   [Bot-{index}] 🧪 CanvasBlocker 사전 테스트 시작...")
                
                # 간단한 테스트 HTML
                test_html = """
                <!DOCTYPE html>
                <html>
                <head><meta charset="UTF-8"><title>Canvas Test</title></head>
                <body>
                    <h1>Canvas Fingerprint Test</h1>
                    <canvas id="testCanvas" width="200" height="200"></canvas>
                </body>
                </html>
                """
                
                # 테스트 페이지 로드
                page.goto(f"data:text/html,{test_html}")
                time.sleep(2)  # Canvas 초기화 대기
                
                # Canvas 노이즈 테스트 - 개선된 버전
                canvas_test_result = page.evaluate("""() => {
                    const results = [];
                    const dataURLs = [];
                    
                    // 테스트: 동일한 작업을 3번 수행하고 toDataURL 비교
                    for (let i = 0; i < 3; i++) {
                        const canvas = document.createElement('canvas');
                        canvas.width = 100;
                        canvas.height = 100;
                        const ctx = canvas.getContext('2d');
                        
                        // 복잡한 패턴 그리기
                        ctx.fillStyle = 'rgb(255, 0, 0)';
                        ctx.fillRect(0, 0, 100, 100);
                        
                        ctx.fillStyle = 'rgb(0, 255, 0)';
                        ctx.fillRect(20, 20, 60, 60);
                        
                        ctx.fillStyle = 'rgb(0, 0, 255)';
                        ctx.beginPath();
                        ctx.arc(50, 50, 30, 0, Math.PI * 2);
                        ctx.fill();
                        
                        // toDataURL로 비교 (가장 확실한 방법)
                        const dataURL = canvas.toDataURL();
                        dataURLs.push(dataURL);
                        
                        // 추가: getImageData로도 체크
                        const imageData = ctx.getImageData(0, 0, 100, 100);
                        let checksum = 0;
                        for (let j = 0; j < Math.min(1000, imageData.data.length); j++) {
                            checksum = (checksum + imageData.data[j]) % 1000000;
                        }
                        results.push(checksum);
                    }
                    
                    // 결과 분석
                    const allChecksumsSame = results.every(r => r === results[0]);
                    const allDataURLsSame = dataURLs.every(d => d === dataURLs[0]);
                    
                    return {
                        checksums: results,
                        allChecksumsSame: allChecksumsSame,
                        allDataURLsSame: allDataURLsSame,
                        dataURLLengths: dataURLs.map(d => d.length),
                        // 디버그: 처음 50자 비교
                        dataURLSamples: dataURLs.map(d => d.substring(0, 50))
                    };
                }""")
                
                # 결과 분석 - toDataURL이 다르거나 checksum이 다르면 작동
                is_working = (not canvas_test_result['allDataURLsSame']) or (not canvas_test_result['allChecksumsSame'])
                
                if is_working:
                    print(f"   [Bot-{index}] ✅ CanvasBlocker 작동 확인!")
                    print(f"   [Bot-{index}]    - Checksums: {canvas_test_result['checksums']}")
                    print(f"   [Bot-{index}]    - DataURL 동일? {canvas_test_result['allDataURLsSame']}")
                    if not canvas_test_result['allDataURLsSame']:
                        print(f"   [Bot-{index}]    - 노이즈 감지: 매번 다른 이미지 생성됨 ✅")
                else:
                    print(f"   [Bot-{index}] ⚠️ CanvasBlocker 미작동!")
                    print(f"   [Bot-{index}]    - Checksums: {canvas_test_result['checksums']}")
                    print(f"   [Bot-{index}]    - DataURL 길이: {canvas_test_result['dataURLLengths']}")
                    
                    # 0인 경우 추가 디버깅
                    if all(c == 0 for c in canvas_test_result['checksums']):
                        print(f"   [Bot-{index}]    - ⚠️ Canvas가 비어있음 - 렌더링 문제 가능성")
                        print(f"   [Bot-{index}]    - DataURL 샘플: {canvas_test_result['dataURLSamples'][0][:30]}...")
                    else:
                        print(f"   [Bot-{index}]    - ⚠️ 경고: Canvas fingerprinting에 취약")
                        print(f"   [Bot-{index}]    - 💡 대안: 수동 Canvas 노이즈 적용 중...")
                        
                        # ✅ CanvasBlocker가 작동하지 않으면 수동으로 Canvas 보호 적용
                        page.add_init_script("""
                            const originalGetImageData = CanvasRenderingContext2D.prototype.getImageData;
                            CanvasRenderingContext2D.prototype.getImageData = function(...args) {
                                const result = originalGetImageData.apply(this, args);
                                // 무작위 노이즈 추가
                                for (let i = 0; i < result.data.length; i += 4) {
                                    const noise = Math.floor(Math.random() * 3) - 1;
                                    result.data[i] = Math.max(0, Math.min(255, result.data[i] + noise));
                                    result.data[i + 1] = Math.max(0, Math.min(255, result.data[i + 1] + noise));
                                    result.data[i + 2] = Math.max(0, Math.min(255, result.data[i + 2] + noise));
                                }
                                return result;
                            };
                            
                            const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
                            HTMLCanvasElement.prototype.toDataURL = function(...args) {
                                const ctx = this.getContext('2d');
                                if (ctx) {
                                    const imageData = ctx.getImageData(0, 0, this.width, this.height);
                                    for (let i = 0; i < imageData.data.length; i += 4) {
                                        const noise = Math.floor(Math.random() * 3) - 1;
                                        imageData.data[i] += noise;
                                        imageData.data[i + 1] += noise;
                                        imageData.data[i + 2] += noise;
                                    }
                                    ctx.putImageData(imageData, 0, 0);
                                }
                                return originalToDataURL.apply(this, args);
                            };
                            console.log('✅ 수동 Canvas 보호 활성화');
                        """)
                        print(f"   [Bot-{index}]    - ✅ 수동 Canvas 노이즈 스크립트 적용 완료")
                
            except Exception as e:
                print(f"   [Bot-{index}] ⚠️ CanvasBlocker 테스트 실패: {e}")
                import traceback
                print(f"   [Bot-{index}]    - 상세: {traceback.format_exc()[:200]}")
                print(f"   [Bot-{index}]    - YouTube 접속은 계속 진행됩니다")

        # ✅ 페이지 로딩
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
                
                # ✅ 봇 의심 페이지 체크 추가
                if detect_bot_suspicion_by_link(page):
                    print(f"   [Bot-{index}] 🚨 봇 의심 페이지 감지! 브라우저 종료")
                    success = False
                    browser.close()
                    playwright_mgr.stop()
                    return  # 함수 종료
                
                # Shorts 페이지 대기
                page.wait_for_selector('video, ytd-player, #shorts-player', timeout=30000)
                page_loaded = True
                print(f"   [Bot-{index}] ✅ 페이지 로딩 완료")
                
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
        
        # ✅ Consent 처리 (수정됨)
        time.sleep(random.uniform(5, 10))
        consent_success = handle_youtube_consent(page, index)
        if not consent_success:
            print(f"   [Bot-{index}] ❌ Consent 처리 실패, 브라우저 종료")
            raise Exception("Consent 처리 실패")

        # ✅ 추가: Shorts 페이지 완전 로딩 대기
        print(f"   [Bot-{index}] ⏳ Shorts 페이지 로딩 대기 중...")
        try:
            # video 요소와 shorts-player 둘 다 확인
            page.wait_for_selector('video, ytd-player, #shorts-player', timeout=30000, state='visible')
            print(f"   [Bot-{index}] ✅ Shorts 페이지 로딩 완료")
        except Exception as e:
            print(f"   [Bot-{index}] ⚠️ Shorts 로딩 타임아웃: {e}")

        # 초기화 대기 (더 길게)
        wait_time = random.uniform(5, 8)
        print(f"   [Bot-{index}] ⏳ 안정화 대기 중... ({wait_time:.1f}초)")
        time.sleep(wait_time)

        # ✅ 재생 트리거 (수정됨)
        print(f"   [Bot-{index}] 🎬 재생 시작 시도...")
        is_playing = try_play_video(page, index)

        if not is_playing:
            print(f"   [Bot-{index}] ⚠️ 재생 시작 실패, 그래도 시청 시도...")

        # 재생 확인 대기
        time.sleep(random.uniform(3, 5))

        
        # ✅ 시청 로직
        watch_duration = random.uniform(VIDEO_WATCH_MIN, VIDEO_WATCH_MAX)
        TIME_BEFORE_END_FOR_SPECIAL_BEHAVIOR = watch_duration - 40
        
        elapsed = 0
        last_v_time = 0
        consecutive_errors = 0
        behavior_interval = random.randint(20, 40)
        
        print(f"   [Bot-{index}] 🎬 시청 시작 (목표: {watch_duration:.0f}초, 스페셜동작 : {TIME_BEFORE_END_FOR_SPECIAL_BEHAVIOR}초 전)")
        
        while elapsed < watch_duration and not stop_event.is_set():
            time.sleep(VIDEO_CHECK_INTERVAL)
            elapsed += VIDEO_CHECK_INTERVAL
            
            try:
                status = page.evaluate("""() => {
                    const v = document.querySelector('video');
                    if (v) {
                        return {
                            t: v.currentTime,
                            p: v.paused,
                            duration: v.duration,
                            ready: v.readyState
                        };
                    }
                    return null;
                }""")
                
                if status:
                    is_playing = not status['p'] and status['t'] > last_v_time
                    icon = "▶️" if is_playing else "⏸️"
                    print(f"   [Bot-{index}] {icon} {elapsed:.0f}/{watch_duration:.0f}초 (영상:{status['t']:.1f}초)")
                    last_v_time = status['t']
                    consecutive_errors = 0
                    
                    # 재생되지 않으면 간단히 재시도
                    if status['p'] and elapsed < watch_duration * 0.8:
                        print(f"   [Bot-{index}] ⏯️ 일시정지 상태, 재생 재시도")
                        page.keyboard.press(" ")
                        time.sleep(1)
                    
                    # 특별 행동 실행
                    should_do_special = (
                        not special_behavior_done and 
                        (watch_duration - elapsed) <= TIME_BEFORE_END_FOR_SPECIAL_BEHAVIOR
                    )
                    
                    if should_do_special:
                        print(f"   [Bot-{index}] 🎯 시청 종료 {TIME_BEFORE_END_FOR_SPECIAL_BEHAVIOR}초 전 - 특별 행동 시작")
                        should_close_after_special_behavior = simulate_mobile_behavior(
                            page, is_near_end=True, search_keyword=search_keyword
                        )
                        special_behavior_done = True
                        
                        if should_close_after_special_behavior:
                            print(f"   [Bot-{index}] 🏁 특별 행동 완료 후 종료 예정")
                            break
                else:
                    consecutive_errors += 1
                    print(f"   [Bot-{index}] ⚠️ 영상 상태 없음 (에러: {consecutive_errors}/{MAX_VIDEO_CHECK_ERRORS})")
                
                if consecutive_errors >= MAX_VIDEO_CHECK_ERRORS:
                    print(f"   [Bot-{index}] 🛑 상태 체크 연속 실패 → 작업 종료")
                    break
                
                # 일반 행동
                if elapsed % behavior_interval == 0 and not special_behavior_done:
                    simulate_mobile_behavior(page, is_near_end=False)
                    behavior_interval = random.randint(20, 40)
                    
            except Exception as e:
                consecutive_errors += 1
                print(f"   [Bot-{index}] ⚠️ 상태 체크 오류: {e}")
                
                if consecutive_errors >= MAX_VIDEO_CHECK_ERRORS:
                    print(f"   [Bot-{index}] 🛑 상태 체크 연속 실패 → 작업 종료")
                    break
        
        # 성공 조건
        if (elapsed >= watch_duration and consecutive_errors < MAX_VIDEO_CHECK_ERRORS) or should_close_after_special_behavior:
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
        
        # Redis 처리
        if r and proxy_url:
            r.zrem(REDIS_ZSET_LEASE, proxy_url)
            
            if success:
                # ✅ SUCCESS_COOL_DOWN 사용하도록 수정
                if SUCCESS_COOL_DOWN > 0:
                    score = int(time.time()) + SUCCESS_COOL_DOWN
                    r.zadd(REDIS_ZSET_ALIVE, {proxy_url: score})
                    print(f"   [Bot-{index}] ✅ 프록시 반환 (성공, {SUCCESS_COOL_DOWN}초 쿨다운)")
                else:
                    r.zadd(REDIS_ZSET_ALIVE, {proxy_url: 0})
                    print(f"   [Bot-{index}] ✅ 프록시 반환 (성공, 즉시 재사용 가능)")
            else:
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

# get_redis() 함수 다음에 추가
def get_mobile_devices():
    """✅ Playwright에서 사용 가능한 모바일 디바이스 목록 가져오기"""
    try:
        with sync_playwright() as p:
            all_devices = list(p.devices.keys())
            # 모바일 디바이스만 필터링 (iPhone, Pixel, Galaxy 등)
            mobile_devices = [
                device for device in all_devices 
                if any(keyword in device for keyword in ['iPhone', 'Pixel', 'Galaxy', 'iPad'])
            ]
            
            if mobile_devices:
                print(f"✅ 모바일 디바이스 목록 로드 완료 ({len(mobile_devices)}개)")
                print(f"   예시: {', '.join(mobile_devices[:5])}")
                return mobile_devices
            else:
                print("⚠️ 모바일 디바이스를 찾지 못함, 기본 목록 사용")
                return ['Pixel 5', 'iPhone 12', 'iPhone 13']
    except Exception as e:
        print(f"⚠️ 디바이스 목록 로드 실패: {e}, 기본 목록 사용")
        return ['Pixel 5', 'iPhone 12', 'iPhone 13']
    
def main():
    global MOBILE_DEVICES_LIST
    r = get_redis()
    
    MOBILE_DEVICES_LIST = get_mobile_devices()
    
    active_slots = {}
    stop_event = threading.Event()

    def signal_handler(sig, frame):
        print("\n🛑 중단 요청... 모든 브라우저를 닫습니다.")
        stop_event.set()
        time.sleep(2)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    print("=" * 80)
    print("📡 Playwright YouTube Bot (CanvasBlocker 버전)")
    print("=" * 80)
    print(f"📱 슬롯: {NUM_BROWSERS}개")
    print(f"⏱️  시청 시간: {VIDEO_WATCH_MIN}-{VIDEO_WATCH_MAX}초")
    print(f"🔍 검색 키워드: {len(SEARCH_KEYWORDS)}개")
    print(f"🎬 특별 행동: 시청 종료 1분 전 홈->검색->랜덤 클릭")
    print(f"🎨 CanvasBlocker: {CANVASBLOCKER_PATH}")
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
                        proxy = r.eval(_LUA_CLAIM, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, int(time.time()), 600)
                        if proxy:
                            url = TARGET_URL if s % 2 == 0 else TARGET_URL1
                            #proxy = None
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