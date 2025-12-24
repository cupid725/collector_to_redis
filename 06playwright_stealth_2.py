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
VIDEO_WATCH_MIN = 180
VIDEO_WATCH_MAX = 300
TIME_BEFORE_END_FOR_SPECIAL_BEHAVIOR = 240  # 나중에 재생시간이 결정되면 그떄 다시 조정
   
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
    """강화된 스텔스 로직"""
    
    stealth_scripts = [
        # 1. 기본 WebDriver 마스킹
        """
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        window.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'languages', { get: () => ['%s', '%s'] });
        """ % (config.get('locale', 'en-US'), 'en-US'),
        
        # 2. Permissions 스푸핑
        """
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );
        """,
        
        # 3. 플러그인 스푸핑
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
        
        # 4. WebGL 스푸핑
        """
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) return 'Intel Inc.';
            if (parameter === 37446) return 'Intel Iris OpenGL Engine';
            return getParameter(parameter);
        };
        """,
        
        # 5. Canvas 방어
        """
        const originalGetImageData = CanvasRenderingContext2D.prototype.getImageData;
        CanvasRenderingContext2D.prototype.getImageData = function(...args) {
            const result = originalGetImageData.apply(this, args);
            for (let i = 0; i < result.data.length; i += 4) {
                result.data[i] += Math.floor(Math.random() * 2) - 1;
                result.data[i + 1] += Math.floor(Math.random() * 2) - 1;
                result.data[i + 2] += Math.floor(Math.random() * 2) - 1;
            }
            return result;
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

# ===================== 3. 메인 워커 (개선됨) =====================

def monitor_service(url, proxy_url, index, stop_event, r):
    """
    ✅ 안정적인 워커 함수 - 재생 문제 해결
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
        
        # ✅ 안정적인 브라우저 옵션
        browser = playwright_mgr.chromium.launch(
            headless=False,
            proxy={"server": proxy_url} if proxy_url else None,
            args=[
                f"--window-position={pos['x']},{pos['y']}",
                f"--window-size={pos['width']},{pos['height']}",
                "--disable-blink-features=AutomationControlled",
                "--autoplay-policy=no-user-gesture-required",
                "--disable-features=IsolateOrigins,site-per-process",  # YouTube 호환성
                "--disable-site-isolation-trials",
            ],
            timeout=BROWSER_LAUNCH_TIMEOUT
        )

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
        
        # ✅ 기본 스텔스만 적용 (YouTube 호환성)
        #apply_stealth_and_custom(page, config, device_name)
        apply_enhanced_stealth(page, config, device_name)

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
        
        # ✅ Consent 처리
        time.sleep(random.uniform(2, 4))
        handle_youtube_consent(page)
        
        # 초기화 대기
        time.sleep(random.uniform(3, 5))
        
        # ✅ 재생 트리거 (간단하고 안정적인 방법)
        print(f"   [Bot-{index}] 🎬 재생 시작 시도...")
        
        # 1. 비디오 요소 확인
        try:
            video_count = page.evaluate("""() => {
                return document.querySelectorAll('video').length;
            }""")
            print(f"   [Bot-{index}] 📊 비디오 요소 개수: {video_count}")
        except:
            pass
        
        # 2. 간단한 클릭으로 재생 시도
        v_size = page.viewport_size
        if v_size:
            # 약간 아래쪽 클릭 (Shorts는 중앙보다 아래쪽에서 재생됨)
            click_x = v_size['width'] // 2
            click_y = v_size['height'] // 2 + 100
            
            page.mouse.move(click_x, click_y, steps=random.randint(5, 10))
            time.sleep(random.uniform(0.5, 1.0))
            page.mouse.click(click_x, click_y)
            print(f"   [Bot-{index}] 🖱️ 화면 클릭 ({click_x}, {click_y})")
        
        # 3. 키보드 스페이스바로 재생 시도
        time.sleep(1)
        page.keyboard.press(" ")
        print(f"   [Bot-{index}] ␣ 스페이스바 재생 시도")
        
        # 4. JavaScript로 직접 재생 시도
        time.sleep(1)
        try:
            play_result = page.evaluate("""() => {
                const videos = document.querySelectorAll('video');
                if (videos.length > 0) {
                    const video = videos[0];
                    return video.play()
                        .then(() => ({success: true, time: video.currentTime}))
                        .catch(e => ({success: false, error: e.message}));
                }
                return {success: false, error: 'No video found'};
            }""")
            
            if play_result and play_result.get('success'):
                print(f"   [Bot-{index}] ▶️ JavaScript 재생 성공")
            else:
                print(f"   [Bot-{index}] ⚠️ JavaScript 재생 실패: {play_result.get('error', '알 수 없음')}")
        except Exception as e:
            print(f"   [Bot-{index}] ⚠️ JavaScript 재생 오류: {e}")
        
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
                r.zadd(REDIS_ZSET_ALIVE, {proxy_url: 0})
                print(f"   [Bot-{index}] ✅ 프록시 반환 (성공)")
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
    print("📡 Playwright YouTube Bot (안정화 버전)")
    print("=" * 80)
    print(f"📱 슬롯: {NUM_BROWSERS}개")
    print(f"⏱️  시청 시간: {VIDEO_WATCH_MIN}-{VIDEO_WATCH_MAX}초")
    print(f"🔍 검색 키워드: {len(SEARCH_KEYWORDS)}개")
    print(f"🎬 특별 행동: 시청 종료 1분 전 홈->검색->랜덤 클릭")
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