import random
import threading
import time
import json
import redis
import os
import sys
import signal
from urllib.parse import urlparse
import config # 설정 파일 임포트
from MobileHumanEvent import MobileHumanEvent
import re
# stealth_browser.py에서 클래스 임포트
from stealth_browser import StealthMobileBrowser

# ===================== 1. 설정 및 데이터 로드 =====================

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

SEARCH_KEYWORDS = [
    "mr redpanda", "funny cat videos", "music 2024", "cooking tutorial",
    "travel vlog", "gaming highlights", "workout routine", "tech review"
]

TARGET_URL = "https://www.youtube.com/shorts/eto2wO2i0iA?feature=share"
TARGET_URL1 = "https://www.youtube.com/shorts/eto2wO2i0iA?feature=share"
TARGET_URL = "https://youtube.com/shorts/eewyMV23vXg?si=vtn1a6WMt0bDcDac" #새해인사
TARGET_URL1 = "https://youtube.com/shorts/eewyMV23vXg?si=vtn1a6WMt0bDcDac" #새해인사

NUM_BROWSERS = 2
REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_LEASE = "proxies:lease"

# Lua 스크립트 (프록시 임대 로직 - 원본 유지)
_LUA_CLAIM = """
local alive_key = KEYS[1]
local lease_key = KEYS[2]
local now = tonumber(ARGV[1])
local lease_time = tonumber(ARGV[2])

local members = redis.call('ZRANGEBYSCORE', alive_key, 0, now)
if #members > 0 then
    local proxy = members[1]
    redis.call('ZREM', alive_key, proxy)
    redis.call('ZADD', lease_key, now + lease_time, proxy)
    return proxy
end
return nil
"""

stop_event = threading.Event()

def get_region_from_proxy(proxy_str):
    """프록시 문자열에서 국가/지역 추출 (원본 유지)"""
    try:
        parsed = urlparse(proxy_str if "://" in proxy_str else f"http://{proxy_str}")
        username = parsed.username
        if username and 'country-' in username:
            parts = username.split('-')
            idx = parts.index('country')
            return parts[idx+1].upper()
    except:
        pass
    return "US"

# ===================== 2. 모니터링 서비스 (DrissionPage 버전) =====================



def handle_youtube_consent(page, slot_index):
    """
    Consent 페이지에서 selectors로 버튼을 찾지 않고,
    <form action="https://consent.youtube.com/save" method="POST"> 를 찾아 submit()으로 처리.
    (DrissionPage page 객체 기준)
    """
    try:
        # consent 페이지가 아닐 수도 있으니 가벼운 가드
        try:
            cur_url = (page.url or "").lower()
        except:
            cur_url = ""

        # 1) consent 저장용 form 탐색
        form = page.ele("css:form[action^='https://consent.youtube.com/save']", timeout=1)
        if not form:
            # 변형 케이스 대비 (혹시 action이 절대경로가 아니거나 파라미터가 붙는 경우)
            form = page.ele("css:form[action*='consent.youtube.com/save']", timeout=1)

        if not form:
            return False

        print(f"[Slot-{slot_index}] 🛡️ Consent form 감지 → submit 시도")

        # 2) JS로 submit (가장 깔끔)
        try:
            page.run_js("""
                (function(){
                    const f = document.querySelector("form[action^='https://consent.youtube.com/save']")
                           || document.querySelector("form[action*='consent.youtube.com/save']");
                    if (f) { f.submit(); return true; }
                    return false;
                })();
            """)
            page.wait.load_start()
            return True
        except Exception as e:
            print(f"[Slot-{slot_index}] ⚠️ Consent submit(JS) 실패: {str(e)[:120]}")

        # 3) Fallback: form 내부 버튼 클릭 (submit이 막히는 변형 대비)
        try:
            btn = form.ele("css:button", timeout=1)
            if btn:
                btn.click()
                page.wait.load_start()
                return True
        except Exception as e:
            print(f"[Slot-{slot_index}] ⚠️ Consent 버튼 클릭 fallback 실패: {str(e)[:120]}")

    except Exception as e:
        print(f"[Slot-{slot_index}] ⚠️ Consent 처리 중 에러: {str(e)[:120]}")

    return False


# [1] 프로그램 시작 시 딱 한 번만 호출되도록 메인 진입점에 넣어주세요
from playwright.sync_api import sync_playwright
# Playwright 기기 목록 로드 (프로그램 시작 시 전역 변수로 관리)
def get_playwright_devices():
    print("🌐 Playwright 기기 데이터베이스 로딩 중...")

    out = {}
    with sync_playwright() as p:
        for name, spec in p.devices.items():
            # ✅ Python(snake_case) / JS(camelCase) 둘 다 호환
            is_mobile = spec.get("is_mobile", spec.get("isMobile", False))
            if not is_mobile:
                continue

            user_agent = spec.get("user_agent", spec.get("userAgent"))
            viewport = spec.get("viewport")
            dsf = spec.get("device_scale_factor", spec.get("deviceScaleFactor", 2))
            has_touch = spec.get("has_touch", spec.get("hasTouch", True))

            # 최소 필수값 체크
            if not user_agent or not viewport:
                continue

            # ✅ StealthMobileBrowser가 기대하는 키로 맞춰서 저장
            out[name] = {
                "user_agent": user_agent,
                "viewport": viewport,
                "device_pixel_ratio": dsf,   # 이름만 맞춰줌
                "has_touch": has_touch,
            }

    print(f"✅ Playwright 모바일 디바이스 로드: {len(out)}개")

    # 디버그(원하면 1~2회만 켜고 끄기)
    if out:
        sample_name = next(iter(out.keys()))
        print(f"🔎 샘플 디바이스: {sample_name} | keys={list(out[sample_name].keys())}")
    else:
        print("⚠️ out이 비었습니다. playwright 버전/설치 상태를 확인하세요.")
    return out

PLAYWRIGHT_DEVICES = get_playwright_devices()

# 네트워크 에러 감지
def check_network_error(page, slot_index):
    """
    오탐 방지 버전:
    - 크롬 네트워크 에러(인터스티셜)는 보통 #main-frame-error 를 가짐
    - 그 안에서 .error-code / 텍스트 ERR_* 를 확인
    """
    try:
        # 1) 크롬 에러 인터스티셜의 대표 루트
        if not page.ele('css:#main-frame-error', timeout=0.3):
            return False

        # 2) error-code 영역이 있는지(있으면 거의 확정)
        code_el = page.ele('css:#main-frame-error .error-code', timeout=0.3)
        if code_el:
            # 디버그용 로그(원하면 유지)
            try:
                txt = (code_el.text or "").strip()
                if txt:
                    print(f"[Slot-{slot_index}] 🌐 chrome neterror code: {txt}")
            except:
                pass
            return True

        # 3) 그래도 애매하면 ERR_ 문자열로 최종 확인
        try:
            html = (page.html or "")
            if "ERR_" in html:
                return True
        except:
            pass

        return False
    except:
        return False

import time
import re

def wait_until_dom_not_empty(page, timeout=30, min_html_len=1500, interval=0.5):
    """
    page.html이 너무 짧거나 body가 비어있으면 계속 대기
    - timeout 초 내에 조건 만족하면 True, 아니면 False
    """
    import re
    end = time.monotonic() + timeout
    empty_body_re = re.compile(r"<body[^>]*>\s*</body>", re.I | re.S)

    while time.monotonic() < end:
        try:
            # ✅ 페이지 연결 상태 체크
            html = page.html or ""
            l = len(html.strip())

            # 완전 텅빈 html / 거의 about:blank 수준이면 대기
            if l < min_html_len:
                time.sleep(interval)
                continue

            # body가 통째로 비어있는 형태면 대기
            if empty_body_re.search(html):
                time.sleep(interval)
                continue

            return True
        except Exception as e:
            # ✅ 연결 끊김 감지
            error_msg = str(e)
            if "连接已断开" in error_msg or "断开" in error_msg or "disconnected" in error_msg.lower():
                print(f"🛑 브라우저 연결 끊김 감지 (DOM 체크)")
                return False
            time.sleep(interval)

    return False

def retry_page_load(page, url, slot_index, max_retries=None, retry_delay=None):
    """
    프록시 환경에서 페이지 로드 재시도 로직
    
    Args:
        page: DrissionPage 인스턴스
        url: 로드할 URL
        slot_index: 슬롯 번호
        max_retries: 최대 재시도 횟수 (None이면 config 사용)
        retry_delay: 재시도 간 대기 시간 (None이면 config 사용)
    
    Returns:
        bool: 성공 여부
    """
    if max_retries is None:
        max_retries = getattr(config, 'MAX_RETRIES', 3)
    if retry_delay is None:
        retry_delay = getattr(config, 'RETRY_DELAY', 5)
    
    timeout = getattr(config, 'PAGE_LOAD_TIMEOUT', 300)
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[Slot-{slot_index}] 🌐 페이지 로드 시도 {attempt}/{max_retries}: {url}")
            page.get(url, timeout=timeout)
            print(f"[Slot-{slot_index}] ✅ 로드 완료 (시도 {attempt})")
            return True
            
        except Exception as e:
            error_msg = str(e)
            print(f"[Slot-{slot_index}] ⚠️ 로드 실패 (시도 {attempt}/{max_retries}): {error_msg[:100]}")
            
            if attempt < max_retries:
                wait_time = retry_delay * attempt  # 점진적 증가 (5초 → 10초 → 15초)
                print(f"[Slot-{slot_index}] ⏳ {wait_time}초 후 재시도...")
                time.sleep(wait_time)
            else:
                print(f"[Slot-{slot_index}] ❌ 최대 재시도 횟수 초과")
                return False
    
    return False

def _wait_youtube_shorts_ready(page, slot_index, timeout_sec=120):
    """
    프록시 환경 최적화: 더 관대한 대기 + 재시도 로직
    """
    end = time.monotonic() + timeout_sec
    refresh_attempted = False
    
    # ========================================
    # ✅ 1단계: 네트워크 안정화 (더 긴 대기)
    # ========================================
    def _wait_network_idle(max_wait=None):
        """네트워크가 조용해질 때까지 대기"""
        if max_wait is None:
            max_wait = getattr(config, 'NETWORK_IDLE_WAIT', 60)
        
        idle_start = time.monotonic()
        idle_threshold = 3.0  # 2초 → 3초 (프록시는 더 느림)
        last_activity = time.monotonic()
        prev_html_len = 0
        
        print(f"[Slot-{slot_index}] ⏳ 네트워크 안정화 대기 중 (최대 {max_wait}초)...")
        
        while time.monotonic() - idle_start < max_wait:
            # ✅ 브라우저 종료 감지
            if stop_event.is_set():
                return False
            
            try:
                # ✅ 페이지 연결 상태 체크
                _ = page.url  # 연결 끊어지면 예외 발생
                cur_html_len = len(page.html or "")
                
                if cur_html_len != prev_html_len:
                    last_activity = time.monotonic()
                    prev_html_len = cur_html_len
                
                # 3초간 변화 없음 = idle
                if time.monotonic() - last_activity > idle_threshold:
                    elapsed = time.monotonic() - idle_start
                    print(f"[Slot-{slot_index}] ✅ 네트워크 안정화 완료 ({elapsed:.1f}초)")
                    return True
                    
            except Exception as e:
                # ✅ 연결 끊김 감지
                error_msg = str(e)
                if "连接已断开" in error_msg or "断开" in error_msg or "disconnected" in error_msg.lower():
                    print(f"[Slot-{slot_index}] 🛑 브라우저 연결 끊김 감지")
                    return False
                print(f"[Slot-{slot_index}] ⚠️ HTML 체크 오류: {error_msg[:100]}")
            
            time.sleep(0.5)  # 0.3 → 0.5초 (프록시 부하 감소)
        
        print(f"[Slot-{slot_index}] ⏰ 네트워크 안정화 타임아웃 (진행)")
        return True
    
    # ========================================
    # ✅ 2단계: 컨텐츠 확인 (관대한 조건)
    # ========================================
    def _check_content_ready(max_attempts=30):  # 20 → 30
        """실제 video 컨텐츠가 있는지 확인"""
        element_timeout = getattr(config, 'ELEMENT_WAIT_TIMEOUT', 45)
        
        for attempt in range(max_attempts):
            # ✅ 브라우저 종료 감지
            if stop_event.is_set():
                return False, "stopped"
            
            try:
                # ✅ 페이지 연결 상태 체크
                cur_url = page.url  # 연결 끊어지면 예외 발생
                
                # Auth/Challenge 체크
                if _is_auth_or_challenge_url(cur_url):
                    if "consent.youtube.com" in cur_url.lower():
                        from main_drission import handle_youtube_consent
                        if handle_youtube_consent(page, slot_index):
                            time.sleep(3)  # 동의 후 충분한 대기
                            continue
                    return False, "auth_or_challenge"
                
                # 네트워크 에러
                from main_drission import check_network_error
                if check_network_error(page, slot_index):
                    return False, "net_error"
                
                # Captcha
                if _has_captcha_dom(page):
                    return False, "captcha_dom"
                
                # HTML 최소 길이 (더 관대하게)
                html_len = len(page.html or "")
                if html_len < 1500:  # 2000 → 1500
                    time.sleep(1.0)  # 0.5 → 1.0초
                    continue
                
                # YouTube shell
                if not _has_youtube_shell(page):
                    time.sleep(1.0)
                    continue
                
                # Video element + stream
                st = _video_stream_state(page)
                if st and st.get("src") and st.get("rs", 0) >= 1:
                    return True, "ok"
                
            except Exception as e:
                # ✅ 연결 끊김 감지
                error_msg = str(e)
                if "连接已断开" in error_msg or "断开" in error_msg or "disconnected" in error_msg.lower():
                    print(f"[Slot-{slot_index}] 🛑 브라우저 연결 끊김 감지 (컨텐츠 체크)")
                    return False, "browser_closed"
                print(f"[Slot-{slot_index}] ⚠️ 컨텐츠 체크 오류: {error_msg[:100]}")
            
            time.sleep(1.0)  # 0.5 → 1.0초
        
        return False, "content_not_ready"
    
    # ========================================
    # 메인 로직
    # ========================================
    while time.monotonic() < end:
        # 1단계: 네트워크 안정화
        if not _wait_network_idle():
            return False, "stopped"
        
        # 2단계: 컨텐츠 확인
        ok, reason = _check_content_ready()
        
        if ok:
            return True, "ok"
        
        # 3단계: Refresh (1회만)
        if not refresh_attempted and reason == "content_not_ready":
            print(f"[Slot-{slot_index}] 🔄 컨텐츠 미확인 → Refresh 시도")
            try:
                page.refresh()
                refresh_attempted = True
                time.sleep(4)  # 2 → 4초 (refresh 후 충분한 대기)
                continue
            except Exception as e:
                print(f"[Slot-{slot_index}] ⚠️ Refresh 실패: {e}")
                return False, "refresh_failed"
        
        # Refresh 후에도 실패
        if refresh_attempted:
            print(f"[Slot-{slot_index}] ❌ Refresh 후에도 실패: {reason}")
            return False, reason
        
        # 다른 이유로 실패
        return False, reason
    
    return False, "timeout"


# ========================================
# 헬퍼 함수들 (기존 유지)
# ========================================

def _is_auth_or_challenge_url(cur_url: str) -> bool:
    if not cur_url:
        return False
    u = cur_url.lower()
    return any(x in u for x in [
        "consent.youtube.com",
        "accounts.google.com",
        "/sorry/",
        "challenge",
        "captcha",
        "verify",
        "signin",
    ])

def _has_captcha_dom(page) -> bool:
    try:
        if page.ele('css:iframe[src*="recaptcha"]', timeout=0.3):
            return True
    except:
        pass
    try:
        if page.ele('css:iframe[title*="recaptcha"]', timeout=0.3):
            return True
    except:
        pass
    return False

def _has_youtube_shell(page) -> bool:
    try:
        return bool(page.ele("tag:ytd-app", timeout=0.3) or page.ele("tag:ytm-app", timeout=0.3))
    except:
        return False

def _video_stream_state(page):
    try:
        return page.run_js("""
            const v = document.querySelector('video');
            if (!v) return null;
            return {
                src: (v.currentSrc || v.src || ''),
                rs: v.readyState,
                ns: v.networkState,
                paused: v.paused
            };
        """)
    except:
        return None


def ensure_video_playing(page, slot_index):
    """
    비디오가 일시정지되었는지 확인하고 재생 상태 유지
    """
    try:
        state = page.run_js("""
            const v = document.querySelector('video');
            if (!v) return null;
            return {
                paused: v.paused,
                ended: v.ended,
                currentTime: v.currentTime,
                duration: v.duration
            };
        """)
        
        if state and state.get('paused') and not state.get('ended'):
            print(f"[Slot-{slot_index}] ⚠️ 영상 일시정지 감지 → 재생 재개")
            page.run_js("""
                const v = document.querySelector('video');
                if (v && v.paused) {
                    v.play().catch(e => console.log('Play failed:', e));
                }
            """)
            return True
        return False
    except Exception as e:
        print(f"[Slot-{slot_index}] ⚠️ 재생 상태 확인 실패: {str(e)[:100]}")
        return False


def keep_browser_focused(page, slot_index):
    """
    브라우저 윈도우를 포커스하고 최상위로 유지
    """
    try:
        # 윈도우를 최상위로 가져오기
        page.run_js("""
            window.focus();
            if (document.hidden) {
                document.dispatchEvent(new Event('visibilitychange'));
            }
        """)
        
        # 비디오에 포커스 이벤트 트리거
        page.run_js("""
            const v = document.querySelector('video');
            if (v) {
                v.dispatchEvent(new Event('focus'));
                // 자동재생 정책 우회를 위한 사용자 제스처 시뮬레이션
                v.muted = false;
            }
        """)
        return True
    except Exception as e:
        print(f"[Slot-{slot_index}] ⚠️ 포커스 유지 실패: {str(e)[:100]}")
        return False
# ========================================
# ✅ monitor_service 함수 수정 (핵심)
# ========================================

def monitor_service_optimized(url, proxy, slot_index, stop_event, r):
    """프록시 최적화 버전 - 재생 중단 방지 로직 추가"""
    import time
    import random
    from stealth_browser import StealthMobileBrowser
    from MobileHumanEvent import MobileHumanEvent

    browser_wrapper = None
    start_time = time.time()
    session_timeout = random.randint(config.SLOT_LIFE_MIN, config.SLOT_LIFE_MAX)
    
    print(f"\n[Slot-{slot_index}] 🚀 [START] 세션 구동 시작 (Proxy: {proxy})")

    try:
        # 1) 프로필/리퍼러 선택
        try:
            from main_drission import REGION_PROFILES
            region_key = random.choice(list(REGION_PROFILES.keys()))
            profile = REGION_PROFILES[region_key]
            selected_referer = random.choice(profile.get("referers", ["https://www.google.com/"]))
            print(f"[Slot-{slot_index}] 🌍 지역: {region_key} | 유입경로: {selected_referer}")
        except:
            profile = {"locale": "en-US", "timezone": "America/New_York"}
            selected_referer = "https://www.google.com/"
            print(f"[Slot-{slot_index}] ⚠️ 기본 프로필 및 구글 리퍼러 사용")

        # 2) 브라우저 생성
        from main_drission import PLAYWRIGHT_DEVICES
        browser_wrapper = StealthMobileBrowser(
            slot_index=slot_index,
            profile=profile,
            proxy=proxy,
            devices_dict=PLAYWRIGHT_DEVICES,
            referer=selected_referer
        )
        page = browser_wrapper.page
        print(f"[Slot-{slot_index}] ✨ 브라우저 초기화 완료")

        # 3) 페이지 로드
        print(f"[Slot-{slot_index}] 🌐 타겟 접속 시작: {url}")
        if not retry_page_load(page, url, slot_index):
            print(f"[Slot-{slot_index}] ❌ [FAIL] 페이지 로드 실패")
            return
        
        # 4) DOM 로드 확인
        print(f"[Slot-{slot_index}] 📄 DOM 로드 확인 중...")
        if not wait_until_dom_not_empty(page, timeout=30, min_html_len=1000):
            print(f"[Slot-{slot_index}] ⚠️ DOM이 비어있거나 너무 작음")

        # 5) 페이지 준비 대기
        print(f"[Slot-{slot_index}] ⏳ 페이지 렌더링 대기 중...")
        ok, reason = _wait_youtube_shorts_ready(page, slot_index, timeout_sec=240)

        if not ok:
            print(f"[Slot-{slot_index}] ❌ [FAIL] 준비 실패: {reason}")
            return

        # 6) 최종 URL 검증
        try:
            cur = page.url
            if ("youtube.com" not in cur) or ("/shorts/" not in cur):
                print(f"[Slot-{slot_index}] ❌ [FAIL] 비정상 주소: {cur[:120]}")
                return
            print(f"[Slot-{slot_index}] ✅ 페이지 준비 완료: {cur}")
        except:
            print(f"[Slot-{slot_index}] ❌ [FAIL] URL 접근 불가")
            return

        # 7) ✅ 초기 포커스 설정 및 재생 확인
        keep_browser_focused(page, slot_index)
        time.sleep(1)
        ensure_video_playing(page, slot_index)

        # 8) 체류 및 액션 시퀀스
        fixed_action_time = 40
        base_stay = random.randint(45, 90)
        stay_time = base_stay + fixed_action_time
        stay_start = time.time()

        human_handler = MobileHumanEvent(page)
        action_performed = False
        
        # ✅ 재생 상태 모니터링 주기 설정
        last_play_check = time.time()
        play_check_interval = 5  # 5초마다 재생 상태 확인

        print(f"[Slot-{slot_index}] ▶️ 재생 확인. {stay_time}초 시청 루프 시작.")

        while time.time() - stay_start < stay_time:
            if stop_event.is_set():
                break

            try:
                _ = page.url
            except:
                print(f"[Slot-{slot_index}] 🛑 브라우저 종료 감지")
                break

            elapsed = int(time.time() - stay_start)

            # ✅ 주기적으로 재생 상태 확인 및 포커스 유지
            if time.time() - last_play_check >= play_check_interval:
                if ensure_video_playing(page, slot_index):
                    # 일시정지가 감지되어 재생을 재개한 경우
                    keep_browser_focused(page, slot_index)
                last_play_check = time.time()

            # 40초 시점 액션 수행
            if not action_performed and elapsed >= fixed_action_time:
                print(f"\n[Slot-{slot_index}] 🔥 [ACTION] 40초 도달! 랜덤 액션 수행")
                
                # ✅ 액션 전 포커스 확보
                keep_browser_focused(page, slot_index)
                time.sleep(0.5)
                
                human_handler.execute_random_action()
                action_performed = True

                # ✅ 액션 후 재생 상태 확인
                time.sleep(1)
                ensure_video_playing(page, slot_index)

                post_delay = random.uniform(8.0, 12.0)
                print(f"[Slot-{slot_index}] 💤 추가 대기 {post_delay:.1f}초 후 세션 종료.")
                time.sleep(post_delay)
                break

            if elapsed > 0 and elapsed % 15 == 0:
                print(f"[Slot-{slot_index}] 📺 시청 진행 중... ({elapsed}s / {stay_time}s)")

            time.sleep(1)

        print(f"[Slot-{slot_index}] ✨ [SUCCESS] 미션 완료.")

    except Exception as e:
        print(f"[Slot-{slot_index}] ❌ [CRITICAL] {e}")

    finally:
        if browser_wrapper:
            browser_wrapper.quit()
        try:
            r.zrem(config.REDIS_LEASE_KEY, proxy)
            r.zadd(config.REDIS_ALIVE_KEY, {proxy: int(time.time()) + 60})
            print(f"[Slot-{slot_index}] 🔄 자원 정리 및 프록시 반납.\n")
        except:
            pass
        
      
def main():
    r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
    active_slots = {}

    print(f"🚀 모니터링 시작 (최대 슬롯: {NUM_BROWSERS})")

    try:
        while not stop_event.is_set():
            # 1) 종료된 스레드 정리
            for s in list(active_slots.keys()):
                if not active_slots[s].is_alive():
                    del active_slots[s]
                    print(f"[Main] 🧹 슬롯-{s} 스레드 정리")

            # 2) 빈 슬롯 채우기
            if len(active_slots) < NUM_BROWSERS:
                for s in range(NUM_BROWSERS):
                    if s not in active_slots:
                        # Lua 스크립트로 프록시 임대
                        proxy = r.eval(_LUA_CLAIM, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, int(time.time()), 600)
                        if proxy:
                            url = TARGET_URL if s % 2 == 0 else TARGET_URL1
                            t = threading.Thread(
                                target=monitor_service_optimized,
                                args=(url, proxy, s, stop_event, r),
                                daemon=True,
                                name=f"Slot-{s}"
                            )
                            t.start()
                            active_slots[s] = t
                            print(f"[Main] ✅ 슬롯-{s} 활성화 (Proxy: {proxy})")
                            time.sleep(2) # 순차적 생성
                        else:
                            print(f"[Main] ⚠️ 사용 가능한 프록시 없음...")
                            break
            
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n🛑 KeyboardInterrupt 감지")
    finally:
        stop_event.set()
        print("\n🛑 종료 중... 모든 스레드 대기")
        for t in active_slots.values():
            t.join(timeout=10)
        print("👋 프로그램 종료")

if __name__ == "__main__":
    main()