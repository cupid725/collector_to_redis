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

NUM_BROWSERS = 1
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
    Playwright 원본 로직을 참고하여 텍스트가 아닌 구조적 셀렉터로 Consent를 처리합니다.
    구글/유튜브의 동의 창은 보통 특정 form 내부의 두 번째 혹은 마지막 버튼인 경우가 많습니다.
    """
    try:
        # 1. 특정 클래스나 구조를 가진 버튼 셀렉터 (Playwright 소스 기반 최적화)
        # 구글 Consent 페이지의 '동의' 버튼은 보통 특정 form 안의 2번째 또는 마지막 버튼임
        selectors = [
            "form[action*='consent.google.com'] button", # Consent 폼 내의 버튼
            "form[action*='google.com/consent'] button",
            "div.VfPpkd-LgbsSe", # 구글 표준 버튼 클래스
            "button[aria-label*='Agree']", 
            "button[aria-label*='Accept']"
        ]
        
        for selector in selectors:
            buttons = page.eles(selector, timeout=1)
            if buttons:
                # 보통 동의 버튼은 리스트의 마지막(last)에 위치하는 경우가 많음
                target_btn = buttons[-1] 
                print(f"[Slot-{slot_index}] 🛡️ Consent 페이지 감지 (Selector: {selector}). 버튼 클릭 시도.")
                target_btn.click()
                page.wait.load_start()
                return True
                
        # 2. 만약 위 방법으로 안될 경우 특정 위치 기반 클릭 (Playwright에서 자주 쓰는 방식)
        # 동의 창이 떴을 때 '동의' 버튼의 일반적인 좌표 영역을 강제 클릭할 수도 있음
    except Exception as e:
        print(f"[Slot-{slot_index}] ⚠️ Consent 처리 중 에러: {e}")
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

def wait_until_dom_not_empty(page, timeout=30, min_html_len=2000, interval=0.3):
    """
    - page.html이 너무 짧거나 body가 비어있으면 계속 대기
    - timeout 초 내에 조건 만족하면 True, 아니면 False
    """
    end = time.monotonic() + timeout
    last_len = -1
    empty_body_re = re.compile(r"<body[^>]*>\s*</body>", re.I | re.S)

    while time.monotonic() < end:
        try:
            html = page.html or ""
            l = len(html.strip())

            # 완전 텅빈 html / 거의 about:blank 수준이면 대기
            if l < min_html_len:
                last_len = l
                time.sleep(interval)
                continue

            # body가 통째로 비어있는 형태면 대기
            if empty_body_re.search(html):
                last_len = l
                time.sleep(interval)
                continue

            return True
        except Exception:
            time.sleep(interval)

    return False

def monitor_service(url, proxy, slot_index, stop_event, r):
    import time
    import random

    browser_wrapper = None
    start_time = time.time()
    session_timeout = random.randint(config.SLOT_LIFE_MIN, config.SLOT_LIFE_MAX)

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

    def _has_captcha_dom() -> bool:
        try:
            if page.ele('css:iframe[src*="recaptcha"]', timeout=0.2):
                return True
        except:
            pass
        try:
            if page.ele('css:iframe[title*="recaptcha"]', timeout=0.2):
                return True
        except:
            pass
        return False

    def _has_youtube_shell() -> bool:
        try:
            return bool(page.ele("tag:ytd-app", timeout=0.2) or page.ele("tag:ytm-app", timeout=0.2))
        except:
            return False

    def _video_stream_state():
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

    def _wait_youtube_shorts_ready(timeout_sec=60):
        """
        1단계: Network idle 대기
        2단계: 컨텐츠 확인
        3단계: 실패 시 refresh 후 재시도
        """
        end = time.monotonic() + timeout_sec
        refresh_attempted = False
        
        def _wait_network_idle(max_wait=30):
            """네트워크가 2초간 조용해질 때까지 대기"""
            idle_start = time.monotonic()
            idle_threshold = 2.0
            last_activity = time.monotonic()
            
            # 간단한 polling 방식 (CDP 이벤트 리스너는 복잡하므로)
            prev_html_len = 0
            
            while time.monotonic() - idle_start < max_wait:
                if stop_event.is_set():
                    return False
                
                try:
                    cur_html_len = len(page.html or "")
                    # HTML 길이 변화 = 네트워크 활동
                    if cur_html_len != prev_html_len:
                        last_activity = time.monotonic()
                        prev_html_len = cur_html_len
                    
                    # 2초간 변화 없음 = idle
                    if time.monotonic() - last_activity > idle_threshold:
                        return True
                        
                except:
                    pass
                
                time.sleep(0.3)
            
            return True  # timeout이어도 일단 진행
        
        def _check_content_ready(max_attempts=20):
            """실제 video 컨텐츠가 있는지 확인"""
            for attempt in range(max_attempts):
                if stop_event.is_set():
                    return False, "stopped"
                
                try:
                    cur_url = page.url
                    
                    # Auth/Challenge 체크
                    if _is_auth_or_challenge_url(cur_url):
                        if "consent.youtube.com" in cur_url.lower():
                            if handle_youtube_consent(page, slot_index):
                                time.sleep(2)
                                continue
                        return False, "auth_or_challenge"
                    
                    # 네트워크 에러
                    if check_network_error(page, slot_index):
                        return False, "net_error"
                    
                    # Captcha
                    if _has_captcha_dom():
                        return False, "captcha_dom"
                    
                    # HTML 최소 길이
                    html_len = len(page.html or "")
                    if html_len < 2000:
                        time.sleep(0.5)
                        continue
                    
                    # YouTube shell
                    if not _has_youtube_shell():
                        time.sleep(0.5)
                        continue
                    
                    # Video element + stream
                    st = _video_stream_state()
                    if st and st.get("src") and st.get("rs", 0) >= 1:
                        return True, "ok"
                    
                except Exception as e:
                    pass
                
                time.sleep(0.5)
            
            return False, "content_not_ready"
        
        # === 메인 로직 ===
        
        while time.monotonic() < end:
            if stop_event.is_set():
                return False, "stopped"
            
            # 1단계: Network idle 대기
            print(f"[Slot-{slot_index}] 🌐 네트워크 안정화 대기 중...")
            if not _wait_network_idle(max_wait=30):
                return False, "stopped"
            
            print(f"[Slot-{slot_index}] ✅ Network idle 감지")
            
            # 2단계: 컨텐츠 확인 (최대 10초)
            ok, reason = _check_content_ready(max_attempts=20)
            
            if ok:
                return True, "ok"
            
            # 3단계: 컨텐츠 없으면 refresh (1회만)
            if not refresh_attempted and reason == "content_not_ready":
                print(f"[Slot-{slot_index}] 🔄 컨텐츠 미확인 → Refresh 시도")
                try:
                    page.refresh()
                    refresh_attempted = True
                    time.sleep(2)  # refresh 후 초기 대기
                    continue  # 다시 1단계부터
                except Exception as e:
                    print(f"[Slot-{slot_index}] ⚠️ Refresh 실패: {e}")
                    return False, "refresh_failed"
            
            # refresh도 했는데 안 되면 종료
            if refresh_attempted:
                print(f"[Slot-{slot_index}] ❌ Refresh 후에도 실패: {reason}")
                return False, reason
            
            # 다른 이유로 실패 (auth, captcha 등)
            return False, reason
        
        return False, "timeout"
    print(f"\n[Slot-{slot_index}] 🚀 [START] 세션 구동 시작 (Proxy: {proxy})")

    try:
        # 1) 프로필/리퍼러 선택
        try:
            region_key = random.choice(list(REGION_PROFILES.keys()))
            profile = REGION_PROFILES[region_key]
            selected_referer = random.choice(profile.get("referers", ["https://www.google.com/"]))
            print(f"[Slot-{slot_index}] 🌍 지역: {region_key} | 유입경로: {selected_referer}")
        except:
            profile = {"locale": "en-US", "timezone": "America/New_York"}
            selected_referer = "https://www.google.com/"
            print(f"[Slot-{slot_index}] ⚠️ 기본 프로필 및 구글 리퍼러 사용")

        # 2) 브라우저 생성
        browser_wrapper = StealthMobileBrowser(
            slot_index=slot_index,
            profile=profile,
            proxy=proxy,
            devices_dict=PLAYWRIGHT_DEVICES,
            referer=selected_referer
        )
        page = browser_wrapper.page
        print(f"[Slot-{slot_index}] ✨ 브라우저 초기화 완료")

        # 3) 페이지 로드 - 타임아웃만 설정하고 즉시 체크하지 않음
        print(f"[Slot-{slot_index}] 🌐 타겟 접속 시작: {url}")
        try:
            page.get(url, timeout=config.PAGE_LOAD_TIMEOUT)
        except Exception as e:
            print(f"[Slot-{slot_index}] ⚠️ page.get() 예외 (계속 진행): {e}")

        # 4) 진짜 대기 - 여기서만 모든 검증 수행
        print(f"[Slot-{slot_index}] ⏳ 페이지 렌더링 대기 중...")
        ok, reason = _wait_youtube_shorts_ready(timeout_sec=120)

        if not ok:
            print(f"[Slot-{slot_index}] ❌ [FAIL] 준비 실패: {reason}")
            # 디버깅용 정보 출력
            try:
                print(f"[Slot-{slot_index}] 📊 최종 URL: {page.url}")
                print(f"[Slot-{slot_index}] 📊 HTML 길이: {len(page.html or '')}")
            except:
                pass
            return

        # 5) 최종 URL 검증
        try:
            cur = page.url
            if ("youtube.com" not in cur) or ("/shorts/" not in cur):
                print(f"[Slot-{slot_index}] ❌ [FAIL] 비정상 주소: {cur[:120]}")
                return
            print(f"[Slot-{slot_index}] ✅ 페이지 준비 완료: {cur}")
        except:
            print(f"[Slot-{slot_index}] ❌ [FAIL] URL 접근 불가")
            return

        # 6) 체류 및 액션 시퀀스
        fixed_action_time = 40
        base_stay = random.randint(45, 90)
        stay_time = base_stay + fixed_action_time
        stay_start = time.time()

        human_handler = MobileHumanEvent(page)
        action_performed = False

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

            if not action_performed and elapsed >= fixed_action_time:
                print(f"\n[Slot-{slot_index}] 🔥 [ACTION] 40초 도달! 랜덤 액션 수행")
                human_handler.execute_random_action()
                action_performed = True

                post_delay = random.uniform(5.0, 8.0)
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

        
def monitor_service_old(url, proxy, slot_index, stop_event, r):
    browser_wrapper = None
    start_time = time.time()
    session_timeout = random.randint(config.SLOT_LIFE_MIN, config.SLOT_LIFE_MAX)
    
    print(f"\n[Slot-{slot_index}] 🚀 [START] 세션 구동 시작 (Proxy: {proxy})")

    try:
        # 1. 프로필 및 리퍼러 선택
        try:
            region_key = random.choice(list(REGION_PROFILES.keys()))
            profile = REGION_PROFILES[region_key]
            # [추가] 프로필 내 리퍼러 리스트에서 랜덤 선택
            selected_referer = random.choice(profile.get("referers", ["https://www.google.com/"]))
            print(f"[Slot-{slot_index}] 🌍 지역: {region_key} | 유입경로: {selected_referer}")
        except:
            profile = {"locale": "en-US", "timezone": "America/New_York"}
            selected_referer = "https://www.google.com/"
            print(f"[Slot-{slot_index}] ⚠️ 기본 프로필 및 구글 리퍼러 사용")

        # 2. [수정] 브라우저 생성 시 selected_referer 전달
        browser_wrapper = StealthMobileBrowser(
            slot_index=slot_index, 
            profile=profile, 
            proxy=proxy, 
            devices_dict=PLAYWRIGHT_DEVICES,
            referer=selected_referer
        )
        page = browser_wrapper.page
        print(f"[Slot-{slot_index}] ✨ 브라우저 초기화 완료")

        # 3. [수정] 페이지 접속 (Referer 적용)
        print(f"[Slot-{slot_index}] 🌐 타겟 접속 시작: {url}")
        page.get(url, timeout=config.PAGE_LOAD_TIMEOUT)
        print(f"[Slot-{slot_index}] 🌐 타겟 접속 리턴: {url}")
        # 4. 네트워크 에러 체크
        if check_network_error(page, slot_index):
            print(f"[Slot-{slot_index}] ❌ [ERROR] 네트워크 에러 감지. 세션 종료.")
            #return

        # 5. 영상 로딩 대기 및 검증
        print(f"[Slot-{slot_index}] ⏳ 영상 재생 확인 중...")
        if not page.wait.ele_displayed('tag:video', timeout=20):
            print(f"[Slot-{slot_index}] ❌ [FAIL] 영상 로드 실패")
            return
        
        if "m.youtube.com" not in page.url:
            print(f"[Slot-{slot_index}] ❌ [FAIL] 비정상 주소: {page.url[:40]}")
            return

        # 6. 체류 및 액션 시퀀스
        fixed_action_time = 80 
        base_stay = random.randint(45, 90)
        stay_time = base_stay + fixed_action_time
        stay_start = time.time()
        
        from MobileHumanEvent import MobileHumanEvent
        human_handler = MobileHumanEvent(page)
        action_performed = False

        print(f"[Slot-{slot_index}] ▶️ 재생 확인. {stay_time}초 시청 루프 시작.")

        while time.time() - stay_start < stay_time:
            if stop_event.is_set(): break
            try:
                _ = page.url 
            except:
                print(f"[Slot-{slot_index}] 🛑 브라우저 종료 감지")
                break

            elapsed = int(time.time() - stay_start)

            # 40초 도달 시 액션 실행
            if not action_performed and elapsed >= fixed_action_time:
                print(f"\n[Slot-{slot_index}] 🔥 [ACTION] 40초 도달! 랜덤 액션 수행")
                human_handler.execute_random_action()
                action_performed = True
                
                post_delay = random.uniform(5.0, 8.0)
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
        except: pass
        
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
                                target=monitor_service,
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