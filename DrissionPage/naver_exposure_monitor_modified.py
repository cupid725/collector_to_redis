import os
import re
import json
import time
import csv
import socket
import shutil
import random
import logging
import tempfile
import threading
import struct
import redis
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from pathlib import Path

import requests
from DrissionPage.common import Keys
from playwright.sync_api import sync_playwright

# stealth_browser.py에서 클래스 임포트
from stealth_browser import StealthMobileBrowser

# =============================================================================
# 0) 사용자 설정
# =============================================================================
MAX_THREADS = 1

ENABLE_WINDOW_SIZE = True
WINDOW_WIDTH = 1000
WINDOW_HEIGHT = 400
ENABLE_WINDOW_JITTER = False
WINDOW_JITTER_RANGE = 80
ENABLE_WINDOW_POSITION = True
WINDOW_POS_X = 50
WINDOW_POS_Y = 400
ENABLE_BLOCK_CHECK = False
CHECK_INTERVAL_SECONDS = 60
MAX_PAGES = 10

TASKS = [
    #{"keyword": "올빼미티비", "domain": "https://www.tvda.co.kr/?srt=1"},
    {"keyword": "블랑티비", "domain": "https://www.flyingobjectives.co.kr/rank/"},
]

MAX_PROXIES_PER_TASK = 30
REFRESH_PROXIES_EACH_CYCLE = True
PAGELOAD_TIMEOUT_SEC = 60*2
ELEM_WAIT_SEC = 30

# 🔒 스텔스 모드 설정
ENABLE_STEALTH = True
RANDOM_DELAY_MIN = 2.0
RANDOM_DELAY_MAX = 5.0
ENABLE_MOUSE_MOVEMENT = True
SCROLL_BEHAVIOR = True

OUT_DIR = os.path.abspath("./naver_monitor_out")
LOG_FILE = os.path.join(OUT_DIR, "monitor.log")
RESULT_JSONL = os.path.join(OUT_DIR, "results.jsonl")
RESULT_CSV = os.path.join(OUT_DIR, "results.csv")
WINDOW_STATE_FILE = os.path.join(OUT_DIR, "window_states.json")
STOP_EVENT = threading.Event()
FILE_LOCK = threading.Lock()
WINDOW_STATE_LOCK = threading.Lock()

# 전역 변수
MY_PUBLIC_IP = None

# Redis 설정
REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_LEASE = "proxies:lease"

# Lua 스크립트 (프록시 임대)
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

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# =============================================================================
# 1) Playwright 디바이스 로드
# =============================================================================
def get_playwright_devices():
    print("🌐 Playwright 기기 데이터베이스 로딩 중...")
    out = {}
    with sync_playwright() as p:
        for name, spec in p.devices.items():
            is_mobile = spec.get("is_mobile", spec.get("isMobile", False))
            if not is_mobile:
                continue
            
            if "landscape" in name.lower():
                continue

            user_agent = spec.get("user_agent", spec.get("userAgent"))
            viewport = spec.get("viewport")
            dsf = spec.get("device_scale_factor", spec.get("deviceScaleFactor", 2))
            has_touch = spec.get("has_touch", spec.get("hasTouch", True))

            if not user_agent or not viewport:
                continue

            out[name] = {
                "user_agent": user_agent,
                "viewport": viewport,
                "device_pixel_ratio": dsf,
                "has_touch": has_touch,
            }

    print(f"✅ Playwright 모바일 디바이스 로드: {len(out)}개")
    if out:
        sample_name = next(iter(out.keys()))
        print(f"🔎 샘플 디바이스: {sample_name}")
    return out

PLAYWRIGHT_DEVICES = get_playwright_devices()

# =============================================================================
# 2) 지역 프로필 로드
# =============================================================================
REGION_PROFILES = {}
try:
    if os.path.exists(r".\DrissionPage\region_profiles_mobile.json"):
        with open(r".\DrissionPage\region_profiles_mobile.json", 'r', encoding='utf-8') as f:
            REGION_PROFILES = json.load(f)
        print(f"✅ 지역 프로필 로드 완료 ({len(REGION_PROFILES)}개 지역)")
    else:
        print("⚠️ .\DrissionPage\region_profiles_mobile.json 파일이 없습니다. 기본 설정 사용")
        REGION_PROFILES = {
            "KR": {
                "locale": "ko-KR",
                "timezone": "Asia/Seoul",
                "referers": ["https://www.naver.com/", "https://www.google.com/"]
            }
        }
except Exception as e:
    print(f"❌ 지역 프로필 로드 실패: {e}")
    REGION_PROFILES = {
        "KR": {
            "locale": "ko-KR",
            "timezone": "Asia/Seoul",
            "referers": ["https://www.naver.com/"]
        }
    }

# =============================================================================
# 3) 데이터 모델 및 유틸
# =============================================================================
@dataclass
class ProxyInfo:
    protocol: str
    address: str
    source: str

@dataclass
class RunResult:
    ts: str
    keyword: str
    target_url: str
    proxy_protocol: Optional[str]
    proxy_address: Optional[str]
    proxy_source: Optional[str]
    found: bool
    found_page: Optional[int]
    found_rank_on_page: Optional[int]
    found_href: Optional[str]
    clicked_ok: bool
    final_url: Optional[str]
    error: Optional[str]
    note: Optional[str]

def setup_logging() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s", datefmt="%H:%M:%S")
    
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    
    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(sh)

# =============================================================================
# 4) IP 확인 및 스텔스 유틸리티 함수
# =============================================================================
def get_my_actual_ip():
    """실행 시점의 내 실제 공인 IP 확인"""
    try:
        res = requests.get("https://api.ipify.org", timeout=10)
        return res.text.strip()
    except:
        return None

def is_proxy_leaking_my_ip(proxy: ProxyInfo, my_ip: str):
    """프록시가 내 IP를 유출하고 있는지 확인"""
    if not my_ip:
        return False
    try:
        proxies = {
            "http": f"{proxy.protocol}://{proxy.address}",
            "https": f"{proxy.protocol}://{proxy.address}"
        }
        res = requests.get("https://api.ipify.org", proxies=proxies, timeout=10)
        return res.text.strip() == my_ip
    except:
        return False

def load_window_state(slot_id: str) -> Optional[Dict]:
    try:
        with WINDOW_STATE_LOCK:
            if os.path.exists(WINDOW_STATE_FILE):
                with open(WINDOW_STATE_FILE, 'r', encoding='utf-8') as f:
                    states = json.load(f)
                    return states.get(slot_id)
    except Exception as e:
        logging.warning(f"⚠️ 창 상태 로드 실패 (슬롯 {slot_id}): {e}")
    return None

def save_window_state(slot_id: str, x: int, y: int, width: int, height: int) -> None:
    try:
        with WINDOW_STATE_LOCK:
            states = {}
            if os.path.exists(WINDOW_STATE_FILE):
                with open(WINDOW_STATE_FILE, 'r', encoding='utf-8') as f:
                    states = json.load(f)
            states[slot_id] = {'x': x, 'y': y, 'width': width, 'height': height}
            with open(WINDOW_STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(states, f, indent=2)
            logging.info(f"💾 창 상태 저장 완료 (슬롯 {slot_id}): 위치({x},{y}) 크기({width}x{height})")
    except Exception as e:
        logging.warning(f"⚠️ 창 상태 저장 실패 (슬롯 {slot_id}): {e}")

def random_delay(min_sec: float = None, max_sec: float = None) -> None:
    if not ENABLE_STEALTH:
        return
    min_val = min_sec if min_sec is not None else RANDOM_DELAY_MIN
    max_val = max_sec if max_sec is not None else RANDOM_DELAY_MAX
    time.sleep(random.uniform(min_val, max_val))

def simulate_human_typing(element, text: str) -> None:
    """DrissionPage 요소에 대한 인간 타이핑 시뮬레이션 (모바일 웹 UI 변경 대응)"""

    try:
        # 1. 검색창 클릭 (UI 변경 트리거)
        logging.debug("🔍 검색창 클릭 시도...")
        try:
            element.click()
            logging.debug("✅ 검색창 클릭 완료")
        except:
            # 클릭 실패 시 JS로 시도
            try:
                element.run_js("this.click();")
                logging.debug("✅ 검색창 클릭 완료 (JS)")
            except Exception as e:
                logging.warning(f"⚠️ 검색창 클릭 실패: {str(e)[:100]}")
        
        # 2. UI 변경 대기 (중요!)
        time.sleep(random.uniform(0.8, 1.5))
        logging.debug("⏳ UI 변경 대기 완료")
        
        # 3. 변경된 UI에서 활성화된 입력창 찾기
        page = element.page
        active_input = None
        
        # 방법 1: 포커스된 요소 찾기
        try:
            active_input = page.run_js("return document.activeElement;")
            if active_input and active_input.tag in ['input', 'textarea']:
                logging.debug("✅ 포커스된 입력창 발견 (activeElement)")
            else:
                active_input = None
        except:
            pass
        
        # 방법 2: name='query'인 visible 입력창 찾기
        if not active_input:
            try:
                inputs = page.eles("@name=query")
                for inp in inputs:
                    try:
                        # 화면에 보이는 입력창인지 확인
                        is_visible = page.run_js("""
                            var elem = arguments[0];
                            return elem.offsetWidth > 0 && 
                                   elem.offsetHeight > 0 && 
                                   window.getComputedStyle(elem).visibility !== 'hidden' &&
                                   window.getComputedStyle(elem).display !== 'none';
                        """, inp)
                        if is_visible:
                            active_input = inp
                            logging.debug("✅ 보이는 입력창 발견 (name=query)")
                            break
                    except:
                        continue
            except:
                pass
        
        # 방법 3: CSS selector로 visible input 찾기
        if not active_input:
            try:
                inputs = page.eles("css:input[type='text'], css:input[type='search'], css:input:not([type])")
                for inp in inputs:
                    try:
                        is_visible = page.run_js("""
                            var elem = arguments[0];
                            var rect = elem.getBoundingClientRect();
                            return rect.width > 0 && 
                                   rect.height > 0 && 
                                   window.getComputedStyle(elem).visibility !== 'hidden' &&
                                   window.getComputedStyle(elem).display !== 'none';
                        """, inp)
                        if is_visible:
                            active_input = inp
                            logging.debug("✅ 보이는 입력창 발견 (CSS selector)")
                            break
                    except:
                        continue
            except:
                pass
        
        # 4. 입력창을 못 찾았으면 원래 element 사용
        if not active_input:
            logging.warning("⚠️ 활성 입력창을 찾지 못함, 원래 element 사용")
            active_input = element
        
        # 5. 입력창에 포커스
        try:
            active_input.click()
            time.sleep(random.uniform(0.2, 0.4))
        except:
            try:
                page.run_js("arguments[0].focus();", active_input)
                time.sleep(random.uniform(0.2, 0.4))
            except:
                pass
        
        # 6. 기존 텍스트 클리어
        try:
            active_input.clear()
            time.sleep(random.uniform(0.1, 0.2))
        except:
            # clear 실패 시 JS로 시도
            try:
                page.run_js("arguments[0].value = '';", active_input)
                time.sleep(random.uniform(0.1, 0.2))
            except:
                pass
        
        # 7. 텍스트 입력
        logging.debug(f"⌨️ 텍스트 입력 시작: {text}")
        if not ENABLE_STEALTH:
            active_input.input(text)
        else:
            # 스텔스 모드: 한 글자씩 입력
            for char in text:
                active_input.input(char)
                time.sleep(random.uniform(0.05, 0.15))
        
        # 8. 입력 완료 후 대기
        time.sleep(random.uniform(0.3, 0.6))
        logging.debug(f"✅ 텍스트 입력 완료: {text}")
        
    except Exception as e:
        logging.error(f"❌ 타이핑 시뮬레이션 중 오류: {str(e)[:200]}")
        import traceback
        logging.debug(f"상세 오류:\n{traceback.format_exc()}")
        # 오류 발생 시에도 최소한 텍스트는 입력 시도
        try:
            element.input(text)
        except:
            pass

def simulate_scroll(page, scroll_count: int = 3) -> None:
    """DrissionPage 페이지 스크롤"""
    if not ENABLE_STEALTH or not SCROLL_BEHAVIOR:
        return
    for _ in range(scroll_count):
        scroll_amount = random.randint(200, 500)
        page.run_js(f"window.scrollBy(0, {scroll_amount});")
        time.sleep(random.uniform(0.3, 0.8))

def simulate_natural_scroll(page, min_actions: int = 6, max_actions: int = 12) -> None:
    """자연스러운 읽기 행동 시뮬레이션"""
    if not ENABLE_STEALTH or not SCROLL_BEHAVIOR:
        return

    try:
        scroll_h = page.run_js(
            "return Math.max(document.body.scrollHeight, document.documentElement.scrollHeight) || 0;"
        )
        view_h = page.run_js("return window.innerHeight || 0;")
        if not scroll_h or not view_h or scroll_h <= view_h + 80:
            return
    except Exception:
        return

    actions = random.randint(min_actions, max_actions)
    down_actions = max(2, int(actions * random.uniform(0.6, 0.8)))
    up_actions = max(1, actions - down_actions)

    # 아래로 스크롤
    for _ in range(down_actions):
        step = random.randint(int(view_h * 0.25), int(view_h * 0.95))
        try:
            page.run_js(
                "window.scrollBy({top: arguments[0], left: 0, behavior: 'smooth'});",
                step,
            )
        except Exception:
            page.run_js("window.scrollBy(0, arguments[0]);", step)
        time.sleep(random.uniform(0.4, 1.2))
        if random.random() < 0.25:
            time.sleep(random.uniform(0.7, 1.8))

    time.sleep(random.uniform(1.0, 2.5))

    # 위로 되돌리기
    for _ in range(up_actions):
        step = random.randint(int(view_h * 0.15), int(view_h * 0.75))
        try:
            page.run_js(
                "window.scrollBy({top: -arguments[0], left: 0, behavior: 'smooth'});",
                step,
            )
        except Exception:
            page.run_js("window.scrollBy(0, -arguments[0]);", step)
        time.sleep(random.uniform(0.35, 1.0))

    if random.random() < 0.5:
        jiggle = random.randint(-120, 120)
        page.run_js("window.scrollBy(0, arguments[0]);", jiggle)
        time.sleep(random.uniform(0.2, 0.6))

# =============================================================================
# 5) Redis 프록시 관리
# =============================================================================
def fetch_proxies_from_redis(r: redis.Redis, max_count: int = 100) -> List[ProxyInfo]:
    """Redis에서 프록시 목록 가져오기"""
    logging.info(f"🔄 [수집] Redis에서 프록시 수집 시작 (최대 {max_count}개)")
    proxies = []
    
    try:
        # alive 키에서 프록시 가져오기
        now = int(time.time())
        members = r.zrangebyscore(REDIS_ZSET_ALIVE, 0, now, start=0, num=max_count)
        
        for proxy_str in members:
            try:
                # 프록시 형식 파싱 (예: "http://user:pass@ip:port" 또는 "ip:port")
                if "://" in proxy_str:
                    protocol = proxy_str.split("://")[0]
                    address = proxy_str.split("://")[1]
                else:
                    protocol = "http"
                    address = proxy_str
                
                proxies.append(ProxyInfo(
                    protocol=protocol,
                    address=address,
                    source="redis"
                ))
            except Exception as e:
                logging.warning(f"⚠️ 프록시 파싱 실패: {proxy_str} | {e}")
                continue
        
        logging.info(f"📊 [최종] Redis에서 {len(proxies)}개의 프록시 로드 완료")
        
    except Exception as e:
        logging.error(f"❌ Redis 프록시 수집 실패: {e}")
    
    return proxies

def return_proxy_to_redis(r: redis.Redis, proxy: ProxyInfo):
    """프록시를 Redis에 반납"""
    try:
        proxy_str = f"{proxy.protocol}://{proxy.address}"
        r.zrem(REDIS_ZSET_LEASE, proxy_str)
        r.zadd(REDIS_ZSET_ALIVE, {proxy_str: int(time.time()) + 60})
        logging.info(f"🔄 프록시 반납: {proxy_str}")
    except Exception as e:
        logging.warning(f"⚠️ 프록시 반납 실패: {e}")

def tcp_quick_check(addr: str, timeout: float = 2.0) -> bool:
    return True
    try:
        host, port_s = addr.split(":", 1)
        port = int(port_s)
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False

# =============================================================================
# 6) 브라우저 생성 (StealthMobileBrowser 사용)
# =============================================================================
def make_stealth_browser(proxy: Optional[ProxyInfo], slot_id: str = "0") -> Tuple[any, any]:
    """StealthMobileBrowser를 사용하여 브라우저 생성"""
    try:
        # 프로필 선택
        region_key = random.choice(list(REGION_PROFILES.keys()))
        profile = REGION_PROFILES[region_key]
        selected_referer = random.choice(profile.get("referers", ["https://www.naver.com/"]))
        
        logging.info(f"🌐 지역: {region_key} | 유입경로: {selected_referer}")
        
        # 프록시 문자열 생성
        proxy_str = None
        if proxy:
            proxy_str = f"{proxy.protocol}://{proxy.address}"
            logging.info(f"🌐 [브라우저 생성] 프록시 적용: {proxy_str} (출처: {proxy.source})")
        
        # StealthMobileBrowser 생성
        browser_wrapper = StealthMobileBrowser(
            slot_index=int(slot_id),
            profile=profile,
            proxy=proxy_str,
            devices_dict=PLAYWRIGHT_DEVICES,
            referer=selected_referer
        )
        
        page = browser_wrapper.page
        
        
        # 타임아웃 설정 (DrissionPage는 set.timeouts 메서드 사용)
        try:
            page.set.timeouts(base=PAGELOAD_TIMEOUT_SEC, page_load=PAGELOAD_TIMEOUT_SEC)
        except:
            pass
        
        logging.info(f"✨ 브라우저 초기화 완료 (슬롯 {slot_id})")
        
        return browser_wrapper, page

    except Exception as e:
        logging.error(f"🛑 make_stealth_browser 예외: {e}")
        raise

def update_query_param(url: str, **kwargs) -> str:
    u = urlparse(url)
    q = parse_qs(u.query)
    for k, v in kwargs.items():
        q[str(k)] = [str(v)]
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))

def wait_and_mouse_click_live_more(page, timeout=20):
    # 1) 햄버거(메뉴) 아이콘: 사람 클릭
    menu = page.ele("css:img[src*='menu_ham.svg']", timeout=timeout)
    if not menu:
        logging.warning("메뉴 아이콘 못 찾음")
        return False

    # element.click / JS click 말고 actions로 클릭
    page.actions.move_to(menu).click().perform()

    # 2) 레이어 뜰 때까지 /live-more 링크가 "보일 때"까지 기다렸다가 사람 클릭
    end = time.time() + timeout
    live = None
    while time.time() < end:
        live = page.ele("css:a[href='/live-more'], css:a[href*='live-more']", timeout=1)
        if live and live.is_displayed():  # DrissionPage 요소 가시성 체크 :contentReference[oaicite:0]{index=0}
            break
        time.sleep(0.1)

    if not live:
        logging.warning("/live-more 링크 못 찾음(메뉴가 안 열렸거나 DOM이 다름)")
        return False

    page.actions.move_to(live).click().perform()
    return True

def wait_and_mouse_click_live_more_old(page, timeout=60):
    """메뉴 버튼 클릭 후 live-more 링크 클릭"""
    try:
        # 1단계: 메뉴 버튼 찾기 및 클릭
        logging.info("🔍 [1단계] 메뉴 버튼 찾기 시작...")
        
        # 메뉴 버튼 이미지를 포함하는 요소 찾기 (여러 방법 시도)
        menu_button = None
        
        # 방법 1: alt 속성으로 찾기
        try:
            menu_button = page.ele("css:img[alt='메뉴 버튼']", timeout=5)
            if menu_button:
                logging.info("✅ 메뉴 버튼 발견 (방법 1: alt 속성)")
        except:
            pass
        
        # 방법 2: src 속성으로 찾기
        if not menu_button:
            try:
                menu_button = page.ele("css:img[src*='menu_ham.svg']", timeout=5)
                if menu_button:
                    logging.info("✅ 메뉴 버튼 발견 (방법 2: src 속성)")
            except:
                pass
        
        # 방법 3: srcset 속성으로 찾기
        if not menu_button:
            try:
                menu_button = page.ele("css:img[srcset*='menu_ham.svg']", timeout=5)
                if menu_button:
                    logging.info("✅ 메뉴 버튼 발견 (방법 3: srcset 속성)")
            except:
                pass
        
        # 방법 4: XPath로 찾기
        if not menu_button:
            try:
                menu_button = page.ele("xpath://img[contains(@src, 'menu_ham.svg') or contains(@srcset, 'menu_ham.svg') or @alt='메뉴 버튼']", timeout=5)
                if menu_button:
                    logging.info("✅ 메뉴 버튼 발견 (방법 4: XPath)")
            except:
                pass
        
        if not menu_button:
            logging.warning("⚠️ 메뉴 버튼을 찾을 수 없음")
            return False
        
        # 메뉴 버튼이 클릭 가능한 부모 요소 찾기 (버튼이나 링크일 수 있음)
        clickable_element = menu_button
        try:
            # 부모 요소가 button이나 a 태그인지 확인
            parent = menu_button.parent()
            if parent and parent.tag in ['button', 'a']:
                clickable_element = parent
                logging.info(f"✅ 클릭 가능한 부모 요소 발견: <{parent.tag}>")
        except:
            pass
        
        # 메뉴 버튼 클릭 (JS 강제 클릭)
        try:
            # 방법 1: 일반 클릭 시도
            try:
                # 화면 중앙으로 스크롤
                page.run_js(
                    "arguments[0].scrollIntoView({block:'center', inline:'center'});",
                    clickable_element
                )
                time.sleep(0.3)
                clickable_element.click()
                logging.info("🔥 [1단계 성공] 메뉴 버튼 클릭 완료 (일반 클릭)")
            except Exception as e1:
                # 방법 2: JS 클릭 시도
                logging.warning(f"⚠️ 일반 클릭 실패: {str(e1)[:100]}, JS 클릭 시도...")
                page.run_js("arguments[0].click();", clickable_element)
                logging.info("🔥 [1단계 성공] 메뉴 버튼 클릭 완료 (JS 클릭)")
        except Exception as e2:
            logging.error(f"❌ 메뉴 버튼 클릭 실패: {str(e2)[:100]}")
            return False
        
        # 메뉴가 열리길 기다림
        time.sleep(2)
        random_delay(1.0, 2.0)
        
        # 2단계: /live-more 링크 찾기 및 클릭
        logging.info("🔍 [2단계] /live-more 링크 찾기 시작...")
        
        live_more_link = None
        end_time = time.time() + timeout
        
        while time.time() < end_time:
            try:
                # 방법 1: href 속성으로 정확히 찾기
                live_more_link = page.ele("css:a[href='/live-more']", timeout=2)
                if live_more_link:
                    logging.info("✅ /live-more 링크 발견 (방법 1)")
                    break
            except:
                pass
            
            try:
                # 방법 2: href에 live-more가 포함된 링크 찾기
                live_more_link = page.ele("css:a[href*='live-more']", timeout=2)
                if live_more_link:
                    logging.info("✅ /live-more 링크 발견 (방법 2)")
                    break
            except:
                pass
            
            try:
                # 방법 3: XPath로 찾기
                live_more_link = page.ele("xpath://a[contains(@href, 'live-more')]", timeout=2)
                if live_more_link:
                    logging.info("✅ /live-more 링크 발견 (방법 3)")
                    break
            except:
                pass
            
            time.sleep(0.5)
        
        if not live_more_link:
            logging.warning("⚠️ /live-more 링크를 찾을 수 없음")
            return False
        
        # /live-more 링크 클릭 (JS 강제 클릭)
        try:
            # 방법 1: 일반 클릭 시도
            try:
                # 화면 중앙으로 스크롤
                page.run_js(
                    "arguments[0].scrollIntoView({block:'center', inline:'center'});",
                    live_more_link
                )
                time.sleep(0.3)
                live_more_link.click()
                logging.info("🔥 [2단계 성공] /live-more 링크 클릭 완료 (일반 클릭)")
            except Exception as e1:
                # 방법 2: JS 클릭 시도
                logging.warning(f"⚠️ 일반 클릭 실패: {str(e1)[:100]}, JS 클릭 시도...")
                page.run_js("arguments[0].click();", live_more_link)
                logging.info("🔥 [2단계 성공] /live-more 링크 클릭 완료 (JS 클릭)")
        except Exception as e2:
            logging.error(f"❌ /live-more 링크 클릭 실패: {str(e2)[:100]}")
            return False
        
        # 페이지 로딩 대기
        time.sleep(2)
        
        logging.info("✅ [전체 성공] 메뉴 버튼 → /live-more 클릭 완료")
        return True
        
    except Exception as e:
        logging.error(f"❌ [동작 실패] wait_and_mouse_click_live_more: {str(e)[:200]}")
        import traceback
        logging.error(f"상세 오류:\n{traceback.format_exc()}")
        return False
    
BAD = "connectivitycheck.gstatic.com"

def get_with_newtab_check(page, url, page_timeout, watch_sec=2.0):
    # 현재 탭(원래 탭) 객체 + 탭 목록 스냅샷
    main_tab = page.get_tab()                 # Page가 컨트롤 중인 탭 :contentReference[oaicite:2]{index=2}
    before = set(page.tab_ids)                # 전체 탭 id 리스트 :contentReference[oaicite:3]{index=3}

    page.get(url, timeout=page_timeout)

    page.wait.ele_displayed('tag:body', timeout=page_timeout)
    
    # get() 이후 잠깐 감시: 새 탭이 뜨는지 확인
    end = time.time() + watch_sec
    while time.time() < end:
        now = set(page.tab_ids)
        new_ids = list(now - before)
        if new_ids:
            for tid in new_ids:
                tab = page.get_tab(tid)       # 새 탭 객체 얻기 :contentReference[oaicite:4]{index=4}
                tab_url = getattr(tab, "url", "") or ""
                if BAD in tab_url:
                    # 원치 않는 탭이면 닫고 :contentReference[oaicite:5]{index=5}
                    page.close_tabs(tid)
                    # 원래 탭을 다시 앞으로 :contentReference[oaicite:6]{index=6}
                    main_tab.set.activate()
                    return True  # "connectivitycheck 탭이 떴다"
            break
        time.sleep(0.05)

    return False  # 그런 탭 안 뜸    
# =============================================================================
# 7) 작업 로직
# =============================================================================
def thread_worker(task: Dict, proxy: ProxyInfo, slot_id: str, r: redis.Redis):
    keyword, target_url = task["keyword"], task["domain"]
    logging.info(f"▶️ 작업 시작 | 슬롯: {slot_id} | 키워드: [{keyword}] | 프록시: {proxy.address}")

    browser_wrapper, page = None, None
    rr = RunResult(
        datetime.now().isoformat(timespec="seconds"),
        keyword, target_url,
        proxy.protocol, proxy.address, proxy.source,
        False, None, None, None,
        False, None, None, None
    )

    try:
        # 1. TCP 체크 및 내 IP 유출 검사
        #if not tcp_quick_check(proxy.address):
        #    logging.warning(f"❌ TCP 연결 실패: {proxy.address}")
        #    rr.error = "TCP_CONNECT_FAIL"

        if is_proxy_leaking_my_ip(proxy, MY_PUBLIC_IP):
            logging.warning(f"❌ 프록시 거부 (내 공인 IP 노출됨): {proxy.address}")
            rr.error = "IP_LEAK_DETECTED"

        else:
            logging.info(f"🌐 브라우저 실행 중 (슬롯 {slot_id})")
            browser_wrapper, page = make_stealth_browser(proxy, slot_id)

            random_delay(1.0, 2.0)
            logging.info(f"🔍 네이버 접속 및 키워드 검색: [{keyword}]")
            
            # 네이버 접속
            #page.get("https://www.naver.com/", timeout=PAGELOAD_TIMEOUT_SEC)
            if get_with_newtab_check(page, "https://m.naver.com/", PAGELOAD_TIMEOUT_SEC, watch_sec=2.0) :
                raise Exception("PROXY ERROR_CONNECTIVITYCHECK")

            time.sleep(2)

            random_delay(1.5, 3.0)
            simulate_scroll(page, scroll_count=2)
            #page.actions.click('#MM_SEARCH_FAKE').click('#query').type('테스트').key_down(Keys.ENTER).key_up(Keys.ENTER)
            ##########################################################
            page.actions.click('#MM_SEARCH_FAKE').click('#query')

            text = keyword
            for ch in text:
                page.actions.type(ch)
                time.sleep(random.uniform(0.5, 0.9))  # 글자 사이 딜레이(원하는대로)

            # 엔터도 사람처럼 약간 쉬었다가
            time.sleep(random.uniform(0.12, 0.35))
            page.actions.key_down(Keys.ENTER).key_up(Keys.ENTER)
            
            ##########################################################
            # 검색 결과 페이지 대기
            page.wait.ele_displayed('tag:body', timeout=30)
            #time.sleep(3) 
            current_url = page.url
            if "search.naver.com" not in current_url:
                s = f"검색 결과 페이지로 이동하지 못함: {current_url}"
                raise Exception(s)
            
            results_url = current_url
            random_delay(2.0, 4.0)

            # 페이지 탐색
            for page_num in range(1, MAX_PAGES + 1):
                if STOP_EVENT.is_set():
                    break

                logging.info(f"📄 페이지 탐색 중 ({page_num}/{MAX_PAGES} page)")
                page.get(update_query_param(results_url, start=1 + (page_num - 1) * 10), 
                        timeout=PAGELOAD_TIMEOUT_SEC)
                random_delay(2.0, 3.5)
                simulate_scroll(page, scroll_count=3)

                # 링크 찾기
                found_data = None
                anchors = page.eles("css:a[href]")

                t_can = urlunparse((
                    urlparse(target_url).scheme,
                    urlparse(target_url).netloc,
                    urlparse(target_url).path or "/",
                    "", "", ""
                )) if target_url else None

                for idx, a in enumerate(anchors, 1):
                    try:
                        href = a.attr("href") or ""
                        if href and target_url:
                            h_can = urlunparse((
                                urlparse(href).scheme,
                                urlparse(href).netloc,
                                urlparse(href).path or "/",
                                "", "", ""
                            ))

                            if h_can.lower() == t_can.lower():
                                found_data = (idx, href, a)
                                break
                    except:
                        continue

                if found_data:
                    rank, href, elem = found_data
                    rr.found = True
                    rr.found_page = page_num
                    rr.found_rank_on_page = rank
                    rr.found_href = href
                    
                    random_delay(1.0, 2.5)

                    # 클릭
                    logging.info(f"[Slot-{slot_id}] 🔗 타겟 링크 발견 (페이지 {page_num}, 순위 {rank})")
                    
                    # 스크롤하여 요소 보이게
                    try:
                        page.run_js(
                            "arguments[0].scrollIntoView({block:'center', inline:'center'});",
                            elem
                        )
                    except:
                        pass
                    random_delay(0.3, 0.8)

                    # 클릭
                    elem.click()
                    logging.info(f"[Slot-{slot_id}] ✅ elem.click() executed")
                    time.sleep(3)

                    # 자연스러운 스크롤
                    random_delay(30.0, 60.0)
                    
                    # live-more 클릭 시도
                    wait_and_mouse_click_live_more(page)
                    
                    random_delay(30.0, 60.0)
                    simulate_natural_scroll(page)
                    random_delay(30.0, 36.0)

                    # 최종 URL 확인
                    final_url = page.url
                    h_final = urlunparse((
                        urlparse(final_url).scheme,
                        urlparse(final_url).netloc,
                        urlparse(final_url).path or "/",
                        "", "", ""
                    ))

                    if t_can and h_final.lower() == t_can.lower():
                        rr.clicked_ok = True
                        rr.final_url = final_url
                    else:
                        rr.clicked_ok = False
                        rr.final_url = final_url
                        rr.note = "FINAL_URL_NOT_MATCH"

                    break

                if page_num < MAX_PAGES:
                    random_delay(1.5, 3.0)

            if not rr.found and not rr.error:
                rr.error = "NOT_FOUND_IN_PAGES"

    except Exception as e:
        logging.error(f"💥 예외 발생: {str(e)[:100]}")
        rr.error = str(e)[:160]

    finally:
        # 브라우저 종료
        if browser_wrapper:
            try:
                browser_wrapper.quit()
            except:
                pass

        # 프록시 반납
        return_proxy_to_redis(r, proxy)

        # 결과 저장
        with FILE_LOCK:
            with open(RESULT_JSONL, "a", encoding="utf-8") as f:
                f.write(json.dumps(asdict(rr), ensure_ascii=False) + "\n")
            
            is_new = not os.path.exists(RESULT_CSV)
            with open(RESULT_CSV, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if is_new:
                    w.writerow([
                        "ts", "keyword", "target_url", "proxy_protocol", "proxy_address", "proxy_source",
                        "found", "found_page", "found_rank_on_page", "found_href",
                        "clicked_ok", "final_url", "error", "note"
                    ])
                w.writerow([
                    rr.ts, rr.keyword, rr.target_url, rr.proxy_protocol, rr.proxy_address, rr.proxy_source,
                    rr.found, rr.found_page, rr.found_rank_on_page, rr.found_href,
                    rr.clicked_ok, rr.final_url, rr.error, rr.note
                ])

        logging.info(f"🏁 작업 종료 | 슬롯: {slot_id} | 결과: {'성공' if rr.found else '실패'}")

# =============================================================================
# 8) 메인 루프
# =============================================================================
def main_loop() -> None:
    global MY_PUBLIC_IP
    setup_logging()
    logging.info("==================================================")
    logging.info("🚀 Naver Exposure Monitor 시작")
    
    # 내 공인 IP 확인
    MY_PUBLIC_IP = get_my_actual_ip()
    logging.info(f"🏠 내 공인 IP: {MY_PUBLIC_IP}")
    
    logging.info(f"⚙️ 설정: 스레드 슬롯 {MAX_THREADS}개 / 탐색 {MAX_PAGES}페이지")
    logging.info("==================================================")
    
    # Redis 연결
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        r.ping()
        logging.info("✅ Redis 연결 성공")
    except Exception as e:
        logging.error(f"❌ Redis 연결 실패: {e}")
        return
    
    proxies_cache = []
    active_threads: List[threading.Thread] = []
    
    try:
        while not STOP_EVENT.is_set():
            # 프록시 수집 (Redis에서)
            if REFRESH_PROXIES_EACH_CYCLE or not proxies_cache:
                proxies_cache = fetch_proxies_from_redis(r, max_count=MAX_PROXIES_PER_TASK * len(TASKS))
            
            if not proxies_cache:
                logging.warning("⚠️ 사용 가능한 프록시가 없습니다. 60초 후 재시도합니다.")
                time.sleep(60)
                continue

            # 각 태스크에 대해 프록시 할당
            for task in TASKS:
                for idx, proxy in enumerate(proxies_cache[:MAX_PROXIES_PER_TASK]):
                    if STOP_EVENT.is_set():
                        break
                    
                    # 활성 스레드 정리
                    active_threads = [t for t in active_threads if t.is_alive()]
                    
                    # 최대 스레드 수 대기
                    while len(active_threads) >= MAX_THREADS:
                        active_threads = [t for t in active_threads if t.is_alive()]
                        time.sleep(1)
                    
                    # 사용 가능한 슬롯 찾기
                    used_slots = set()
                    for t in active_threads:
                        if t.is_alive() and '-slot' in t.name:
                            try:
                                used_slots.add(int(t.name.split('-slot')[-1]))
                            except:
                                pass
                    
                    available_slot = None
                    for slot_num in range(MAX_THREADS):
                        if slot_num not in used_slots:
                            available_slot = slot_num
                            break
                    if available_slot is None:
                        available_slot = 0
                    
                    slot_id = str(available_slot)
                    
                    # Redis에서 프록시 임대
                    proxy_str = f"{proxy.protocol}://{proxy.address}"
                    claimed = r.eval(_LUA_CLAIM, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, 
                                   int(time.time()), 600)
                    
                    if not claimed:
                        logging.warning(f"⚠️ 프록시 임대 실패: {proxy_str}")
                        continue
                    
                    # 스레드 시작
                    t_name = f"{task['keyword']}-{idx}-slot{slot_id}"
                    t = threading.Thread(
                        target=thread_worker, 
                        args=(task, proxy, slot_id, r), 
                        name=t_name, 
                        daemon=True
                    )
                    active_threads.append(t)
                    t.start()
                    logging.info(f"➕ 새 스레드 할당: [{t_name}]")
                    time.sleep(2)  # 순차적 생성

            # 모든 스레드 완료 대기
            while any(t.is_alive() for t in active_threads):
                active_threads = [t for t in active_threads if t.is_alive()]
                time.sleep(2)
            
            logging.info(f"✅ 사이클 완료. {CHECK_INTERVAL_SECONDS}초 대기...")
            time.sleep(CHECK_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        STOP_EVENT.set()
        logging.info("🛑 프로그램 종료")
    finally:
        # 모든 활성 스레드 종료 대기
        for t in active_threads:
            if t.is_alive():
                t.join(timeout=10)

if __name__ == "__main__":
    main_loop()