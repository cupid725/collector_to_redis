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
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urlunparse

import requests

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# =============================================================================
# 0) 사용자 설정 (실행 파라미터 없음: 여기만 수정)
# =============================================================================
# 창 크기/위치 설정
ENABLE_WINDOW_SIZE = True
WINDOW_WIDTH = 1280
WINDOW_HEIGHT = 900

ENABLE_WINDOW_JITTER = False     # True면 약간 랜덤 가감
WINDOW_JITTER_RANGE = 80         # -80 ~ +80

ENABLE_WINDOW_POSITION = True
WINDOW_POS_X = 50
WINDOW_POS_Y = 50


ENABLE_BLOCK_CHECK = False  # 기본 OFF (오탐 방지)

CHECK_INTERVAL_SECONDS = 60*30
MAX_PAGES = 10

# ✅ domain 값은 "정확한 URL 문자열"로 취급합니다.
TASKS = [
    {"keyword": "킹콩티비", "domain": "https://www.kingkonglive.co.kr/"},
]

MAX_PROXIES_PER_TASK = 30
REFRESH_PROXIES_EACH_CYCLE = True
RUN_HEADLESS = False

PAGELOAD_TIMEOUT_SEC = 60*2
ELEM_WAIT_SEC = 30

OUT_DIR = os.path.abspath("./naver_monitor_out")
LOG_FILE = os.path.join(OUT_DIR, "monitor.log")
RESULT_JSONL = os.path.join(OUT_DIR, "results.jsonl")
RESULT_CSV = os.path.join(OUT_DIR, "results.csv")
PROXY_CURSOR_FILE = os.path.join(OUT_DIR, "proxy_cursor.json")

STOP_EVENT = threading.Event()


# =============================================================================
# 1) 프록시 소스 URL (요청하신 7종 그대로)
# =============================================================================
HTTP_PROXY_LIST_URL = (
    "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt"
)

SOCKS5_PROXY_LIST_URL_SPEEDX = (
    "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt"
)

SOCKS5_PROXY_LIST_URL_PROXIFLY = (
    "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt"
)

VAKHOV_SOCKS4_URL = "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt"
VAKHOV_SOCKS5_URL = "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt"
VAKHOV_HTTP_URL = "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt"
VAKHOV_HTTPS_URL = "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/https.txt"


# =============================================================================
# 2) 로깅
# =============================================================================
def setup_logging() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)

    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(sh)


# =============================================================================
# 3) 데이터 모델
# =============================================================================
@dataclass
class ProxyInfo:
    protocol: str   # http / https / socks4 / socks5
    address: str    # ip:port
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

def make_fail_result(task: Dict, proxy: ProxyInfo, error: str, note: str = None) -> RunResult:
    return RunResult(
        ts=datetime.now().isoformat(timespec="seconds"),
        keyword=task["keyword"],
        target_url=task["domain"],
        proxy_protocol=proxy.protocol,
        proxy_address=proxy.address,
        proxy_source=proxy.source,
        found=False,
        found_page=None,
        found_rank_on_page=None,
        found_href=None,
        clicked_ok=False,
        final_url=None,
        error=error,
        note=note,
    )

# =============================================================================
# 4) 프록시 수집/정규화/중복제거
# =============================================================================
def _normalize_addr(line: str) -> Optional[str]:
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    if line.startswith("http://") or line.startswith("https://"):
        addr = line.split("://", 1)[1]
    else:
        addr = line

    addr = addr.split("/")[0].strip()
    if ":" not in addr:
        return None
    return addr


def fetch_text(url: str, timeout: int = 30) -> str:
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.text


def fetch_http_proxy_list(url: str) -> List[ProxyInfo]:
    proxies: List[ProxyInfo] = []
    try:
        txt = fetch_text(url)
        for line in txt.splitlines():
            addr = _normalize_addr(line)
            if not addr:
                continue
            proxies.append(ProxyInfo(protocol="http", address=addr, source="proxifly_http"))
        logging.info(f"[PROXY] HTTP 수집: {len(proxies)}개")
    except Exception as e:
        logging.warning(f"[PROXY] HTTP 수집 실패: {e}")
    return proxies


def fetch_socks5_proxy_list(url: str, source_name: str) -> List[ProxyInfo]:
    proxies: List[ProxyInfo] = []
    try:
        txt = fetch_text(url)
        for line in txt.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            addr = line.split()[0].strip()
            if ":" not in addr:
                continue
            proxies.append(ProxyInfo(protocol="socks5", address=addr, source=source_name))
        logging.info(f"[PROXY] SOCKS5 수집: {len(proxies)}개 ({source_name})")
    except Exception as e:
        logging.warning(f"[PROXY] SOCKS5 수집 실패: {e} ({source_name})")
    return proxies


def fetch_plain_proxy_list(url: str, protocol: str, source_name: str) -> List[ProxyInfo]:
    proxies: List[ProxyInfo] = []
    try:
        txt = fetch_text(url)
        for line in txt.splitlines():
            addr = _normalize_addr(line)
            if not addr:
                continue
            proxies.append(ProxyInfo(protocol=protocol, address=addr, source=source_name))
        logging.info(f"[PROXY] {protocol.upper()} 수집: {len(proxies)}개 ({source_name})")
    except Exception as e:
        logging.warning(f"[PROXY] {protocol.upper()} 수집 실패: {e} ({source_name})")
    return proxies


def fetch_all_proxies() -> List[ProxyInfo]:
    raw: List[ProxyInfo] = []

    raw += fetch_plain_proxy_list(VAKHOV_SOCKS5_URL, "socks5", "vakhov_socks5")
    raw += fetch_plain_proxy_list(VAKHOV_SOCKS4_URL, "socks4", "vakhov_socks4")
    
    raw += fetch_plain_proxy_list(VAKHOV_HTTP_URL, "http", "vakhov_http")
    raw += fetch_plain_proxy_list(VAKHOV_HTTPS_URL, "https", "vakhov_https")

    raw += fetch_http_proxy_list(HTTP_PROXY_LIST_URL)
    raw += fetch_socks5_proxy_list(SOCKS5_PROXY_LIST_URL_SPEEDX, "speedx_socks5")
    raw += fetch_socks5_proxy_list(SOCKS5_PROXY_LIST_URL_PROXIFLY, "proxifly_socks5")

    uniq: Dict[Tuple[str, str], ProxyInfo] = {}
    for p in raw:
        key = (p.protocol, p.address)
        if key not in uniq:
            uniq[key] = p

    proxies = list(uniq.values())
    logging.info(f"[PROXY] 총 프록시(중복 제거): {len(proxies)}개")
    return proxies


def load_proxy_cursor() -> int:
    try:
        if os.path.exists(PROXY_CURSOR_FILE):
            with open(PROXY_CURSOR_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return int(data.get("cursor", 0))
    except Exception:
        pass
    return 0


def save_proxy_cursor(cursor: int) -> None:
    try:
        with open(PROXY_CURSOR_FILE, "w", encoding="utf-8") as f:
            json.dump({"cursor": cursor}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning(f"[PROXY] cursor 저장 실패: {e}")


def pick_next_proxy(proxies: List[ProxyInfo], cursor: int) -> Tuple[Optional[ProxyInfo], int]:
    if not proxies:
        return None, cursor
    p = proxies[cursor % len(proxies)]
    cursor += 1
    return p, cursor


def tcp_quick_check(addr: str, timeout: float = 2.0) -> bool:
    try:
        host, port_s = addr.split(":", 1)
        port = int(port_s)
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


# =============================================================================
# 5) URL “정확 매칭(루트만)” 로직 ✅ 핵심 변경
# =============================================================================
def canonicalize_root_url(url: str) -> str:
    """
    비교용 정규화:
    - scheme/host는 소문자
    - path는 ''면 '/'로
    - query/fragment 제거
    - '루트(/)'만 허용하는 비교를 위해 path는 '/'만 유지
    """
    u = urlparse(url.strip())
    scheme = (u.scheme or "").lower()
    netloc = (u.netloc or "").lower()

    path = u.path or "/"
    if path == "":
        path = "/"

    # 루트만 허용: '/something'이면 그대로 두고 나중에 reject
    # query/fragment는 비교에서는 제거
    return urlunparse((scheme, netloc, path, "", "", ""))


def is_exact_root_target_href(href: str, target_url: str) -> bool:
    """
    ✅ 요구사항:
    - href가 타겟과 "정확히 루트 URL" 이어야 함
    - 즉, 타겟이 https://www.kingkonglive.co.kr/ 이면
      href도 scheme+host 동일 + path가 '/'(또는 '') 이어야 통과
    - /aaa 같은 하위 경로는 무조건 False
    """
    if not href or not target_url:
        return False

    try:
        h = canonicalize_root_url(href)
        t = canonicalize_root_url(target_url)

        hu = urlparse(h)
        tu = urlparse(t)

        # scheme, host 동일해야 함
        if hu.scheme != tu.scheme or hu.netloc != tu.netloc:
            return False

        # 루트만 허용
        if (hu.path or "/") != "/":
            return False
        if (tu.path or "/") != "/":
            # 타겟 자체가 /가 아니면, “정확히 그 경로”로 바꿀 수도 있으나
            # 지금 요구사항이 루트 고정이라 타겟도 /이어야 정상
            return False

        # 여기까지 오면 동일 루트
        return True
    except Exception:
        return False


# =============================================================================
# 6) 네이버 검색
# =============================================================================
NAVER_HOME = "https://www.naver.com/"


def build_proxy_server_arg(p: ProxyInfo) -> str:
    if p.protocol in ("http", "https"):
        return f"http://{p.address}"
    if p.protocol == "socks5":
        return f"socks5://{p.address}"
    if p.protocol == "socks4":
        return f"socks4://{p.address}"
    return f"http://{p.address}"


def make_driver(proxy: Optional[ProxyInfo]) -> Tuple[uc.Chrome, str]:
    profile_dir = tempfile.mkdtemp(prefix="naver_mon_profile_")

    options = uc.ChromeOptions()
    options.add_argument(f"--user-data-dir={profile_dir}")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--lang=ko-KR")

    if RUN_HEADLESS:
        options.add_argument("--headless=new")

    if proxy:
        proxy_arg = build_proxy_server_arg(proxy)
        ##############################
        #proxy_arg = "socks5://36.110.143.55:8080"
        #############################
        options.add_argument(f"--proxy-server={proxy_arg}")

    driver = uc.Chrome(options=options, use_subprocess=True)
    driver.set_page_load_timeout(PAGELOAD_TIMEOUT_SEC)
    # ✅ 창 크기/위치 적용
    try:
        if ENABLE_WINDOW_SIZE:
            w, h = WINDOW_WIDTH, WINDOW_HEIGHT
            if ENABLE_WINDOW_JITTER:
                w += random.randint(-WINDOW_JITTER_RANGE, WINDOW_JITTER_RANGE)
                h += random.randint(-WINDOW_JITTER_RANGE, WINDOW_JITTER_RANGE)
                w = max(300, w)
                h = max(300, h)
            driver.set_window_size(w, h)

        if ENABLE_WINDOW_POSITION:
            driver.set_window_position(WINDOW_POS_X, WINDOW_POS_Y)
    except Exception as e:
        logging.warning(f"[WINDOW] set size/position failed: {e}")

    return driver, profile_dir


def safe_write_debug(driver, prefix: str) -> None:
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        ddir = os.path.join(OUT_DIR, "debug")
        os.makedirs(ddir, exist_ok=True)

        png = os.path.join(ddir, f"{prefix}_{ts}.png")
        html = os.path.join(ddir, f"{prefix}_{ts}.html")

        driver.save_screenshot(png)
        with open(html, "w", encoding="utf-8") as f:
            f.write(driver.page_source or "")
    except Exception:
        pass


def looks_like_block_or_captcha(driver, context: str = "") -> bool:
    if not ENABLE_BLOCK_CHECK:
        return False
    """
    ✅ 'captchaApi' 같은 설정 문자열 때문에 오탐하지 않도록,
    '실제 캡차 UI/차단 UI가 화면에 존재/표시되는지' 위주로 판별.
    """
    def _tag(ctx: str) -> str:
        return f"[BLOCK?]{'['+ctx+']' if ctx else ''}"

    try:
        url = (driver.current_url or "")
        url_l = url.lower()
        title = (driver.title or "")

        # 1) URL이 대놓고 캡차/차단이면 바로 True
        url_hits = [k for k in ["captcha", "blocked", "denied"] if k in url_l]
        if url_hits:
            logging.warning(f"{_tag(context)} URL 키워드 감지: hits={url_hits}, url={url}, title='{title}'")
            return True

        # 2) '캡차 이미지/iframe/입력' 같은 실제 UI 요소가 화면에 표시되는지 확인
        captcha_selectors = [
            "img[src*='captcha.nid.naver.com']",
            "iframe[src*='captcha']",
            "input[name*='captcha']",
            "input[id*='captcha']",
        ]
        for sel in captcha_selectors:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if any(e.is_displayed() for e in els):
                logging.warning(f"{_tag(context)} 캡차 UI 표시 감지: selector={sel}, url={url}, title='{title}'")
                return True

        # 3) 화면에 '자동입력 방지', '로봇이 아닙니다' 같은 문구가 "가시 텍스트"로 보이면 True
        visible_text = (driver.find_element(By.TAG_NAME, "body").text or "").strip()
        text_hits = []
        for needle in ["자동입력", "자동 입력", "로봇", "비정상", "접속이 제한", "접속이 차단"]:
            if needle in visible_text:
                text_hits.append(needle)
        if text_hits:
            snippet = visible_text[:200].replace("\n", " ")
            logging.warning(f"{_tag(context)} 가시 텍스트 차단 문구 감지: hits={text_hits}, url={url}, title='{title}', text='{snippet}...'")
            return True

        # 4) 마지막 보루: 페이지 소스에 captcha 문자열이 있어도,
        #    검색 결과 컨테이너가 정상적으로 있으면 '정상'으로 간주(오탐 방지)
        #    (네이버는 설정에 captchaApi가 포함될 수 있음)
        has_results = False
        for sel in ["#main_pack", "#content", "#wrap"]:
            try:
                if driver.find_elements(By.CSS_SELECTOR, sel):
                    has_results = True
                    break
            except Exception:
                pass

        src_l = (driver.page_source or "").lower()
        if ("captcha" in src_l or "captcha.nid.naver.com" in src_l) and not has_results:
            logging.warning(f"{_tag(context)} captcha 문자열 + 결과컨테이너 부재 -> 차단 의심. url={url}, title='{title}'")
            return True

        # 정상
        return False

    except Exception as e:
        logging.warning(f"{_tag(context)} 판별 중 예외 -> 차단으로 간주: {e}")
        return True




def update_query_param(url: str, **kwargs) -> str:
    u = urlparse(url)
    q = parse_qs(u.query)
    for k, v in kwargs.items():
        q[str(k)] = [str(v)]
    new_query = urlencode(q, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))

def wait_page_fully_loaded(driver, timeout=20):
    # DOM 로딩 완료(readyState=complete)까지 대기
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )

from urllib.parse import urlparse

def assert_https_url(driver, expected_hosts: list[str], context: str):
    """
    현재 페이지가 HTTPS + 기대 host 중 하나인지 확인.
    아니면 예외 발생(=실패 처리).
    """
    cur = (driver.current_url or "").strip()
    u = urlparse(cur)
    scheme = (u.scheme or "").lower()
    host = (u.netloc or "").lower()

    if scheme != "https":
        raise RuntimeError(f"{context}_NOT_HTTPS: {cur}")

    if expected_hosts and host not in [h.lower() for h in expected_hosts]:
        raise RuntimeError(f"{context}_UNEXPECTED_HOST: {cur}")

def detect_chrome_ssl_error(driver) -> str | None:
    """
    Chrome의 '개인 정보 보호 오류(인증서 오류)' / 크롬 에러 페이지 감지.
    감지되면 에러코드 문자열을 반환, 아니면 None.
    """
    try:
        cur = (driver.current_url or "").lower()
        title = (driver.title or "").strip()

        # Chrome 내부 에러 페이지 URL
        if cur.startswith("chrome-error://") or "chromewebdata" in cur:
            src = (driver.page_source or "").lower()
            # 대표적인 SSL/인증서 에러 키워드
            for code in [
                "net::err_cert_authority_invalid",
                "net::err_cert_common_name_invalid",
                "net::err_cert_date_invalid",
                "net::err_ssl_protocol_error",
                "net::err_cert_invalid",
                "net::err_connection_closed",
            ]:
                if code in src:
                    return code.upper()
            return "CHROME_ERROR_PAGE"

        # 제목/본문으로도 한 번 더(언어/표현 바뀔 수 있음)
        if "개인 정보 보호 오류" in title or "your connection is not private" in title.lower():
            src = (driver.page_source or "").lower()
            if "net::err_cert" in src:
                # 어떤 코드인지 있으면 잡아줌
                m = re.search(r"net::err_[a-z0-9_]+", src)
                return (m.group(0) if m else "NET::ERR_CERT_*").upper()
            return "PRIVACY_ERROR_PAGE"

        return None
    except Exception:
        return None
    
def search_on_naver_home(driver, keyword: str) -> str:
    driver.get(NAVER_HOME)
    wait_page_fully_loaded(driver, timeout=PAGELOAD_TIMEOUT_SEC)

    ssl_err = detect_chrome_ssl_error(driver)
    if ssl_err:
        raise RuntimeError(f"NAVER_HOME_SSL_ERROR:{ssl_err}")

    # ✅ 네이버 메인은 반드시 HTTPS여야 함 (http면 바로 실패)
    assert_https_url(driver, expected_hosts=["www.naver.com", "naver.com"], context="NAVER_HOME")
    
    if looks_like_block_or_captcha(driver):
        raise RuntimeError("NAVER_HOME_BLOCK_OR_CAPTCHA")


    candidates = [
        (By.CSS_SELECTOR, "input#query"),
        (By.CSS_SELECTOR, "input[name='query']"),
        (By.CSS_SELECTOR, "input[type='search']"),
    ]

    box = None
    for by, sel in candidates:
        try:
            box = WebDriverWait(driver, ELEM_WAIT_SEC).until(
                EC.presence_of_element_located((by, sel))
            )
            if box:
                break
        except TimeoutException:
            continue

    if not box:
        raise RuntimeError("NAVER_SEARCHBOX_NOT_FOUND")

    box.clear()
    box.send_keys(keyword)
    box.send_keys(Keys.ENTER)

    WebDriverWait(driver, ELEM_WAIT_SEC).until(
        lambda d: "search.naver.com" in (d.current_url or "")
    )

    if looks_like_block_or_captcha(driver):
        raise RuntimeError("NAVER_SEARCH_BLOCK_OR_CAPTCHA")

    return driver.current_url


def find_target_in_current_page(driver, target_url: str) -> Optional[Tuple[int, str]]:
    """
    ✅ 변경:
    - target_url과 "정확히 루트 일치"하는 href만 인정
    - /aaa 같은 하위 경로는 제외
    """
    anchors = []
    for css in ["#main_pack a[href]", "#content a[href]", "a[href]"]:
        try:
            anchors = driver.find_elements(By.CSS_SELECTOR, css)
            if anchors:
                break
        except Exception:
            continue

    rank = 0
    for a in anchors:
        try:
            href = a.get_attribute("href") or ""
            if not href:
                continue

            # rank는 “전체 a[href]” 기준으로 카운팅(원하면 매칭 후보만 카운팅하게 바꿀 수 있음)
            rank += 1

            if is_exact_root_target_href(href, target_url):
                return rank, href
        except Exception:
            continue
    return None


def verify_click_and_open(driver, href: str, target_url: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    ✅ 변경:
    - 클릭 후 최종 URL도 “타겟 루트 URL”로 정규화했을 때 동일해야 성공
    """
    try:
        driver.get(href)
        WebDriverWait(driver, ELEM_WAIT_SEC).until(
            lambda d: (d.execute_script("return document.readyState") in ("interactive", "complete"))
        )

        if looks_like_block_or_captcha(driver):
            return False, driver.current_url, "BLOCK_OR_CAPTCHA_AFTER_CLICK"

        final_url = driver.current_url

        # 최종 URL도 루트 정확 일치해야 성공
        if final_url and is_exact_root_target_href(final_url, target_url):
            return True, final_url, None

        return False, final_url, "FINAL_URL_NOT_EXACT_ROOT_TARGET"
    except TimeoutException:
        return False, driver.current_url, "TIMEOUT_AFTER_CLICK"
    except WebDriverException as e:
        return False, driver.current_url, f"WEBDRIVER_AFTER_CLICK:{str(e)[:120]}"
    except Exception as e:
        return False, driver.current_url, f"ERROR_AFTER_CLICK:{str(e)[:120]}"


def run_one_task_with_proxy(task: Dict, proxy: ProxyInfo) -> RunResult:
    keyword = task["keyword"]
    # ✅ domain을 “정확 타겟 URL”로 사용
    target_url = task["domain"]

    driver = None
    profile_dir = ""
    results_url = None

    rr = RunResult(
        ts=datetime.now().isoformat(timespec="seconds"),
        keyword=keyword,
        target_url=target_url,
        proxy_protocol=proxy.protocol,
        proxy_address=proxy.address,
        proxy_source=proxy.source,
        found=False,
        found_page=None,
        found_rank_on_page=None,
        found_href=None,
        clicked_ok=False,
        final_url=None,
        error=None,
        note=None,
    )

    try:
        driver, profile_dir = make_driver(proxy)

        # 1) 네이버 메인 -> 검색
        results_url = search_on_naver_home(driver, keyword)

        # 2) 1~MAX_PAGES 순회
        for page in range(1, MAX_PAGES + 1):
            if STOP_EVENT.is_set():
                rr.error = "INTERRUPTED"
                return rr

            start = 1 + (page - 1) * 10
            page_url = update_query_param(results_url, start=start)

            driver.get(page_url)
            if looks_like_block_or_captcha(driver):
                rr.error = "BLOCK_OR_CAPTCHA_ON_RESULTS"
                safe_write_debug(driver, "blocked_results")
                return rr

            found = find_target_in_current_page(driver, target_url)
            if not found:
                continue

            rank_on_page, href = found
            rr.found = True
            rr.found_page = page
            rr.found_rank_on_page = rank_on_page
            rr.found_href = href

            # 3) 클릭/접속 확인
            clicked_ok, final_url, err = verify_click_and_open(driver, href, target_url)
            rr.clicked_ok = clicked_ok
            rr.final_url = final_url
            rr.note = err

            if not clicked_ok:
                safe_write_debug(driver, "click_failed")
            return rr

        rr.found = False
        rr.error = "NOT_FOUND_EXACT_ROOT_URL_IN_1_TO_10"
        return rr

    except TimeoutException:
        rr.error = "TIMEOUT"
        if driver:
            safe_write_debug(driver, "timeout")
        return rr
    except WebDriverException as e:
        rr.error = f"WEBDRIVER:{str(e)[:160]}"
        return rr
    except Exception as e:
        rr.error = f"ERROR:{str(e)[:160]}"
        if driver:
            safe_write_debug(driver, "error")
        return rr
    finally:
        try:
            if driver:
                ################
                #sleep_interruptible(5)
                ################
                driver.quit()
        except Exception:
            pass
        try:
            if profile_dir and os.path.isdir(profile_dir):
                shutil.rmtree(profile_dir, ignore_errors=True)
        except Exception:
            pass


# =============================================================================
# 7) 결과 저장
# =============================================================================
def append_jsonl(path: str, data: Dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")


def ensure_csv_header(path: str) -> None:
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "ts", "keyword", "target_url",
            "proxy_protocol", "proxy_address", "proxy_source",
            "found", "found_page", "found_rank_on_page", "found_href",
            "clicked_ok", "final_url", "error", "note"
        ])


def append_csv(path: str, rr: RunResult) -> None:
    ensure_csv_header(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            rr.ts, rr.keyword, rr.target_url,
            rr.proxy_protocol, rr.proxy_address, rr.proxy_source,
            rr.found, rr.found_page, rr.found_rank_on_page, rr.found_href,
            rr.clicked_ok, rr.final_url, rr.error, rr.note
        ])


# =============================================================================
# 8) 메인 루프 (데몬)
# =============================================================================
def sleep_interruptible(seconds: int) -> None:
    for _ in range(seconds):
        if STOP_EVENT.is_set():
            return
        time.sleep(1)


def main_loop() -> None:
    setup_logging()
    os.makedirs(OUT_DIR, exist_ok=True)

    logging.info("=" * 80)
    logging.info("NAVER 노출/링크 모니터 데몬 시작 (전체 프록시 전수 테스트, 커서/셔플 없음)")
    logging.info(f"주기: {CHECK_INTERVAL_SECONDS}s, MAX_PAGES: {MAX_PAGES}, headless={RUN_HEADLESS}")
    logging.info(f"TASKS: {len(TASKS)}개")
    logging.info("=" * 80)

    proxies_cache: List[ProxyInfo] = []

    try:
        while not STOP_EVENT.is_set():
            cycle_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logging.info(f"[CYCLE] 시작: {cycle_ts}")

            # 프록시 새로고침
            if REFRESH_PROXIES_EACH_CYCLE or not proxies_cache:
                proxies_cache = fetch_all_proxies()
                # ✅ 순서 고정: shuffle 금지
                # random.shuffle(proxies_cache)

            if not proxies_cache:
                logging.warning("[CYCLE] 프록시가 0개라 이번 사이클은 스킵")
                sleep_interruptible(CHECK_INTERVAL_SECONDS)
                continue

            # 각 task에 대해: 프록시를 성공/실패 상관없이 전부 순서대로 실행 + 전부 기록
            for task in TASKS:
                if STOP_EVENT.is_set():
                    break

                keyword = task["keyword"]
                target_url = task["domain"]
                logging.info(f"[TASK] keyword='{keyword}', target_url='{target_url}' 시작 (전체 프록시 전수 테스트, 순서 고정)")

                total = len(proxies_cache)

                for i, proxy in enumerate(proxies_cache, start=1):
                    if STOP_EVENT.is_set():
                        break

                    logging.info(f"[PROXY {i}/{total}] {proxy.protocol}://{proxy.address} ({proxy.source})")

                    # ✅ TCP 실패도 '스킵'이 아니라 '실패로 기록'
                    if not tcp_quick_check(proxy.address, timeout=2.0):
                        rr = make_fail_result(task, proxy, error="TCP_CONNECT_FAIL")
                        append_jsonl(RESULT_JSONL, asdict(rr))
                        append_csv(RESULT_CSV, rr)
                        continue

                    # ✅ 성공/실패 상관없이 항상 실행 + 기록, 성공해도 break 금지
                    rr = run_one_task_with_proxy(task, proxy)
                    append_jsonl(RESULT_JSONL, asdict(rr))
                    append_csv(RESULT_CSV, rr)

                    if rr.found and rr.clicked_ok:
                        logging.info(
                            f"[OK] (기록만) page={rr.found_page}, rank={rr.found_rank_on_page}, final={rr.final_url}"
                        )
                    else:
                        logging.info(f"[FAIL] err={rr.error}, note={rr.note}")

                logging.info(f"[TASK] 완료: keyword='{keyword}', target_url='{target_url}' (총 {total}개 프록시 전수 테스트)")

            logging.info(f"[CYCLE] 종료. 다음 실행까지 {CHECK_INTERVAL_SECONDS}s 대기")
            sleep_interruptible(CHECK_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt 감지 -> 종료합니다.")
        STOP_EVENT.set()
    finally:
        logging.info("모니터 데몬 종료 완료.")



if __name__ == "__main__":
    main_loop()
