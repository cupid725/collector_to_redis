import os
import argparse
import signal
import asyncio
import time
import random
import threading
import traceback
import math
import re
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode, urlunparse, unquote
from typing import Optional
# Redis proxy lease client
from redis_proxy_lease import RedisProxyLeaseClient, RedisConnConfig
from PatchrightWrapper import StealthPatchrightBrowser

_TLS = threading.local()

def _ts() -> str:
    return time.strftime("%H:%M:%S")

def log(msg: str) -> None:
    slot = getattr(_TLS, 'slot_id', None)
    prefix = f"[Slot-{slot}] " if slot is not None else ""
    print(f"[{_ts()}] {prefix}{msg}", flush=True)

def _inc_global_click_count(n: int = 1) -> int:
    """슬롯/쓰레드 합산 성공(클릭) 횟수 카운터 증가 후 총합 반환."""
    global GLOBAL_CLICK_COUNT
    with GLOBAL_CLICK_LOCK:
        GLOBAL_CLICK_COUNT += int(n)
        return GLOBAL_CLICK_COUNT

# ✅ 기본 설정
TARGET_URL = "https://bot.sannysoft.com/"
#TARGET_URL = "https://abrahamjuliot.github.io/creepjs/"
TARGET_URL = "https://www.naver.com/"
PROXY = "154.3.236.202:3128"
# 예)
# PROXY = "http://127.0.0.1:8888"
# PROXY = "http://user:pass@host:port"
# PROXY = "socks5://host:port"

TASKS = [
    #{"keyword": "올빼미티비", "domain": "https://www.tvda.co.kr/?srt=1"},
    {"keyword": "킹콩티비", "domain": "https://www.kingkonglive.co.kr"},
]


# ===================== GLOBAL SUCCESS COUNTER =====================
GLOBAL_CLICK_COUNT = 0
GLOBAL_CLICK_LOCK = threading.Lock()


SLOT_WINDOW_LAYOUT = {}  # slot_id -> (x, y, w, h)

# ===================== Naver Search 설정 =====================
MAX_PAGES = 10
ELEM_WAIT_SEC = 30

def canonicalize_url(url: str) -> Optional[str]:
    try:
        if not url or "://" not in url:
            return None
        u = urlparse(url)
        scheme = (u.scheme or "https").lower()
        netloc = (u.netloc or "").lower()
        path = u.path or "/"
        return urlunparse((scheme, netloc, path, "", "", ""))
    except Exception:
        return None

def update_query_param(url: str, **kwargs) -> str:
    u = urlparse(url)
    q = parse_qs(u.query)
    for k, v in kwargs.items():
        q[str(k)] = [str(v)]
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))

def extract_candidate_urls(href: str) -> list:
    """
    Naver 검색 결과 href는 리다이렉트/트래킹 URL인 경우가 많아
    href 자체 + 쿼리 파라미터(url/u/r/q 등) + 퍼센트 인코딩된 URL까지 후보로 추출한다.
    """
    cands = []
    if not href:
        return cands

    # 1) href 자체
    cands.append(href)

    # 2) query param 후보
    try:
        u = urlparse(href)
        qs = parse_qs(u.query)
        for key in ("url", "u", "r", "q", "target", "to"):
            for v in qs.get(key, []):
                v = unquote(v)
                if v.startswith("http://") or v.startswith("https://"):
                    cands.append(v)
    except Exception:
        pass

    # 3) 퍼센트 인코딩 URL 패턴 추출
    try:
        for m in re.findall(r"https?%3A%2F%2F[^&]+", href):
            v = unquote(m)
            if v.startswith("http://") or v.startswith("https://"):
                cands.append(v)
    except Exception:
        pass

    # 중복 제거(순서 유지)
    seen = set()
    uniq = []
    for x in cands:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq

def href_matches_target(href: str, target_url: str) -> bool:
    t_can = canonicalize_url(target_url)
    if not t_can:
        return False

    # 후보 URL들 중 하나라도 canonical이 target canonical과 같으면 매칭
    for cand in extract_candidate_urls(href):
        h_can = canonicalize_url(cand)
        if h_can and h_can.lower() == t_can.lower():
            return True

        # netloc 포함만으로도 강한 힌트(트래킹 URL이 target을 포함하는 케이스)
        try:
            t_netloc = urlparse(t_can).netloc.lower()
            if t_netloc and t_netloc in cand.lower():
                return True
        except Exception:
            pass

    return False

async def human_scroll_to_locator(page, loc, max_steps: int = 20):
    """
    locator가 화면에 보이도록 '휠'로 조금씩 스크롤해서 접근.
    - scroll_into_view_if_needed 같은 순간 점프를 피함
    """
    # locator의 bounding box를 얻기 위해 몇 번 트라이
    box = None
    for _ in range(3):
        try:
            box = await loc.bounding_box()
            if box:
                break
        except Exception:
            pass
        await asyncio.sleep(random.uniform(0.05, 0.12))

    if not box:
        # box를 못 얻으면 최소한의 fallback (그냥 조금 스크롤)
        for _ in range(5):
            await page.mouse.wheel(0, random.randint(200, 520))
            await asyncio.sleep(random.uniform(0.08, 0.18))
        return

    # 현재 뷰포트 중앙 근처로 끌어오기 위해 여러 번 휠
    viewport = page.viewport_size or {"width": 1280, "height": 720}
    target_y = box["y"] + box["height"] * 0.5
    center_y = viewport["height"] * 0.45  # 살짝 위쪽에 멈추는 느낌

    # 화면 밖이면 delta가 커지고, 가까우면 작게 움직이도록
    for _ in range(max_steps):
        # box는 스크롤 후 바뀌므로 갱신
        try:
            box = await loc.bounding_box()
        except Exception:
            box = None

        if not box:
            break

        target_y = box["y"] + box["height"] * 0.5
        delta = target_y - center_y

        # 충분히 근접하면 멈춤
        if abs(delta) < 80:
            break

        # 한 번에 너무 많이 안 움직이게 제한 + 랜덤성
        step = int(max(-700, min(700, delta * 0.65)))
        step += random.randint(-60, 60)

        await page.mouse.wheel(0, step)
        await asyncio.sleep(random.uniform(0.08, 0.22))

    # 마지막 미세조정(사람이 한 번 더 살짝 휠 하는 느낌)
    if random.random() < 0.6:
        await page.mouse.wheel(0, random.randint(-80, 140))
        await asyncio.sleep(random.uniform(0.06, 0.16))



async def wait_for_naver_search_box(page, timeout_ms: int = 20000):
    """네이버 메인에서 검색 입력창(또는 검색 UI)이 나타날 때까지 대기."""
    selectors = [
        "input#query",
        "input[name='query']",
        "div.search_input_box input",
        "input.search_input",
        "input[placeholder*='검색']",
        "input[title*='검색']",
        "input[aria-label*='검색']",
    ]
    sel = ", ".join(selectors)
    try:
        await page.wait_for_selector(sel, timeout=timeout_ms, state="visible")
        return True
    except Exception:
        return False


async def wait_for_naver_results_page(page, timeout_ms: int = 30000) -> bool:
    """
    검색 결과 페이지 '도착' 판별:
    - wait_for_url(search.naver.com...) 대신, 결과 페이지 pagination(1번) 또는 결과 컨테이너를 기다림.
    """
    selectors = [
        # pagination '1' (현재 페이지)
        'a.btn[role="button"][aria-pressed="true"]:has-text("1")',
        'a.btn[aria-pressed="true"]:has-text("1")',
        'a[aria-current="page"]:has-text("1")',
        # 결과 컨테이너(레이아웃이 바뀌어도 대체로 존재)
        "div#content",
        "div.api_subject_bx",
        "div.site_name",
    ]
    sel = ", ".join(selectors)
    try:
        await page.wait_for_selector(sel, timeout=timeout_ms, state="attached")
        return True
    except Exception:
        return False

async def naver_search_and_click(page, keyword: str, target_url: str) -> dict:
    """
    네이버 접속 → 검색 → 결과 페이지에서 target_url 도메인 클릭
    반환: dict(found, clicked, page, rank, href, final_url)
    """
    result = {
        "found": False,
        "clicked": False,
        "page": None,
        "rank": None,
        "href": None,
        "final_url": None,
        "note": None,
    }

    # 1) 네이버 접속
    log(f"[NAVER] goto https://www.naver.com/")
    #await page.goto("https://www.naver.com/", wait_until="domcontentloaded", timeout=60000*2)
    await page.goto("https://www.naver.com/", wait_until="commit", timeout=60000*2)

    # 2) 검색창 찾고 검색
    log(f"[NAVER] search keyword='{keyword}'")
    search_selectors = [
        "input#query",
        "input[name='query']",
        "div.search_input_box input",
        "input.search_input",
        "input[placeholder*='검색']",
        "input[title*='검색']",
        "input[aria-label*='검색']",
    ]

    # ✅ 1) 메인 페이지에서 기다려서 찾기 (OR 셀렉터)
    box = None
    try:
        box = await page.wait_for_selector(", ".join(search_selectors), timeout=15_000)
    except Exception:
        box = None

    # ✅ 2) 그래도 없으면 frame 안까지 뒤지기
    if not box:
        for fr in page.frames:
            try:
                box = await fr.wait_for_selector(", ".join(search_selectors), timeout=2_000)
                if box:
                    break
            except Exception:
                continue

    if not box:
        log(f"[NAVER] ❌ search box not found | url={page.url}")
        # 디버깅: input들 뭐가 있는지 찍어보기
        try:
            inputs = await page.evaluate("""
            () => [...document.querySelectorAll('input')].slice(0, 30).map(i => ({
            id: i.id, name: i.name, type: i.type, cls: i.className, placeholder: i.placeholder, title: i.title
            }))
            """)
            log(f"[NAVER] inputs(sample)={inputs}")
        except Exception as e:
            log(f"[NAVER] inputs dump fail: {e}")
        result["note"] = "SEARCH_BOX_NOT_FOUND"
        return result

    box = page.locator("input#query")
    await box.wait_for(state="visible", timeout=15000)
    await box.click()
    await box.press_sequentially(keyword, delay=random.randint(160, 320))
    await box.press("Enter")

    # 3) 결과 페이지 도착 대기
    ok = await wait_for_naver_results_page(page, timeout_ms=ELEM_WAIT_SEC * 1000 * 2)
    if not ok:
        log(f"[NAVER] ❌ results page not detected within {ELEM_WAIT_SEC}s | url={page.url} title={await page.title()}")
        raise TimeoutError(f"results page wait timeout ({ELEM_WAIT_SEC}s)")
    results_url = page.url
    log(f"[NAVER] results_url={results_url}")

    async def _go_to_page_by_click(target_page: int) -> bool:
        """
        네이버 검색 결과에서 페이지 번호/다음 버튼을 '클릭'해서 이동.
        - Naver DOM/클래스가 바뀔 수 있어 여러 셀렉터를 순차 시도
        - 요구사항상 URL 직접 이동은 하지 않음(실패 시 False 반환)
        """
        if target_page <= 1:
            return True

        # 현재 페이지 추정(활성 페이지 숫자)
        cur = 1
        try:
            for sel in [
                "div.sc_page_inner strong",
                "div.sc_page_inner a[aria-current='page']",
                "a[aria-current='page']",
                "strong[aria-current='page']",
            ]:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    t = (await loc.first.inner_text()).strip()
                    if t.isdigit():
                        cur = int(t)
                        break
        except Exception:
            cur = 1

        if cur == target_page:
            return True

        # 1) 페이지 숫자 링크 직접 클릭 시도(현재 화면에 있을 때)
        try:
            # 페이지네이션 영역 우선 탐색
            candidates = [
                f"div.sc_page_inner a:has-text('{target_page}')",
                f"a:has-text('{target_page}')",
            ]
            for sel in candidates:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    #a = loc.first
                    #await a.scroll_into_view_if_needed(timeout=3000)
                    #await a.click()
                    a = loc.first
                    # ✅ 사람처럼 휠로 스크롤해서 근처까지 접근
                    await human_scroll_to_locator(page, a, max_steps=18)
                    # (선택) 마우스 올리고 약간 머뭇
                    try:
                        await a.hover()
                    except Exception:
                        pass
                    await asyncio.sleep(random.uniform(0.12, 0.35))

                    # ✅ 클릭도 약간 딜레이
                    await a.click(delay=random.randint(30, 90))

                    try:
                        #await page.wait_for_load_state("domcontentloaded", timeout=60000)
                        await page.wait_for_load_state("commit", timeout=60000)
                    except Exception:
                        pass
                    return True
        except Exception:
            pass

        # 2) 다음 버튼 반복 클릭
        steps = max(0, target_page - cur)
        next_selectors = [
            "a.btn_next",
            "a[aria-label*='다음']",
            "a:has-text('다음')",
            "button:has-text('다음')",
        ]

        for _ in range(steps):
            clicked = False
            for sel in next_selectors:
                loc = page.locator(sel)
                try:
                    if await loc.count() > 0:
                        btn = loc.first
                        await human_scroll_to_locator(page, btn, max_steps=12)
                        try:
                            await btn.hover()
                        except Exception:
                            pass
                        await asyncio.sleep(random.uniform(0.10, 0.28))
                        await btn.click(delay=random.randint(30, 90))
                        clicked = True
                        try:
                            #await page.wait_for_load_state("domcontentloaded", timeout=60000)
                            await page.wait_for_load_state("commit", timeout=60000)
                        except Exception:
                            pass
                        break
                except Exception:
                    continue
            if not clicked:
                return False

        return True


    # 4) 페이지 순회하며 target 링크 찾기/클릭
    for p in range(1, MAX_PAGES + 1):
        if p == 1:
            log(f"[NAVER] scan page {p}/{MAX_PAGES} (current)")
        else:
            log(f"[NAVER] move to page {p}/{MAX_PAGES} by CLICK (no direct url nav)")
            ok = await _go_to_page_by_click(p)
            if not ok:
                log(f"[NAVER] ❌ failed to move to page {p} by click. stop scanning.")
                break
            log(f"[NAVER] scan page {p}/{MAX_PAGES} url={page.url}")

        anchors = page.locator("a[href]")
        try:
            total = await anchors.count()
        except Exception:
            total = 0

        # 너무 많은 a를 전부 돌면 느려질 수 있어 상한을 둠(필요시 늘려도 됨)
        limit = min(total, 600)

        for i in range(limit):
            a = anchors.nth(i)
            href = None
            try:
                href = await a.get_attribute("href")
            except Exception:
                continue

            if not href:
                continue

            if href_matches_target(href, target_url):
                result["found"] = True
                result["page"] = p
                result["rank"] = i + 1
                result["href"] = href
                log(f"[NAVER] ✅ found target on page={p} rank={i+1} href={href}")

                # 클릭 (새 탭/같은 탭 모두 대응) - ✅ 사람처럼 스크롤/호버/딜레이 클릭
                try:
                    await human_scroll_to_locator(page, a, max_steps=18)
                    try:
                        await a.hover()
                    except Exception:
                        pass
                    await asyncio.sleep(random.uniform(0.12, 0.35))
                except Exception:
                    pass

                ctx = page.context
                before_pages = list(ctx.pages)

                clicked_page = None
                try:
                    async with ctx.expect_page(timeout=2500) as pi:
                        await a.click(delay=random.randint(30, 90))
                    clicked_page = await pi.value
                    #await clicked_page.wait_for_load_state("domcontentloaded", timeout=60000)
                    await clicked_page.wait_for_load_state("commit", timeout=60000)
                    log(f"[NAVER] click opened new page url={clicked_page.url}")
                except Exception:
                    # same tab navigate
                    try:
                        await page.wait_for_load_state("commit", timeout=60000)
                    except Exception:
                        pass
                    clicked_page = page
                    log(f"[NAVER] click stayed in same page url={clicked_page.url}")

                result["clicked"] = True
                result["final_url"] = clicked_page.url
                return result

    result["note"] = "NOT_FOUND_IN_PAGES"
    log("[NAVER] ❌ target not found within pages")
    return result

# ===================== Redis 설정 (proxy lease) =====================
# (Redis 관련 로직은 redis_proxy_lease.py 의 RedisProxyLeaseClient로 모듈화)
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None




def _set_slot(slot_id: int):
    _TLS.slot_id = slot_id


def init_window_layout(args) -> None:
    """
    처음 실행 시 슬롯별 '브라우저 영역'을 고정으로 산정해 SLOT_WINDOW_LAYOUT에 저장.
    - 슬롯이 재실행되어도 같은 위치를 유지한다.
    - 해상도 탐지 없이 tile_w/tile_h/cols 기준으로 계산한다.
    """
    global SLOT_WINDOW_LAYOUT
    SLOT_WINDOW_LAYOUT = {}

    if args.slots <= 0:
        return

    # cols 자동: 지정값 우선, 없으면 sqrt 기반으로 적당히 배치
    cols = int(args.tile_cols) if int(args.tile_cols) > 0 else int(math.ceil(math.sqrt(args.slots)))
    cols = max(1, cols)

    w = int(args.tile_w)
    h = int(args.tile_h)
    gap = int(getattr(args, "tile_gap", 8))

    for slot_id in range(args.slots):
        col = slot_id % cols
        row = slot_id // cols
        x = col * (w + gap)
        y = row * (h + gap)
        SLOT_WINDOW_LAYOUT[slot_id] = (x, y, w, h)

    # 로그
    for slot_id, (x, y, w, h) in SLOT_WINDOW_LAYOUT.items():
        log(f"[WIN] reserved area slot={slot_id} x={x} y={y} w={w} h={h} cols={cols} gap={gap}")

async def run_one_session(slot_id: int, args) -> None:
    """
    슬롯 1회 세션:
    - (옵션) Redis에서 프록시 1개 claim
    - Patchright browser 실행
    - TASKS 수행 (네이버 검색→도메인 클릭)
    - dwell 후 종료
    - Redis release/ban 처리
    """
    _set_slot(slot_id)

    redis_client: Optional[RedisProxyLeaseClient] = None
    proxy_member: Optional[str] = None
    session_ok = False
    local_proxy = args.proxy

    # (옵션) Redis에서 프록시 임대
    if args.proxy_from_redis:
        log(f"[REDIS] connecting host={args.redis_host}:{args.redis_port} db={args.redis_db} auth={'yes' if bool(args.redis_password) else 'no'}")
        try:
            redis_client = RedisProxyLeaseClient(
                RedisConnConfig(
                    host=args.redis_host,
                    port=int(args.redis_port),
                    db=int(args.redis_db),
                    password=args.redis_password,
                )
            )
            redis_client.connect()
            log("[REDIS] ping=OK")
        except Exception as e:
            log(f"[REDIS] ping=FAIL: {type(e).__name__}: {e}")
            return

        proxy_member = redis_client.claim(lease_seconds=int(args.lease_seconds), reclaim_limit=200, sample_k=50)
        if not proxy_member:
            log("[REDIS] 사용 가능한 프록시가 없어 종료함.")
            try:
                redis_client.close()
            except Exception:
                pass
            return

        local_proxy = proxy_member
        log(f"[REDIS] ✅ proxy claimed: {proxy_member}")
        log(f"[REDIS] lease_seconds={args.lease_seconds} (member is expected to be like proto://ip:port)")

    try:
        log(f"[RUN] proxy_in_use={local_proxy}")

        # ✅ 슬롯별 창 배치(데스크톱에서만)
        extra_args = []
        if (not args.headless):
            if args.mobile:
                log("[WIN] tile_windows requested with mobile=True (best-effort; may be ignored by some setups)")
            w = int(args.tile_w)
            h = int(args.tile_h)
            cols = int(args.tile_cols) if args.tile_cols > 0 else max(1, args.slots)
            x = (slot_id % cols) * w
            y = (slot_id // cols) * h

            extra_args = [f"--window-size={w},{h}", f"--window-position={x},{y}"]
            log(f"[WIN] tile window pos=({x},{y}) size=({w},{h}) cols={cols}")


        browser = StealthPatchrightBrowser(
            proxy=local_proxy,
            webrtc_leak_protection=True,
            headless=args.headless,
            mobile=args.mobile,
            cleanup_user_data_dir=not args.keep_profile,
            extra_args=extra_args,
        )

        async with browser:
            page = await browser.new_page()
            # ✅ 사용자가 브라우저/탭을 닫으면 즉시 세션 종료되도록 감시
            closed_evt = asyncio.Event()
            try:
                page.on("close", lambda: closed_evt.set())
            except Exception:
                pass
            if getattr(browser, "selected_device_name", None):
                log(f"[DEVICE] selected={browser.selected_device_name}")
            else:
                log("[DEVICE] selected=(none)")

            # 최초 진입 URL
            t0 = time.time()
            log(f"[NAV] goto start wait_until=commit timeout={60000*3}ms url={args.url}")
            # ✅ 프록시가 느릴 수 있으니 'commit'까지만 기다리고, 필요한 UI(검색창)가 뜰 때까지 별도 대기
            await page.goto(args.url, wait_until="commit", timeout=60000*3)

            ok = await wait_for_naver_search_box(page, timeout_ms=60000)
            if not ok:
                log(f"[NAV] ⚠️ search box not visible within 20s (proxy slow or different page). url={page.url}")
            log(f"[NAV] goto done elapsed={time.time()-t0:.2f}s")
            log(f"[OK] 접속 완료: {args.url}")

            # ✅ TASKS 실행
            any_clicked = False
            if TASKS:
                for idx, task in enumerate(TASKS, 1):
                    kw = task.get("keyword", "")
                    dom = task.get("domain", "")
                    log(f"[TASK] {idx}/{len(TASKS)} keyword='{kw}' domain='{dom}'")
                    try:
                        res = await naver_search_and_click(page, kw, dom)
                        log(f"[TASK] result found={res.get('found')} clicked={res.get('clicked')} page={res.get('page')} rank={res.get('rank')} final_url={res.get('final_url')}")
                        if res.get('clicked'):
                            total = _inc_global_click_count(1)
                            log(f"[COUNT] ✅ click success +1 -> total={total}")
                            any_clicked = True
                    except Exception as e:
                        log(f"[TASK] ❌ exception: {type(e).__name__}: {e}")
                        log(traceback.format_exc())

            # ✅ 정상적으로 작업이 끝나면(링크 클릭까지 완료) 10초 대기 후 세션 종료
            wait_seconds = 10 if any_clicked else int(args.dwell_seconds)
            if wait_seconds > 0:
                if any_clicked:
                    log("[WAIT] task clicked -> 10초 대기 후 세션 종료")
                else:
                    log(f"[WAIT] {wait_seconds}초 대기... (브라우저를 닫으면 즉시 다음 세션)")
                try:
                    # page가 닫히면 wait가 즉시 풀림
                    if not page.is_closed():
                        await asyncio.wait_for(closed_evt.wait(), timeout=wait_seconds)
                        log("[WAIT] page closed by user -> end session now")
                except asyncio.TimeoutError:
                    pass
                except Exception:
                    # page 상태 확인 중 예외가 나면 세션 종료로 간주
                    pass

        session_ok = True
        log("[RUN] session_ok=True")
        log(f"[성공횟수] current_total={GLOBAL_CLICK_COUNT}")

    except Exception as e:
        log(f"[ERR] 실행 중 예외: {type(e).__name__}: {e}")
        log(traceback.format_exc())

    finally:
        # ✅ Redis 반납(성공/실패에 따라 cooldown/ban 처리)
        if redis_client and proxy_member:
            info = redis_client.release_on_result(
                proxy_member,
                session_ok=session_ok,
                cooldown_success=int(args.cooldown_success),
                cooldown_fail_base=int(args.cooldown_fail_base),
                cooldown_fail_jitter=int(args.cooldown_fail_jitter),
                max_fail=int(args.max_fail),
            )
            if info.get("action") == "banned":
                log(f"[REDIS] ⛔ proxy banned (fails={info.get('fails')}): {proxy_member}")
            else:
                if session_ok:
                    log(f"[REDIS] 🔓 proxy released (ok): {proxy_member}")
                else:
                    log(f"[REDIS] 🔓 proxy released (fail={info.get('fails')}, cooldown={info.get('cooldown')}s): {proxy_member}")
            try:
                redis_client.close()
            except Exception:
                pass



def _thread_entry(slot_id: int, args, stop_event: threading.Event):
    """
    요구사항:
      - slot=N이면 slot[0..N-1] 각각에 '쓰레드'를 하나 띄움
      - 쓰레드가 끝나면(1회 세션 종료) 해당 슬롯에 '새 쓰레드'를 만들어 다시 채움
      - 이를 반복
    """
    _set_slot(slot_id)
    if stop_event.is_set():
        return
    try:
        asyncio.run(run_one_session(slot_id, args))
    except Exception as e:
        log(f"[THREAD] fatal: {type(e).__name__}: {e}")
        log(traceback.format_exc())


def run_slot_supervisor(args):
    """
    메인 스레드에서 슬롯 상태를 감시하며,
    빈 슬롯이 생기면 새 쓰레드를 만들어 채운다.
    """
    stop_event = threading.Event()

    # slot_id -> (thread, run_count)
    threads = {i: None for i in range(args.slots)}
    run_counts = {i: 0 for i in range(args.slots)}

    def _spawn(slot_id: int):
        run_counts[slot_id] += 1
        t = threading.Thread(
            target=_thread_entry,
            args=(slot_id, args, stop_event),
            name=f"slot-{slot_id}-run-{run_counts[slot_id]}",
            daemon=True,
        )
        threads[slot_id] = t
        log(f"[SUP] spawn thread slot={slot_id} run={run_counts[slot_id]}")
        t.start()

    # 초기 스폰
    for i in range(args.slots):
        _spawn(i)

    try:
        while True:
            # 종료 조건: cycles > 0 이면 각 슬롯이 cycles번 세션 돌면 종료
            if args.cycles > 0:
                done_slots = [i for i in range(args.slots) if run_counts[i] >= args.cycles and threads[i] and (not threads[i].is_alive())]
                if len(done_slots) == args.slots:
                    log("[SUP] all slots completed requested cycles. stop.")
                    break

            # 슬롯 감시 & 재스폰
            for i in range(args.slots):
                t = threads[i]
                if t is None:
                    _spawn(i)
                    continue

                if not t.is_alive():
                    # cycles 제한이 있으면, 다 찼으면 재스폰 안함
                    if args.cycles > 0 and run_counts[i] >= args.cycles:
                        continue
                    _spawn(i)

            time.sleep(0.5)

    except KeyboardInterrupt:
        log("[SUP] KeyboardInterrupt. stopping ALL...")
        stop_event.set()
        # ✅ Ctrl+C 즉시 전체 프로세스 종료(쓰레드/플레이wright 정리 대기 없이)
        os._exit(0)

    finally:
        stop_event.set()
        # 현재 실행중인 쓰레드들 join
        for i in range(args.slots):
            t = threads[i]
            if t and t.is_alive():
                log(f"[SUP] join slot={i} ...")
                t.join(timeout=10)
        log("[SUP] done.")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=TARGET_URL)
    parser.add_argument("--proxy", default=PROXY)
    parser.add_argument("--mobile", action="store_true", help="모바일(Android) 디바이스만 랜덤 선택")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--keep-profile", action="store_true", help="자동 생성 user_data_dir 삭제하지 않음")

    # ✅ Redis에서 프록시 claim/release 사용
    parser.add_argument("--proxy-from-redis", action="store_true", help="Redis에서 프록시를 하나 임대해서 사용 후 반납")
    parser.add_argument("--redis-host", default=REDIS_HOST)
    parser.add_argument("--redis-port", type=int, default=REDIS_PORT)
    parser.add_argument("--redis-db", type=int, default=REDIS_DB)
    parser.add_argument("--redis-password", default=REDIS_PASSWORD)

    # 운영 파라미터
    parser.add_argument("--lease-seconds", type=int, default=300)
    parser.add_argument("--cooldown-success", type=int, default=0)
    parser.add_argument("--cooldown-fail-base", type=int, default=30)
    parser.add_argument("--cooldown-fail-jitter", type=int, default=60)
    parser.add_argument("--max-fail", type=int, default=5)

    # ✅ 슬롯/스레드 옵션
    parser.add_argument("--slots", type=int, default=2, help="동시에 돌릴 슬롯(쓰레드) 수")
    parser.add_argument("--cycles", type=int, default=0, help="각 슬롯이 실행할 세션 횟수(0이면 무한 반복)")

    # ✅ 대기(기존 동작 유지: 기본 120초)
    parser.add_argument("--dwell-seconds", type=int, default=120)

    # ✅ 창 타일 배치(데스크톱에서만)


    parser.add_argument("--tile-w", type=int, default=960)
    parser.add_argument("--tile-h", type=int, default=900)
    parser.add_argument("--tile-gap", type=int, default=8, help="슬롯 창 사이 간격(px)")
    parser.add_argument("--tile-cols", type=int, default=0, help="타일 컬럼 수(0이면 slots 사용)")

    args = parser.parse_args()
    # ✅ 항상 창 타일 배치 사용(옵션 제거)
    args.tile_windows = True

    # ✅ Ctrl+C 즉시 종료
    try:
        signal.signal(signal.SIGINT, lambda sig, frame: os._exit(0))
    except Exception:
        pass

    # main thread slot id None
    _TLS.slot_id = None
    log(f"[BOOT] url={args.url} | slots={args.slots} cycles={args.cycles} | mobile={args.mobile} | headless={args.headless} | keep_profile={args.keep_profile} | proxy_from_redis={args.proxy_from_redis}")

    # 슬롯이 1이면(단일) 기존처럼 한 번만 실행하고 종료(단, cycles=0이면 무한)
    if args.slots <= 1:
        if args.cycles <= 0:
            # 무한 반복ㄱ
            n = 0
            while True:
                n += 1
                log(f"[SUP] single-slot loop n={n}")
                await run_one_session(0, args)
        else:
            for n in range(1, args.cycles + 1):
                log(f"[SUP] single-slot cycle {n}/{args.cycles}")
                await run_one_session(0, args)
        return

    # slots>1이면 supervisor는 동기(메인 스레드에서 감시)
    run_slot_supervisor(args)


if __name__ == "__main__":
    asyncio.run(main())
