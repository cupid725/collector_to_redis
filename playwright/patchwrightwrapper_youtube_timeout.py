import asyncio
import json
import random
from pathlib import Path
from redis_proxy_lease import RedisProxyLeaseClient, RedisConnConfig
from PatchrightWrapper import StealthPatchrightBrowser

# 설정 상수
TARGET_URL = "https://youtube.com/shorts/eewyMV23vXg?si=vtn1a6WMt0bDcDac"
TARGET_URL = "https://youtube.com/shorts/u7sO-mNEpT4?si=-niEKY13Q38Nqq4W"

REDIS_CONFIG = RedisConnConfig(host="127.0.0.1", port=6379)
PROFILES_PATH = Path(__file__).parent / "region_profiles_mobile.json"

# 프록시 환경에서 네비게이션이 느릴 수 있으므로, 더 길게 기다리며 재시도
NAV_TIMEOUTS_MS = [180_000, 360_000, 600_000]  # 3분 → 6분 → 10분
NAV_RETRY_SLEEP_MS = [30_000, 60_000, 120_000]    # 재시도 간격

ERROR_BODY_MARKERS = (
    "ERR_TIMED_OUT",
    "ERR_TUNNEL_CONNECTION_FAILED",
    "ERR_PROXY_CONNECTION_FAILED",
    "ERR_CONNECTION_RESET",
    "ERR_CONNECTION_CLOSED",
    "This site can’t be reached",
    "This site can't be reached",
    "사이트에 연결할 수 없음",
    "연결할 수 없습니다",
    "프록시 서버에 문제가 있습니다",
    "Proxy server is refusing connections",
)

async def has_error_in_body(page, *, timeout_ms: int = 1500) -> bool:
    """
    페이지 본문에 크롬 네트워크 에러(ERR_*) 페이지 텍스트가 있는지 빠르게 검사.
    page.content()는 느리거나 예외가 나기 쉬워서 evaluate로 innerText만 뽑음.
    """
    async def _grab_text():
        return await page.evaluate("""
            () => {
                const t = (document.title || "") + "\\n" + (document.body ? document.body.innerText : "");
                return t.slice(0, 20000);
            }
        """)

    try:
        text = await asyncio.wait_for(_grab_text(), timeout=timeout_ms / 1000)
    except Exception:
        return False

    # 대문자 ERR_ 체크가 핵심
    if "ERR_" in text:
        for m in ERROR_BODY_MARKERS:
            if m in text:
                return True
        # ERR_만 있어도 거의 크롬 에러페이지라 true 처리해도 됨(원하면)
        return True

    # ERR_가 없어도 대표 문구로 잡히는 경우
    return any(m in text for m in ERROR_BODY_MARKERS)

async def error_body_stable(page) -> bool:
    # 1번 보였다고 바로 끊지 말고, 2번 연속이면 '고정 에러'로 판단
    if not await has_error_in_body(page):
        return False
    await page.wait_for_timeout(1200)
    return await has_error_in_body(page)


async def robust_goto(page, url: str, *, referer: str | None = None, wait_until: str = "domcontentloaded"):
    """프록시에서 Page.goto 타임아웃이 잦을 때, timeout을 늘려가며 재시도.
    ⚠️ 중요한 포인트:
      - Playwright timeout 예외가 나도, 크롬 쪽 네비게이션이 '늦게' 성공하는 경우가 있음
      - 그때 about:blank로 리셋해버리면, 막 붙으려던 네비게이션을 우리가 끊어버림
    그래서:
      - timeout 계열이면 'grace' 대기 후 페이지가 정상적으로 붙었는지 확인
      - timeout 계열일 때는 about:blank 리셋을 기본적으로 하지 않음(진짜 꼬였을 때만)
    """
    last_exc: Exception | None = None
    GRACE_AFTER_TIMEOUT_MS = 20_000   # goto timeout 이후 "늦게 붙는" 케이스를 위한 여유
    RESET_ON_NON_TIMEOUT = True       # 타임아웃이 아닌 오류는 about:blank 리셋 후 재시도
    RESET_ON_TIMEOUT = False          # 타임아웃이면 기본적으로 리셋하지 않음(늦게 붙는 케이스 보호)

    def _is_timeout_error(msg: str) -> bool:
        m = msg.lower()
        return ("net::err_timed_out" in msg) or ("timeout" in m) or ("timed out" in m)

    def _is_error_page_url(u: str) -> bool:
        return u.startswith("chrome-error://") or "chromewebdata" in u

    async def _looks_navigated_ok() -> bool:
        try:
            u = page.url or ""
            if not u or u == "about:blank" or _is_error_page_url(u):
                return False
            # body가 있으면 대부분 정상 페이지
            try:
                await page.wait_for_selector("body", timeout=3_000)
                return True
            except Exception:
                # 그래도 에러 URL이 아니면 '붙었다'로 취급(사이트에 따라 body 늦을 수 있음)
                return True
        except Exception:
            return False

    for i, timeout_ms in enumerate(NAV_TIMEOUTS_MS):
        try:
            print(f"[NAV] goto attempt={i+1}/{len(NAV_TIMEOUTS_MS)} timeout={timeout_ms}ms url={url}")
            resp = await page.goto(url, wait_until=wait_until, timeout=timeout_ms, referer=referer)
            return resp
        except Exception as e:
            last_exc = e
            msg = str(e)
            is_timeout = _is_timeout_error(msg)
            print(f"[NAV] ⚠️ goto failed attempt={i+1} timeout_like={is_timeout} err={e}")

            # 1) timeout 계열이면: 늦게 붙는 케이스를 위해 grace 대기 후 상태 확인
            if is_timeout:
                try:
                    # 네비게이션이 계속 진행 중이면 이 대기에서 잡히는 경우가 있음
                    await page.wait_for_load_state(wait_until, timeout=GRACE_AFTER_TIMEOUT_MS)
                except Exception:
                    pass

                if await _looks_navigated_ok():
                    print("[NAV] ✅ timeout exception but page seems navigated; accept and continue.")
                    # 여기서 Response는 못 구할 수 있음. 호출부에서 response None도 성공으로 판단할 수 있게 보완 필요.
                    return None

            # 2) 재시도 가능하면 잠깐 쉬었다가 재시도
            retryable = is_timeout or ("net::" in msg) or ("Timeout" in msg)
            if (i < len(NAV_TIMEOUTS_MS) - 1) and retryable:
                await page.wait_for_timeout(NAV_RETRY_SLEEP_MS[min(i, len(NAV_RETRY_SLEEP_MS) - 1)])

                # 3) about:blank 리셋은 '타임아웃이 아닌 오류' 또는 '에러 페이지 URL에 있을 때'만
                #do_reset = (RESET_ON_NON_TIMEOUT and not is_timeout) or (RESET_ON_TIMEOUT and is_timeout) or _is_error_page_url(page.url or "")
                do_reset = await error_body_stable(page)
                if do_reset:
                    # ✅ 바로 blank로 끊지 말고 3~5초만 더 보고 여전히 에러면 그때 reset
                    await page.wait_for_timeout(5_000)
                    if await error_body_stable(page):
                        await page.goto("about:blank", wait_until="commit", timeout=15_000)
                    else:
                        do_reset = False
                '''
                if do_reset:
                    try:
                        await page.goto("about:blank", wait_until="commit", timeout=15_000)
                    except Exception:
                        pass
                else:
                    print("[NAV] ✅ 연결 성공.")
                    return True
                '''    
                continue
                

            raise

    raise last_exc if last_exc else RuntimeError("robust_goto failed")


async def check_bot_detected(page):
    """봇 의심 페이지 감지 로직"""
    target_link_patterns = [
        "https://support.google.com/youtube/answer/3037019",
        "/answer/3037019",
        "3037019",
        "#zippy=%2ccheck-that-youre-signed-into-youtube",
        "answer/3037019#zippy",
    ]
    
    # 1. URL 기반 즉시 체크
    current_url = page.url
    if any(pattern in current_url for pattern in target_link_patterns):
        return True
    
    # 2. 페이지 내부 a 태그 검사
    try:
        # 봇 감지 페이지는 로딩이 매우 빠르므로 잠시 대기 후 체크
        for pattern in target_link_patterns:
            if await page.locator(f"a[href*='{pattern}']").count() > 0:
                return True
    except:
        pass
        
    return False



async def handle_google_consent(page):
    if "consent" not in page.url:
        return False

    try:
        # 1. 'save'를 수행하는 form 내부를 타겟팅 (도메인 로직상 고정)
        save_form = page.locator("form[action*='/save']")
        
        # 2. 그 폼 안에 있는 버튼 중 '제출' 역할을 하는 버튼 찾기
        # 버튼 텍스트나 jsname에 의존하지 않고 HTML 표준 속성만 사용
        consent_button = save_form.locator("button, input[type='submit']").last
        
        # 3. 발견 시 스크롤 및 클릭
        await consent_button.scroll_into_view_if_needed()
        await asyncio.sleep(1)
        
        print(f"🔘 동의 폼 제출 버튼 클릭 시도")
        await consent_button.click(force=True)
        
        await page.wait_for_load_state("networkidle", timeout=10000)
        return True
    except Exception as e:
        print(f"⚠️ Consent 실패: {e}")
    return False

async def run_single_task(task_id):
    # 1. 지역 프로필 로드
    try:
        with open(PROFILES_PATH, 'r', encoding='utf-8') as f:
            profiles = json.load(f)
    except Exception as e:
        print(f"❌ 프로필 로드 실패: {e}")
        return False

    region_name = random.choice(list(profiles.keys()))
    profile = profiles[region_name]
    
    # 2. Redis 프록시 대여
    lease_client = RedisProxyLeaseClient(config=REDIS_CONFIG)
    lease_client.connect()
    proxy_url = lease_client.claim(lease_seconds=300)
    #proxy_url =  "socks5://34.124.190.108:8080" #봇페이지 뜨는 프록시
    if not proxy_url:
        print(f"[{task_id}] ❌ 사용 가능한 프록시 없음")
        lease_client.close()
        return False

    print(f"[{task_id}] 🚀 시작 | 지역: {region_name} | 프록시: {proxy_url}")

    session_ok = False
    response = None # ⭐ 에러 방지를 위해 response 변수를 미리 None으로 초기화
    
    try:
        browser = StealthPatchrightBrowser(
            proxy=proxy_url,
            headless=False,
            mobile=True,
            locale=profile["locale"],
            timezone_id=profile["timezone"],
            cleanup_user_data_dir=True
        )

        async with browser:
            page = await browser.new_page()
            

            # 프록시가 느릴 때를 대비해 기본 타임아웃을 넉넉히 설정
            page.set_default_timeout(max(NAV_TIMEOUTS_MS))
            page.set_default_navigation_timeout(max(NAV_TIMEOUTS_MS))
            # 3. 접속 시도
            try:
                response = await robust_goto(
                    page,
                    TARGET_URL,
                    wait_until="commit", # 데이터가 오기 시작하면 바로 제어권 획득
                    referer=random.choice(profile["referers"]),
                )# 4. 바디 태그가 나타날 때까지 대기 (봇 감지 페이지 확인용)
                # 봇 감지 페이지는 구조가 단순해서 매우 빨리 뜹니다.
                await page.wait_for_selector("body", timeout=1000*60)
                await asyncio.sleep(5) # 리다이렉트 대기 시간
                
            except Exception as e:
                print(f"[{task_id}] ⚠️ 페이지 이동 중 예외: {e}")

            # 5. Consent 체크
            await handle_google_consent(page)
          
            
            # 6. 봇 탐지 우선 체크 (Body 로드 후)
            if await check_bot_detected(page):
                print(f"🛑 [{task_id}] 봇 의심 페이지 감지! (URL: {page.url})")
                return False            
            
            # 7. 최종 결과 확인
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except:
                pass

            # response 변수가 할당되었는지 확인 후 상태 체크
            if (response and response.status < 400) or (page.url and "youtube.com" in page.url and not page.url.startswith("chrome-error://")):
                print(f"[{task_id}] ✅ 성공")
                await asyncio.sleep(80) # 60초 동안 브라우저 유지 및 시청
                session_ok = True
            else:
                print(f"[{task_id}] ❌ 실패 (Status: {response.status if response else 'N/A'})")

    except Exception as e:
        print(f"[{task_id}] 🔥 실행 에러: {e}")
    finally:
        lease_client.release_on_result(member=proxy_url, session_ok=session_ok)
        lease_client.close()
    return session_ok

async def main_loop():
    count = 1
    while True:
        await run_single_task(count)
        count += 1
        await asyncio.sleep(2)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("\n🛑 중단됨")