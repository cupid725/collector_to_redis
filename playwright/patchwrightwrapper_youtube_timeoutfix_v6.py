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

\
ERROR_BODY_MARKERS = (
    "ERR_TIMED_OUT",
    "ERR_TUNNEL_CONNECTION_FAILED",
    "ERR_PROXY_CONNECTION_FAILED",
    "ERR_CONNECTION_RESET",
    "ERR_CONNECTION_CLOSED",
    "This site can’t be reached",
    "This site can't be reached",
    "Proxy server is refusing connections",
    "사이트에 연결할 수 없음",
    "연결할 수 없습니다",
    "프록시 서버에 문제가 있습니다",
)

async def _get_body_probe_text(page, *, limit: int = 20000) -> str:
    """document.title + body.innerText 일부를 가져와 에러 페이지/문구 여부를 가볍게 판별."""
    try:
        js = f"""
        () => {{
            const title = document.title || "";
            const body = document.body ? document.body.innerText : "";
            const t = title + "\\n" + body;
            return t.slice(0, {int(limit)});
        }}
        """
        text = await page.evaluate(js)
        return text if isinstance(text, str) else str(text)
    except Exception:
        return ""

async def has_error_in_body(page) -> bool:
    text = await _get_body_probe_text(page)
    if not text:
        return False
    if "ERR_" in text:
        return True
    return any(m in text for m in ERROR_BODY_MARKERS)

async def error_body_stable(page, *, confirm_delay_ms: int = 1200) -> bool:
    """에러 문구가 '잠깐' 보이는 레이스를 줄이기 위해 2회 연속이면 안정적으로 에러로 판단."""
    if not await has_error_in_body(page):
        return False
    await page.wait_for_timeout(confirm_delay_ms)
    return await has_error_in_body(page)

async def robust_goto(page, url: str, *, referer: str | None = None, wait_until: str = "domcontentloaded"):
    """권장 플로우:
      1) page.goto(wait_until="commit") 으로 '커밋'만 빠르게 잡고
      2) body 등장까지 대기
      3) body 텍스트에 ERR_* 등 에러가 있으면 NAV_RETRY_SLEEP_MS[i] 만큼 기다린 뒤 다시 확인
      4) 여전히 에러면 다음 시도(최대 3회 기본)
    """
    last_exc: Exception | None = None
    BODY_WAIT_MS = 60_000

    max_tries = min(len(NAV_TIMEOUTS_MS), 3) if NAV_TIMEOUTS_MS else 3

    for i in range(max_tries):
        timeout_ms = NAV_TIMEOUTS_MS[min(i, len(NAV_TIMEOUTS_MS) - 1)] if NAV_TIMEOUTS_MS else 180_000
        try:
            print(f"[NAV] goto(commit) attempt={i+1}/{max_tries} timeout={timeout_ms}ms url={url}")
            await page.goto(url, wait_until="commit", timeout=timeout_ms, referer=referer)

            # commit 이후 body 대기
            await page.wait_for_selector("body", timeout=BODY_WAIT_MS)

            # body 에러 여부 확인
            if await has_error_in_body(page):
                print(f"[NAV] ⚠️ error marker found in body (attempt={i+1})")
                sleep_ms = NAV_RETRY_SLEEP_MS[min(i, len(NAV_RETRY_SLEEP_MS) - 1)] if NAV_RETRY_SLEEP_MS else 5_000
                await page.wait_for_timeout(sleep_ms)

                # 기다린 후에도 에러가 안정적으로 남아있는지 확인
                if await error_body_stable(page):
                    print(f"[NAV] ❌ error still present after sleep={sleep_ms}ms; will retry (attempt={i+1})")
                    continue

                print("[NAV] ✅ error cleared after sleep; accept navigation.")
                return None

            # 정상으로 보이면 추가 로드 상태를 원하면 wait_until 기준으로 한번 더 기다릴 수 있음(옵션)
            try:
                await page.wait_for_load_state(wait_until, timeout=15_000)
            except Exception:
                pass

            return None
        except Exception as e:
            last_exc = e
            print(f"[NAV] ⚠️ goto/await failed attempt={i+1}/{max_tries} err={e}")

            # 예외 후에도 body가 생겼고 에러가 아니면 '늦게 성공'으로 인정
            try:
                await page.wait_for_selector("body", timeout=10_000)
                if not await error_body_stable(page):
                    print("[NAV] ✅ exception but body is present and not stable-error; accept.")
                    return None
            except Exception:
                pass

            # 재시도 전 대기
            if i < max_tries - 1:
                sleep_ms = NAV_RETRY_SLEEP_MS[min(i, len(NAV_RETRY_SLEEP_MS) - 1)] if NAV_RETRY_SLEEP_MS else 5_000
                await page.wait_for_timeout(sleep_ms)
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
                    wait_until="domcontentloaded",
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