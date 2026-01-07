import asyncio
import json
import random
from pathlib import Path
from redis_proxy_lease import RedisProxyLeaseClient, RedisConnConfig
from PatchrightWrapper import StealthPatchrightBrowser

# 설정 상수
TARGET_URL = "https://youtube.com/shorts/eewyMV23vXg?si=vtn1a6WMt0bDcDac"
REDIS_CONFIG = RedisConnConfig(host="127.0.0.1", port=6379)
PROFILES_PATH = Path(__file__).parent / "region_profiles_mobile.json"

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
    #proxy_url =  "socks5://194.163.167.32:1080"
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
            
            # 3. 접속 시도
            try:
                response = await page.goto(
                    TARGET_URL, 
                    wait_until="domcontentloaded", # 데이터가 오기 시작하면 바로 제어권 획득
                    timeout=60000*3,
                    referer=random.choice(profile["referers"])
                )
                
                # 4. 바디 태그가 나타날 때까지 대기 (봇 감지 페이지 확인용)
                # 봇 감지 페이지는 구조가 단순해서 매우 빨리 뜹니다.
                await page.wait_for_selector("body", timeout=1000*60)
                await asyncio.sleep(5) # 리다이렉트 대기 시간
                
            except Exception as e:
                print(f"[{task_id}] ⚠️ 페이지 이동 중 예외: {e}")

            # 5. 봇 탐지 우선 체크 (Body 로드 후)
            if await check_bot_detected(page):
                print(f"🛑 [{task_id}] 봇 의심 페이지 감지! (URL: {page.url})")
                return False

            # 6. Consent 체크
            await handle_google_consent(page)
            
            # 7. 최종 결과 확인
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except:
                pass

            # response 변수가 할당되었는지 확인 후 상태 체크
            if response and response.status < 400:
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