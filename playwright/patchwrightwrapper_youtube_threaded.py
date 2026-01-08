import asyncio
import json
import random
import threading
import signal
import sys
from pathlib import Path
from redis_proxy_lease import RedisProxyLeaseClient, RedisConnConfig
from PatchrightWrapper import StealthPatchrightBrowser
from patchright_human_events import HumanEvent, HumanEventMobile

# 설정 상수
TARGET_URL = "https://youtube.com/shorts/u7sO-mNEpT4?si=-niEKY13Q38Nqq4W" #크리스마스 2
TARGET_URL = "https://youtube.com/shorts/eewyMV23vXg?si=vtn1a6WMt0bDcDac" #새해

REDIS_CONFIG = RedisConnConfig(host="127.0.0.1", port=6379)
PROFILES_PATH = Path(__file__).parent / "region_profiles_mobile.json"

# 슬롯 설정
SLOT_NUM = 2
SLOT_POSITIONS = [
    {"x": 10, "y": 10},   # 슬롯 0
    {"x": 700, "y": 30},  # 슬롯 1
    {"x": 1000, "y": 50},  # 슬롯 2
]

# 슬롯 상태 관리
slot_threads = [None] * SLOT_NUM
slot_lock = threading.Lock()
shutdown_event = threading.Event()  # 전역 종료 이벤트

# ✅ 전역 성공 카운터 (쓰레드 모두 합산)
success_lock = threading.Lock()
total_success = 0


def inc_success_and_print(task_id: str):
    """모든 슬롯/쓰레드 합산 성공 카운트 + 콘솔 출력"""
    global total_success
    with success_lock:
        total_success += 1
        print(f"[{task_id}] ✅ GLOBAL SUCCESS +1  => total_success={total_success}")


async def check_bot_detected(page):
    """봇 의심 페이지 감지 로직"""
    target_link_patterns = [
        "https://support.google.com/youtube/answer/3037019",
        "/answer/3037019",
        "3037019",
        "#zippy=%2ccheck-that-youre-signed-into-youtube",
        "answer/3037019#zippy",
    ]
    
    current_url = page.url
    if any(pattern in current_url for pattern in target_link_patterns):
        return True
    
    try:
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
        save_form = page.locator("form[action*='/save']")
        consent_button = save_form.locator("button, input[type='submit']").last
        
        await consent_button.scroll_into_view_if_needed()
        await asyncio.sleep(1)
        
        print(f"📘 동의 폼 제출 버튼 클릭 시도")
        await consent_button.click(force=True)
        
        await page.wait_for_load_state("networkidle", timeout=10000)
        return True
    except Exception as e:
        print(f"⚠️ Consent 실패: {e}")
    return False


ERROR_BODY_MARKERS = (
    "ERR_TIMED_OUT",
    "ERR_TUNNEL_CONNECTION_FAILED",
    "ERR_PROXY_CONNECTION_FAILED",
    "ERR_CONNECTION_RESET",
    "ERR_CONNECTION_CLOSED",
    "This site can't be reached",
    "Proxy server is refusing connections",
    "사이트에 연결할 수 없음",
    "연결할 수 없습니다",
    "프록시 서버에 문제가 있습니다",
)


async def _get_body_probe_text(page, *, limit: int = 20000) -> str:
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
    if not await has_error_in_body(page):
        return False
    await page.wait_for_timeout(confirm_delay_ms)
    return await has_error_in_body(page)


async def run_single_task(slot_id, task_count):
    """단일 작업 실행 (슬롯 ID와 작업 번호 포함)"""
    task_id = f"S{slot_id}-T{task_count}"
    position = SLOT_POSITIONS[slot_id]
    
    # 1. 지역 프로필 로드
    try:
        with open(PROFILES_PATH, 'r', encoding='utf-8') as f:
            profiles = json.load(f)
    except Exception as e:
        print(f"[{task_id}] ❌ 프로필 로드 실패: {e}")
        return False

    region_name = random.choice(list(profiles.keys()))
    profile = profiles[region_name]
    
    # 2. Redis 프록시 대여
    lease_client = RedisProxyLeaseClient(config=REDIS_CONFIG)
    lease_client.connect()
    proxy_url = lease_client.claim(lease_seconds=300)
    
    if not proxy_url:
        print(f"[{task_id}] ❌ 사용 가능한 프록시 없음")
        lease_client.close()
        return False

    print(f"[{task_id}] 🚀 시작 | 지역: {region_name} | 프록시: {proxy_url} | 위치: ({position['x']}, {position['y']})")

    session_ok = False
    response = None
    nav_ok = False
    
    try:
        browser = StealthPatchrightBrowser(
            proxy=proxy_url,
            headless=False,
            mobile=True,
            locale=profile["locale"],
            timezone_id=profile["timezone"],
            cleanup_user_data_dir=True,
            window_position=position  # 슬롯별 고정 위치
        )

        async with browser:
            page = await browser.new_page()
            bRaiseException = False
            Exception_waittime = 60
            
            # 3. 접속 시도
            try:
                response = await page.goto(
                    TARGET_URL, 
                    wait_until="commit",
                    timeout=60000*3,
                    referer=random.choice(profile["referers"])
                )
                
                await page.wait_for_selector("body", timeout=1000*60)
                await asyncio.sleep(5)
                
            except Exception as e:
                print(f"[{task_id}] ⚠️ 페이지 이동 중 예외: {e}")
                bRaiseException = True
                await asyncio.sleep(Exception_waittime)
                
            if "error" in page.url or await error_body_stable(page, confirm_delay_ms=5000):
                print(f"🛑 [{task_id}] 페이지 로드 실패! (URL: {page.url})")
                return False
            else:
                print(f"🛑 [{task_id}] 페이지 정상으로 열림 (URL: {page.url})")
                nav_ok = True

            # 4. Consent 체크
            await handle_google_consent(page)
            print(f"🛑 [{task_id}] consent 통과")
          
            # 5. 봇 탐지 체크
            if await check_bot_detected(page):
                print(f"🛑 [{task_id}] 봇 의심 페이지 감지! (URL: {page.url})")
                return False            
            print(f"🛑 [{task_id}] 봇 탐지 통과")
            
            # 6. 최종 결과 확인
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except:
                pass
            
            # 7. 봇 탐지 체크 한번 더
            if await check_bot_detected(page):
                print(f"🛑 [{task_id}] 봇 의심 페이지 감지! (URL: {page.url})")
                return False            
            print(f"🛑 [{task_id}] 봇 탐지 통과")            

            if nav_ok:
                print(f"[{task_id}] ✅ 영상 시청 시작 40초 후 휴먼 동작 실행")
                await asyncio.sleep(60)
                human_m = HumanEventMobile(page)
                await human_m.execute_random_action()
                print(f"🛑 [{task_id}] 휴먼 동작 완료!")
                                
                wait_time = (80 - Exception_waittime) if bRaiseException else 80
                print(f"[{task_id}] ✅ {wait_time}초 동안 브라우저 유지")
                
                # 브라우저 유지하면서 사용자가 닫는지 체크
                for i in range(wait_time):
                    try:
                        if page.is_closed():
                            print(f"[{task_id}] 🔴 사용자가 브라우저를 닫음!")
                            return "BROWSER_CLOSED"
                    except:
                        print(f"[{task_id}] 🔴 브라우저 연결 끊김!")
                        return "BROWSER_CLOSED"
                    
                    await asyncio.sleep(1)
                
                session_ok = True
            else:
                print(f"[{task_id}] ❌ 실패 (Status: {response.status if response else 'N/A'})")

    except Exception as e:
        print(f"[{task_id}] 🔥 실행 에러: {e}")
    finally:
        lease_client.release_on_result(member=proxy_url, session_ok=session_ok)
        lease_client.close()
    
    return session_ok


def slot_worker(slot_id):
    """슬롯별 워커 쓰레드"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    task_count = 1
    print(f"🎰 슬롯 {slot_id} 워커 시작!")
    
    while not shutdown_event.is_set():
        task_id = f"S{slot_id}-T{task_count}"
        try:
            result = loop.run_until_complete(run_single_task(slot_id, task_count))

            # ✅ 성공 카운트(모든 쓰레드 합산)
            if result is True:
                inc_success_and_print(task_id)

            if result == "BROWSER_CLOSED":
                print(f"\n🔄 슬롯 {slot_id} - 브라우저 닫힘 감지, 워커 종료\n")
                break
            
            if shutdown_event.is_set():
                break
                
            task_count += 1
            import time
            time.sleep(2)
            
        except Exception as e:
            if shutdown_event.is_set():
                break
            print(f"🔥 슬롯 {slot_id} 워커 에러: {e}")
            import time
            time.sleep(5)
    
    loop.close()
    print(f"🎰 슬롯 {slot_id} 워커 종료됨")


def manage_slot(slot_id):
    """슬롯 관리 - 워커 쓰레드가 종료되면 새로 시작"""
    while not shutdown_event.is_set():
        print(f"▶️  슬롯 {slot_id} 새 워커 시작...")
        
        worker_thread = threading.Thread(target=slot_worker, args=(slot_id,), daemon=True)
        
        with slot_lock:
            slot_threads[slot_id] = worker_thread
        
        worker_thread.start()
        worker_thread.join()
        
        if shutdown_event.is_set():
            break
            
        print(f"🔄 슬롯 {slot_id} 워커 재시작 대기...\n")
        import time
        time.sleep(1)


def start_all_slots():
    """모든 슬롯 매니저 시작"""
    print(f"🎬 총 {SLOT_NUM}개 슬롯으로 시스템 시작\n")
    
    managers = []
    for slot_id in range(SLOT_NUM):
        manager_thread = threading.Thread(target=manage_slot, args=(slot_id,), daemon=True)
        managers.append(manager_thread)
        manager_thread.start()
        import time
        time.sleep(0.5)  # 슬롯 간 시작 간격
    
    # 메인 쓰레드는 종료 시그널 대기
    try:
        while not shutdown_event.is_set():
            import time
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass


def signal_handler(signum, frame):
    """Ctrl+C 시그널 핸들러"""
    print("\n\n🛑 종료 시그널 수신! 모든 슬롯 종료 중...\n")
    shutdown_event.set()
    import time
    time.sleep(2)
    print("✅ 프로그램 종료\n")
    sys.exit(0)


if __name__ == "__main__":
    # Ctrl+C 시그널 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        start_all_slots()
    except KeyboardInterrupt:
        print("\n🛑 중단됨")
    except Exception as e:
        print(f"\n🔥 예상치 못한 에러: {e}")
    finally:
        shutdown_event.set()
