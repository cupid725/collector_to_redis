import asyncio
import os
from patchright.async_api import async_playwright

async def run_final_stealth():
    async with async_playwright() as p:
        chrome_exe = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
        user_data_path = os.path.join(os.getcwd(), "stealth_profile_final")

        # 수정된 컨텍스트 실행 부분
        context = await p.chromium.launch_persistent_context(
            user_data_path,
            executable_path=chrome_exe,
            headless=False,
            # [추가] 이 줄이 있어야 상단 배너가 진짜로 사라집니다.
            ignore_default_args=["--enable-automation"], 
            args=[
                "--disable-blink-features=AutomationControlled",
                "--enable-blink-features=ContentIndex,ContactsManager,NetworkInformation",
                "--start-maximized",
                "--no-sandbox"
            ],
            no_viewport=True
        )

        page = await context.new_page()

        try:
            target_url = "https://abrahamjuliot.github.io/creepjs/"
            print(f"접속 시도 중: {target_url}")
            
            await page.goto(target_url, wait_until="networkidle", timeout=60000)
            
            print("접속 성공! 배너가 사라졌는지 확인하고 30초만 기다려주세요.")
            # 분석이 완료되어 점수가 뜰 때까지 대기
            await asyncio.sleep(120) 
            
        except Exception as e:
            print(f"접속 에러 발생: {e}")
            
        finally:
            await context.close()

if __name__ == "__main__":
    asyncio.run(run_final_stealth())