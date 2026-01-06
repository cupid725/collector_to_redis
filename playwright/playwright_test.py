import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, unquote

from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page, Playwright

# =========================
# 설정
# =========================
TARGET_URL = "https://abrahamjuliot.github.io/creepjs/"
PROXY: Optional[str] = None  # 예: "http://user:pass@1.2.3.4:3128" 또는 "http://1.2.3.4:3128"

# ✅ 핵심: launch() 대신 "일반 크롬을 띄우고 CDP로 붙기"
USE_CDP_ATTACH = True

# Windows 기본 크롬 경로 (다를 수 있으니 필요하면 수정)
CHROME_PATH_CANDIDATES = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
]

CDP_HOST = "127.0.0.1"
CDP_PORT = 9222


def find_chrome_exe() -> str:
    # 우선 환경변수로 지정 가능하게
    env = os.environ.get("CHROME_PATH")
    if env and Path(env).exists():
        return env

    for p in CHROME_PATH_CANDIDATES:
        if Path(p).exists():
            return p

    raise FileNotFoundError(
        "Chrome 실행 파일을 찾지 못했어.\n"
        "1) CHROME_PATH 환경변수로 chrome.exe 경로를 지정하거나\n"
        "2) CHROME_PATH_CANDIDATES에 너 PC 경로를 추가해줘."
    )


def parse_proxy(proxy_url: str) -> dict:
    if not proxy_url:
        return {}
    u = urlparse(proxy_url)
    if not u.hostname or not u.port:
        raise ValueError(f"Invalid proxy url: {proxy_url!r}")
    server = f"{u.scheme}://{u.hostname}:{u.port}"
    proxy = {"server": server}
    if u.username:
        proxy["username"] = unquote(u.username)
    if u.password:
        proxy["password"] = unquote(u.password)
    return proxy


class StealthBrowser:
    def __init__(self, target_url: str, proxy: Optional[str] = None):
        self.TARGET_URL = target_url
        self.PROXY = proxy

        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None

        self._chrome_proc: Optional[subprocess.Popen] = None
        self.profile_path: Optional[Path] = None

    # -------------------------
    # CDP로 일반 Chrome 붙기
    # -------------------------
    def _start_regular_chrome_for_cdp(self) -> None:
        chrome_exe = find_chrome_exe()

        current_dir = Path(__file__).parent
        self.profile_path = current_dir / f"_cdp_profile_{int(time.time())}"
        self.profile_path.mkdir(parents=True, exist_ok=True)

        args = [
            chrome_exe,
            f"--remote-debugging-port={CDP_PORT}",
            f"--remote-debugging-address={CDP_HOST}",
            f'--user-data-dir={str(self.profile_path)}',
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-popup-blocking",
            "--disable-features=TranslateUI",
            # ✅ 창을 “일반 데스크톱”처럼 (CreepJS Like Headless: noTaskbar 같은 거 줄이려면 풀스크린/최대화 피하기)
            "--window-size=1280,800",
            "--window-position=120,80",
        ]

        # 프록시를 “크롬 자체”에 적용 (CDP attach 방식에선 context proxy 옵션이 제한적이라 이게 확실함)
        if self.PROXY:
            px = parse_proxy(self.PROXY)
            # username/password는 크롬 cli proxy-server에 직접 못 넣어서
            # 인증 프록시는 별도 확장/인증 처리 필요.
            # 일단 무인증 프록시 기준으로 적용.
            args.append(f'--proxy-server={px["server"]}')

        print("🟢 일반 Chrome 실행 (CDP attach 모드)")
        self._chrome_proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # CDP 포트가 뜰 시간을 조금 줌
        time.sleep(1.2)

    def _connect_over_cdp(self) -> None:
        assert self._playwright is not None
        endpoint = f"http://{CDP_HOST}:{CDP_PORT}"
        self._browser = self._playwright.chromium.connect_over_cdp(endpoint)

        # CDP로 붙으면 보통 이미 1개 context가 존재함
        if self._browser.contexts:
            self._context = self._browser.contexts[0]
        else:
            self._context = self._browser.new_context()

        self._page = self._context.new_page()

    # -------------------------
    # 일반 launch (비추천, 남겨만 둠)
    # -------------------------
    def _launch_playwright(self) -> None:
        assert self._playwright is not None

        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ]

        proxy_cfg = None
        if self.PROXY:
            proxy_cfg = parse_proxy(self.PROXY)
            print(f"🧭 PROXY 적용: {proxy_cfg}")

        # channel="chrome" 시도
        try:
            self._browser = self._playwright.chromium.launch(
                channel="chrome",
                headless=False,
                args=launch_args,
                ignore_default_args=["--enable-automation"],
            )
            print("🟢 channel='chrome' 로 실행")
        except Exception:
            self._browser = self._playwright.chromium.launch(
                headless=False,
                args=launch_args,
                ignore_default_args=["--enable-automation"],
            )
            print("🟡 기본 chromium 로 실행 (chrome 채널 실패)")

        context_options = {
            "viewport": {"width": 1280, "height": 800},
            "locale": "en-US",
            "timezone_id": "America/New_York",
        }
        if proxy_cfg:
            context_options["proxy"] = proxy_cfg

        self._context = self._browser.new_context(**context_options)
        self._page = self._context.new_page()

    # -------------------------
    # 실행/종료
    # -------------------------
    def start(self):
        self._playwright = sync_playwright().start()

        if USE_CDP_ATTACH:
            self._start_regular_chrome_for_cdp()
            self._connect_over_cdp()
        else:
            self._launch_playwright()

        assert self._page is not None

        self._page.goto("about:blank", wait_until="domcontentloaded")
        self._page.goto(self.TARGET_URL, wait_until="domcontentloaded", timeout=60000)

        # 디버그: creepjs 가 true라면 여기 평가도 보통 true로 나옴
        try:
            wd = self._page.evaluate("() => navigator.webdriver")
            print(f"🔎 DEBUG navigator.webdriver = {wd!r}")
        except Exception as e:
            print(f"🔎 DEBUG evaluate 실패: {e}")

        print("\n✅ CreepJS에서 webDriverIsOn 확인하고 Enter 누르면 종료")

    def stop(self):
        try:
            if self._context:
                self._context.close()
        except Exception:
            pass

        try:
            if self._browser:
                self._browser.close()
        except Exception:
            pass

        try:
            if self._playwright:
                self._playwright.stop()
        except Exception:
            pass

        # CDP로 띄운 크롬 프로세스 종료
        if self._chrome_proc and self._chrome_proc.poll() is None:
            try:
                self._chrome_proc.terminate()
            except Exception:
                pass

        # 프로필 정리
        if self.profile_path and self.profile_path.exists():
            try:
                shutil.rmtree(self.profile_path, ignore_errors=True)
            except Exception:
                pass

    def run(self):
        try:
            self.start()
            input()
        except Exception as e:
            print(f"❌ 오류 발생: {e}")
        finally:
            self.stop()


if __name__ == "__main__":
    StealthBrowser(target_url=TARGET_URL, proxy=PROXY).run()
