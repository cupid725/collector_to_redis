"""
pip install patchright
patchright install chromium
"""
import os
import asyncio
import shutil
import time
from pathlib import Path
from urllib.parse import urlparse, unquote
from typing import Optional, Dict, Any, List

from patchright.async_api import async_playwright, BrowserContext, Playwright


# ✅ 여기만 바꿔서 쓰면 됨
TARGET_URL = "https://bot.sannysoft.com/"
PROXY = None
# 예)
# PROXY = "http://127.0.0.1:8888"
# PROXY = "http://user:pass@host:port"
# PROXY = "socks5://host:port"


class StealthPatchrightBrowser:
    """
    - Patchright(Playwright) 기반 Chromium persistent context 래퍼
    - WebRTC IP leak 방지 플래그 기본 적용
    - PROXY 값이 있으면 proxy 설정 자동 적용
    - ✅ user_data_dir를 자동 생성한 경우 close()에서 자동 삭제
    """

    def __init__(
        self,
        chrome_exe: str = r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        user_data_dir: Optional[str] = None,
        proxy: Optional[str] = None,
        headless: bool = False,
        no_viewport: bool = True,
        extra_args: Optional[List[str]] = None,
        webrtc_leak_protection: bool = True,
        cleanup_user_data_dir: bool = True,  # ✅ 자동 생성 프로필 삭제 여부
        profile_base_dir: Optional[str] = None,  # ✅ 프로필 생성 위치(기본: ./_tmp_profiles)
    ):
        self.chrome_exe = chrome_exe
        self.proxy = proxy
        self.headless = headless
        self.no_viewport = no_viewport
        self.extra_args = extra_args or []
        self.webrtc_leak_protection = webrtc_leak_protection

        self.cleanup_user_data_dir = cleanup_user_data_dir

        # ✅ user_data_dir 자동 생성(기본)
        self._auto_user_data_dir = False
        if user_data_dir:
            self.user_data_dir = user_data_dir
        else:
            base = Path(profile_base_dir) if profile_base_dir else (Path(os.getcwd()) / "_tmp_profiles")
            base.mkdir(parents=True, exist_ok=True)
            # 충돌 방지용 유니크 디렉토리
            stamp = f"{int(time.time())}_{os.getpid()}"
            p = base / f"patchright_profile_{stamp}"
            p.mkdir(parents=True, exist_ok=True)

            self.user_data_dir = str(p)
            self._auto_user_data_dir = True

        self._p: Optional[Playwright] = None
        self._context: Optional[BrowserContext] = None

    @staticmethod
    def _build_proxy_config(proxy_url: Optional[str]) -> Optional[Dict[str, str]]:
        if not proxy_url:
            return None

        # scheme 없으면 http로 가정
        if "://" not in proxy_url:
            proxy_url = "http://" + proxy_url

        u = urlparse(proxy_url)
        if not u.hostname or not u.port:
            raise ValueError(f"Invalid PROXY format: {proxy_url}")

        cfg: Dict[str, str] = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
        if u.username:
            cfg["username"] = unquote(u.username)
        if u.password:
            cfg["password"] = unquote(u.password)
        return cfg

    def _build_args(self) -> List[str]:
        args = [
            "--disable-blink-features=AutomationControlled",
            "--enable-blink-features=ContentIndex,ContactsManager,NetworkInformation",
            "--start-maximized",
            "--no-sandbox",
        ]

        # ✅ WebRTC 누수 방지(프록시 환경에서 중요)
        if self.webrtc_leak_protection:
            args += [
                "--webrtc-ip-handling-policy=disable_non_proxied_udp",
                "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
                "--enable-webrtc-hide-local-ips-with-mdns",
            ]

        args += self.extra_args
        return args

    async def start(self) -> BrowserContext:
        """
        persistent context 생성 (필수 1회)
        """
        if self._context:
            return self._context

        self._p = await async_playwright().start()

        proxy_cfg = self._build_proxy_config(self.proxy)
        launch_kwargs: Dict[str, Any] = dict(
            user_data_dir=self.user_data_dir,
            executable_path=self.chrome_exe,
            headless=self.headless,
            # ✅ 자동화 배너/노출 줄이기
            ignore_default_args=["--enable-automation"],
            args=self._build_args(),
            no_viewport=self.no_viewport,
        )

        # ✅ PROXY 있을 때만 적용
        if proxy_cfg:
            launch_kwargs["proxy"] = proxy_cfg

        self._context = await self._p.chromium.launch_persistent_context(**launch_kwargs)
        return self._context

    @property
    def context(self) -> BrowserContext:
        if not self._context:
            raise RuntimeError("Browser not started. Call await start() first.")
        return self._context

    async def new_page(self):
        ctx = await self.start()
        return await ctx.new_page()

    async def _safe_rmtree(self, path: Path, retries: int = 15, delay: float = 0.25):
        """
        Windows에서 chrome이 파일 핸들을 늦게 놓는 경우가 있어 재시도 삭제.
        """
        if not path.exists():
            return

        last_err: Optional[Exception] = None
        for i in range(retries):
            try:
                shutil.rmtree(path)
                return
            except Exception as e:
                last_err = e
                await asyncio.sleep(delay + i * 0.05)

        # 마지막은 ignore_errors로 한 번 더(최대한 정리)
        try:
            shutil.rmtree(path, ignore_errors=True)
        except Exception:
            if last_err:
                raise last_err

    async def close(self):
        """
        context + playwright 종료 + (자동 생성 프로필이면) user_data_dir 삭제
        """
        # 1) 컨텍스트/플레이라이트 종료
        if self._context:
            await self._context.close()
            self._context = None

        if self._p:
            await self._p.stop()
            self._p = None

        # 2) user_data_dir 삭제(자동 생성한 경우만)
        if self.cleanup_user_data_dir and self._auto_user_data_dir:
            p = Path(self.user_data_dir)
            # 크롬이 완전히 핸들 놓을 시간을 약간 줌
            await asyncio.sleep(0.3)
            await self._safe_rmtree(p)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()


# -------------------------
# ✅ 사용 예시 (단독 실행)
# -------------------------
async def main():
    # 여기서 PROXY만 바꿔주면 동일 셋팅 재사용 가능
    browser = StealthPatchrightBrowser(proxy=PROXY, webrtc_leak_protection=True)

    async with browser:
        page = await browser.new_page()
        await page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)
        print("테스트 페이지 접속 완료. 120초 대기...")
        await asyncio.sleep(120)


if __name__ == "__main__":
    asyncio.run(main())
