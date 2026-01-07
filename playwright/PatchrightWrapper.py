"""
pip install patchright
patchright install chromium
"""
import os
import asyncio
import shutil
import time
import argparse
import random
import uuid
from pathlib import Path
from urllib.parse import urlparse, unquote
from typing import Optional, Dict, Any, List, Tuple

from patchright.async_api import async_playwright, BrowserContext, Playwright


class StealthPatchrightBrowser:
    """
    - Patchright(Playwright) 기반 Chromium persistent context 래퍼
    - WebRTC IP leak 방지 플래그 기본 적용
    - PROXY 값이 있으면 proxy 설정 자동 적용
    - user_data_dir를 자동 생성한 경우 close()에서 자동 삭제
    - 모바일/PC 모드에 따라 Playwright devices에서 랜덤 디바이스 선택
        - mobile=True  => Android 디바이스만 랜덤
        - mobile=False => Windows 디바이스만 랜덤
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
        cleanup_user_data_dir: bool = True,
        profile_base_dir: Optional[str] = None,
        mobile: bool = False,
        locale: str = "en-US",
        timezone_id: str = "America/New_York",
    ):
        self.chrome_exe = chrome_exe
        self.proxy = proxy
        self.headless = headless
        self.mobile = mobile

        self.locale = locale
        self.timezone_id = timezone_id

        # 모바일 디바이스를 고를 때는 viewport를 device descriptor가 제공하므로 no_viewport=False가 더 안전
        self.no_viewport = False if mobile else no_viewport

        self.extra_args = extra_args or []
        self.webrtc_leak_protection = webrtc_leak_protection
        self.cleanup_user_data_dir = cleanup_user_data_dir

        # user_data_dir 자동 생성(기본)
        # ✅ 요구사항: 소스 위치의 patchright_temp 하위에, 브라우저(컨텍스트)마다 새 디렉 생성
        self._auto_user_data_dir = False
        if user_data_dir:
            self.user_data_dir = user_data_dir
        else:
            # profile_base_dir가 주어지면 그걸 사용, 아니면 PatchrightWrapper.py가 있는 디렉 기준으로 고정
            base = Path(profile_base_dir) if profile_base_dir else (Path(__file__).resolve().parent / "patchright_temp")
            base.mkdir(parents=True, exist_ok=True)

            # 컨텍스트마다 고유 디렉 생성
            stamp = f"{int(time.time())}_{os.getpid()}_{uuid.uuid4().hex[:8]}"
            p = base / f"patchright_profile_{stamp}"
            p.mkdir(parents=True, exist_ok=True)
            self.user_data_dir = str(p)
            self._auto_user_data_dir = True

        self._p: Optional[Playwright] = None
        self._context: Optional[BrowserContext] = None

        self.selected_device_name: Optional[str] = None

    # =========================
    # ✅ private static helpers
    # =========================
    @staticmethod
    def __normalize_device_descriptor(desc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Playwright devices descriptor는 버전에 따라 key naming이 다를 수 있어 안전하게 정규화.
        (예: userAgent vs user_agent)
        """
        out = dict(desc)

        if "user_agent" not in out and "userAgent" in out:
            out["user_agent"] = out["userAgent"]

        if "device_scale_factor" not in out and "deviceScaleFactor" in out:
            out["device_scale_factor"] = out["deviceScaleFactor"]

        if "is_mobile" not in out and "isMobile" in out:
            out["is_mobile"] = out["isMobile"]

        if "has_touch" not in out and "hasTouch" in out:
            out["has_touch"] = out["hasTouch"]

        if "default_browser_type" not in out and "defaultBrowserType" in out:
            out["default_browser_type"] = out["defaultBrowserType"]

        return out

    @staticmethod
    def __pick_random_device(playwright: Playwright, mobile: bool) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        playwright.devices에서 조건에 맞는 디바이스를 랜덤으로 고른다.
        - mobile=True  => Android만 (UA에 'Android' 포함)
        - mobile=False => Windows만 (UA에 'Windows' 포함)
        """
        devices = getattr(playwright, "devices", None)
        if not isinstance(devices, dict) or not devices:
            return None, None

        candidates: List[Tuple[str, Dict[str, Any]]] = []
        for name, raw in devices.items():
            if not isinstance(raw, dict):
                continue

            d = StealthPatchrightBrowser.__normalize_device_descriptor(raw)
            ua = str(d.get("user_agent", ""))

            if mobile:
                if "Android" in ua:
                    candidates.append((name, d))
            else:
                if "Windows" in ua:
                    if d.get("is_mobile") is False or "Mobile" not in ua:
                        candidates.append((name, d))

        if not candidates:
            return None, None

        return random.choice(candidates)

    @staticmethod
    def _resolve_chrome_exe_path(chrome_exe: Optional[str]) -> str:
        """
        executable_path로 전달할 Chrome/Edge 실행 파일 경로를 안전하게 결정한다.

        - chrome_exe가 존재하면 그대로 사용
        - 없으면 흔한 설치 경로(Chrome/Edge)를 자동 탐색
        - 그래도 없으면 FileNotFoundError 발생
        """
        # 1) 명시 경로 우선
        if chrome_exe:
            p = Path(chrome_exe)
            if p.exists():
                return str(p)

        # 2) 환경변수 우선
        env_path = os.environ.get("CHROME_PATH") or os.environ.get("GOOGLE_CHROME_SHIM")
        if env_path:
            p = Path(env_path)
            if p.exists():
                return str(p)

        # 3) 흔한 설치 경로 탐색(Windows 기준)
        candidates = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        ]

        local = os.environ.get("LOCALAPPDATA")
        if local:
            candidates += [
                str(Path(local) / "Google" / "Chrome" / "Application" / "chrome.exe"),
                str(Path(local) / "Chromium" / "Application" / "chrome.exe"),
                str(Path(local) / "Microsoft" / "Edge" / "Application" / "msedge.exe"),
            ]

        for c in candidates:
            p = Path(c)
            if p.exists():
                return str(p)

        raise FileNotFoundError(
            "Chrome/Edge 실행 파일을 찾지 못했어. "
            "chrome_exe 인자로 정확한 경로를 지정하거나, "
            "환경변수 CHROME_PATH를 설정해줘."
        )

    # =========================
    # 기존 메서드들
    # =========================
    @staticmethod
    def _build_proxy_config(proxy_url: Optional[str]) -> Optional[Dict[str, str]]:
        if not proxy_url:
            return None

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
            "--no-sandbox",
        ]

        # 데스크톱 모드에선 창 크기/위치 제어를 위해 최대화 플래그를 유지
        # (모바일 모드에선 viewport/UA를 디바이스 디스크립터로 맞추는게 중요하므로 제외)
        if not self.mobile:
            args.append("--start-maximized")


        if self.webrtc_leak_protection:
            args += [
                "--webrtc-ip-handling-policy=disable_non_proxied_udp",
                "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
                "--enable-webrtc-hide-local-ips-with-mdns",
            ]

        if self.mobile:
            args += ["--window-size=430,930"]

        args += self.extra_args
        return args

    async def start(self) -> BrowserContext:
        if self._context:
            return self._context

        self._p = await async_playwright().start()
        proxy_cfg = self._build_proxy_config(self.proxy)

        launch_kwargs: Dict[str, Any] = dict(
            user_data_dir=self.user_data_dir,
            executable_path=self._resolve_chrome_exe_path(self.chrome_exe),
            headless=self.headless,
            ignore_default_args=["--enable-automation"],
            args=self._build_args(),
            no_viewport=self.no_viewport,
            locale=self.locale,
            timezone_id=self.timezone_id,
        )

        # ✅ devices 목록에서 랜덤 선택
        dev_name, dev_desc = self.__pick_random_device(self._p, mobile=self.mobile)

        if dev_name and dev_desc:
            self.selected_device_name = dev_name

            if "viewport" in dev_desc and dev_desc["viewport"]:
                launch_kwargs["no_viewport"] = False

            for k in ("viewport", "user_agent", "device_scale_factor", "is_mobile", "has_touch"):
                if k in dev_desc:
                    launch_kwargs[k] = dev_desc[k]

            print(f"📱 랜덤 디바이스 선택: {dev_name}" if self.mobile else f"🖥️ 랜덤 디바이스 선택: {dev_name}")
        else:
            if self.mobile:
                print("⚠️ Android 디바이스 후보를 devices에서 찾지 못했어. 기본 컨텍스트로 실행함.")
            else:
                print("⚠️ Windows 디바이스 후보를 devices에서 찾지 못했어. 기본 데스크톱 컨텍스트로 실행함.")

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
        if not path.exists():
            return

        last_err: Optional[Exception] = None

        def _onerror(func, p, exc_info):
            # Windows에서 프로필 삭제 시 read-only/권한 이슈가 흔해서 강제로 쓰기 가능으로 바꾸고 재시도
            try:
                os.chmod(p, 0o777)
            except Exception:
                pass
            try:
                func(p)
            except Exception:
                pass

        for i in range(retries):
            try:
                shutil.rmtree(path, onerror=_onerror)
                return
            except Exception as e:
                last_err = e
                await asyncio.sleep(delay + i * 0.05)

        # 마지막 시도(그래도 실패하면 마지막 에러를 다시 raise)
        try:
            shutil.rmtree(path, onerror=_onerror)
        except Exception:
            if last_err:
                raise last_err

    async def close(self):
        if self._context:
            await self._context.close()
            self._context = None

        if self._p:
            await self._p.stop()
            self._p = None

        if self.cleanup_user_data_dir and self._auto_user_data_dir:
            p = Path(self.user_data_dir)
            await asyncio.sleep(0.3)
            await self._safe_rmtree(p)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()


'''
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=TARGET_URL)
    parser.add_argument("--proxy", default=PROXY)
    parser.add_argument("--mobile", action="store_true", help="모바일(Android) 디바이스만 랜덤 선택")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--keep-profile", action="store_true", help="자동 생성 user_data_dir 삭제하지 않음")
    args = parser.parse_args()

    browser = StealthPatchrightBrowser(
        proxy=args.proxy,
        webrtc_leak_protection=True,
        headless=args.headless,
        mobile=args.mobile,
        cleanup_user_data_dir=not args.keep_profile,
    )

    async with browser:
        page = await browser.new_page()
        await page.goto(args.url, wait_until="networkidle", timeout=60000*2)
        print(f"접속 완료: {args.url}")
        print("120초 대기...")
        await asyncio.sleep(120)


if __name__ == "__main__":
    asyncio.run(main())
'''    
