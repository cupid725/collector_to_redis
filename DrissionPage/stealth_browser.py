import os
import shutil
import subprocess
import time
import random
from DrissionPage import ChromiumPage, ChromiumOptions
import config

class StealthMobileBrowser:
    def __init__(self, slot_index: int, profile: dict, proxy: str = None, devices_dict: dict = None, referer: str = None):
        self.slot_index = slot_index
        self.port = 15000 + slot_index
        self.profile = profile or {}
        self.proxy = proxy
        self.devices_dict = devices_dict
        self.referer = referer

        self.base_path = os.path.dirname(os.path.abspath(__file__))
        self.temp_root = os.path.join(self.base_path, "browser_temp")
        self.temp_dir = os.path.join(self.temp_root, f"slot_{self.slot_index}")

        self._force_clean_up()
        self.page = self._create_browser()

    def _force_clean_up(self):
        try:
            cmd = (
                f'for /f "tokens=5" %a in (\'netstat -aon ^| findstr :{self.port} ^| findstr LISTENING\') '
                f'do taskkill /f /pid %a'
            )
            subprocess.call(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except:
            pass

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        os.makedirs(self.temp_dir, exist_ok=True)

    def _pick_device_profile(self):
        default = {
            "name": "Fallback Mobile (390x844)",
            "user_agent": (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
            ),
            "viewport": {"width": 390, "height": 844},
            "device_pixel_ratio": 2,
            "has_touch": True,
        }

        if not self.devices_dict:
            return default

        try:
            device_name = random.choice(list(self.devices_dict.keys()))
            device = self.devices_dict[device_name]
            if "user_agent" not in device or "viewport" not in device:
                return default
            device = dict(device)
            device["name"] = device_name
            return device
        except:
            return default

    def _create_browser(self):
        co = ChromiumOptions()
        co.set_local_port(self.port)
        co.set_user_data_path(self.temp_dir)

        device = self._pick_device_profile()
        device_name = device.get("name", "UnknownDevice")

        ua = device.get("user_agent")
        width = int(device.get("viewport", {}).get("width", 390))
        height = int(device.get("viewport", {}).get("height", 844))

        dpr = device.get("device_pixel_ratio", 2)
        try:
            dpr = float(dpr)
        except:
            dpr = 2.0
        dpr = min(dpr, 2.0)

        if ua:
            co.set_user_agent(ua)

        co.set_argument(f"--window-size={width},{height}")
        co.set_argument(f"--force-device-scale-factor={dpr}")

        if device.get("has_touch"):
            co.set_argument("--blink-settings=touchEventEnabled=true")

        # 기본 스텔스 옵션
        co.set_argument("--no-sandbox")
        co.set_argument("--disable-blink-features=AutomationControlled")
        co.set_argument("--log-level=3")

        locale = self.profile.get("locale", "en-US")
        timezone = self.profile.get("timezone", "America/New_York")
        co.set_argument(f"--lang={locale}")

        # ========================================
        # ✅ 프록시 환경 최적화 옵션 (대폭 강화)
        # ========================================
        
        # 1. 연결 최적화
        co.set_argument('--disable-features=NetworkService')
        co.set_argument('--disable-features=VizDisplayCompositor')
        co.set_argument('--enable-features=NetworkServiceInProcess')  # 프록시 안정성 향상
        
        # 2. 타임아웃 증가
        co.set_argument('--load-extension-timeout=300000')  # 5분
        co.set_argument('--no-proxy-server-timeout')  # 프록시 타임아웃 무시
        
        # 3. 메모리/캐시 최적화
        co.set_argument('--disk-cache-size=536870912')  # 512MB (2배 증가)
        co.set_argument('--media-cache-size=536870912')
        co.set_argument('--aggressive-cache-discard')  # 적극적 메모리 관리
        
        # 4. GPU 가속 (렌더링 속도 향상)
        co.set_argument('--enable-gpu-rasterization')
        co.set_argument('--enable-zero-copy')
        co.set_argument('--enable-accelerated-video-decode')
        
        # 5. 프리페치 비활성화 (프록시 부하 감소)
        co.set_argument('--dns-prefetch-disable')
        co.set_argument('--disable-features=Prerender2')
        
        # 6. 병렬 연결 증가 (느린 프록시 대응)
        co.set_argument('--max-connections-per-host=10')  # 기본 6 → 10
        co.set_argument('--max-connections-per-proxy=32')  # 기본 8 → 32
        
        # 7. HTTP/2 최적화
        co.set_argument('--enable-quic')  # QUIC 프로토콜 (더 빠른 연결)
        co.set_argument('--enable-features=NetworkTimeServiceQuerying')
        
        # 8. 리소스 로딩 최적화 (선택적)
        if getattr(config, 'DISABLE_IMAGES', False):
            co.set_argument('--blink-settings=imagesEnabled=false')
            print(f"[Slot-{self.slot_index}] 🚫 이미지 로딩 비활성화")
        
        # 9. 프록시 전용 플래그
        co.set_argument('--proxy-bypass-list=<-loopback>')  # 로컬 우회
        co.set_argument('--force-fieldtrials=*NetworkIsolationKey/Enabled')
        
        # ========================================

        if self.proxy:
            co.set_proxy(self.proxy)
            print(f"[Slot-{self.slot_index}] 🌐 프록시 설정: {self.proxy[:50]}...")

        # 페이지 생성 (타임아웃 증가)
        try:
            page = ChromiumPage(co)
        except Exception as e:
            print(f"[Slot-{self.slot_index}] ❌ 브라우저 생성 실패: {e}")
            raise

        # ========================================
        # ✅ CDP 최적화 설정 (프록시 환경)
        # ========================================
        try:
            # 1. 네트워크 캐시 활성화
            page.run_cdp("Network.enable")
            page.run_cdp("Network.setCacheDisabled", cacheDisabled=False)
            
            # 2. 타임아웃 증가 (CDP 레벨)
            page.run_cdp("Runtime.enable")
            page.run_cdp("Runtime.setMaxCallStackSizeToCapture", size=0)  # 스택 추적 비활성화 (성능 향상)
            
            # 3. 우선순위 낮은 리소스 지연 로드
            page.run_cdp("Network.setBypassServiceWorker", bypass=True)
            
            print(f"[Slot-{self.slot_index}] ✅ CDP 최적화 완료")
        except Exception as e:
            print(f"[Slot-{self.slot_index}] ⚠️ CDP 설정 일부 실패: {e}")

        # ========================================
        # 스텔스 JS 주입
        # ========================================
        stealth_js = """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        """
        try:
            page.run_cdp("Page.addScriptToEvaluateOnNewDocument", source=stealth_js)
        except:
            try:
                page.run_js("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
            except:
                pass

        # Timezone / Locale
        try:
            page.run_cdp("Emulation.setTimezoneOverride", timezoneId=timezone)
        except:
            pass
        try:
            page.run_cdp("Emulation.setLocaleOverride", locale=locale)
        except:
            pass

        # Referer 설정
        if self.referer:
            try:
                page.run_cdp("Network.setExtraHTTPHeaders", headers={"Referer": self.referer})
            except:
                pass

        print(
            f"[Slot-{self.slot_index}] 📱 기기: {device_name} | {width}x{height} | DPR={dpr} | "
            f"locale={locale} | tz={timezone} | referer={self.referer}"
        )
        return page

    def quit(self):
        try:
            self.page.quit()
            time.sleep(1)
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir, ignore_errors=True)
        except:
            pass