import os
import shutil
import subprocess
import time
import random
from DrissionPage import ChromiumPage, ChromiumOptions

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
        # 포트 점유 프로세스 강제 종료 (Windows)
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
        """
        devices_dict가 없거나 비어있어도 '모바일스러운' 기본 조합을 리턴.
        DPR은 과확대를 막기 위해 2로 상한.
        """
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
        #######################
        # ✅ 추가: 페이지 로드 전략 명시
        co.set_argument('--disable-features=NetworkService')  # 네트워크 지연 감소
        co.set_argument('--disable-features=VizDisplayCompositor')  # 렌더링 최적화
        
        # ✅ GPU 가속 (렌더링 속도 향상)
        co.set_argument('--enable-gpu-rasterization')
        co.set_argument('--enable-zero-copy')
        
        # ✅ 캐시/프리로드 설정
        co.set_argument('--disk-cache-size=268435456')  # 256MB
        co.set_argument('--media-cache-size=268435456')
        
        # ✅ DNS prefetch
        co.set_argument('--dns-prefetch-disable')  # 역설적이지만 프록시 환경에선 더 빠를 수 있음
        
        #######################

        if self.proxy:
            co.set_proxy(self.proxy)

        page = ChromiumPage(co)
        ###############################
        # ✅ 추가: Performance 관련 CDP 설정
        try:
            page.run_cdp("Network.enable")
            page.run_cdp("Network.setCacheDisabled", cacheDisabled=False)  # 캐시 활성화
        except:
            pass
        ###############################

        # 1) webdriver 흔적 최소화: 문서 시작부터 주입 시도
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

        # 2) timezone / locale: CDP Emulation로 시도 (실패해도 무시)
        try:
            page.run_cdp("Emulation.setTimezoneOverride", timezoneId=timezone)
        except:
            pass
        try:
            page.run_cdp("Emulation.setLocaleOverride", locale=locale)
        except:
            pass

        # 3) Referer: Extra Headers로 시도 (document.referrer 덮어쓰기 X)
        if self.referer:
            try:
                page.run_cdp("Network.enable")
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
        
class StealthMobileBrowser_origin:
    # [수정] referer 인자를 추가로 받도록 변경
    def __init__(self, slot_index: int, profile: dict, proxy: str = None, devices_dict: dict = None, referer: str = None):
        self.slot_index = slot_index
        self.port = 15000 + slot_index
        self.profile = profile
        self.proxy = proxy #
        self.devices_dict = devices_dict 
        self.referer = referer  # [추가] 전달받은 리퍼러 저장
        
        self.base_path = os.path.dirname(os.path.abspath(__file__))
        self.temp_root = os.path.join(self.base_path, "browser_temp")
        self.temp_dir = os.path.join(self.temp_root, f"slot_{self.slot_index}")

        self._force_clean_up()
        self.page = self._create_browser()

    def _force_clean_up(self):
        try:
            cmd = f'for /f "tokens=5" %a in (\'netstat -aon ^| findstr :{self.port} ^| findstr LISTENING\') do taskkill /f /pid %a'
            subprocess.call(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except: pass
        
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        os.makedirs(self.temp_dir, exist_ok=True)

    def _create_browser(self):
        co = ChromiumOptions()
        co.set_local_port(self.port)
        co.set_user_data_path(self.temp_dir)
        
        # Playwright 기반 기기 정보 선택 및 적용
        device_name = "Default iPhone"
        dpr = 3
        if self.devices_dict:
            device_name = random.choice(list(self.devices_dict.keys()))
            device = self.devices_dict[device_name]
            co.set_user_agent(device['user_agent'])
            width = device['viewport']['width']
            height = device['viewport']['height']
            dpr = device.get('device_pixel_ratio', 3)
            co.set_argument(f'--window-size={width},{height}')
            co.set_argument(f'--force-device-scale-factor={dpr}')
            if device.get('has_touch'):
                co.set_argument('--blink-settings=touchEventEnabled=true')
        
        co.set_argument('--use-mobile-user-agent')
        co.set_argument('--no-sandbox')
        co.set_argument('--disable-blink-features=AutomationControlled')
        co.set_argument('--log-level=3')

        locale = self.profile.get("locale", "en-US")
        timezone = self.profile.get("timezone", "America/New_York")
        co.set_argument(f'--lang={locale}')

        if self.proxy:
            co.set_proxy(self.proxy)

        try:
            page = ChromiumPage(co)
            # [수정] 리퍼러(document.referrer)까지 자바스크립트로 강제 주입
            page.run_js(f"""
                Object.defineProperty(navigator, 'webdriver', {{get: () => undefined}});
                Object.defineProperty(window, 'devicePixelRatio', {{get: () => {dpr}}});
                Object.defineProperty(document, 'referrer', {{get: () => '{self.referer}'}});
                Intl.DateTimeFormat.prototype.resolvedOptions = () => {{
                    return {{ timeZone: '{timezone}', locale: '{locale}' }};
                }};
            """)
            print(f"[Slot-{self.slot_index}] 📱 기기: {device_name} | 🔗 Referer 주입: {self.referer}")
            return page
        except Exception as e:
            raise e

    def quit(self):
        try:
            self.page.quit()
            time.sleep(1)
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir, ignore_errors=True)
        except: pass