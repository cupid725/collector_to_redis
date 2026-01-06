import shutil
import time
from pathlib import Path
from urllib.parse import urlparse, unquote
from playwright.sync_api import sync_playwright

#이건 완전히 통과. 다만 PC버전임.
# =========================
# 사용자 설정
# =========================
TARGET_URL = "https://www.naver.com"

PROXY = None  # "http://5.75.198.72:80"


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


def get_hw_info_by_ua(user_agent: str) -> tuple[str, str]:
    # 현실적인 Windows 데스크톱 값으로 고정 (Intel은 Windows에서 가장 흔함)
    return "Intel Inc.", "Intel Iris Xe Graphics"


def build_stealth_init_script(vendor: str, renderer: str, user_agent: str) -> str:
    platform_value = "Win32"
    mem_value = 8
    cpu_value = 8

    js = r"""
(function () {
  'use strict';

  // 기존 코드 유지 (webdriver, navigator, permissions, plugins 등)

  // webdriver 숨김
  try {
    const proto = Object.getPrototypeOf(navigator);
    try { delete proto.webdriver; } catch(e) {}
    try { delete navigator.webdriver; } catch(e) {}
  } catch(e) {}

  // navigator 스펙
  try { Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => __CPU__, enumerable: true }); } catch(e) {}
  try { Object.defineProperty(navigator, 'deviceMemory', { get: () => __MEM__, enumerable: true }); } catch(e) {}
  try { Object.defineProperty(navigator, 'platform', { get: () => "__PLATFORM__", enumerable: true }); } catch(e) {}

  // WebGL vendor/renderer 패치 강화 (UNMASKED_VENDOR_WEBGL, UNMASKED_RENDERER_WEBGL)
  try {
    const getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {
      if (parameter === 37445) { // UNMASKED_VENDOR_WEBGL
        return "__VENDOR__";
      }
      if (parameter === 37446) { // UNMASKED_RENDERER_WEBGL
        return "__RENDERER__";
      }
      return getParameter.apply(this, arguments);
    };

    // WebGL2RenderingContext에도 동일 적용
    if (typeof WebGL2RenderingContext !== 'undefined') {
      WebGL2RenderingContext.prototype.getParameter = WebGLRenderingContext.prototype.getParameter;
    }
  } catch(e) {}

  // window.chrome 강화
  try {
    if (!window.chrome) {
      window.chrome = { runtime: {}, app: {}, csi: function() {}, loadTimes: function() {} };
    } else if (!window.chrome.runtime) {
      window.chrome.runtime = {};
    }
  } catch(e) {}

  // 기존 plugins/mimeTypes, permissions, performance.memory 패치 유지 (생략하지 말고 그대로 복사)

  // ... (기존 plugins, permissions, performance.memory 패치 코드 그대로 붙여넣기)

})();
"""

    js = js.replace("__CPU__", str(cpu_value))
    js = js.replace("__MEM__", str(mem_value))
    js = js.replace("__PLATFORM__", platform_value.replace('"', '\\"'))
    js = js.replace("__VENDOR__", vendor.replace('"', '\\"'))
    js = js.replace("__RENDERER__", renderer.replace('"', '\\"'))
    return js


def run():
    current_dir = Path(__file__).parent
    profile_path = current_dir / f"temp_profile_{int(time.time())}"
    print(f"📂 임시 프로필 생성: {profile_path}")

    try:
        with sync_playwright() as p:
            clean_config = {
                "viewport": {"width": 1920, "height": 1080},
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "device_scale_factor": 1,
                "is_mobile": False,
                "has_touch": False,
                "locale": "en-US",
                "timezone_id": "America/New_York",
            }

            vendor, renderer = get_hw_info_by_ua(clean_config.get("user_agent", ""))

            launch_kwargs = dict(
                user_data_dir=str(profile_path),
                headless=False,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--exclude-switches=enable-automation",
                    # ✅ GPU 하드웨어 가속 강제 + SwiftShader 방지
                    "--enable-gpu",
                    "--enable-webgl",
                    "--use-gl=angle",          # ANGLE (DirectX) 사용
                    "--use-angle=d3d11",       # Direct3D11 우선 (Windows에서 하드웨어 GPU 사용)
                    "--disable-gpu-sandbox",
                    "--no-sandbox",            # 필요시 추가 (일부 환경에서 GPU 문제 해결)
                ],
                **clean_config,
            )

            if PROXY:
                proxy_cfg = parse_proxy(PROXY)
                launch_kwargs["proxy"] = proxy_cfg
                print(f"🧭 PROXY 적용: {proxy_cfg}")

            context = p.chromium.launch_persistent_context(**launch_kwargs)

            context.add_init_script(build_stealth_init_script(vendor, renderer, clean_config.get("user_agent", "")))

            page = context.pages[0] if context.pages else context.new_page()

            page.goto("about:blank", wait_until="domcontentloaded")
            page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)

            print("\n✅ 실행 완료. 결과 확인 후 Enter 눌러 종료...")
            input()

            context.close()

    except Exception as e:
        print(f"❌ 오류 발생: {e}")

    finally:
        if profile_path.exists():
            print(f"🧹 임시 폴더 삭제 중: {profile_path}")
            time.sleep(1.0)
            shutil.rmtree(profile_path, ignore_errors=True)
            print("✨ 정리 완료")


if __name__ == "__main__":
    run()