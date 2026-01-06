import os
import shutil
import random
import time
from pathlib import Path
from playwright.sync_api import sync_playwright, Page

# --- 설정 변수 ---
TARGET_URL = "https://bot.sannysoft.com"

def get_hw_info_by_ua(user_agent: str) -> tuple:
    ua = user_agent.lower()
    if 'iphone' in ua or 'ipad' in ua:
        return 'Apple Inc.', 'Apple GPU'
    return 'ARM', 'Mali-G72'

def inject_custom_stealth(page: Page, vendor: str, renderer: str, device_config: dict):
    """CHR_MEMORY FAIL 및 주요 탐지 항목 완벽 해결 버전"""
    ua = device_config.get('user_agent', '')
    platform_value = "iPhone" if "iPhone" in ua else "Linux armv8l"
    
    # 메모리 값 표준화 (6 제거) 및 CPU 개수 설정
    mem_value = random.choice([4, 8]) 
    cpu_value = random.choice([4, 8])

    page.add_init_script(f"""
        (function() {{
            'use strict';
            
            // 1. WebDriver 완전 은닉 (missing 상태 유도)
            const newProto = Object.getPrototypeOf(navigator);
            delete newProto.webdriver;
            delete navigator.webdriver;

            // 2. CHR_MEMORY FAIL 해결의 핵심: performance.memory 추가
            if (window.performance && !window.performance.memory) {{
                Object.defineProperty(window.performance, 'memory', {{
                    get: () => ({{
                        jsHeapSizeLimit: 2172649472,
                        totalJSHeapSize: 30000000,
                        usedJSHeapSize: 20000000
                    }}),
                    enumerable: true,
                    configurable: true
                }});
            }}

            // 3. navigator 하드웨어 정보 (표준 값 사용)
            Object.defineProperty(navigator, 'deviceMemory', {{ get: () => {mem_value}, enumerable: true }});
            Object.defineProperty(navigator, 'hardwareConcurrency', {{ get: () => {cpu_value}, enumerable: true }});
            Object.defineProperty(navigator, 'platform', {{ get: () => '{platform_value}', enumerable: true }});

            // 4. Plugins & MimeTypes 모사 (이미지의 Red 항목 해결)
            const makeFauxData = () => {{
                const pluginsData = [
                    {{ name: 'PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format' }},
                    {{ name: 'Chrome PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format' }},
                    {{ name: 'WebKit built-in PDF', filename: 'internal-pdf-viewer', description: 'Portable Document Format' }}
                ];
                const pluginArray = Object.create(PluginArray.prototype);
                const mimeTypeArray = Object.create(MimeTypeArray.prototype);
                pluginsData.forEach((p, i) => {{
                    const mimeType = Object.create(MimeType.prototype);
                    const plugin = Object.create(Plugin.prototype);
                    Object.defineProperties(mimeType, {{ type: {{ value: 'application/pdf', enumerable: true }}, enabledPlugin: {{ value: plugin, enumerable: true }} }});
                    Object.defineProperties(plugin, {{ name: {{ value: p.name, enumerable: true }}, filename: {{ value: p.filename, enumerable: true }}, 0: {{ value: mimeType, enumerable: true }}, length: {{ value: 1, enumerable: true }} }});
                    pluginArray[i] = plugin;
                    mimeTypeArray[i] = mimeType;
                }});
                Object.defineProperty(pluginArray, 'length', {{ value: pluginsData.length, enumerable: true }});
                return {{ pluginArray, mimeTypeArray }};
            }};
            const {{ pluginArray, mimeTypeArray }} = makeFauxData();
            Object.defineProperty(navigator, 'plugins', {{ get: () => pluginArray, enumerable: true, configurable: true }});
            Object.defineProperty(navigator, 'mimeTypes', {{ get: () => mimeTypeArray, enumerable: true, configurable: true }});

            // 5. WebGL 렌더러 정보 주입
            const getParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {{
                if (parameter === 37445) return '{vendor}';
                if (parameter === 37446) return '{renderer}';
                return getParameter.call(this, parameter);
            }};

            // 6. toString() 은닉
            const originalToString = Function.prototype.toString;
            Function.prototype.toString = function() {{
                if (this === WebGLRenderingContext.prototype.getParameter) return 'function getParameter() {{ [native code] }}';
                return originalToString.call(this);
            }};
        }})();
    """)

def get_playwright_devices():
    print("🌐 Playwright 기기 데이터베이스 로딩 중...")

    out = {}
    with sync_playwright() as p:
        for name, spec in p.devices.items():
            # ✅ Python(snake_case) / JS(camelCase) 둘 다 호환
            is_mobile = spec.get("is_mobile", spec.get("isMobile", False))
            if not is_mobile:
                continue

            user_agent = spec.get("user_agent", spec.get("userAgent"))
            viewport = spec.get("viewport")
            dsf = spec.get("device_scale_factor", spec.get("deviceScaleFactor", 2))
            has_touch = spec.get("has_touch", spec.get("hasTouch", True))

            # 최소 필수값 체크
            if not user_agent or not viewport:
                continue

            # ✅ StealthMobileBrowser가 기대하는 키로 맞춰서 저장
            out[name] = {
                "user_agent": user_agent,
                "viewport": viewport,
                "device_pixel_ratio": dsf,   # 이름만 맞춰줌
                "has_touch": has_touch,
            }

    print(f"✅ Playwright 모바일 디바이스 로드: {len(out)}개")

    # 디버그(원하면 1~2회만 켜고 끄기)
    if out:
        sample_name = next(iter(out.keys()))
        print(f"🔎 샘플 디바이스: {sample_name} | keys={list(out[sample_name].keys())}")
    else:
        print("⚠️ out이 비었습니다. playwright 버전/설치 상태를 확인하세요.")
    return out

PLAYWRIGHT_DEVICES = get_playwright_devices()

def run():
    # 1. 소스 폴더 하위에 임시 프로필 폴더 설정
    current_dir = Path(__file__).parent
    profile_path = current_dir / f"temp_profile_{int(time.time())}"
    
    print(f"📂 임시 프로필 생성: {profile_path}")

    try:
        get_playwright_devices()
        with sync_playwright() as p:
            # 기기 설정 및 default_browser_type 에러 방지
            device_name = random.choice(list(PLAYWRIGHT_DEVICES.keys()))
            device_config = p.devices[device_name]
            clean_config = {k: v for k, v in device_config.items() if k != 'default_browser_type'}
            vendor, renderer = get_hw_info_by_ua(clean_config['user_agent'])

            # 2. 새로운 프로필 디렉토리로 실행
            context = p.chromium.launch_persistent_context(
                user_data_dir=str(profile_path),
                headless=False,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--exclude-switches=enable-automation"
                ],
                **clean_config
            )

            page = context.pages[0] if context.pages else context.new_page()
            inject_custom_stealth(page, vendor, renderer, clean_config)

            print(f"🌐 {TARGET_URL} 접속 중...")
            page.goto(TARGET_URL)
            
            print("\n✅ 실행 완료. 종료하려면 Enter를 누르세요...")
            input()
            context.close()

    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    finally:
        # 3. 종료 시 임시 폴더 삭제
        if profile_path.exists():
            print(f"🧹 임시 폴더 삭제 중: {profile_path}")
            time.sleep(2)  # 브라우저 완전 종료 대기
            shutil.rmtree(profile_path, ignore_errors=True)
            print("✨ 정리 완료")

if __name__ == "__main__":
    run()