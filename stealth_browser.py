"""
Stealth Browser Manager
재사용 가능한 브라우저 관리 클래스
"""
import random
import threading
import time
import tempfile
import os
import shutil
from typing import Dict, Any, Optional, Tuple
from urllib.parse import urlparse

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    NoSuchElementException,
    InvalidSessionIdException,
    NoSuchWindowException,
)


class StealthBrowser:
    """
    스텔스 브라우저 관리 클래스
    - 프로필 기반 브라우저 생성
    - 자동화 감지 우회
    - 세션 관리
    """
    
    # 드라이버 생성 시 동시 접근 방지용 Lock (클래스 변수)
    _driver_creation_lock = threading.Lock()
    
    # Chrome 에러 URL 접두사
    CHROME_ERROR_URL_PREFIXES = (
        "chrome-error://",
        "chrome://error",
    )
    
    # 에러 텍스트 마커
    ERROR_TEXT_MARKERS = (
        "This site can't be reached",
        "ERR_TIMED_OUT",
        "net::ERR_",
        "Connect to network",
    )
    
    def __init__(
        self,
        profile: Dict[str, Any],
        proxy: Optional[str] = None,
        slot_index: int = 0,
        headless: bool = False,
        command_timeout: int = 300,
        load_timeout: int = 300,
        window_width: int = 800,
        window_height: int = 700,
        screen_width: int = 1920,
        screen_height: int = 1080,
        total_slots: int = 1,
    ):
        """
        Args:
            profile: region_profiles.json의 프로필 딕셔너리
            proxy: 프록시 주소 (proto://ip:port 형식)
            slot_index: 슬롯 번호 (창 배치용)
            headless: 헤드리스 모드 여부
            command_timeout: 명령 타임아웃 (초)
            load_timeout: 페이지 로드 타임아웃 (초)
            window_width: 창 너비
            window_height: 창 높이
            screen_width: 전체 화면 너비
            screen_height: 전체 화면 높이
            total_slots: 전체 슬롯 수 (창 배치 계산용)
        """
        self.profile = profile
        self.proxy = proxy
        self.slot_index = slot_index
        self.headless = headless
        self.command_timeout = command_timeout
        self.load_timeout = load_timeout
        self.window_width = window_width
        self.window_height = window_height
        self.screen_width = screen_width
        self.screen_height = screen_height
        self.total_slots = total_slots
        
        self.driver = None
        self.temp_dir = None
    
    @staticmethod
    def normalize_proxy(proxy: Optional[str]) -> Optional[str]:
        """
        프록시 주소를 Chrome이 인식할 수 있는 형태로 정규화
        
        Args:
            proxy: 원본 프록시 주소
            
        Returns:
            정규화된 프록시 주소
        """
        if not proxy:
            return proxy
        
        p = proxy.strip()
        
        # https:// -> http:// 변환 (대부분의 프록시는 HTTP CONNECT)
        if p.startswith("https://"):
            return "http://" + p[len("https://"):]
        
        # socks:// -> socks5:// 변환
        if p.startswith("socks://"):
            return "socks5://" + p[len("socks://"):]
        
        return p
    
    def _calculate_window_position(self) -> Dict[str, int]:
        """
        슬롯별 창 위치 계산
        
        Returns:
            {'x', 'y', 'width', 'height'} 딕셔너리
        """
        if self.total_slots <= 3:
            cols, rows = self.total_slots, 1
        elif self.total_slots <= 4:
            cols, rows = 2, 2
        elif self.total_slots <= 6:
            cols, rows = 3, 2
        else:
            cols = 3
            rows = (self.total_slots + 2) // 3
        
        window_width = self.screen_width // cols
        window_height = self.screen_height // rows
        row = self.slot_index // cols
        col = self.slot_index % cols
        
        return {
            'x': col * window_width,
            'y': row * window_height,
            'width': window_width,
            'height': window_height
        }
    
    def create_driver(self) -> Tuple[Optional[Any], Optional[str]]:
        """
        스텔스 드라이버 생성
        
        Returns:
            (driver, temp_dir) 튜플. 실패 시 (None, None)
        """
        options = uc.ChromeOptions()
        
        # 슬롯별 고유 temp_dir
        self.temp_dir = tempfile.mkdtemp(prefix=f"stealth_browser_{self.slot_index}_")
        options.add_argument(f"--user-data-dir={self.temp_dir}")
        
        # User-Agent 설정
        if "user_agents" in self.profile:
            ua = random.choice(self.profile["user_agents"])
            options.add_argument(f"--user-agent={ua}")
            print(f"[Browser-{self.slot_index}] 🎭 User-Agent: {ua[:80]}...")
        
        # 타임존 및 언어 설정
        options.add_argument(f"--timezone-id={self.profile['timezone']}")
        options.add_argument(f"--lang={self.profile['locale']}")
        
        # 브라우저 설정
        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
            "webrtc.ip_handling_policy": "disable_non_proxied_udp",
            "webrtc.multiple_routes_enabled": False,
            "webrtc.nonproxied_udp_enabled": False,
            "webrtc.udp.max_packet_size": 0,
            "intl.accept_languages": random.choice(self.profile["accept_languages"]),
        }
        options.add_experimental_option("prefs", prefs)
        
        options.add_argument("--disable-quic")
        options.add_argument("--disable-features=NetworkService,NetworkServiceInProcess")
        options.add_argument("--homepage=about:blank")
        options.add_argument("about:blank")
        
        if self.headless:
            options.add_argument("--headless=new")
        
        # 프록시 설정
        if self.proxy:
            normalized_proxy = self.normalize_proxy(self.proxy)
            if normalized_proxy != self.proxy:
                print(f"[Browser-{self.slot_index}] 🔧 Proxy normalized: {self.proxy} → {normalized_proxy}")
            options.add_argument(f"--proxy-server={normalized_proxy}")
        
        # 자동화 감지 우회 옵션
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-first-run")
        
        # 창 위치 설정
        pos = self._calculate_window_position()
        options.add_argument(f"--window-position={pos['x']},{pos['y']}")
        options.add_argument(f"--window-size={pos['width']},{pos['height']}")
        
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        
        # 드라이버 생성 (동시 접근 방지)
        with self._driver_creation_lock:
            try:
                self.driver = uc.Chrome(
                    options=options,
                    use_subprocess=True,
                    command_executor_process_timeout=self.command_timeout,
                )
                self.driver.command_executor.set_timeout(self.command_timeout)
                self.driver.set_page_load_timeout(self.load_timeout)
                
                # 창 크기 랜덤 조정
                self.driver.set_window_size(
                    pos['width'] + random.randint(-50, 50),
                    pos['height'] + random.randint(-50, 50),
                )
                
            except Exception as e:
                print(f"[Browser-{self.slot_index}] ❌ Driver creation failed: {e}")
                self._cleanup_temp_dir()
                return None, None
        
        # CDP 명령으로 자동화 감지 우회
        self._inject_stealth_scripts()
        
        print(f"[Browser-{self.slot_index}] ✅ Driver created successfully")
        return self.driver, self.temp_dir
    
    def _inject_stealth_scripts(self):
        """CDP를 통한 스텔스 스크립트 주입"""
        if not self.driver:
            return
        
        try:
            self.driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {
                    "source": """
                        // Navigator 속성 재정의
                        Object.defineProperty(navigator, 'webdriver', { 
                            get: () => undefined 
                        });
                        
                        Object.defineProperty(navigator, 'plugins', { 
                            get: () => [1, 2, 3, 4, 5] 
                        });
                        
                        Object.defineProperty(navigator, 'languages', { 
                            get: () => ['ko-KR', 'ko', 'en-US', 'en'] 
                        });
                        
                        // Chrome 객체 추가
                        window.chrome = { 
                            runtime: {},
                            loadTimes: function() {},
                            csi: function() {},
                            app: {}
                        };
                        
                        // Permissions 쿼리 오버라이드
                        const originalQuery = window.navigator.permissions.query;
                        window.navigator.permissions.query = (parameters) => (
                            parameters.name === 'notifications' ?
                                Promise.resolve({ state: Notification.permission }) :
                                originalQuery(parameters)
                        );
                        
                        // WebGL Vendor 정보 랜덤화
                        const getParameter = WebGLRenderingContext.prototype.getParameter;
                        WebGLRenderingContext.prototype.getParameter = function(parameter) {
                            if (parameter === 37445) {
                                const vendors = ['Intel Inc.', 'Google Inc.', 'Mozilla'];
                                return vendors[Math.floor(Math.random() * vendors.length)];
                            }
                            if (parameter === 37446) {
                                const renderers = [
                                    'Intel Iris OpenGL Engine',
                                    'ANGLE (Intel, Intel(R) HD Graphics 630 Direct3D11 vs_5_0 ps_5_0)',
                                    'Mesa DRI Intel(R) HD Graphics'
                                ];
                                return renderers[Math.floor(Math.random() * renderers.length)];
                            }
                            return getParameter.apply(this, [parameter]);
                        };
                        
                        // Canvas Fingerprinting 방지
                        const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
                        HTMLCanvasElement.prototype.toDataURL = function() {
                            if (Math.random() < 0.1) {
                                const context = this.getContext('2d');
                                if (context) {
                                    context.fillStyle = 'rgba(' + 
                                        Math.floor(Math.random()*255) + ',' +
                                        Math.floor(Math.random()*255) + ',' +
                                        Math.floor(Math.random()*255) + ',0.01)';
                                    context.fillRect(0, 0, 1, 1);
                                }
                            }
                            return originalToDataURL.apply(this, arguments);
                        };
                        
                        console.debug = () => {};
                    """
                },
            )
            print(f"[Browser-{self.slot_index}] ✅ Stealth scripts injected")
            
        except Exception as e:
            print(f"[Browser-{self.slot_index}] ⚠️ Failed to inject stealth scripts: {e}")
    
    def reset_browser_data(self) -> bool:
        """
        세션 내에서 쿠키, 로컬/세션 스토리지 초기화
        
        Returns:
            성공 여부
        """
        if not self.driver:
            return False
        
        try:
            current_url = self.driver.current_url
            if not current_url or current_url == "data:,":
                try:
                    self.driver.get("about:blank")
                except:
                    print(f"[Browser-{self.slot_index}] ⚠️ Failed to navigate to about:blank")
                    return False
            
            try:
                self.driver.delete_all_cookies()
            except WebDriverException:
                pass
            
            try:
                self.driver.execute_script("window.localStorage.clear();")
            except WebDriverException:
                pass
            
            try:
                self.driver.execute_script("window.sessionStorage.clear();")
            except WebDriverException:
                pass
            
            print(f"[Browser-{self.slot_index}] 🧹 Browser data reset")
            return True
            
        except Exception as e:
            print(f"[Browser-{self.slot_index}] ⚠️ Reset failed: {e.__class__.__name__}")
            return False
    
    def set_referer(self, referer: str) -> bool:
        """
        Referer 헤더 설정
        
        Args:
            referer: Referer URL
            
        Returns:
            성공 여부
        """
        if not self.driver:
            return False
        
        try:
            self.driver.execute_cdp_cmd(
                "Network.setExtraHTTPHeaders",
                {"headers": {"Referer": referer}}
            )
            print(f"[Browser-{self.slot_index}] 🔗 Referer set: {referer}")
            return True
        except Exception as e:
            print(f"[Browser-{self.slot_index}] ⚠️ Failed to set referer: {e}")
            return False
    
    def is_alive(self) -> bool:
        """
        드라이버 세션이 살아있는지 확인
        
        Returns:
            세션 생존 여부
        """
        if not self.driver:
            return False
        
        try:
            handles = self.driver.window_handles
            if not handles:
                return False
            
            self.driver.execute_script("return 1;")
            return True
        except (InvalidSessionIdException, NoSuchWindowException, WebDriverException):
            return False
    
    def page_looks_like_error(self) -> bool:
        """
        현재 페이지가 에러 페이지인지 확인
        
        Returns:
            에러 페이지 여부
        """
        if not self.driver:
            return True
        
        # 1) Chrome 에러 페이지 URL 확인
        try:
            cur = (self.driver.current_url or "").lower()
            if any(cur.startswith(p) for p in self.CHROME_ERROR_URL_PREFIXES):
                return True
        except Exception:
            pass
        
        # 2) 화면 텍스트로 감지
        try:
            body = self.driver.find_element(By.TAG_NAME, "body")
            txt = (body.text or "")
            if any(m in txt for m in self.ERROR_TEXT_MARKERS):
                return True
        except Exception:
            pass
        
        # 3) page_source로 추가 감지
        try:
            src = self.driver.page_source or ""
            if any(m in src for m in self.ERROR_TEXT_MARKERS):
                return True
        except Exception:
            pass
        
        # 4) 프록시 서버 에러 감지
        try:
            url = self.driver.current_url or ""
            host = urlparse(url).hostname or ""
            if "connectivitycheck.gstatic.com" == host:
                return True
        except Exception:
            pass
        
        return False
    
    def safe_get(self, url: str, page_load_timeout: float = 30.0) -> bool:
        """
        안전한 페이지 로딩 (에러 감지 포함)
        
        Args:
            url: 로드할 URL
            page_load_timeout: 페이지 로드 타임아웃 (초)
            
        Returns:
            성공 여부 (True: 정상 페이지, False: 실패/에러 페이지)
        """
        if not self.driver:
            return False
        
        try:
            self.driver.set_page_load_timeout(page_load_timeout)
        except Exception:
            pass
        
        try:
            self.driver.get(url)
        except TimeoutException:
            print(f"[Browser-{self.slot_index}] ⚠️ Page load timeout")
            return False
        except WebDriverException as e:
            msg = str(e)
            if "net::ERR_" in msg or "ERR_TIMED_OUT" in msg or "timeout" in msg.lower():
                print(f"[Browser-{self.slot_index}] ⚠️ Network error: {msg[:160]}")
                return False
            print(f"[Browser-{self.slot_index}] ⚠️ WebDriverException: {msg[:160]}")
            return False
        
        # 에러 페이지 확인
        if self.page_looks_like_error():
            print(f"[Browser-{self.slot_index}] ⚠️ Error page detected")
            return False
        
        return True
    
    def ensure_page_ready(self, timeout: int = 120) -> bool:
        """
        페이지가 완전히 로드될 때까지 대기
        
        Args:
            timeout: 대기 타임아웃 (초)
            
        Returns:
            성공 여부
        """
        if not self.driver:
            return False
        
        def _page_ready(driver):
            ready = driver.execute_script("return document.readyState") == "complete"
            if not ready:
                return False
            
            bodies = driver.find_elements(By.TAG_NAME, "body")
            if not bodies or not any(b.is_displayed() for b in bodies):
                return False
            
            is_error = driver.execute_script(
                """
                const href  = window.location.href || '';
                const title = document.title || '';
                const text  = document.body ? document.body.innerText : '';

                if (href.startsWith('chrome-error://')) return true;

                if (text.includes('ERR_TIMED_OUT') ||
                    text.includes('ERR_CONNECTION_TIMED_OUT')) return true;

                if (text.includes("This site can't be reached")) return true;

                if (text.includes("사이트에 연결할 수 없음") ||
                    text.includes("사이트에 접속할 수 없습니다")) return true;

                return false;
                """
            )
            if is_error:
                return False
            return True
        
        try:
            WebDriverWait(self.driver, timeout).until(_page_ready)
            return True
        except (TimeoutException, WebDriverException):
            return False
    
    def click_youtube_consent(self, timeout: int = 8) -> bool:
        """
        유튜브 동의 페이지 자동 처리
        
        Args:
            timeout: 대기 타임아웃 (초)
            
        Returns:
            처리 성공 여부
        """
        if not self.driver:
            return False
        
        try:
            url = self.driver.current_url
            host = urlparse(url).hostname or ""
            if "consent.youtube.com" not in host:
                print(f"[Browser-{self.slot_index}] Not a consent page, skipping")
                return False
            
            forms = self.driver.find_elements(
                By.CSS_SELECTOR,
                "form[action='https://consent.youtube.com/save']",
            )
            if not forms:
                print(f"[Browser-{self.slot_index}] No consent form found")
                return False
            
            btn = WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable(
                    (
                        By.CSS_SELECTOR,
                        "form[action='https://consent.youtube.com/save'] button[jsname='b3VHJd']",
                    )
                )
            )
            btn.click()
            print(f"[Browser-{self.slot_index}] ✅ YouTube consent accepted")
            return True
            
        except (TimeoutException, NoSuchElementException):
            print(f"[Browser-{self.slot_index}] ⚠️ Consent button not found")
            return False
        except Exception as e:
            print(f"[Browser-{self.slot_index}] ⚠️ Consent handling error: {e}")
            return False
    
    def _cleanup_temp_dir(self):
        """임시 디렉토리 정리"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            for attempt in range(3):
                try:
                    shutil.rmtree(self.temp_dir)
                    print(f"[Browser-{self.slot_index}] 🧹 Temp dir removed: {self.temp_dir}")
                    break
                except PermissionError:
                    if attempt < 2:
                        print(f"[Browser-{self.slot_index}] ⚠️ Retry cleanup {attempt + 1}/3")
                        time.sleep(2)
                    else:
                        print(f"[Browser-{self.slot_index}] ⚠️ Failed to cleanup temp dir")
                except Exception as e:
                    print(f"[Browser-{self.slot_index}] ⚠️ Cleanup error: {e}")
                    break
    
    def close(self):
        """드라이버 종료 및 정리"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
        
        time.sleep(2)
        self._cleanup_temp_dir()
    
    def __enter__(self):
        """Context manager 지원"""
        self.create_driver()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager 지원"""
        self.close()
        return False