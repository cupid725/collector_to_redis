import random
import threading
import time
import subprocess
import redis
from appium import webdriver
from appium.options.android import UiAutomator2Options

# 전역 설정
NUM_BROWSERS = 2
APPIUM_CONFIGS = [
    {"appium_server": "http://127.0.0.1:4723", "device_name": "127.0.0.1:62001"},
    {"appium_server": "http://127.0.0.1:4723", "device_name": "127.0.0.1:62025"},
]
TARGET_URLS = ["https://www.youtube.com/shorts/5y-_oaunCCQ?feature=share"]

driver_creation_lock = threading.Lock()
stop_event = threading.Event()

def get_redis():
    return redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

def check_adb_connection(device_name):
    """장치가 오프라인이면 재연결 시도"""
    try:
        res = subprocess.run(f"adb -s {device_name} shell getprop sys.boot_completed", 
                             shell=True, capture_output=True, timeout=5)
        if b"1" not in res.stdout:
            subprocess.run(f"adb connect {device_name}", shell=True)
    except:
        subprocess.run(f"adb connect {device_name}", shell=True)

def create_driver(proxy, config, thread_id):
    device_name = config["device_name"]
    check_adb_connection(device_name)

    options = UiAutomator2Options()
    options.platform_name = "Android"
    options.udid = device_name
    
    # [핵심] ADB 데몬 붕괴 방지 옵션
    options.set_capability("appium:suppressKillServer", True)
    options.set_capability("appium:adbExecTimeout", 60000)
    options.set_capability("appium:uiautomator2ServerLaunchTimeout", 60000)
    
    # Chrome 직접 실행 모드
    options.set_capability("appium:appPackage", "com.android.chrome")
    options.set_capability("appium:appActivity", "com.google.android.apps.chrome.Main")
    options.set_capability("appium:noReset", True)
    options.set_capability("appium:systemPort", 8200 + thread_id)

    with driver_creation_lock:
        try:
            print(f"[Bot-{thread_id}] ⏳ 세션 연결 시작: {device_name}")
            driver = webdriver.Remote(config["appium_server"], options=options)
            return driver
        except Exception as e:
            print(f"[Bot-{thread_id}] ❌ 드라이버 생성 에러: {e}")
            return None

def worker(index, config, r):
    device_name = config["device_name"]
    while not stop_event.is_set():
        # Redis에서 프록시 가져오기 (이전 Claude 로직 활용)
        proxy = r.eval("return redis.call('ZRANGE', KEYS[1], 0, 0)[1]", 1, "proxies:alive")
        if not proxy:
            time.sleep(10); continue
            
        driver = create_driver(proxy, config, index)
        if driver:
            try:
                url = random.choice(TARGET_URLS)
                print(f"[Bot-{index}] 🚀 접속: {url}")
                driver.get(url)
                time.sleep(random.randint(30, 60))
            except Exception as e:
                print(f"[Bot-{index}] 실행 오류: {e}")
            finally:
                try: driver.quit()
                except: pass
        time.sleep(5)

if __name__ == "__main__":
    r = get_redis()
    threads = []
    for i in range(NUM_BROWSERS):
        t = threading.Thread(target=worker, args=(i, APPIUM_CONFIGS[i], r))
        t.start()
        threads.append(t)
    
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()