import os

# 크롬 실행 경로 및 통신 포트
CHROME_EXE_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
BROWSER_BASE_PORT = 15000

# 타임아웃 및 슬롯 수명 설정 (초 단위)
PAGE_LOAD_TIMEOUT = 180
SLOT_LIFE_MIN = 300  # 5분
SLOT_LIFE_MAX = 600  # 10분

# 경로 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BROWSER_TEMP_DIR = os.path.join(BASE_DIR, "browser_temp")

# Redis 관련 키
REDIS_ALIVE_KEY = "proxy:alive"
REDIS_LEASE_KEY = "proxy:lease"