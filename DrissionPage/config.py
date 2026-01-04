import os

# 크롬 실행 경로 및 통신 포트
CHROME_EXE_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
BROWSER_BASE_PORT = 15000

# ✅ 프록시 환경 최적화: 타임아웃 대폭 증가
PAGE_LOAD_TIMEOUT = 300  # 180 → 300초 (5분)
ELEMENT_WAIT_TIMEOUT = 45  # 요소 대기 시간 (초)
NETWORK_IDLE_WAIT = 90  # 네트워크 안정화 대기 (초)

# ✅ 재시도 설정
MAX_RETRIES = 3  # 페이지 로드 재시도 횟수
RETRY_DELAY = 10  # 재시도 간 대기 시간 (초)

# 슬롯 수명 설정
SLOT_LIFE_MIN = 300
SLOT_LIFE_MAX = 600

# 경로 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BROWSER_TEMP_DIR = os.path.join(BASE_DIR, "browser_temp")

# Redis 관련 키
REDIS_ALIVE_KEY = "proxy:alive"
REDIS_LEASE_KEY = "proxy:lease"

# ✅ 프록시 환경 성능 설정
ENABLE_CACHE = False  # 브라우저 캐시 활성화
DISABLE_IMAGES = True  # 이미지 로딩 (False=로딩함, True=차단)
DISABLE_CSS = False  # CSS 로딩
PRELOAD_STRATEGY = "minimal"  # none/minimal/aggressive