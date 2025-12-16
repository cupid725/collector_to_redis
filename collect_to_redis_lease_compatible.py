import time
import requests
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import lru_cache
from collections import Counter

import redis  # pip install redis
import threading
# SOCKS 프록시 사용 시: pip install "requests[socks]"

# ================= 전역 중단 신호 =================
STOP_EVENT = threading.Event()

# ================= Redis 설정 =================
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None  # 필요하면 문자열로 설정

REDIS_ZSET_ALIVE = "proxies:alive"  # 살아있는 프록시 모음 (score=next_available_epoch, lease 방식과 호환)
REDIS_ZSET_LEASE = "proxies:lease"  # 사용 중(임대) 프록시 모음 (score=lease_expire_epoch)
REDIS_KEY_PREFIX = "proxy"          # proxy:{protocol}:{address}

# ================= 수집/테스트 주기 설정 =================
COLLECT_INTERVAL_MINUTES = 240   # 240분(4시간)마다 한 번 수집

# 개별 프록시 정보 TTL(초) – 수집 주기의 3배 정도로 넉넉하게
PROXY_TTL_SECONDS = COLLECT_INTERVAL_MINUTES * 3 * 60

# 테스트할 프록시 최대 개수 제한 (None이면 전체)
MAX_TOTAL_PROXIES: Optional[int] = None  # 예: 500

# ================= 프록시 리스트 소스 =================

HTTP_PROXY_LIST_URL = (
    "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt"
)

SOCKS5_PROXY_LIST_URL_SPEEDX = (
    "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt"
)

SOCKS5_PROXY_LIST_URL_PROXIFLY = (
    "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt"
)

# (추가) vakhov/fresh-proxy-list (줄바꿈 ip:port 형식)
# 사용자가 준 GitHub "blob" URL은 HTML이라 raw로 받는 게 안전합니다.
VAKHOV_SOCKS4_URL = "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt"
VAKHOV_SOCKS5_URL = "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt"
VAKHOV_HTTP_URL = "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt"
VAKHOV_HTTPS_URL = "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/https.txt"

# ================= 테스트 설정 =================

# HTTP와 HTTPS 혼합 (HTTP 프록시 호환성 향상)
IP_CHECK_URLS = [
    ("http://api.ipify.org?format=text", "http"),
    ("http://icanhazip.com", "http"),
    ("http://ifconfig.me/ip", "http"),
    ("http://checkip.amazonaws.com", "http"),
    # HTTPS 백업 (SOCKS나 터널링 지원 프록시용)
    ("https://api.ipify.org?format=text", "https"),
    ("https://icanhazip.com", "https"),
]

CONNECT_TIMEOUT = 12  # 연결 타임아웃 (초)
READ_TIMEOUT = 12      # 읽기 타임아웃 (초)
MAX_WORKERS = 40      # 프록시 테스트 쓰레드 수
RR_TEST_RUNS = 3      # 한 프록시당 IP 체크 반복 횟수

# GeoIP 조회용 URL
GEOIP_URL = "http://ip-api.com/json/{ip}?fields=status,country,countryCode,query,message"

# ======================================================
# Redis 유틸
# ======================================================

def get_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )


def make_proxy_key(protocol: str, address: str) -> str:
    """proxy:http:1.2.3.4:8080 또는 proxy:socks5:5.6.7.8:1080"""
    return f"{REDIS_KEY_PREFIX}:{protocol}:{address}"


# ======================================================
# GeoIP 조회
# ======================================================

@lru_cache(maxsize=1000)
def get_ip_country(ip: str) -> str:
    """IP의 국가 정보 반환: 'Netherlands (NL)' 또는 'Unknown'"""
    try:
        resp = requests.get(
            GEOIP_URL.format(ip=ip),
            timeout=(5, 5),
            headers={"User-Agent": "Mozilla/5.0"}
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") == "success":
            country = data.get("country")
            code = data.get("countryCode")
            if country and code:
                return f"{country} ({code})"
            elif country:
                return country
    except Exception:
        pass
    return "Unknown"


# ======================================================
# 프록시 리스트 수집
# ======================================================

def _normalize_addr(line: str) -> Optional[str]:
    """
    다양한 입력(단순 ip:port, http://ip:port, https://ip:port)을 ip:port로 정규화.
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    # 형식: http://ip:port, https://ip:port 또는 단순 ip:port
    if line.startswith("http://") or line.startswith("https://"):
        addr = line.split("://", 1)[1]
    else:
        addr = line

    addr = addr.split("/")[0].strip()
    if ":" not in addr:
        return None
    return addr


def fetch_http_proxy_list(url: str) -> List[Dict]:
    if STOP_EVENT.is_set():
        return []
    print(f"📥 HTTP 프록시 목록 다운로드: {url}")
    proxies: List[Dict] = []
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        for line in resp.text.strip().splitlines():
            if STOP_EVENT.is_set():
                break
            addr = _normalize_addr(line)
            if not addr:
                continue
            proxies.append({
                "address": addr,
                "protocol": "http",
                "source": "proxifly_http",
            })
        print(f"✅ HTTP 프록시 {len(proxies)}개 수집\n")
    except Exception as e:
        if not STOP_EVENT.is_set():
            print(f"❌ HTTP 프록시 목록 다운로드 실패: {e}")
    return proxies


def fetch_socks5_proxy_list(url: str, source_name: str) -> List[Dict]:
    if STOP_EVENT.is_set():
        return []
    print(f"📥 SOCKS5 프록시 목록 다운로드: {url} (source={source_name})")
    proxies: List[Dict] = []
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        for line in resp.text.strip().splitlines():
            if STOP_EVENT.is_set():
                break
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            addr = parts[0].strip()
            if ":" not in addr:
                continue
            proxies.append({
                "address": addr,
                "protocol": "socks5",
                "source": source_name,
            })
        print(f"✅ SOCKS5 프록시 {len(proxies)}개 수집 (source={source_name})\n")
    except Exception as e:
        if not STOP_EVENT.is_set():
            print(f"❌ SOCKS5 프록시 목록 다운로드 실패 ({source_name}): {e}")
    return proxies


def fetch_plain_proxy_list(url: str, protocol: str, source_name: str) -> List[Dict]:
    """
    줄바꿈 ip:port 형식(또는 주석 포함)을 받아서 protocol별로 리스트화.
    - vakhov/fresh-proxy-list 형식 지원
    """
    if STOP_EVENT.is_set():
        return []
    print(f"📥 {protocol.upper()} 프록시 목록 다운로드: {url} (source={source_name})")
    proxies: List[Dict] = []
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        for line in resp.text.strip().splitlines():
            if STOP_EVENT.is_set():
                break
            addr = _normalize_addr(line)
            if not addr:
                continue
            proxies.append({
                "address": addr,
                "protocol": protocol,
                "source": source_name,
            })
        print(f"✅ {protocol.upper()} 프록시 {len(proxies)}개 수집 (source={source_name})\n")
    except Exception as e:
        if not STOP_EVENT.is_set():
            print(f"❌ {protocol.upper()} 프록시 목록 다운로드 실패 ({source_name}): {e}")
    return proxies


def fetch_all_proxies() -> List[Dict]:
    if STOP_EVENT.is_set():
        return []

    http_proxies = fetch_http_proxy_list(HTTP_PROXY_LIST_URL)
    s5_speedx = fetch_socks5_proxy_list(SOCKS5_PROXY_LIST_URL_SPEEDX, "speedx_socks5")
    s5_proxifly = fetch_socks5_proxy_list(SOCKS5_PROXY_LIST_URL_PROXIFLY, "proxifly_socks5")

    # (추가) vakhov/fresh-proxy-list 4종
    vakhov_s4 = fetch_plain_proxy_list(VAKHOV_SOCKS4_URL, "socks4", "vakhov_socks4")
    vakhov_s5 = fetch_plain_proxy_list(VAKHOV_SOCKS5_URL, "socks5", "vakhov_socks5")
    vakhov_http = fetch_plain_proxy_list(VAKHOV_HTTP_URL, "http", "vakhov_http")
    # "https.txt"는 보통 "HTTPS 사이트 접속 가능한 HTTP 프록시" 리스트입니다.
    # 분리해서 보관하고 싶어 protocol을 "https"로 저장하되,
    # 실제 requests 사용 시에는 HTTP 프록시로 처리(아래 build_requests_proxies 참고).
    vakhov_https = fetch_plain_proxy_list(VAKHOV_HTTPS_URL, "https", "vakhov_https")

    raw = http_proxies + s5_speedx + s5_proxifly + vakhov_s4 + vakhov_s5 + vakhov_http + vakhov_https

    # protocol + address 기준 중복 제거
    unique: Dict[tuple, Dict] = {}
    for p in raw:
        key = (p["protocol"], p["address"])
        if key not in unique:
            unique[key] = p

    all_proxies = list(unique.values())

    print("📦 프록시 집계 (중복 제거 후):")
    print(f"  • HTTP              : {len(http_proxies) + len(vakhov_http)} (proxifly_http + vakhov_http)")
    print(f"  • HTTPS             : {len(vakhov_https)} (vakhov_https)")
    print(f"  • SOCKS4            : {len(vakhov_s4)} (vakhov_socks4)")
    print(f"  • SOCKS5 SpeedX     : {len(s5_speedx)}")
    print(f"  • SOCKS5 Proxifly   : {len(s5_proxifly)}")
    print(f"  • SOCKS5 vakhov     : {len(vakhov_s5)}")
    print(f"  → Uniq 총합         : {len(all_proxies)}")

    if MAX_TOTAL_PROXIES is not None and len(all_proxies) > MAX_TOTAL_PROXIES:
        print(f"  ⚠️ 너무 많아서 {MAX_TOTAL_PROXIES}개까지만 사용합니다.")
        all_proxies = all_proxies[:MAX_TOTAL_PROXIES]

    print(f"  ▶ 실제 테스트 대상  : {len(all_proxies)}개\n")
    return all_proxies


# ======================================================
# 프록시 테스트
# ======================================================

def build_requests_proxies(proxy_info: Dict) -> Dict[str, str]:
    addr = proxy_info["address"]
    protocol = proxy_info["protocol"]

    if protocol == "http":
        proxy_url = f"http://{addr}"
    elif protocol == "https":
        # "https 프록시 리스트"는 대개 HTTP 프록시(HTTPS 사이트 CONNECT 가능)를 의미합니다.
        # 실제 프록시 접속 스킴은 http:// 로 두는 게 호환성이 좋습니다.
        proxy_url = f"http://{addr}"
    elif protocol == "socks5":
        proxy_url = f"socks5://{addr}"
    elif protocol == "socks4":
        proxy_url = f"socks4://{addr}"
    else:
        raise ValueError(f"Unknown protocol: {protocol}")

    return {
        "http": proxy_url,
        "https": proxy_url,
    }


def check_ip_once(proxy_info: Dict) -> Optional[Tuple[str, str]]:
    """
    프록시를 통해 IP 체크
    Returns: (ip, service_url) 또는 None
    """
    if STOP_EVENT.is_set():
        return None

    proxies = build_requests_proxies(proxy_info)

    for url, protocol in IP_CHECK_URLS:
        if STOP_EVENT.is_set():
            return None
        try:
            r = requests.get(
                url,
                proxies=proxies,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                },
            )
            r.raise_for_status()
            ip = r.text.strip()
            # 기본적인 IP 형식 체크
            if ip and ('.' in ip or ':' in ip) and len(ip) < 50:
                return (ip, url)
        except Exception:
            continue

        # 실패 시 짧은 대기 후 다음 시도
        time.sleep(0.3)

    return None


def test_proxy(proxy_info: Dict) -> Dict:
    """
    프록시를 RR_TEST_RUNS번 테스트하고 결과 반환
    {
        "ok": True/False,
        "latency_ms": float or None,
        "ips": ["1.2.3.4", ...],
        "proxy_type": "Static" / "Full Rotating" / "Partial Rotating" / "Unknown",
        "countries": ["South Korea (KR)", ...],
        "error": str or None
    }
    """
    ips: List[str] = []
    services_used: List[str] = []
    start = time.time()
    last_error = None

    for i in range(RR_TEST_RUNS):
        if STOP_EVENT.is_set():
            break

        result = check_ip_once(proxy_info)
        if result:
            ip, service = result
            ips.append(ip)
            services_used.append(service)
        else:
            last_error = "All IP check services failed"

        # 다음 테스트 전 짧은 대기
        if i < RR_TEST_RUNS - 1 and not STOP_EVENT.is_set():
            time.sleep(0.5)

    if STOP_EVENT.is_set():
        return {
            "ok": False,
            "latency_ms": None,
            "ips": [],
            "proxy_type": "Interrupted",
            "countries": [],
            "error": "Interrupted by stop signal",
        }

    elapsed = (time.time() - start) * 1000.0  # ms

    if not ips:
        return {
            "ok": False,
            "latency_ms": None,
            "ips": [],
            "proxy_type": "Unknown",
            "countries": [],
            "error": last_error or "No response",
        }

    unique_ips = list(set(ips))
    cnt = len(ips)
    uniq_cnt = len(unique_ips)

    # 프록시 타입 판단
    if uniq_cnt == 1:
        proxy_type = "Static"
    elif uniq_cnt == cnt and cnt >= 3:
        proxy_type = "Full Rotating"
    elif uniq_cnt > 1:
        proxy_type = "Partial Rotating"
    else:
        proxy_type = "Unknown"

    # 각 IP의 국가 정보 수집
    countries = [get_ip_country(ip) for ip in unique_ips]

    return {
        "ok": True,
        "latency_ms": elapsed / cnt,  # 평균 레이턴시
        "ips": unique_ips,
        "proxy_type": proxy_type,
        "countries": countries,
        "error": None,
    }


# ======================================================
# Redis 저장
# ======================================================

def store_proxy_to_redis(r: redis.Redis, proxy_info: Dict, test_result: Dict):
    protocol = proxy_info["protocol"]
    address = proxy_info["address"]
    source = proxy_info.get("source", "")

    key = make_proxy_key(protocol, address)
    now = datetime.utcnow().isoformat()

    if not test_result["ok"]:
        # 실패한 프록시
        r.hset(
            key,
            mapping={
                "protocol": protocol,
                "address": address,
                "source": source,
                "status": "dead",
                "last_fail": now,
                "proxy_type": test_result.get("proxy_type", "Unknown"),
                "error": test_result.get("error", "Unknown"),
            },
        )
        r.zrem(REDIS_ZSET_ALIVE, f"{protocol}://{address}")
        r.zrem(REDIS_ZSET_LEASE, f"{protocol}://{address}")  # lease에 잡혀있던 것도 정리
        r.expire(key, PROXY_TTL_SECONDS)
        return

    # 성공한 프록시
    latency_ms = test_result["latency_ms"] or 999999
    proxy_type = test_result["proxy_type"]
    ips = ",".join(test_result["ips"])
    countries = ",".join(test_result["countries"])

    r.hset(
        key,
        mapping={
            "protocol": protocol,
            "address": address,
            "source": source,
            "status": "alive",
            "proxy_type": proxy_type,
            "latency_ms": f"{latency_ms:.1f}",
            "last_ok": now,
            "ips": ips,
            "countries": countries,
        },
    )
    r.expire(key, PROXY_TTL_SECONDS)

    # alive 풀에 추가
    # lease 방식(client_from_redis_lease.py)과 호환되도록 score는 "다음 사용 가능 시각(epoch)" 개념을 사용합니다.
    # collector는 보통 0(즉시 사용 가능)로 추가만 하고, 재사용 쿨다운/백오프는 client가 score를 갱신합니다.
    member = f"{protocol}://{address}"

    # 이미 lease(사용 중)에 잡혀있다면 alive에 다시 넣지 않습니다(중복 배정 방지).
    if r.zscore(REDIS_ZSET_LEASE, member) is None:
        # NX로만 추가해서, client가 설정한 cooldown(score)을 collector가 덮어쓰지 않게 함
        try:
            r.zadd(REDIS_ZSET_ALIVE, {member: 0}, nx=True)
        except TypeError:
            # 구버전 redis-py 호환: execute_command로 NX 사용
            r.execute_command("ZADD", REDIS_ZSET_ALIVE, "NX", 0, member)

# ======================================================
# 한 번 수집+테스트 실행
# ======================================================

def process_one_proxy(idx: int, total: int, proxy_info: Dict, r: redis.Redis) -> Dict:
    """
    한 개 프록시 테스트 및 저장
    Returns: 결과 통계용 딕셔너리
    """
    if STOP_EVENT.is_set():
        return {"status": "skipped", "protocol": proxy_info["protocol"]}

    address = proxy_info["address"]
    protocol = proxy_info["protocol"]

    # 간결한 로그 (진행 상황만)
    if idx % 10 == 0 or idx == total:
        print(f"[{idx}/{total}] 진행 중... (최근: {protocol.upper()}://{address})")

    try:
        result = test_proxy(proxy_info)
    except Exception as e:
        result = {
            "ok": False,
            "latency_ms": None,
            "ips": [],
            "proxy_type": "Unknown",
            "countries": [],
            "error": str(e)[:100]
        }

    if STOP_EVENT.is_set():
        return {"status": "interrupted", "protocol": protocol}

    store_proxy_to_redis(r, proxy_info, result)

    return {
        "status": "alive" if result["ok"] else "dead",
        "protocol": protocol,
        "latency_ms": result.get("latency_ms"),
        "proxy_type": result.get("proxy_type"),
    }


def collect_once():
    """프록시 수집 + 테스트 + Redis 업데이트를 한 번 수행"""
    if STOP_EVENT.is_set():
        print("⏹ collect_once 호출 시 이미 중단 신호가 설정되어 있음. 스킵.")
        return

    start_dt = datetime.now()

    print("=" * 80)
    print(f"🕒 수집 작업 시작: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    r = get_redis()
    proxies = fetch_all_proxies()
    total = len(proxies)

    if STOP_EVENT.is_set():
        print("⏹ 수집 중단 신호로 인해 테스트를 시작하지 않습니다.")
        return

    if not total:
        print("❌ 수집된 프록시가 없습니다. 작업 종료.")
        return

    print(f"🔍 총 {total}개 프록시 테스트 시작 (workers={MAX_WORKERS})")
    print(f"⏱️  타임아웃: 연결 {CONNECT_TIMEOUT}초 / 읽기 {READ_TIMEOUT}초\n")

    start = time.time()
    idx = 0
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for p in proxies:
            if STOP_EVENT.is_set():
                print("\n⏹ 중단 신호 감지, 나머지 프록시는 제출하지 않습니다.")
                break
            idx += 1
            futures.append(executor.submit(process_one_proxy, idx, total, p, r))

        # 결과 수집
        for f in as_completed(futures):
            if STOP_EVENT.is_set():
                break
            try:
                result = f.result()
                results.append(result)
            except Exception as e:
                print(f"⚠️ 쓰레드 처리 중 예외: {e}")
                results.append({"status": "error", "protocol": "unknown"})

    elapsed = time.time() - start
    end_dt = datetime.now()

    # 통계 출력
    print("\n" + "=" * 80)
    print("📊 테스트 결과 통계")
    print("=" * 80)

    status_counts = Counter(r["status"] for r in results)
    protocol_counts = Counter(r["protocol"] for r in results)

    alive_count = status_counts.get("alive", 0)
    dead_count = status_counts.get("dead", 0)
    total_tested = len(results)
    success_rate = (alive_count / total_tested * 100) if total_tested > 0 else 0

    print(f"✅ 성공: {alive_count}개 ({success_rate:.1f}%)")
    print(f"❌ 실패: {dead_count}개")
    print(f"⏹  중단/에러: {status_counts.get('skipped', 0) + status_counts.get('interrupted', 0) + status_counts.get('error', 0)}개")

    print(f"\n📋 프로토콜별 통계:")
    for proto, count in protocol_counts.most_common():
        proto_alive = sum(1 for r in results if r["protocol"] == proto and r["status"] == "alive")
        print(f"  • {proto.upper():8s}: {proto_alive}/{count} alive")

    # Redis alive 풀 현황
    redis_alive = r.zcard(REDIS_ZSET_ALIVE)
    print(f"\n💾 Redis alive 풀: {redis_alive}개 (key={REDIS_ZSET_ALIVE})")

    # 상위 10개 프록시 (가장 빨리 사용 가능한 순: next_available_epoch 기준)
    top_proxies = r.zrange(REDIS_ZSET_ALIVE, 0, 9, withscores=True)
    if top_proxies:
        print(f"\n🏆 레이턴시 상위 10개 프록시:")
        for proxy_str, latency in top_proxies:
            # proxy 정보 가져오기
            protocol, addr = proxy_str.split("://", 1)
            pkey = make_proxy_key(protocol, addr)
            pinfo = r.hgetall(pkey)
            countries = pinfo.get("countries", "Unknown")
            print(f"  • {proxy_str:30s} | {latency:6.1f}ms | {countries}")

    print(f"\n⏱️  소요시간: {elapsed:.1f}초")
    print(f"✅ 완료 시각: {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()


# ======================================================
# 데몬 루프
# ======================================================

def main_loop():
    print("=" * 80)
    print("🚀 Redis 프록시 수집 데몬")
    print("=" * 80)
    print(f"⏱️  주기: {COLLECT_INTERVAL_MINUTES}분마다 한 번 수집/테스트")
    print(f"🧪 최대 테스트 프록시 수: {MAX_TOTAL_PROXIES if MAX_TOTAL_PROXIES is not None else '제한 없음'}")
    print(f"🔧 동시 작업 스레드: {MAX_WORKERS}개")
    print(f"🌍 IP 체크: HTTP 우선, HTTPS 백업 전략")
    print("🛑 언제든지 Ctrl + C로 중단 가능")
    print("=" * 80)
    print()

    try:
        # 시작하자마자 한 번 실행
        collect_once()

        # 이후 주기적으로 반복
        while not STOP_EVENT.is_set():
            print(f"💤 {COLLECT_INTERVAL_MINUTES}분 대기 후 다음 수집 실행...")
            print(f"   (현재 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")

            # 1초 단위로 잘게 쪼개서 중간에 Ctrl+C 누르면 바로 반응
            total_sleep = COLLECT_INTERVAL_MINUTES * 60
            for i in range(total_sleep):
                if STOP_EVENT.is_set():
                    break
                # 1분마다 진행 상황 표시
                if i > 0 and i % 60 == 0:
                    remaining_min = (total_sleep - i) // 60
                    print(f"   ⏳ 대기 중... (남은 시간: {remaining_min}분)")
                time.sleep(1)

            if STOP_EVENT.is_set():
                break
            collect_once()

    except KeyboardInterrupt:
        print("\n🛑 KeyboardInterrupt (Ctrl+C) 감지, 중단 신호 설정.")
        STOP_EVENT.set()
        print("⏳ 실행 중인 작업이 완료될 때까지 잠시 기다려주세요...")

    finally:
        print("🔚 collector_redis.py 종료 완료.")


if __name__ == "__main__":
    main_loop()
