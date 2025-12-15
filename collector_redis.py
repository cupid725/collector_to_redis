import time
import requests
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

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

REDIS_ZSET_ALIVE = "proxies:alive"  # 살아있는 프록시 모음 (score=latency)
REDIS_KEY_PREFIX = "proxy"          # proxy:{protocol}:{address}

# ================= 수집/테스트 주기 설정 =================
# 몇 분마다 한 번씩 전체 수집+테스트를 돌릴지
COLLECT_INTERVAL_MINUTES = 240   # 60분마다 한 번 수집

# 개별 프록시 정보 TTL(초) – 수집 주기의 3배 정도로 넉넉하게
PROXY_TTL_SECONDS = COLLECT_INTERVAL_MINUTES * 3 * 60  # 3시간

# 너무 오래 걸리면, 테스트할 프록시 최대 개수 제한 (None이면 전체)
MAX_TOTAL_PROXIES: Optional[int] = None  # 예: 500 으로 두면 500개까지만 테스트

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

# ================= 테스트 설정 =================

IP_CHECK_URLS = [
    "https://api.ipify.org?format=text",
    "https://icanhazip.com",
    "https://ifconfig.me/ip",
    "https://checkip.amazonaws.com",
]

REQUEST_TIMEOUT = 10  # 초
MAX_WORKERS = 50      # 프록시 테스트 쓰레드 수
RR_TEST_RUNS = 5      # 한 프록시당 IP 체크 반복 횟수 (회전 여부 판단용)

# ======================================================
# Redis 유틸
# ======================================================

def get_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,  # str로 받기
    )


def make_proxy_key(protocol: str, address: str) -> str:
    """
    proxy:http:1.2.3.4:8080
    proxy:socks5:5.6.7.8:1080
    """
    return f"{REDIS_KEY_PREFIX}:{protocol}:{address}"


# ======================================================
# 프록시 리스트 수집
# ======================================================

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
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # 형식: http://ip:port, https://ip:port 또는 단순 ip:port
            if line.startswith("http://") or line.startswith("https://"):
                addr = line.split("://", 1)[1]
            else:
                addr = line

            # 뒤에 path 붙어 있으면 제거
            addr = addr.split("/")[0]
            if ":" not in addr:
                continue

            proxies.append(
                {
                    "address": addr,
                    "protocol": "http",
                    "source": "proxifly_http",
                }
            )
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
            # 보통 "ip:port" 한 줄
            parts = line.split()
            addr = parts[0]
            if ":" not in addr:
                continue
            proxies.append(
                {
                    "address": addr,
                    "protocol": "socks5",
                    "source": source_name,
                }
            )
        print(f"✅ SOCKS5 프록시 {len(proxies)}개 수집 (source={source_name})\n")
    except Exception as e:
        if not STOP_EVENT.is_set():
            print(f"❌ SOCKS5 프록시 목록 다운로드 실패 ({source_name}): {e}")
    return proxies


def fetch_all_proxies() -> List[Dict]:
    if STOP_EVENT.is_set():
        return []

    http_proxies = fetch_http_proxy_list(HTTP_PROXY_LIST_URL)
    s5_speedx = fetch_socks5_proxy_list(SOCKS5_PROXY_LIST_URL_SPEEDX, "speedx_socks5")
    s5_proxifly = fetch_socks5_proxy_list(SOCKS5_PROXY_LIST_URL_PROXIFLY, "proxifly_socks5")

    raw = http_proxies + s5_speedx + s5_proxifly

    # protocol + address 기준 중복 제거
    unique: Dict[tuple, Dict] = {}
    for p in raw:
        key = (p["protocol"], p["address"])
        if key not in unique:
            unique[key] = p

    all_proxies = list(unique.values())

    print("📦 프록시 집계 (중복 제거 후):")
    print(f"  • HTTP           : {len(http_proxies)}")
    print(f"  • SOCKS5 SpeedX  : {len(s5_speedx)}")
    print(f"  • SOCKS5 Proxifly: {len(s5_proxifly)}")
    print(f"  → Uniq 총합      : {len(all_proxies)}")

    # 너무 많으면 상단 일부만 사용 (선택 사항)
    if MAX_TOTAL_PROXIES is not None and len(all_proxies) > MAX_TOTAL_PROXIES:
        print(f"  ⚠️ 너무 많아서 {MAX_TOTAL_PROXIES}개까지만 사용합니다.")
        all_proxies = all_proxies[:MAX_TOTAL_PROXIES]

    print(f"  ▶ 실제 테스트 대상: {len(all_proxies)}개\n")
    return all_proxies


# ======================================================
# 프록시 테스트
# ======================================================

def build_requests_proxies(proxy_info: Dict) -> Dict[str, str]:
    addr = proxy_info["address"]
    protocol = proxy_info["protocol"]

    if protocol == "http":
        proxy_url = f"http://{addr}"
    elif protocol == "socks5":
        # pip install "requests[socks]"
        proxy_url = f"socks5://{addr}"
    else:
        raise ValueError(f"Unknown protocol: {protocol}")

    return {
        "http": proxy_url,
        "https": proxy_url,
    }


def check_ip_once(proxy_info: Dict) -> Optional[str]:
    if STOP_EVENT.is_set():
        return None
    proxies = build_requests_proxies(proxy_info)
    for url in IP_CHECK_URLS:
        if STOP_EVENT.is_set():
            return None
        try:
            r = requests.get(
                url,
                proxies=proxies,
                timeout=REQUEST_TIMEOUT,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                },
            )
            r.raise_for_status()
            ip = r.text.strip()
            if ip:
                return ip
        except Exception:
            continue
    return None


def test_proxy(proxy_info: Dict) -> Dict:
    """
    반환값 예:
    {
        "ok": True/False,
        "latency_ms": float or None,
        "ips": ["1.2.3.4", ...],    # RR 테스트 결과
        "proxy_type": "Static" / "Full Rotating" / "Partial Rotating" / "Unknown"
    }
    """
    ips: List[str] = []
    start = time.time()

    for i in range(RR_TEST_RUNS):
        if STOP_EVENT.is_set():
            break
        ip = check_ip_once(proxy_info)
        if ip:
            ips.append(ip)
        # 너무 빡세게 때리지 않게 약간 대기
        if i < RR_TEST_RUNS - 1 and not STOP_EVENT.is_set():
            time.sleep(0.5)

    if STOP_EVENT.is_set():
        # 중단 요청이 들어왔으면 그냥 실패로 보고 종료
        return {
            "ok": False,
            "latency_ms": None,
            "ips": [],
            "proxy_type": "Interrupted",
        }

    elapsed = (time.time() - start) * 1000.0  # ms
    if not ips:
        return {
            "ok": False,
            "latency_ms": None,
            "ips": [],
            "proxy_type": "Unknown",
        }

    unique_ips = set(ips)
    cnt = len(ips)
    uniq_cnt = len(unique_ips)

    if uniq_cnt == 1:
        proxy_type = "Static"
    elif uniq_cnt == cnt and cnt >= 3:
        proxy_type = "Full Rotating"
    elif uniq_cnt > 1:
        proxy_type = "Partial Rotating"
    else:
        proxy_type = "Unknown"

    return {
        "ok": True,
        "latency_ms": elapsed / cnt,  # 평균값 비슷하게
        "ips": list(unique_ips),
        "proxy_type": proxy_type,
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
        # 실패한 프록시는 alive 풀에서 제거 + 상태 갱신
        r.hset(
            key,
            mapping={
                "protocol": protocol,
                "address": address,
                "source": source,
                "status": "dead",
                "last_fail": now,
                "proxy_type": test_result.get("proxy_type", "Unknown"),
            },
        )
        r.zrem(REDIS_ZSET_ALIVE, f"{protocol}://{address}")
        r.expire(key, PROXY_TTL_SECONDS)
        return

    latency_ms = test_result["latency_ms"] or 999999
    proxy_type = test_result["proxy_type"]
    ips = ",".join(test_result["ips"])

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
        },
    )
    r.expire(key, PROXY_TTL_SECONDS)

    # 정렬된 alive 풀 (score = latency)
    r.zadd(REDIS_ZSET_ALIVE, {f"{protocol}://{address}": latency_ms})


# ======================================================
# 한 번 수집+테스트 실행
# ======================================================

def process_one_proxy(idx: int, total: int, proxy_info: Dict, r: redis.Redis) -> None:
    if STOP_EVENT.is_set():
        print(f"[{idx}/{total}] ⏹ 중단 신호 감지, 이 프록시는 스킵합니다.")
        return

    address = proxy_info["address"]
    protocol = proxy_info["protocol"]
    print(f"[{idx}/{total}] 테스트 시작: {protocol.upper()}://{address}")

    try:
        result = test_proxy(proxy_info)
    except Exception as e:
        print(f"  ❌ 테스트 중 예외: {e}")
        result = {"ok": False, "latency_ms": None, "ips": [], "proxy_type": "Unknown"}

    if STOP_EVENT.is_set():
        print(f"  ⏹ 중단 신호로 인해 결과 저장 스킵.")
        return

    if result["ok"]:
        print(
            f"  ✅ OK  | type={result['proxy_type']}, "
            f"avg_latency={result['latency_ms']:.1f} ms, ips={result['ips']}"
        )
    else:
        print(f"  ❌ DEAD (type={result.get('proxy_type')})")

    store_proxy_to_redis(r, proxy_info, result)
    print()


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

    print(f"🔍 총 {total}개 프록시 테스트 시작 (workers={MAX_WORKERS})\n")

    start = time.time()
    idx = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for p in proxies:
            if STOP_EVENT.is_set():
                print("⏹ 중단 신호 감지, 나머지 프록시는 제출하지 않습니다.")
                break
            idx += 1
            futures.append(executor.submit(process_one_proxy, idx, total, p, r))

        # 이미 제출된 작업들에 대해 결과 수집
        for f in as_completed(futures):
            if STOP_EVENT.is_set():
                break
            try:
                _ = f.result()
            except Exception as e:
                print(f"⚠️ 쓰레드 처리 중 예외: {e}")

    elapsed = time.time() - start
    alive_count = r.zcard(REDIS_ZSET_ALIVE)
    end_dt = datetime.now()

    print("=" * 80)
    print(f"⏱️ 이번 수집/테스트 소요시간: {elapsed:.1f}초")
    print(f"💾 Redis alive 풀 현재 개수: {alive_count}개 (key={REDIS_ZSET_ALIVE})")
    print(f"✅ 수집 작업 완료 시각: {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()


# ======================================================
# 데몬 루프
# ======================================================

def main_loop():
    print("=" * 80)
    print("🚀 Redis 프록시 수집 데몬")
    print("=" * 80)
    print(f"⏱️ 주기: {COLLECT_INTERVAL_MINUTES}분 마다 한 번 수집/테스트")
    print(f"🧪 한 번에 테스트할 최대 프록시 수: {MAX_TOTAL_PROXIES if MAX_TOTAL_PROXIES is not None else '제한 없음'}")
    print("🛑 언제든지 Ctrl + C 로 중단 가능")
    print("=" * 80)
    print()

    try:
        # 시작하자마자 한 번 실행
        collect_once()

        # 이후 주기적으로 반복
        while not STOP_EVENT.is_set():
            print(f"💤 {COLLECT_INTERVAL_MINUTES}분 대기 후 다음 수집 실행...")
            # 1초 단위로 잘게 쪼개서 중간에 Ctrl+C 누르면 바로 반응하게
            total_sleep = COLLECT_INTERVAL_MINUTES * 60
            for _ in range(total_sleep):
                if STOP_EVENT.is_set():
                    break
                time.sleep(1)
            if STOP_EVENT.is_set():
                break
            collect_once()

    except KeyboardInterrupt:
        print("\n🛑 KeyboardInterrupt(Ctrl+C) 감지, 중단 신호 설정.")
        STOP_EVENT.set()

    finally:
        print("🔚 collector_redis.py 종료 준비 완료.")


if __name__ == "__main__":
    main_loop()
