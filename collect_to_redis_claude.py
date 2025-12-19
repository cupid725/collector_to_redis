import time
import json
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
REDIS_PASSWORD = None

REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_LEASE = "proxies:lease"
REDIS_KEY_PREFIX = "proxy"

# ================= 수집/테스트 주기 설정 =================
COLLECT_INTERVAL_MINUTES = 240   # 240분(4시간)마다 한 번 수집
PROXY_TTL_SECONDS = COLLECT_INTERVAL_MINUTES * 3 * 60
MAX_TOTAL_PROXIES: Optional[int] = None  # None이면 제한 없음

# ================= 프록시 리스트 소스 (monosans + victorgeel) =================

# ⭐⭐⭐⭐⭐ Tier 1: monosans (1시간마다 업데이트, Rust 검증)
MONOSANS_HTTP = "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt"
MONOSANS_SOCKS4 = "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt"
MONOSANS_SOCKS5 = "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt"

# ⭐⭐⭐⭐⭐ Tier 1: victorgeel (30분마다 업데이트, monosans 도구 사용)
VICTORGEEL_HTTP = "https://raw.githubusercontent.com/victorgeel/proxy-list-update/main/proxies/http.txt"
VICTORGEEL_SOCKS4 = "https://raw.githubusercontent.com/victorgeel/proxy-list-update/main/proxies/socks4.txt"
VICTORGEEL_SOCKS5 = "https://raw.githubusercontent.com/victorgeel/proxy-list-update/main/proxies/socks5.txt"

# ⭐⭐⭐⭐ Tier 2: ErcinDedeoglu (1시간마다 업데이트, 보조 소스)
ERCINDEDEOGLU_HTTP = "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt"
ERCINDEDEOGLU_HTTPS = "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt"
ERCINDEDEOGLU_SOCKS4 = "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt"
ERCINDEDEOGLU_SOCKS5 = "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt"

# 백업 소스 (선택적)
VAKHOV_HTTP = "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt"
VAKHOV_HTTPS = "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/https.txt"
VAKHOV_SOCKS4 = "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt"
VAKHOV_SOCKS5 = "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt"

# ================= 테스트 설정 =================

# HTTPS 우선 (YouTube 등 HTTPS 사이트 대응)
ALLOW_HTTP_FALLBACK = False

IP_CHECK_URLS = [
    ("https://api.ipify.org?format=text", "https"),
    ("https://icanhazip.com", "https"),
    ("https://checkip.amazonaws.com", "https"),
]

if ALLOW_HTTP_FALLBACK:
    IP_CHECK_URLS += [
        ("http://api.ipify.org?format=text", "http"),
        ("http://icanhazip.com", "http"),
    ]

CONNECT_TIMEOUT = 12
READ_TIMEOUT = 12
MAX_WORKERS = 40
RR_TEST_RUNS = 1

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
# 프록시 리스트 수집 (통합 함수)
# ======================================================

def _normalize_addr(line: str) -> Optional[str]:
    """다양한 입력을 ip:port로 정규화"""
    line = line.strip()
    if not line or line.startswith("#"):
        return None

    # http://, https://, socks5:// 등 프로토콜 제거
    if "://" in line:
        addr = line.split("://", 1)[1]
    else:
        addr = line

    addr = addr.split("/")[0].strip()
    if ":" not in addr:
        return None
    return addr

def fetch_proxy_list(url: str, protocol: str, source_name: str) -> List[Dict]:
    """
    단일 소스에서 프록시 리스트 다운로드
    Args:
        url: 다운로드할 URL
        protocol: "http", "socks4", "socks5" 등
        source_name: 소스 식별명 (예: "monosans_http")
    """
    if STOP_EVENT.is_set():
        return []
    
    print(f"📥 {protocol.upper():7s} 다운로드: {source_name:25s} ({url.split('/')[-2]})")
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
        
        print(f"   ✅ {len(proxies):4d}개 수집")
        
    except Exception as e:
        if not STOP_EVENT.is_set():
            print(f"   ❌ 실패: {str(e)[:50]}")
    
    return proxies

def fetch_all_proxies() -> List[Dict]:
    """모든 소스에서 프록시 수집 (우선순위: victorgeel > monosans > ErcinDedeoglu > vakhov)"""
    if STOP_EVENT.is_set():
        return []

    print("=" * 80)
    print("🔍 프록시 수집 시작")
    print("=" * 80)
    
    all_sources = [
        # Tier 1: victorgeel (30분마다 업데이트 - 최고 신선도!)
        (VICTORGEEL_HTTP, "http", "victorgeel_http"),
        (VICTORGEEL_SOCKS4, "socks4", "victorgeel_socks4"),
        (VICTORGEEL_SOCKS5, "socks5", "victorgeel_socks5"),
        
        # Tier 1: monosans (1시간마다 업데이트)
        (MONOSANS_HTTP, "http", "monosans_http"),
        (MONOSANS_SOCKS4, "socks4", "monosans_socks4"),
        (MONOSANS_SOCKS5, "socks5", "monosans_socks5"),
        
        # Tier 2: ErcinDedeoglu (추가 다양성)
        (ERCINDEDEOGLU_HTTP, "http", "ercindedeoglu_http"),
        (ERCINDEDEOGLU_HTTPS, "http", "ercindedeoglu_https"),  # https.txt는 http 프록시로 취급
        (ERCINDEDEOGLU_SOCKS4, "socks4", "ercindedeoglu_socks4"),
        (ERCINDEDEOGLU_SOCKS5, "socks5", "ercindedeoglu_socks5"),
        
        # Tier 3: vakhov (5-20분 업데이트, 검증된 품질)
        (VAKHOV_HTTP, "http", "vakhov_http"),
        (VAKHOV_HTTPS, "http", "vakhov_https"),
        (VAKHOV_SOCKS4, "socks4", "vakhov_socks4"),
        (VAKHOV_SOCKS5, "socks5", "vakhov_socks5"),
    ]
    
    raw_proxies = []
    
    for url, protocol, source_name in all_sources:
        if STOP_EVENT.is_set():
            break
        proxies = fetch_proxy_list(url, protocol, source_name)
        raw_proxies.extend(proxies)
        time.sleep(0.5)  # API 레이트 리밋 방지
    
    # protocol + address 기준 중복 제거
    unique: Dict[tuple, Dict] = {}
    for p in raw_proxies:
        key = (p["protocol"], p["address"])
        if key not in unique:
            unique[key] = p
    
    all_proxies = list(unique.values())
    
    print("\n" + "=" * 80)
    print("📦 프록시 집계 (중복 제거 후)")
    print("=" * 80)
    
    # 소스별 통계
    source_counts = Counter(p["source"] for p in all_proxies)
    protocol_counts = Counter(p["protocol"] for p in all_proxies)
    
    print("\n📊 소스별 통계:")
    for source, count in sorted(source_counts.items()):
        print(f"  • {source:25s}: {count:4d}개")
    
    print("\n📊 프로토콜별 통계:")
    for protocol, count in sorted(protocol_counts.items()):
        print(f"  • {protocol.upper():7s}: {count:4d}개")
    
    print(f"\n  → Unique 총합: {len(all_proxies)}개")
    
    if MAX_TOTAL_PROXIES is not None and len(all_proxies) > MAX_TOTAL_PROXIES:
        print(f"  ⚠️  너무 많아서 {MAX_TOTAL_PROXIES}개까지만 사용합니다.")
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
    elif protocol == "https":
        proxy_url = f"http://{addr}"  # https 프록시도 http:// 스킴 사용
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
    """프록시를 통해 IP 체크. Returns: (ip, service_url) 또는 None"""
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
            if ip and ('.' in ip or ':' in ip) and len(ip) < 50:
                return (ip, url)
        except Exception:
            continue
        time.sleep(0.3)

    return None

def test_proxy(proxy_info: Dict) -> Dict:
    """
    프록시를 RR_TEST_RUNS번 테스트하고 결과 반환
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
        "latency_ms": elapsed / cnt,
        "ips": unique_ips,
        "proxy_type": proxy_type,
        "countries": countries,
        "error": None,
    }

# ======================================================
# Redis 저장
# ======================================================

def store_proxy_to_redis(r: redis.Redis, proxy_info: Dict, test_result: Dict):
    """Redis에 프록시 정보 저장"""
    raw_protocol = proxy_info["protocol"]
    address = proxy_info["address"]
    source = proxy_info.get("source", "")

    # Canonical protocol (브라우저/requests 공용)
    protocol = "http" if raw_protocol == "https" else raw_protocol

    key = make_proxy_key(protocol, address)
    now = datetime.utcnow().isoformat()
    member = f"{protocol}://{address}"

    if not test_result["ok"]:
        r.hset(
            key,
            mapping={
                "protocol": protocol,
                "list_protocol": raw_protocol,
                "address": address,
                "source": source,
                "status": "dead",
                "updated_at": now,
                "error": test_result.get("error") or "",
            },
        )
        r.zrem(REDIS_ZSET_ALIVE, member)
        return

    r.hset(
        key,
        mapping={
            "protocol": protocol,
            "list_protocol": raw_protocol,
            "address": address,
            "source": source,
            "status": "alive",
            "updated_at": now,
            "latency_ms": test_result.get("latency_ms") or "",
            "proxy_type": test_result.get("proxy_type") or "",
            "ips": json.dumps(test_result.get("ips") or [], ensure_ascii=False),
            "countries": json.dumps(test_result.get("countries") or [], ensure_ascii=False),
        },
    )

    # 이미 lease(사용 중)에 잡혀있다면 alive에 다시 넣지 않습니다
    if r.zscore(REDIS_ZSET_LEASE, member) is None:
        try:
            r.zadd(REDIS_ZSET_ALIVE, {member: 0}, nx=True)
        except TypeError:
            r.execute_command("ZADD", REDIS_ZSET_ALIVE, "NX", 0, member)

# ======================================================
# 한 번 수집+테스트 실행
# ======================================================

def process_one_proxy(idx: int, total: int, proxy_info: Dict, r: redis.Redis) -> Dict:
    """한 개 프록시 테스트 및 저장"""
    if STOP_EVENT.is_set():
        return {"status": "skipped", "protocol": proxy_info["protocol"]}

    address = proxy_info["address"]
    protocol = proxy_info["protocol"]
    source = proxy_info.get("source", "")

    # 간결한 로그
    if idx % 20 == 0 or idx == total:
        print(f"[{idx}/{total}] 진행 중... {protocol.upper()}://{address} ({source})")

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
        "source": source,
        "latency_ms": result.get("latency_ms"),
        "proxy_type": result.get("proxy_type"),
    }

def collect_once():
    """프록시 수집 + 테스트 + Redis 업데이트를 한 번 수행"""
    if STOP_EVENT.is_set():
        print("ℹ collect_once 호출 시 이미 중단 신호가 설정되어 있음. 스킵.")
        return

    start_dt = datetime.now()

    print("\n" + "=" * 80)
    print(f"🕐 수집 작업 시작: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print("📌 소스: victorgeel (30분) + monosans (1시간) + ErcinDedeoglu (1시간) + vakhov (5-20분)")
    print("=" * 80)

    r = get_redis()
    proxies = fetch_all_proxies()
    total = len(proxies)

    if STOP_EVENT.is_set():
        print("ℹ 수집 중단 신호로 인해 테스트를 시작하지 않습니다.")
        return

    if not total:
        print("❌ 수집된 프록시가 없습니다. 작업 종료.")
        return

    print(f"\n🔬 총 {total}개 프록시 테스트 시작 (workers={MAX_WORKERS})")
    print(f"⏱️  타임아웃: 연결 {CONNECT_TIMEOUT}초 / 읽기 {READ_TIMEOUT}초\n")

    start = time.time()
    idx = 0
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for p in proxies:
            if STOP_EVENT.is_set():
                print("\nℹ 중단 신호 감지, 나머지 프록시는 제출하지 않습니다.")
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
                print(f"⚠️  쓰레드 처리 중 예외: {e}")
                results.append({"status": "error", "protocol": "unknown"})

    elapsed = time.time() - start
    end_dt = datetime.now()

    # 통계 출력
    print("\n" + "=" * 80)
    print("📊 테스트 결과 통계")
    print("=" * 80)

    status_counts = Counter(r["status"] for r in results)
    protocol_counts = Counter(r["protocol"] for r in results)
    source_counts = Counter(r.get("source", "unknown") for r in results if r["status"] == "alive")

    alive_count = status_counts.get("alive", 0)
    dead_count = status_counts.get("dead", 0)
    total_tested = len(results)
    success_rate = (alive_count / total_tested * 100) if total_tested > 0 else 0

    print(f"✅ 성공: {alive_count}개 ({success_rate:.1f}%)")
    print(f"❌ 실패: {dead_count}개")
    print(f"ℹ  중단/에러: {status_counts.get('skipped', 0) + status_counts.get('interrupted', 0) + status_counts.get('error', 0)}개")

    print(f"\n📋 프로토콜별 통계:")
    for proto, count in protocol_counts.most_common():
        proto_alive = sum(1 for r in results if r["protocol"] == proto and r["status"] == "alive")
        print(f"  • {proto.upper():8s}: {proto_alive}/{count} alive")

    print(f"\n🌟 성공한 프록시의 소스별 통계:")
    for source, count in source_counts.most_common():
        print(f"  • {source:25s}: {count:4d}개")

    # Redis alive 풀 현황
    redis_alive = r.zcard(REDIS_ZSET_ALIVE)
    print(f"\n💾 Redis alive 풀: {redis_alive}개 (key={REDIS_ZSET_ALIVE})")

    # 상위 10개 프록시
    top_proxies = r.zrange(REDIS_ZSET_ALIVE, 0, 9, withscores=True)
    if top_proxies:
        print(f"\n🏆 사용 가능 시각(score) 기준 상위 10개 프록시:")
        for proxy_str, score in top_proxies:
            protocol, addr = proxy_str.split("://", 1)
            pkey = make_proxy_key(protocol, addr)
            pinfo = r.hgetall(pkey)
            source = pinfo.get("source", "unknown")
            countries = pinfo.get("countries", "Unknown")

            score_int = int(score)
            if score_int <= 0:
                score_human = "now"
            else:
                score_human = datetime.fromtimestamp(score_int).strftime("%Y-%m-%d %H:%M:%S")

            print(f"  • {proxy_str:30s} | {source:20s} | score={score_int:>10} ({score_human}) | {countries}")

    print(f"\n⏱️  소요시간: {elapsed:.1f}초")
    print(f"✅ 완료 시각: {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()

# ======================================================
# 데몬 루프
# ======================================================

def main_loop():
    print("=" * 80)
    print("🚀 Redis 프록시 수집 데몬 (monosans + victorgeel)")
    print("=" * 80)
    print(f"⏱️  주기: {COLLECT_INTERVAL_MINUTES}분마다 한 번 수집/테스트")
    print(f"🧪 최대 테스트 프록시 수: {MAX_TOTAL_PROXIES if MAX_TOTAL_PROXIES is not None else '제한 없음'}")
    print(f"🔧 동시 작업 스레드: {MAX_WORKERS}개")
    print(f"🌐 IP 체크: HTTPS 우선 전략")
    print(f"📦 소스 우선순위: victorgeel (30분) > monosans (1시간) > ErcinDedeoglu > vakhov")
    print("🛑 언제든지 Ctrl + C로 중단 가능")
    print("=" * 80)
    print()

    try:
        # 시작하자마자 한 번 실행
        collect_once()

        # 이후 주기적으로 반복
        while not STOP_EVENT.is_set():
            print(f"\n💤 {COLLECT_INTERVAL_MINUTES}분 대기 후 다음 수집 실행...")
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
        print("📚 collector_redis.py 종료 완료.")

if __name__ == "__main__":
    main_loop()