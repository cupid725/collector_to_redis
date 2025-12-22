import time
import json
import requests
from typing import List, Dict, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import lru_cache
from collections import Counter
import ipaddress

import redis  # pip install redis
import threading
import signal
import os
import sys

# SOCKS 프록시 사용 시: pip install "requests[socks]"

# ================= 전역 중단 신호 =================
STOP_EVENT = threading.Event()

# Ctrl+C 2번 누르면 강제종료
_SIGINT_COUNT = 0


def _sigint_handler(sig, frame):
    """
    1회 Ctrl+C: STOP_EVENT 설정 + 가능한 한 빨리 빠져나오도록 유도
    2회 Ctrl+C: 즉시 강제 종료
    """
    global _SIGINT_COUNT
    _SIGINT_COUNT += 1

    if _SIGINT_COUNT == 1:
        print("\n🛑 Ctrl+C 감지: 중단 신호 설정(STOP_EVENT). "
              "진행 중인 네트워크 요청은 타임아웃까지 걸릴 수 있습니다.")
        STOP_EVENT.set()
    else:
        print("\n💥 Ctrl+C 2회 감지: 강제 종료합니다.")
        os._exit(1)


# Windows/리눅스 공통: SIGINT 핸들러 설치
signal.signal(signal.SIGINT, _sigint_handler)

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

# ================= 🏠 RESIDENTIAL 필터링 설정 =================
RESIDENTIAL_ONLY = True  # True: residential만 허용, False: 모두 허용

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

GEOIP_URL = "http://ip-api.com/json/{ip}?fields=status,country,countryCode,query,message,isp,org,as,asname"

# ================= 🏢 Datacenter IP 범위 (주요 클라우드/호스팅) =================
# 주요 데이터센터 CIDR 블록 - 실제로는 훨씬 더 많지만 대표적인 것들만
DATACENTER_CIDRS = [
    # Amazon AWS
    "3.0.0.0/8", "13.32.0.0/15", "18.0.0.0/8", "52.0.0.0/8", "54.0.0.0/8",
    # Google Cloud
    "34.64.0.0/10", "35.184.0.0/13", "35.192.0.0/12", "35.208.0.0/12",
    # Microsoft Azure
    "13.64.0.0/11", "20.0.0.0/8", "40.64.0.0/10", "51.0.0.0/8",
    # DigitalOcean
    "104.131.0.0/16", "159.65.0.0/16", "167.71.0.0/16", "167.99.0.0/16",
    # Linode
    "45.33.0.0/16", "45.56.0.0/16", "50.116.0.0/16", "66.228.0.0/16",
    # OVH
    "51.254.0.0/16", "51.255.0.0/16", "54.36.0.0/16", "54.37.0.0/16",
    # Hetzner
    "5.9.0.0/16", "46.4.0.0/16", "78.46.0.0/15", "88.198.0.0/16",
    # Vultr
    "45.32.0.0/16", "45.76.0.0/16", "108.61.0.0/16", "207.246.0.0/16",
]

# 데이터센터 CIDR을 ipaddress 객체로 변환
_DATACENTER_NETWORKS: List[ipaddress.IPv4Network] = []
for cidr in DATACENTER_CIDRS:
    try:
        _DATACENTER_NETWORKS.append(ipaddress.ip_network(cidr))
    except Exception:
        pass

# 데이터센터로 알려진 ASN 키워드
DATACENTER_ASN_KEYWORDS = [
    "amazon", "aws", "google", "microsoft", "azure", "digitalocean",
    "linode", "ovh", "hetzner", "vultr", "contabo", "online.net",
    "scaleway", "cloudflare", "fastly", "akamai", "incapsula",
    "datacamp", "datacenter", "hosting", "server", "cloud", "vps"
]

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
# 🏠 Residential IP 판별 함수
# ======================================================

def is_datacenter_ip(ip: str) -> bool:
    """IP가 알려진 데이터센터 범위에 속하는지 확인"""
    try:
        ip_obj = ipaddress.ip_address(ip)
        for network in _DATACENTER_NETWORKS:
            if ip_obj in network:
                return True
    except Exception:
        pass
    return False

def is_residential_by_asn(isp: str, org: str, asname: str) -> bool:
    """
    ISP/조직/ASN 이름을 기반으로 residential 여부 판단
    datacenter 키워드가 있으면 False, 없으면 True
    """
    combined = f"{isp} {org} {asname}".lower()
    
    for keyword in DATACENTER_ASN_KEYWORDS:
        if keyword in combined:
            return False  # datacenter로 판단
    
    return True  # residential로 판단

@lru_cache(maxsize=1000)
def is_residential_ip(ip: str) -> bool:
    """
    종합적으로 IP가 residential인지 판단
    1. CIDR 범위 체크 (빠른 로컬 체크)
    2. ip-api.com ASN 정보 체크
    """
    # 1단계: 알려진 datacenter CIDR 범위 체크
    if is_datacenter_ip(ip):
        return False
    
    # 2단계: ip-api.com으로 ISP/ASN 정보 조회
    try:
        resp = requests.get(
            GEOIP_URL.format(ip=ip),
            timeout=(5, 5),
            headers={"User-Agent": "Mozilla/5.0"}
        )
        resp.raise_for_status()
        data = resp.json()
        
        if data.get("status") == "success":
            isp = data.get("isp", "")
            org = data.get("org", "")
            asname = data.get("asname", "")
            
            return is_residential_by_asn(isp, org, asname)
    except Exception:
        pass
    
    # 판단 불가시 보수적으로 residential로 간주
    return True

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

    print(f"\n  → Unique 이합: {len(all_proxies)}개")

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
    RESIDENTIAL_ONLY=True인 경우 residential 여부도 체크
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
            "is_residential": False,
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
            "is_residential": False,
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

    # 🏠 Residential 여부 판단
    is_residential = False
    if RESIDENTIAL_ONLY:
        # RR 프록시의 경우: 획득한 IP 중 1개라도 residential이면 인정
        residential_ips = [ip for ip in unique_ips if is_residential_ip(ip)]
        is_residential = len(residential_ips) > 0
        
        # residential이 아니면 실패로 처리
        if not is_residential:
            return {
                "ok": False,
                "latency_ms": elapsed / cnt,
                "ips": unique_ips,
                "proxy_type": proxy_type,
                "countries": countries,
                "is_residential": False,
                "error": "Not a residential IP",
            }
    else:
        # RESIDENTIAL_ONLY=False인 경우 모두 허용
        is_residential = None  # 체크하지 않음

    return {
        "ok": True,
        "latency_ms": elapsed / cnt,
        "ips": unique_ips,
        "proxy_type": proxy_type,
        "countries": countries,
        "is_residential": is_residential,
        "error": None,
    }

# ======================================================
# Redis 저장
# ======================================================

def store_proxy_to_redis(r: redis.Redis, proxy_info: Dict, test_result: Dict):
    """Redis에 프록시 정보 저장 (alive만 저장)"""
    raw_protocol = proxy_info["protocol"]
    address = proxy_info["address"]
    source = proxy_info.get("source", "")

    # Canonical protocol (브라우저/requests 공용)
    protocol = "http" if raw_protocol == "https" else raw_protocol

    key = make_proxy_key(protocol, address)
    now = datetime.utcnow().isoformat()
    member = f"{protocol}://{address}"

    # Dead 프록시는 저장하지 않음
    if not test_result["ok"]:
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
            "is_residential": str(test_result.get("is_residential", "")),
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
            "is_residential": False,
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
        "is_residential": result.get("is_residential"),
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
    if RESIDENTIAL_ONLY:
        print("🏠 RESIDENTIAL_ONLY=True: residential IP만 허용")
    else:
        print("🌐 RESIDENTIAL_ONLY=False: 모든 프록시 허용")
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

    print(f"\n🔬 이 {total}개 프록시 테스트 시작 (workers={MAX_WORKERS})")
    print(f"⏱️  타임아웃: 연결 {CONNECT_TIMEOUT}초 / 읽기 {READ_TIMEOUT}초\n")

    start = time.time()
    idx = 0
    results = []

    executor = None
    futures = []

    try:
        executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

        for p in proxies:
            if STOP_EVENT.is_set():
                print("\nℹ 중단 신호 감지, 나머지 프록시는 제출하지 않습니다.")
                break
            idx += 1
            futures.append(executor.submit(process_one_proxy, idx, total, p, r))

        # 결과 수집 (중단 시 빨리 빠져나오도록)
        for f in as_completed(futures):
            if STOP_EVENT.is_set():
                break
            try:
                result = f.result()
                results.append(result)
            except Exception as e:
                if not STOP_EVENT.is_set():
                    print(f"⚠️  쓰레드 처리 중 예외: {e}")
                results.append({"status": "error", "protocol": "unknown"})

    except KeyboardInterrupt:
        # collect_once 안에서 Ctrl+C가 들어온 경우도 처리
        print("\n🛑 collect_once 내부 KeyboardInterrupt: 중단 신호 설정.")
        STOP_EVENT.set()

    finally:
        # pending future 취소 + executor 비대기 종료 시도
        if executor is not None:
            try:
                for fu in futures:
                    fu.cancel()
                # Python 3.9+ : cancel_futures 지원
                executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:
                executor.shutdown(wait=False)

    elapsed = time.time() - start
    end_dt = datetime.now()

    if STOP_EVENT.is_set():
        print("\n" + "=" * 80)
        print("🧯 중단 처리 완료: 통계는 부분적으로만 집계될 수 있습니다.")
        print(f"⏱️  현재까지 소요시간: {elapsed:.1f}초")
        print("=" * 80)
        return

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

    if RESIDENTIAL_ONLY:
        residential_count = sum(1 for r in results if r.get("is_residential") is True)
        print(f"🏠 Residential: {residential_count}개")

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
            is_res = pinfo.get("is_residential", "")

            score_int = int(score)
            if score_int <= 0:
                score_human = "now"
            else:
                score_human = datetime.fromtimestamp(score_int).strftime("%Y-%m-%d %H:%M:%S")

            res_marker = " 🏠" if is_res == "True" else ""
            print(f"  • {proxy_str:30s} | {source:20s} | score={score_int:>10} ({score_human}) | {countries}{res_marker}")

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
    if RESIDENTIAL_ONLY:
        print(f"🏠 RESIDENTIAL_ONLY=True: residential IP만 허용")
    else:
        print(f"🌐 RESIDENTIAL_ONLY=False: 모든 프록시 허용")
    print("🛑 언제든지 Ctrl + C로 중단 가능 (2번 누르면 강제 종료)")
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
        # main_loop 밖에서 들어오는 경우도 대비
        print("\n🛑 main_loop KeyboardInterrupt 감지, 중단 신호 설정.")
        STOP_EVENT.set()

    finally:
        print("📚 collector_redis.py 종료 완료.")

if __name__ == "__main__":
    main_loop()