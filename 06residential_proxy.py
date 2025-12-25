import time
import json
import requests
import redis
import threading
import signal
import os
import sys
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import lru_cache
from collections import Counter

# ================= 1. 전역 설정 및 신호 처리 =================
STOP_EVENT = threading.Event()
_SIGINT_COUNT = 0

# 자신의 실제 공인 IP (프로그램 시작 시 한 번 확인)
MY_REAL_IP: Optional[str] = None

def _sigint_handler(sig, frame):
    global _SIGINT_COUNT
    _SIGINT_COUNT += 1
    if _SIGINT_COUNT == 1:
        print("\n🛑 Ctrl+C 감지: 중단 신호 설정. 타임아웃 대기 중...")
        STOP_EVENT.set()
    else:
        print("\n💥 Ctrl+C 2회 감지: 즉시 강제 종료합니다.")
        os._exit(1)

signal.signal(signal.SIGINT, _sigint_handler)

# ================= 2. Redis 및 수집 설정 =================
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_LEASE = "proxies:lease"
REDIS_KEY_PREFIX = "proxy"

COLLECT_INTERVAL_MINUTES = 10
MAX_WORKERS = 60

# ✅ Residential 필터링 옵션
RESIDENTIAL_ONLY = True  # True: residential만, False: 모두

# ================= 3. GeoIP 및 IP 검사 설정 =================
GEOIP_URL = "http://ip-api.com/json/{ip}?fields=status,country,countryCode,query,message,isp,org,as,asname"

IP_CHECK_URLS = [
    ("https://api.ipify.org?format=text", "https"),
    ("https://icanhazip.com", "https"),
    ("https://checkip.amazonaws.com", "https")
]

# ✅ 소스 목록 - 프로토콜 정보 추가
# format: (url, default_protocol, has_protocol_prefix)
SOURCES = [
    # 기존 소스 (프로토콜 접두사 없음)
    #("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt", "http", False),
    ("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt", "socks5", False),
    #("https://raw.githubusercontent.com/victorgeel/proxy-list-update/main/proxies/http.txt", "http", False),
    ("https://raw.githubusercontent.com/victorgeel/proxy-list-update/main/proxies/socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/victorgeel/proxy-list-update/main/proxies/socks5.txt", "socks5", False),
    #("https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt", "http", False),
    ("https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt", "socks5", False),
    #("https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt", "http", False),
    ("https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt", "socks5", False),
    
    # ✅ 새로운 소스 (프로토콜 접두사 있거나 다양한 형식)
    ("https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt", "http", True),
    ("https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt", "socks5", True),
    ("https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt", "socks5", True),
]

# ✅ Datacenter 키워드
DATACENTER_KEYWORDS = [
    "amazon", "aws", "google", "microsoft", "azure", "digitalocean",
    "linode", "ovh", "hetzner", "vultr", "datacenter", "hosting", "cloud", "vps"
]

# ================= 4. Rate Limit 관리 =================
_geoip_lock = threading.Lock()
_last_geoip_call = 0

# ================= 5. 핵심 분석 함수 =================

def parse_proxy_line(line: str, default_protocol: str, has_protocol_prefix: bool) -> Optional[Dict]:
    """
    ✅ 새로운 함수: 다양한 형식의 프록시 라인 파싱
    
    지원 형식:
    1. ip:port (예: 181.174.231.30:999)
    2. protocol://ip:port (예: http://80.241.251.54:8080)
    3. socks5://ip:port (예: socks5://24.249.199.12:4145)
    """
    line = line.strip()
    if not line or line.startswith('#'):
        return None
    
    protocol = default_protocol
    addr = line
    
    # 프로토콜 접두사가 있는 경우 추출
    if "://" in line:
        parts = line.split("://", 1)
        protocol = parts[0].lower()
        addr = parts[1]
    
    # URL 경로 제거 (있을 경우)
    addr = addr.split("/")[0].strip()
    
    # 유효성 검사: ip:port 형식인지 확인
    if ":" not in addr:
        return None
    
    # 포트 번호 검증
    try:
        ip_part, port_part = addr.rsplit(":", 1)
        port = int(port_part)
        if not (1 <= port <= 65535):
            return None
    except (ValueError, AttributeError):
        return None
    
    return {
        "address": addr,
        "protocol": protocol
    }

@lru_cache(maxsize=1000)
def get_ip_info(ip: str) -> Dict:
    """
    ✅ 개선: IP 정보 + Residential 판단
    Rate limit 고려 (2초에 1번)
    """
    global _last_geoip_call
    
    # Rate limit: 30 req/min = 2초에 1번
    with _geoip_lock:
        now = time.time()
        elapsed = now - _last_geoip_call
        if elapsed < 2:
            time.sleep(2 - elapsed)
        _last_geoip_call = time.time()
    
    try:
        resp = requests.get(
            GEOIP_URL.format(ip=ip), 
            timeout=5,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        data = resp.json()
        
        if data.get("status") == "success":
            country = f"{data.get('country', 'Unknown')} ({data.get('countryCode', '??')})"
            
            # ✅ Residential 판단
            isp = data.get("isp", "").lower()
            org = data.get("org", "").lower()
            asname = data.get("asname", "").lower()
            combined = f"{isp} {org} {asname}"
            
            is_datacenter = any(kw in combined for kw in DATACENTER_KEYWORDS)
            is_residential = not is_datacenter
            
            return {
                "country": country,
                "is_residential": is_residential,
                "isp": data.get("isp", "Unknown"),
            }
    except Exception as e:
        pass
    
    return {
        "country": "Unknown",
        "is_residential": True,  # 판단 불가시 보수적으로 허용
        "isp": "Unknown",
    }

def check_ip_once(p: Dict) -> Optional[str]:
    """✅ 개선: 프로토콜 정규화 추가"""
    # 프로토콜 정규화
    protocol = p['protocol']
    if protocol == "https":
        protocol = "http"  # https 프록시는 http로 통일
    
    proxy_url = f"{protocol}://{p['address']}"
    proxies = {"http": proxy_url, "https": proxy_url}
    
    for url, _ in IP_CHECK_URLS:
        if STOP_EVENT.is_set(): 
            return None
        try:
            r = requests.get(
                url, 
                proxies=proxies, 
                timeout=(12, 12),
                headers={"User-Agent": "Mozilla/5.0"}
            )
            if r.status_code == 200:
                ip = r.text.strip()
                if ip and ('.' in ip or ':' in ip) and len(ip) < 50:
                    return ip
        except:
            continue
        time.sleep(0.3)
    return None

def process_one_proxy(p: Dict, r: redis.Redis, idx: int, total: int) -> bool:
    """
    ✅ 대폭 개선: 테스트 + Residential 체크 + Redis 저장
    """
    if STOP_EVENT.is_set(): 
        return False
    
    # 진행 상황 표시
    if idx % 20 == 0 or idx == total:
        print(f"[{idx}/{total}] 진행 중... {p['protocol']}://{p['address']}", flush=True)
    
    # IP 획득
    ip = check_ip_once(p)
    if not ip:
        return False
    
    # ✅ 추가: 내 실제 IP와 동일하면 실패 처리 (프록시가 실제로 동작하지 않음)
    global MY_REAL_IP
    if MY_REAL_IP and ip == MY_REAL_IP:
        print(f"   ⚠️ 프록시 무효: 외부 IP가 내 실제 IP와 동일 ({ip}) → 스킵")
        return False
    
    # GeoIP 정보 조회 (RESIDENTIAL_ONLY=False면 간단하게만)
    if RESIDENTIAL_ONLY:
        ip_info = get_ip_info(ip)
        
        # Residential이 아니면 저장 안함
        if not ip_info["is_residential"]:
            return False
        
        country = ip_info["country"]
        is_residential = True
    else:
        # RESIDENTIAL_ONLY=False면 국가만 간단히 조회 (빠름)
        country = "Unknown"  # GeoIP 스킵
        is_residential = None
    
    # ✅ 프로토콜 정규화 (https → http)
    protocol = p['protocol']
    if protocol == "https":
        protocol = "http"
    
    member = f"{protocol}://{p['address']}"
    key = f"{REDIS_KEY_PREFIX}:{protocol}:{p['address']}"
    
    # Hash 저장
    r.hset(key, mapping={
        "protocol": protocol,
        "list_protocol": p['protocol'],  # 원본 프로토콜 기록
        "address": p["address"],
        "source": p.get("source", "unknown"),
        "status": "alive",
        "obtained_ip": ip,
        "country": country,
        "is_residential": str(is_residential) if is_residential is not None else "",
        "updated_at": datetime.utcnow().isoformat()
    })
    
    # ✅ ZSET 저장: score=0 (즉시 사용 가능!)
    if r.zscore(REDIS_ZSET_LEASE, member) is None:
        r.zadd(REDIS_ZSET_ALIVE, {member: 0})
    
    return True

# ================= 6. 메인 워커 및 루프 =================

def collect_once():
    """✅ 개선: 수집 로직"""
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    
    print("\n" + "=" * 80)
    print(f"🕐 수집 작업 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    if RESIDENTIAL_ONLY:
        print("🏠 RESIDENTIAL_ONLY=True: residential IP만 허용")
    else:
        print("🌐 RESIDENTIAL_ONLY=False: 모든 프록시 허용 (GeoIP 스킵)")
    print("=" * 80)
    
    # 프록시 다운로드
    raw_proxies = []
    for url, default_protocol, has_prefix in SOURCES:
        if STOP_EVENT.is_set():
            break
        try:
            # URL에서 파일명 추출 (보기 좋게)
            filename = url.split('/')[-1]
            print(f"📥 다운로드 중: {filename}")
            
            res = requests.get(url, timeout=30)
            if res.status_code == 200:
                count = 0
                for line in res.text.strip().splitlines():
                    parsed = parse_proxy_line(line, default_protocol, has_prefix)
                    if parsed:
                        parsed["source"] = filename
                        raw_proxies.append(parsed)
                        count += 1
                print(f"   ✅ {count}개 수집")
        except Exception as e:
            print(f"   ❌ 실패: {str(e)[:50]}")
        time.sleep(0.5)

    # 중복 제거
    unique_proxies = list({(p['protocol'], p['address']): p for p in raw_proxies}.values())
    total = len(unique_proxies)
    
    print("\n" + "=" * 80)
    print(f"📦 프록시 집계 (중복 제거 후): {total}개")
    print("=" * 80)
    
    if total == 0:
        print("❌ 수집된 프록시가 없습니다.")
        return
    
    # 프로토콜별 통계
    protocol_stats = Counter(p['protocol'] for p in unique_proxies)
    print("📊 프로토콜별 분포:")
    for proto, cnt in protocol_stats.most_common():
        print(f"   • {proto:8s}: {cnt:5d}개")
    print()
    
    print(f"🔬 {total}개 프록시 테스트 시작 (workers={MAX_WORKERS})")
    if RESIDENTIAL_ONLY:
        print("⚠️  GeoIP 조회로 인해 시간이 오래 걸릴 수 있습니다 (Rate Limit)")
    print()
    
    # 테스트 실행
    alive_count = 0
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_one_proxy, p, r, i+1, total): p 
            for i, p in enumerate(unique_proxies)
        }
        
        for f in as_completed(futures):
            if STOP_EVENT.is_set():
                break
            try:
                if f.result():
                    alive_count += 1
            except Exception as e:
                pass
    
    elapsed = time.time() - start_time
    
    # 통계 출력
    print("\n" + "=" * 80)
    print("📊 테스트 결과 통계")
    print("=" * 80)
    print(f"✅ 성공: {alive_count}개 ({alive_count/total*100:.1f}%)")
    print(f"❌ 실패: {total - alive_count}개")
    print(f"💾 Redis alive 풀: {r.zcard(REDIS_ZSET_ALIVE)}개")
    print(f"⏱️  소요시간: {elapsed:.1f}초")
    print("=" * 80)
    
    # 상위 10개 프록시
    top_proxies = r.zrange(REDIS_ZSET_ALIVE, 0, 9, withscores=True)
    if top_proxies:
        print("\n🏆 상위 10개 프록시:")
        for proxy_str, score in top_proxies:
            protocol, addr = proxy_str.split("://", 1)
            pkey = f"{REDIS_KEY_PREFIX}:{protocol}:{addr}"
            pinfo = r.hgetall(pkey)
            country = pinfo.get("country", "Unknown")
            is_res = pinfo.get("is_residential", "")
            res_marker = " 🏠" if is_res == "True" else ""
            
            print(f"  • {proxy_str:35s} | score={int(score):>10} | {country}{res_marker}")
    
    print()

def get_my_real_ip() -> Optional[str]:
    """프록시 없이 자신의 실제 공인 IP 확인"""
    print("🔍 실제 공인 IP 확인 중...", end=" ")
    for url, _ in IP_CHECK_URLS:
        try:
            r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200:
                ip = r.text.strip()
                if ip and ('.' in ip or ':' in ip) and len(ip) < 50:
                    print(f"확인됨: {ip}")
                    return ip
        except:
            continue
    print("실패 (네트워크 문제 또는 차단)")
    return None

def main():
    global MY_REAL_IP
    
    print("=" * 80)
    print("🚀 Proxy Collector (개선됨)")
    print("=" * 80)
    
    # 프로그램 시작 시 한 번만 자신의 실제 IP 확인
    MY_REAL_IP = get_my_real_ip()
    
    print(f"⏱️  주기: {COLLECT_INTERVAL_MINUTES}분마다")
    print(f"🔧 동시 작업: {MAX_WORKERS}개 스레드")
    if RESIDENTIAL_ONLY:
        print("🏠 RESIDENTIAL_ONLY=True: residential IP만 수집")
    else:
        print("🌐 RESIDENTIAL_ONLY=False: 모든 프록시 수집 (빠름)")
    print("🛑 Ctrl+C로 중단 가능 (2번 누르면 강제 종료)")
    print("=" * 80)
    
    try:
        # 시작하자마자 한 번 실행
        collect_once()
        
        # 이후 주기적으로 반복
        while not STOP_EVENT.is_set():
            print(f"\n💤 {COLLECT_INTERVAL_MINUTES}분 대기 중...")
            print(f"   (현재 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
            
            # 1초 단위로 쪼개서 Ctrl+C 즉시 반응
            for i in range(COLLECT_INTERVAL_MINUTES * 60):
                if STOP_EVENT.is_set():
                    break
                if i > 0 and i % 60 == 0:
                    remaining = (COLLECT_INTERVAL_MINUTES * 60 - i) // 60
                    print(f"   ⏳ 남은 시간: {remaining}분")
                time.sleep(1)
            
            if STOP_EVENT.is_set():
                break
            
            collect_once()
    
    except KeyboardInterrupt:
        print("\n🛑 KeyboardInterrupt 감지")
        STOP_EVENT.set()
    finally:
        print("📚 Collector 종료 완료")

if __name__ == "__main__":
    main()