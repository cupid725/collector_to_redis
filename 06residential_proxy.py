import time
import requests
import redis
import threading
import signal
import os
import socket
import struct
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import lru_cache
from urllib3.connection import HTTPConnection

# ================= 1. 전역 설정 및 신호 처리 =================
STOP_EVENT = threading.Event()
MY_REAL_IP: Optional[str] = None

def _sigint_handler(sig, frame):
    print("\n🛑 중단 신호 감지: 현재 진행 중인 검사까지만 완료하고 종료합니다.")
    STOP_EVENT.set()
    signal.signal(signal.SIGINT, lambda s, f: os._exit(1))

signal.signal(signal.SIGINT, _sigint_handler)

# Redis 설정
REDIS_HOST, REDIS_PORT, REDIS_DB = "127.0.0.1", 6379, 0
REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_LEASE = "proxies:lease"
REDIS_KEY_PREFIX = "proxy"

COLLECT_INTERVAL_MINUTES = 10
MAX_WORKERS = 60
RESIDENTIAL_ONLY = True

# ================= 2. 원본 데이터 (누락 없음) =================

# ✅ 요청하신 14개 키워드 정확히 포함
DATACENTER_KEYWORDS = [
    "amazon", "aws", "google", "microsoft", "azure", "digitalocean",
    "linode", "ovh", "hetzner", "vultr", "datacenter", "hosting", "cloud", "vps"
]

# ✅ SOURCES 1, 2, 3 (총 21개 소스 전체 복구)
SOURCES_1 = [
    ("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt", "http", False),
    ("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt", "socks5", False),
    ("https://raw.githubusercontent.com/victorgeel/proxy-list-update/main/proxies/http.txt", "http", False),
    ("https://raw.githubusercontent.com/victorgeel/proxy-list-update/main/proxies/socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/victorgeel/proxy-list-update/main/proxies/socks5.txt", "socks5", False),
    ("https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt", "http", False),
    ("https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt", "socks5", False),
    ("https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt", "http", False),
    ("https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt", "socks5", False),
    
    ("https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt", "http", True),
    ("https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt", "socks5", True),
    ("https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt", "socks5", True),
]

SOURCES_2 = [
    ("https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt", "socks5", False),
    ("https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt", "http", False),
    ("https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt", "http", False),
    ("https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/https.txt", "http", False),
    ("https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks5.txt", "socks5", False),
    ("https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/http/http.txt", "http", False),
    ("https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/https/https.txt", "http", False),
    ("https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/socks4/socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/socks5/socks5.txt", "socks5", False),
    ("https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt", "http", False),
    ("https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt", "socks5", False),    
]

SOURCES_3 = [
    ("https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt", "http", False),
    ("https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks5.txt", "socks5", False),
    ("https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt", "http", False),
    ("https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt", "socks5", False),
    ("https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt", "http", False),
    ("https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt", "socks4", False),
    ("https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt", "socks5", False),
    ("https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/http.txt", "http", True),
    ("https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/socks4.txt", "socks4", True),
    ("https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/socks5.txt", "socks5", True),
    ("https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt", "http", False),
    ("https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt", "socks5", False),
    ("https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/http.txt", "http", False),
    ("https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/socks5.txt", "socks5", False),
]

SOURCES = SOURCES_1
ALL_SOURCES = SOURCES_3 + SOURCES_2 + SOURCES_1
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

# ================= 3. SO_LINGER 주입 설정 =================

# 모든 새 소켓 연결에 대해 SO_LINGER(1, 0) 옵션을 기본값으로 설정합니다.
# 이렇게 하면 별도의 세션 관리 없이도 모든 requests.get 호출 시 적용됩니다.
linger_option = (socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
HTTPConnection.default_socket_options = HTTPConnection.default_socket_options + [linger_option]



# ================= 4. 핵심 검증 로직 =================

@lru_cache(maxsize=3000)
def get_ip_info(ip: str):
    """GeoIP를 통해 Residential 여부 판별 (원본 로직 준수)"""
    try:
        r = requests.get(f"http://ip-api.com/json/{ip}?fields=status,country,isp,org,as", timeout=5)
        data = r.json()
        if data.get("status") == "success":
            info = f"{data.get('isp', '')} {data.get('org', '')} {data.get('as', '')}".lower()
            # 사용자 지정 14개 키워드 검사
            is_res = not any(kw in info for kw in DATACENTER_KEYWORDS)
            return data.get("country", "Unknown"), is_res
    except: pass
    return "Unknown", True

def check_proxy(p: Dict) -> Optional[str]:
    """성공률을 위해 원본과 동일하게 독립적인 requests.get 호출"""
    proxy_url = f"{p['protocol']}://{p['address']}"
    proxies = {"http": proxy_url, "https": proxy_url}
    try:
        # 상단에서 설정한 default_socket_options 덕분에 자동으로 SO_LINGER 적용됨
        r = requests.get("https://api.ipify.org?format=text", 
                         proxies=proxies, headers=HEADERS, timeout=(12, 12))
        if r.status_code == 200 and len(r.text.strip()) < 50:
            return r.text.strip()
    except: pass
    return None

def process_proxy(p: Dict, r_client: redis.Redis):
    """프록시 검증 후 Redis 파이프라인으로 저장"""
    if STOP_EVENT.is_set(): return
    obtained_ip = check_proxy(p)
    if not obtained_ip or obtained_ip == MY_REAL_IP: return

    country, is_res = "Unknown", True
    if RESIDENTIAL_ONLY:
        country, is_res = get_ip_info(obtained_ip)
        if not is_res: return

    member = f"{p['protocol']}://{p['address']}"
    key = f"{REDIS_KEY_PREFIX}:{p['protocol']}:{p['address']}"
    
    try:
        with r_client.pipeline() as pipe:
            pipe.hset(key, mapping={
                "protocol": p['protocol'], "address": p['address'], 
                "ip": obtained_ip, "country": country,
                "is_residential": str(is_res), "updated_at": datetime.now().isoformat()
            })
            pipe.expire(key, 86400)
            if not r_client.zscore(REDIS_ZSET_LEASE, member):
                pipe.zadd(REDIS_ZSET_ALIVE, {member: 0})
            pipe.execute()
        return True
    except: return False

def collect_once():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    raw_list = []
    # 21개 소스 순회
    for url, proto, _ in ALL_SOURCES:
        if STOP_EVENT.is_set(): break
        try:
            resp = requests.get(url, timeout=20, headers=HEADERS)
            if resp.status_code == 200:
                for line in resp.text.splitlines():
                    line = line.strip()
                    if not line or line.startswith('#'): continue
                    addr = line.split("://")[-1] if "://" in line else line
                    if ":" in addr: raw_list.append({"address": addr, "protocol": proto})
        except: continue

    unique_proxies = list({(p['protocol'], p['address']): p for p in raw_list}.values())
    total = len(unique_proxies)
    print(f"\n🚀 {datetime.now().strftime('%H:%M:%S')} | 고유 대상: {total}개")
    
    success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_proxy, p, r) for p in unique_proxies]
        for i, f in enumerate(as_completed(futures)):
            if f.result(): success += 1
            if (i+1) % 100 == 0: print(f" 진행 중: [{i+1}/{total}] | 성공: {success}", end='\r')
    print(f"\n✅ 완료! 유효 프록시: {success}개")

def main():
    global MY_REAL_IP
    try:
        MY_REAL_IP = requests.get("https://api.ipify.org", timeout=10).text.strip()
    except: pass
    while not STOP_EVENT.is_set():
        collect_once()
        for _ in range(COLLECT_INTERVAL_MINUTES * 60):
            if STOP_EVENT.is_set(): break
            time.sleep(1)

if __name__ == "__main__":
    main()