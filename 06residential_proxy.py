import time
import requests
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import redis
import threading
import signal
import os
import sys

# ================= 1. 설정 및 전역 변수 =================
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_ZSET_ALIVE = "proxies:alive"

RESIDENTIAL_ONLY = False 
MAX_WORKERS = 100  
COLLECT_INTERVAL_MINUTES = 10
STOP_EVENT = threading.Event()

# ================= 2. 유틸리티 함수 =================

def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def extract_ip(proxy_url: str):
    try: return proxy_url.split('@')[-1].split(':')[0]
    except: return None

def check_proxy_alive(proxy_url: str) -> bool:
    if STOP_EVENT.is_set(): return False
    proxies = {"http": proxy_url, "https": proxy_url}
    try:
        # 응답성 확인을 위해 타임아웃 3초
        resp = requests.get("https://www.google.com", proxies=proxies, timeout=3)
        return resp.status_code == 200
    except:
        return False

# ================= 3. 수집 소스 및 상세 로그 복구 =================

def fetch_all_proxies() -> List[str]:
    sources = {
        "monosans": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
        "victorgeel": "https://raw.githubusercontent.com/victorgeel/proxy-list-update/main/proxies/http.txt",
        "ErcinDedeoglu": "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt",
        "vakhov": "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt"
    }
    
    total_list = []
    print(f"\n🌐 [수집] 소스별 데이터 가져오기...")
    
    for name, url in sources.items():
        if STOP_EVENT.is_set(): break
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                proxies = [f"http://{l.strip()}" for l in r.text.splitlines() if l.strip()]
                print(f"   📥 {name:15} | 수집: {len(proxies):5}개")
                total_list.extend(proxies)
        except Exception as e:
            print(f"   ⚠️ {name:15} | 실패: {e}")
            
    return total_list

# ================= 4. 핵심 수집 루프 (로그 + 최적화) =================

def collect_once():
    start_time = time.time()
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 🚀 수집 프로세스 시작")
    
    # 1. 원본 로그 스타일 복구
    raw_proxies = fetch_all_proxies()
    if not raw_proxies:
        print("❌ 수집된 데이터가 없습니다.")
        return

    # 2. 중복 제거 상세 보고
    before_count = len(raw_proxies)
    unique_proxies = list(set(raw_proxies))
    after_count = len(unique_proxies)
    
    print(f"📊 수집 결과 요약:")
    print(f"   - 총 수집 개수  : {before_count}개")
    print(f"   - 고유 IP 개수  : {after_count}개")
    print(f"   - 중복 제거됨   : {before_count - after_count}개")

    # 3. 검증 대상 제한 (중단 속도 확보)
    limit = 10000
    check_list = unique_proxies[:limit]
    print(f"🔍 검증 진입: 선착순 {len(check_list)}개 진행 (RESIDENTIAL_ONLY={RESIDENTIAL_ONLY})")

    r = get_redis_client()
    valid_count = 0
    checked_count = 0
    
    # 4. 병렬 검증 (실시간 진행 로그)
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    future_to_proxy = {}

    try:
        for proxy in check_list:
            if STOP_EVENT.is_set(): break
            future = executor.submit(check_proxy_alive, proxy)
            future_to_proxy[future] = proxy

        for future in as_completed(future_to_proxy):
            if STOP_EVENT.is_set(): 
                print("\n🛑 중단 신호: 현재 스레드만 정리하고 즉시 멈춥니다.")
                break
            
            checked_count += 1
            try:
                if future.result():
                    # ZSET에 저장 (중복은 자동 처리됨)
                    r.zadd(REDIS_ZSET_ALIVE, {future_to_proxy[future]: int(time.time())})
                    valid_count += 1
            except: pass
            
            # 실시간 로그
            if checked_count % 100 == 0 or checked_count == len(check_list):
                print(f"\r   ⚡ 진행: {checked_count}/{len(check_list)} | ✅ 유효: {valid_count}", end="", flush=True)

    finally:
        executor.shutdown(wait=False)
        total_valid = r.zcard(REDIS_ZSET_ALIVE)
        elapsed = time.time() - start_time
        print(f"\n✨ 이번 주기 완료 ({elapsed:.1f}초)")
        print(f"   - 새로 추가: {valid_count}개")
        print(f"   - 현재 Redis 총 유효 IP (ZCARD): {total_valid}개")

# ================= 5. 메인 및 신호 처리 =================

def signal_handler(sig, frame):
    if not STOP_EVENT.is_set():
        print("\n\n🛑 [Signal] 중단 요청! 안전하게 마무리 중...")
        STOP_EVENT.set()
    else:
        print("\n💥 [Signal] 강제 종료!")
        os._exit(1)

def main_loop():
    print("=" * 60)
    print(f"📡 Collector v2.1 | Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"📡 Ctrl+C 1번: 안전 중단 | 2번: 즉시 사살")
    print("=" * 60)
    
    while not STOP_EVENT.is_set():
        collect_once()
        if STOP_EVENT.is_set(): break
        
        print(f"\n💤 {COLLECT_INTERVAL_MINUTES}분 대기...")
        for _ in range(COLLECT_INTERVAL_MINUTES * 60):
            if STOP_EVENT.is_set(): break
            time.sleep(1)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    main_loop()