# collector.py (SET ... NX 버전)
# 설계는 그대로 유지:
#   ✅ SADD proxies:pool proxy
#   ✅ SET proxy "<meta>" EX 21600  (proxy 문자열을 key로 TTL)
#
# 개선점(중요):
#   - SET에 NX를 붙여서 "키가 없을 때만" 생성 + TTL 설정
#   - 이미 존재하는 proxy key는 TTL을 갱신하지 않음
#     => 재실행/반복 실행해도 매번 19만건 TTL 갱신으로 느려지는 문제 해결
#     => TTL 6시간의 의미(오래된 건 자연히 사라짐)도 더 선명해짐
#
# 설치:
#   pip install requests redis
# 실행:
#   python collector.py

import json
import random
import signal
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import redis
import requests

# =========================
# 프록시 소스 URL
# =========================
MONOSANS_HTTP = "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt"
MONOSANS_SOCKS4 = "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt"
MONOSANS_SOCKS5 = "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt"

VICTORGEEL_HTTP = "https://raw.githubusercontent.com/victorgeel/proxy-list-update/main/proxies/http.txt"
VICTORGEEL_SOCKS4 = "https://raw.githubusercontent.com/victorgeel/proxy-list-update/main/proxies/socks4.txt"
VICTORGEEL_SOCKS5 = "https://raw.githubusercontent.com/victorgeel/proxy-list-update/main/proxies/socks5.txt"

ERCINDEDEOGLU_HTTP = "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt"
ERCINDEDEOGLU_HTTPS = "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt"
ERCINDEDEOGLU_SOCKS4 = "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt"
ERCINDEDEOGLU_SOCKS5 = "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt"

VAKHOV_HTTP = "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt"
VAKHOV_HTTPS = "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/https.txt"
VAKHOV_SOCKS4 = "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt"
VAKHOV_SOCKS5 = "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt"

SOURCES: List[Tuple[str, str, str]] = [
    (MONOSANS_HTTP, "http", "monosans_http"),
    (MONOSANS_SOCKS4, "socks4", "monosans_socks4"),
    (MONOSANS_SOCKS5, "socks5", "monosans_socks5"),
    (VICTORGEEL_HTTP, "http", "victorgeel_http"),
    (VICTORGEEL_SOCKS4, "socks4", "victorgeel_socks4"),
    (VICTORGEEL_SOCKS5, "socks5", "victorgeel_socks5"),
    (ERCINDEDEOGLU_HTTP, "http", "ercindedeoglu_http"),
    (ERCINDEDEOGLU_HTTPS, "https", "ercindedeoglu_https"),
    (ERCINDEDEOGLU_SOCKS4, "socks4", "ercindedeoglu_socks4"),
    (ERCINDEDEOGLU_SOCKS5, "socks5", "ercindedeoglu_socks5"),
    (VAKHOV_HTTP, "http", "vakhov_http"),
    (VAKHOV_HTTPS, "https", "vakhov_https"),
    (VAKHOV_SOCKS4, "socks4", "vakhov_socks4"),
    (VAKHOV_SOCKS5, "socks5", "vakhov_socks5"),
]

# =========================
# 설정
# =========================
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None

POOL_KEY = "proxies:pool"

TTL_SECONDS = 21600  # 6h
COLLECT_INTERVAL_MINUTES = 30

FETCH_TIMEOUT = 30
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

# 대량 저장 멈춤 방지용 청크
REDIS_CHUNK_SIZE = 5000

# 너무 많으면 이번 라운드에서 샘플링(원하면 None)
MAX_ADD_PER_ROUND: Optional[int] = None  # 예: 50000 / None

STOP = False


def _handle_sigint(sig, frame):
    global STOP
    STOP = True
    print("\n🛑 Ctrl+C 감지: 가능한 빨리 중단합니다(현재 작업/청크 완료 후 종료).")


signal.signal(signal.SIGINT, _handle_sigint)


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_connect_timeout=10,
        socket_timeout=30,
        retry_on_timeout=True,
    )


def normalize_line_to_addr(line: str) -> Optional[str]:
    s = line.strip()
    if not s or s.startswith("#"):
        return None

    if "://" in s:
        s = s.split("://", 1)[1]

    s = s.split("/", 1)[0].strip()
    if ":" not in s:
        return None

    host, port = s.rsplit(":", 1)
    host = host.strip()
    port = port.strip()

    if not host or not port.isdigit():
        return None

    p = int(port)
    if p <= 0 or p > 65535:
        return None

    return f"{host}:{p}"


def fetch_source(url: str, protocol: str, source_name: str) -> List[str]:
    if STOP:
        return []

    print(f"📥 GET {source_name:20s} ({protocol})")
    try:
        resp = requests.get(url, timeout=FETCH_TIMEOUT, headers={"User-Agent": UA})
        resp.raise_for_status()

        out: List[str] = []
        for line in resp.text.splitlines():
            addr = normalize_line_to_addr(line)
            if not addr:
                continue
            out.append(f"{protocol}://{addr}")

        print(f"   ✅ parsed={len(out)}")
        return out
    except Exception as e:
        print(f"   ❌ fail: {type(e).__name__}: {str(e)[:140]}")
        return []


def collect_all_unique() -> Tuple[List[str], Dict[str, int]]:
    unique: Set[str] = set()
    stats = {"http": 0, "https": 0, "socks4": 0, "socks5": 0, "sources_ok": 0, "sources_total": len(SOURCES)}

    for url, proto, name in SOURCES:
        if STOP:
            break
        items = fetch_source(url, proto, name)
        if items:
            stats["sources_ok"] += 1
        unique.update(items)
        time.sleep(0.15)

    for m in unique:
        if m.startswith("http://"):
            stats["http"] += 1
        elif m.startswith("https://"):
            stats["https"] += 1
        elif m.startswith("socks4://"):
            stats["socks4"] += 1
        elif m.startswith("socks5://"):
            stats["socks5"] += 1

    return list(unique), stats


def iter_chunks(items: List[str], chunk_size: int):
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def redis_save_chunked_nx(r: redis.Redis, proxies: List[str]) -> Tuple[int, int]:
    """
    설계 고정 + NX 최적화:
      - SADD proxies:pool proxy
      - SET proxy "<meta>" EX 21600 NX  (없을 때만 생성)
    반환:
      (pool_added_total, keys_created_total)
    """
    if not proxies:
        return (0, 0)

    if MAX_ADD_PER_ROUND is not None and len(proxies) > MAX_ADD_PER_ROUND:
        proxies = random.sample(proxies, MAX_ADD_PER_ROUND)

    total = len(proxies)
    ts = utc_iso()

    pool_added_total = 0
    keys_created_total = 0

    print(f"💾 Redis 저장(청크+NX): total={total}, chunk={REDIS_CHUNK_SIZE}")

    # 1) pool 저장 (SADD) - 청크
    done = 0
    for ck in iter_chunks(proxies, REDIS_CHUNK_SIZE):
        if STOP:
            break
        done += len(ck)
        try:
            added = r.sadd(POOL_KEY, *ck)
            pool_added_total += int(added) if added is not None else 0
            print(f"  [POOL] {done}/{total} | +{added} new")
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"  ⚠️ [POOL] chunk fail: {type(e).__name__}: {str(e)[:160]}")

    if STOP:
        return (pool_added_total, keys_created_total)

    # 2) key 저장 (SET EX NX) - 청크
    done = 0
    for ck in iter_chunks(proxies, REDIS_CHUNK_SIZE):
        if STOP:
            break

        pipe = r.pipeline(transaction=False)

        # SET ... NX 는 성공 시 True/OK, 실패(이미 존재) 시 None
        for p in ck:
            meta = {"collected_at": ts}
            pipe.set(p, json.dumps(meta, ensure_ascii=False), ex=TTL_SECONDS, nx=True)

        try:
            results = pipe.execute()
            created = sum(1 for x in results if x)  # True/OK count
            keys_created_total += created
            done += len(ck)
            print(f"  [KEYS] {done}/{total} | created={created} (NX)")
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"  ⚠️ [KEYS] chunk fail: {type(e).__name__}: {str(e)[:160]}")

    return (pool_added_total, keys_created_total)


def main_loop():
    r = get_redis()
    try:
        r.ping()
        print("✅ Redis PING OK")
    except Exception as e:
        print(f"❌ Redis 연결 실패: {type(e).__name__}: {e}")
        return

    print("=" * 80)
    print("🚀 collector (fixed design + NX optimization)")
    print("✅ SADD proxies:pool proxy")
    print("✅ SET proxy '<meta>' EX 21600 NX  (키 없을 때만 생성)")
    print(f"• interval: {COLLECT_INTERVAL_MINUTES} min | chunk: {REDIS_CHUNK_SIZE}")
    print("🛑 Ctrl+C 로 종료")
    print("=" * 80)

    while not STOP:
        t0 = time.time()
        print("\n" + "=" * 80)
        print(f"🕐 collect start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

        proxies, stats = collect_all_unique()
        print("-" * 80)
        print(
            f"📦 unique={len(proxies)} | "
            f"http={stats['http']} https={stats['https']} socks4={stats['socks4']} socks5={stats['socks5']} | "
            f"sources_ok={stats['sources_ok']}/{stats['sources_total']}"
        )
        print("-" * 80)

        try:
            pool_added, keys_created = redis_save_chunked_nx(r, proxies)
            pool_size = r.scard(POOL_KEY)
            print(f"✅ redis done: pool_added={pool_added} keys_created={keys_created} pool_size={pool_size}")
        except KeyboardInterrupt:
            print("\n🛑 종료합니다.")
            break

        elapsed = time.time() - t0
        print(f"⏱️  elapsed: {elapsed:.1f}s")

        if STOP:
            break

        sleep_sec = max(5, COLLECT_INTERVAL_MINUTES * 60 - int(elapsed))
        print(f"💤 sleep {sleep_sec}s ...")
        for _ in range(sleep_sec):
            if STOP:
                break
            time.sleep(1)

    print("👋 collector stopped.")


if __name__ == "__main__":
    main_loop()
