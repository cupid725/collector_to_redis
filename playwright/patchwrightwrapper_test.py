import os
import argparse
import asyncio
import time
import random
from typing import Optional
import redis  # pip install redis
from PatchrightWrapper import StealthPatchrightBrowser

def _ts() -> str:
    return time.strftime("%H:%M:%S")

def log(msg: str) -> None:
    print(f"[{_ts()}] {msg}", flush=True)

# ✅ 기본 설정
TARGET_URL = "https://bot.sannysoft.com/"
#TARGET_URL = "https://abrahamjuliot.github.io/creepjs/"
PROXY = "http://65.109.177.138:8080"
# 예)
# PROXY = "http://127.0.0.1:8888"
# PROXY = "http://user:pass@host:port"
# PROXY = "socks5://host:port"


# ===================== Redis 설정 (proxy lease) =====================
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None

# Lease 방식 키
REDIS_ZSET_ALIVE = "proxies:alive"        # collector가 넣는 풀 (score는 next_available_epoch 권장. 0이면 즉시 사용 가능)
REDIS_ZSET_LEASE = "proxies:lease"        # client가 임대 중인 프록시 (score는 lease_expire_epoch)
REDIS_HASH_FAIL  = "proxies:fail"         # 실패 카운트 (선택)

# claim:
#  1) 만료된 lease를 alive로 회수
#  2) alive에서 (score<=now) 인 후보 중 앞쪽 sample_k개를 가져와 랜덤 1개 선택
#  3) alive -> lease로 이동
_LUA_CLAIM = r"""
local alive = KEYS[1]
local lease = KEYS[2]
local now = tonumber(ARGV[1])
local lease_sec = tonumber(ARGV[2])
local reclaim_limit = tonumber(ARGV[3])
local sample_k = tonumber(ARGV[4])
local rand_int = tonumber(ARGV[5])

-- 1) 만료된 lease 회수
local expired = redis.call('ZRANGEBYSCORE', lease, '-inf', now, 'LIMIT', 0, reclaim_limit)
for i, m in ipairs(expired) do
  redis.call('ZREM', lease, m)
  redis.call('ZADD', alive, 0, m)
end

-- 2) 사용 가능한 후보들 중 앞쪽 sample_k개
local cands = redis.call('ZRANGEBYSCORE', alive, '-inf', now, 'LIMIT', 0, sample_k)
if (not cands) or (#cands == 0) then
  return nil
end

-- 3) 랜덤 1개 선택 (rand_int를 이용해 결정)
local idx = (rand_int % #cands) + 1
local m = cands[idx]

redis.call('ZREM', alive, m)
redis.call('ZADD', lease, now + lease_sec, m)
return m
"""

# release: lease -> alive 로 이동, score = next_time(epoch)
_LUA_RELEASE = r"""
local alive = KEYS[1]
local lease = KEYS[2]
local member = ARGV[1]
local next_time = tonumber(ARGV[2])

redis.call('ZREM', lease, member)
redis.call('ZADD', alive, next_time, member)
return 1
"""

# ban: alive/lease 모두에서 제거
_LUA_BAN = r"""
local alive = KEYS[1]
local lease = KEYS[2]
local member = ARGV[1]
redis.call('ZREM', alive, member)
redis.call('ZREM', lease, member)
return 1
"""

def get_redis(
    host: str = REDIS_HOST,
    port: int = REDIS_PORT,
    db: int = REDIS_DB,
    password: Optional[str] = REDIS_PASSWORD,
) -> redis.Redis:
    return redis.Redis(
        host=host,
        port=port,
        db=db,
        password=password,
        decode_responses=True,  # member를 str로 다루기
    )

def claim_proxy(
    r: redis.Redis,
    lease_seconds: int,
    reclaim_limit: int = 200,
    sample_k: int = 50,
) -> Optional[str]:
    """alive에서 프록시 1개를 임대(claim). 반환: 'proto://ip:port' or None"""
    now = int(time.time())
    rand_int = random.randint(0, 2_147_483_647)
    try:
        member = r.eval(
            _LUA_CLAIM,
            2,
            REDIS_ZSET_ALIVE,
            REDIS_ZSET_LEASE,
            now,
            int(lease_seconds),
            int(reclaim_limit),
            int(sample_k),
            int(rand_int),
        )
    except redis.RedisError as e:
        log(f"[REDIS] claim_proxy 실패: {e}")
        return None

    if not member:
        return None
    if "://" not in member:
        return None
    return member

def release_proxy(r: redis.Redis, member: str, cooldown_seconds: int = 0) -> None:
    """임대된 프록시를 alive로 반납(release)."""
    next_time = int(time.time()) + max(0, int(cooldown_seconds))
    try:
        r.eval(_LUA_RELEASE, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, member, next_time)
    except redis.RedisError as e:
        log(f"[REDIS] release_proxy 실패: {e}")

def ban_proxy(r: redis.Redis, member: str) -> None:
    """문제 프록시를 풀에서 제거(ban)."""
    try:
        r.eval(_LUA_BAN, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, member)
    except redis.RedisError as e:
        log(f"[REDIS] ban_proxy 실패: {e}")

def inc_fail(r: redis.Redis, member: str) -> int:
    """실패 카운트 +1"""
    try:
        return int(r.hincrby(REDIS_HASH_FAIL, member, 1))
    except redis.RedisError:
        return 1

def reset_fail(r: redis.Redis, member: str) -> None:
    """실패 카운트 초기화"""
    try:
        r.hdel(REDIS_HASH_FAIL, member)
    except redis.RedisError:
        pass


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=TARGET_URL)
    parser.add_argument("--proxy", default=PROXY)
    parser.add_argument("--mobile", action="store_true", help="모바일(Android) 디바이스만 랜덤 선택")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--keep-profile", action="store_true", help="자동 생성 user_data_dir 삭제하지 않음")

    # ✅ Redis에서 프록시 claim/release 사용
    parser.add_argument("--proxy-from-redis", action="store_true", help="Redis에서 프록시를 하나 임대해서 사용 후 반납")
    parser.add_argument("--redis-host", default=REDIS_HOST)
    parser.add_argument("--redis-port", type=int, default=REDIS_PORT)
    parser.add_argument("--redis-db", type=int, default=REDIS_DB)
    parser.add_argument("--redis-password", default=REDIS_PASSWORD)

    # 운영 파라미터(기본값은 live_undetectedchrome_from_redis02_mobile.py의 의도에 맞게 넉넉히)
    parser.add_argument("--lease-seconds", type=int, default=300)
    parser.add_argument("--cooldown-success", type=int, default=0)
    parser.add_argument("--cooldown-fail-base", type=int, default=30)
    parser.add_argument("--cooldown-fail-jitter", type=int, default=60)
    parser.add_argument("--max-fail", type=int, default=5)

    args = parser.parse_args()

    log(f"[BOOT] url={args.url} | mobile={args.mobile} | headless={args.headless} | keep_profile={args.keep_profile} | proxy_from_redis={args.proxy_from_redis}")

    # ✅ (추가) redis에서 프록시를 가져오는 경우
    r = None
    proxy_member = None
    session_ok = False

    if args.proxy_from_redis:
        log(f"[REDIS] connecting host={args.redis_host}:{args.redis_port} db={args.redis_db} auth={'yes' if bool(args.redis_password) else 'no'}")
        r = get_redis(
            host=args.redis_host,
            port=args.redis_port,
            db=args.redis_db,
            password=args.redis_password,
        )
        try:
            r.ping()
            log("[REDIS] ping=OK")
        except Exception as e:
            log(f"[REDIS] ping=FAIL: {type(e).__name__}: {e}")
            return
        proxy_member = claim_proxy(r, lease_seconds=int(args.lease_seconds), reclaim_limit=200, sample_k=50)
        if not proxy_member:
            log("[REDIS] 사용 가능한 프록시가 없어 종료함.")
            return
        args.proxy = proxy_member
        log(f"[REDIS] ✅ proxy claimed: {proxy_member}")
        log(f"[REDIS] lease_seconds={args.lease_seconds} (member is expected to be like proto://ip:port)")

    try:
        log(f"[RUN] proxy_in_use={args.proxy}")

        browser = StealthPatchrightBrowser(
            proxy=args.proxy,
            webrtc_leak_protection=True,
            headless=args.headless,
            mobile=args.mobile,
            cleanup_user_data_dir=not args.keep_profile,
        )

        async with browser:
            page = await browser.new_page()
            if getattr(browser, "selected_device_name", None):
                log(f"[DEVICE] selected={browser.selected_device_name}")
            else:
                log("[DEVICE] selected=(none)")
            t0 = time.time()
            log(f"[NAV] goto start wait_until=networkidle timeout={60000*2}ms")
            await page.goto(args.url, wait_until="networkidle", timeout=60000*2)
            log(f"[NAV] goto done elapsed={time.time()-t0:.2f}s")
            log(f"[OK] 접속 완료: {args.url}")
            log("[WAIT] 120초 대기...")
            await asyncio.sleep(120)

        session_ok = True
        log("[RUN] session_ok=True")

    except Exception as e:
        log(f"[ERR] 실행 중 예외: {type(e).__name__}: {e}")

    finally:
        # ✅ Redis 반납(성공/실패에 따라 cooldown/ban 처리)
        if r and proxy_member:
            if session_ok:
                reset_fail(r, proxy_member)
                release_proxy(r, proxy_member, cooldown_seconds=int(args.cooldown_success))
                log(f"[REDIS] 🔓 proxy released (ok): {proxy_member}")
            else:
                fails = inc_fail(r, proxy_member)
                if fails >= int(args.max_fail):
                    ban_proxy(r, proxy_member)
                    log(f"[REDIS] ⛔ proxy banned (fails={fails}): {proxy_member}")
                else:
                    cooldown = int(args.cooldown_fail_base) + random.randint(0, max(0, int(args.cooldown_fail_jitter)))
                    release_proxy(r, proxy_member, cooldown_seconds=cooldown)
                    log(f"[REDIS] 🔓 proxy released (fail={fails}, cooldown={cooldown}s): {proxy_member}")


if __name__ == "__main__":
    asyncio.run(main())
