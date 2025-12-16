#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
client_from_redis.py (lease-based)
- 목적: Redis ZSET 'proxies:alive'에 있는 프록시를 "전체를 골고루" 사용하면서,
        멀티 워커에서도 같은 프록시가 동시에 중복 배정되지 않게 임대(lease) 방식으로 pick.

요구되는 Redis 구조
- ZSET proxies:alive  : member="proto://ip:port", score=next_available_epoch(초). (보통 0이면 즉시 사용 가능)
- ZSET proxies:lease  : member="proto://ip:port", score=lease_expire_epoch(초).
- HASH proxies:fail   : field=member, value=fail_count (선택)

collector 측 권장 동작
- 살아있다고 확인된 프록시는:
  ZADD proxies:alive 0 "http://1.2.3.4:8080"
  ZADD proxies:alive 0 "https://1.2.3.4:8080"   # https 프록시도 member는 https:// 로 저장
  ZADD proxies:alive 0 "socks4://1.2.3.4:1080"
  ZADD proxies:alive 0 "socks5://1.2.3.4:1080"

주의
- requests에서 socks4/socks5를 쓰려면: pip install "requests[socks]"
"""

import argparse
import random
import time
from typing import Optional, Tuple

import redis
import requests


REDIS_ZSET_ALIVE = "proxies:alive"
REDIS_ZSET_LEASE = "proxies:lease"
REDIS_HASH_FAIL = "proxies:fail"


# ---- Lua: claim(임대) / release(반납) / ban(제거) ----

_LUA_CLAIM = r"""
local alive = KEYS[1]
local lease = KEYS[2]
local now = tonumber(ARGV[1])
local lease_sec = tonumber(ARGV[2])
local reclaim_limit = tonumber(ARGV[3])

-- 1) 만료된 lease 회수
local expired = redis.call('ZRANGEBYSCORE', lease, '-inf', now, 'LIMIT', 0, reclaim_limit)
for i, m in ipairs(expired) do
  redis.call('ZREM', lease, m)
  redis.call('ZADD', alive, 0, m)  -- 즉시 사용 가능으로 복귀
end

-- 2) 지금 사용 가능한 후보( score <= now ) 중 1개
local cand = redis.call('ZRANGEBYSCORE', alive, '-inf', now, 'LIMIT', 0, 1)
if (not cand) or (#cand == 0) then
  return nil
end
local m = cand[1]

-- 3) alive -> lease (임대 만료 시각 = now + lease_sec)
redis.call('ZREM', alive, m)
redis.call('ZADD', lease, now + lease_sec, m)
return m
"""

_LUA_RELEASE = r"""
local alive = KEYS[1]
local lease = KEYS[2]
local member = ARGV[1]
local next_time = tonumber(ARGV[2])

redis.call('ZREM', lease, member)
redis.call('ZADD', alive, next_time, member)
return 1
"""

_LUA_BAN = r"""
local alive = KEYS[1]
local lease = KEYS[2]
local member = ARGV[1]
redis.call('ZREM', alive, member)
redis.call('ZREM', lease, member)
return 1
"""


def _decode(x) -> str:
    if isinstance(x, (bytes, bytearray)):
        return x.decode("utf-8", "ignore")
    return str(x)


def claim_proxy(r: redis.Redis, lease_seconds: int = 90, reclaim_limit: int = 200) -> Optional[str]:
    """alive에서 하나 임대(claim). 반환: 'proto://ip:port' or None"""
    now = int(time.time())
    m = r.eval(_LUA_CLAIM, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, now, int(lease_seconds), int(reclaim_limit))
    if not m:
        return None
    m = _decode(m)
    if "://" not in m:
        return None
    return m


def release_proxy(r: redis.Redis, proxy: str, cooldown_seconds: int = 0) -> None:
    """작업 완료 후 반납(release)."""
    next_time = int(time.time()) + max(0, int(cooldown_seconds))
    r.eval(_LUA_RELEASE, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, proxy, next_time)


def ban_proxy(r: redis.Redis, proxy: str) -> None:
    """문제 프록시를 풀에서 제거(ban)."""
    r.eval(_LUA_BAN, 2, REDIS_ZSET_ALIVE, REDIS_ZSET_LEASE, proxy)


def inc_fail(r: redis.Redis, proxy: str) -> int:
    """실패 카운트 +1"""
    return int(r.hincrby(REDIS_HASH_FAIL, proxy, 1))


def reset_fail(r: redis.Redis, proxy: str) -> None:
    """실패 카운트 초기화"""
    r.hdel(REDIS_HASH_FAIL, proxy)


def parse_member(member: str) -> Tuple[str, str]:
    """'proto://ip:port' -> (proto, 'ip:port')"""
    proto, rest = member.split("://", 1)
    return proto.lower(), rest.strip()


def build_requests_proxies(member: str) -> dict:
    """
    requests용 proxies dict 생성
    - http/https member: 실제 프록시 스킴은 http://ip:port 로 사용 (CONNECT)
    - socks4/socks5 member: socks4://ip:port / socks5://ip:port
    """
    proto, addr = parse_member(member)

    if proto in ("http", "https"):
        proxy_url = f"http://{addr}"
        return {"http": proxy_url, "https": proxy_url}

    if proto == "socks4":
        proxy_url = f"socks4://{addr}"
        return {"http": proxy_url, "https": proxy_url}

    if proto == "socks5":
        proxy_url = f"socks5://{addr}"
        return {"http": proxy_url, "https": proxy_url}

    return {}


def test_proxy_requests(member: str, url: str, timeout: int = 12) -> bool:
    """간단 접속 테스트"""
    proxies = build_requests_proxies(member)
    if not proxies:
        return False

    try:
        with requests.get(url, proxies=proxies, timeout=timeout, stream=True, allow_redirects=True) as resp:
            ok = 200 <= resp.status_code < 400
            if ok:
                _ = resp.raw.read(256)
            return ok
    except Exception:
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-host", default="127.0.0.1")
    ap.add_argument("--redis-port", type=int, default=6379)
    ap.add_argument("--redis-db", type=int, default=0)

    ap.add_argument("--target-url", default="https://example.com/")
    ap.add_argument("--timeout", type=int, default=12)

    ap.add_argument("--lease-seconds", type=int, default=90, help="프록시 임대 유지 시간(초)")
    ap.add_argument("--cooldown-success", type=int, default=0, help="성공 후 재사용 쿨다운(초)")
    ap.add_argument("--cooldown-fail-base", type=int, default=10, help="실패 후 기본 쿨다운(초)")
    ap.add_argument("--cooldown-fail-jitter", type=int, default=10, help="실패 후 랜덤 지터(초)")
    ap.add_argument("--max-fail", type=int, default=5, help="이 횟수 이상 실패하면 ban(풀에서 제거)")

    ap.add_argument("--loop", action="store_true", help="무한 루프로 계속 소비")
    ap.add_argument("--sleep-when-empty", type=int, default=2, help="사용 가능한 프록시 없을 때 대기(초)")
    args = ap.parse_args()

    r = redis.Redis(host=args.redis_host, port=args.redis_port, db=args.redis_db, decode_responses=False)

    def one_round() -> None:
        member = claim_proxy(r, lease_seconds=args.lease_seconds)
        if not member:
            time.sleep(max(1, args.sleep_when_empty))
            return

        ok = test_proxy_requests(member, args.target_url, timeout=args.timeout)

        if ok:
            reset_fail(r, member)
            release_proxy(r, member, cooldown_seconds=args.cooldown_success)
            print(f"[OK] {member}")
        else:
            fails = inc_fail(r, member)
            if fails >= args.max_fail:
                ban_proxy(r, member)
                print(f"[BAN] {member} (fails={fails})")
            else:
                cooldown = args.cooldown_fail_base + random.randint(0, max(0, args.cooldown_fail_jitter))
                release_proxy(r, member, cooldown_seconds=cooldown)
                print(f"[FAIL] {member} (fails={fails}, cooldown={cooldown}s)")

    if args.loop:
        while True:
            one_round()
    else:
        one_round()


if __name__ == "__main__":
    main()
