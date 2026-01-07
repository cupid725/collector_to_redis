# redis_proxy_lease.py
from __future__ import annotations

import time
import random
from dataclasses import dataclass
from typing import Optional, Dict, Any

import redis


@dataclass(frozen=True)
class RedisConnConfig:
    host: str = "127.0.0.1"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None


class RedisProxyLeaseClient:
    """
    ZSET 기반 프록시 풀(Alive/Lease) + Fail 카운트(Hash) 관리 클래스.

    - alive zset  : score = next_available_epoch (0이면 즉시 사용)
    - lease zset  : score = lease_expire_epoch
    - fail  hash  : member -> fail_count

    claim:
      1) 만료된 lease를 alive로 회수
      2) alive에서 (score<=now) 후보 sample_k개를 가져와 랜덤 1개 선택
      3) alive -> lease 이동 (lease 만료시간 부여)
    """

    DEFAULT_ALIVE_KEY = "proxies:alive"
    DEFAULT_LEASE_KEY = "proxies:lease"
    DEFAULT_FAIL_HASH = "proxies:fail"

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

    -- 3) 랜덤 1개 선택
    local idx = (rand_int % #cands) + 1
    local m = cands[idx]

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

    def __init__(
        self,
        config: RedisConnConfig,
        *,
        alive_key: str = DEFAULT_ALIVE_KEY,
        lease_key: str = DEFAULT_LEASE_KEY,
        fail_hash: str = DEFAULT_FAIL_HASH,
        decode_responses: bool = True,
        socket_timeout: Optional[float] = None,
    ):
        self.config = config
        self.alive_key = alive_key
        self.lease_key = lease_key
        self.fail_hash = fail_hash
        self.decode_responses = decode_responses
        self.socket_timeout = socket_timeout
        self._r: Optional[redis.Redis] = None

    def connect(self) -> redis.Redis:
        r = redis.Redis(
            host=self.config.host,
            port=self.config.port,
            db=self.config.db,
            password=self.config.password,
            decode_responses=self.decode_responses,
            socket_timeout=self.socket_timeout,
        )
        r.ping()
        self._r = r
        return r

    @property
    def r(self) -> redis.Redis:
        if self._r is None:
            raise RuntimeError("RedisProxyLeaseClient: not connected. call connect() first.")
        return self._r

    def close(self) -> None:
        try:
            if self._r is not None:
                self._r.close()
        except Exception:
            pass
        self._r = None

    def claim(self, *, lease_seconds: int, reclaim_limit: int = 200, sample_k: int = 50) -> Optional[str]:
        now = int(time.time())
        rand_int = random.randint(0, 2_147_483_647)
        try:
            member = self.r.eval(
                self._LUA_CLAIM,
                2,
                self.alive_key,
                self.lease_key,
                now,
                int(lease_seconds),
                int(reclaim_limit),
                int(sample_k),
                int(rand_int),
            )
        except redis.RedisError:
            return None

        if not member:
            return None
        if not isinstance(member, str):
            member = str(member)
        if "://" not in member:
            return None
        return member

    def release(self, member: str, *, cooldown_seconds: int = 0) -> bool:
        next_time = int(time.time()) + max(0, int(cooldown_seconds))
        try:
            self.r.eval(self._LUA_RELEASE, 2, self.alive_key, self.lease_key, member, next_time)
            return True
        except redis.RedisError:
            return False

    def ban(self, member: str) -> bool:
        try:
            self.r.eval(self._LUA_BAN, 2, self.alive_key, self.lease_key, member)
            return True
        except redis.RedisError:
            return False

    def inc_fail(self, member: str) -> int:
        try:
            return int(self.r.hincrby(self.fail_hash, member, 1))
        except Exception:
            return 1

    def reset_fail(self, member: str) -> None:
        try:
            self.r.hdel(self.fail_hash, member)
        except Exception:
            pass

    def release_on_result(
        self,
        member: str,
        *,
        session_ok: bool,
        cooldown_success: int = 0,
        cooldown_fail_base: int = 30,
        cooldown_fail_jitter: int = 60,
        max_fail: int = 5,
    ) -> Dict[str, Any]:
        if session_ok:
            self.reset_fail(member)
            self.release(member, cooldown_seconds=int(cooldown_success))
            return {"action": "released", "fails": 0, "cooldown": int(cooldown_success)}

        fails = self.inc_fail(member)
        if fails >= int(max_fail):
            self.ban(member)
            return {"action": "banned", "fails": int(fails), "cooldown": 0}

        cooldown = int(cooldown_fail_base) + random.randint(0, max(0, int(cooldown_fail_jitter)))
        self.release(member, cooldown_seconds=cooldown)
        return {"action": "released", "fails": int(fails), "cooldown": int(cooldown)}
