# redis_ping_test.py
# pip install redis
import redis

HOST = "127.0.0.1"
PORT = 6379
DB = 0

r = redis.Redis(host=HOST, port=PORT, db=DB, socket_connect_timeout=3)

try:
    print("PING ->", r.ping())  # True면 연결 OK
    r.set("ping_test", "ok", ex=10)
    print("GET  ->", r.get("ping_test").decode())
    print("OK")
except Exception as e:
    print("FAIL:", e)
