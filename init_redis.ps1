redis-cli --raw ZRANGE proxies:lease 0 -1 | while read p; do
  redis-cli ZADD proxies:alive 0 "$p" >/dev/null
done
redis-cli DEL proxies:lease
