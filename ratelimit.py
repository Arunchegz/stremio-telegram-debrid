"""
Optional per-IP rate limiting for the Telegram Stremio addon.

Design notes
------------
- Disabled by default. The instance owner enables it (env or /configure wizard)
  when sharing the addon publicly.
- Two token buckets per IP:
    * "api"    -> JSON endpoints (catalog / meta / stream resolve / subtitles /
                  configure). Bursty because Stremio fires metadata lookups in
                  parallel.
    * "stream" -> media range endpoints (/stream/file, /stream/split,
                  /stream/zip, /stream/subtitle). Media players issue a lot of
                  small range requests, so the budget is much higher.
  A token bucket (instead of the old hard sliding window) lets a player burst a
  few requests after a seek without permanently starving the rest of the window.
- A separate per-IP concurrency cap stops one viewer from opening dozens of
  simultaneous streams and saturating the Telegram connection pool.
- State is in memory, i.e. per worker process. Multi-worker deployments should
  stick to one worker (the Telegram client is stateful anyway).
"""

import time
import logging
from collections import defaultdict

from config import Config

logger = logging.getLogger("ratelimit")

# (ip, bucket) -> [tokens, last_refill_monotonic]
_buckets: dict = {}
# ip -> number of in-flight stream responses
_stream_slots: dict = defaultdict(int)
_last_cleanup = 0.0


def _client_ip(request) -> str:
    """Resolve the caller IP, honouring proxy headers only when trusted."""
    if Config.TRUST_PROXY_HEADERS:
        xff = request.headers.get("x-forwarded-for")
        if xff:
            return xff.split(",")[0].strip()
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip.strip()
    if request.client:
        return request.client.host or "unknown"
    return "unknown"


def _limits(bucket: str) -> tuple:
    """Return (requests_per_window, burst, concurrent_streams)."""
    if bucket == "stream":
        return (
            Config.RATE_LIMIT_STREAM_REQUESTS,
            Config.RATE_LIMIT_STREAM_BURST,
            Config.RATE_LIMIT_STREAM_CONCURRENCY,
        )
    return (Config.RATE_LIMIT_REQUESTS, Config.RATE_LIMIT_BURST, 0)


def _cleanup(now: float):
    global _last_cleanup
    window = max(1, Config.RATE_LIMIT_WINDOW)
    if now - _last_cleanup < 60:
        return
    _last_cleanup = now
    stale_after = max(window * 10, 300)
    for key in [k for k, v in _buckets.items() if now - v[1] > stale_after]:
        _buckets.pop(key, None)
    for ip in [ip for ip, count in _stream_slots.items() if count <= 0]:
        _stream_slots.pop(ip, None)


def _consume(ip: str, bucket: str, now: float) -> tuple:
    """Take one token. Returns (allowed, retry_after_seconds, remaining)."""
    limit, burst, _ = _limits(bucket)
    if limit <= 0:
        return True, 0, -1

    window = max(1, Config.RATE_LIMIT_WINDOW)
    rate = limit / window                      # tokens per second
    capacity = max(1, burst)                   # burst == 0 -> strict one-at-a-time

    key = (ip, bucket)
    entry = _buckets.get(key)
    if entry is None:
        # Start full so a fresh client can immediately open a stream.
        entry = [float(capacity), now]
        _buckets[key] = entry

    tokens, updated = entry
    tokens = min(capacity, tokens + max(0.0, now - updated) * rate)
    if tokens < 1.0:
        entry[0] = tokens
        entry[1] = now
        retry_after = max(1, int((1.0 - tokens) / rate) + 1)
        return False, retry_after, 0

    tokens -= 1.0
    entry[0] = tokens
    entry[1] = now
    return True, 0, int(tokens)


def _acquire_stream_slot(ip: str) -> bool:
    _, _, max_concurrent = _limits("stream")
    if max_concurrent <= 0:
        return True
    return _stream_slots[ip] < max_concurrent


def _make_release(ip: str):
    released = False

    def release():
        nonlocal released
        if released:
            return
        released = True
        count = _stream_slots.get(ip, 0) - 1
        if count > 0:
            _stream_slots[ip] = count
        else:
            _stream_slots.pop(ip, None)

    return release


async def check_rate_limit(request, bucket: str = "api", track_concurrency: bool = False) -> tuple:
    """Check the request against the bucket and optionally reserve a stream slot.

    Returns ``(allowed, retry_after, headers, release)``. ``release`` is a
    callable that must be invoked once the streaming response has finished
    (or immediately when it was rejected). It is ``None`` when no slot was
    reserved or when rate limiting is disabled.
    """
    if not Config.RATE_LIMIT_ENABLED:
        return True, 0, {}, None

    ip = _client_ip(request)
    now = time.monotonic()

    allowed, retry_after, remaining = _consume(ip, bucket, now)

    release = None
    if allowed and track_concurrency:
        if not _acquire_stream_slot(ip):
            allowed = False
            retry_after = max(retry_after, 2)
            limit, _, max_concurrent = _limits("stream")
            logger.warning(
                f"Stream concurrency limit ({max_concurrent}) hit for {ip}"
            )
        else:
            _stream_slots[ip] += 1
            release = _make_release(ip)

    limit, _, _ = _limits(bucket)
    headers = {
        "X-RateLimit-Limit": str(max(0, limit)),
        "X-RateLimit-Remaining": str(max(0, remaining)),
    }
    if not allowed:
        headers["Retry-After"] = str(retry_after)
        logger.warning(
            f"Rate limit exceeded for {ip} on bucket '{bucket}' (retry after {retry_after}s)"
        )

    _cleanup(now)
    return allowed, retry_after, headers, release


def enabled() -> bool:
    return bool(Config.RATE_LIMIT_ENABLED)
