"""Small process-local rate limiter for the single-instance Mini App service.

The current Render architecture runs one web process, so a local sliding-window
limiter is sufficient. If the service is scaled to multiple instances, replace
this with a shared limiter (for example Redis) so all instances share counters.
"""
from __future__ import annotations

import math
import time
from collections import defaultdict, deque
from threading import Lock


class SlidingWindowLimiter:
    """Thread-safe fixed policy / sliding-window limiter."""

    def __init__(self) -> None:
        self._hits: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()
        self._last_cleanup = 0.0

    def allow(self, key: str, *, limit: int, window_seconds: int) -> tuple[bool, int]:
        now = time.monotonic()
        cutoff = now - window_seconds

        with self._lock:
            bucket = self._hits[key]
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()

            if len(bucket) >= limit:
                retry_after = max(1, math.ceil(window_seconds - (now - bucket[0])))
                return False, retry_after

            bucket.append(now)

            # Bound memory even if many one-off user IDs hit the service.
            if now - self._last_cleanup >= 300:
                self._cleanup(now)
                self._last_cleanup = now

        return True, 0

    def _cleanup(self, now: float) -> None:
        stale_before = now - 600
        stale = [key for key, bucket in self._hits.items() if not bucket or bucket[-1] < stale_before]
        for key in stale:
            self._hits.pop(key, None)


GLOBAL_API_LIMITER = SlidingWindowLimiter()
