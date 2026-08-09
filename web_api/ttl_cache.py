"""Tiny thread-safe TTL cache for non-critical observability reads."""
from __future__ import annotations

import time
from threading import Lock
from typing import Callable, Generic, TypeVar

T = TypeVar("T")
_MISSING = object()


class TTLValueCache(Generic[T]):
    """Cache one loader result briefly and coalesce concurrent refreshes."""

    def __init__(self, ttl_seconds: float) -> None:
        self.ttl_seconds = max(0.0, float(ttl_seconds))
        self._value: object = _MISSING
        self._expires_at = 0.0
        self._loader: object = None
        self._lock = Lock()

    def get(self, loader: Callable[[], T]) -> T:
        now = time.monotonic()
        if self._value is not _MISSING and self._loader is loader and now < self._expires_at:
            return self._value  # type: ignore[return-value]

        with self._lock:
            now = time.monotonic()
            if self._value is not _MISSING and self._loader is loader and now < self._expires_at:
                return self._value  # type: ignore[return-value]
            value = loader()
            self._value = value
            self._loader = loader
            self._expires_at = now + self.ttl_seconds
            return value

    def clear(self) -> None:
        with self._lock:
            self._value = _MISSING
            self._expires_at = 0.0
            self._loader = None
