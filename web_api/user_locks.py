"""Bounded same-user operation locks for the single-process Mini App runtime.

Waitress serves requests concurrently in threads. Quiz active/start/current/answer/cancel
operations for one Telegram user are serialized so recovery, cancellation, and answer
commits cannot race the same session lifecycle. Stripes keep memory bounded.

This is process-local by design. Horizontal scaling requires a shared lock or a
fully transactional database lifecycle across instances.
"""
from __future__ import annotations

from threading import Lock

_LOCK_STRIPES = 128
_USER_OPERATION_LOCKS = tuple(Lock() for _ in range(_LOCK_STRIPES))


def user_operation_lock(user_id: int | str) -> Lock:
    """Return the stable lock stripe for a Telegram user id."""
    try:
        numeric_id = int(user_id)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid Telegram user id") from exc
    return _USER_OPERATION_LOCKS[numeric_id % _LOCK_STRIPES]
