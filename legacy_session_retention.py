"""State-aware retention policy for durable legacy quiz sessions.

Historically every quiz session had a six-hour TTL on ``updated_at_dt``. That
made a fully answered but not-yet-scored ``in_progress`` session eligible for
Mongo TTL deletion, destroying the only crash-recovery evidence. This module
migrates the index to a partial terminal-state TTL: active/pending sessions are
never TTL candidates, while finished/cancelled history remains bounded.
"""
from __future__ import annotations

import logging

from pymongo import ASCENDING
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)

_LEGACY_TTL_NAME = "ttl_updated_at"
_TERMINAL_TTL_NAME = "ttl_terminal_updated_at"
_TERMINAL_RETENTION_SECONDS = 90 * 24 * 60 * 60
_TERMINAL_FILTER = {"status": {"$in": ["finished", "cancelled"]}}


class QuizSessionRetentionUnavailable(RuntimeError):
    """Raised when the safety-critical quiz-session TTL migration cannot run."""


def ensure_state_aware_session_ttl() -> bool:
    """Install a terminal-only TTL and remove the unsafe generic six-hour TTL.

    The function is idempotent. It deliberately fails closed when Mongo is
    reachable but index migration fails: callers should not pretend the pending
    result evidence is protected while the unsafe generic TTL may still exist.
    """
    import database

    collection = getattr(database, "quiz_sessions_collection", None)
    if collection is None:
        return False

    try:
        info = collection.index_information()
        legacy = info.get(_LEGACY_TTL_NAME)
        terminal = info.get(_TERMINAL_TTL_NAME)

        if legacy is not None:
            collection.drop_index(_LEGACY_TTL_NAME)

        expected_terminal = (
            terminal is not None
            and terminal.get("key") == [("updated_at_dt", ASCENDING)]
            and terminal.get("expireAfterSeconds") == _TERMINAL_RETENTION_SECONDS
            and terminal.get("partialFilterExpression") == _TERMINAL_FILTER
        )
        if terminal is not None and not expected_terminal:
            collection.drop_index(_TERMINAL_TTL_NAME)
            terminal = None

        if terminal is None or not expected_terminal:
            collection.create_index(
                [("updated_at_dt", ASCENDING)],
                expireAfterSeconds=_TERMINAL_RETENTION_SECONDS,
                partialFilterExpression=_TERMINAL_FILTER,
                name=_TERMINAL_TTL_NAME,
                background=True,
            )
        return True
    except PyMongoError as exc:
        logger.exception("failed to install state-aware quiz-session retention")
        raise QuizSessionRetentionUnavailable(
            "quiz-session retention migration failed"
        ) from exc
