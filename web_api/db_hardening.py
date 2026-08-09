"""Database-level invariants for Mini App sessions.

Application checks remain useful for UX, but concurrency invariants belong in
MongoDB as well. Index creation is lazy and retried if MongoDB is temporarily
unavailable during startup.
"""
from __future__ import annotations

import logging
from threading import Lock

logger = logging.getLogger(__name__)
_INDEX_LOCK = Lock()
_INDEXES_READY = False


def ensure_miniapp_indexes() -> bool:
    global _INDEXES_READY
    if _INDEXES_READY:
        return True

    with _INDEX_LOCK:
        if _INDEXES_READY:
            return True
        try:
            import database

            db = getattr(database, "db", None)
            if db is None:
                return False
            sessions = db["miniapp_sessions"]
            sessions.create_index(
                [("updated_at_dt", 1)],
                expireAfterSeconds=6 * 60 * 60,
                name="ttl_miniapp_updated_at",
            )
            sessions.create_index(
                [("user_id", 1), ("status", 1)],
                name="idx_miniapp_user_status",
            )
            sessions.create_index(
                [("user_id", 1), ("status", 1), ("finished_at_dt", -1)],
                name="idx_miniapp_history",
            )
            sessions.create_index(
                [("user_id", 1)],
                unique=True,
                partialFilterExpression={"status": "in_progress"},
                name="uniq_miniapp_active_user",
            )
            _INDEXES_READY = True
            return True
        except Exception as exc:
            # Do not make the whole bot unavailable because an index migration
            # cannot run. The application-level session guard still applies,
            # and the next quiz start will retry index creation.
            logger.warning("Mini App index hardening pending: %s", exc)
            return False
