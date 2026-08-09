"""Database-level invariants for Mini App sessions.

Application checks remain useful for UX, but concurrency invariants belong in
MongoDB as well. Index creation is lazy and retried if MongoDB is temporarily
unavailable during startup.
"""
from __future__ import annotations

import logging
from datetime import datetime
from threading import Lock

logger = logging.getLogger(__name__)
_INDEX_LOCK = Lock()
_INDEXES_READY = False


def _repair_duplicate_active_sessions(sessions) -> int:
    """Abandon stale legacy duplicates before installing the unique index.

    Older builds could leave more than one ``in_progress`` document for a user.
    Keep the most recently updated document and abandon only the older documents
    that are *still* in progress when the repair update executes. That status
    predicate prevents this migration from overwriting a session that has
    concurrently advanced to ``finalizing`` or another terminal state.
    """
    now = datetime.utcnow()
    repaired = 0
    duplicate_groups = sessions.aggregate(
        [
            {"$match": {"status": "in_progress"}},
            {"$sort": {"updated_at_dt": -1, "_id": -1}},
            {
                "$group": {
                    "_id": "$user_id",
                    "keep": {"$first": "$_id"},
                    "ids": {"$push": "$_id"},
                    "count": {"$sum": 1},
                }
            },
            {"$match": {"count": {"$gt": 1}}},
        ]
    )

    for group in duplicate_groups:
        keep_id = group.get("keep")
        stale_ids = [session_id for session_id in group.get("ids", []) if session_id != keep_id]
        if not stale_ids:
            continue
        result = sessions.update_many(
            {"_id": {"$in": stale_ids}, "status": "in_progress"},
            {
                "$set": {
                    "status": "abandoned",
                    "abandon_reason": "duplicate_active_repair",
                    "updated_at_dt": now,
                }
            },
        )
        repaired += int(getattr(result, "modified_count", 0))

    if repaired:
        logger.warning("Repaired %d duplicate active Mini App session(s)", repaired)
    return repaired


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
            _repair_duplicate_active_sessions(sessions)
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
