"""Database-level invariants for Mini App sessions.

Application checks remain useful for UX, but concurrency and retention
invariants belong in MongoDB as well. Index creation is lazy and retried when
MongoDB is temporarily unavailable. User session documents are never repaired
or discarded automatically by this migration layer.
"""
from __future__ import annotations

import logging
from threading import Lock

logger = logging.getLogger(__name__)
_INDEX_LOCK = Lock()
_INDEXES_READY = False

LEGACY_TTL_NAME = "ttl_miniapp_updated_at"
TERMINAL_TTL_NAME = "ttl_miniapp_terminal_updated_at"
TERMINAL_RETENTION_SECONDS = 90 * 24 * 60 * 60
UNIQUE_ACTIVE_NAME = "uniq_miniapp_active_user"
TERMINAL_FILTER = {"status": {"$in": ["finished", "abandoned"]}}
ACTIVE_FILTER = {"status": "in_progress"}


def _key_spec(index: dict) -> list[tuple]:
    raw = index.get("key")
    if not isinstance(raw, list):
        return []
    return [tuple(item) for item in raw]


def _index_matches(
    index: dict | None,
    *,
    key: list[tuple],
    expire_after: int | None = None,
    partial_filter: dict | None = None,
    unique: bool | None = None,
) -> bool:
    if not isinstance(index, dict) or _key_spec(index) != key:
        return False
    if expire_after is not None and index.get("expireAfterSeconds") != expire_after:
        return False
    if partial_filter is not None and index.get("partialFilterExpression") != partial_filter:
        return False
    if unique is not None and bool(index.get("unique", False)) is not unique:
        return False
    return True


def _ensure_index(
    sessions,
    info: dict,
    *,
    name: str,
    key: list[tuple],
    expire_after: int | None = None,
    partial_filter: dict | None = None,
    unique: bool | None = None,
) -> None:
    existing = info.get(name)
    if existing is not None and not _index_matches(
        existing,
        key=key,
        expire_after=expire_after,
        partial_filter=partial_filter,
        unique=unique,
    ):
        sessions.drop_index(name)
        existing = None
    if existing is not None:
        return

    kwargs = {"name": name}
    if expire_after is not None:
        kwargs["expireAfterSeconds"] = expire_after
    if partial_filter is not None:
        kwargs["partialFilterExpression"] = partial_filter
    if unique is not None:
        kwargs["unique"] = unique
    sessions.create_index(key, **kwargs)


def find_duplicate_active_users(sessions, *, limit: int = 20) -> list[dict]:
    """Read-only duplicate preflight; never chooses or mutates a winner."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")
    return list(
        sessions.aggregate(
            [
                {"$match": {"status": "in_progress"}},
                {
                    "$group": {
                        "_id": "$user_id",
                        "session_ids": {"$push": "$_id"},
                        "count": {"$sum": 1},
                    }
                },
                {"$match": {"count": {"$gt": 1}}},
                {"$sort": {"_id": 1}},
                {"$limit": limit},
            ]
        )
    )


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
            info = sessions.index_information()

            # Historical builds installed one 6-hour TTL over every state. Drop
            # it before any later migration step so active/finalizing/error
            # recovery evidence is never age-deleted by this process again.
            if LEGACY_TTL_NAME in info:
                sessions.drop_index(LEGACY_TTL_NAME)
                info = sessions.index_information()

            _ensure_index(
                sessions,
                info,
                name=TERMINAL_TTL_NAME,
                key=[("updated_at_dt", 1)],
                expire_after=TERMINAL_RETENTION_SECONDS,
                partial_filter=TERMINAL_FILTER,
            )
            info = sessions.index_information()
            _ensure_index(
                sessions,
                info,
                name="idx_miniapp_user_status",
                key=[("user_id", 1), ("status", 1)],
            )
            info = sessions.index_information()
            _ensure_index(
                sessions,
                info,
                name="idx_miniapp_history",
                key=[("user_id", 1), ("status", 1), ("finished_at_dt", -1)],
            )

            duplicates = find_duplicate_active_users(sessions, limit=20)
            if duplicates:
                logger.error(
                    "Mini App unique-index migration blocked by duplicate active users: %s",
                    [group.get("_id") for group in duplicates],
                )
                return False

            info = sessions.index_information()
            _ensure_index(
                sessions,
                info,
                name=UNIQUE_ACTIVE_NAME,
                key=[("user_id", 1)],
                partial_filter=ACTIVE_FILTER,
                unique=True,
            )

            # Read back the final contracts. A create_index call returning
            # without exception is not sufficient proof if an incompatible
            # index raced into existence under the same name.
            final_info = sessions.index_information()
            if LEGACY_TTL_NAME in final_info:
                return False
            if not _index_matches(
                final_info.get(TERMINAL_TTL_NAME),
                key=[("updated_at_dt", 1)],
                expire_after=TERMINAL_RETENTION_SECONDS,
                partial_filter=TERMINAL_FILTER,
            ):
                return False
            if not _index_matches(
                final_info.get(UNIQUE_ACTIVE_NAME),
                key=[("user_id", 1)],
                partial_filter=ACTIVE_FILTER,
                unique=True,
            ):
                return False

            _INDEXES_READY = True
            return True
        except Exception as exc:
            logger.warning("Mini App index hardening pending: %s", exc)
            return False
