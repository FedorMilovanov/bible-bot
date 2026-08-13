"""Recovery-safe cleanup policy for abandoned legacy PvP battles."""
from __future__ import annotations

from datetime import timedelta

from pymongo.errors import PyMongoError


class LegacyBattleCleanupUnavailable(RuntimeError):
    """Raised when stale-battle cleanup cannot reach MongoDB."""


def _database():
    import database

    return database


def _battle_collection():
    collection = getattr(_database(), "battles_collection", None)
    if collection is None:
        raise LegacyBattleCleanupUnavailable("battle collection is unavailable")
    return collection


def cleanup_stale_waiting_battles(*, max_age_minutes: int = 10) -> int:
    """Delete only abandoned pre-progress battles, never recovery evidence.

    Waiting battles expire from creation. Joined ``in_progress`` battles receive
    a fresh grace window from the atomic opponent claim. Older documents created
    before ``joined_at_dt`` existed fall back to creation time so they cannot
    become immortal. Any durable progress/finalization evidence makes a document
    intentionally ineligible for cleanup.
    """
    if (
        isinstance(max_age_minutes, bool)
        or not isinstance(max_age_minutes, int)
        or max_age_minutes <= 0
    ):
        raise ValueError("max_age_minutes must be a positive integer")

    database = _database()
    collection = _battle_collection()
    cutoff = database._now_utc() - timedelta(minutes=max_age_minutes)
    query = {
        "$or": [
            {
                "status": "waiting",
                "created_at_dt": {"$lt": cutoff},
            },
            {
                "status": "in_progress",
                "joined_at_dt": {"$lt": cutoff},
            },
            {
                "status": "in_progress",
                "joined_at_dt": None,
                "created_at_dt": {"$lt": cutoff},
            },
        ],
        "creator_finished": {"$ne": True},
        "opponent_finished": {"$ne": True},
        "final_claimed": {"$ne": True},
        "live_progress": {"$exists": False},
    }
    try:
        result = collection.delete_many(query)
    except PyMongoError as exc:
        raise LegacyBattleCleanupUnavailable("battle cleanup failed") from exc

    deleted = getattr(result, "deleted_count", None)
    if isinstance(deleted, bool) or not isinstance(deleted, int) or deleted < 0:
        raise LegacyBattleCleanupUnavailable("battle cleanup result is invalid")
    return deleted
