"""Destruction-safe explicit cancellation for durable PvP battles."""
from __future__ import annotations

from pymongo.errors import PyMongoError

from legacy_battle_protocols import BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE


class LegacyBattleCancelUnavailable(RuntimeError):
    """Battle cancellation storage is unavailable."""


def _collection():
    import database

    collection = getattr(database, "battles_collection", None)
    if collection is None:
        raise LegacyBattleCancelUnavailable("battle collection is unavailable")
    return collection


def cancel_unstarted_battle(battle_id: str, user_id: int) -> bool:
    """Delete only an owned durable battle before either participant starts.

    Once ``live_progress`` exists, the shared document is recovery evidence for
    one or both participants and cancellation must not destroy it. Product-level
    forfeit semantics can be added separately; deletion is deliberately not used
    as an implicit forfeit.
    """
    if not isinstance(battle_id, str) or not battle_id.strip():
        raise ValueError("battle_id is required")
    if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id must be a positive integer")
    try:
        result = _collection().delete_one(
            {
                "_id": battle_id.strip(),
                "$or": [{"creator_id": user_id}, {"opponent_id": user_id}],
                "question_progress_protocol": BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE,
                "status": {"$in": ["waiting", "in_progress"]},
                "creator_finished": {"$ne": True},
                "opponent_finished": {"$ne": True},
                "final_claimed": {"$ne": True},
                "live_progress": {"$exists": False},
            }
        )
    except PyMongoError as exc:
        raise LegacyBattleCancelUnavailable("battle cancellation failed") from exc
    deleted = getattr(result, "deleted_count", None)
    if isinstance(deleted, bool) or not isinstance(deleted, int):
        raise LegacyBattleCancelUnavailable("battle cancellation result is invalid")
    return deleted == 1
