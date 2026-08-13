"""Read-only discovery of a user's open durable PvP battles for UI recovery."""
from __future__ import annotations

from pymongo import DESCENDING
from pymongo.errors import PyMongoError

from legacy_battle_protocols import BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE


class LegacyBattleRecoveryUnavailable(RuntimeError):
    """Open battle recovery lookup cannot reach MongoDB."""


def _collection():
    import database

    collection = getattr(database, "battles_collection", None)
    if collection is None:
        raise LegacyBattleRecoveryUnavailable("battle collection is unavailable")
    return collection


def get_open_durable_battles_for_user(user_id: int, *, limit: int = 10) -> list[dict]:
    if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
        raise ValueError("user_id must be a positive integer")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0 or limit > 50:
        raise ValueError("limit must be an integer between 1 and 50")
    try:
        return list(
            _collection().find(
                {
                    "$or": [{"creator_id": user_id}, {"opponent_id": user_id}],
                    "question_progress_protocol": BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE,
                    "status": {"$in": ["waiting", "in_progress"]},
                    "final_claimed": {"$ne": True},
                }
            ).sort("updated_at", DESCENDING).limit(limit)
        )
    except PyMongoError as exc:
        raise LegacyBattleRecoveryUnavailable("open battle recovery lookup failed") from exc
