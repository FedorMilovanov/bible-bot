"""Atomic Mongo operations and authorization helpers for legacy PvP battles."""
from __future__ import annotations

import logging

from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)


class BattleStoreUnavailable(RuntimeError):
    """Raised when the battle collection cannot complete an operation."""


def battle_role_for_user(battle: dict | None, user_id: int) -> str | None:
    """Return the persisted participant role for a user, never trusting callback data."""
    if not battle:
        return None
    if battle.get("creator_id") == user_id:
        return "creator"
    if battle.get("opponent_id") == user_id:
        return "opponent"
    return None


def claim_battle_opponent(battle_id: str, user_id: int, user_name: str) -> dict | None:
    """Atomically claim the only opponent slot of a waiting battle."""
    import database

    collection = getattr(database, "battles_collection", None)
    if collection is None:
        raise BattleStoreUnavailable("battle collection is unavailable")

    try:
        return collection.find_one_and_update(
            {
                "_id": battle_id,
                "status": "waiting",
                "opponent_id": None,
                "creator_id": {"$ne": user_id},
            },
            {
                "$set": {
                    "opponent_id": user_id,
                    "opponent_name": user_name,
                    "status": "in_progress",
                }
            },
            return_document=ReturnDocument.AFTER,
        )
    except PyMongoError as exc:
        logger.exception("failed to atomically claim opponent slot for battle %s", battle_id)
        raise BattleStoreUnavailable("battle claim failed") from exc


def delete_battle_for_participant(battle_id: str, user_id: int) -> bool:
    """Delete a battle only when the requesting user is one of its persisted participants."""
    import database

    collection = getattr(database, "battles_collection", None)
    if collection is None:
        raise BattleStoreUnavailable("battle collection is unavailable")

    try:
        result = collection.delete_one(
            {
                "_id": battle_id,
                "$or": [
                    {"creator_id": user_id},
                    {"opponent_id": user_id},
                ],
            }
        )
        return result.deleted_count == 1
    except PyMongoError as exc:
        logger.exception("failed to delete battle %s for participant %s", battle_id, user_id)
        raise BattleStoreUnavailable("battle delete failed") from exc
