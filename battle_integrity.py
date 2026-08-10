"""Atomic Mongo operations and authorization helpers for legacy PvP battles."""
from __future__ import annotations

import logging

from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)


class BattleStoreUnavailable(RuntimeError):
    """Raised when the battle collection cannot complete an operation."""


def _battle_collection():
    import database

    collection = getattr(database, "battles_collection", None)
    if collection is None:
        raise BattleStoreUnavailable("battle collection is unavailable")
    return collection


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
    collection = _battle_collection()
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


def record_battle_result(
    battle_id: str,
    user_id: int,
    role: str,
    *,
    score: int,
    time_seconds: float,
    points: int,
) -> dict | None:
    """Persist one participant result once; safe retries return the stored snapshot."""
    if role not in {"creator", "opponent"}:
        return None

    collection = _battle_collection()
    participant_field = f"{role}_id"
    finished_field = f"{role}_finished"
    try:
        updated = collection.find_one_and_update(
            {
                "_id": battle_id,
                participant_field: user_id,
                finished_field: {"$ne": True},
                "status": {"$in": ["waiting", "in_progress"]},
            },
            {
                "$set": {
                    f"{role}_score": int(score),
                    f"{role}_time": float(time_seconds),
                    f"{role}_points": int(points),
                    finished_field: True,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if updated is not None:
            return updated

        # A retry can arrive after the first atomic write succeeded but before
        # the caller observed it. Return the already persisted participant
        # result without overwriting score/time/points a second time.
        return collection.find_one(
            {
                "_id": battle_id,
                participant_field: user_id,
                finished_field: True,
            }
        )
    except PyMongoError as exc:
        logger.exception("failed to record %s result for battle %s", role, battle_id)
        raise BattleStoreUnavailable("battle result write failed") from exc


def claim_battle_results(battle_id: str) -> dict | None:
    """Allow exactly one finisher to process the shared battle result."""
    collection = _battle_collection()
    try:
        return collection.find_one_and_update(
            {
                "_id": battle_id,
                "creator_finished": True,
                "opponent_finished": True,
                "results_processed": {"$ne": True},
            },
            {"$set": {"results_processed": True, "status": "finished"}},
            return_document=ReturnDocument.AFTER,
        )
    except PyMongoError as exc:
        logger.exception("failed to claim shared results for battle %s", battle_id)
        raise BattleStoreUnavailable("battle result claim failed") from exc


def delete_battle_for_participant(battle_id: str, user_id: int) -> bool:
    """Delete a battle only when the requesting user is one of its persisted participants."""
    collection = _battle_collection()
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
