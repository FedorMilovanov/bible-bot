"""Atomic MongoDB primitives for legacy PvP battle consistency.

This module isolates race-sensitive battle mutations from the large legacy
``bot.py`` handler graph. It deliberately builds on the existing database
module/collections instead of changing unrelated quiz persistence.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta

from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

import database

logger = logging.getLogger(__name__)

_BATTLE_REWARD_RECEIPT_LIMIT = 100
_FINALIZATION_LEASE_SECONDS = 30
_VALID_RESULTS = frozenset({"win", "lose", "draw"})
_VALID_ROLES = frozenset({"creator", "opponent"})


@dataclass(frozen=True)
class BattleRewardResult:
    applied: bool
    already_applied: bool = False
    missing_user: bool = False
    retryable_error: bool = False


def battle_role_for_user(battle: dict | None, user_id: int) -> str | None:
    """Return the server-authoritative PvP role for a participant."""
    if not battle:
        return None
    if battle.get("creator_id") == user_id:
        return "creator"
    if battle.get("opponent_id") == user_id:
        return "opponent"
    return None


def join_battle_atomic(battle_id: str, user_id: int, user_name: str) -> dict | None:
    """Claim the only opponent slot and return the updated battle."""
    collection = database.battles_collection
    if collection is None:
        return None

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
                    "updated_at": database._now_utc().isoformat(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
    except PyMongoError:
        logger.exception("join_battle_atomic failed for %s", battle_id)
        return None


def record_battle_finish_atomic(
    battle_id: str,
    user_id: int,
    role: str,
    *,
    score: int,
    time_taken: float,
    points: int,
) -> dict | None:
    """Record a participant finish once and return the post-update battle."""
    if role not in _VALID_ROLES:
        return None
    collection = database.battles_collection
    if collection is None:
        return None

    prefix = role
    try:
        return collection.find_one_and_update(
            {
                "_id": battle_id,
                f"{prefix}_id": user_id,
                f"{prefix}_finished": {"$ne": True},
            },
            {
                "$set": {
                    f"{prefix}_score": max(0, int(score)),
                    f"{prefix}_time": max(0.0, float(time_taken)),
                    f"{prefix}_points": max(0, int(points)),
                    f"{prefix}_finished": True,
                    "updated_at": database._now_utc().isoformat(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
    except (PyMongoError, TypeError, ValueError, OverflowError):
        logger.exception("record_battle_finish_atomic failed for battle=%s user=%s", battle_id, user_id)
        return None


def _finalization_filter(now):
    cutoff = now - timedelta(seconds=_FINALIZATION_LEASE_SECONDS)
    return {
        "creator_finished": True,
        "opponent_finished": True,
        "$or": [
            {"result_state": {"$exists": False}},
            {"result_state": "pending"},
            {
                "result_state": "finalizing",
                "result_claimed_at_dt": {"$lt": cutoff},
            },
            {
                "result_state": "finalizing",
                "result_claimed_at_dt": {"$exists": False},
            },
        ],
    }


def claim_battle_finalization(battle_id: str) -> dict | None:
    """Acquire or recover a bounded finalization lease for a finished battle."""
    collection = database.battles_collection
    if collection is None:
        return None
    now = database._now_utc()
    predicate = {"_id": battle_id, **_finalization_filter(now)}
    try:
        return collection.find_one_and_update(
            predicate,
            {
                "$set": {
                    "result_state": "finalizing",
                    "result_claimed_at_dt": now,
                    "updated_at": now.isoformat(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
    except PyMongoError:
        logger.exception("claim_battle_finalization failed for %s", battle_id)
        return None


def get_battles_needing_finalization(limit: int = 20) -> list[str]:
    """Return finished battle IDs with no active finalization lease."""
    collection = database.battles_collection
    if collection is None:
        return []
    predicate = _finalization_filter(database._now_utc())
    try:
        cursor = collection.find(predicate, {"_id": 1}).limit(max(1, int(limit)))
        return [doc["_id"] for doc in cursor if doc.get("_id")]
    except (PyMongoError, TypeError, ValueError):
        logger.exception("get_battles_needing_finalization failed")
        return []


def mark_battle_result_delivered(battle_id: str, user_id: int, role: str) -> bool:
    """Persist one participant's result-delivery receipt idempotently."""
    if role not in _VALID_ROLES:
        return False
    collection = database.battles_collection
    if collection is None:
        return False

    now = database._now_utc()
    delivered_key = f"{role}_result_delivered"
    try:
        outcome = collection.update_one(
            {
                "_id": battle_id,
                f"{role}_id": user_id,
                delivered_key: {"$ne": True},
            },
            {
                "$set": {
                    delivered_key: True,
                    f"{role}_result_delivered_at": now,
                    "updated_at": now.isoformat(),
                }
            },
        )
        if outcome.modified_count == 1:
            return True
        return collection.count_documents(
            {
                "_id": battle_id,
                f"{role}_id": user_id,
                delivered_key: True,
            },
            limit=1,
        ) > 0
    except PyMongoError:
        logger.exception("mark_battle_result_delivered failed for battle=%s user=%s", battle_id, user_id)
        return False


def delete_battle_if_fully_delivered(battle_id: str) -> bool:
    """Delete only a battle whose two result deliveries are durably marked."""
    collection = database.battles_collection
    if collection is None:
        return False
    try:
        result = collection.delete_one(
            {
                "_id": battle_id,
                "creator_result_delivered": True,
                "opponent_result_delivered": True,
            }
        )
        return result.deleted_count == 1
    except PyMongoError:
        logger.exception("delete_battle_if_fully_delivered failed for %s", battle_id)
        return False


def cancel_battle_for_participant(battle_id: str, user_id: int) -> bool:
    """Delete a battle only when the requester is one of its participants."""
    collection = database.battles_collection
    if collection is None:
        return False
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
    except PyMongoError:
        logger.exception("cancel_battle_for_participant failed for %s", battle_id)
        return False


def _battle_result_increment(result: str) -> dict[str, int]:
    if result not in _VALID_RESULTS:
        raise ValueError(f"unsupported battle result: {result}")

    inc = {"battles_played": 1}
    if result == "win":
        inc.update({"battles_won": 1, "total_points": 5})
    elif result == "lose":
        inc["battles_lost"] = 1
    else:
        inc.update({"battles_draw": 1, "total_points": 2})
    return inc


def apply_battle_reward_once(user_id: int, battle_id: str, result: str) -> BattleRewardResult:
    """Atomically apply one user's battle stats/reward at most once."""
    collection = database.collection
    if collection is None:
        return BattleRewardResult(applied=False, retryable_error=True)

    uid = database._uid(user_id)
    receipt = str(battle_id)
    inc = _battle_result_increment(result)

    try:
        outcome = collection.update_one(
            {
                "_id": uid,
                "battle_reward_receipts": {"$ne": receipt},
            },
            {
                "$inc": inc,
                "$set": {"last_activity": database._now_utc()},
                "$push": {
                    "battle_reward_receipts": {
                        "$each": [receipt],
                        "$slice": -_BATTLE_REWARD_RECEIPT_LIMIT,
                    }
                },
            },
        )
    except PyMongoError:
        logger.exception("apply_battle_reward_once failed for battle=%s user=%s", battle_id, uid)
        return BattleRewardResult(applied=False, retryable_error=True)

    if outcome.modified_count == 1:
        return BattleRewardResult(applied=True)

    try:
        receipt_exists = collection.count_documents(
            {"_id": uid, "battle_reward_receipts": receipt}, limit=1
        ) > 0
        if receipt_exists:
            return BattleRewardResult(applied=False, already_applied=True)
        user_exists = collection.count_documents({"_id": uid}, limit=1) > 0
    except PyMongoError:
        logger.exception("could not verify battle reward receipt for battle=%s user=%s", battle_id, uid)
        return BattleRewardResult(applied=False, retryable_error=True)

    if not user_exists:
        return BattleRewardResult(applied=False, missing_user=True)
    return BattleRewardResult(applied=False, retryable_error=True)
