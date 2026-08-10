"""Atomic MongoDB primitives for legacy PvP battle consistency.

This module isolates race-sensitive battle mutations from the large legacy
``bot.py`` handler graph. It deliberately builds on the existing database
module/collections instead of changing unrelated quiz persistence.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

import database

logger = logging.getLogger(__name__)

_BATTLE_REWARD_RECEIPT_LIMIT = 100
_VALID_RESULTS = frozenset({"win", "lose", "draw"})
_VALID_ROLES = frozenset({"creator", "opponent"})


@dataclass(frozen=True)
class BattleRewardResult:
    applied: bool
    missing_user: bool = False


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
    """Record a participant finish once and return the post-update battle.

    The participant identity, role and unfinished flag are all part of the
    Mongo predicate. Across two concurrent finishers, only the second document
    update can return a battle with both ``*_finished`` flags set, which gives
    the caller a natural single-finalizer handoff without a read/write race.
    """
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
    """Atomically apply one user's battle stats/reward at most once.

    The bounded receipt list is mutated in the same MongoDB user document as
    the counters, so the receipt and ``$inc`` are indivisible. Concurrent
    finalizers can safely retry the same battle without double-awarding it.
    """
    collection = database.collection
    if collection is None:
        return BattleRewardResult(applied=False)

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
        return BattleRewardResult(applied=False)

    if outcome.modified_count == 1:
        return BattleRewardResult(applied=True)

    try:
        exists = collection.count_documents({"_id": uid}, limit=1) > 0
    except PyMongoError:
        exists = True
    return BattleRewardResult(applied=False, missing_user=not exists)
