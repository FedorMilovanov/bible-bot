"""Atomic Mongo operations and authorization helpers for legacy PvP battles."""
from __future__ import annotations

import logging

from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)

_BATTLE_RECEIPT_LIMIT = 64


class BattleStoreUnavailable(RuntimeError):
    """Raised when the battle collection cannot complete an operation."""


def _battle_collection():
    import database

    collection = getattr(database, "battles_collection", None)
    if collection is None:
        raise BattleStoreUnavailable("battle collection is unavailable")
    return collection


def _user_collection():
    import database

    collection = getattr(database, "collection", None)
    if collection is None:
        raise BattleStoreUnavailable("user stats collection is unavailable")
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

        # Retry after a successful write but lost response: return the stored
        # snapshot without overwriting the participant's first result.
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


def _result_for_role(battle: dict, role: str) -> str:
    creator_points = int(battle.get("creator_points", 0))
    opponent_points = int(battle.get("opponent_points", 0))
    if creator_points == opponent_points:
        return "draw"
    winner = "creator" if creator_points > opponent_points else "opponent"
    return "win" if role == winner else "lose"


def _apply_battle_outcome_once(
    battle_id: str,
    user_id: int,
    result: str,
    *,
    first_name: str,
) -> None:
    """Increment one user's PvP stats once, keyed by battle_id receipt."""
    import database

    collection = _user_collection()
    uid = database._uid(user_id)
    inc = {"battles_played": 1}
    if result == "win":
        inc.update({"battles_won": 1, "total_points": 5})
    elif result == "lose":
        inc["battles_lost"] = 1
    elif result == "draw":
        inc.update({"battles_draw": 1, "total_points": 2})
    else:
        raise ValueError(f"unsupported battle outcome: {result}")

    def apply_once():
        return collection.update_one(
            {"_id": uid, "battle_result_receipts": {"$ne": battle_id}},
            {
                "$inc": inc,
                "$set": {"last_activity": database._now_utc()},
                "$push": {
                    "battle_result_receipts": {
                        "$each": [battle_id],
                        "$slice": -_BATTLE_RECEIPT_LIMIT,
                    }
                },
            },
        )

    try:
        write = apply_once()
        if write.modified_count == 1:
            return

        existing = collection.find_one(
            {"_id": uid},
            {"battle_result_receipts": 1},
        )
        if existing is None:
            # Normally /start already initialized every participant, but keep
            # finalization recoverable for old/incomplete user records.
            database.init_user_stats(user_id, "", first_name or "Игрок")
            write = apply_once()
            if write.modified_count == 1:
                return
            existing = collection.find_one(
                {"_id": uid},
                {"battle_result_receipts": 1},
            )

        if existing and battle_id in existing.get("battle_result_receipts", []):
            return
        raise BattleStoreUnavailable("battle outcome receipt could not be persisted")
    except PyMongoError as exc:
        logger.exception("failed to apply battle outcome for user %s", user_id)
        raise BattleStoreUnavailable("battle outcome write failed") from exc


def claim_final_battle(battle_id: str) -> dict | None:
    """Apply both outcomes idempotently, then atomically claim the result message snapshot."""
    collection = _battle_collection()
    try:
        battle = collection.find_one(
            {
                "_id": battle_id,
                "creator_finished": True,
                "opponent_finished": True,
            }
        )
        if not battle:
            return None

        _apply_battle_outcome_once(
            battle_id,
            int(battle["creator_id"]),
            _result_for_role(battle, "creator"),
            first_name=battle.get("creator_name", "Игрок"),
        )
        _apply_battle_outcome_once(
            battle_id,
            int(battle["opponent_id"]),
            _result_for_role(battle, "opponent"),
            first_name=battle.get("opponent_name", "Игрок"),
        )

        # Only one concurrent finisher receives the deleted snapshot and thus
        # only one sends the shared result message. User rewards are already
        # protected by per-user battle receipts above.
        return collection.find_one_and_delete(
            {
                "_id": battle_id,
                "creator_finished": True,
                "opponent_finished": True,
            }
        )
    except BattleStoreUnavailable:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        logger.exception("battle %s has invalid participant/result data", battle_id)
        raise BattleStoreUnavailable("battle result data is invalid") from exc
    except PyMongoError as exc:
        logger.exception("failed to claim final battle %s", battle_id)
        raise BattleStoreUnavailable("battle finalization failed") from exc


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
