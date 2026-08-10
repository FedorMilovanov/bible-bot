"""Atomic Mongo operations and authorization helpers for legacy PvP battles."""
from __future__ import annotations

import hashlib
import logging
import math
import uuid
from datetime import timedelta

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


def _user_collection():
    import database

    collection = getattr(database, "collection", None)
    if collection is None:
        raise BattleStoreUnavailable("user stats collection is unavailable")
    return collection


def _battle_receipt_digest(battle_id: str) -> str:
    value = str(battle_id or "").strip()
    if not value:
        raise ValueError("battle_id is required for idempotent outcome scoring")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _battle_receipt_path(battle_id: str) -> str:
    return f"battle_result_receipt_map.{_battle_receipt_digest(battle_id)}"


def _entry_field(entry: dict, path: str) -> tuple[object | None, bool]:
    current: object = entry
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None, False
        current = current[part]
    return current, True


def _nonnegative_int(value, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a non-negative integer")
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a non-negative integer") from exc
    if number < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return number


def _nonnegative_float(value, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a finite non-negative number")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite non-negative number") from exc
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"{field} must be a finite non-negative number")
    return number


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

    score = _nonnegative_int(score, "battle score")
    time_seconds = _nonnegative_float(time_seconds, "battle time")
    points = _nonnegative_int(points, "battle points")
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
                    f"{role}_score": score,
                    f"{role}_time": time_seconds,
                    f"{role}_points": points,
                    finished_field: True,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if updated is not None:
            return updated

        # Retry after a successful write but lost response: return the stored
        # snapshot without overwriting the participant's first result. This also
        # lets a participant recover its already-stored score after the shared
        # battle document has advanced to the retained ``finalized`` state.
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
    creator_points = _nonnegative_int(battle.get("creator_points", 0), "creator points")
    opponent_points = _nonnegative_int(battle.get("opponent_points", 0), "opponent points")
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
    """Increment one user's PvP stats once, keyed by a durable battle marker.

    New outcomes use a non-evicting SHA-256 field marker. The historical
    ``battle_result_receipts`` array remains read-only migration evidence: once
    this code is deployed it is no longer pushed/sliced, so IDs already present
    there also stop expiring.
    """
    import database

    collection = _user_collection()
    uid = database._uid(user_id)
    receipt_path = _battle_receipt_path(battle_id)
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
            {
                "_id": uid,
                receipt_path: {"$exists": False},
                # Migration guard: do not re-credit an outcome already present
                # in the frozen legacy array.
                "battle_result_receipts": {"$ne": battle_id},
            },
            {
                "$inc": inc,
                "$set": {
                    "last_activity": database._now_utc(),
                    receipt_path: True,
                },
            },
        )

    def confirm_existing(entry: dict | None) -> bool:
        if not entry:
            return False
        marker, marker_exists = _entry_field(entry, receipt_path)
        if marker_exists:
            if marker is True:
                return True
            raise BattleStoreUnavailable("battle outcome receipt marker is invalid")
        legacy = entry.get("battle_result_receipts")
        if legacy is None:
            return False
        if not isinstance(legacy, list):
            raise BattleStoreUnavailable("legacy battle receipt list is invalid")
        return battle_id in legacy

    try:
        write = apply_once()
        if write.modified_count == 1:
            return

        existing = collection.find_one(
            {"_id": uid},
            {receipt_path: 1, "battle_result_receipts": 1},
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
                {receipt_path: 1, "battle_result_receipts": 1},
            )

        if confirm_existing(existing):
            return
        raise BattleStoreUnavailable("battle outcome receipt could not be persisted")
    except BattleStoreUnavailable:
        raise
    except PyMongoError as exc:
        logger.exception("failed to apply battle outcome for user %s", user_id)
        raise BattleStoreUnavailable("battle outcome write failed") from exc


def claim_final_battle(battle_id: str) -> dict | None:
    """Apply both outcomes, then retain and atomically claim the final snapshot.

    The legacy implementation deleted the only shared battle document before the
    two Telegram result messages were acknowledged. A process crash at that point
    permanently lost delivery evidence. Final battles now remain in Mongo with a
    per-recipient outbox state. The current handler still receives the snapshot
    only once, preserving its no-duplicate behavior until it is wired to the
    delivery-lease API below.
    """
    import database

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

        now = database._now_utc()
        return collection.find_one_and_update(
            {
                "_id": battle_id,
                "creator_finished": True,
                "opponent_finished": True,
                "final_claimed": {"$ne": True},
            },
            {
                "$set": {
                    "final_claimed": True,
                    "status": "finalized",
                    "finalized_at": now,
                    "result_delivery": {
                        "creator": {"delivered": False, "attempts": 0},
                        "opponent": {"delivered": False, "attempts": 0},
                    },
                }
            },
            return_document=ReturnDocument.AFTER,
        )
    except BattleStoreUnavailable:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        logger.exception("battle %s has invalid participant/result data", battle_id)
        raise BattleStoreUnavailable("battle result data is invalid") from exc
    except PyMongoError as exc:
        logger.exception("failed to claim final battle %s", battle_id)
        raise BattleStoreUnavailable("battle finalization failed") from exc


def claim_battle_result_delivery(
    battle_id: str,
    user_id: int,
    *,
    lease_seconds: int = 120,
) -> dict | None:
    """Lease one pending recipient delivery from a retained finalized battle.

    Delivery is at-least-once: Telegram has no idempotency key for sendMessage,
    so a process may still die after Telegram accepts a message but before the
    acknowledgement write. The lease prevents concurrent workers from sending
    the same recipient simultaneously and keeps durable retry evidence.
    """
    import database

    if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int) or lease_seconds <= 0:
        raise ValueError("lease_seconds must be a positive integer")

    collection = _battle_collection()
    try:
        battle = collection.find_one({"_id": battle_id, "final_claimed": True})
        role = battle_role_for_user(battle, user_id)
        if role is None:
            return None

        now = database._now_utc()
        lease_until = now + timedelta(seconds=lease_seconds)
        token = uuid.uuid4().hex
        path = f"result_delivery.{role}"
        claimed = collection.find_one_and_update(
            {
                "_id": battle_id,
                "final_claimed": True,
                f"{path}.delivered": {"$ne": True},
                "$or": [
                    {f"{path}.lease_until": {"$exists": False}},
                    {f"{path}.lease_until": {"$lte": now}},
                ],
            },
            {
                "$set": {
                    f"{path}.claim_token": token,
                    f"{path}.lease_until": lease_until,
                    f"{path}.last_attempt_at": now,
                },
                "$inc": {f"{path}.attempts": 1},
            },
            return_document=ReturnDocument.AFTER,
        )
        if claimed is None:
            return None
        return {"battle": claimed, "role": role, "claim_token": token}
    except PyMongoError as exc:
        logger.exception("failed to claim result delivery for battle %s user %s", battle_id, user_id)
        raise BattleStoreUnavailable("battle result delivery claim failed") from exc


def mark_battle_result_delivered(
    battle_id: str,
    user_id: int,
    claim_token: str,
) -> bool:
    """Acknowledge one leased recipient delivery after Telegram send succeeds."""
    import database

    if not isinstance(claim_token, str) or not claim_token:
        raise ValueError("claim_token is required")
    collection = _battle_collection()
    try:
        battle = collection.find_one({"_id": battle_id, "final_claimed": True})
        role = battle_role_for_user(battle, user_id)
        if role is None:
            return False
        path = f"result_delivery.{role}"
        result = collection.update_one(
            {
                "_id": battle_id,
                f"{path}.delivered": {"$ne": True},
                f"{path}.claim_token": claim_token,
            },
            {
                "$set": {
                    f"{path}.delivered": True,
                    f"{path}.delivered_at": database._now_utc(),
                },
                "$unset": {
                    f"{path}.claim_token": "",
                    f"{path}.lease_until": "",
                    f"{path}.last_error": "",
                },
            },
        )
        if result.modified_count == 1:
            return True
        existing = collection.find_one({"_id": battle_id}, {f"{path}.delivered": 1})
        delivered, exists = _entry_field(existing or {}, f"{path}.delivered")
        return exists and delivered is True
    except PyMongoError as exc:
        logger.exception("failed to acknowledge result delivery for battle %s user %s", battle_id, user_id)
        raise BattleStoreUnavailable("battle result delivery acknowledgement failed") from exc


def release_battle_result_delivery(
    battle_id: str,
    user_id: int,
    claim_token: str,
    *,
    error: str = "",
) -> bool:
    """Release a failed delivery lease so a later worker can retry it."""
    if not isinstance(claim_token, str) or not claim_token:
        raise ValueError("claim_token is required")
    collection = _battle_collection()
    try:
        battle = collection.find_one({"_id": battle_id, "final_claimed": True})
        role = battle_role_for_user(battle, user_id)
        if role is None:
            return False
        path = f"result_delivery.{role}"
        result = collection.update_one(
            {
                "_id": battle_id,
                f"{path}.delivered": {"$ne": True},
                f"{path}.claim_token": claim_token,
            },
            {
                "$set": {f"{path}.last_error": str(error or "")[:500]},
                "$unset": {
                    f"{path}.claim_token": "",
                    f"{path}.lease_until": "",
                },
            },
        )
        return result.modified_count == 1
    except PyMongoError as exc:
        logger.exception("failed to release result delivery for battle %s user %s", battle_id, user_id)
        raise BattleStoreUnavailable("battle result delivery release failed") from exc


def get_pending_final_battles(limit: int = 50) -> list[dict]:
    """Return retained finalized battles with at least one unacknowledged recipient."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")
    collection = _battle_collection()
    try:
        return list(
            collection.find(
                {
                    "status": "finalized",
                    "$or": [
                        {"result_delivery.creator.delivered": {"$ne": True}},
                        {"result_delivery.opponent.delivered": {"$ne": True}},
                    ],
                }
            ).limit(limit)
        )
    except PyMongoError as exc:
        logger.exception("failed to list pending finalized battles")
        raise BattleStoreUnavailable("battle result delivery listing failed") from exc


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
                # Retained finalized battles are delivery evidence/outbox state,
                # not user-cancellable game sessions.
                "final_claimed": {"$ne": True},
            }
        )
        return result.deleted_count == 1
    except PyMongoError as exc:
        logger.exception("failed to delete battle %s for participant %s", battle_id, user_id)
        raise BattleStoreUnavailable("battle delete failed") from exc
