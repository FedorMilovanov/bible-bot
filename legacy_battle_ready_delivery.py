"""Durable creator notification outbox for newly joined PvP battles.

Opponent claiming is already an atomic Mongo transition. This module provides a
small delivery marker that is staged by that same transition so a process crash,
Telegram RetryAfter, or transient network failure cannot erase the creator's
"opponent found" notification. Historical/in-progress battles without this
marker are deliberately invisible to the worker and are never backfilled.
"""
from __future__ import annotations

import logging
import math
import uuid
from datetime import timedelta

from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

from legacy_battle_protocols import BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE

logger = logging.getLogger(__name__)

BATTLE_READY_DELIVERY_PROTOCOL = "battle_ready_outbox_v1"
_BATTLE_READY_PATH = "creator_ready_delivery"


class LegacyBattleReadyDeliveryUnavailable(RuntimeError):
    """Raised when Mongo cannot confirm battle-ready delivery state."""


class LegacyBattleReadyDeliveryConflict(RuntimeError):
    """Raised when persisted battle-ready delivery evidence is contradictory."""


def _database():
    import database

    return database


def _collection():
    collection = getattr(_database(), "battles_collection", None)
    if collection is None:
        raise LegacyBattleReadyDeliveryUnavailable("battle collection is unavailable")
    return collection


def _required_battle_id(value) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("battle_id is required")
    return value.strip()


def _required_claim_token(value) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("claim_token is required")
    return value.strip()


def _positive_delay(value) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("delay_seconds must be a positive finite number")
    delay = float(value)
    if not math.isfinite(delay) or delay <= 0:
        raise ValueError("delay_seconds must be a positive finite number")
    return delay


def battle_ready_delivery_marker() -> dict:
    """Return the marker embedded atomically by a future opponent claim."""
    return {
        "protocol": BATTLE_READY_DELIVERY_PROTOCOL,
        "delivered": False,
        "attempts": 0,
    }


def _due_filter(now) -> dict:
    path = _BATTLE_READY_PATH
    return {
        "status": "in_progress",
        "question_progress_protocol": BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE,
        "creator_finished": {"$ne": True},
        f"{path}.protocol": BATTLE_READY_DELIVERY_PROTOCOL,
        f"{path}.delivered": {"$ne": True},
        "$and": [
            {
                "$or": [
                    {f"{path}.retry_after": {"$exists": False}},
                    {f"{path}.retry_after": {"$lte": now}},
                ]
            },
            {
                "$or": [
                    {f"{path}.lease_until": {"$exists": False}},
                    {f"{path}.lease_until": {"$lte": now}},
                ]
            },
        ],
    }


def claim_creator_ready_delivery(
    battle_id: str,
    *,
    lease_seconds: int = 120,
) -> dict | None:
    """Lease one due creator-ready notification for remote Telegram delivery."""
    battle_id = _required_battle_id(battle_id)
    if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int) or lease_seconds <= 0:
        raise ValueError("lease_seconds must be a positive integer")
    database = _database()
    collection = _collection()
    now = database._now_utc()
    token = uuid.uuid4().hex
    path = _BATTLE_READY_PATH
    try:
        claimed = collection.find_one_and_update(
            {"_id": battle_id, **_due_filter(now)},
            {
                "$set": {
                    f"{path}.claim_token": token,
                    f"{path}.lease_until": now + timedelta(seconds=lease_seconds),
                    f"{path}.last_attempt_at": now,
                },
                "$inc": {f"{path}.attempts": 1},
                "$unset": {f"{path}.retry_after": ""},
            },
            return_document=ReturnDocument.AFTER,
        )
        if claimed is None:
            return None
        marker = claimed.get(path)
        if not isinstance(marker, dict):
            raise LegacyBattleReadyDeliveryConflict("claimed battle-ready marker is invalid")
        creator_id = claimed.get("creator_id")
        opponent_name = claimed.get("opponent_name")
        if (
            isinstance(creator_id, bool)
            or not isinstance(creator_id, int)
            or creator_id <= 0
            or not isinstance(opponent_name, str)
            or not opponent_name.strip()
        ):
            raise LegacyBattleReadyDeliveryConflict("battle-ready recipient evidence is invalid")
        return {"battle": claimed, "marker": marker, "claim_token": token}
    except LegacyBattleReadyDeliveryConflict:
        raise
    except PyMongoError as exc:
        logger.exception("failed to lease creator-ready notification for %s", battle_id)
        raise LegacyBattleReadyDeliveryUnavailable("battle-ready claim failed") from exc


def mark_creator_ready_delivered(battle_id: str, claim_token: str) -> bool:
    battle_id = _required_battle_id(battle_id)
    claim_token = _required_claim_token(claim_token)
    database = _database()
    collection = _collection()
    now = database._now_utc()
    path = _BATTLE_READY_PATH
    try:
        result = collection.update_one(
            {
                "_id": battle_id,
                f"{path}.protocol": BATTLE_READY_DELIVERY_PROTOCOL,
                f"{path}.delivered": {"$ne": True},
                f"{path}.claim_token": claim_token,
            },
            {
                "$set": {
                    f"{path}.delivered": True,
                    f"{path}.delivered_at": now,
                },
                "$unset": {
                    f"{path}.claim_token": "",
                    f"{path}.lease_until": "",
                    f"{path}.retry_after": "",
                    f"{path}.last_error": "",
                },
            },
        )
        if result.modified_count == 1:
            return True
        existing = collection.find_one({"_id": battle_id}, {path: 1})
        marker = existing.get(path) if isinstance(existing, dict) else None
        return isinstance(marker, dict) and marker.get("delivered") is True
    except PyMongoError as exc:
        raise LegacyBattleReadyDeliveryUnavailable("battle-ready acknowledgement failed") from exc


def defer_creator_ready_delivery(
    battle_id: str,
    claim_token: str,
    *,
    delay_seconds: float,
    error: str = "",
) -> bool:
    battle_id = _required_battle_id(battle_id)
    claim_token = _required_claim_token(claim_token)
    delay = _positive_delay(delay_seconds)
    database = _database()
    collection = _collection()
    now = database._now_utc()
    path = _BATTLE_READY_PATH
    try:
        result = collection.update_one(
            {
                "_id": battle_id,
                f"{path}.protocol": BATTLE_READY_DELIVERY_PROTOCOL,
                f"{path}.delivered": {"$ne": True},
                f"{path}.claim_token": claim_token,
            },
            {
                "$set": {
                    f"{path}.retry_after": now + timedelta(seconds=delay),
                    f"{path}.last_error": str(error or "")[:500],
                },
                "$unset": {
                    f"{path}.claim_token": "",
                    f"{path}.lease_until": "",
                },
            },
        )
        return result.modified_count == 1
    except PyMongoError as exc:
        raise LegacyBattleReadyDeliveryUnavailable("battle-ready deferral failed") from exc


def release_creator_ready_delivery(
    battle_id: str,
    claim_token: str,
    *,
    error: str = "",
) -> bool:
    battle_id = _required_battle_id(battle_id)
    claim_token = _required_claim_token(claim_token)
    collection = _collection()
    path = _BATTLE_READY_PATH
    try:
        result = collection.update_one(
            {
                "_id": battle_id,
                f"{path}.protocol": BATTLE_READY_DELIVERY_PROTOCOL,
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
        raise LegacyBattleReadyDeliveryUnavailable("battle-ready release failed") from exc


def settle_creator_ready_failure(
    battle_id: str,
    claim_token: str,
    *,
    error: str,
) -> bool:
    battle_id = _required_battle_id(battle_id)
    claim_token = _required_claim_token(claim_token)
    detail = str(error or "").strip()
    if not detail:
        raise ValueError("terminal failure detail is required")
    database = _database()
    collection = _collection()
    now = database._now_utc()
    path = _BATTLE_READY_PATH
    try:
        result = collection.update_one(
            {
                "_id": battle_id,
                f"{path}.protocol": BATTLE_READY_DELIVERY_PROTOCOL,
                f"{path}.delivered": {"$ne": True},
                f"{path}.claim_token": claim_token,
            },
            {
                "$set": {
                    f"{path}.delivered": True,
                    f"{path}.terminal_failed": True,
                    f"{path}.terminal_error": detail[:500],
                    f"{path}.delivered_at": now,
                },
                "$unset": {
                    f"{path}.claim_token": "",
                    f"{path}.lease_until": "",
                    f"{path}.retry_after": "",
                },
            },
        )
        return result.modified_count == 1
    except PyMongoError as exc:
        raise LegacyBattleReadyDeliveryUnavailable("battle-ready terminal settlement failed") from exc


def get_pending_creator_ready_battles(limit: int = 50) -> list[dict]:
    """List only future protocol markers that are due and not currently leased."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0 or limit > 100:
        raise ValueError("limit must be an integer between 1 and 100")
    database = _database()
    now = database._now_utc()
    try:
        return list(
            _collection()
            .find(_due_filter(now))
            .sort("joined_at_dt", 1)
            .limit(limit)
        )
    except PyMongoError as exc:
        logger.exception("failed to list pending creator-ready notifications")
        raise LegacyBattleReadyDeliveryUnavailable("pending battle-ready lookup failed") from exc
