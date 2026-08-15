"""Durable delivery authority for terminal legacy quiz result cards.

Scoring/finalization is already idempotent, but the Telegram result card used to
be sent only from RAM after the session had become durably terminal. A process
crash or Telegram RetryAfter at that point could therefore lose the user's only
result notification. This module adds a small per-session outbox marker without
owning scoring, bonuses, achievements or result rendering.

The marker is created only by the same CAS that first changes an in-progress
session to ``finished``. Historical finished sessions are never backfilled, so
deploying this code cannot replay old result cards.
"""
from __future__ import annotations

import logging
import math
import uuid
from datetime import timedelta

from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)

RESULT_CARD_DELIVERY_PROTOCOL = "result_card_outbox_v1"
_MAX_RESULT_TEXT = 4096


class ResultCardDeliveryUnavailable(RuntimeError):
    """Raised when durable result-card delivery state cannot be confirmed."""


class ResultCardDeliveryConflict(RuntimeError):
    """Raised when persisted result-card state contradicts the expected marker."""


def _database():
    import database

    return database


def _collection():
    collection = getattr(_database(), "quiz_sessions_collection", None)
    if collection is None:
        raise ResultCardDeliveryUnavailable("quiz session collection is unavailable")
    return collection


def _owner_id(user_id: int | str) -> str:
    return _database()._uid(user_id)


def _required_session_id(value) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("session_id is required")
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


def _bounded_text(value) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("result-card text is required")
    if len(value) > _MAX_RESULT_TEXT:
        raise ValueError("result-card text is too long")
    return value


def build_result_card_delivery_marker(session: dict) -> dict | None:
    """Build minimal truthful fallback evidence from an exact-complete session.

    ``None`` means no durable Telegram destination was persisted. That remains a
    valid legacy/migration case and must never block terminal scoring.
    """
    if not isinstance(session, dict):
        raise ValueError("session must be a dict")
    chat_id = session.get("chat_id")
    if chat_id is None:
        return None
    if isinstance(chat_id, bool) or not isinstance(chat_id, int):
        raise ResultCardDeliveryConflict("finished quiz chat_id is invalid")

    score = session.get("correct_count")
    question_ids = session.get("question_ids")
    if (
        isinstance(score, bool)
        or not isinstance(score, int)
        or score < 0
        or not isinstance(question_ids, list)
        or not question_ids
        or score > len(question_ids)
    ):
        raise ResultCardDeliveryConflict("finished quiz result evidence is invalid")

    level_name = session.get("level_name")
    if not isinstance(level_name, str) or not level_name.strip():
        level_name = "Тест"
    else:
        level_name = level_name.strip()[:200]

    mode = session.get("mode")
    if not isinstance(mode, str) or not mode.strip():
        mode = "level"

    return {
        "protocol": RESULT_CARD_DELIVERY_PROTOCOL,
        "delivered": False,
        "attempts": 0,
        "chat_id": chat_id,
        "score": score,
        "total": len(question_ids),
        "level_name": level_name,
        "mode": mode[:64],
        "is_retry": session.get("is_retry") is True,
    }


def set_result_card_delivery_text(
    session_id: str,
    user_id: int | str,
    text: str,
) -> bool:
    """Persist the rich live-rendered card once, rejecting conflicting replay."""
    session_id = _required_session_id(session_id)
    text = _bounded_text(text)
    collection = _collection()
    owner_filter = {"_id": session_id, "user_id": _owner_id(user_id)}
    path = "result_card_delivery"
    try:
        updated = collection.find_one_and_update(
            {
                **owner_filter,
                "status": "finished",
                f"{path}.protocol": RESULT_CARD_DELIVERY_PROTOCOL,
                f"{path}.delivered": {"$ne": True},
                f"{path}.text": {"$exists": False},
            },
            {"$set": {f"{path}.text": text}},
            return_document=ReturnDocument.AFTER,
        )
        if updated is not None:
            return True

        existing = collection.find_one(owner_filter, {path: 1, "status": 1})
        marker = existing.get(path) if isinstance(existing, dict) else None
        if not isinstance(marker, dict):
            raise ResultCardDeliveryConflict("result-card outbox marker is missing")
        if marker.get("protocol") != RESULT_CARD_DELIVERY_PROTOCOL:
            raise ResultCardDeliveryConflict("result-card outbox protocol is invalid")
        if marker.get("delivered") is True:
            return False
        stored_text = marker.get("text")
        if stored_text == text:
            return True
        if stored_text is not None:
            raise ResultCardDeliveryConflict("result-card text conflicts with durable evidence")
        raise ResultCardDeliveryUnavailable("result-card text write was not confirmed")
    except (ResultCardDeliveryConflict, ResultCardDeliveryUnavailable):
        raise
    except PyMongoError as exc:
        logger.exception("failed to persist result-card text for %s", session_id)
        raise ResultCardDeliveryUnavailable("result-card text write failed") from exc


def claim_result_card_delivery(
    session_id: str,
    user_id: int | str,
    *,
    lease_seconds: int = 120,
) -> dict | None:
    """Lease one due pending result card for remote Telegram delivery."""
    session_id = _required_session_id(session_id)
    if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int) or lease_seconds <= 0:
        raise ValueError("lease_seconds must be a positive integer")
    database = _database()
    collection = _collection()
    now = database._now_utc()
    lease_until = now + timedelta(seconds=lease_seconds)
    token = uuid.uuid4().hex
    path = "result_card_delivery"
    try:
        claimed = collection.find_one_and_update(
            {
                "_id": session_id,
                "user_id": _owner_id(user_id),
                "status": "finished",
                f"{path}.protocol": RESULT_CARD_DELIVERY_PROTOCOL,
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
            },
            {
                "$set": {
                    f"{path}.claim_token": token,
                    f"{path}.lease_until": lease_until,
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
            raise ResultCardDeliveryConflict("claimed result-card marker is invalid")
        return {"session": claimed, "marker": marker, "claim_token": token}
    except ResultCardDeliveryConflict:
        raise
    except PyMongoError as exc:
        logger.exception("failed to lease result card for %s", session_id)
        raise ResultCardDeliveryUnavailable("result-card claim failed") from exc


def mark_result_card_delivered(
    session_id: str,
    user_id: int | str,
    claim_token: str,
) -> bool:
    session_id = _required_session_id(session_id)
    claim_token = _required_claim_token(claim_token)
    database = _database()
    collection = _collection()
    path = "result_card_delivery"
    now = database._now_utc()
    try:
        result = collection.update_one(
            {
                "_id": session_id,
                "user_id": _owner_id(user_id),
                f"{path}.protocol": RESULT_CARD_DELIVERY_PROTOCOL,
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
        existing = collection.find_one(
            {"_id": session_id, "user_id": _owner_id(user_id)},
            {f"{path}.delivered": 1},
        )
        marker = existing.get(path) if isinstance(existing, dict) else None
        return isinstance(marker, dict) and marker.get("delivered") is True
    except PyMongoError as exc:
        raise ResultCardDeliveryUnavailable("result-card acknowledgement failed") from exc


def defer_result_card_delivery(
    session_id: str,
    user_id: int | str,
    claim_token: str,
    *,
    delay_seconds: float,
    error: str = "",
) -> bool:
    session_id = _required_session_id(session_id)
    claim_token = _required_claim_token(claim_token)
    delay = _positive_delay(delay_seconds)
    database = _database()
    collection = _collection()
    now = database._now_utc()
    path = "result_card_delivery"
    try:
        result = collection.update_one(
            {
                "_id": session_id,
                "user_id": _owner_id(user_id),
                f"{path}.protocol": RESULT_CARD_DELIVERY_PROTOCOL,
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
        raise ResultCardDeliveryUnavailable("result-card deferral failed") from exc


def release_result_card_delivery(
    session_id: str,
    user_id: int | str,
    claim_token: str,
    *,
    error: str = "",
) -> bool:
    session_id = _required_session_id(session_id)
    claim_token = _required_claim_token(claim_token)
    collection = _collection()
    path = "result_card_delivery"
    try:
        result = collection.update_one(
            {
                "_id": session_id,
                "user_id": _owner_id(user_id),
                f"{path}.protocol": RESULT_CARD_DELIVERY_PROTOCOL,
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
        raise ResultCardDeliveryUnavailable("result-card release failed") from exc


def settle_result_card_delivery_failure(
    session_id: str,
    user_id: int | str,
    claim_token: str,
    *,
    error: str,
) -> bool:
    session_id = _required_session_id(session_id)
    claim_token = _required_claim_token(claim_token)
    detail = str(error or "").strip()
    if not detail:
        raise ValueError("terminal failure detail is required")
    database = _database()
    collection = _collection()
    now = database._now_utc()
    path = "result_card_delivery"
    try:
        result = collection.update_one(
            {
                "_id": session_id,
                "user_id": _owner_id(user_id),
                f"{path}.protocol": RESULT_CARD_DELIVERY_PROTOCOL,
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
        raise ResultCardDeliveryUnavailable("result-card terminal settlement failed") from exc


def get_pending_result_card_sessions(limit: int = 50) -> list[dict]:
    """List due pending markers only; historical finished sessions are invisible."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")
    database = _database()
    now = database._now_utc()
    path = "result_card_delivery"
    try:
        return list(
            _collection()
            .find(
                {
                    "status": "finished",
                    f"{path}.protocol": RESULT_CARD_DELIVERY_PROTOCOL,
                    f"{path}.delivered": {"$ne": True},
                    "$or": [
                        {f"{path}.retry_after": {"$exists": False}},
                        {f"{path}.retry_after": {"$lte": now}},
                    ],
                }
            )
            .sort("end_time", 1)
            .limit(limit)
        )
    except PyMongoError as exc:
        logger.exception("failed to list pending result cards")
        raise ResultCardDeliveryUnavailable("pending result-card lookup failed") from exc
