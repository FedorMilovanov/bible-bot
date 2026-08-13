"""Attempt/index-bound timer marker for persisted legacy quiz questions."""
from __future__ import annotations

import math

from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

from legacy_attempt_identity import persisted_attempt_id


class LegacyQuestionTimerUnavailable(RuntimeError):
    """Durable question timer storage is unavailable."""


class LegacyQuestionTimerConflict(RuntimeError):
    """A timer operation targets a stale/corrupt logical attempt or question."""


def _database():
    import database

    return database


def _collection():
    collection = getattr(_database(), "quiz_sessions_collection", None)
    if collection is None:
        raise LegacyQuestionTimerUnavailable("quiz session collection is unavailable")
    return collection


def _finite_nonnegative(value, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a finite non-negative number")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"{field} must be a finite non-negative number")
    return number


def _index(value) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("expected_index must be a non-negative integer")
    return value


def _attempt_filter(attempt_id: str) -> dict:
    if not isinstance(attempt_id, str) or not attempt_id:
        raise ValueError("expected_attempt_id is required")
    return {
        "$or": [
            {"attempt_id": attempt_id},
            {"attempt_id": {"$exists": False}, "_id": attempt_id},
        ]
    }


def mark_question_sent_once(
    session_id: str,
    user_id: int | str,
    *,
    expected_attempt_id: str,
    expected_index: int,
    sent_at: float,
) -> dict:
    """Persist the first send timestamp for one exact attempt/question.

    A replay never moves an existing timer forward. Restart resets
    ``question_sent_at`` to ``None`` and changes ``attempt_id``, so stale send
    tasks cannot install a timer in the replacement attempt.
    """
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("session_id is required")
    expected_index = _index(expected_index)
    sent_at = _finite_nonnegative(sent_at, "sent_at")
    database = _database()
    collection = _collection()
    owner = database._uid(user_id)
    now = database._now_utc()
    owner_filter = {"_id": session_id, "user_id": owner}

    try:
        updated = collection.find_one_and_update(
            {
                **owner_filter,
                "status": "in_progress",
                "current_index": expected_index,
                **_attempt_filter(expected_attempt_id),
                "$and": [
                    {
                        "$or": [
                            {"question_sent_at": None},
                            {"question_sent_at": {"$exists": False}},
                        ]
                    }
                ],
            },
            {
                "$set": {
                    "question_sent_at": sent_at,
                    "updated_at": now.isoformat(),
                    "updated_at_dt": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if updated is not None:
            return {"applied": True, "session": updated, "sent_at": sent_at}

        existing = collection.find_one(owner_filter)
        if existing is None or existing.get("status") != "in_progress":
            raise LegacyQuestionTimerConflict("question timer session is missing or terminal")
        try:
            durable_attempt = persisted_attempt_id(existing)
        except ValueError as exc:
            raise LegacyQuestionTimerConflict("durable attempt identity is invalid") from exc
        if durable_attempt != expected_attempt_id:
            raise LegacyQuestionTimerConflict("question timer belongs to another attempt")
        if existing.get("current_index") != expected_index:
            raise LegacyQuestionTimerConflict("question timer belongs to another question")
        durable_sent_at = existing.get("question_sent_at")
        try:
            durable_sent_at = _finite_nonnegative(durable_sent_at, "question_sent_at")
        except ValueError as exc:
            raise LegacyQuestionTimerConflict("durable question timer is invalid") from exc
        return {
            "applied": False,
            "session": existing,
            "sent_at": durable_sent_at,
        }
    except LegacyQuestionTimerConflict:
        raise
    except PyMongoError as exc:
        raise LegacyQuestionTimerUnavailable("question timer write failed") from exc


def question_is_timed_out(session: dict, *, now: float) -> bool:
    """Evaluate timeout only from strict durable timer evidence."""
    if not isinstance(session, dict):
        raise ValueError("session must be a dict")
    limit = session.get("time_limit")
    if limit is None:
        return False
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise LegacyQuestionTimerConflict("durable question time_limit is invalid")
    sent_at = session.get("question_sent_at")
    if sent_at is None:
        return False
    try:
        sent_at = _finite_nonnegative(sent_at, "question_sent_at")
        now_value = _finite_nonnegative(now, "now")
    except ValueError as exc:
        raise LegacyQuestionTimerConflict("durable question timer is invalid") from exc
    if now_value < sent_at:
        raise LegacyQuestionTimerConflict("durable question timer is in the future")
    return now_value - sent_at >= limit
