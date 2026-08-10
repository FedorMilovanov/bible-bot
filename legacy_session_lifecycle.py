"""Crash-safe lifecycle transitions for persisted legacy quiz attempts.

The Mongo document id is a stable session container. ``attempt_id`` identifies
the current logical quiz pass inside that container. Restart therefore resets
the same document atomically and assigns a fresh attempt id instead of performing
the historical destructive ``cancel -> insert`` sequence.
"""
from __future__ import annotations

import time
import uuid

from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

from legacy_attempt_identity import persisted_attempt_id
from legacy_restart_policy import LegacyRestartStateInvalid, classify_restart_session
from legacy_session_spec import validated_session_spec


class QuizSessionLifecycleUnavailable(RuntimeError):
    """Raised when a lifecycle transition cannot reach durable storage."""


class QuizSessionLifecycleConflict(RuntimeError):
    """Raised when stale/corrupt evidence makes a transition unsafe."""


def _database():
    import database

    return database


def _collection():
    collection = getattr(_database(), "quiz_sessions_collection", None)
    if collection is None:
        raise QuizSessionLifecycleUnavailable("quiz session collection is unavailable")
    return collection


def _required_id(value, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    return value.strip()


def _owner_filter(session_id: str, user_id: int | str) -> dict:
    return {
        "_id": _required_id(session_id, "session_id"),
        "user_id": _database()._uid(user_id),
    }


def _attempt_query(expected_attempt_id: str) -> dict:
    """Match explicit attempts or legacy documents whose attempt equals `_id`."""
    expected_attempt_id = _required_id(expected_attempt_id, "expected_attempt_id")
    return {
        "$or": [
            {"attempt_id": expected_attempt_id},
            {
                "attempt_id": {"$exists": False},
                "_id": expected_attempt_id,
            },
        ]
    }


def _classify_partial(session: dict) -> None:
    try:
        decision = classify_restart_session(session)
    except LegacyRestartStateInvalid as exc:
        raise QuizSessionLifecycleConflict(
            "quiz session state is not safe for lifecycle mutation"
        ) from exc
    if decision.action == "finalize":
        raise QuizSessionLifecycleConflict(
            "completed quiz evidence must be finalized, not cancelled or restarted"
        )
    if decision.action != "resume":
        raise QuizSessionLifecycleConflict("quiz session lifecycle state is unsupported")


def _exact_state_filter(session: dict) -> dict:
    return {
        "status": "in_progress",
        "current_index": session.get("current_index"),
        "correct_count": session.get("correct_count"),
        "question_ids": session.get("question_ids"),
        "answered_questions": session.get("answered_questions"),
    }


def restart_owned_quiz_attempt(
    session_id: str,
    user_id: int | str,
    *,
    expected_attempt_id: str,
    mode: str,
    question_ids: list,
    questions_data: list,
    level_key: str | None = None,
    level_name: str | None = None,
    time_limit: int | None = None,
    chat_id: int | None = None,
) -> dict:
    """Atomically replace one incomplete logical attempt inside its container.

    If the same old-attempt request is retried after the update committed but
    before the caller received the response, ``previous_attempt_id`` identifies
    the already-created replacement and the durable snapshot is returned without
    resetting it a second time.
    """
    expected_attempt_id = _required_id(expected_attempt_id, "expected_attempt_id")
    spec = validated_session_spec(
        mode=mode,
        question_ids=question_ids,
        questions_data=questions_data,
        level_key=level_key,
        level_name=level_name,
        time_limit=time_limit,
        chat_id=chat_id,
    )
    database = _database()
    collection = _collection()
    owner_filter = _owner_filter(session_id, user_id)

    try:
        existing = collection.find_one(owner_filter)
        if existing is None:
            raise QuizSessionLifecycleConflict("quiz session is missing or not owned")
        if existing.get("status") != "in_progress":
            raise QuizSessionLifecycleConflict("quiz session is not in progress")

        # Lost response after a committed restart: the old attempt id remains as
        # durable replay evidence. Never reset the replacement a second time.
        if existing.get("previous_attempt_id") == expected_attempt_id:
            return {
                "applied": False,
                "session": existing,
                "attempt_id": persisted_attempt_id(existing),
                "previous_attempt_id": expected_attempt_id,
            }

        try:
            current_attempt_id = persisted_attempt_id(existing)
        except ValueError as exc:
            raise QuizSessionLifecycleConflict("quiz attempt identity is invalid") from exc
        if current_attempt_id != expected_attempt_id:
            raise QuizSessionLifecycleConflict("restart belongs to another quiz attempt")
        _classify_partial(existing)

        now = database._now_utc()
        new_attempt_id = str(uuid.uuid4())
        updated = collection.find_one_and_update(
            {
                **owner_filter,
                **_attempt_query(expected_attempt_id),
                **_exact_state_filter(existing),
            },
            {
                "$set": {
                    "status": "in_progress",
                    "attempt_id": new_attempt_id,
                    "previous_attempt_id": expected_attempt_id,
                    **spec,
                    "current_index": 0,
                    "correct_count": 0,
                    "answered_questions": [],
                    "question_sent_at": None,
                    "start_time": time.time(),
                    "started_at": now.isoformat(),
                    "updated_at": now.isoformat(),
                    "updated_at_dt": now,
                    "restarted_at": now,
                },
                "$inc": {"restart_count": 1},
                "$unset": {
                    "end_time": "",
                    "cancelled_at": "",
                },
            },
            return_document=ReturnDocument.AFTER,
        )
        if updated is not None:
            return {
                "applied": True,
                "session": updated,
                "attempt_id": new_attempt_id,
                "previous_attempt_id": expected_attempt_id,
            }

        latest = collection.find_one(owner_filter)
        if latest is None:
            raise QuizSessionLifecycleConflict("quiz session disappeared during restart")
        if latest.get("previous_attempt_id") == expected_attempt_id:
            return {
                "applied": False,
                "session": latest,
                "attempt_id": persisted_attempt_id(latest),
                "previous_attempt_id": expected_attempt_id,
            }
        try:
            latest_attempt_id = persisted_attempt_id(latest)
        except ValueError as exc:
            raise QuizSessionLifecycleConflict("quiz attempt identity is invalid") from exc
        if latest_attempt_id != expected_attempt_id:
            raise QuizSessionLifecycleConflict("restart lost a race with another attempt")
        raise QuizSessionLifecycleConflict("quiz session changed during restart")
    except QuizSessionLifecycleConflict:
        raise
    except PyMongoError as exc:
        raise QuizSessionLifecycleUnavailable("quiz attempt restart failed") from exc


def cancel_owned_incomplete_quiz_attempt(
    session_id: str,
    user_id: int | str,
    *,
    expected_attempt_id: str,
) -> dict:
    """Cancel only the exact incomplete attempt rendered to the caller."""
    expected_attempt_id = _required_id(expected_attempt_id, "expected_attempt_id")
    database = _database()
    collection = _collection()
    owner_filter = _owner_filter(session_id, user_id)

    try:
        existing = collection.find_one(owner_filter)
        if existing is None:
            raise QuizSessionLifecycleConflict("quiz session is missing or not owned")
        if existing.get("status") == "cancelled":
            try:
                if persisted_attempt_id(existing) == expected_attempt_id:
                    return {"applied": False, "session": existing}
            except ValueError:
                pass
            raise QuizSessionLifecycleConflict("cancelled session belongs to another attempt")
        if existing.get("status") != "in_progress":
            raise QuizSessionLifecycleConflict("quiz session is not cancellable")

        try:
            current_attempt_id = persisted_attempt_id(existing)
        except ValueError as exc:
            raise QuizSessionLifecycleConflict("quiz attempt identity is invalid") from exc
        if current_attempt_id != expected_attempt_id:
            raise QuizSessionLifecycleConflict("cancel belongs to another quiz attempt")
        _classify_partial(existing)

        now = database._now_utc()
        cancelled = collection.find_one_and_update(
            {
                **owner_filter,
                **_attempt_query(expected_attempt_id),
                **_exact_state_filter(existing),
            },
            {
                "$set": {
                    "status": "cancelled",
                    "cancelled_at": now,
                    "updated_at": now.isoformat(),
                    "updated_at_dt": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if cancelled is not None:
            return {"applied": True, "session": cancelled}

        latest = collection.find_one(owner_filter)
        if latest is not None and latest.get("status") == "cancelled":
            try:
                if persisted_attempt_id(latest) == expected_attempt_id:
                    return {"applied": False, "session": latest}
            except ValueError:
                pass
        raise QuizSessionLifecycleConflict("quiz session changed during cancellation")
    except QuizSessionLifecycleConflict:
        raise
    except PyMongoError as exc:
        raise QuizSessionLifecycleUnavailable("quiz attempt cancellation failed") from exc
