"""Owner-scoped Mongo helpers for legacy Telegram quiz-session callbacks."""
from __future__ import annotations

import logging
import math

from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

from legacy_attempt_identity import persisted_attempt_id
from legacy_delivery_retention import ensure_state_aware_delivery_ttl
from legacy_session_retention import ensure_state_aware_session_ttl

logger = logging.getLogger(__name__)

# database._ensure_indexes() historically installs generic TTL indexes before
# bot.py imports this integrity layer. Migrate them immediately during normal
# runtime import so pending quiz results and undelivered outbox evidence are not
# eligible for age-only deletion.
SESSION_RETENTION_READY = ensure_state_aware_session_ttl()
DELIVERY_RETENTION_READY = ensure_state_aware_delivery_ttl()


class QuizSessionStoreUnavailable(RuntimeError):
    """Raised when an owner-scoped quiz-session operation cannot reach MongoDB."""


class QuizSessionAnswerConflict(RuntimeError):
    """Raised when an answer cannot be reconciled with the durable session index."""


def _database():
    import database

    return database


def _quiz_session_collection():
    collection = getattr(_database(), "quiz_sessions_collection", None)
    if collection is None:
        raise QuizSessionStoreUnavailable("quiz session collection is unavailable")
    return collection


def _owner_id(user_id: int | str) -> str:
    """Use the same canonical representation as database.create_quiz_session."""
    return _database()._uid(user_id)


def _expected_index(value) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("expected_index must be a non-negative integer")
    return value


def _required_attempt_id(value) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("expected_attempt_id is required")
    return value.strip()


def _latency_seconds(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("latency_seconds must be a finite non-negative number")
    try:
        latency = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("latency_seconds must be a finite non-negative number") from exc
    if not math.isfinite(latency) or latency < 0:
        raise ValueError("latency_seconds must be a finite non-negative number")
    return latency


def get_owned_quiz_session(session_id: str, user_id: int) -> dict | None:
    collection = _quiz_session_collection()
    try:
        return collection.find_one({"_id": session_id, "user_id": _owner_id(user_id)})
    except PyMongoError as exc:
        logger.exception("failed to load owned quiz session %s", session_id)
        raise QuizSessionStoreUnavailable("quiz session lookup failed") from exc


def record_owned_quiz_answer(
    session_id: str,
    user_id: int,
    *,
    expected_attempt_id: str,
    expected_index: int,
    question_id: str,
    user_answer: str,
    is_correct: bool,
    question_obj: dict,
    latency_seconds: float | None = None,
) -> dict:
    """Atomically persist one answer for one exact logical quiz attempt.

    ``session_id`` identifies the durable container; ``expected_attempt_id``
    identifies the logical attempt currently rendered to the user. Restart can
    therefore reset the same container atomically without allowing an old
    callback to mutate the new attempt. Legacy documents with no ``attempt_id``
    match only when the expected attempt equals the container id; the successful
    answer write backfills the field.

    The handler must mutate RAM counters/index only after this function returns
    either ``applied=True`` or an exact ``applied=False`` replay.
    """
    expected_attempt_id = _required_attempt_id(expected_attempt_id)
    expected_index = _expected_index(expected_index)
    if not isinstance(question_id, str) or not question_id:
        raise ValueError("question_id is required")
    if not isinstance(user_answer, str):
        raise ValueError("user_answer must be a string")
    if not isinstance(is_correct, bool):
        raise ValueError("is_correct must be a boolean")
    if not isinstance(question_obj, dict):
        raise ValueError("question_obj must be a dict")
    latency = _latency_seconds(latency_seconds)

    database = _database()
    collection = _quiz_session_collection()
    owner = _owner_id(user_id)
    now = database._now_utc()
    answer_record = {
        "index": expected_index,
        "qid": question_id,
        "user_answer": user_answer,
        "is_correct": is_correct,
        "question_obj": question_obj,
        "latency_seconds": latency,
        "ts": now.isoformat(),
    }
    owner_filter = {"_id": session_id, "user_id": owner}
    attempt_filter = {
        "$or": [
            {"attempt_id": expected_attempt_id},
            {
                "attempt_id": {"$exists": False},
                "_id": expected_attempt_id,
            },
        ]
    }
    transition_filter = {
        **owner_filter,
        "status": "in_progress",
        "current_index": expected_index,
        f"question_ids.{expected_index}": question_id,
        **attempt_filter,
    }

    try:
        updated = collection.find_one_and_update(
            transition_filter,
            {
                "$inc": {
                    "current_index": 1,
                    "correct_count": 1 if is_correct else 0,
                },
                "$push": {"answered_questions": answer_record},
                "$set": {
                    "attempt_id": expected_attempt_id,
                    "updated_at": now.isoformat(),
                    "updated_at_dt": now,
                    "question_sent_at": None,
                },
            },
            return_document=ReturnDocument.AFTER,
        )
        if updated is not None:
            return {
                "applied": True,
                "session": updated,
                "answer": answer_record,
            }

        existing = collection.find_one(owner_filter)
        if existing is None:
            raise QuizSessionAnswerConflict("quiz answer session is missing or not owned")
        if existing.get("status") != "in_progress":
            raise QuizSessionAnswerConflict("quiz answer session is not in progress")
        try:
            durable_attempt_id = persisted_attempt_id(existing)
        except ValueError as exc:
            raise QuizSessionAnswerConflict("durable quiz attempt identity is invalid") from exc
        if durable_attempt_id != expected_attempt_id:
            raise QuizSessionAnswerConflict("quiz answer belongs to another attempt")

        durable_index = existing.get("current_index")
        if (
            isinstance(durable_index, bool)
            or not isinstance(durable_index, int)
            or durable_index != expected_index + 1
        ):
            raise QuizSessionAnswerConflict(
                "quiz answer is not the immediately preceding durable transition"
            )

        ledger = existing.get("answered_questions", [])
        if not isinstance(ledger, list) or len(ledger) != durable_index:
            raise QuizSessionAnswerConflict("durable quiz answer ledger is inconsistent")
        stored = ledger[expected_index]
        if not isinstance(stored, dict):
            raise QuizSessionAnswerConflict("durable quiz answer ledger is invalid")

        same_transition = (
            stored.get("index", expected_index) == expected_index
            and stored.get("qid") == question_id
            and stored.get("user_answer") == user_answer
            and stored.get("is_correct") is is_correct
        )
        if not same_transition:
            raise QuizSessionAnswerConflict(
                "conflicting quiz answer already occupies expected index"
            )
        return {
            "applied": False,
            "session": existing,
            "answer": stored,
        }
    except QuizSessionAnswerConflict:
        raise
    except PyMongoError as exc:
        logger.exception(
            "failed to persist owned quiz answer %s[%s]",
            session_id,
            expected_index,
        )
        raise QuizSessionStoreUnavailable("quiz answer write failed") from exc


def cancel_owned_quiz_session(session_id: str, user_id: int) -> dict | None:
    """Atomically cancel and return the caller's active session snapshot."""
    collection = _quiz_session_collection()
    try:
        return collection.find_one_and_update(
            {"_id": session_id, "user_id": _owner_id(user_id), "status": "in_progress"},
            {"$set": {"status": "cancelled"}},
            return_document=ReturnDocument.BEFORE,
        )
    except PyMongoError as exc:
        logger.exception("failed to cancel owned quiz session %s", session_id)
        raise QuizSessionStoreUnavailable("quiz session cancellation failed") from exc


def finish_owned_quiz_session(session_id: str, user_id: int) -> dict | None:
    """Atomically finish the caller's active session after durable result writes.

    The transition is intentionally owner/status scoped. If another idempotent
    finalizer already finished the session, the existing finished document is
    returned. Other terminal/missing states return ``None``. Mongo failures are
    surfaced so callers can keep the result retryable instead of pretending the
    recovery record was closed successfully.
    """
    database = _database()
    collection = _quiz_session_collection()
    now = database._now_utc()
    owner_filter = {"_id": session_id, "user_id": _owner_id(user_id)}
    try:
        finished = collection.find_one_and_update(
            {**owner_filter, "status": "in_progress"},
            {"$set": {"status": "finished", "end_time": now}},
            return_document=ReturnDocument.AFTER,
        )
        if finished is not None:
            return finished

        existing = collection.find_one(owner_filter)
        if existing and existing.get("status") == "finished":
            return existing
        return None
    except PyMongoError as exc:
        logger.exception("failed to finish owned quiz session %s", session_id)
        raise QuizSessionStoreUnavailable("quiz session finish failed") from exc
