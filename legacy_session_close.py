"""Strict terminal transition for crash-safe legacy quiz finalization.

The generic legacy finish helper is owner/status scoped but intentionally knows
nothing about whether all persisted questions were actually answered. Scoring
finalizers need a stronger contract: an ``in_progress`` session can become
``finished`` only when Mongo itself proves an exact complete answer ledger.
"""
from __future__ import annotations

import logging

from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)


class QuizSessionCompletionInvalid(RuntimeError):
    """Raised when an owned persisted session is not durably complete."""


class QuizSessionCompletionStoreUnavailable(RuntimeError):
    """Raised when completion proof/transition cannot reach MongoDB."""


def _database():
    import database

    return database


def _collection():
    collection = getattr(_database(), "quiz_sessions_collection", None)
    if collection is None:
        raise QuizSessionCompletionStoreUnavailable(
            "quiz session collection is unavailable"
        )
    return collection


def _owner_id(user_id: int | str) -> str:
    return _database()._uid(user_id)


def _completion_snapshot(session: dict) -> tuple[int, list[str], list[dict]]:
    current = session.get("current_index")
    question_ids = session.get("question_ids")
    answered = session.get("answered_questions")
    correct_count = session.get("correct_count")
    if (
        isinstance(current, bool)
        or not isinstance(current, int)
        or current < 0
        or not isinstance(question_ids, list)
        or not question_ids
        or any(not isinstance(qid, str) or not qid for qid in question_ids)
        or not isinstance(answered, list)
        or current != len(question_ids)
        or current != len(answered)
        or isinstance(correct_count, bool)
        or not isinstance(correct_count, int)
        or correct_count < 0
        or correct_count > current
    ):
        raise QuizSessionCompletionInvalid(
            "quiz session does not contain an exact completed answer ledger"
        )

    counted_correct = 0
    for index, (question_id, answer) in enumerate(zip(question_ids, answered, strict=True)):
        if not isinstance(answer, dict):
            raise QuizSessionCompletionInvalid(
                "quiz session answer ledger contains a non-object record"
            )
        if answer.get("qid") != question_id:
            raise QuizSessionCompletionInvalid(
                "quiz session answer order does not match durable question ids"
            )
        if "index" in answer:
            answer_index = answer.get("index")
            if (
                isinstance(answer_index, bool)
                or not isinstance(answer_index, int)
                or answer_index != index
            ):
                raise QuizSessionCompletionInvalid(
                    "quiz session answer index does not match durable order"
                )
        is_correct = answer.get("is_correct")
        if not isinstance(is_correct, bool):
            raise QuizSessionCompletionInvalid(
                "quiz session answer correctness is not boolean"
            )
        counted_correct += int(is_correct)

    if counted_correct != correct_count:
        raise QuizSessionCompletionInvalid(
            "quiz session correct_count does not match durable answer ledger"
        )
    return current, question_ids, answered


def validate_completed_owned_quiz_session(
    session_id: str,
    user_id: int | str,
) -> dict | None:
    """Read-only proof that an owned recoverable session is exactly complete."""
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("session_id is required")
    collection = _collection()
    owner_filter = {"_id": session_id, "user_id": _owner_id(user_id)}
    try:
        existing = collection.find_one(owner_filter)
        if existing is None:
            return None
        if existing.get("status") not in {"in_progress", "finished"}:
            return None
        _completion_snapshot(existing)
        return existing
    except QuizSessionCompletionInvalid:
        raise
    except PyMongoError as exc:
        logger.exception("failed to validate completed quiz session %s", session_id)
        raise QuizSessionCompletionStoreUnavailable(
            "quiz session completion proof failed"
        ) from exc


def finish_completed_owned_quiz_session(
    session_id: str,
    user_id: int | str,
) -> dict | None:
    """Finish only a durably complete owned session, idempotently.

    ``None`` is reserved for genuinely missing/non-recoverable terminal state.
    An owned ``in_progress`` document with incomplete or contradictory evidence
    raises ``QuizSessionCompletionInvalid`` so result finalization stays pending
    rather than silently closing bad evidence.
    """
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("session_id is required")

    database = _database()
    collection = _collection()
    owner_filter = {"_id": session_id, "user_id": _owner_id(user_id)}

    try:
        existing = collection.find_one(owner_filter)
        if existing is None:
            return None

        status = existing.get("status")
        if status == "finished":
            _completion_snapshot(existing)
            return existing
        if status != "in_progress":
            return None

        current, question_ids, answered = _completion_snapshot(existing)
        correct_count = existing["correct_count"]
        now = database._now_utc()
        finished = collection.find_one_and_update(
            {
                **owner_filter,
                "status": "in_progress",
                "current_index": current,
                "correct_count": correct_count,
                "question_ids": question_ids,
                "answered_questions": answered,
            },
            {
                "$set": {
                    "status": "finished",
                    "end_time": now,
                    "updated_at": now.isoformat(),
                    "updated_at_dt": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if finished is not None:
            return finished

        # Lost response or a concurrent terminal transition: only an exact
        # completed ``finished`` state is idempotent success. If it remained
        # in-progress, revalidate and surface conflict instead of guessing.
        latest = collection.find_one(owner_filter)
        if latest is None:
            return None
        if latest.get("status") == "finished":
            _completion_snapshot(latest)
            return latest
        if latest.get("status") == "in_progress":
            _completion_snapshot(latest)
            raise QuizSessionCompletionInvalid(
                "completed quiz session could not be atomically closed"
            )
        return None
    except (QuizSessionCompletionInvalid, QuizSessionCompletionStoreUnavailable):
        raise
    except PyMongoError as exc:
        logger.exception("failed to finish completed quiz session %s", session_id)
        raise QuizSessionCompletionStoreUnavailable(
            "quiz session completion write failed"
        ) from exc
