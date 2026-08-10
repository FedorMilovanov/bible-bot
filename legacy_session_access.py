"""Fail-closed access to persisted legacy quiz sessions."""
from __future__ import annotations

import time
import uuid

from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError, PyMongoError

_ACTIVE_INDEX = "uniq_active_quiz_user"
_ACTIVE_FILTER = {"status": "in_progress"}
_ALLOWED_MODES = frozenset({"level", "random20", "hardcore20"})


class QuizSessionAccessUnavailable(RuntimeError):
    """Mongo session access is unavailable."""


class QuizSessionAlreadyActive(RuntimeError):
    """The user already owns an active persisted quiz."""


class QuizSessionAccessSchemaInvalid(RuntimeError):
    """Persisted session/index state contradicts the expected schema."""


def _database():
    import database

    return database


def _collection():
    collection = getattr(_database(), "quiz_sessions_collection", None)
    if collection is None:
        raise QuizSessionAccessUnavailable("quiz session collection is unavailable")
    return collection


def ensure_active_session_unique_index() -> bool:
    """Ensure at most one ``in_progress`` session exists per user."""
    collection = _collection()
    try:
        existing = collection.index_information().get(_ACTIVE_INDEX)
        if existing is not None:
            if (
                existing.get("key") != [("user_id", ASCENDING)]
                or existing.get("unique") is not True
                or existing.get("partialFilterExpression") != _ACTIVE_FILTER
            ):
                raise QuizSessionAccessSchemaInvalid(
                    "active-session unique index has incompatible options"
                )
            return True
        collection.create_index(
            [("user_id", ASCENDING)],
            unique=True,
            partialFilterExpression=_ACTIVE_FILTER,
            name=_ACTIVE_INDEX,
            background=True,
        )
        return True
    except QuizSessionAccessSchemaInvalid:
        raise
    except DuplicateKeyError as exc:
        raise QuizSessionAccessSchemaInvalid(
            "duplicate active sessions prevent unique-index creation"
        ) from exc
    except PyMongoError as exc:
        raise QuizSessionAccessUnavailable(
            "active-session unique-index preparation failed"
        ) from exc


def _validate_questions(question_ids: list, questions_data: list) -> None:
    if not question_ids or not questions_data:
        raise ValueError("quiz questions must be non-empty")
    if len(question_ids) != len(questions_data):
        raise ValueError("question ids/data length mismatch")
    if any(not isinstance(qid, str) or not qid for qid in question_ids):
        raise ValueError("question ids must be non-empty strings")
    if any(not isinstance(question, dict) for question in questions_data):
        raise ValueError("question data must contain dictionaries")


def create_quiz_session_strict(
    *,
    user_id: int | str,
    mode: str,
    question_ids: list,
    questions_data: list,
    level_key: str | None = None,
    level_name: str | None = None,
    time_limit: int | None = None,
    chat_id: int | None = None,
) -> dict:
    """Create one durable session or raise; never return phantom success."""
    if mode not in _ALLOWED_MODES:
        raise ValueError("unsupported persisted quiz session mode")
    _validate_questions(question_ids, questions_data)
    if time_limit is not None and (
        isinstance(time_limit, bool)
        or not isinstance(time_limit, int)
        or time_limit <= 0
    ):
        raise ValueError("time_limit must be a positive integer or None")

    ensure_active_session_unique_index()
    database = _database()
    collection = _collection()
    now = database._now_utc()
    session_id = str(uuid.uuid4())
    uid = database._uid(user_id)
    doc = {
        "_id": session_id,
        "user_id": uid,
        "session_id": session_id,
        "status": "in_progress",
        "mode": mode,
        "level_key": level_key,
        "level_name": level_name,
        "question_ids": list(question_ids),
        "questions_data": list(questions_data),
        "current_index": 0,
        "correct_count": 0,
        "answered_questions": [],
        "time_limit": time_limit,
        "question_sent_at": None,
        "chat_id": chat_id,
        "start_time": time.time(),
        "started_at": now.isoformat(),
        "created_at": now,
        "updated_at": now.isoformat(),
        "updated_at_dt": now,
    }
    try:
        collection.insert_one(doc)
        return doc
    except DuplicateKeyError as exc:
        try:
            existing = collection.find_one(
                {"user_id": uid, "status": "in_progress"}
            )
        except PyMongoError as read_exc:
            raise QuizSessionAccessUnavailable(
                "session insert conflicted and active state cannot be confirmed"
            ) from read_exc
        if existing is not None:
            raise QuizSessionAlreadyActive(
                "user already has an active quiz session"
            ) from exc
        raise QuizSessionAccessSchemaInvalid(
            "quiz session insert hit an unexplained duplicate key"
        ) from exc
    except PyMongoError as exc:
        raise QuizSessionAccessUnavailable("quiz session creation failed") from exc


def get_active_quiz_session_strict(user_id: int | str) -> dict | None:
    """Distinguish a genuinely absent active session from Mongo outage."""
    database = _database()
    try:
        return _collection().find_one(
            {"user_id": database._uid(user_id), "status": "in_progress"}
        )
    except PyMongoError as exc:
        raise QuizSessionAccessUnavailable(
            "active quiz session lookup failed"
        ) from exc


def get_quiz_session_strict(
    session_id: str,
    *,
    user_id: int | str | None = None,
) -> dict | None:
    """Load one durable session, optionally owner-scoped, without fail-open None."""
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("session_id is required")
    database = _database()
    query = {"_id": session_id}
    if user_id is not None:
        query["user_id"] = database._uid(user_id)
    try:
        return _collection().find_one(query)
    except PyMongoError as exc:
        raise QuizSessionAccessUnavailable("quiz session lookup failed") from exc
