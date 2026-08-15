"""Fail-closed access to persisted legacy quiz sessions."""
from __future__ import annotations

import time
import uuid

from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError, OperationFailure, PyMongoError

from legacy_session_spec import validated_session_spec

_ACTIVE_INDEX = "uniq_active_quiz_user"
_ACTIVE_FILTER = {"status": "in_progress"}


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


def find_duplicate_active_session_users(limit: int = 50) -> list[dict]:
    """Read-only preflight for legacy users with multiple active sessions."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")
    try:
        rows = _collection().aggregate(
            [
                {"$match": {"status": "in_progress"}},
                {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
                {"$match": {"count": {"$gt": 1}}},
                {"$sort": {"count": -1, "_id": 1}},
                {"$limit": limit},
            ]
        )
        return [
            {"user_id": row.get("_id"), "count": row.get("count")}
            for row in rows
        ]
    except PyMongoError as exc:
        raise QuizSessionAccessUnavailable(
            "active-session duplicate preflight failed"
        ) from exc


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
    except OperationFailure as exc:
        if exc.code == 11000:
            raise QuizSessionAccessSchemaInvalid(
                "duplicate active sessions prevent unique-index creation"
            ) from exc
        raise QuizSessionAccessUnavailable(
            "active-session unique-index preparation failed"
        ) from exc
    except PyMongoError as exc:
        raise QuizSessionAccessUnavailable(
            "active-session unique-index preparation failed"
        ) from exc


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
    is_retry: bool = False,
) -> dict:
    """Create one durable session/attempt or raise; never return phantom success."""
    spec = validated_session_spec(
        mode=mode,
        question_ids=question_ids,
        questions_data=questions_data,
        level_key=level_key,
        level_name=level_name,
        time_limit=time_limit,
        chat_id=chat_id,
        is_retry=is_retry,
    )

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
        "attempt_id": session_id,
        "status": "in_progress",
        **spec,
        "current_index": 0,
        "correct_count": 0,
        "answered_questions": [],
        "question_sent_at": None,
        "start_time": time.time(),
        "started_at": now.isoformat(),
        "created_at": now,
        "updated_at": now.isoformat(),
        "updated_at_dt": now,
    }
    try:
        write = collection.insert_one(doc)
        if getattr(write, "acknowledged", True) is not True:
            raise QuizSessionAccessUnavailable(
                "quiz session insert was not acknowledged"
            )
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
    except QuizSessionAccessUnavailable:
        raise
    except PyMongoError as exc:
        raise QuizSessionAccessUnavailable("quiz session creation failed") from exc


def get_active_quiz_session_strict(user_id: int | str) -> dict | None:
    """Return the only active session, rejecting ambiguous legacy duplicates."""
    database = _database()
    query = {"user_id": database._uid(user_id), "status": "in_progress"}
    try:
        active = list(_collection().find(query).limit(2))
    except PyMongoError as exc:
        raise QuizSessionAccessUnavailable(
            "active quiz session lookup failed"
        ) from exc
    if len(active) > 1:
        raise QuizSessionAccessSchemaInvalid(
            "multiple active quiz sessions exist for one user"
        )
    return active[0] if active else None


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
