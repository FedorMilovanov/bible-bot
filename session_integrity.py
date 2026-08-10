"""Owner-scoped Mongo helpers for legacy Telegram quiz-session callbacks."""
from __future__ import annotations

import logging

from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)


class QuizSessionStoreUnavailable(RuntimeError):
    """Raised when an owner-scoped quiz-session operation cannot reach MongoDB."""


def _quiz_session_collection():
    import database

    collection = getattr(database, "quiz_sessions_collection", None)
    if collection is None:
        raise QuizSessionStoreUnavailable("quiz session collection is unavailable")
    return collection


def get_owned_quiz_session(session_id: str, user_id: int) -> dict | None:
    collection = _quiz_session_collection()
    try:
        return collection.find_one({"_id": session_id, "user_id": int(user_id)})
    except PyMongoError as exc:
        logger.exception("failed to load owned quiz session %s", session_id)
        raise QuizSessionStoreUnavailable("quiz session lookup failed") from exc


def cancel_owned_quiz_session(session_id: str, user_id: int) -> dict | None:
    """Atomically cancel and return the caller's active session snapshot."""
    collection = _quiz_session_collection()
    try:
        return collection.find_one_and_update(
            {"_id": session_id, "user_id": int(user_id), "status": "in_progress"},
            {"$set": {"status": "cancelled"}},
            return_document=ReturnDocument.BEFORE,
        )
    except PyMongoError as exc:
        logger.exception("failed to cancel owned quiz session %s", session_id)
        raise QuizSessionStoreUnavailable("quiz session cancellation failed") from exc
