"""Owner-scoped Mongo helpers for legacy Telegram quiz-session callbacks."""
from __future__ import annotations

import logging

from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)


class QuizSessionStoreUnavailable(RuntimeError):
    """Raised when an owner-scoped quiz-session operation cannot reach MongoDB."""


def get_owned_quiz_session(session_id: str, user_id: int) -> dict | None:
    import database

    collection = getattr(database, "quiz_sessions_collection", None)
    if collection is None:
        raise QuizSessionStoreUnavailable("quiz session collection is unavailable")
    try:
        return collection.find_one({"_id": session_id, "user_id": int(user_id)})
    except PyMongoError as exc:
        logger.exception("failed to load owned quiz session %s", session_id)
        raise QuizSessionStoreUnavailable("quiz session lookup failed") from exc


def cancel_owned_quiz_session(session_id: str, user_id: int) -> bool:
    import database

    collection = getattr(database, "quiz_sessions_collection", None)
    if collection is None:
        raise QuizSessionStoreUnavailable("quiz session collection is unavailable")
    try:
        result = collection.update_one(
            {"_id": session_id, "user_id": int(user_id), "status": "in_progress"},
            {"$set": {"status": "cancelled"}},
        )
        return result.modified_count == 1
    except PyMongoError as exc:
        logger.exception("failed to cancel owned quiz session %s", session_id)
        raise QuizSessionStoreUnavailable("quiz session cancellation failed") from exc
