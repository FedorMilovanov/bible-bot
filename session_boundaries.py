"""Server-authoritative ownership primitives for legacy quiz session callbacks."""
from __future__ import annotations

import logging

from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

import database

logger = logging.getLogger(__name__)


def get_owned_quiz_session(
    session_id: str,
    user_id: int,
    *,
    require_in_progress: bool = True,
) -> dict | None:
    """Fetch a session only when it belongs to the requesting Telegram user."""
    collection = database.quiz_sessions_collection
    if collection is None or not session_id:
        return None

    predicate = {
        "_id": str(session_id),
        "user_id": database._uid(user_id),
    }
    if require_in_progress:
        predicate["status"] = "in_progress"

    try:
        return collection.find_one(predicate)
    except PyMongoError:
        logger.exception("get_owned_quiz_session failed for user=%s", user_id)
        return None


def claim_owned_quiz_session_restart(session_id: str, user_id: int) -> dict | None:
    """Atomically claim one in-progress owned session for restart.

    The previous document is returned exactly once. Concurrent/delayed restart
    callbacks cannot both cancel the same old session and create two new runs.
    """
    collection = database.quiz_sessions_collection
    if collection is None or not session_id:
        return None

    now = database._now_utc()
    predicate = {
        "_id": str(session_id),
        "user_id": database._uid(user_id),
        "status": "in_progress",
    }
    update = {
        "$set": {
            "status": "cancelled",
            "updated_at": now.isoformat(),
            "updated_at_dt": now,
            "restart_claimed_by": database._uid(user_id),
        }
    }

    try:
        return collection.find_one_and_update(
            predicate,
            update,
            return_document=ReturnDocument.BEFORE,
        )
    except PyMongoError:
        logger.exception("claim_owned_quiz_session_restart failed for user=%s", user_id)
        return None


def cancel_owned_quiz_session(session_id: str, user_id: int) -> bool:
    """Cancel only an in-progress session owned by the requesting user."""
    collection = database.quiz_sessions_collection
    if collection is None or not session_id:
        return False

    now = database._now_utc()
    try:
        result = collection.update_one(
            {
                "_id": str(session_id),
                "user_id": database._uid(user_id),
                "status": "in_progress",
            },
            {
                "$set": {
                    "status": "cancelled",
                    "updated_at": now.isoformat(),
                    "updated_at_dt": now,
                }
            },
        )
        return result.modified_count == 1
    except PyMongoError:
        logger.exception("cancel_owned_quiz_session failed for user=%s", user_id)
        return False
