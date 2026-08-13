"""Restart-safe lookup for retry-error practice source questions.

The legacy final-result button carries only the user id. After a process restart
its in-memory result card is gone, but the finished quiz session still contains
an exact persisted answer ledger. Resolve that ledger against the timestamp of
the Telegram result-menu message so an old button cannot silently select a newer
quiz completed afterwards.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pymongo import DESCENDING
from pymongo.errors import PyMongoError

from legacy_session_recovery import (
    LegacyPersistedSessionModeInvalid,
    completed_result_inputs,
)


class LegacyRetrySourceUnavailable(RuntimeError):
    """The persisted retry source cannot be read from MongoDB."""


class LegacyRetrySourceInvalid(RuntimeError):
    """A candidate retry source contradicts the durable completed-session schema."""


@dataclass(frozen=True)
class RetrySource:
    session_id: str
    level_name: str
    questions: tuple[dict, ...]


def _database():
    import database

    return database


def _collection():
    collection = getattr(_database(), "quiz_sessions_collection", None)
    if collection is None:
        raise LegacyRetrySourceUnavailable("quiz session collection is unavailable")
    return collection


def _message_cutoff(message_date: datetime) -> datetime:
    """Return the exclusive UTC second after one Telegram message timestamp."""
    if not isinstance(message_date, datetime):
        raise ValueError("message_date must be a datetime")
    if message_date.tzinfo is None:
        normalized = message_date
    else:
        normalized = message_date.astimezone(UTC).replace(tzinfo=None)
    return normalized.replace(microsecond=0) + timedelta(seconds=1)


def _retry_questions(session: dict) -> tuple[dict, ...]:
    try:
        recovered = completed_result_inputs(session)
    except LegacyPersistedSessionModeInvalid as exc:
        raise LegacyRetrySourceInvalid(
            "retry source has unsupported persisted quiz mode"
        ) from exc
    if not isinstance(recovered, dict):
        raise LegacyRetrySourceInvalid(
            "retry source does not contain an exact completed answer ledger"
        )
    data = recovered.get("data")
    answered = data.get("answered_questions") if isinstance(data, dict) else None
    if not isinstance(answered, list):
        raise LegacyRetrySourceInvalid("retry source answer ledger is invalid")

    wrong: list[dict] = []
    for item in answered:
        if not isinstance(item, dict):
            raise LegacyRetrySourceInvalid("retry source answer entry is invalid")
        if item.get("is_correct") is False:
            question = item.get("question_obj")
            if not isinstance(question, dict):
                raise LegacyRetrySourceInvalid(
                    "retry source wrong answer has no question snapshot"
                )
            wrong.append(dict(question))
    return tuple(wrong)


def load_retry_source_for_result_message(
    *,
    user_id: int | str,
    chat_id: int,
    message_date: datetime,
) -> RetrySource | None:
    """Load the finished quiz that produced a result-menu message.

    Result menus are sent only after durable finalization. Telegram timestamps are
    second-granularity, while Mongo ``end_time`` can contain microseconds, so the
    lookup uses the exclusive start of the next second. Any quiz completed later
    than the menu message is therefore excluded, including when an old button is
    clicked after a newer quiz has finished.
    """
    if isinstance(chat_id, bool) or not isinstance(chat_id, int):
        raise ValueError("chat_id must be an integer")

    database = _database()
    cutoff = _message_cutoff(message_date)
    query = {
        "user_id": database._uid(user_id),
        "chat_id": chat_id,
        "status": "finished",
        "end_time": {"$lt": cutoff},
    }
    try:
        session = _collection().find_one(
            query,
            sort=[("end_time", DESCENDING), ("_id", DESCENDING)],
        )
    except PyMongoError as exc:
        raise LegacyRetrySourceUnavailable("retry source lookup failed") from exc

    if session is None:
        return None
    if not isinstance(session, dict):
        raise LegacyRetrySourceInvalid("retry source lookup returned invalid data")

    session_id = session.get("_id")
    if not isinstance(session_id, str) or not session_id:
        raise LegacyRetrySourceInvalid("retry source has no durable session id")
    if session.get("status") != "finished":
        raise LegacyRetrySourceInvalid("retry source is not finished")

    level_name = session.get("level_name")
    if not isinstance(level_name, str) or not level_name.strip():
        level_name = "Тест"

    return RetrySource(
        session_id=session_id,
        level_name=level_name,
        questions=_retry_questions(session),
    )
