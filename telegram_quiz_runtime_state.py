"""Canonical process-local quiz runtime mirrors and session projection.

Mongo remains the durable authority for active quiz attempts. The mappings in
this module are deliberately process-local UI/runtime mirrors only and are
owned directly by the canonical production runtime.
"""
from __future__ import annotations

import asyncio
from collections.abc import MutableMapping


user_data: dict = {}
user_locks: dict = {}
bad_input_counts: dict = {}


def create_session_data(
    user_id: int,
    session_id: str,
    questions: list,
    level_name: str,
    chat_id: int,
    **extra_fields,
) -> dict:
    """Create the canonical process-local projection for one quiz session."""
    base_data = {
        "session_id": session_id,
        "questions": questions,
        "current_question": 0,
        "answered_questions": [],
        "level_name": level_name,
        "quiz_chat_id": chat_id,
        "quiz_message_id": None,
        "processing_answer": False,
        "timer_task": None,
        "countdown_task": None,
        "question_sent_at": None,
        "current_streak": 0,
        "max_streak": 0,
    }
    base_data.update(extra_fields)
    return base_data


def get_user_data() -> MutableMapping:
    """Return the canonical process-local quiz projection mapping."""
    return user_data


def get_user_locks() -> MutableMapping:
    """Return the canonical per-user lock mapping."""
    return user_locks


def get_user_lock(user_id: int):
    """Return the exact per-user asyncio lock used by the runtime projection."""
    return user_locks.setdefault(user_id, asyncio.Lock())


def get_bad_input_counts() -> MutableMapping:
    """Return the canonical process-local bad-input counter mapping."""
    return bad_input_counts


def increment_bad_input(user_id: int) -> int:
    """Increment and return one user's process-local invalid-input count."""
    bad_input_counts[user_id] = bad_input_counts.get(user_id, 0) + 1
    return bad_input_counts[user_id]


def reset_bad_input(user_id: int) -> None:
    """Drop one user's process-local invalid-input count."""
    bad_input_counts.pop(user_id, None)


__all__ = [
    "bad_input_counts",
    "create_session_data",
    "get_bad_input_counts",
    "get_user_data",
    "get_user_lock",
    "get_user_locks",
    "increment_bad_input",
    "reset_bad_input",
    "user_data",
    "user_locks",
]
