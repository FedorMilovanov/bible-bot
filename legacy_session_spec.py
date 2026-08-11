"""Canonical validation for persisted legacy quiz-attempt specifications."""
from __future__ import annotations

from config import SPEED_MODE_TIMEOUT, TIMED_MODE_TIMEOUT

_CHALLENGE_MODES = frozenset({"random20", "hardcore20"})
_ALLOWED_MODES = frozenset({"level", *_CHALLENGE_MODES})


def validated_session_spec(
    *,
    mode: str,
    question_ids: list,
    questions_data: list,
    level_key: str | None,
    level_name: str | None,
    time_limit: int | None,
    chat_id: int | None,
) -> dict:
    if mode not in _ALLOWED_MODES:
        raise ValueError("unsupported persisted quiz session mode")
    if not isinstance(question_ids, list) or not question_ids:
        raise ValueError("quiz question ids must be a non-empty list")
    if not isinstance(questions_data, list) or not questions_data:
        raise ValueError("quiz question data must be a non-empty list")
    if len(question_ids) != len(questions_data):
        raise ValueError("question ids/data length mismatch")
    if any(not isinstance(qid, str) or not qid for qid in question_ids):
        raise ValueError("question ids must be non-empty strings")
    if any(not isinstance(question, dict) for question in questions_data):
        raise ValueError("question data must contain dictionaries")
    if level_key is not None and (not isinstance(level_key, str) or not level_key):
        raise ValueError("level_key must be a non-empty string or None")
    if level_name is not None and not isinstance(level_name, str):
        raise ValueError("level_name must be a string or None")
    if time_limit is not None and (
        isinstance(time_limit, bool)
        or not isinstance(time_limit, int)
        or time_limit <= 0
    ):
        raise ValueError("time_limit must be a positive integer or None")
    if chat_id is not None and (isinstance(chat_id, bool) or not isinstance(chat_id, int)):
        raise ValueError("chat_id must be an integer or None")

    if mode == "level":
        if not isinstance(level_key, str) or not level_key or level_key in _CHALLENGE_MODES:
            raise ValueError("level mode requires a normal level_key")
        if time_limit not in {None, TIMED_MODE_TIMEOUT, SPEED_MODE_TIMEOUT}:
            raise ValueError("level mode time_limit is not a recognized product timer")
    elif level_key not in {None, mode}:
        raise ValueError("Challenge level_key must be None or match mode")

    return {
        "mode": mode,
        "level_key": level_key,
        "level_name": level_name,
        "question_ids": list(question_ids),
        "questions_data": list(questions_data),
        "time_limit": time_limit,
        "chat_id": chat_id,
    }
