"""Pure recovery policy for persisted legacy quiz sessions.

The Mongo session is the authority after a process restart. These helpers derive
runtime/scoring fields from persisted data without importing Telegram handlers.
"""
from __future__ import annotations

from datetime import datetime

from config import SPEED_MODE_TIMEOUT, TIMED_MODE_TIMEOUT

_CHALLENGE_MODES = frozenset({"random20", "hardcore20"})


def _answers(session: dict) -> list[dict]:
    value = session.get("answered_questions", [])
    return value if isinstance(value, list) else []


def session_is_complete(session: dict) -> bool:
    questions = session.get("questions_data", [])
    total = len(questions) if isinstance(questions, list) else 0
    try:
        current = max(0, int(session.get("current_index", 0) or 0))
    except (TypeError, ValueError):
        current = 0
    return total > 0 and current >= total


def _streaks(answered: list[dict]) -> tuple[int, int]:
    current = 0
    maximum = 0
    for item in answered:
        if bool(item.get("is_correct")):
            current += 1
            maximum = max(maximum, current)
        else:
            current = 0
    return current, maximum


def _normal_mode(time_limit) -> tuple[str, float, int | None]:
    if time_limit in (None, 0, ""):
        return "relaxed", 1.0, None
    try:
        limit = int(time_limit)
    except (TypeError, ValueError):
        return "relaxed", 1.0, None
    if limit == SPEED_MODE_TIMEOUT:
        return "speed", 2.0, limit
    if limit == TIMED_MODE_TIMEOUT:
        return "timed", 1.5, limit
    # Unknown legacy/custom limits must not receive an invented multiplier.
    return "timed", 1.0, limit


def persisted_result_time_seconds(session: dict) -> float | None:
    """Return result duration capped at the last persisted answer timestamp.

    This avoids counting process downtime between the last answer and recovery.
    """
    try:
        started_epoch = float(session.get("start_time"))
    except (TypeError, ValueError):
        return None
    if started_epoch < 0:
        return None

    answered = _answers(session)
    if not answered:
        return None
    last_ts = answered[-1].get("ts")
    if not isinstance(last_ts, str) or not last_ts:
        return None
    try:
        completed = datetime.fromisoformat(last_ts)
    except ValueError:
        return None

    # database._now_utc() stores naive UTC ISO timestamps, so compare with a
    # naive UTC epoch conversion instead of local-time datetime.fromtimestamp.
    if completed.tzinfo is not None:
        completed = completed.replace(tzinfo=None)
    started = datetime.utcfromtimestamp(started_epoch)
    return max(0.0, (completed - started).total_seconds())


def recovery_fields(session: dict) -> dict:
    """Build non-Telegram runtime fields from one persisted quiz session."""
    mode = str(session.get("mode") or "level")
    is_challenge = mode in _CHALLENGE_MODES
    time_limit = session.get("time_limit")
    current_streak, max_streak = _streaks(_answers(session))

    if is_challenge:
        quiz_mode = None
        score_multiplier = 1.0
        quiz_time_limit = None
        challenge_mode = mode
        challenge_time_limit = time_limit
    else:
        quiz_mode, score_multiplier, quiz_time_limit = _normal_mode(time_limit)
        challenge_mode = None
        challenge_time_limit = None

    try:
        correct_answers = max(0, int(session.get("correct_count", 0) or 0))
    except (TypeError, ValueError):
        correct_answers = 0
    try:
        current_question = max(0, int(session.get("current_index", 0) or 0))
    except (TypeError, ValueError):
        current_question = 0

    return {
        "session_id": session.get("_id"),
        "questions": session.get("questions_data", []),
        "level_name": session.get("level_name", "Тест"),
        "quiz_chat_id": session.get("chat_id"),
        "current_question": current_question,
        "answered_questions": _answers(session),
        "level_key": session.get("level_key", mode),
        "correct_answers": correct_answers,
        "start_time": session.get("start_time"),
        "is_battle": False,
        "battle_points": 0,
        "is_challenge": is_challenge,
        "challenge_mode": challenge_mode,
        "challenge_time_limit": challenge_time_limit,
        "quiz_mode": quiz_mode,
        "score_multiplier": score_multiplier,
        "quiz_time_limit": quiz_time_limit,
        "current_streak": current_streak,
        "max_streak": max_streak,
        # answer latency is not persisted by the legacy schema. None is safer
        # than manufacturing a Lightning-achievement value after restart.
        "fastest_answer": None,
        "result_pending": session_is_complete(session),
        "persisted_result_time": persisted_result_time_seconds(session),
    }
