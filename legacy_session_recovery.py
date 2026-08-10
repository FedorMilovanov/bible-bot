"""Pure recovery policy for persisted legacy quiz sessions.

The Mongo session is the authority after a process restart. These helpers derive
runtime/scoring fields from persisted data without importing Telegram handlers.
"""
from __future__ import annotations

from datetime import UTC, datetime

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
        if not isinstance(item, dict) or item.get("is_correct") is not True:
            current = 0
            continue
        current += 1
        maximum = max(maximum, current)
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


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _persisted_answer_timeline(session: dict) -> tuple[datetime, list[datetime]] | None:
    try:
        started_epoch = float(session.get("start_time"))
        started = datetime.utcfromtimestamp(started_epoch)
    except (TypeError, ValueError, OSError, OverflowError):
        return None
    if started_epoch < 0:
        return None

    answered = _answers(session)
    if not answered:
        return None

    timeline: list[datetime] = []
    previous = started
    for item in answered:
        if not isinstance(item, dict):
            return None
        raw_ts = item.get("ts")
        if not isinstance(raw_ts, str) or not raw_ts:
            return None
        try:
            answer_time = _naive_utc(datetime.fromisoformat(raw_ts))
        except ValueError:
            return None
        if answer_time < started or answer_time < previous:
            return None
        timeline.append(answer_time)
        previous = answer_time
    return started, timeline


def persisted_result_time_seconds(session: dict) -> float | None:
    """Return duration bounded by the persisted answer chronology.

    All answer timestamps must be parseable, non-decreasing and not precede the
    persisted start time. Offset-aware timestamps are converted to UTC instead
    of having their offset discarded. This prevents contradictory evidence from
    becoming an artificial zero-second or multi-hour result after restart.
    """
    timeline = _persisted_answer_timeline(session)
    if timeline is None:
        return None
    started, answer_times = timeline
    return (answer_times[-1] - started).total_seconds()


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


def completed_result_inputs(session: dict) -> dict | None:
    """Return authoritative scoring inputs for a completed persisted session.

    Recovery is intentionally strict. The completed index, answer ledger and
    aggregate correct counter must agree, every answer must carry a boolean
    correctness flag, and the full timestamp chronology must prove the original
    duration boundary. Any inconsistent legacy/corrupt document stays pending
    rather than receiving guessed statistics.
    """
    if not session_is_complete(session):
        return None
    fields = recovery_fields(session)
    duration = fields.get("persisted_result_time")
    if duration is None:
        return None
    questions = fields.get("questions", [])
    total = len(questions) if isinstance(questions, list) else 0
    if total <= 0:
        return None

    answered = fields.get("answered_questions", [])
    if not isinstance(answered, list) or len(answered) != total:
        return None
    if any(
        not isinstance(item, dict) or not isinstance(item.get("is_correct"), bool)
        for item in answered
    ):
        return None
    score_from_answers = sum(1 for item in answered if item["is_correct"] is True)
    try:
        stored_score = int(session.get("correct_count", 0) or 0)
    except (TypeError, ValueError):
        return None
    if stored_score != score_from_answers:
        return None

    return {
        "score": score_from_answers,
        "total": total,
        "time_seconds": float(duration),
        "data": fields,
    }
