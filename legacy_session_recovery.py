"""Pure recovery policy for persisted legacy quiz sessions.

The Mongo session is the authority after a process restart. These helpers derive
runtime/scoring fields from persisted data without importing Telegram handlers.
"""
from __future__ import annotations

import math
from datetime import UTC, datetime

from config import SPEED_MODE_TIMEOUT, TIMED_MODE_TIMEOUT

_CHALLENGE_MODES = frozenset({"random20", "hardcore20"})
_PERSISTED_QUIZ_MODES = frozenset({"level", *_CHALLENGE_MODES})


class LegacyPersistedSessionModeInvalid(RuntimeError):
    """Raised when persisted mode/timer evidence cannot be interpreted safely."""


def _answers(session: dict) -> list[dict]:
    value = session.get("answered_questions", [])
    return value if isinstance(value, list) else []


def session_is_complete(session: dict) -> bool:
    questions = session.get("questions_data", [])
    total = len(questions) if isinstance(questions, list) else 0
    current = session.get("current_index", 0)
    if isinstance(current, bool) or not isinstance(current, int) or current < 0:
        return False
    # Only an exact end position is valid completion evidence. An index beyond
    # the persisted question ledger is contradictory/corrupt state and must not
    # be normalized into a recoverable completed result.
    return total > 0 and current == total


def _persisted_mode(session: dict) -> str:
    mode = session.get("mode")
    if not isinstance(mode, str) or mode not in _PERSISTED_QUIZ_MODES:
        raise LegacyPersistedSessionModeInvalid(
            "persisted quiz session mode is not recognized"
        )
    return mode


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


def persisted_fastest_answer(session: dict) -> float | None:
    """Return the fastest durable latency that is safe to use for achievements.

    Legacy answer records may have no latency field at all, so missing/None
    values are treated as unknown evidence and skipped. A present malformed
    value invalidates the speed-achievement evidence for the whole recovered
    result: silently ignoring corrupt latency could manufacture a Lightning
    achievement from an incomplete ledger. Valid latencies are finite,
    non-negative real numbers; booleans are rejected explicitly.
    """
    fastest: float | None = None
    for item in _answers(session):
        if not isinstance(item, dict):
            return None
        if "latency_seconds" not in item or item.get("latency_seconds") is None:
            continue
        value = item.get("latency_seconds")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        latency = float(value)
        if not math.isfinite(latency) or latency < 0:
            return None
        fastest = latency if fastest is None else min(fastest, latency)
    return fastest


def _normal_mode(time_limit) -> tuple[str, float, int | None]:
    if time_limit is None:
        return "relaxed", 1.0, None
    if isinstance(time_limit, bool) or not isinstance(time_limit, int):
        raise LegacyPersistedSessionModeInvalid(
            "persisted normal quiz time_limit is invalid"
        )
    if time_limit == SPEED_MODE_TIMEOUT:
        return "speed", 2.0, time_limit
    if time_limit == TIMED_MODE_TIMEOUT:
        return "timed", 1.5, time_limit
    raise LegacyPersistedSessionModeInvalid(
        "persisted normal quiz time_limit is not a recognized product mode"
    )


def _challenge_timer(time_limit) -> int | None:
    if time_limit is None:
        return None
    if (
        isinstance(time_limit, bool)
        or not isinstance(time_limit, int)
        or time_limit <= 0
    ):
        raise LegacyPersistedSessionModeInvalid(
            "persisted Challenge time_limit is invalid"
        )
    return time_limit


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


def persisted_completed_at(session: dict) -> str | None:
    """Return the final persisted answer timestamp normalized to naive UTC ISO.

    A recovered result must keep the day/week when the quiz actually finished,
    not the later process-restart time. The same validated answer chronology used
    for elapsed time is therefore also the authority for result completion time.
    """
    timeline = _persisted_answer_timeline(session)
    if timeline is None:
        return None
    _started, answer_times = timeline
    return answer_times[-1].isoformat()


def recovery_fields(session: dict) -> dict:
    """Build non-Telegram runtime fields from one persisted quiz session."""
    mode = _persisted_mode(session)
    is_challenge = mode in _CHALLENGE_MODES
    time_limit = session.get("time_limit")
    current_streak, max_streak = _streaks(_answers(session))

    if is_challenge:
        quiz_mode = None
        score_multiplier = 1.0
        quiz_time_limit = None
        challenge_mode = mode
        challenge_time_limit = _challenge_timer(time_limit)
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
        "fastest_answer": persisted_fastest_answer(session),
        "result_pending": session_is_complete(session),
        "persisted_result_time": persisted_result_time_seconds(session),
        "persisted_completed_at": persisted_completed_at(session),
    }


def completed_result_inputs(session: dict) -> dict | None:
    """Return authoritative scoring inputs for a completed persisted session.

    Recovery is intentionally strict. The completed index, answer ledger and
    aggregate correct counter must agree, every answer must carry a boolean
    correctness flag, and the full timestamp chronology must prove the original
    duration and completion-time boundary. Any inconsistent legacy/corrupt
    document stays pending rather than receiving guessed statistics.
    """
    if not session_is_complete(session):
        return None
    fields = recovery_fields(session)
    duration = fields.get("persisted_result_time")
    completed_at = fields.get("persisted_completed_at")
    if duration is None or not isinstance(completed_at, str) or not completed_at:
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
    stored_score = session.get("correct_count", 0)
    if isinstance(stored_score, bool) or not isinstance(stored_score, int):
        return None
    if stored_score != score_from_answers:
        return None

    return {
        "score": score_from_answers,
        "total": total,
        "time_seconds": float(duration),
        "completed_at": completed_at,
        "data": fields,
    }
