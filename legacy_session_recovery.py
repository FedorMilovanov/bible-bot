"""Pure recovery policy for persisted legacy quiz sessions.

The Mongo session is the authority after a process restart. These helpers derive
runtime/scoring fields from persisted data without importing Telegram handlers.
"""
from __future__ import annotations

import math
from datetime import UTC, datetime

from config import SPEED_MODE_TIMEOUT, TIMED_MODE_TIMEOUT
from legacy_attempt_identity import persisted_attempt_id

_CHALLENGE_MODES = frozenset({"random20", "hardcore20"})
_PERSISTED_QUIZ_MODES = frozenset({"level", *_CHALLENGE_MODES})


class LegacyPersistedSessionModeInvalid(RuntimeError):
    """Raised when persisted mode/timer evidence cannot be interpreted safely."""


class LegacyPersistedSessionStateInvalid(LegacyPersistedSessionModeInvalid):
    """Raised when persisted counters/identity carry contradictory types."""


def _answers(session: dict) -> list[dict]:
    value = session.get("answered_questions", [])
    return value if isinstance(value, list) else []


def _strict_nonnegative_int(value, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LegacyPersistedSessionStateInvalid(f"persisted {field} is invalid")
    return value


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
    raw_started = session.get("start_time")
    if isinstance(raw_started, bool) or not isinstance(raw_started, (int, float)):
        return None
    started_epoch = float(raw_started)
    if not math.isfinite(started_epoch) or started_epoch < 0:
        return None
    try:
        started = datetime.utcfromtimestamp(started_epoch)
    except (OSError, OverflowError, ValueError):
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
    """Return duration bounded by the persisted answer chronology."""
    timeline = _persisted_answer_timeline(session)
    if timeline is None:
        return None
    started, answer_times = timeline
    return (answer_times[-1] - started).total_seconds()


def persisted_completed_at(session: dict) -> str | None:
    """Return the final persisted answer timestamp normalized to naive UTC ISO."""
    timeline = _persisted_answer_timeline(session)
    if timeline is None:
        return None
    _started, answer_times = timeline
    return answer_times[-1].isoformat()


def recovery_fields(session: dict) -> dict:
    """Build non-Telegram runtime fields from one persisted quiz session."""
    mode = _persisted_mode(session)
    try:
        attempt_id = persisted_attempt_id(session)
    except ValueError as exc:
        raise LegacyPersistedSessionStateInvalid(
            "persisted quiz attempt identity is invalid"
        ) from exc
    is_challenge = mode in _CHALLENGE_MODES
    time_limit = session.get("time_limit")
    answered = _answers(session)
    current_streak, max_streak = _streaks(answered)

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

    correct_answers = _strict_nonnegative_int(
        session.get("correct_count", 0),
        "correct_count",
    )
    current_question = _strict_nonnegative_int(
        session.get("current_index", 0),
        "current_index",
    )

    return {
        "session_id": session.get("_id"),
        "attempt_id": attempt_id,
        "questions": session.get("questions_data", []),
        "level_name": session.get("level_name", "Тест"),
        "quiz_chat_id": session.get("chat_id"),
        "current_question": current_question,
        "answered_questions": answered,
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
    aggregate correct counter must agree, every answer must carry the full
    persisted payload plus a boolean correctness flag, and the full timestamp
    chronology must prove the original duration and completion-time boundary.
    Any inconsistent legacy/corrupt document stays pending rather than receiving
    guessed statistics.
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
    question_ids = session.get("question_ids")
    if (
        not isinstance(answered, list)
        or len(answered) != total
        or not isinstance(question_ids, list)
        or len(question_ids) != total
        or any(not isinstance(qid, str) or not qid for qid in question_ids)
    ):
        return None

    score_from_answers = 0
    for index, item in enumerate(answered):
        if not isinstance(item, dict) or not isinstance(item.get("is_correct"), bool):
            return None
        if item.get("qid") != question_ids[index]:
            return None
        stored_index = item.get("index", index)
        if (
            isinstance(stored_index, bool)
            or not isinstance(stored_index, int)
            or stored_index != index
        ):
            return None
        if not isinstance(item.get("user_answer"), str):
            return None
        if not isinstance(item.get("question_obj"), dict):
            return None
        score_from_answers += int(item["is_correct"])

    stored_score = fields["correct_answers"]
    if stored_score != score_from_answers:
        return None

    return {
        "score": score_from_answers,
        "total": total,
        "time_seconds": float(duration),
        "completed_at": completed_at,
        "data": fields,
    }
