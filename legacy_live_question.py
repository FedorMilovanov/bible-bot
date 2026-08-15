"""Question-delivery boundary for attempt-bound live quiz timers."""
from __future__ import annotations

import math
from dataclasses import dataclass

from legacy_attempt_identity import persisted_attempt_id
from legacy_live_answer import LegacyLiveAnswerStale, ensure_callback_scope
from legacy_question_timer import mark_question_sent_once


class LegacyLiveQuestionStateInvalid(RuntimeError):
    """Live question state cannot safely create a timer target."""


@dataclass(frozen=True)
class LiveQuestionTarget:
    attempt_id: str
    question_index: int


def capture_live_question_target(data: dict) -> LiveQuestionTarget:
    if not isinstance(data, dict):
        raise ValueError("data must be a dict")
    index = data.get("current_question")
    if isinstance(index, bool) or not isinstance(index, int) or index < 0:
        raise LegacyLiveQuestionStateInvalid("current_question is invalid")
    questions = data.get("questions")
    if not isinstance(questions, list) or index >= len(questions):
        raise LegacyLiveQuestionStateInvalid("current question is not renderable")
    attempt_id = ensure_callback_scope(data)
    return LiveQuestionTarget(attempt_id=attempt_id, question_index=index)


def _canonical_store_timer(
    result: dict,
    *,
    session_id: str,
    target: LiveQuestionTarget,
) -> float:
    if not isinstance(result, dict) or not isinstance(result.get("applied"), bool):
        raise LegacyLiveQuestionStateInvalid("question timer store returned invalid state")
    session = result.get("session")
    if not isinstance(session, dict) or session.get("_id") != session_id:
        raise LegacyLiveQuestionStateInvalid("question timer store returned invalid session")
    try:
        durable_attempt = persisted_attempt_id(session)
    except ValueError as exc:
        raise LegacyLiveQuestionStateInvalid("durable question timer attempt is invalid") from exc
    if durable_attempt != target.attempt_id:
        raise LegacyLiveQuestionStateInvalid("durable question timer belongs to another attempt")
    durable_index = session.get("current_index")
    if (
        isinstance(durable_index, bool)
        or not isinstance(durable_index, int)
        or durable_index != target.question_index
    ):
        raise LegacyLiveQuestionStateInvalid("durable question timer belongs to another question")
    durable_sent_at = result.get("sent_at")
    if isinstance(durable_sent_at, bool) or not isinstance(durable_sent_at, (int, float)):
        raise LegacyLiveQuestionStateInvalid("durable question timer is invalid")
    canonical = float(durable_sent_at)
    if not math.isfinite(canonical) or canonical < 0:
        raise LegacyLiveQuestionStateInvalid("durable question timer is invalid")
    return canonical


def mark_live_question_sent(
    user_id: int,
    data: dict,
    target: LiveQuestionTarget,
    *,
    sent_at: float,
) -> float:
    if not isinstance(target, LiveQuestionTarget):
        raise ValueError("target must be a LiveQuestionTarget")
    if isinstance(sent_at, bool) or not isinstance(sent_at, (int, float)):
        raise ValueError("sent_at must be a finite non-negative number")
    sent_value = float(sent_at)
    if not math.isfinite(sent_value) or sent_value < 0:
        raise ValueError("sent_at must be a finite non-negative number")
    current = capture_live_question_target(data)
    if current != target:
        raise LegacyLiveAnswerStale("question delivery target is no longer current")
    session_id = data.get("session_id")
    if isinstance(session_id, str) and session_id:
        result = mark_question_sent_once(
            session_id,
            user_id,
            expected_attempt_id=target.attempt_id,
            expected_index=target.question_index,
            sent_at=sent_value,
        )
        canonical = _canonical_store_timer(
            result,
            session_id=session_id,
            target=target,
        )
    elif session_id is None:
        canonical = sent_value
    else:
        raise LegacyLiveQuestionStateInvalid("session_id is invalid")
    if capture_live_question_target(data) != target:
        raise LegacyLiveAnswerStale("question delivery target changed before timer sync")
    data["question_sent_at"] = canonical
    return canonical
