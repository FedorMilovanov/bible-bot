"""Pure live-answer orchestration for the legacy Telegram controller.

This module deliberately contains no Telegram UI calls. It binds answer buttons
to a logical attempt scope + exact question/rendered-option slot, persists a
Mongo-backed answer before mutating in-memory runtime state, and rebuilds that
RAM state from the durable session ledger.
"""
from __future__ import annotations

import hashlib
import math
import time
import uuid
from dataclasses import dataclass

from legacy_attempt_identity import bind_runtime_attempt, runtime_attempt_id
from legacy_callback_protocol import (
    build_answer_callback,
    callback_matches_option,
    callback_matches_session,
    parse_answer_callback,
)
from session_integrity import record_owned_quiz_answer

_TIMEOUT_ANSWER = "⏱ Время вышло"


class LegacyLiveAnswerStale(RuntimeError):
    """Raised when a button/timeout no longer targets the current live question."""


class LegacyLiveStateInvalid(RuntimeError):
    """Raised when mutable or durable quiz state is internally contradictory."""


@dataclass(frozen=True)
class LiveAnswerOutcome:
    applied: bool
    persisted: bool
    question_index: int
    option_index: int | None
    question_id: str
    user_answer: str
    correct_text: str
    is_correct: bool
    latency_seconds: float | None
    current_index: int
    correct_count: int
    current_streak: int
    max_streak: int
    fastest_answer: float | None


def legacy_question_id(question: dict) -> str:
    """Match the stable question id algorithm currently used by ``bot.py``."""
    if not isinstance(question, dict):
        raise ValueError("question must be a dict")
    text = question.get("question", "")
    options = question.get("options", [])
    if not isinstance(text, str) or not isinstance(options, list) or any(
        not isinstance(option, str) for option in options
    ):
        raise ValueError("question text/options are invalid")
    return hashlib.sha256((text + "".join(options)).encode("utf-8")).hexdigest()[:12]


def ensure_callback_scope(data: dict) -> str:
    """Return logical attempt id, or memoize an in-memory review scope id."""
    if not isinstance(data, dict):
        raise ValueError("data must be a dict")
    attempt_id = runtime_attempt_id(data)
    if attempt_id is not None:
        return attempt_id
    scope = data.get("callback_scope_id")
    if isinstance(scope, str) and scope:
        return scope
    scope = f"memory:{uuid.uuid4()}"
    data["callback_scope_id"] = scope
    return scope


def build_live_answer_callback(
    prefix: str,
    data: dict,
    question_index: int,
    option_index: int,
) -> str:
    if question_index != _current_index(data):
        raise LegacyLiveAnswerStale("answer callback target is not the current question")
    question = _question_at(data, question_index)
    if isinstance(option_index, bool) or not isinstance(option_index, int) or option_index < 0:
        raise ValueError("option_index must be a non-negative integer")
    shuffled = _validated_current_options(data, question)
    if option_index >= len(shuffled):
        raise LegacyLiveAnswerStale("answer option is no longer available")
    option_text = shuffled[option_index]
    scope = ensure_callback_scope(data)
    return build_answer_callback(
        prefix,
        scope,
        question_index,
        option_index,
        option_text,
    )


def _current_index(data: dict) -> int:
    value = data.get("current_question")
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LegacyLiveStateInvalid("current_question is invalid")
    return value


def _question_at(data: dict, index: int) -> dict:
    questions = data.get("questions")
    if not isinstance(questions, list) or index < 0 or index >= len(questions):
        raise LegacyLiveAnswerStale("question index is no longer active")
    question = questions[index]
    if not isinstance(question, dict):
        raise LegacyLiveStateInvalid("question record is invalid")
    return question


def _canonical_options(question: dict) -> list[str]:
    options = question.get("options")
    if (
        not isinstance(options, list)
        or not options
        or any(not isinstance(option, str) for option in options)
    ):
        raise LegacyLiveStateInvalid("question options are invalid")
    return options


def _validated_current_options(data: dict, question: dict) -> list[str]:
    canonical = _canonical_options(question)
    shuffled = data.get("current_options")
    if not isinstance(shuffled, list) or any(not isinstance(item, str) for item in shuffled):
        raise LegacyLiveStateInvalid("current_options are invalid")
    if len(shuffled) != len(canonical) or sorted(shuffled) != sorted(canonical):
        raise LegacyLiveStateInvalid("current_options are not a permutation of question options")
    return shuffled


def _callback_option_text(question: dict, option_index: int, option_token: str) -> str:
    options = _canonical_options(question)
    if option_index >= len(options):
        raise LegacyLiveAnswerStale("answer option is no longer available")
    matches = {
        option for option in options
        if callback_matches_option(option_token, option)
    }
    if not matches:
        raise LegacyLiveAnswerStale("answer option no longer belongs to this question")
    if len(matches) != 1:
        raise LegacyLiveStateInvalid("answer option fingerprint is ambiguous")
    return next(iter(matches))


def _correct_text(question: dict, data: dict) -> str:
    options = _canonical_options(question)
    correct = question.get("correct")
    if (
        isinstance(correct, bool)
        or not isinstance(correct, int)
        or correct < 0
        or correct >= len(options)
    ):
        raise LegacyLiveStateInvalid("correct answer definition is invalid")
    expected = options[correct]
    cached = data.get("current_correct_text")
    if cached is not None and cached != expected:
        raise LegacyLiveStateInvalid("cached correct answer contradicts question")
    return expected


def _elapsed_seconds(data: dict, now: float | None) -> float | None:
    sent_at = data.get("question_sent_at")
    if sent_at is None:
        return None
    if isinstance(sent_at, bool) or not isinstance(sent_at, (int, float)):
        return None
    sent = float(sent_at)
    if not math.isfinite(sent):
        return None
    current = time.time() if now is None else now
    if isinstance(current, bool) or not isinstance(current, (int, float)):
        return None
    current = float(current)
    if not math.isfinite(current) or current < sent:
        return None
    return current - sent


def _ledger_state(session: dict) -> tuple[int, int, list[dict], int, int, float | None]:
    current = session.get("current_index")
    correct_count = session.get("correct_count")
    ledger = session.get("answered_questions")
    if (
        isinstance(current, bool)
        or not isinstance(current, int)
        or current < 0
        or isinstance(correct_count, bool)
        or not isinstance(correct_count, int)
        or correct_count < 0
        or correct_count > current
        or not isinstance(ledger, list)
        or len(ledger) != current
    ):
        raise LegacyLiveStateInvalid("durable answer counters/ledger are inconsistent")

    current_streak = 0
    max_streak = 0
    fastest: float | None = None
    ui_answers: list[dict] = []
    latency_evidence_valid = True
    score_from_ledger = 0

    for expected_index, item in enumerate(ledger):
        if not isinstance(item, dict) or not isinstance(item.get("is_correct"), bool):
            raise LegacyLiveStateInvalid("durable answer ledger entry is invalid")
        stored_index = item.get("index", expected_index)
        if (
            isinstance(stored_index, bool)
            or not isinstance(stored_index, int)
            or stored_index != expected_index
        ):
            raise LegacyLiveStateInvalid("durable answer ledger index is invalid")
        question_obj = item.get("question_obj")
        user_answer = item.get("user_answer")
        if not isinstance(question_obj, dict) or not isinstance(user_answer, str):
            raise LegacyLiveStateInvalid("durable answer payload is invalid")

        if item["is_correct"]:
            score_from_ledger += 1
            current_streak += 1
            max_streak = max(max_streak, current_streak)
        else:
            current_streak = 0

        if "latency_seconds" in item and item.get("latency_seconds") is not None:
            value = item.get("latency_seconds")
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                latency_evidence_valid = False
            else:
                latency = float(value)
                if not math.isfinite(latency) or latency < 0:
                    latency_evidence_valid = False
                elif fastest is None or latency < fastest:
                    fastest = latency

        ui_answers.append(
            {"question_obj": question_obj, "user_answer": user_answer}
        )

    if score_from_ledger != correct_count:
        raise LegacyLiveStateInvalid("durable correct_count contradicts answer ledger")
    if not latency_evidence_valid:
        fastest = None
    return current, correct_count, ui_answers, current_streak, max_streak, fastest


def _sync_ram_from_session(data: dict, session: dict) -> tuple[int, int, int, int, float | None]:
    current, correct, ui_answers, streak, max_streak, fastest = _ledger_state(session)
    bind_runtime_attempt(data, session)
    data["current_question"] = current
    data["correct_answers"] = correct
    data["answered_questions"] = ui_answers
    data["current_streak"] = streak
    data["max_streak"] = max_streak
    data["fastest_answer"] = fastest
    return current, correct, streak, max_streak, fastest


def _apply_memory_only(
    data: dict,
    *,
    question: dict,
    user_answer: str,
    is_correct: bool,
    latency_seconds: float | None,
) -> tuple[int, int, int, int, float | None]:
    answered = data.get("answered_questions")
    if not isinstance(answered, list):
        raise LegacyLiveStateInvalid("answered_questions is invalid")
    current = _current_index(data)
    correct = data.get("correct_answers", 0)
    current_streak = data.get("current_streak", 0)
    max_streak = data.get("max_streak", 0)
    for value, name in (
        (correct, "correct_answers"),
        (current_streak, "current_streak"),
        (max_streak, "max_streak"),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise LegacyLiveStateInvalid(f"{name} is invalid")

    answered.append({"question_obj": question, "user_answer": user_answer})
    data["current_question"] = current + 1
    if is_correct:
        correct += 1
        current_streak += 1
        max_streak = max(max_streak, current_streak)
    else:
        current_streak = 0
    data["correct_answers"] = correct
    data["current_streak"] = current_streak
    data["max_streak"] = max_streak

    previous_fastest = data.get("fastest_answer")
    if isinstance(previous_fastest, bool) or not isinstance(previous_fastest, (int, float)):
        previous_fastest = None
    if previous_fastest is not None:
        previous_fastest = float(previous_fastest)
        if not math.isfinite(previous_fastest) or previous_fastest < 0:
            previous_fastest = None
    fastest = previous_fastest
    if latency_seconds is not None and (fastest is None or latency_seconds < fastest):
        fastest = latency_seconds
    data["fastest_answer"] = fastest
    return data["current_question"], correct, current_streak, max_streak, fastest


def apply_live_answer_once(
    user_id: int,
    data: dict,
    payload: str,
    prefix: str,
    *,
    now: float | None = None,
) -> LiveAnswerOutcome:
    """Validate one button and durably advance before touching RAM counters."""
    token, question_index, option_index, option_token = parse_answer_callback(payload, prefix)
    scope = ensure_callback_scope(data)
    if not callback_matches_session(token, scope):
        raise LegacyLiveAnswerStale("answer button belongs to another attempt")
    if question_index != _current_index(data):
        raise LegacyLiveAnswerStale("answer button belongs to another question")

    question = _question_at(data, question_index)
    shuffled = _validated_current_options(data, question)
    if option_index >= len(shuffled):
        raise LegacyLiveAnswerStale("answer option is no longer available")
    user_answer = _callback_option_text(question, option_index, option_token)
    correct_text = _correct_text(question, data)
    is_correct = user_answer == correct_text
    latency = _elapsed_seconds(data, now)
    question_id = legacy_question_id(question)
    session_id = data.get("session_id")

    if isinstance(session_id, str) and session_id:
        result = record_owned_quiz_answer(
            session_id,
            user_id,
            expected_attempt_id=scope,
            expected_index=question_index,
            question_id=question_id,
            user_answer=user_answer,
            is_correct=is_correct,
            question_obj=question,
            latency_seconds=latency,
        )
        if not isinstance(result, dict) or not isinstance(result.get("session"), dict):
            raise LegacyLiveStateInvalid("answer store returned an invalid durable snapshot")
        current, correct, streak, max_streak, fastest = _sync_ram_from_session(
            data, result["session"]
        )
        applied = result.get("applied") is True
        persisted = True
    else:
        current, correct, streak, max_streak, fastest = _apply_memory_only(
            data,
            question=question,
            user_answer=user_answer,
            is_correct=is_correct,
            latency_seconds=latency,
        )
        applied = True
        persisted = False

    return LiveAnswerOutcome(
        applied=applied,
        persisted=persisted,
        question_index=question_index,
        option_index=option_index,
        question_id=question_id,
        user_answer=user_answer,
        correct_text=correct_text,
        is_correct=is_correct,
        latency_seconds=latency,
        current_index=current,
        correct_count=correct,
        current_streak=streak,
        max_streak=max_streak,
        fastest_answer=fastest,
    )


def apply_live_timeout_once(
    user_id: int,
    data: dict,
    expected_index: int,
    *,
    expected_attempt_id: str,
    now: float | None = None,
) -> LiveAnswerOutcome:
    """Durably record a timeout for the exact attempt/question captured at send."""
    if not isinstance(expected_attempt_id, str) or not isinstance(expected_index, int) or isinstance(expected_index, bool) or expected_index < 0:
        if not isinstance(expected_attempt_id, str) or not expected_attempt_id:
            raise ValueError("expected_attempt_id is required")
        raise ValueError("expected_index must be a non-negative integer")
    if not expected_attempt_id:
        raise ValueError("expected_attempt_id is required")

    scope = ensure_callback_scope(data)
    if scope != expected_attempt_id:
        raise LegacyLiveAnswerStale("timeout belongs to another attempt")
    if expected_index != _current_index(data):
        raise LegacyLiveAnswerStale("timeout belongs to another question")

    question = _question_at(data, expected_index)
    correct_text = _correct_text(question, data)
    latency = _elapsed_seconds(data, now)
    question_id = legacy_question_id(question)
    session_id = data.get("session_id")

    if isinstance(session_id, str) and session_id:
        result = record_owned_quiz_answer(
            session_id,
            user_id,
            expected_attempt_id=expected_attempt_id,
            expected_index=expected_index,
            question_id=question_id,
            user_answer=_TIMEOUT_ANSWER,
            is_correct=False,
            question_obj=question,
            latency_seconds=latency,
        )
        if not isinstance(result, dict) or not isinstance(result.get("session"), dict):
            raise LegacyLiveStateInvalid("timeout store returned an invalid durable snapshot")
        current, correct, streak, max_streak, fastest = _sync_ram_from_session(
            data, result["session"]
        )
        applied = result.get("applied") is True
        persisted = True
    else:
        current, correct, streak, max_streak, fastest = _apply_memory_only(
            data,
            question=question,
            user_answer=_TIMEOUT_ANSWER,
            is_correct=False,
            latency_seconds=latency,
        )
        applied = True
        persisted = False

    return LiveAnswerOutcome(
        applied=applied,
        persisted=persisted,
        question_index=expected_index,
        option_index=None,
        question_id=question_id,
        user_answer=_TIMEOUT_ANSWER,
        correct_text=correct_text,
        is_correct=False,
        latency_seconds=latency,
        current_index=current,
        correct_count=correct,
        current_streak=streak,
        max_streak=max_streak,
        fastest_answer=fastest,
    )
