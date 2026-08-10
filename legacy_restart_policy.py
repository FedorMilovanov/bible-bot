"""Pure restart policy for persisted legacy quiz sessions."""
from __future__ import annotations

from dataclasses import dataclass

from legacy_session_recovery import (
    LegacyPersistedSessionModeInvalid,
    completed_result_inputs,
    recovery_fields,
)


class LegacyRestartStateInvalid(RuntimeError):
    """Persisted restart evidence is contradictory or incomplete."""


@dataclass(frozen=True)
class RestartDecision:
    action: str
    current_index: int
    total: int
    result_inputs: dict | None = None


@dataclass(frozen=True)
class RestartTimeoutRoute:
    route: str
    time_limit: int | None


def _validate_partial_ledger(session: dict, current: int, total: int) -> None:
    question_ids = session.get("question_ids")
    answered = session.get("answered_questions")
    correct_count = session.get("correct_count")
    if (
        not isinstance(question_ids, list)
        or len(question_ids) != total
        or any(not isinstance(qid, str) or not qid for qid in question_ids)
        or not isinstance(answered, list)
        or len(answered) != current
        or isinstance(correct_count, bool)
        or not isinstance(correct_count, int)
        or correct_count < 0
        or correct_count > current
    ):
        raise LegacyRestartStateInvalid("partial restart answer ledger is inconsistent")

    durable_correct = 0
    for index, item in enumerate(answered):
        if not isinstance(item, dict) or not isinstance(item.get("is_correct"), bool):
            raise LegacyRestartStateInvalid("partial restart answer entry is invalid")
        stored_index = item.get("index", index)
        if (
            isinstance(stored_index, bool)
            or not isinstance(stored_index, int)
            or stored_index != index
            or item.get("qid") != question_ids[index]
            or not isinstance(item.get("user_answer"), str)
            or not isinstance(item.get("question_obj"), dict)
        ):
            raise LegacyRestartStateInvalid("partial restart answer entry is invalid")
        durable_correct += int(item["is_correct"])
    if durable_correct != correct_count:
        raise LegacyRestartStateInvalid(
            "partial restart correct_count contradicts answer ledger"
        )


def classify_restart_session(session: dict) -> RestartDecision:
    """Classify one owned active session as resume/finalize/conflict."""
    if not isinstance(session, dict) or session.get("status") != "in_progress":
        raise LegacyRestartStateInvalid("restart session is not in progress")

    questions = session.get("questions_data")
    if not isinstance(questions, list) or not questions:
        raise LegacyRestartStateInvalid("restart session has no durable questions")
    total = len(questions)
    current = session.get("current_index", 0)
    if isinstance(current, bool) or not isinstance(current, int) or current < 0:
        raise LegacyRestartStateInvalid("restart current_index is invalid")

    if current < total:
        _validate_partial_ledger(session, current, total)
        try:
            recovery_fields(session)
        except LegacyPersistedSessionModeInvalid as exc:
            raise LegacyRestartStateInvalid("restart session mode is invalid") from exc
        return RestartDecision("resume", current, total)

    if current > total:
        raise LegacyRestartStateInvalid("restart current_index exceeds question count")

    try:
        result_inputs = completed_result_inputs(session)
    except LegacyPersistedSessionModeInvalid as exc:
        raise LegacyRestartStateInvalid("restart session mode is invalid") from exc
    if result_inputs is None:
        raise LegacyRestartStateInvalid(
            "completed restart session lacks consistent scoring evidence"
        )
    return RestartDecision("finalize", current, total, result_inputs)


def restart_timeout_route(session: dict) -> RestartTimeoutRoute:
    """Route recovered timers through normal vs Challenge semantics correctly."""
    raw_limit = session.get("time_limit") if isinstance(session, dict) else None
    if raw_limit is not None and (
        isinstance(raw_limit, bool)
        or not isinstance(raw_limit, int)
        or raw_limit <= 0
    ):
        raise LegacyRestartStateInvalid("restart time_limit is invalid")

    try:
        fields = recovery_fields(session)
    except LegacyPersistedSessionModeInvalid as exc:
        raise LegacyRestartStateInvalid("restart session mode is invalid") from exc

    if fields["is_challenge"]:
        limit = fields.get("challenge_time_limit")
        if limit is None:
            return RestartTimeoutRoute("none", None)
        return RestartTimeoutRoute("challenge", limit)

    limit = fields.get("quiz_time_limit")
    if limit is None:
        return RestartTimeoutRoute("none", None)
    return RestartTimeoutRoute("normal", limit)
