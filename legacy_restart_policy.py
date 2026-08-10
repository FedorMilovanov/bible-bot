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


def classify_restart_session(session: dict) -> RestartDecision:
    """Classify one owned active session as resume/finalize/conflict.

    Exact completion is never treated as a stale session to cancel. It may be
    finalized only when the strict persisted-result validator accepts the full
    question/answer/timestamp/score evidence. Index overrun and malformed state
    are explicit conflicts rather than guessed completion.
    """
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
