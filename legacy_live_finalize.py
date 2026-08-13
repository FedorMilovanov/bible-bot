"""Live persisted-result boundary for the legacy Telegram controller.

After answer CAS migration, Mongo is the authority for completed score, total,
elapsed time, completion timestamp and logical attempt identity. The controller
must not re-derive those values from mutable RAM. This module strict-loads the
owned session, proves the runtime attempt still matches it, reconstructs the
result from durable evidence and routes it through completed-session finalization.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from legacy_attempt_finalize import LegacyAttemptFinalizationPending
from legacy_attempt_identity import persisted_attempt_id, runtime_attempt_id
from legacy_result_finalize import LegacyResultFinalizationPending
from legacy_session_access import QuizSessionAccessUnavailable, get_quiz_session_strict
from legacy_session_finalize import (
    LegacyCompletedSessionEvidenceIncomplete,
    LegacyCompletedSessionOwnerMismatch,
    LegacyCompletedSessionStateInvalid,
    finalize_completed_session,
)
from legacy_session_recovery import LegacyPersistedSessionModeInvalid, completed_result_inputs


class LegacyLiveFinalizationPending(RuntimeError):
    """Live result cannot yet be proven/finalized safely and should be retried."""


@dataclass(frozen=True)
class LiveFinalizationOutcome:
    session_id: str
    attempt_id: str
    score: int
    total: int
    time_seconds: float
    completed_at: str
    is_challenge: bool
    result: dict


def finalize_live_persisted_attempt(
    *,
    user_id: int,
    data: dict,
    username: str | None,
    first_name: str | None,
    achievement_rewards: Mapping[str, int],
) -> LiveFinalizationOutcome:
    if not isinstance(data, dict):
        raise ValueError("data must be a dict")
    session_id = data.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        raise LegacyLiveFinalizationPending("live result has no durable session id")
    try:
        expected_attempt_id = runtime_attempt_id(data)
    except ValueError as exc:
        raise LegacyLiveFinalizationPending("live result attempt identity is invalid") from exc
    if expected_attempt_id is None:
        raise LegacyLiveFinalizationPending("live result attempt identity is missing")
    try:
        session = get_quiz_session_strict(session_id, user_id=user_id)
    except QuizSessionAccessUnavailable as exc:
        raise LegacyLiveFinalizationPending("live result session lookup failed") from exc
    if not isinstance(session, dict):
        raise LegacyLiveFinalizationPending("live result session is missing or not owned")
    try:
        durable_attempt_id = persisted_attempt_id(session)
    except ValueError as exc:
        raise LegacyLiveFinalizationPending("durable live attempt identity is invalid") from exc
    if durable_attempt_id != expected_attempt_id:
        raise LegacyLiveFinalizationPending("live result belongs to a different durable quiz attempt")
    try:
        recovered = completed_result_inputs(session)
    except LegacyPersistedSessionModeInvalid as exc:
        raise LegacyLiveFinalizationPending("durable live result evidence is invalid") from exc
    if not isinstance(recovered, dict):
        raise LegacyLiveFinalizationPending("durable live result is not exactly complete")
    try:
        result = finalize_completed_session(
            user_id=user_id,
            session=session,
            username=username,
            first_name=first_name,
            achievement_rewards=achievement_rewards,
        )
    except (
        LegacyCompletedSessionEvidenceIncomplete,
        LegacyCompletedSessionOwnerMismatch,
        LegacyCompletedSessionStateInvalid,
        LegacyAttemptFinalizationPending,
        LegacyResultFinalizationPending,
    ) as exc:
        raise LegacyLiveFinalizationPending("live result finalization is retryable") from exc
    if not isinstance(result, dict):
        raise LegacyLiveFinalizationPending("live result finalizer returned invalid state")
    recovered_data = recovered.get("data")
    is_challenge = recovered_data.get("is_challenge") is True if isinstance(recovered_data, dict) else False
    completed_at = recovered.get("completed_at")
    if not isinstance(completed_at, str) or not completed_at:
        raise LegacyLiveFinalizationPending("durable live completion time is missing")
    return LiveFinalizationOutcome(
        session_id=session_id,
        attempt_id=durable_attempt_id,
        score=int(recovered["score"]),
        total=int(recovered["total"]),
        time_seconds=float(recovered["time_seconds"]),
        completed_at=completed_at,
        is_challenge=is_challenge,
        result=result,
    )
