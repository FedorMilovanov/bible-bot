"""Attempt-bound facade over crash-safe legacy result finalization.

The underlying result finalizer proves owner/completion/score/mode before writes.
This facade adds the lifecycle invariant introduced by atomic in-place restart:
the runtime result must belong to the exact logical ``attempt_id`` currently
stored in the session container. Only then may the existing scoring pipeline run.
"""
from __future__ import annotations

from collections.abc import Mapping

from legacy_attempt_identity import persisted_attempt_id, runtime_attempt_id
from legacy_result_finalize import (
    LegacyResultFinalizationPending,
    finalize_challenge_result as _finalize_challenge_result,
    finalize_normal_result as _finalize_normal_result,
)
from legacy_session_close import (
    QuizSessionCompletionInvalid,
    QuizSessionCompletionStoreUnavailable,
    validate_completed_owned_quiz_session,
)


class LegacyAttemptFinalizationPending(LegacyResultFinalizationPending):
    """The logical attempt cannot yet be proven safe for result finalization."""


def _prove_current_attempt(user_id: int, data: dict) -> str:
    session_id = data.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        raise LegacyAttemptFinalizationPending("result session id is missing")
    try:
        expected_attempt_id = runtime_attempt_id(data)
    except ValueError as exc:
        raise LegacyAttemptFinalizationPending("runtime attempt identity is invalid") from exc
    if expected_attempt_id is None:
        raise LegacyAttemptFinalizationPending("runtime attempt identity is missing")

    try:
        session = validate_completed_owned_quiz_session(session_id, user_id)
    except (QuizSessionCompletionInvalid, QuizSessionCompletionStoreUnavailable) as exc:
        raise LegacyAttemptFinalizationPending("durable completion proof is retryable") from exc
    if not isinstance(session, dict):
        raise LegacyAttemptFinalizationPending("durable completed session is unavailable")
    try:
        durable_attempt_id = persisted_attempt_id(session)
    except ValueError as exc:
        raise LegacyAttemptFinalizationPending("durable attempt identity is invalid") from exc
    if durable_attempt_id != expected_attempt_id:
        raise LegacyAttemptFinalizationPending(
            "runtime result belongs to a different durable quiz attempt"
        )

    memoized_result_id = data.get("result_id")
    if memoized_result_id is not None:
        expected_result_id = f"quiz:{expected_attempt_id}"
        if not isinstance(memoized_result_id, str) or memoized_result_id != expected_result_id:
            raise LegacyAttemptFinalizationPending(
                "runtime result receipt identity belongs to another quiz attempt"
            )
    return expected_attempt_id


def finalize_normal_result(
    *,
    user_id: int,
    data: dict,
    score: int,
    total: int,
    time_seconds: float,
    achievement_rewards: Mapping[str, int],
) -> dict:
    if data.get("is_retry"):
        # Memory-only error review intentionally has no durable attempt.
        return _finalize_normal_result(
            user_id=user_id,
            data=data,
            score=score,
            total=total,
            time_seconds=time_seconds,
            achievement_rewards=achievement_rewards,
        )
    _prove_current_attempt(user_id, data)
    return _finalize_normal_result(
        user_id=user_id,
        data=data,
        score=score,
        total=total,
        time_seconds=time_seconds,
        achievement_rewards=achievement_rewards,
    )


def finalize_challenge_result(
    *,
    user_id: int,
    data: dict,
    score: int,
    total: int,
    time_seconds: float,
    achievement_rewards: Mapping[str, int],
) -> dict:
    _prove_current_attempt(user_id, data)
    return _finalize_challenge_result(
        user_id=user_id,
        data=data,
        score=score,
        total=total,
        time_seconds=time_seconds,
        achievement_rewards=achievement_rewards,
    )
