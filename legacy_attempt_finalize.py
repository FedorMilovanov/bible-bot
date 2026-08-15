"""Attempt-bound facade over crash-safe legacy result finalization.

The underlying result finalizer proves owner/completion/score/mode before writes.
This facade adds the lifecycle invariant introduced by atomic in-place restart:
the runtime result must belong to the exact logical ``attempt_id`` currently
stored in the session container. Only then may the existing scoring pipeline run.
"""
from __future__ import annotations

from collections.abc import Mapping

from legacy_attempt_identity import persisted_attempt_id, runtime_attempt_id
from legacy_learning_result_store import (
    LegacyLearningProgressUnavailable,
    apply_learning_progress_once,
)
from legacy_result_finalize import (
    LegacyResultFinalizationPending,
    finalize_challenge_result as _finalize_challenge_result,
    finalize_normal_result as _finalize_normal_result,
)
from legacy_result_flow import stable_result_id
from legacy_session_close import (
    QuizSessionCompletionInvalid,
    QuizSessionCompletionStoreUnavailable,
    finish_completed_owned_quiz_session,
    validate_completed_owned_quiz_session,
)
from questions.pool_policy import is_non_scoring_learning_pool


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


def _reload_proven_session(user_id: int, data: dict, expected_attempt_id: str) -> dict:
    """Re-read completion immediately before a learning progress write."""
    session_id = data.get("session_id")
    try:
        session = validate_completed_owned_quiz_session(str(session_id), user_id)
    except (QuizSessionCompletionInvalid, QuizSessionCompletionStoreUnavailable) as exc:
        raise LegacyAttemptFinalizationPending("learning completion proof is retryable") from exc
    if not isinstance(session, dict):
        raise LegacyAttemptFinalizationPending("learning completed session is unavailable")
    try:
        durable_attempt_id = persisted_attempt_id(session)
    except ValueError as exc:
        raise LegacyAttemptFinalizationPending("learning attempt identity is invalid") from exc
    if durable_attempt_id != expected_attempt_id:
        raise LegacyAttemptFinalizationPending(
            "learning result belongs to a different durable quiz attempt"
        )
    return session


def _finalize_learning_result(
    *,
    user_id: int,
    data: dict,
    session: dict,
    score: int,
    total: int,
) -> dict:
    """Persist non-scoring course progress and close the durable attempt safely."""
    level_key = data.get("level_key")
    question_ids = session.get("question_ids")
    if (
        not isinstance(level_key, str)
        or session.get("mode") != "level"
        or session.get("level_key") != level_key
        or session.get("correct_count") != score
        or not isinstance(question_ids, list)
        or len(question_ids) != total
    ):
        raise LegacyAttemptFinalizationPending(
            "learning result does not match durable completed session"
        )

    try:
        result_id = stable_result_id(user_id, data)
        progress = apply_learning_progress_once(
            result_id=result_id,
            user_id=user_id,
            username=data.get("username") or "",
            first_name=data.get("first_name") or "Игрок",
            level_key=level_key,
            score=score,
            total=total,
        )
        finished = finish_completed_owned_quiz_session(str(data["session_id"]), user_id)
    except (
        LegacyLearningProgressUnavailable,
        QuizSessionCompletionInvalid,
        QuizSessionCompletionStoreUnavailable,
        ValueError,
    ) as exc:
        raise LegacyAttemptFinalizationPending(
            "learning result finalization is retryable"
        ) from exc
    if not isinstance(finished, dict):
        raise LegacyAttemptFinalizationPending(
            "learning result progress was stored but session close is retryable"
        )

    return {
        "scored": False,
        "learning": True,
        "result_id": result_id,
        "learning_progress": progress,
        "earned_base": 0,
        "daily_bonus": {"bonus": 0, "eligible": False, "claimed_now": False},
        "new_achievements": [],
        "session_finished": True,
    }


def finalize_normal_result(
    *,
    user_id: int,
    data: dict,
    score: int,
    total: int,
    time_seconds: float,
    achievement_rewards: Mapping[str, int],
) -> dict:
    # Historical memory-only retry drills had no durable session. Keep that
    # compatibility path unscored, but persisted retry attempts must prove the
    # same attempt/completion boundary as every other durable result.
    attempt_id = None
    if not (data.get("is_retry") and not data.get("session_id")):
        attempt_id = _prove_current_attempt(user_id, data)

    if (
        data.get("is_retry") is not True
        and is_non_scoring_learning_pool(data.get("level_key"))
    ):
        if not isinstance(attempt_id, str) or not attempt_id:
            raise LegacyAttemptFinalizationPending(
                "learning result requires durable completion proof"
            )
        proven_session = _reload_proven_session(user_id, data, attempt_id)
        return _finalize_learning_result(
            user_id=user_id,
            data=data,
            session=proven_session,
            score=score,
            total=total,
        )

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
