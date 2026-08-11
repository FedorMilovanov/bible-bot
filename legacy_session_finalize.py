"""Recovery orchestration for completed legacy quiz sessions.

A process may die after the last answer is durable in ``quiz_sessions`` but before
result scoring finishes. This module converts that persisted evidence into the
same crash-safe finalizers used by live handlers. It contains no Telegram UI.
"""
from __future__ import annotations

from collections.abc import Mapping

from legacy_attempt_finalize import finalize_challenge_result, finalize_normal_result
from legacy_retry_policy import LegacyRetryPolicyInvalid, persisted_is_retry
from legacy_session_close import (
    QuizSessionCompletionInvalid,
    QuizSessionCompletionStoreUnavailable,
    finish_completed_owned_quiz_session,
)
from legacy_session_recovery import (
    LegacyPersistedSessionModeInvalid,
    completed_result_inputs,
)

_RECOVERABLE_STATUSES = frozenset({"in_progress", "finished"})


class LegacyCompletedSessionEvidenceIncomplete(RuntimeError):
    """Raised when a completed Mongo session cannot prove safe result inputs."""


class LegacyCompletedSessionOwnerMismatch(RuntimeError):
    """Raised when recovery is attempted for another user's persisted session."""


class LegacyCompletedSessionStateInvalid(RuntimeError):
    """Raised when a cancelled/unknown Mongo session is not eligible for recovery."""


def _assert_owner(session: dict, user_id: int) -> None:
    stored = session.get("user_id")
    if stored is None or str(stored) != str(user_id):
        raise LegacyCompletedSessionOwnerMismatch("persisted session owner does not match caller")


def _assert_recoverable_status(session: dict) -> None:
    status = session.get("status")
    if status not in _RECOVERABLE_STATUSES:
        raise LegacyCompletedSessionStateInvalid(
            "persisted session status is not eligible for result recovery"
        )


def _finish_retry_practice(session: dict, user_id: int) -> dict:
    session_id = session.get("_id")
    if not isinstance(session_id, str) or not session_id:
        raise LegacyCompletedSessionEvidenceIncomplete(
            "completed retry practice has no durable session id"
        )
    try:
        finished = finish_completed_owned_quiz_session(session_id, user_id)
    except (QuizSessionCompletionInvalid, QuizSessionCompletionStoreUnavailable) as exc:
        raise LegacyCompletedSessionEvidenceIncomplete(
            "completed retry practice cannot be closed safely"
        ) from exc
    if not isinstance(finished, dict):
        raise LegacyCompletedSessionEvidenceIncomplete(
            "completed retry practice close returned no durable session"
        )
    return {
        "scored": False,
        "practice": True,
        "earned_base": 0,
        "daily_bonus": {"bonus": 0, "eligible": False, "claimed_now": False},
        "new_achievements": [],
        "session_finished": True,
    }


def finalize_completed_session(
    *,
    user_id: int,
    session: dict,
    username: str | None,
    first_name: str | None,
    achievement_rewards: Mapping[str, int],
) -> dict:
    """Finalize one completed, owner-validated persisted legacy quiz session.

    Score, total, elapsed time, logical attempt and completion timestamp are
    taken exclusively from Mongo evidence. Caller-provided identity is used only
    for display.

    Durable retry-error practice uses the same answer/completion proof and then
    closes without entering any scoring/bonus/achievement stage.
    """
    _assert_owner(session, user_id)
    _assert_recoverable_status(session)
    try:
        is_retry = persisted_is_retry(session)
    except LegacyRetryPolicyInvalid as exc:
        raise LegacyCompletedSessionEvidenceIncomplete(
            "completed session has unsupported persisted retry policy"
        ) from exc
    try:
        recovered = completed_result_inputs(session)
    except LegacyPersistedSessionModeInvalid as exc:
        raise LegacyCompletedSessionEvidenceIncomplete(
            "completed session has unsupported persisted quiz mode"
        ) from exc
    if recovered is None:
        raise LegacyCompletedSessionEvidenceIncomplete(
            "completed session lacks authoritative result timing evidence"
        )

    if is_retry:
        return _finish_retry_practice(session, user_id)

    data = dict(recovered["data"])
    data["is_retry"] = False
    data["username"] = username or ""
    data["first_name"] = first_name or "Игрок"
    data["result_completed_at"] = recovered["completed_at"]
    score = recovered["score"]
    total = recovered["total"]
    time_seconds = float(recovered["time_seconds"])

    if data.get("is_challenge"):
        return finalize_challenge_result(
            user_id=user_id,
            data=data,
            score=score,
            total=total,
            time_seconds=time_seconds,
            achievement_rewards=achievement_rewards,
        )
    return finalize_normal_result(
        user_id=user_id,
        data=data,
        score=score,
        total=total,
        time_seconds=time_seconds,
        achievement_rewards=achievement_rewards,
    )
