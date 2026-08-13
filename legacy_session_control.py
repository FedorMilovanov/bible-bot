"""Safe current-session control for command/global cancellation paths.

Legacy `/cancel`, `/reset` and the global "exit quiz" button historically called
`cancel_active_quiz_session(user_id)`, which can destroy exact-completed but not
yet scored evidence. This module resolves the current durable attempt first and
only cancels a proven incomplete attempt through the attempt-bound lifecycle CAS.
"""
from __future__ import annotations

from dataclasses import dataclass

from legacy_attempt_identity import persisted_attempt_id
from legacy_restart_policy import LegacyRestartStateInvalid, classify_restart_session
from legacy_session_access import (
    QuizSessionAccessSchemaInvalid,
    QuizSessionAccessUnavailable,
    get_active_quiz_session_strict,
)
from legacy_session_lifecycle import (
    QuizSessionLifecycleConflict,
    QuizSessionLifecycleUnavailable,
    cancel_owned_incomplete_quiz_attempt,
)


class LegacySessionControlUnavailable(RuntimeError):
    """Durable current-session state is temporarily unavailable."""


class LegacySessionResultPending(RuntimeError):
    """The active session is exactly complete and must be finalized, not reset."""


class LegacySessionControlConflict(RuntimeError):
    """The current durable session is malformed or changed during control action."""


@dataclass(frozen=True)
class CurrentSessionCancellation:
    had_active_session: bool
    cancelled_now: bool
    session_id: str | None = None
    attempt_id: str | None = None


def _validated_cancel_result(
    result: dict,
    *,
    session_id: str,
    attempt_id: str,
) -> tuple[bool, dict]:
    if not isinstance(result, dict) or not isinstance(result.get("applied"), bool):
        raise LegacySessionControlConflict(
            "session cancellation returned invalid applied state"
        )
    session = result.get("session")
    if not isinstance(session, dict) or session.get("_id") != session_id:
        raise LegacySessionControlConflict(
            "session cancellation returned invalid durable session"
        )
    try:
        durable_attempt_id = persisted_attempt_id(session)
    except ValueError as exc:
        raise LegacySessionControlConflict(
            "session cancellation returned invalid attempt identity"
        ) from exc
    if durable_attempt_id != attempt_id:
        raise LegacySessionControlConflict(
            "session cancellation returned another quiz attempt"
        )
    if session.get("status") != "cancelled":
        raise LegacySessionControlConflict(
            "session cancellation did not return cancelled durable state"
        )
    return result["applied"], session


def cancel_current_incomplete_session(user_id: int | str) -> CurrentSessionCancellation:
    """Cancel only a proven incomplete current attempt; never erase result evidence."""
    try:
        session = get_active_quiz_session_strict(user_id)
    except QuizSessionAccessSchemaInvalid as exc:
        raise LegacySessionControlConflict(
            "active quiz session state is ambiguous"
        ) from exc
    except QuizSessionAccessUnavailable as exc:
        raise LegacySessionControlUnavailable("active session lookup failed") from exc
    if session is None:
        return CurrentSessionCancellation(False, False)
    if not isinstance(session, dict):
        raise LegacySessionControlConflict("active session snapshot is invalid")

    session_id = session.get("_id")
    if not isinstance(session_id, str) or not session_id:
        raise LegacySessionControlConflict("active session id is invalid")
    try:
        attempt_id = persisted_attempt_id(session)
        decision = classify_restart_session(session)
    except (ValueError, LegacyRestartStateInvalid) as exc:
        raise LegacySessionControlConflict("active session evidence is contradictory") from exc

    if decision.action == "finalize":
        raise LegacySessionResultPending(
            "completed quiz evidence must be finalized before it can be cleared"
        )
    if decision.action != "resume":
        raise LegacySessionControlConflict("active session is not safely cancellable")

    try:
        result = cancel_owned_incomplete_quiz_attempt(
            session_id,
            user_id,
            expected_attempt_id=attempt_id,
        )
    except QuizSessionLifecycleUnavailable as exc:
        raise LegacySessionControlUnavailable("session cancellation failed") from exc
    except QuizSessionLifecycleConflict as exc:
        raise LegacySessionControlConflict("session changed during cancellation") from exc

    cancelled_now, _cancelled_session = _validated_cancel_result(
        result,
        session_id=session_id,
        attempt_id=attempt_id,
    )
    return CurrentSessionCancellation(
        True,
        cancelled_now,
        session_id=session_id,
        attempt_id=attempt_id,
    )
