"""Crash-safe arbitration for starting persisted legacy quiz attempts.

Generic launch actions create a new durable session only when the user has no
active attempt. An existing incomplete attempt is never silently cancelled or
replaced: the controller must surface its attempt-bound resume/restart/cancel
controls. Exact-completed evidence must be finalized before another quiz starts.
Only the explicit attempt-bound restart operation is allowed to replace an
attempt in place.
"""
from __future__ import annotations

from dataclasses import dataclass

from legacy_attempt_identity import persisted_attempt_id
from legacy_restart_policy import LegacyRestartStateInvalid, classify_restart_session
from legacy_session_access import (
    QuizSessionAccessSchemaInvalid,
    QuizSessionAccessUnavailable,
    QuizSessionAlreadyActive,
    create_quiz_session_strict,
    get_active_quiz_session_strict,
)


class LegacySessionLaunchUnavailable(RuntimeError):
    """Durable launch storage is temporarily unavailable."""


class LegacySessionLaunchConflict(RuntimeError):
    """Concurrent/corrupt state prevents a safe launch decision."""


class LegacySessionLaunchResultPending(RuntimeError):
    """A completed active attempt must be finalized before starting another."""


class LegacySessionLaunchActiveAttempt(RuntimeError):
    """An incomplete active attempt must be explicitly resumed/restarted/cancelled."""

    def __init__(self, session: dict, session_id: str, attempt_id: str):
        super().__init__("an incomplete quiz attempt is already active")
        self.session = session
        self.session_id = session_id
        self.attempt_id = attempt_id


@dataclass(frozen=True)
class SessionLaunchOutcome:
    session: dict
    session_id: str
    attempt_id: str


def _outcome(session: dict) -> SessionLaunchOutcome:
    if not isinstance(session, dict):
        raise LegacySessionLaunchConflict("durable launch returned invalid session state")
    session_id = session.get("_id")
    if not isinstance(session_id, str) or not session_id:
        raise LegacySessionLaunchConflict("durable launch returned no session id")
    try:
        attempt_id = persisted_attempt_id(session)
    except ValueError as exc:
        raise LegacySessionLaunchConflict("durable launch returned invalid attempt id") from exc
    return SessionLaunchOutcome(
        session=session,
        session_id=session_id,
        attempt_id=attempt_id,
    )


def launch_quiz_attempt(
    *,
    user_id: int | str,
    mode: str,
    question_ids: list,
    questions_data: list,
    level_key: str | None = None,
    level_name: str | None = None,
    time_limit: int | None = None,
    chat_id: int | None = None,
) -> SessionLaunchOutcome:
    """Create a durable attempt only when no active quiz evidence exists."""
    try:
        active = get_active_quiz_session_strict(user_id)
    except QuizSessionAccessSchemaInvalid as exc:
        raise LegacySessionLaunchConflict(
            "active quiz session state is ambiguous"
        ) from exc
    except QuizSessionAccessUnavailable as exc:
        raise LegacySessionLaunchUnavailable("active session lookup failed") from exc

    if active is not None:
        if not isinstance(active, dict):
            raise LegacySessionLaunchConflict("active session snapshot is invalid")
        try:
            decision = classify_restart_session(active)
            attempt_id = persisted_attempt_id(active)
        except (LegacyRestartStateInvalid, ValueError) as exc:
            raise LegacySessionLaunchConflict("active session evidence is contradictory") from exc
        session_id = active.get("_id")
        if not isinstance(session_id, str) or not session_id:
            raise LegacySessionLaunchConflict("active session id is invalid")

        if decision.action == "finalize":
            raise LegacySessionLaunchResultPending(
                "completed quiz result must be finalized before starting another quiz"
            )
        if decision.action == "resume":
            raise LegacySessionLaunchActiveAttempt(active, session_id, attempt_id)
        raise LegacySessionLaunchConflict("active session cannot be safely classified")

    try:
        created = create_quiz_session_strict(
            user_id=user_id,
            mode=mode,
            question_ids=question_ids,
            questions_data=questions_data,
            level_key=level_key,
            level_name=level_name,
            time_limit=time_limit,
            chat_id=chat_id,
        )
    except QuizSessionAlreadyActive as exc:
        # A concurrent launch appeared between read and create. Do not decide
        # which competing request should win or replace the winner.
        raise LegacySessionLaunchConflict(
            "another quiz launch won the active-session race"
        ) from exc
    except QuizSessionAccessSchemaInvalid as exc:
        raise LegacySessionLaunchConflict("quiz session schema prevents launch") from exc
    except QuizSessionAccessUnavailable as exc:
        raise LegacySessionLaunchUnavailable("quiz session creation failed") from exc
    return _outcome(created)
