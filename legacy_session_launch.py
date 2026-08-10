"""Crash-safe arbitration for starting any persisted legacy quiz attempt.

A new quiz must never be started in RAM without durable evidence. If the user
has no active session, create one strictly. If an incomplete active attempt
exists, replace it atomically in the same Mongo container with a fresh
``attempt_id``. Exact-completed evidence must be finalized before another quiz
can start. This removes the legacy family of ``cancel -> create`` crash gaps.
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
from legacy_session_lifecycle import (
    QuizSessionLifecycleConflict,
    QuizSessionLifecycleUnavailable,
    restart_owned_quiz_attempt,
)


class LegacySessionLaunchUnavailable(RuntimeError):
    """Durable launch storage is temporarily unavailable."""


class LegacySessionLaunchConflict(RuntimeError):
    """Concurrent/corrupt state prevents a safe launch decision."""


class LegacySessionLaunchResultPending(RuntimeError):
    """A completed active attempt must be finalized before starting another."""


@dataclass(frozen=True)
class SessionLaunchOutcome:
    session: dict
    session_id: str
    attempt_id: str
    created_new_container: bool
    replaced_incomplete_attempt: bool


def _outcome(
    session: dict,
    *,
    created: bool,
    replaced: bool,
) -> SessionLaunchOutcome:
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
        created_new_container=created,
        replaced_incomplete_attempt=replaced,
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
    """Create or atomically replace the user's current incomplete quiz attempt."""
    try:
        active = get_active_quiz_session_strict(user_id)
    except QuizSessionAccessUnavailable as exc:
        raise LegacySessionLaunchUnavailable("active session lookup failed") from exc

    if active is None:
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
            # which competing requested spec should win.
            raise LegacySessionLaunchConflict(
                "another quiz launch won the active-session race"
            ) from exc
        except QuizSessionAccessSchemaInvalid as exc:
            raise LegacySessionLaunchConflict("quiz session schema prevents launch") from exc
        except QuizSessionAccessUnavailable as exc:
            raise LegacySessionLaunchUnavailable("quiz session creation failed") from exc
        return _outcome(created, created=True, replaced=False)

    if not isinstance(active, dict):
        raise LegacySessionLaunchConflict("active session snapshot is invalid")
    try:
        decision = classify_restart_session(active)
        attempt_id = persisted_attempt_id(active)
    except (LegacyRestartStateInvalid, ValueError) as exc:
        raise LegacySessionLaunchConflict("active session evidence is contradictory") from exc

    if decision.action == "finalize":
        raise LegacySessionLaunchResultPending(
            "completed quiz result must be finalized before starting another quiz"
        )
    if decision.action != "resume":
        raise LegacySessionLaunchConflict("active session cannot be safely replaced")

    session_id = active.get("_id")
    if not isinstance(session_id, str) or not session_id:
        raise LegacySessionLaunchConflict("active session id is invalid")
    try:
        restarted = restart_owned_quiz_attempt(
            session_id,
            user_id,
            expected_attempt_id=attempt_id,
            mode=mode,
            question_ids=question_ids,
            questions_data=questions_data,
            level_key=level_key,
            level_name=level_name,
            time_limit=time_limit,
            chat_id=chat_id,
        )
    except QuizSessionLifecycleUnavailable as exc:
        raise LegacySessionLaunchUnavailable("quiz attempt replacement failed") from exc
    except QuizSessionLifecycleConflict as exc:
        raise LegacySessionLaunchConflict("active attempt changed during launch") from exc
    session = restarted.get("session") if isinstance(restarted, dict) else None
    return _outcome(session, created=False, replaced=True)
