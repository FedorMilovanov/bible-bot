"""Resolve legacy resume/restart/cancel callbacks against durable attempt state."""
from __future__ import annotations

from dataclasses import dataclass

from legacy_attempt_identity import persisted_attempt_id
from legacy_restart_policy import RestartDecision, classify_restart_session
from legacy_session_access import (
    QuizSessionAccessUnavailable,
    get_quiz_session_strict,
)
from legacy_session_callback_protocol import (
    build_session_action_callback,
    callback_matches_attempt,
    parse_session_action_callback,
)


class LegacySessionActionStale(RuntimeError):
    """The lifecycle button no longer refers to the currently durable attempt."""


class LegacySessionActionUnavailable(RuntimeError):
    """Durable session state cannot currently be resolved."""


@dataclass(frozen=True)
class ResolvedSessionAction:
    action: str
    session_id: str
    attempt_id: str
    session: dict
    decision: RestartDecision


def session_action_payloads(session: dict) -> dict[str, str]:
    """Build resume/restart/cancel callbacks for one exact durable attempt."""
    if not isinstance(session, dict):
        raise ValueError("session must be a dict")
    session_id = session.get("_id")
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("session id is missing")
    try:
        attempt_id = persisted_attempt_id(session)
    except ValueError as exc:
        raise ValueError("session attempt identity is invalid") from exc
    return {
        action: build_session_action_callback(action, session_id, attempt_id)
        for action in ("res", "rst", "can")
    }


def resolve_session_action(
    payload: str,
    action: str,
    user_id: int | str,
) -> ResolvedSessionAction:
    """Resolve a lifecycle button without trusting callback ids as authorization."""
    session_id, token = parse_session_action_callback(payload, action)
    try:
        session = get_quiz_session_strict(session_id, user_id=user_id)
    except QuizSessionAccessUnavailable as exc:
        raise LegacySessionActionUnavailable("quiz session lookup is unavailable") from exc
    if not isinstance(session, dict):
        raise LegacySessionActionStale("quiz session is missing or not owned")
    try:
        attempt_id = persisted_attempt_id(session)
    except ValueError as exc:
        raise LegacySessionActionStale("durable quiz attempt identity is invalid") from exc
    if not callback_matches_attempt(token, attempt_id):
        raise LegacySessionActionStale("session button belongs to another attempt")

    try:
        decision = classify_restart_session(session)
    except RuntimeError as exc:
        raise LegacySessionActionStale("session button targets contradictory durable state") from exc
    return ResolvedSessionAction(
        action=action,
        session_id=session_id,
        attempt_id=attempt_id,
        session=session,
        decision=decision,
    )
