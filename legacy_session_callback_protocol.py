"""Compact attempt-bound protocol for resume/restart/cancel session buttons."""
from __future__ import annotations

import hmac
import uuid

from legacy_callback_protocol import session_callback_token

_ACTIONS = frozenset({"res", "rst", "can"})
_MAX_CALLBACK_BYTES = 64


def _session_uuid(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("session_id is required")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise ValueError("session_id must be a UUID") from exc
    canonical = str(parsed)
    if canonical != value.lower():
        raise ValueError("session_id must use canonical UUID form")
    return canonical


def build_session_action_callback(action: str, session_id: str, attempt_id: str) -> str:
    """Build ``action:session_uuid:attempt_token`` within Telegram's 64-byte limit."""
    if action not in _ACTIONS:
        raise ValueError("unsupported session callback action")
    session_id = _session_uuid(session_id)
    token = session_callback_token(attempt_id)
    payload = f"{action}:{session_id}:{token}"
    if len(payload.encode("utf-8")) > _MAX_CALLBACK_BYTES:
        raise ValueError("session callback payload is too long")
    return payload


def parse_session_action_callback(payload: str, expected_action: str) -> tuple[str, str]:
    """Parse one lifecycle callback into ``(session_id, attempt_token)``."""
    if expected_action not in _ACTIONS:
        raise ValueError("unsupported session callback action")
    if not isinstance(payload, str) or not payload:
        raise ValueError("session callback payload is required")
    if len(payload.encode("utf-8")) > _MAX_CALLBACK_BYTES:
        raise ValueError("session callback payload is too long")
    parts = payload.split(":")
    if len(parts) != 3 or parts[0] != expected_action:
        raise ValueError("malformed session callback")
    session_id = _session_uuid(parts[1])
    token = parts[2]
    if len(token) != 12 or any(
        ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        for ch in token
    ):
        raise ValueError("malformed session attempt token")
    return session_id, token


def callback_matches_attempt(token: str, attempt_id: str) -> bool:
    if not isinstance(token, str):
        return False
    try:
        expected = session_callback_token(attempt_id)
    except ValueError:
        return False
    return hmac.compare_digest(token, expected)
