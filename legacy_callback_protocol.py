"""Compact callback protocol for legacy inline quiz answers.

Telegram buttons can outlive both the question they were rendered for and the
in-memory process that created them. Answer callbacks therefore carry a compact
attempt fingerprint, the exact question/display-option indexes, and a compact
fingerprint of the option text rendered in that slot. The fingerprints are not
authorization; Mongo owner/attempt/index CAS remains the authority. Their job is
to make stale UI and same-question reshuffles unambiguous before mutation.
"""
from __future__ import annotations

import base64
import hashlib
import hmac

_ALLOWED_PREFIXES = frozenset({"qa", "cha"})
_TOKEN_BYTES = 9  # 72-bit attempt fingerprint -> 12 base64url characters
_OPTION_TOKEN_BYTES = 6  # 48-bit rendered-option fingerprint -> 8 characters
_MAX_CALLBACK_BYTES = 64


def _index(value, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _base64url_digest(value: str, size: int) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).digest()[:size]
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def session_callback_token(session_id: str) -> str:
    """Return a deterministic compact fingerprint for one persisted attempt id."""
    if not isinstance(session_id, str) or not session_id.strip():
        raise ValueError("session_id is required")
    return _base64url_digest(session_id, _TOKEN_BYTES)


def option_callback_token(option_text: str) -> str:
    """Fingerprint the exact answer text rendered in one callback slot."""
    if not isinstance(option_text, str):
        raise ValueError("option_text must be a string")
    return _base64url_digest(option_text, _OPTION_TOKEN_BYTES)


def build_answer_callback(
    prefix: str,
    session_id: str,
    question_index: int,
    option_index: int,
    option_text: str,
) -> str:
    """Build an attempt/question/slot/option-bound payload under Telegram's limit."""
    if prefix not in _ALLOWED_PREFIXES:
        raise ValueError("unsupported callback prefix")
    question_index = _index(question_index, "question_index")
    option_index = _index(option_index, "option_index")
    option_token = option_callback_token(option_text)
    payload = (
        f"{prefix}:{session_callback_token(session_id)}:"
        f"{question_index}:{option_index}:{option_token}"
    )
    if len(payload.encode("utf-8")) > _MAX_CALLBACK_BYTES:
        raise ValueError("callback payload is too long")
    return payload


def parse_answer_callback(
    payload: str,
    expected_prefix: str,
) -> tuple[str, int, int, str]:
    """Parse one answer callback without consulting mutable runtime state."""
    if expected_prefix not in _ALLOWED_PREFIXES:
        raise ValueError("unsupported callback prefix")
    if not isinstance(payload, str) or not payload:
        raise ValueError("callback payload is required")
    if len(payload.encode("utf-8")) > _MAX_CALLBACK_BYTES:
        raise ValueError("callback payload is too long")

    parts = payload.split(":")
    if len(parts) != 5 or parts[0] != expected_prefix:
        raise ValueError("malformed answer callback")
    token = parts[1]
    if len(token) != 12 or any(
        ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        for ch in token
    ):
        raise ValueError("malformed session callback token")
    option_token = parts[4]
    if len(option_token) != 8 or any(
        ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        for ch in option_token
    ):
        raise ValueError("malformed option callback token")
    try:
        question_index = int(parts[2])
        option_index = int(parts[3])
    except ValueError as exc:
        raise ValueError("malformed answer callback indexes") from exc
    return (
        token,
        _index(question_index, "question_index"),
        _index(option_index, "option_index"),
        option_token,
    )


def callback_matches_session(token: str, session_id: str) -> bool:
    """Compare a parsed attempt fingerprint with current persisted attempt scope."""
    if not isinstance(token, str):
        return False
    try:
        expected = session_callback_token(session_id)
    except ValueError:
        return False
    return hmac.compare_digest(token, expected)


def callback_matches_option(token: str, option_text: str) -> bool:
    """Verify that a callback slot still renders the same answer text."""
    if not isinstance(token, str):
        return False
    try:
        expected = option_callback_token(option_text)
    except ValueError:
        return False
    return hmac.compare_digest(token, expected)
