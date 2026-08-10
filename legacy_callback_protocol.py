"""Compact callback protocol for legacy inline quiz answers.

Telegram buttons can outlive both the question they were rendered for and the
in-memory process that created them. Answer callbacks therefore carry a compact
session fingerprint plus the exact question and option indexes. The fingerprint
is not authorization; Mongo owner/session CAS remains the authority. Its job is
to make stale UI from another question/session unambiguous before mutation.
"""
from __future__ import annotations

import base64
import hashlib
import hmac

_ALLOWED_PREFIXES = frozenset({"qa", "cha"})
_TOKEN_BYTES = 9  # 72-bit fingerprint -> 12 base64url characters
_MAX_CALLBACK_BYTES = 64


def _index(value, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def session_callback_token(session_id: str) -> str:
    """Return a deterministic compact fingerprint for one persisted session id."""
    if not isinstance(session_id, str) or not session_id.strip():
        raise ValueError("session_id is required")
    digest = hashlib.sha256(session_id.encode("utf-8")).digest()[:_TOKEN_BYTES]
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def build_answer_callback(
    prefix: str,
    session_id: str,
    question_index: int,
    option_index: int,
) -> str:
    """Build ``prefix:token:question_index:option_index`` within callback limits."""
    if prefix not in _ALLOWED_PREFIXES:
        raise ValueError("unsupported callback prefix")
    question_index = _index(question_index, "question_index")
    option_index = _index(option_index, "option_index")
    payload = (
        f"{prefix}:{session_callback_token(session_id)}:"
        f"{question_index}:{option_index}"
    )
    if len(payload.encode("utf-8")) > _MAX_CALLBACK_BYTES:
        raise ValueError("callback payload is too long")
    return payload


def parse_answer_callback(payload: str, expected_prefix: str) -> tuple[str, int, int]:
    """Parse and validate one answer callback without consulting mutable RAM."""
    if expected_prefix not in _ALLOWED_PREFIXES:
        raise ValueError("unsupported callback prefix")
    if not isinstance(payload, str) or not payload:
        raise ValueError("callback payload is required")
    if len(payload.encode("utf-8")) > _MAX_CALLBACK_BYTES:
        raise ValueError("callback payload is too long")

    parts = payload.split(":")
    if len(parts) != 4 or parts[0] != expected_prefix:
        raise ValueError("malformed answer callback")
    token = parts[1]
    if len(token) != 12 or any(ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_" for ch in token):
        raise ValueError("malformed session callback token")
    try:
        question_index = int(parts[2])
        option_index = int(parts[3])
    except ValueError as exc:
        raise ValueError("malformed answer callback indexes") from exc
    return (
        token,
        _index(question_index, "question_index"),
        _index(option_index, "option_index"),
    )


def callback_matches_session(token: str, session_id: str) -> bool:
    """Compare a parsed callback fingerprint with the current persisted session."""
    if not isinstance(token, str):
        return False
    try:
        expected = session_callback_token(session_id)
    except ValueError:
        return False
    return hmac.compare_digest(token, expected)
