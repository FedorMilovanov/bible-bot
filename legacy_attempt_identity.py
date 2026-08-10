"""Logical attempt identity for persisted legacy quiz-session containers.

A Mongo quiz-session document is the durable container owned by one user. A
restart must not destroy that container before a replacement is durable, and it
must not reuse the same result/callback identity. ``attempt_id`` therefore names
the current logical quiz attempt inside the container. Legacy documents created
before this field existed use their document/session id as the attempt id.
"""
from __future__ import annotations


def _required_id(value, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    value = value.strip()
    if not value:
        raise ValueError(f"{field} is required")
    if len(value) > 128:
        raise ValueError(f"{field} is too long")
    return value


def persisted_attempt_id(session: dict) -> str:
    """Return the logical attempt id from one durable session snapshot."""
    if not isinstance(session, dict):
        raise ValueError("session must be a dict")
    value = session.get("attempt_id")
    if value is None:
        value = session.get("_id") or session.get("session_id")
    return _required_id(value, "attempt_id")


def runtime_attempt_id(data: dict) -> str | None:
    """Return a persisted runtime attempt id, with legacy session-id fallback."""
    if not isinstance(data, dict):
        raise ValueError("data must be a dict")
    value = data.get("attempt_id")
    if value is None:
        value = data.get("session_id")
    if value is None:
        return None
    return _required_id(value, "attempt_id")


def bind_runtime_attempt(data: dict, session: dict) -> str:
    """Bind RAM state to the exact durable attempt represented by ``session``."""
    attempt_id = persisted_attempt_id(session)
    data["attempt_id"] = attempt_id
    return attempt_id
