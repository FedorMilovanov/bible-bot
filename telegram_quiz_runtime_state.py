"""Canonical access to process-local quiz runtime mirrors and session shape.

Mongo remains the durable authority for active quiz attempts. This module only
exposes the exact in-process dictionaries already created by the transitional
legacy layer and the process-local session projection factory. It owns no
durable quiz state.
"""
from __future__ import annotations

import asyncio
from collections.abc import MutableMapping


_user_data: MutableMapping | None = None
_user_locks: MutableMapping | None = None


def create_session_data(
    user_id: int,
    session_id: str,
    questions: list,
    level_name: str,
    chat_id: int,
    **extra_fields,
) -> dict:
    """Create the canonical process-local projection for one quiz session.

    This intentionally preserves the historical projection shape exactly while
    Mongo remains authoritative for the durable attempt.
    """
    base_data = {
        "session_id": session_id,
        "questions": questions,
        "current_question": 0,
        "answered_questions": [],
        "level_name": level_name,
        "quiz_chat_id": chat_id,
        "quiz_message_id": None,
        "processing_answer": False,
        "timer_task": None,
        "countdown_task": None,
        "question_sent_at": None,
        "current_streak": 0,
        "max_streak": 0,
    }
    base_data.update(extra_fields)
    return base_data


def _session_factory_probe_kwargs() -> dict:
    """Return deterministic fields that exercise defaults and override behavior."""
    return {
        "user_id": 7,
        "session_id": "runtime-probe-session",
        "questions": [{"question": "probe"}],
        "level_name": "Runtime probe",
        "chat_id": 700,
        "attempt_id": "runtime-probe-attempt",
        "current_question": 2,
        "answered_questions": [{"user_answer": "probe"}],
        "level_key": "runtime_probe",
        "correct_answers": 1,
        "quiz_mode": "speed",
        "score_multiplier": 1.5,
        "first_name": "Probe",
    }


def _validate_legacy_session_factory(legacy_factory) -> None:
    """Fail closed unless the transitional factory matches canonical behavior."""
    if legacy_factory is create_session_data:
        return
    if not callable(legacy_factory):
        raise TypeError("legacy module must expose a callable _create_session_data")

    try:
        legacy_projection = legacy_factory(**_session_factory_probe_kwargs())
        canonical_projection = create_session_data(**_session_factory_probe_kwargs())
    except Exception as exc:
        raise RuntimeError("legacy session factory probe failed") from exc

    if legacy_projection != canonical_projection:
        raise RuntimeError("legacy session factory drifted from canonical projection")


def install_legacy_bridge(legacy_module) -> None:
    """Bind exact runtime dictionaries and canonicalize session reconstruction."""
    global _user_data, _user_locks

    legacy_user_data = getattr(legacy_module, "user_data", None)
    legacy_user_locks = getattr(legacy_module, "user_locks", None)
    if not isinstance(legacy_user_data, dict):
        raise TypeError("legacy module must expose a user_data dict")
    if not isinstance(legacy_user_locks, dict):
        raise TypeError("legacy module must expose a user_locks dict")

    if _user_data is not None and _user_data is not legacy_user_data:
        raise RuntimeError("quiz runtime user_data is already bound to another mapping")
    if _user_locks is not None and _user_locks is not legacy_user_locks:
        raise RuntimeError("quiz runtime user_locks is already bound to another mapping")

    legacy_session_factory = getattr(legacy_module, "_create_session_data", None)
    _validate_legacy_session_factory(legacy_session_factory)

    _user_data = legacy_user_data
    _user_locks = legacy_user_locks
    legacy_module._create_session_data = create_session_data


def get_user_data() -> MutableMapping:
    """Return the installed process-local quiz projection, fail-closed if absent."""
    if _user_data is None:
        raise RuntimeError("quiz runtime user_data is not installed")
    return _user_data


def get_user_locks() -> MutableMapping:
    """Return the installed per-user lock mapping, fail-closed if absent."""
    if _user_locks is None:
        raise RuntimeError("quiz runtime user_locks is not installed")
    return _user_locks


def get_user_lock(user_id: int):
    """Return the exact per-user asyncio lock used by the runtime projection."""
    return get_user_locks().setdefault(user_id, asyncio.Lock())
