"""Canonical process-local quiz runtime state and session projection.

Mongo remains the durable authority for active quiz attempts. This module owns
only process-local mirrors used to render and serialize the live Telegram flow.
The dictionaries exist independently of ``bot.py``; the transitional legacy
bridge can migrate pre-existing entries into them exactly once and then points
legacy references at these canonical objects.
"""
from __future__ import annotations

import asyncio
from collections.abc import MutableMapping


_user_data: MutableMapping = {}
_user_locks: MutableMapping = {}
_bad_input_counts: MutableMapping = {}
_legacy_bridge_installed = False


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


def _validate_mapping(name: str, value) -> MutableMapping:
    if not isinstance(value, dict):
        raise TypeError(f"legacy module must expose a {name} dict")
    return value


def _validate_migration(name: str, canonical: MutableMapping, legacy: MutableMapping) -> None:
    """Reject contradictory pre-bridge RAM instead of silently choosing a winner."""
    if canonical is legacy:
        return
    for key in canonical.keys() & legacy.keys():
        left = canonical[key]
        right = legacy[key]
        if left is right:
            continue
        try:
            equal = left == right
        except Exception:
            equal = False
        if not equal:
            raise RuntimeError(f"legacy {name} conflicts with canonical runtime state")


def _merge_legacy(canonical: MutableMapping, legacy: MutableMapping) -> None:
    if canonical is legacy:
        return
    for key, value in legacy.items():
        canonical.setdefault(key, value)


def install_legacy_bridge(legacy_module) -> None:
    """Migrate transitional RAM once, then point legacy names at canonical state."""
    global _legacy_bridge_installed

    legacy_user_data = _validate_mapping("user_data", getattr(legacy_module, "user_data", None))
    legacy_user_locks = _validate_mapping("user_locks", getattr(legacy_module, "user_locks", None))
    legacy_bad_input_counts = _validate_mapping(
        "_bad_input_count", getattr(legacy_module, "_bad_input_count", None)
    )

    legacy_session_factory = getattr(legacy_module, "_create_session_data", None)
    legacy_increment_bad_input = getattr(legacy_module, "_inc_bad_input", None)
    legacy_reset_bad_input = getattr(legacy_module, "_reset_bad_input", None)
    _validate_legacy_session_factory(legacy_session_factory)
    if not callable(legacy_increment_bad_input):
        raise TypeError("legacy module must expose a callable _inc_bad_input")
    if not callable(legacy_reset_bad_input):
        raise TypeError("legacy module must expose a callable _reset_bad_input")

    if _legacy_bridge_installed:
        if legacy_user_data is not _user_data:
            raise RuntimeError("quiz runtime user_data is already installed")
        if legacy_user_locks is not _user_locks:
            raise RuntimeError("quiz runtime user_locks is already installed")
        if legacy_bad_input_counts is not _bad_input_counts:
            raise RuntimeError("quiz runtime bad-input counts are already installed")
    else:
        # Validate every mapping before mutating any of them so the migration is
        # all-or-nothing from the process-local controller's perspective.
        _validate_migration("user_data", _user_data, legacy_user_data)
        _validate_migration("user_locks", _user_locks, legacy_user_locks)
        _validate_migration("bad-input counts", _bad_input_counts, legacy_bad_input_counts)
        _merge_legacy(_user_data, legacy_user_data)
        _merge_legacy(_user_locks, legacy_user_locks)
        _merge_legacy(_bad_input_counts, legacy_bad_input_counts)
        _legacy_bridge_installed = True

    legacy_module.user_data = _user_data
    legacy_module.user_locks = _user_locks
    legacy_module._bad_input_count = _bad_input_counts
    legacy_module._create_session_data = create_session_data
    legacy_module._inc_bad_input = increment_bad_input
    legacy_module._reset_bad_input = reset_bad_input


def get_user_data() -> MutableMapping:
    """Return the canonical process-local quiz projection."""
    return _user_data


def get_user_locks() -> MutableMapping:
    """Return the canonical per-user lock mapping."""
    return _user_locks


def get_user_lock(user_id: int):
    """Return the exact per-user asyncio lock used by the runtime projection."""
    return _user_locks.setdefault(user_id, asyncio.Lock())


def get_bad_input_counts() -> MutableMapping:
    """Return the canonical process-local bad-input counter mapping."""
    return _bad_input_counts


def increment_bad_input(user_id: int) -> int:
    """Increment and return one user's process-local invalid-input count."""
    _bad_input_counts[user_id] = _bad_input_counts.get(user_id, 0) + 1
    return _bad_input_counts[user_id]


def reset_bad_input(user_id: int) -> None:
    """Drop one user's process-local invalid-input count."""
    _bad_input_counts.pop(user_id, None)
