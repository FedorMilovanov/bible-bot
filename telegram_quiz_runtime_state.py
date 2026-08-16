"""Canonical process-local quiz runtime mirrors and session projection.

Mongo remains the durable authority for active quiz attempts. The mappings in
this module are deliberately process-local UI/runtime mirrors only. Production
owns them here directly; the legacy bridge exists solely to migrate any
pre-existing compatibility state and point ``bot.py`` at the exact same
objects.
"""
from __future__ import annotations

import asyncio
from collections.abc import MutableMapping


user_data: dict = {}
user_locks: dict = {}
bad_input_counts: dict = {}


def create_session_data(
    user_id: int,
    session_id: str,
    questions: list,
    level_name: str,
    chat_id: int,
    **extra_fields,
) -> dict:
    """Create the canonical process-local projection for one quiz session."""
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


def _assert_migration_safe(
    name: str,
    canonical: MutableMapping,
    legacy_mapping: MutableMapping,
) -> None:
    """Reject ambiguous merges before mutating any runtime mapping."""
    if canonical is legacy_mapping:
        return
    for key, legacy_value in legacy_mapping.items():
        if key not in canonical:
            continue
        canonical_value = canonical[key]
        if canonical_value is legacy_value:
            continue
        try:
            equal = canonical_value == legacy_value
        except Exception:
            equal = False
        if not equal:
            raise RuntimeError(f"legacy {name} conflicts with canonical runtime state")


def install_legacy_bridge(legacy_module) -> None:
    """Migrate compatibility state and point legacy globals at canonical mappings.

    Every shape/parity/conflict check runs before the first mutation, so a
    failed bridge cannot partially migrate one mapping and leave the others
    split. Production itself does not require this bridge once ``bot.py`` is no
    longer imported; it remains for the standalone compatibility launcher.
    """
    legacy_user_data = getattr(legacy_module, "user_data", None)
    legacy_user_locks = getattr(legacy_module, "user_locks", None)
    legacy_bad_input_counts = getattr(legacy_module, "_bad_input_count", None)
    if not isinstance(legacy_user_data, dict):
        raise TypeError("legacy module must expose a user_data dict")
    if not isinstance(legacy_user_locks, dict):
        raise TypeError("legacy module must expose a user_locks dict")
    if not isinstance(legacy_bad_input_counts, dict):
        raise TypeError("legacy module must expose a _bad_input_count dict")

    legacy_session_factory = getattr(legacy_module, "_create_session_data", None)
    legacy_increment_bad_input = getattr(legacy_module, "_inc_bad_input", None)
    legacy_reset_bad_input = getattr(legacy_module, "_reset_bad_input", None)
    _validate_legacy_session_factory(legacy_session_factory)
    if not callable(legacy_increment_bad_input):
        raise TypeError("legacy module must expose a callable _inc_bad_input")
    if not callable(legacy_reset_bad_input):
        raise TypeError("legacy module must expose a callable _reset_bad_input")

    _assert_migration_safe("user_data", user_data, legacy_user_data)
    _assert_migration_safe("user_locks", user_locks, legacy_user_locks)
    _assert_migration_safe("bad-input counts", bad_input_counts, legacy_bad_input_counts)

    if legacy_user_data is not user_data:
        user_data.update(legacy_user_data)
    if legacy_user_locks is not user_locks:
        user_locks.update(legacy_user_locks)
    if legacy_bad_input_counts is not bad_input_counts:
        bad_input_counts.update(legacy_bad_input_counts)

    legacy_module.user_data = user_data
    legacy_module.user_locks = user_locks
    legacy_module._bad_input_count = bad_input_counts
    legacy_module._create_session_data = create_session_data
    legacy_module._inc_bad_input = increment_bad_input
    legacy_module._reset_bad_input = reset_bad_input


def get_user_data() -> MutableMapping:
    """Return the canonical process-local quiz projection mapping."""
    return user_data


def get_user_locks() -> MutableMapping:
    """Return the canonical per-user lock mapping."""
    return user_locks


def get_user_lock(user_id: int):
    """Return the exact per-user asyncio lock used by the runtime projection."""
    return user_locks.setdefault(user_id, asyncio.Lock())


def get_bad_input_counts() -> MutableMapping:
    """Return the canonical process-local bad-input counter mapping."""
    return bad_input_counts


def increment_bad_input(user_id: int) -> int:
    """Increment and return one user's process-local invalid-input count."""
    bad_input_counts[user_id] = bad_input_counts.get(user_id, 0) + 1
    return bad_input_counts[user_id]


def reset_bad_input(user_id: int) -> None:
    """Drop one user's process-local invalid-input count."""
    bad_input_counts.pop(user_id, None)


__all__ = [
    "bad_input_counts",
    "create_session_data",
    "get_bad_input_counts",
    "get_user_data",
    "get_user_lock",
    "get_user_locks",
    "increment_bad_input",
    "install_legacy_bridge",
    "reset_bad_input",
    "user_data",
    "user_locks",
]
