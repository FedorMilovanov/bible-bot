"""Canonical access to process-local quiz runtime mirrors.

Mongo remains the durable authority for active quiz attempts. This module only
exposes the exact in-process dictionaries already created by the transitional
legacy layer so focused production controllers do not depend on controller
wiring or import ``bot.py`` themselves.
"""
from __future__ import annotations

from collections.abc import MutableMapping


_user_data: MutableMapping | None = None
_user_locks: MutableMapping | None = None


def install_legacy_bridge(legacy_module) -> None:
    """Bind accessors to the exact transitional runtime dictionaries."""
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

    _user_data = legacy_user_data
    _user_locks = legacy_user_locks


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
