"""Narrow test-only seams shared by legacy regression modules."""
from __future__ import annotations

import pytest

_DOWNSTREAM_FINALIZER_MODULES = frozenset(
    {
        "test_legacy_result_finalize",
        "test_legacy_result_finalize_badges",
        "test_legacy_result_snapshot_strictness",
        "test_legacy_result_timestamps",
    }
)


@pytest.fixture(autouse=True)
def _legacy_finalizer_policy_session_preflight(request, monkeypatch):
    """Keep downstream result-policy suites below the session-proof boundary.

    `test_legacy_result_preflight.py` owns the real pre-scoring completion guard,
    while `test_legacy_session_close.py` owns the durable ledger proof itself.
    The modules listed here test later bonus, weekly, timestamp, snapshot,
    achievement and terminal-close behavior and therefore receive only this
    narrow preflight stub.
    """
    module_name = request.module.__name__.rsplit(".", 1)[-1]
    if module_name in _DOWNSTREAM_FINALIZER_MODULES:
        import legacy_result_finalize as finalize

        monkeypatch.setattr(finalize, "_preflight_recovery_session", lambda **_: {})
