"""Narrow test-only seams shared by legacy regression modules."""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _legacy_finalizer_policy_session_preflight(request, monkeypatch):
    """Keep the old orchestration suite focused below the new session proof.

    `test_legacy_result_preflight.py` owns the real pre-scoring completion guard.
    The historical `test_legacy_result_finalize.py` tests later bonus, weekly,
    achievement and close stages, so only that module receives this stub.
    """
    if request.module.__name__.endswith("test_legacy_result_finalize"):
        import legacy_result_finalize as finalize

        monkeypatch.setattr(finalize, "_preflight_recovery_session", lambda **_: {})
