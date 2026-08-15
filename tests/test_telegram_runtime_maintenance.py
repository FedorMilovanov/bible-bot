import asyncio
from pathlib import Path
from types import SimpleNamespace

import telegram_runtime_maintenance as maintenance


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def test_cleanup_stale_userdata_only_drops_expired_runtime_entries():
    data = {
        1: {"last_activity": 100.0},
        2: {"last_activity": 190.0},
        3: {},
    }

    deleted = maintenance.cleanup_stale_userdata(
        data,
        stale_threshold=50.0,
        now=200.0,
    )

    assert deleted == 1
    assert data == {
        2: {"last_activity": 190.0},
        3: {},
    }


def test_cleanup_job_uses_supplied_runtime_map(monkeypatch):
    calls = []

    def fake_cleanup(user_data, *, stale_threshold):
        calls.append((user_data, stale_threshold))
        return 0

    monkeypatch.setattr(maintenance, "cleanup_stale_userdata", fake_cleanup)
    runtime = {7: {"last_activity": 1.0}}

    asyncio.run(
        maintenance.cleanup_stale_userdata_job(
            SimpleNamespace(),
            user_data=runtime,
            stale_threshold=123.0,
        )
    )

    assert calls == [(runtime, 123.0)]


def test_production_routes_runtime_gc_outside_legacy():
    assert "import telegram_runtime_maintenance as maintenance" in PRODUCTION_SOURCE
    assert "async def _cleanup_stale_userdata_job" in PRODUCTION_SOURCE
    assert "maintenance.cleanup_stale_userdata_job" in PRODUCTION_SOURCE
    assert "user_data=quiz.user_data" in PRODUCTION_SOURCE
    assert "interval=maintenance.GC_INTERVAL" in PRODUCTION_SOURCE
    assert "first=maintenance.GC_INTERVAL" in PRODUCTION_SOURCE

    assert "legacy.cleanup_stale_userdata_job" not in PRODUCTION_SOURCE
    assert "legacy.GC_INTERVAL" not in PRODUCTION_SOURCE
