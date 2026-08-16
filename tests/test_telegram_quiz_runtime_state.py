from pathlib import Path
from types import SimpleNamespace

import pytest

import telegram_quiz_runtime_state as runtime_state


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
RUNTIME_SOURCE = (ROOT / "telegram_quiz_runtime_state.py").read_text(encoding="utf-8")


def _clear_runtime(monkeypatch):
    monkeypatch.setattr(runtime_state, "_user_data", None)
    monkeypatch.setattr(runtime_state, "_user_locks", None)


def test_runtime_bridge_exposes_exact_legacy_mapping_identity(monkeypatch):
    _clear_runtime(monkeypatch)
    user_data = {7: {"last_activity": 1.0}}
    user_locks = {7: object()}
    legacy = SimpleNamespace(user_data=user_data, user_locks=user_locks)

    runtime_state.install_legacy_bridge(legacy)

    assert runtime_state.get_user_data() is user_data
    assert runtime_state.get_user_locks() is user_locks

    runtime_state.install_legacy_bridge(legacy)
    assert runtime_state.get_user_data() is user_data
    assert runtime_state.get_user_locks() is user_locks


def test_runtime_bridge_fails_closed_on_rebinding(monkeypatch):
    _clear_runtime(monkeypatch)
    first = SimpleNamespace(user_data={}, user_locks={})
    runtime_state.install_legacy_bridge(first)

    with pytest.raises(RuntimeError, match="user_data"):
        runtime_state.install_legacy_bridge(
            SimpleNamespace(user_data={}, user_locks=first.user_locks)
        )

    with pytest.raises(RuntimeError, match="user_locks"):
        runtime_state.install_legacy_bridge(
            SimpleNamespace(user_data=first.user_data, user_locks={})
        )


@pytest.mark.parametrize(
    "legacy",
    [
        SimpleNamespace(user_data=None, user_locks={}),
        SimpleNamespace(user_data={}, user_locks=None),
        SimpleNamespace(user_locks={}),
        SimpleNamespace(user_data={}),
    ],
)
def test_runtime_bridge_rejects_malformed_legacy_state(monkeypatch, legacy):
    _clear_runtime(monkeypatch)
    with pytest.raises(TypeError):
        runtime_state.install_legacy_bridge(legacy)


def test_runtime_access_fails_closed_before_install(monkeypatch):
    _clear_runtime(monkeypatch)

    with pytest.raises(RuntimeError, match="user_data"):
        runtime_state.get_user_data()
    with pytest.raises(RuntimeError, match="user_locks"):
        runtime_state.get_user_locks()


def test_runtime_state_is_only_process_local_projection_and_has_no_legacy_import():
    assert "Mongo remains the durable authority" in RUNTIME_SOURCE
    assert "import bot" not in RUNTIME_SOURCE
    assert "from bot" not in RUNTIME_SOURCE


def test_production_installs_runtime_bridge_and_stops_manual_userdata_wiring():
    assert "import telegram_quiz_runtime_state as quiz_runtime" in PRODUCTION_SOURCE
    assert "quiz_runtime.install_legacy_bridge(legacy)" in PRODUCTION_SOURCE
    assert "user_data=quiz.user_data" not in PRODUCTION_SOURCE
