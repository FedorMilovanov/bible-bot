import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

import telegram_quiz_runtime_state as runtime_state


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
CORE_SOURCE = (ROOT / "telegram_quiz_controller.py").read_text(encoding="utf-8")
RUNTIME_SOURCE = (ROOT / "telegram_quiz_runtime_state.py").read_text(encoding="utf-8")


def _historical_session_factory(
    user_id: int,
    session_id: str,
    questions: list,
    level_name: str,
    chat_id: int,
    **extra_fields,
) -> dict:
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


def _legacy(
    *,
    user_data=None,
    user_locks=None,
    bad_input_counts=None,
    factory=_historical_session_factory,
):
    counts = {} if bad_input_counts is None else bad_input_counts

    def inc_bad_input(user_id: int) -> int:
        counts[user_id] = counts.get(user_id, 0) + 1
        return counts[user_id]

    def reset_bad_input(user_id: int) -> None:
        counts.pop(user_id, None)

    return SimpleNamespace(
        user_data={} if user_data is None else user_data,
        user_locks={} if user_locks is None else user_locks,
        _bad_input_count=counts,
        _create_session_data=factory,
        _inc_bad_input=inc_bad_input,
        _reset_bad_input=reset_bad_input,
    )


def _clear_runtime(monkeypatch):
    monkeypatch.setattr(runtime_state, "_user_data", {})
    monkeypatch.setattr(runtime_state, "_user_locks", {})
    monkeypatch.setattr(runtime_state, "_bad_input_counts", {})
    monkeypatch.setattr(runtime_state, "_legacy_bridge_installed", False)


def test_session_factory_preserves_historical_projection_shape_and_overrides():
    questions = [{"question": "Кто написал послание?"}]
    answered = [{"user_answer": "Пётр"}]
    kwargs = {
        "user_id": 7,
        "session_id": "session-7",
        "questions": questions,
        "level_name": "Уровень",
        "chat_id": 700,
        "attempt_id": "attempt-7",
        "current_question": 3,
        "answered_questions": answered,
        "level_key": "level_easy",
        "correct_answers": 2,
        "quiz_mode": "speed",
        "score_multiplier": 1.5,
        "first_name": "Пётр",
    }

    canonical = runtime_state.create_session_data(**kwargs)
    historical = _historical_session_factory(**kwargs)

    assert canonical == historical
    assert canonical["questions"] is questions
    assert canonical["answered_questions"] is answered
    assert canonical["attempt_id"] == "attempt-7"
    assert canonical["current_question"] == 3
    assert canonical["quiz_message_id"] is None
    assert canonical["processing_answer"] is False
    assert canonical["timer_task"] is None
    assert canonical["countdown_task"] is None
    assert canonical["question_sent_at"] is None


def test_runtime_state_exists_before_legacy_bridge(monkeypatch):
    _clear_runtime(monkeypatch)

    assert runtime_state.get_user_data() == {}
    assert runtime_state.get_user_locks() == {}
    assert runtime_state.get_bad_input_counts() == {}

    runtime_state.get_user_data()[7] = {"last_activity": 1.0}
    assert runtime_state.get_user_data()[7]["last_activity"] == 1.0


def test_first_bridge_migrates_legacy_ram_and_rebinds_exact_canonical_objects(monkeypatch):
    _clear_runtime(monkeypatch)
    runtime_state.get_user_data()[5] = {"canonical": True}
    legacy_user_data = {7: {"last_activity": 1.0}}
    legacy_user_locks = {7: object()}
    legacy_bad_input_counts = {9: 2}
    legacy = _legacy(
        user_data=legacy_user_data,
        user_locks=legacy_user_locks,
        bad_input_counts=legacy_bad_input_counts,
    )

    runtime_state.install_legacy_bridge(legacy)

    assert runtime_state.get_user_data() == {
        5: {"canonical": True},
        7: {"last_activity": 1.0},
    }
    assert runtime_state.get_user_locks()[7] is legacy_user_locks[7]
    assert runtime_state.get_bad_input_counts() == {9: 2}
    assert legacy.user_data is runtime_state.get_user_data()
    assert legacy.user_locks is runtime_state.get_user_locks()
    assert legacy._bad_input_count is runtime_state.get_bad_input_counts()
    assert legacy._create_session_data is runtime_state.create_session_data
    assert legacy._inc_bad_input is runtime_state.increment_bad_input
    assert legacy._reset_bad_input is runtime_state.reset_bad_input

    runtime_state.install_legacy_bridge(legacy)
    assert legacy.user_data is runtime_state.get_user_data()


def test_runtime_bridge_fails_closed_before_any_migration_on_mapping_conflict(monkeypatch):
    _clear_runtime(monkeypatch)
    runtime_state.get_user_data()[7] = {"source": "canonical"}
    legacy = _legacy(user_data={7: {"source": "legacy"}}, user_locks={9: object()})

    with pytest.raises(RuntimeError, match="user_data conflicts"):
        runtime_state.install_legacy_bridge(legacy)

    assert runtime_state.get_user_locks() == {}
    assert runtime_state.get_bad_input_counts() == {}
    assert legacy.user_data == {7: {"source": "legacy"}}
    assert runtime_state._legacy_bridge_installed is False


def test_runtime_bridge_fails_closed_on_session_factory_drift(monkeypatch):
    _clear_runtime(monkeypatch)

    def drifted_factory(**kwargs):
        data = _historical_session_factory(**kwargs)
        data["processing_answer"] = True
        return data

    legacy = _legacy(factory=drifted_factory)

    with pytest.raises(RuntimeError, match="factory drifted"):
        runtime_state.install_legacy_bridge(legacy)

    assert legacy._create_session_data is drifted_factory
    assert runtime_state._legacy_bridge_installed is False


def test_runtime_bridge_rejects_missing_process_local_helpers(monkeypatch):
    _clear_runtime(monkeypatch)

    missing_factory = SimpleNamespace(
        user_data={},
        user_locks={},
        _bad_input_count={},
        _inc_bad_input=lambda _user_id: 1,
        _reset_bad_input=lambda _user_id: None,
    )
    with pytest.raises(TypeError, match="_create_session_data"):
        runtime_state.install_legacy_bridge(missing_factory)

    missing_increment = _legacy()
    del missing_increment._inc_bad_input
    with pytest.raises(TypeError, match="_inc_bad_input"):
        runtime_state.install_legacy_bridge(missing_increment)

    missing_reset = _legacy()
    del missing_reset._reset_bad_input
    with pytest.raises(TypeError, match="_reset_bad_input"):
        runtime_state.install_legacy_bridge(missing_reset)


def test_runtime_bridge_rejects_different_mapping_after_install(monkeypatch):
    _clear_runtime(monkeypatch)
    first = _legacy()
    runtime_state.install_legacy_bridge(first)

    second = _legacy(
        user_data={},
        user_locks=runtime_state.get_user_locks(),
        bad_input_counts=runtime_state.get_bad_input_counts(),
    )
    with pytest.raises(RuntimeError, match="user_data"):
        runtime_state.install_legacy_bridge(second)


@pytest.mark.parametrize(
    "legacy",
    [
        SimpleNamespace(
            user_data=None,
            user_locks={},
            _bad_input_count={},
            _create_session_data=_historical_session_factory,
        ),
        SimpleNamespace(
            user_data={},
            user_locks=None,
            _bad_input_count={},
            _create_session_data=_historical_session_factory,
        ),
        SimpleNamespace(
            user_data={},
            user_locks={},
            _bad_input_count=None,
            _create_session_data=_historical_session_factory,
        ),
    ],
)
def test_runtime_bridge_rejects_malformed_legacy_state(monkeypatch, legacy):
    _clear_runtime(monkeypatch)
    with pytest.raises(TypeError):
        runtime_state.install_legacy_bridge(legacy)


def test_get_user_lock_reuses_exact_canonical_lock_mapping(monkeypatch):
    _clear_runtime(monkeypatch)

    first = runtime_state.get_user_lock(77)
    second = runtime_state.get_user_lock(77)

    assert isinstance(first, asyncio.Lock)
    assert second is first
    assert runtime_state.get_user_locks()[77] is first


def test_bad_input_helpers_share_canonical_mapping_and_legacy_alias(monkeypatch):
    _clear_runtime(monkeypatch)
    legacy = _legacy()
    runtime_state.install_legacy_bridge(legacy)

    assert runtime_state.increment_bad_input(42) == 1
    assert legacy._inc_bad_input(42) == 2
    assert runtime_state.get_bad_input_counts() == {42: 2}

    legacy._reset_bad_input(42)
    assert runtime_state.get_bad_input_counts() == {}


def test_runtime_state_is_canonical_process_local_owner_and_has_no_legacy_import():
    assert "Mongo remains the durable authority" in RUNTIME_SOURCE
    assert "import bot" not in RUNTIME_SOURCE
    assert "from bot" not in RUNTIME_SOURCE
    assert "_user_data: MutableMapping = {}" in RUNTIME_SOURCE
    assert "legacy_module.user_data = _user_data" in RUNTIME_SOURCE
    assert "legacy_module.user_locks = _user_locks" in RUNTIME_SOURCE
    assert "legacy_module._bad_input_count = _bad_input_counts" in RUNTIME_SOURCE
    assert "legacy_module._create_session_data = create_session_data" in RUNTIME_SOURCE


def test_production_bridge_and_core_use_canonical_process_local_helpers():
    assert "import telegram_quiz_runtime_state as quiz_runtime" in PRODUCTION_SOURCE
    assert "quiz_runtime.install_legacy_bridge(legacy)" in PRODUCTION_SOURCE
    assert "quiz_runtime.create_session_data(" in CORE_SOURCE
    assert "quiz_runtime.reset_bad_input(user_id)" in CORE_SOURCE
    assert "quiz_runtime.get_user_lock(user_id)" in CORE_SOURCE
    assert "legacy." not in CORE_SOURCE
    assert "import bot" not in CORE_SOURCE
