import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

import telegram_quiz_runtime_state as runtime_state


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
CONTROLLER_SOURCE = (ROOT / "telegram_controller.py").read_text(encoding="utf-8")
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
    monkeypatch.setattr(runtime_state, "_user_data", None)
    monkeypatch.setattr(runtime_state, "_user_locks", None)
    monkeypatch.setattr(runtime_state, "_bad_input_counts", None)


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


def test_runtime_bridge_exposes_exact_identity_and_canonicalizes_helpers(monkeypatch):
    _clear_runtime(monkeypatch)
    user_data = {7: {"last_activity": 1.0}}
    user_locks = {7: object()}
    bad_input_counts = {9: 2}
    legacy = _legacy(
        user_data=user_data,
        user_locks=user_locks,
        bad_input_counts=bad_input_counts,
    )
    historical_factory = legacy._create_session_data
    historical_increment = legacy._inc_bad_input
    historical_reset = legacy._reset_bad_input

    runtime_state.install_legacy_bridge(legacy)

    assert runtime_state.get_user_data() is user_data
    assert runtime_state.get_user_locks() is user_locks
    assert runtime_state.get_bad_input_counts() is bad_input_counts
    assert historical_factory is not runtime_state.create_session_data
    assert historical_increment is not runtime_state.increment_bad_input
    assert historical_reset is not runtime_state.reset_bad_input
    assert legacy._create_session_data is runtime_state.create_session_data
    assert legacy._inc_bad_input is runtime_state.increment_bad_input
    assert legacy._reset_bad_input is runtime_state.reset_bad_input

    runtime_state.install_legacy_bridge(legacy)
    assert runtime_state.get_user_data() is user_data
    assert runtime_state.get_user_locks() is user_locks
    assert runtime_state.get_bad_input_counts() is bad_input_counts


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
    with pytest.raises(RuntimeError, match="user_data"):
        runtime_state.get_user_data()
    with pytest.raises(RuntimeError, match="user_locks"):
        runtime_state.get_user_locks()
    with pytest.raises(RuntimeError, match="bad-input"):
        runtime_state.get_bad_input_counts()


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


def test_runtime_bridge_fails_closed_on_rebinding(monkeypatch):
    _clear_runtime(monkeypatch)
    first = _legacy()
    runtime_state.install_legacy_bridge(first)

    with pytest.raises(RuntimeError, match="user_data"):
        runtime_state.install_legacy_bridge(
            _legacy(
                user_data={},
                user_locks=first.user_locks,
                bad_input_counts=first._bad_input_count,
            )
        )

    with pytest.raises(RuntimeError, match="user_locks"):
        runtime_state.install_legacy_bridge(
            _legacy(
                user_data=first.user_data,
                user_locks={},
                bad_input_counts=first._bad_input_count,
            )
        )

    with pytest.raises(RuntimeError, match="bad-input"):
        runtime_state.install_legacy_bridge(
            _legacy(
                user_data=first.user_data,
                user_locks=first.user_locks,
                bad_input_counts={},
            )
        )


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
        SimpleNamespace(
            user_locks={},
            _bad_input_count={},
            _create_session_data=_historical_session_factory,
        ),
        SimpleNamespace(
            user_data={},
            _bad_input_count={},
            _create_session_data=_historical_session_factory,
        ),
        SimpleNamespace(
            user_data={},
            user_locks={},
            _create_session_data=_historical_session_factory,
        ),
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
    with pytest.raises(RuntimeError, match="bad-input"):
        runtime_state.get_bad_input_counts()


def test_get_user_lock_reuses_exact_installed_lock_mapping(monkeypatch):
    _clear_runtime(monkeypatch)
    locks = {}
    legacy = _legacy(user_locks=locks)
    runtime_state.install_legacy_bridge(legacy)

    first = runtime_state.get_user_lock(77)
    second = runtime_state.get_user_lock(77)

    assert isinstance(first, asyncio.Lock)
    assert second is first
    assert locks[77] is first
    assert runtime_state.get_user_locks() is locks


def test_bad_input_helpers_share_exact_legacy_mapping(monkeypatch):
    _clear_runtime(monkeypatch)
    counts = {}
    legacy = _legacy(bad_input_counts=counts)
    runtime_state.install_legacy_bridge(legacy)

    assert runtime_state.increment_bad_input(42) == 1
    assert legacy._inc_bad_input(42) == 2
    assert counts == {42: 2}

    legacy._reset_bad_input(42)
    assert counts == {}
    runtime_state.reset_bad_input(42)
    assert counts == {}


def test_runtime_state_is_only_process_local_projection_and_has_no_legacy_import():
    assert "Mongo remains the durable authority" in RUNTIME_SOURCE
    assert "import bot" not in RUNTIME_SOURCE
    assert "from bot" not in RUNTIME_SOURCE
    assert "legacy_module._create_session_data = create_session_data" in RUNTIME_SOURCE
    assert "legacy_module._inc_bad_input = increment_bad_input" in RUNTIME_SOURCE
    assert "legacy_module._reset_bad_input = reset_bad_input" in RUNTIME_SOURCE


def test_production_bridge_canonicalizes_controller_process_local_helpers():
    assert "import telegram_quiz_runtime_state as quiz_runtime" in PRODUCTION_SOURCE
    assert "quiz_runtime.install_legacy_bridge(legacy)" in PRODUCTION_SOURCE
    assert "legacy._create_session_data(" in CONTROLLER_SOURCE
    assert "legacy._reset_bad_input(user_id)" in CONTROLLER_SOURCE
    assert "user_data=quiz.user_data" not in PRODUCTION_SOURCE
