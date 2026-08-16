import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

import telegram_quiz_runtime_state as runtime_state


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
ADMIN_SOURCE = (ROOT / "telegram_admin_controller.py").read_text(encoding="utf-8")
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


@pytest.fixture(autouse=True)
def _clear_canonical_runtime():
    runtime_state.user_data.clear()
    runtime_state.user_locks.clear()
    runtime_state.bad_input_counts.clear()
    yield
    runtime_state.user_data.clear()
    runtime_state.user_locks.clear()
    runtime_state.bad_input_counts.clear()


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


def test_runtime_is_canonical_and_available_without_legacy_install():
    assert runtime_state.get_user_data() is runtime_state.user_data
    assert runtime_state.get_user_locks() is runtime_state.user_locks
    assert runtime_state.get_bad_input_counts() is runtime_state.bad_input_counts

    runtime_state.user_data[7] = {"last_activity": 1.0}
    assert runtime_state.get_user_data()[7]["last_activity"] == 1.0


def test_runtime_bridge_migrates_legacy_state_and_repoints_all_identities():
    legacy_user_data = {7: {"last_activity": 1.0}}
    legacy_lock = object()
    legacy_user_locks = {7: legacy_lock}
    legacy_bad_input_counts = {9: 2}
    legacy = _legacy(
        user_data=legacy_user_data,
        user_locks=legacy_user_locks,
        bad_input_counts=legacy_bad_input_counts,
    )
    historical_factory = legacy._create_session_data
    historical_increment = legacy._inc_bad_input
    historical_reset = legacy._reset_bad_input

    runtime_state.install_legacy_bridge(legacy)

    assert runtime_state.user_data == legacy_user_data
    assert runtime_state.user_locks[7] is legacy_lock
    assert runtime_state.bad_input_counts == legacy_bad_input_counts
    assert legacy.user_data is runtime_state.user_data
    assert legacy.user_locks is runtime_state.user_locks
    assert legacy._bad_input_count is runtime_state.bad_input_counts
    assert historical_factory is not runtime_state.create_session_data
    assert historical_increment is not runtime_state.increment_bad_input
    assert historical_reset is not runtime_state.reset_bad_input
    assert legacy._create_session_data is runtime_state.create_session_data
    assert legacy._inc_bad_input is runtime_state.increment_bad_input
    assert legacy._reset_bad_input is runtime_state.reset_bad_input

    runtime_state.install_legacy_bridge(legacy)
    assert legacy.user_data is runtime_state.user_data
    assert legacy.user_locks is runtime_state.user_locks
    assert legacy._bad_input_count is runtime_state.bad_input_counts


def test_runtime_bridge_preserves_existing_canonical_state_when_legacy_is_empty():
    session = {"session_id": "canonical"}
    lock = object()
    runtime_state.user_data[1] = session
    runtime_state.user_locks[1] = lock
    runtime_state.bad_input_counts[1] = 2
    legacy = _legacy()

    runtime_state.install_legacy_bridge(legacy)

    assert legacy.user_data is runtime_state.user_data
    assert legacy.user_data[1] is session
    assert legacy.user_locks[1] is lock
    assert legacy._bad_input_count == {1: 2}


def test_runtime_bridge_fails_closed_on_session_factory_drift():
    def drifted_factory(**kwargs):
        data = _historical_session_factory(**kwargs)
        data["processing_answer"] = True
        return data

    legacy = _legacy(
        user_data={7: {"legacy": True}},
        user_locks={7: object()},
        bad_input_counts={7: 1},
        factory=drifted_factory,
    )

    with pytest.raises(RuntimeError, match="factory drifted"):
        runtime_state.install_legacy_bridge(legacy)

    assert runtime_state.user_data == {}
    assert runtime_state.user_locks == {}
    assert runtime_state.bad_input_counts == {}
    assert legacy._create_session_data is drifted_factory
    assert legacy.user_data is not runtime_state.user_data


def test_runtime_bridge_conflict_is_atomic_across_all_mappings():
    canonical_session = {"source": "canonical"}
    canonical_lock = object()
    runtime_state.user_data[7] = canonical_session
    runtime_state.user_locks[8] = canonical_lock
    runtime_state.bad_input_counts[9] = 1

    legacy_user_data = {7: {"source": "legacy"}, 70: {"migrate": True}}
    legacy_user_locks = {80: object()}
    legacy_counts = {90: 2}
    legacy = _legacy(
        user_data=legacy_user_data,
        user_locks=legacy_user_locks,
        bad_input_counts=legacy_counts,
    )

    with pytest.raises(RuntimeError, match="user_data"):
        runtime_state.install_legacy_bridge(legacy)

    assert runtime_state.user_data == {7: canonical_session}
    assert runtime_state.user_locks == {8: canonical_lock}
    assert runtime_state.bad_input_counts == {9: 1}
    assert legacy.user_data is legacy_user_data
    assert legacy.user_locks is legacy_user_locks
    assert legacy._bad_input_count is legacy_counts


def test_runtime_bridge_accepts_equal_nonidentical_values_without_splitting_state():
    runtime_state.user_data[7] = {"value": 1}
    legacy = _legacy(user_data={7: {"value": 1}, 8: {"value": 2}})

    runtime_state.install_legacy_bridge(legacy)

    assert legacy.user_data is runtime_state.user_data
    assert runtime_state.user_data == {
        7: {"value": 1},
        8: {"value": 2},
    }


def test_runtime_bridge_rejects_missing_process_local_helpers():
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


@pytest.mark.parametrize(
    "legacy",
    [
        SimpleNamespace(
            user_data=None,
            user_locks={},
            _bad_input_count={},
            _create_session_data=_historical_session_factory,
            _inc_bad_input=lambda _user_id: 1,
            _reset_bad_input=lambda _user_id: None,
        ),
        SimpleNamespace(
            user_data={},
            user_locks=None,
            _bad_input_count={},
            _create_session_data=_historical_session_factory,
            _inc_bad_input=lambda _user_id: 1,
            _reset_bad_input=lambda _user_id: None,
        ),
        SimpleNamespace(
            user_data={},
            user_locks={},
            _bad_input_count=None,
            _create_session_data=_historical_session_factory,
            _inc_bad_input=lambda _user_id: 1,
            _reset_bad_input=lambda _user_id: None,
        ),
        SimpleNamespace(
            user_locks={},
            _bad_input_count={},
            _create_session_data=_historical_session_factory,
            _inc_bad_input=lambda _user_id: 1,
            _reset_bad_input=lambda _user_id: None,
        ),
        SimpleNamespace(
            user_data={},
            _bad_input_count={},
            _create_session_data=_historical_session_factory,
            _inc_bad_input=lambda _user_id: 1,
            _reset_bad_input=lambda _user_id: None,
        ),
        SimpleNamespace(
            user_data={},
            user_locks={},
            _create_session_data=_historical_session_factory,
            _inc_bad_input=lambda _user_id: 1,
            _reset_bad_input=lambda _user_id: None,
        ),
    ],
)
def test_runtime_bridge_rejects_malformed_legacy_state(legacy):
    with pytest.raises(TypeError):
        runtime_state.install_legacy_bridge(legacy)

    assert runtime_state.user_data == {}
    assert runtime_state.user_locks == {}
    assert runtime_state.bad_input_counts == {}


def test_get_user_lock_reuses_canonical_lock_mapping():
    first = runtime_state.get_user_lock(77)
    second = runtime_state.get_user_lock(77)

    assert isinstance(first, asyncio.Lock)
    assert second is first
    assert runtime_state.user_locks[77] is first
    assert runtime_state.get_user_locks() is runtime_state.user_locks


def test_bad_input_helpers_share_canonical_mapping_with_legacy_bridge():
    legacy = _legacy()
    runtime_state.install_legacy_bridge(legacy)

    assert runtime_state.increment_bad_input(42) == 1
    assert legacy._inc_bad_input(42) == 2
    assert runtime_state.bad_input_counts == {42: 2}
    assert legacy._bad_input_count is runtime_state.bad_input_counts

    legacy._reset_bad_input(42)
    assert runtime_state.bad_input_counts == {}
    runtime_state.reset_bad_input(42)
    assert runtime_state.bad_input_counts == {}


def test_runtime_state_is_only_process_local_projection_and_has_no_legacy_import():
    assert "Mongo remains the durable authority" in RUNTIME_SOURCE
    assert "import bot" not in RUNTIME_SOURCE
    assert "from bot" not in RUNTIME_SOURCE
    assert "user_data: dict = {}" in RUNTIME_SOURCE
    assert "user_locks: dict = {}" in RUNTIME_SOURCE
    assert "bad_input_counts: dict = {}" in RUNTIME_SOURCE
    assert "legacy_module.user_data = user_data" in RUNTIME_SOURCE
    assert "legacy_module.user_locks = user_locks" in RUNTIME_SOURCE
    assert "legacy_module._bad_input_count = bad_input_counts" in RUNTIME_SOURCE
    assert "legacy_module._create_session_data = create_session_data" in RUNTIME_SOURCE
    assert "legacy_module._inc_bad_input = increment_bad_input" in RUNTIME_SOURCE
    assert "legacy_module._reset_bad_input = reset_bad_input" in RUNTIME_SOURCE


def test_production_installs_runtime_bridge_before_controller_import():
    runtime_import = PRODUCTION_SOURCE.index(
        "import telegram_quiz_runtime_state as quiz_runtime"
    )
    runtime_install = PRODUCTION_SOURCE.index("quiz_runtime.install_legacy_bridge(legacy)")
    admin_import = PRODUCTION_SOURCE.index("import telegram_admin_controller as admin")
    controller_import = PRODUCTION_SOURCE.index("import telegram_controller as quiz")

    assert runtime_import < runtime_install < admin_import < controller_import


def test_admin_uses_canonical_runtime_mapping_without_importing_controller():
    assert "from telegram_quiz_runtime_state import user_data" in ADMIN_SOURCE
    assert "from telegram_controller import user_data" not in ADMIN_SOURCE
    assert "import telegram_controller" not in ADMIN_SOURCE
