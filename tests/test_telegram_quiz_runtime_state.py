import asyncio
from pathlib import Path

import pytest

import telegram_quiz_runtime_state as runtime_state


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
ADMIN_SOURCE = (ROOT / "telegram_admin_controller.py").read_text(encoding="utf-8")
RUNTIME_SOURCE = (ROOT / "telegram_quiz_runtime_state.py").read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def _clear_canonical_runtime():
    runtime_state.user_data.clear()
    runtime_state.user_locks.clear()
    runtime_state.bad_input_counts.clear()
    yield
    runtime_state.user_data.clear()
    runtime_state.user_locks.clear()
    runtime_state.bad_input_counts.clear()


def test_session_factory_preserves_runtime_projection_shape_and_overrides():
    questions = [{"question": "Кто написал послание?"}]
    answered = [{"user_answer": "Пётр"}]
    canonical = runtime_state.create_session_data(
        user_id=7,
        session_id="session-7",
        questions=questions,
        level_name="Уровень",
        chat_id=700,
        attempt_id="attempt-7",
        current_question=3,
        answered_questions=answered,
        level_key="level_easy",
        correct_answers=2,
        quiz_mode="speed",
        score_multiplier=1.5,
        first_name="Пётр",
    )

    assert canonical["session_id"] == "session-7"
    assert canonical["questions"] is questions
    assert canonical["answered_questions"] is answered
    assert canonical["attempt_id"] == "attempt-7"
    assert canonical["current_question"] == 3
    assert canonical["level_name"] == "Уровень"
    assert canonical["quiz_chat_id"] == 700
    assert canonical["quiz_message_id"] is None
    assert canonical["processing_answer"] is False
    assert canonical["timer_task"] is None
    assert canonical["countdown_task"] is None
    assert canonical["question_sent_at"] is None
    assert canonical["current_streak"] == 0
    assert canonical["max_streak"] == 0


def test_runtime_mappings_are_direct_canonical_owners():
    assert runtime_state.get_user_data() is runtime_state.user_data
    assert runtime_state.get_user_locks() is runtime_state.user_locks
    assert runtime_state.get_bad_input_counts() is runtime_state.bad_input_counts

    runtime_state.user_data[7] = {"last_activity": 1.0}
    assert runtime_state.get_user_data()[7]["last_activity"] == 1.0


def test_get_user_lock_reuses_canonical_lock_mapping():
    first = runtime_state.get_user_lock(77)
    second = runtime_state.get_user_lock(77)

    assert isinstance(first, asyncio.Lock)
    assert second is first
    assert runtime_state.user_locks[77] is first
    assert runtime_state.get_user_locks() is runtime_state.user_locks


def test_bad_input_helpers_share_only_canonical_mapping():
    assert runtime_state.increment_bad_input(42) == 1
    assert runtime_state.increment_bad_input(42) == 2
    assert runtime_state.bad_input_counts == {42: 2}

    runtime_state.reset_bad_input(42)
    assert runtime_state.bad_input_counts == {}
    runtime_state.reset_bad_input(42)
    assert runtime_state.bad_input_counts == {}


def test_runtime_state_is_only_process_local_projection_and_has_no_monolith_bridge():
    assert "Mongo remains the durable authority" in RUNTIME_SOURCE
    assert "import bot" not in RUNTIME_SOURCE
    assert "from bot" not in RUNTIME_SOURCE
    assert "install_legacy_bridge" not in RUNTIME_SOURCE
    assert "legacy_module" not in RUNTIME_SOURCE
    assert "user_data: dict = {}" in RUNTIME_SOURCE
    assert "user_locks: dict = {}" in RUNTIME_SOURCE
    assert "bad_input_counts: dict = {}" in RUNTIME_SOURCE


def test_production_uses_canonical_quiz_runtime_without_state_bridge():
    assert "import telegram_quiz_runtime_controller as quiz" in PRODUCTION_SOURCE
    assert "install_legacy_bridge" not in PRODUCTION_SOURCE
    assert "import telegram_controller as quiz" not in PRODUCTION_SOURCE
    assert "legacy =" not in PRODUCTION_SOURCE


def test_admin_uses_canonical_runtime_mapping_without_importing_controller():
    assert "from telegram_quiz_runtime_state import user_data" in ADMIN_SOURCE
    assert "from telegram_controller import user_data" not in ADMIN_SOURCE
    assert "import telegram_controller" not in ADMIN_SOURCE
