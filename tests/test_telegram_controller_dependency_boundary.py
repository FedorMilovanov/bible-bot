import asyncio
import os
import threading
from pathlib import Path

os.environ.setdefault("ADMIN_USER_ID", "1")

import telegram_controller as controller


SOURCE = Path(controller.__file__).read_text(encoding="utf-8")


def run(coro):
    return asyncio.run(coro)


def test_controller_uses_canonical_pure_dependencies_instead_of_legacy_proxies():
    forbidden = (
        "legacy.get_user_position",
        "legacy.format_time",
        "legacy.get_pool_by_key",
        "legacy.TIMED_MODE_TIMEOUT",
        "legacy.SPEED_MODE_TIMEOUT",
        "legacy.record_question_stat",
        "legacy.FEEDBACK_DELAY_CORRECT",
        "legacy.FEEDBACK_DELAY_WRONG",
        "legacy.QUIZ_TIMEOUT",
        "legacy._main_keyboard",
        "legacy.safe_edit",
        "legacy.init_user_stats",
        "legacy._touch",
    )
    for token in forbidden:
        assert token not in SOURCE

    assert "from config import (" in SOURCE
    assert "from database import (" in SOURCE
    assert "from questions import get_pool_by_key" in SOURCE
    assert "from utils import safe_edit" in SOURCE
    assert "import telegram_main_menu as main_menu" in SOURCE


def test_touch_activity_preserves_memory_and_moves_database_write_off_event_loop(monkeypatch):
    event_loop_thread = threading.get_ident()
    db_threads = []
    controller.user_data[42] = {"last_activity": 1.0}

    def persist(user_id):
        db_threads.append((user_id, threading.get_ident()))

    monkeypatch.setattr(controller, "touch_user_activity", persist)
    try:
        run(controller._touch_activity(42))
        assert controller.user_data[42]["last_activity"] > 1.0
        assert db_threads == [(42, db_threads[0][1])]
        assert db_threads[0][1] != event_loop_thread
    finally:
        controller.user_data.pop(42, None)


def test_touch_activity_still_persists_without_loaded_runtime_session(monkeypatch):
    calls = []
    controller.user_data.pop(77, None)
    monkeypatch.setattr(controller, "touch_user_activity", lambda user_id: calls.append(user_id))

    run(controller._touch_activity(77))

    assert calls == [77]
    assert 77 not in controller.user_data
