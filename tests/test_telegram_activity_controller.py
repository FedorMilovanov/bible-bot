import ast
import asyncio
import os
import threading
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("ADMIN_USER_ID", "1")

import telegram_activity_controller as activity


def run(coro):
    return asyncio.run(coro)


def test_presentation_touch_preserves_memory_and_moves_db_off_event_loop(monkeypatch):
    event_loop_thread = threading.get_ident()
    db_threads = []
    user_data = {42: {"last_activity": 1.0}}

    def persist(user_id):
        db_threads.append((user_id, threading.get_ident()))

    monkeypatch.setattr(activity, "get_user_data", lambda: user_data)
    monkeypatch.setattr(activity, "touch_user_activity", persist)
    query = SimpleNamespace(from_user=SimpleNamespace(id=42))
    update = SimpleNamespace(callback_query=query)

    returned = run(activity.touch_presentation(update))

    assert returned is query
    assert user_data[42]["last_activity"] > 1.0
    assert db_threads == [(42, db_threads[0][1])]
    assert db_threads[0][1] != event_loop_thread


def test_presentation_touch_still_persists_when_runtime_session_is_absent(monkeypatch):
    calls = []
    user_data = {}
    monkeypatch.setattr(activity, "get_user_data", lambda: user_data)
    monkeypatch.setattr(activity, "touch_user_activity", calls.append)
    update = SimpleNamespace(
        callback_query=SimpleNamespace(from_user=SimpleNamespace(id=77))
    )

    run(activity.touch_presentation(update))

    assert calls == [77]
    assert user_data == {}


def test_nonachievement_presentation_callbacks_use_activity_controller():
    source = Path("telegram_production.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    targets = {
        "_about_callback",
        "_start_test_callback",
        "_leaderboard_callback",
        "_leaderboard_page_callback",
        "_my_stats_callback",
        "_coming_soon_callback",
    }
    functions = {
        node.name: ast.get_source_segment(source, node) or ""
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name in targets
    }

    assert set(functions) == targets
    for callback_source in functions.values():
        assert "await activity.touch_presentation(update)" in callback_source
        assert "user_data=quiz.user_data" not in callback_source
        assert "legacy_module=legacy" not in callback_source
        assert "_touch_presentation_callback(update)" not in callback_source


def test_activity_controller_has_no_legacy_or_controller_state_dependency():
    source = Path("telegram_activity_controller.py").read_text(encoding="utf-8")
    assert "legacy_module" not in source
    assert "import bot" not in source
    assert "quiz.user_data" not in source
    assert "from database import touch_user_activity" in source
    assert "from telegram_quiz_runtime_state import get_user_data" in source
