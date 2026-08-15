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


def test_presentation_touch_preserves_memory_and_moves_db_off_event_loop():
    event_loop_thread = threading.get_ident()
    db_threads = []
    legacy = SimpleNamespace(
        user_data={42: {"last_activity": 1.0}},
        touch_user_activity=lambda user_id: db_threads.append(
            (user_id, threading.get_ident())
        ),
    )
    query = SimpleNamespace(from_user=SimpleNamespace(id=42))
    update = SimpleNamespace(callback_query=query)

    returned = run(activity.touch_presentation(update, legacy_module=legacy))

    assert returned is query
    assert legacy.user_data[42]["last_activity"] > 1.0
    assert db_threads == [(42, db_threads[0][1])]
    assert db_threads[0][1] != event_loop_thread


def test_presentation_touch_still_persists_when_runtime_session_is_absent():
    calls = []
    legacy = SimpleNamespace(
        user_data={},
        touch_user_activity=lambda user_id: calls.append(user_id),
    )
    update = SimpleNamespace(
        callback_query=SimpleNamespace(from_user=SimpleNamespace(id=77))
    )

    run(activity.touch_presentation(update, legacy_module=legacy))

    assert calls == [77]
    assert legacy.user_data == {}


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
        assert "await activity.touch_presentation(" in callback_source
        assert "legacy_module=legacy" in callback_source
        assert "_touch_presentation_callback(update)" not in callback_source
