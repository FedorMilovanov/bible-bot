import ast
import asyncio
import os
import threading
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("ADMIN_USER_ID", "1")

import telegram_challenge_controller as challenge


class Query:
    def __init__(self, data="challenge_menu"):
        self.data = data
        self.from_user = SimpleNamespace(id=42)
        self.answers = []
        self.edits = []

    async def answer(self, text=None, show_alert=False):
        self.answers.append((text, show_alert))

    async def edit_message_text(self, text, **kwargs):
        self.edits.append((text, kwargs))


def run(coro):
    return asyncio.run(coro)


def test_challenge_menu_bonus_reads_run_off_event_loop(monkeypatch):
    event_loop_thread = threading.get_ident()
    calls = []

    def eligible(user_id, mode):
        calls.append((user_id, mode, threading.get_ident()))
        return mode == "random20"

    monkeypatch.setattr(challenge, "is_bonus_eligible", eligible)
    query = Query()
    update = SimpleNamespace(callback_query=query)

    run(challenge.challenge_menu(update, None))

    assert {(user_id, mode) for user_id, mode, _thread in calls} == {
        (42, "random20"),
        (42, "hardcore20"),
    }
    assert all(thread_id != event_loop_thread for _user, _mode, thread_id in calls)
    assert query.answers == [(None, False)]
    text, kwargs = query.edits[0]
    assert "Normal:   ✅ доступен" in text
    assert "Hardcore: ❌ уже получен" in text
    callbacks = [row[0].callback_data for row in kwargs["reply_markup"].inline_keyboard]
    assert callbacks == [
        "challenge_rules_random20",
        "challenge_rules_hardcore20",
        "weekly_lb_random20",
        "back_to_main",
    ]


def test_challenge_rules_bonus_read_runs_off_event_loop(monkeypatch):
    event_loop_thread = threading.get_ident()
    calls = []

    def eligible(user_id, mode):
        calls.append((user_id, mode, threading.get_ident()))
        return False

    monkeypatch.setattr(challenge, "is_bonus_eligible", eligible)
    query = Query("challenge_rules_hardcore20")
    update = SimpleNamespace(callback_query=query)

    run(challenge.challenge_rules(update, None))

    assert len(calls) == 1
    assert calls[0][:2] == (42, "hardcore20")
    assert calls[0][2] != event_loop_thread
    text, kwargs = query.edits[0]
    assert "Hardcore Random (20)" in text
    assert "⏱ 10 сек на вопрос" in text
    assert "❌ уже получен сегодня" in text
    callbacks = [row[0].callback_data for row in kwargs["reply_markup"].inline_keyboard]
    assert callbacks == ["challenge_start_hardcore20", "challenge_menu"]


def test_production_routes_challenge_menu_and_rules_to_controller():
    source = Path("telegram_production.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    main = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "main"
    )
    main_source = ast.get_source_segment(source, main) or ""

    assert "CallbackQueryHandler(challenge.challenge_menu, pattern=\"^challenge_menu$\")" in main_source
    assert "CallbackQueryHandler(challenge.challenge_rules, pattern=\"^challenge_rules_\")" in main_source
    assert "CallbackQueryHandler(legacy.challenge_menu" not in main_source
    assert "CallbackQueryHandler(legacy.challenge_rules" not in main_source
