import ast
import asyncio
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

import telegram_achievement_controller as achievements


CATALOG = {
    "first_steps": {
        "name": "Первые шаги",
        "icon": "⭐",
        "description": "Пройди свой первый тест",
        "reward": 10,
    },
    "perfect": {
        "name": "Перфекционист",
        "icon": "💎",
        "description": "100% в тестах",
        "reward": 25,
        "requirement": {"perfect_count": 5},
    },
}


class _Query:
    def __init__(self, user_id=42):
        self.from_user = SimpleNamespace(id=user_id)
        self.answers = []
        self.edits = []

    async def answer(self, text=None, show_alert=False):
        self.answers.append((text, show_alert))

    async def edit_message_text(self, text, **kwargs):
        self.edits.append((text, kwargs))


class _Update:
    def __init__(self, query):
        self.callback_query = query


def _run(coro):
    return asyncio.run(coro)


def _legacy(user_id=42):
    return SimpleNamespace(
        ACHIEVEMENTS=CATALOG,
        user_data={user_id: {"last_activity": 0.0}},
    )


def test_achievement_db_touch_and_stats_read_run_off_event_loop(monkeypatch):
    event_loop_thread = threading.get_ident()
    calls = []

    def touch(user_id):
        calls.append(("touch", user_id, threading.get_ident()))

    def get_stats(user_id):
        calls.append(("stats", user_id, threading.get_ident()))
        return {
            "achievements": {"first_steps": "15.08.2026"},
            "perfect_count": 2,
            "max_streak_ever": 7,
            "total_tests": 12,
            "daily_activity_streak": 3,
        }

    monkeypatch.setattr(achievements, "touch_user_activity", touch)
    monkeypatch.setattr(achievements, "get_user_stats", get_stats)
    query = _Query()
    legacy = _legacy()

    _run(
        achievements.show_achievements(
            _Update(query),
            object(),
            legacy_module=legacy,
        )
    )

    assert [call[:2] for call in calls] == [("touch", 42), ("stats", 42)]
    assert all(call[2] != event_loop_thread for call in calls)
    assert legacy.user_data[42]["last_activity"] > 0
    assert query.answers == [(None, False)]

    assert len(query.edits) == 1
    text, kwargs = query.edits[0]
    assert "✅ ⭐ *Первые шаги*" in text
    assert "📅 15.08.2026" in text
    assert "🔒 💎 *Перфекционист* (2/5)" in text
    assert "Разблокировано: 1/2" in text
    assert kwargs["parse_mode"] == "Markdown"
    assert kwargs["reply_markup"].inline_keyboard[0][0].callback_data == "back_to_main"


def test_achievement_screen_handles_missing_user_stats(monkeypatch):
    monkeypatch.setattr(achievements, "touch_user_activity", lambda _user_id: None)
    monkeypatch.setattr(achievements, "get_user_stats", lambda _user_id: None)
    query = _Query()

    _run(
        achievements.show_achievements(
            _Update(query),
            object(),
            legacy_module=_legacy(),
        )
    )

    text = query.edits[0][0]
    assert "Разблокировано: 0/2" in text
    assert "📊 Тестов пройдено: 0" in text
    assert "🔒 💎 *Перфекционист* (0/5)" in text


@pytest.mark.parametrize(
    "catalog",
    [
        None,
        {},
        {"bad": {"name": "Bad"}},
        {"bad": {"name": "Bad", "icon": "x", "description": "x", "reward": -1}},
    ],
)
def test_achievement_catalog_fails_closed_when_authority_is_malformed(catalog):
    legacy = SimpleNamespace(ACHIEVEMENTS=catalog, user_data={})
    with pytest.raises(RuntimeError):
        achievements._achievement_catalog(legacy)


def test_production_achievement_callback_uses_controller_not_legacy_handler():
    source = Path("telegram_production.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    callback = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_achievements_callback"
    )
    callback_source = ast.get_source_segment(source, callback)

    assert "import telegram_achievement_controller as achievements" in source
    assert "achievements.show_achievements" in callback_source
    assert "legacy_module=legacy" in callback_source
    assert "legacy.show_achievements" not in callback_source
    assert "_touch_presentation_callback" not in callback_source
