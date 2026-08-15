import asyncio
import threading
from pathlib import Path

import telegram_stats_controller as stats


class _Message:
    def __init__(self):
        self.replies = []

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))


class _Update:
    def __init__(self, user_id=42):
        self.effective_user = type("User", (), {"id": user_id})()
        self.message = _Message()


class _Query:
    def __init__(self, user_id=42, data="leaderboard"):
        self.from_user = type("User", (), {"id": user_id})()
        self.data = data
        self.edits = []

    async def edit_message_text(self, text, **kwargs):
        self.edits.append((text, kwargs))


def _run(coro):
    return asyncio.run(coro)


def test_stats_command_reads_user_position_off_event_loop_thread(monkeypatch):
    event_loop_thread = threading.get_ident()
    worker_threads = []
    keyboard = object()

    def get_position(user_id):
        assert user_id == 42
        worker_threads.append(threading.get_ident())
        return None, None

    monkeypatch.setattr(stats, "get_user_position", get_position)
    update = _Update()

    _run(
        stats.stats_command(
            update,
            object(),
            main_keyboard_factory=lambda: keyboard,
        )
    )

    assert len(worker_threads) == 1
    assert worker_threads[0] != event_loop_thread
    assert len(update.message.replies) == 1
    text, kwargs = update.message.replies[0]
    assert "МОЯ СТАТИСТИКА" in text
    assert kwargs["reply_markup"] is keyboard


def test_inline_my_stats_reads_user_position_off_event_loop_thread(monkeypatch):
    event_loop_thread = threading.get_ident()
    worker_threads = []

    def get_position(user_id):
        assert user_id == 42
        worker_threads.append(threading.get_ident())
        return None, None

    monkeypatch.setattr(stats, "get_user_position", get_position)
    query = _Query(data="my_stats")

    _run(stats.show_my_stats(query))

    assert len(worker_threads) == 1
    assert worker_threads[0] != event_loop_thread
    assert len(query.edits) == 1
    assert "МОЯ СТАТИСТИКА" in query.edits[0][0]


def test_general_leaderboard_moves_all_mongo_reads_off_event_loop_thread(monkeypatch):
    event_loop_thread = threading.get_ident()
    calls = []

    def get_page(page):
        calls.append(("page", page, threading.get_ident()))
        return []

    def get_total():
        calls.append(("total", None, threading.get_ident()))
        return 0

    def get_position(user_id):
        calls.append(("position", user_id, threading.get_ident()))
        return None, None

    monkeypatch.setattr(stats, "get_leaderboard_page", get_page)
    monkeypatch.setattr(stats, "get_total_users", get_total)
    monkeypatch.setattr(stats, "get_user_position", get_position)
    query = _Query()

    _run(stats.show_general_leaderboard(query, page=0))

    assert [call[:2] for call in calls] == [
        ("page", 0),
        ("total", None),
        ("position", 42),
    ]
    assert all(call[2] != event_loop_thread for call in calls)
    assert len(query.edits) == 1
    assert "ТАБЛИЦА ЛИДЕРОВ" in query.edits[0][0]


def test_production_root_routes_primary_stats_surfaces_to_stats_controller():
    source = Path("telegram_production.py").read_text(encoding="utf-8")

    assert "import telegram_stats_controller as stats" in source
    assert 'CommandHandler("stats", _stats_command)' in source
    assert "await stats.show_general_leaderboard(query, 0)" in source
    assert "await stats.show_general_leaderboard(query, page)" in source
    assert "await stats.show_my_stats(query)" in source

    assert 'CommandHandler("stats", legacy.stats_command)' not in source
    assert "await legacy.show_general_leaderboard" not in source
    assert "await legacy.show_my_stats" not in source
