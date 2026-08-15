import asyncio
import os
import threading

os.environ.setdefault("ADMIN_USER_ID", "1")

import telegram_admin_controller as admin
from legacy_battle_cleanup import LegacyBattleCleanupUnavailable


class _Message:
    def __init__(self):
        self.replies = []

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))


class _CommandUpdate:
    def __init__(self, user_id=1):
        self.effective_user = type("User", (), {"id": user_id})()
        self.message = _Message()


class _Query:
    def __init__(self, user_id=1, data="admin_back"):
        self.from_user = type("User", (), {"id": user_id})()
        self.data = data
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


def test_non_admin_cannot_open_admin_command(monkeypatch):
    monkeypatch.setattr(
        admin,
        "get_admin_stats",
        lambda: (_ for _ in ()).throw(AssertionError("must not read")),
    )
    update = _CommandUpdate(user_id=999)

    _run(admin.admin_command(update, object()))

    assert update.message.replies == [
        ("❌ У тебя нет доступа к этой команде.", {})  # noqa: RUF001
    ]


def test_admin_command_db_read_runs_off_event_loop_and_preserves_panel(monkeypatch):
    event_loop_thread = threading.get_ident()
    worker_threads = []

    def read_stats():
        worker_threads.append(threading.get_ident())
        return {"total_users": 12, "online_24h": 4, "new_today": 2}

    monkeypatch.setattr(admin, "get_admin_stats", read_stats)
    monkeypatch.setattr(admin.legacy, "user_data", {10: {}, 11: {}})
    update = _CommandUpdate()

    _run(admin.admin_command(update, object()))

    assert len(worker_threads) == 1
    assert worker_threads[0] != event_loop_thread
    assert len(update.message.replies) == 1
    text, kwargs = update.message.replies[0]
    assert "ПАНЕЛЬ АДМИНИСТРАТОРА" in text
    assert "Всего пользователей: *12*" in text  # noqa: RUF001
    assert "Онлайн за 24ч: *4*" in text
    assert "Новых сегодня: *2*" in text
    assert "Активных сессий в памяти: *2*" in text
    assert kwargs["parse_mode"] == "Markdown"
    buttons = [row[0] for row in kwargs["reply_markup"].inline_keyboard]
    assert [button.callback_data for button in buttons] == [
        "admin_hard_questions",
        "admin_active_sessions",
        "admin_cleanup",
        "admin_broadcast_prompt",
    ]
    assert [button.text for button in buttons] == [
        "🔍 Сложные вопросы",
        "👥 Активные сессии",
        "🧹 Очистка данных",
        "📢 Рассылка",
    ]


def test_non_admin_cannot_run_cleanup(monkeypatch):
    monkeypatch.setattr(
        admin,
        "cleanup_stale_waiting_battles",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not clean")),
    )
    query = _Query(user_id=999)

    _run(admin.admin_cleanup(_Update(query), object()))

    assert query.answers == [("Access denied.", True)]
    assert query.edits == []


def test_non_admin_cannot_use_admin_read_callbacks(monkeypatch):
    monkeypatch.setattr(
        admin.legacy,
        "get_hardest_questions",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not read")),
    )
    query = _Query(user_id=999, data="admin_hard_questions")

    _run(admin.admin_read_callback(_Update(query), object()))

    assert query.answers == [("Access denied.", True)]
    assert query.edits == []


def test_unknown_admin_read_action_is_rejected_without_dispatch(monkeypatch):
    monkeypatch.setattr(
        admin.legacy,
        "get_hardest_questions",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not read")),
    )
    query = _Query(data="admin_cleanup")

    _run(admin.admin_read_callback(_Update(query), object()))

    assert query.answers == [("Unsupported action.", True)]
    assert query.edits == []


def test_admin_hard_questions_is_read_only_presentation(monkeypatch):
    monkeypatch.setattr(
        admin.legacy,
        "get_hardest_questions",
        lambda limit=10: [
            {
                "question": "Question text",
                "total_attempts": 12,
                "correct_attempts": 5,
            }
        ],
    )
    query = _Query(data="admin_hard_questions")

    _run(admin.admin_read_callback(_Update(query), object()))

    assert query.answers == [(None, False)]
    assert len(query.edits) == 1
    text, kwargs = query.edits[0]
    assert "Question text" in text
    assert "12" in text
    assert kwargs["reply_markup"].inline_keyboard[0][0].callback_data == "admin_back"


def test_admin_hard_questions_db_read_runs_off_event_loop_thread(monkeypatch):
    event_loop_thread = threading.get_ident()
    worker_threads = []

    def read_hardest(limit=10):
        assert limit == 10
        worker_threads.append(threading.get_ident())
        return []

    monkeypatch.setattr(admin.legacy, "get_hardest_questions", read_hardest)
    query = _Query(data="admin_hard_questions")

    _run(admin.admin_read_callback(_Update(query), object()))

    assert len(worker_threads) == 1
    assert worker_threads[0] != event_loop_thread
    assert "No statistics yet." in query.edits[0][0]


def test_admin_active_sessions_reads_process_local_projection(monkeypatch):
    monkeypatch.setattr(
        admin.legacy,
        "user_data",
        {
            10: {"first_name": "Test", "current_question": 2, "questions": [1, 2, 3]},
            11: "broken",
        },
    )
    query = _Query(data="admin_active_sessions")

    _run(admin.admin_read_callback(_Update(query), object()))

    text, _kwargs = query.edits[0]
    assert "Active in-memory sessions: 2" in text
    assert "10 | Test | 2/3" in text
    assert "11: malformed record" in text


def test_admin_back_renders_only_safe_menu_actions():
    query = _Query(data="admin_back")

    _run(admin.admin_read_callback(_Update(query), object()))

    markup = query.edits[0][1]["reply_markup"]
    callbacks = [row[0].callback_data for row in markup.inline_keyboard]
    assert callbacks == [
        "admin_hard_questions",
        "admin_active_sessions",
        "admin_cleanup",
        "admin_broadcast_prompt",
    ]


def test_cleanup_uses_recovery_safe_battle_policy_then_prunes_stale_ram(monkeypatch):
    calls = []

    def safe_cleanup(**kwargs):
        calls.append(kwargs)
        return 3

    monkeypatch.setattr(admin, "cleanup_stale_waiting_battles", safe_cleanup)
    monkeypatch.setattr(admin.time, "time", lambda: 100_000.0)
    monkeypatch.setattr(admin.legacy, "GC_STALE_THRESHOLD", 1000)
    monkeypatch.setattr(
        admin.legacy,
        "user_data",
        {
            10: {"last_activity": 98_000.0},
            11: {"last_activity": 99_500.0},
            12: {"last_activity": "broken"},
        },
    )
    query = _Query()

    _run(admin.admin_cleanup(_Update(query), object()))

    assert calls == [{"max_age_minutes": 10}]
    assert set(admin.legacy.user_data) == {11}
    assert query.answers == [(None, False)]
    assert len(query.edits) == 1
    text, kwargs = query.edits[0]
    assert "Safely deleted pre-progress battles: 3" in text
    assert "Removed user_data records: 2" in text
    assert kwargs["reply_markup"].inline_keyboard[0][0].callback_data == "admin_back"


def test_cleanup_db_delete_runs_off_event_loop_thread(monkeypatch):
    event_loop_thread = threading.get_ident()
    worker_threads = []

    def safe_cleanup(**kwargs):
        assert kwargs == {"max_age_minutes": 10}
        worker_threads.append(threading.get_ident())
        return 0

    monkeypatch.setattr(admin, "cleanup_stale_waiting_battles", safe_cleanup)
    monkeypatch.setattr(admin.legacy, "user_data", {})
    query = _Query()

    _run(admin.admin_cleanup(_Update(query), object()))

    assert len(worker_threads) == 1
    assert worker_threads[0] != event_loop_thread
    assert query.answers == [(None, False)]
    assert "Safely deleted pre-progress battles: 0" in query.edits[0][0]


def test_cleanup_fails_closed_before_ram_prune_on_mongo_outage(monkeypatch):
    def unavailable(**_kwargs):
        raise LegacyBattleCleanupUnavailable("mongo down")

    monkeypatch.setattr(admin, "cleanup_stale_waiting_battles", unavailable)
    original = {10: {"last_activity": 0.0}}
    monkeypatch.setattr(admin.legacy, "user_data", dict(original))
    query = _Query()

    _run(admin.admin_cleanup(_Update(query), object()))

    assert admin.legacy.user_data == original
    assert query.answers == [("Battle storage is temporarily unavailable.", True)]
    assert query.edits == []


def test_stale_ram_classifier_is_fail_closed_for_malformed_records(monkeypatch):
    monkeypatch.setattr(admin.legacy, "GC_STALE_THRESHOLD", 100)
    monkeypatch.setattr(
        admin.legacy,
        "user_data",
        {
            1: {"last_activity": 950.0},
            2: {"last_activity": 800.0},
            3: {"last_activity": True},
            4: "broken",
        },
    )

    assert admin._stale_ram_users(now=1000.0) == [2, 3, 4]


def test_admin_adapter_never_calls_broad_legacy_or_unsafe_database_cleanup():
    from pathlib import Path

    source = Path(admin.__file__).read_text(encoding="utf-8")
    assert "legacy.admin_command" not in source
    assert "legacy.admin_callback_handler" not in source
    assert "db_cleanup_stale_battles" not in source
    assert "database.cleanup_stale_battles" not in source
    assert "cleanup_stale_waiting_battles" in source
