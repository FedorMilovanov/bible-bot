import asyncio
import os

os.environ.setdefault("ADMIN_USER_ID", "1")

import telegram_admin_controller as admin
from legacy_battle_cleanup import LegacyBattleCleanupUnavailable


class _Query:
    def __init__(self, user_id=1):
        self.from_user = type("User", (), {"id": user_id})()
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


def test_non_admin_cannot_run_cleanup(monkeypatch):
    monkeypatch.setattr(
        admin,
        "cleanup_stale_waiting_battles",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not clean")),
    )
    query = _Query(user_id=999)

    _run(admin.admin_cleanup(_Update(query), object()))

    assert query.answers == [("Нет доступа.", True)]
    assert query.edits == []


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
    assert "pre-progress" in text
    assert "*3*" in text
    assert "*2*" in text
    assert kwargs["reply_markup"].inline_keyboard[0][0].callback_data == "admin_back"


def test_cleanup_fails_closed_before_ram_prune_on_mongo_outage(monkeypatch):
    def unavailable(**_kwargs):
        raise LegacyBattleCleanupUnavailable("mongo down")

    monkeypatch.setattr(admin, "cleanup_stale_waiting_battles", unavailable)
    original = {10: {"last_activity": 0.0}}
    monkeypatch.setattr(admin.legacy, "user_data", dict(original))
    query = _Query()

    _run(admin.admin_cleanup(_Update(query), object()))

    assert admin.legacy.user_data == original
    assert query.answers == [("База битв временно недоступна.", True)]
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


def test_admin_adapter_never_imports_or_calls_legacy_database_cleanup():
    from pathlib import Path

    source = Path(admin.__file__).read_text(encoding="utf-8")
    assert "db_cleanup_stale_battles" not in source
    assert "database.cleanup_stale_battles" not in source
    assert "cleanup_stale_waiting_battles" in source
