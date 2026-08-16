import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import telegram_intro_controller as intro


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
INTRO_SOURCE = (ROOT / "telegram_intro_controller.py").read_text(encoding="utf-8")


def _update(data: str):
    query = SimpleNamespace(
        data=data,
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
    )
    return SimpleNamespace(callback_query=query), query


def test_resolve_intro_course_uses_canonical_catalog(monkeypatch):
    entry = SimpleNamespace(key="level_intro1")
    calls = []

    def fake_resolve_course(key, *, surface):
        calls.append(("course", key, surface))
        return entry

    def fake_resolve_pool(resolved):
        calls.append(("pool", resolved))
        return [{"explanation": "fact"}]

    monkeypatch.setattr(intro, "resolve_course", fake_resolve_course)
    monkeypatch.setattr(intro, "resolve_course_pool", fake_resolve_pool)

    resolved, pool = intro._resolve_intro_course("level_intro1")

    assert resolved is entry
    assert pool == [{"explanation": "fact"}]
    assert calls == [
        ("course", "level_intro1", intro.SURFACE_TELEGRAM),
        ("pool", entry),
    ]


def test_intro_hint_preserves_legacy_callback_contract(monkeypatch):
    update, query = _update("intro_hint_level_intro2")
    entry = SimpleNamespace(title="Intro title")
    pool = [
        {"explanation": "fact one"},
        {"explanation": "fact two"},
        {"explanation": "fact three"},
        {"explanation": "fact four"},
    ]
    monkeypatch.setattr(intro, "_resolve_intro_course", lambda key: (entry, pool))
    monkeypatch.setattr(intro.random, "sample", lambda items, count: items[:count])

    asyncio.run(intro.intro_hint_handler(update, object()))

    query.answer.assert_awaited_once_with()
    query.edit_message_text.assert_awaited_once()
    args = query.edit_message_text.await_args.args
    kwargs = query.edit_message_text.await_args.kwargs
    assert "Intro title" in args[0]
    assert all(f"fact {word}" in args[0] for word in ("one", "two", "three"))
    assert "fact four" not in args[0]
    assert kwargs["parse_mode"] == "Markdown"
    assert [row[0].callback_data for row in kwargs["reply_markup"].inline_keyboard] == [
        "intro_start_level_intro2",
        "historical_menu",
    ]


def test_intro_hint_rejects_non_intro_course_before_catalog_lookup(monkeypatch):
    update, query = _update("intro_hint_level_easy")
    monkeypatch.setattr(
        intro,
        "resolve_course",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not resolve")),
    )

    asyncio.run(intro.intro_hint_handler(update, object()))

    query.answer.assert_awaited_once()
    assert query.answer.await_args.kwargs["show_alert"] is True
    query.edit_message_text.assert_awaited_once()


def test_random_fact_aggregates_canonical_intro_courses(monkeypatch):
    update, query = _update("random_fact_intro")

    def fake_resolve(key):
        return SimpleNamespace(key=key), [{"explanation": f"fact:{key}"}]

    monkeypatch.setattr(intro, "_resolve_intro_course", fake_resolve)
    monkeypatch.setattr(intro.random, "choice", lambda items: items[-1])

    asyncio.run(intro.random_fact_handler(update, object()))

    query.answer.assert_awaited_once_with()
    query.edit_message_text.assert_awaited_once()
    args = query.edit_message_text.await_args.args
    kwargs = query.edit_message_text.await_args.kwargs
    assert "fact:level_intro3" in args[0]
    assert [row[0].callback_data for row in kwargs["reply_markup"].inline_keyboard] == [
        "random_fact_intro",
        "historical_menu",
    ]


def test_intro_controller_has_no_legacy_course_authority():
    assert "LEVEL_CONFIG" not in INTRO_SOURCE
    assert "get_pool_by_key" not in INTRO_SOURCE
    assert "import bot" not in INTRO_SOURCE
    assert "resolve_course(" in INTRO_SOURCE
    assert "resolve_course_pool(" in INTRO_SOURCE


def test_production_routes_intro_presentation_outside_legacy_surface():
    assert "import telegram_intro_controller as intro" in PRODUCTION_SOURCE
    assert "intro.intro_hint_handler" in PRODUCTION_SOURCE
    assert "intro.random_fact_handler" in PRODUCTION_SOURCE
    assert "legacy.intro_hint_handler" not in PRODUCTION_SOURCE
    assert "legacy.random_fact_handler" not in PRODUCTION_SOURCE
