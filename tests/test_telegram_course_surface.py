from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import telegram_course_surface as surface
from course_catalog import SURFACE_TELEGRAM, list_courses
from telegram.ext import ConversationHandler

ROOT = Path(__file__).resolve().parents[1]
PRODUCTION = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
BOT = (ROOT / "bot.py").read_text(encoding="utf-8")


class FakeQuery:
    def __init__(self, data: str):
        self.data = data
        self.answers: list[tuple[str | None, bool]] = []
        self.edits: list[tuple[str, object]] = []
        self.message = SimpleNamespace(chat_id=123)

    async def answer(self, text=None, show_alert=False):
        self.answers.append((text, bool(show_alert)))

    async def edit_message_text(self, text, reply_markup=None, parse_mode=None):
        self.edits.append((text, reply_markup))


class FakeMessage:
    def __init__(self):
        self.replies: list[tuple[str, object]] = []

    async def reply_text(self, text, reply_markup=None, parse_mode=None):
        self.replies.append((text, reply_markup))


def test_catalog_derived_telegram_level_view_matches_available_courses():
    config = surface.legacy_level_config()
    assert config["chapter2"]["pool_key"] == "chapter2"
    assert config["chapter2"]["points_per_q"] == 0
    assert config["chapter3"]["pool_key"] == "chapter3"
    assert config["chapter3"]["points_per_q"] == 0

    available = {entry.key for entry in list_courses(surface=SURFACE_TELEGRAM)}
    for key in ("chapter4", "chapter5"):
        assert (key in config) is (key in available)


def test_telegram_learning_menu_is_rendered_from_available_catalog_groups():
    keyboard = surface._group_keyboard()
    labels = [button.text for row in keyboard.inline_keyboard for button in row]
    callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]

    assert any("Глава 2" in label for label in labels)
    assert any("Глава 3" in label for label in labels)
    assert "course:chapter2" in callbacks
    assert "course:chapter3" in callbacks


def test_course_deep_link_resolves_against_catalog_and_emits_catalog_modes():
    message = FakeMessage()
    update = SimpleNamespace(message=message)

    handled = asyncio.run(surface.start_course_deep_link(update, SimpleNamespace(), "chapter3"))

    assert handled is True
    assert len(message.replies) == 1
    text, keyboard = message.replies[0]
    assert "Глава 3" in text
    assert "без рейтинговых баллов" in text
    callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]
    assert "course_mode:relaxed:chapter3" in callbacks
    assert all("ranked" not in callback for callback in callbacks)


def test_unknown_course_deep_link_is_left_for_other_deep_link_owners():
    message = FakeMessage()
    update = SimpleNamespace(message=message)

    handled = asyncio.run(surface.start_course_deep_link(update, SimpleNamespace(), "unknown-token"))

    assert handled is False
    assert message.replies == []


def test_stale_unknown_course_callback_fails_gracefully():
    query = FakeQuery("course:chapter999")
    update = SimpleNamespace(callback_query=query)

    result = asyncio.run(surface.course_callback(update, SimpleNamespace()))

    assert result == ConversationHandler.END
    assert query.answers
    assert query.answers[-1][1] is True
    assert "недоступен" in (query.answers[-1][0] or "")
    assert query.edits
    assert "больше недоступен" in query.edits[-1][0]


def test_stale_confirm_callback_is_revalidated_through_catalog():
    query = FakeQuery("confirm_level_chapter999")
    update = SimpleNamespace(callback_query=query)

    result = asyncio.run(surface.legacy_confirm_level_callback(update, SimpleNamespace()))

    assert result == ConversationHandler.END
    assert query.answers[-1][1] is True
    assert query.edits


def test_stale_intro_callback_cannot_target_non_intro_course():
    query = FakeQuery("intro_start_chapter3")
    update = SimpleNamespace(callback_query=query)

    result = asyncio.run(surface.legacy_intro_start_callback(update, SimpleNamespace()))

    assert result == ConversationHandler.END
    assert query.answers[-1][1] is True
    assert query.edits


def test_ranked_like_mode_payload_is_not_a_valid_course_mode_callback():
    assert surface._parse_mode_callback("course_mode:ranked:chapter3") is None
    assert surface._parse_mode_callback("course_mode:relaxed:chapter3") == (
        "relaxed",
        "chapter3",
    )


def test_malformed_course_callback_cannot_smuggle_extra_authority():
    query = FakeQuery("course:chapter3:ranked")
    update = SimpleNamespace(callback_query=query)

    result = asyncio.run(surface.course_callback(update, SimpleNamespace()))

    assert result == ConversationHandler.END
    assert query.answers[-1][1] is True


def test_course_launch_acknowledges_callback_even_when_durable_launcher_reports_conflict(monkeypatch):
    import telegram_controller as quiz

    async def fake_launch_attempt(**_kwargs):
        return None

    monkeypatch.setattr(quiz, "_launch_attempt", fake_launch_attempt)
    query = FakeQuery("course_mode:relaxed:chapter2")
    update = SimpleNamespace(
        callback_query=query,
        effective_user=SimpleNamespace(id=42, username="u", first_name="User"),
    )
    context = SimpleNamespace(bot=object())

    result = asyncio.run(surface.course_mode_callback(update, context))

    assert result == ConversationHandler.END
    assert query.answers == [(None, False)]
    assert query.edits == []


def test_production_has_no_level_config_or_choose_level_shadow():
    assert "legacy.LEVEL_CONFIG" not in PRODUCTION
    assert "legacy.choose_level =" not in PRODUCTION
    assert 'CommandHandler("test", courses.choose_level)' in PRODUCTION
    assert "courses.start_course_deep_link" in PRODUCTION


def test_production_registers_all_learning_callbacks_on_catalog_surface():
    assert "courses.course_callback" in PRODUCTION
    assert "courses.course_mode_callback" in PRODUCTION
    assert "courses.legacy_level_callback" in PRODUCTION
    assert "courses.legacy_mode_callback" in PRODUCTION
    assert "courses.legacy_confirm_level_callback" in PRODUCTION
    assert "courses.legacy_intro_start_callback" in PRODUCTION
    assert "CallbackQueryHandler(legacy.level_handler" not in PRODUCTION
    assert "CallbackQueryHandler(legacy.confirm_level_handler" not in PRODUCTION
    assert "CallbackQueryHandler(quiz.intro_start_handler" not in PRODUCTION


def test_bot_historical_level_config_is_not_production_authority():
    # The literal remains transitional standalone code only. Production neither
    # reads nor patches it: /test, /start course tokens and all course callbacks
    # enter telegram_course_surface directly.
    assert "LEVEL_CONFIG = {" in BOT
    assert "legacy.LEVEL_CONFIG" not in PRODUCTION
    assert 'CommandHandler("test", courses.choose_level)' in PRODUCTION
    assert "courses.start_course_deep_link" in PRODUCTION
