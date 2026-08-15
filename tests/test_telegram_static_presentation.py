# ruff: noqa: RUF001
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

import telegram_static_presentation as static


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")


def _keyboard():
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("menu", callback_data="menu")]]
    )


def test_back_to_main_preserves_new_message_presentation():
    query = SimpleNamespace(
        answer=AsyncMock(),
        message=SimpleNamespace(chat_id=77),
    )
    bot = SimpleNamespace(send_message=AsyncMock())
    update = SimpleNamespace(callback_query=query)
    context = SimpleNamespace(bot=bot)
    keyboard = _keyboard()

    asyncio.run(
        static.back_to_main(
            update,
            context,
            main_keyboard_factory=lambda: keyboard,
        )
    )

    query.answer.assert_awaited_once_with()
    bot.send_message.assert_awaited_once()
    kwargs = bot.send_message.await_args.kwargs
    assert kwargs["chat_id"] == 77
    assert kwargs["text"] == (
        "📖 *БИБЛЕЙСКИЙ ТЕСТ-БОТ*\n\n"
        "📖 Глава 1 • 🔬 Лингвистика • 🏛 Контекст • ⚔️ Битвы\n\n"
        "Выбери действие:"
    )
    assert kwargs["reply_markup"] is keyboard
    assert kwargs["parse_mode"] == "Markdown"


def test_help_command_preserves_deployed_copy_and_keyboard():
    message = SimpleNamespace(reply_text=AsyncMock())
    update = SimpleNamespace(message=message)
    keyboard = _keyboard()

    asyncio.run(
        static.help_command(
            update,
            SimpleNamespace(),
            main_keyboard_factory=lambda: keyboard,
        )
    )

    message.reply_text.assert_awaited_once()
    args = message.reply_text.await_args.args
    kwargs = message.reply_text.await_args.kwargs
    text = args[0]
    assert text.startswith("📖 *ПОМОЩЬ*\n\n*Команды:*")
    assert "/status — статус активного теста" in text
    assert "⚡ Скоростной — 15 сек/вопрос, ×2.0 баллов" in text
    assert text.endswith("_v4.0 • Soli Deo Gloria_")
    assert kwargs == {"parse_mode": "Markdown", "reply_markup": keyboard}


def test_report_menu_preserves_callback_targets(monkeypatch):
    query = SimpleNamespace(answer=AsyncMock())
    edit = AsyncMock()
    monkeypatch.setattr(static, "safe_edit", edit)

    asyncio.run(
        static.report_menu(
            SimpleNamespace(callback_query=query),
            SimpleNamespace(),
        )
    )

    query.answer.assert_awaited_once_with()
    edit.assert_awaited_once()
    assert edit.await_args.args[1] == "✉️ *Написать автору*\n\nВыбери тип сообщения:"
    markup = edit.await_args.kwargs["reply_markup"]
    payloads = [row[0].callback_data for row in markup.inline_keyboard]
    assert payloads == [
        "report_start_bug",
        "report_start_idea",
        "report_start_question",
        "back_to_main",
    ]


def test_noop_only_acknowledges_callback():
    query = SimpleNamespace(answer=AsyncMock())
    asyncio.run(
        static.noop_handler(
            SimpleNamespace(callback_query=query),
            SimpleNamespace(),
        )
    )
    query.answer.assert_awaited_once_with()


def test_production_routes_static_handlers_outside_legacy():
    assert "import telegram_static_presentation as static_presentation" in PRODUCTION_SOURCE
    assert 'CommandHandler("help", _help_command)' in PRODUCTION_SOURCE
    assert 'CallbackQueryHandler(_back_to_main, pattern="^back_to_main$")' in PRODUCTION_SOURCE
    assert 'CallbackQueryHandler(static_presentation.report_menu, pattern="^report_menu$")' in PRODUCTION_SOURCE
    assert 'CallbackQueryHandler(static_presentation.noop_handler, pattern="^noop$")' in PRODUCTION_SOURCE

    assert "legacy.help_command" not in PRODUCTION_SOURCE
    assert "legacy.back_to_main" not in PRODUCTION_SOURCE
    assert "legacy.report_menu" not in PRODUCTION_SOURCE
    assert "legacy.noop_handler" not in PRODUCTION_SOURCE
