# ruff: noqa: RUF001
"""Pure Telegram presentation handlers detached from the legacy bot module."""
from __future__ import annotations

from collections.abc import Callable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from utils import safe_edit


_MAIN_MENU_TEXT = (
    "📖 *БИБЛЕЙСКИЙ ТЕСТ-БОТ*\n\n"
    "📖 Глава 1 • 🔬 Лингвистика • 🏛 Контекст • ⚔️ Битвы\n\n"
    "Выбери действие:"
)

_HELP_TEXT = (
    "📖 *ПОМОЩЬ*\n\n"
    "*Команды:*\n"
    "/start — главное меню\n"
    "/test — начать тест\n"
    "/status — статус активного теста\n"
    "/reset — сбросить текущий тест\n"
    "/cancel — отменить действие\n"
    "/help — эта справка\n\n"
    "*Как играть:*\n"
    "1. Выбери категорию и уровень сложности\n"
    "2. Выбери режим: 🧘 Спокойный / ⏱ На время / ⚡ Скоростной\n"
    "3. Отвечай на вопросы, нажимая кнопки с цифрами\n"
    "4. После теста — просмотри разбор ошибок и пересдай!\n\n"
    "*Режимы:*\n"
    "🧘 Спокойный — без таймера, ×1.0 баллов\n"
    "⏱ На время — 30 сек/вопрос, ×1.5 баллов\n"
    "⚡ Скоростной — 15 сек/вопрос, ×2.0 баллов\n\n"
    "Нашёл ошибку в вопросе? Нажми «⚠️ Неточность» во время теста.\n\n"
    "_v4.0 • Soli Deo Gloria_"
)


def _keyboard(factory: Callable[[], InlineKeyboardMarkup]) -> InlineKeyboardMarkup:
    keyboard = factory()
    if not isinstance(keyboard, InlineKeyboardMarkup):
        raise TypeError("main keyboard factory must return InlineKeyboardMarkup")
    return keyboard


async def back_to_main(update, context, *, main_keyboard_factory: Callable[[], InlineKeyboardMarkup]):
    """Send the historical main-menu card as a new message at the bottom of chat."""
    query = update.callback_query
    await query.answer()
    await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=_MAIN_MENU_TEXT,
        reply_markup=_keyboard(main_keyboard_factory),
        parse_mode="Markdown",
    )


async def help_command(update, context, *, main_keyboard_factory: Callable[[], InlineKeyboardMarkup]):
    """Render the deployed /help copy without consulting legacy handlers."""
    del context
    await update.message.reply_text(
        _HELP_TEXT,
        parse_mode="Markdown",
        reply_markup=_keyboard(main_keyboard_factory),
    )


async def report_menu(update, context):
    """Render the static report-type chooser."""
    del context
    query = update.callback_query
    await query.answer()
    await safe_edit(
        query,
        "✉️ *Написать автору*\n\nВыбери тип сообщения:",
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🐞 Сообщить о баге", callback_data="report_start_bug")],
                [InlineKeyboardButton("💡 Предложение", callback_data="report_start_idea")],
                [
                    InlineKeyboardButton(
                        "❓ Вопрос по материалу",
                        callback_data="report_start_question",
                    )
                ],
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")],
            ]
        ),
    )


async def noop_handler(update, context):
    """Acknowledge inert counter buttons without invoking the legacy module."""
    del context
    await update.callback_query.answer()
