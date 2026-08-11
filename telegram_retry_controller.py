"""Telegram adapter for restart-safe retry-error practice."""
from __future__ import annotations

import logging

from telegram.ext import ConversationHandler

import telegram_controller as quiz
from legacy_retry_source import (
    LegacyRetrySourceInvalid,
    LegacyRetrySourceUnavailable,
    load_retry_source_for_result_message,
)

logger = logging.getLogger(__name__)


def _display_source_name(value: str) -> str:
    name = str(value or "").strip() or "Тест"
    prefix = "🔁 Повторение ошибок ("
    if name.startswith(prefix) and name.endswith(")"):
        inner = name[len(prefix) : -1].strip()
        if inner:
            return inner
    return name


async def retry_errors(update, context):
    """Start non-scoring practice from the durable result behind this message."""
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    try:
        target_id = int((query.data or "").replace("retry_errors_", "", 1))
    except (TypeError, ValueError):
        await query.answer("Некорректная кнопка.", show_alert=True)
        return ConversationHandler.END

    if target_id != user_id:
        await query.answer("Эта кнопка принадлежит другому пользователю.", show_alert=True)
        return ConversationHandler.END

    message = query.message
    if message is None or message.date is None:
        await query.answer("Данные результата устарели.", show_alert=True)
        return ConversationHandler.END

    try:
        source = load_retry_source_for_result_message(
            user_id=user_id,
            chat_id=message.chat_id,
            message_date=message.date,
        )
    except LegacyRetrySourceUnavailable:
        await query.answer("База результатов временно недоступна.", show_alert=True)
        return ConversationHandler.END
    except (LegacyRetrySourceInvalid, ValueError):
        logger.error("durable retry source is invalid for user %s", user_id, exc_info=True)
        await query.answer("Сохранённый результат повреждён.", show_alert=True)
        return ConversationHandler.END

    if source is None:
        await query.answer("Данные результата устарели.", show_alert=True)
        return ConversationHandler.END

    wrong = list(source.questions)
    if not wrong:
        await query.answer("Ошибок нет!", show_alert=True)
        return ConversationHandler.END

    await query.answer()
    data = await quiz._launch_attempt(
        user=user,
        bot=context.bot,
        chat_id=message.chat_id,
        mode="level",
        questions=wrong,
        level_key="retry_errors",
        level_name=f"🔁 Повторение ошибок ({_display_source_name(source.level_name)})",
        time_limit=None,
        is_retry=True,
    )
    if data is None:
        return ConversationHandler.END

    await query.edit_message_text(
        f"🔁 *ПОВТОРЕНИЕ ОШИБОК*\n\n"
        f"Вопросов: {len(wrong)}\n"
        "Тренировочный режим: баллы и достижения не начисляются.",
        parse_mode="Markdown",
    )
    await quiz.send_question(context.bot, user_id)
    return quiz.ANSWERING
