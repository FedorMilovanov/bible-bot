"""Canonical Telegram action menu shown after a completed quiz result."""
from __future__ import annotations

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from quiz_answer_history import is_wrong

logger = logging.getLogger(__name__)

_FALLBACK_BOT_USERNAME = "milovanovaibot"


async def send_final_results_menu(bot, chat_id: int, data: dict) -> None:
    """Render the deployed final-result action menu from an existing result projection."""
    total = len(data.get("questions", []))
    correct_count = data.get("correct_answers", None)
    if correct_count is None:
        answered = data.get("answered_questions", [])
        correct_count = sum(
            1 for item in answered
            if isinstance(item, dict) and not is_wrong(item)
        )
    wrong_count = total - correct_count
    level_key = data.get("level_key", "")
    level_name = data.get("level_name", "Тест")
    percentage = int(correct_count / total * 100) if total > 0 else 0

    if percentage >= 90:
        emoji = "🏆"
        comment = "Отличный результат!"
    elif percentage >= 70:
        emoji = "👍"
        comment = "Хорошо!"
    elif percentage >= 50:
        emoji = "📚"
        comment = "Неплохо, но можно лучше"
    else:
        emoji = "💪"
        comment = "Попробуй ещё раз!"

    text = (
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"{emoji} *{level_name}*\n"
        f"Результат: *{correct_count}/{total}* ({percentage}%)\n"
        f"_{comment}_\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📋 *Выбери действие:*"
    )

    keyboard = [[
        InlineKeyboardButton(
            "📖 ПОСМОТРЕТЬ ОТВЕТЫ И РАЗБОР",
            callback_data="review_test_0",
        )
    ]]

    if wrong_count > 0:
        user_id = data.get("user_id")
        if user_id:
            keyboard.append([
                InlineKeyboardButton(
                    f"🔄 Пересдать ошибки ({wrong_count} шт.)",
                    callback_data=f"retry_errors_{user_id}",
                )
            ])

    if level_key:
        keyboard.append([
            InlineKeyboardButton(
                "🔁 Пройти этот тест заново",
                callback_data=f"level_{level_key}",
            )
        ])

    if percentage == 100:
        result_emoji = "🏆"
        result_comment = "Идеально!"
    elif percentage >= 80:
        result_emoji = "⭐"
        result_comment = "Отлично!"
    elif percentage >= 60:
        result_emoji = "👍"
        result_comment = "Хорошо!"
    else:
        result_emoji = "📚"
        result_comment = "Есть над чем работать"

    filled = "🟩" * (percentage // 10)
    empty = "⬜" * (10 - percentage // 10)
    progress_bar = filled + empty

    try:
        bot_info = await bot.get_me()
        bot_username = bot_info.username or _FALLBACK_BOT_USERNAME
    except Exception:
        bot_username = _FALLBACK_BOT_USERNAME

    challenge_mode = data.get("challenge_mode")
    if challenge_mode:
        mode_name = (
            "🎲 Random Challenge"
            if challenge_mode == "random20"
            else "💀 Hardcore Challenge"
        )
        share_text = (
            "⚡ Challenge по 1 Посланию Петра\n\n"
            f"{mode_name}\n"
            f"{progress_bar} {percentage}%\n\n"
            f"{result_emoji} {correct_count}/{total} — {result_comment}\n"
        )
        bonus = data.get("challenge_bonus", 0)
        if bonus and bonus > 0:
            share_text += f"🎁 Бонус: +{bonus} баллов!\n"
        share_text += f"\nПримешь вызов? 👉 @{bot_username}"  # noqa: RUF001
    else:
        share_text = (
            "📖 Тест по 1 Посланию Петра\n\n"
            f"{level_name}\n"
            f"{progress_bar} {percentage}%\n\n"
            f"{result_emoji} {correct_count}/{total} — {result_comment}\n"
        )
        max_streak = data.get("max_streak", 0)
        if max_streak >= 3:
            share_text += f"🔥 Серия: {max_streak} подряд!\n"
        share_text += f"\nПроверь свои знания 👉 @{bot_username}"  # noqa: RUF001

    keyboard.append([
        InlineKeyboardButton(
            "📤 Поделиться результатом",
            switch_inline_query=share_text,
        )
    ])
    keyboard.append([
        InlineKeyboardButton("📚 Выбрать другой тест", callback_data="start_test"),
        InlineKeyboardButton("🏠 Меню", callback_data="back_to_main"),
    ])

    try:
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as exc:
        logger.error("send_final_results_menu: ошибка отправки: %s", exc)


def validate_legacy_bridge(legacy_module) -> None:
    """Fail closed unless the transitional module exposes the deployed callable."""
    legacy_callable = getattr(legacy_module, "send_final_results_menu", None)
    if not callable(legacy_callable):
        raise TypeError("legacy module must expose callable send_final_results_menu")


def install_legacy_bridge(legacy_module) -> None:
    """Replace only the transitional result-menu callable with canonical authority."""
    validate_legacy_bridge(legacy_module)
    legacy_module.send_final_results_menu = send_final_results_menu


__all__ = ["install_legacy_bridge", "send_final_results_menu", "validate_legacy_bridge"]