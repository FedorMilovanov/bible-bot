"""Production Challenge adapter using only ranking-eligible questions."""
from __future__ import annotations

import asyncio
import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ConversationHandler

import telegram_controller as quiz
from database import is_bonus_eligible
from legacy_retry_policy import LegacyRetryPolicyInvalid, persisted_is_retry
from legacy_session_action import (
    LegacySessionActionStale,
    LegacySessionActionUnavailable,
    resolve_session_action,
)
from legacy_session_lifecycle import (
    QuizSessionLifecycleConflict,
    QuizSessionLifecycleUnavailable,
    restart_owned_quiz_attempt,
)
from question_identity import get_qid
from questions import pick_competitive_challenge_questions

logger = logging.getLogger(__name__)
_COMPETITIVE_MODES = frozenset({"random20", "hardcore20"})


async def challenge_menu(update, context):
    """Render Challenge bonus availability without blocking the PTB event loop."""
    del context
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    normal_ok, hardcore_ok = await asyncio.gather(
        asyncio.to_thread(is_bonus_eligible, user_id, "random20"),
        asyncio.to_thread(is_bonus_eligible, user_id, "hardcore20"),
    )

    def badge(ok: bool) -> str:
        return "✅ доступен" if ok else "❌ уже получен"

    text = (
        "🎲 *RANDOM CHALLENGE (20)*\n\n"
        "🎁 Бонус сегодня:\n"
        f"• 🎲 Normal:   {badge(normal_ok)}\n"
        f"• 💀 Hardcore: {badge(hardcore_ok)}\n\n"
        "Выбери режим:"
    )
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "🎲 Normal (20) — без таймера",
                    callback_data="challenge_rules_random20",
                )
            ],
            [
                InlineKeyboardButton(
                    "💀 Hardcore (20) — 10 сек",
                    callback_data="challenge_rules_hardcore20",
                )
            ],
            [
                InlineKeyboardButton(
                    "🏆 Лидерборд недели",
                    callback_data="weekly_lb_random20",
                )
            ],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")],
        ]),
        parse_mode="Markdown",
    )


async def challenge_rules(update, context):
    """Render one Challenge rule card with the bonus read off-loop."""
    del context
    query = update.callback_query
    await query.answer()
    mode = query.data.replace("challenge_rules_", "")
    user_id = query.from_user.id
    eligible = await asyncio.to_thread(is_bonus_eligible, user_id, mode)
    today_status = "✅ доступен" if eligible else "❌ уже получен сегодня"
    title = (
        "🎲 *Random Challenge (20)*"
        if mode == "random20"
        else "💀 *Hardcore Random (20)*"
    )
    timer_info = "• без таймера" if mode == "random20" else "• ⏱ 10 сек на вопрос"
    await query.edit_message_text(
        f"{title}\n━━━━━━━━━━━━━━━━\n{timer_info}\n"
        f"*Статус бонуса:* {today_status}",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "▶️ Начать!",
                    callback_data=f"challenge_start_{mode}",
                )
            ],
            [InlineKeyboardButton("⬅️ Назад", callback_data="challenge_menu")],
        ]),
        parse_mode="Markdown",
    )


async def challenge_start(update, context):
    query = update.callback_query
    await query.answer()
    mode = query.data.replace("challenge_start_", "")
    if mode not in _COMPETITIVE_MODES:
        await query.edit_message_text("⚠️ Неизвестный Challenge.")
        return ConversationHandler.END
    try:
        questions = pick_competitive_challenge_questions(mode)
    except ValueError:
        logger.exception("competitive Challenge question selection failed")
        await query.edit_message_text("⚠️ Вопросы Challenge временно недоступны.")
        return ConversationHandler.END

    time_limit = 10 if mode == "hardcore20" else None
    mode_name = "🎲 Random Challenge" if mode == "random20" else "💀 Hardcore Random"
    data = await quiz._launch_attempt(
        user=update.effective_user,
        bot=context.bot,
        chat_id=query.message.chat_id,
        mode=mode,
        questions=questions,
        level_key=mode,
        level_name=mode_name,
        time_limit=time_limit,
    )
    if data is None:
        return ConversationHandler.END
    await query.edit_message_text(
        f"{mode_name}\n\n📋 {len(questions)} вопросов\nПоехали! 💪",
        parse_mode="Markdown",
    )
    await quiz.send_challenge_question(context.bot, update.effective_user.id)
    return quiz.ANSWERING


async def restart_session_handler(update, context):
    query = update.callback_query
    user_id = query.from_user.id
    try:
        resolved = await quiz._run_blocking_io(
            resolve_session_action,
            query.data,
            "rst",
            user_id,
        )
    except LegacySessionActionUnavailable:
        await query.answer("⚠️ База сессий временно недоступна.", show_alert=True)
        return
    except LegacySessionActionStale:
        await query.answer("Эта кнопка уже устарела.", show_alert=True)
        return

    session = resolved.session
    mode = session.get("mode")
    if mode not in _COMPETITIVE_MODES:
        return await quiz.restart_session_handler(update, context)

    try:
        if persisted_is_retry(session):
            await query.answer(
                "Повторение ошибок нельзя перезапускать. Отмени его и запусти заново из результатов.",
                show_alert=True,
            )
            return
    except LegacyRetryPolicyInvalid:
        await query.answer("⚠️ Политика сохранённой попытки повреждена.", show_alert=True)
        return

    try:
        questions = pick_competitive_challenge_questions(mode)
    except ValueError:
        logger.exception("competitive Challenge restart selection failed")
        await query.answer(
            "⚠️ Не удалось собрать вопросы Challenge для перезапуска.",
            show_alert=True,
        )
        return

    try:
        result = await quiz._run_blocking_io(
            restart_owned_quiz_attempt,
            resolved.session_id,
            user_id,
            expected_attempt_id=resolved.attempt_id,
            mode=mode,
            question_ids=[get_qid(item) for item in questions],
            questions_data=questions,
            level_key=session.get("level_key"),
            level_name=session.get("level_name"),
            time_limit=session.get("time_limit"),
            chat_id=query.message.chat_id,
        )
    except QuizSessionLifecycleUnavailable:
        await query.answer("⚠️ База сессий временно недоступна.", show_alert=True)
        return
    except QuizSessionLifecycleConflict:
        await query.answer("Попытка уже изменилась. Открой /status.", show_alert=True)
        return

    await query.answer()
    data = quiz._hydrate_session(
        user_id,
        result["session"],
        chat_id=query.message.chat_id,
        username=query.from_user.username,
        first_name=query.from_user.first_name,
    )
    await query.edit_message_text(
        f"🔁 *Начинаем заново*\n_{data.get('level_name', 'Challenge')}_\n\n"
        f"Вопросов: {len(questions)}",
        parse_mode="Markdown",
    )
    await quiz.send_challenge_question(context.bot, user_id)
