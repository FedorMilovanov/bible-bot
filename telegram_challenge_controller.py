"""Production Challenge adapter using only ranking-eligible questions."""
from __future__ import annotations

import logging

from telegram.ext import ConversationHandler

import telegram_controller as quiz
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
from questions import pick_competitive_challenge_questions

logger = logging.getLogger(__name__)
_COMPETITIVE_MODES = frozenset({"random20", "hardcore20"})


async def challenge_start(update, context):
    query = update.callback_query
    await query.answer()
    mode = query.data.replace("challenge_start_", "")
    if mode not in _COMPETITIVE_MODES:
        await query.edit_message_text("\u26a0\ufe0f \u041d\u0435\u0438\u0437\u0432\u0435\u0441\u0442\u043d\u044b\u0439 Challenge.")
        return ConversationHandler.END
    try:
        questions = pick_competitive_challenge_questions(mode)
    except ValueError:
        logger.exception("competitive Challenge question selection failed")
        await query.edit_message_text("\u26a0\ufe0f \u0412\u043e\u043f\u0440\u043e\u0441\u044b Challenge \u0432\u0440\u0435\u043c\u0435\u043d\u043d\u043e \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b.")
        return ConversationHandler.END

    time_limit = 10 if mode == "hardcore20" else None
    mode_name = "\U0001f3b2 Random Challenge" if mode == "random20" else "\U0001f480 Hardcore Random"
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
        f"{mode_name}\n\n\U0001f4cb {len(questions)} \u0432\u043e\u043f\u0440\u043e\u0441\u043e\u0432\n\u041f\u043e\u0435\u0445\u0430\u043b\u0438! \U0001f4aa",
        parse_mode="Markdown",
    )
    await quiz.send_challenge_question(context.bot, update.effective_user.id)
    return quiz.ANSWERING


async def restart_session_handler(update, context):
    query = update.callback_query
    user_id = query.from_user.id
    try:
        resolved = resolve_session_action(query.data, "rst", user_id)
    except LegacySessionActionUnavailable:
        await query.answer("\u26a0\ufe0f \u0411\u0430\u0437\u0430 \u0441\u0435\u0441\u0441\u0438\u0439 \u0432\u0440\u0435\u043c\u0435\u043d\u043d\u043e \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0430.", show_alert=True)
        return
    except LegacySessionActionStale:
        await query.answer("\u042d\u0442\u0430 \u043a\u043d\u043e\u043f\u043a\u0430 \u0443\u0436\u0435 \u0443\u0441\u0442\u0430\u0440\u0435\u043b\u0430.", show_alert=True)
        return

    session = resolved.session
    mode = session.get("mode")
    if mode not in _COMPETITIVE_MODES:
        return await quiz.restart_session_handler(update, context)

    try:
        if persisted_is_retry(session):
            await query.answer(
                "\u041f\u043e\u0432\u0442\u043e\u0440\u0435\u043d\u0438\u0435 \u043e\u0448\u0438\u0431\u043e\u043a \u043d\u0435\u043b\u044c\u0437\u044f \u043f\u0435\u0440\u0435\u0437\u0430\u043f\u0443\u0441\u043a\u0430\u0442\u044c. \u041e\u0442\u043c\u0435\u043d\u0438 \u0435\u0433\u043e \u0438 \u0437\u0430\u043f\u0443\u0441\u0442\u0438 \u0437\u0430\u043d\u043e\u0432\u043e \u0438\u0437 \u0440\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442\u043e\u0432.",
                show_alert=True,
            )
            return
    except LegacyRetryPolicyInvalid:
        await query.answer("\u26a0\ufe0f \u041f\u043e\u043b\u0438\u0442\u0438\u043a\u0430 \u0441\u043e\u0445\u0440\u0430\u043d\u0451\u043d\u043d\u043e\u0439 \u043f\u043e\u043f\u044b\u0442\u043a\u0438 \u043f\u043e\u0432\u0440\u0435\u0436\u0434\u0435\u043d\u0430.", show_alert=True)
        return

    try:
        questions = pick_competitive_challenge_questions(mode)
    except ValueError:
        logger.exception("competitive Challenge restart selection failed")
        await query.answer(
            "\u26a0\ufe0f \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0441\u043e\u0431\u0440\u0430\u0442\u044c \u0432\u043e\u043f\u0440\u043e\u0441\u044b Challenge \u0434\u043b\u044f \u043f\u0435\u0440\u0435\u0437\u0430\u043f\u0443\u0441\u043a\u0430.",
            show_alert=True,
        )
        return

    try:
        result = restart_owned_quiz_attempt(
            resolved.session_id,
            user_id,
            expected_attempt_id=resolved.attempt_id,
            mode=mode,
            question_ids=[quiz.legacy.get_qid(item) for item in questions],
            questions_data=questions,
            level_key=session.get("level_key"),
            level_name=session.get("level_name"),
            time_limit=session.get("time_limit"),
            chat_id=query.message.chat_id,
        )
    except QuizSessionLifecycleUnavailable:
        await query.answer("\u26a0\ufe0f \u0411\u0430\u0437\u0430 \u0441\u0435\u0441\u0441\u0438\u0439 \u0432\u0440\u0435\u043c\u0435\u043d\u043d\u043e \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0430.", show_alert=True)
        return
    except QuizSessionLifecycleConflict:
        await query.answer("\u041f\u043e\u043f\u044b\u0442\u043a\u0430 \u0443\u0436\u0435 \u0438\u0437\u043c\u0435\u043d\u0438\u043b\u0430\u0441\u044c. \u041e\u0442\u043a\u0440\u043e\u0439 /status.", show_alert=True)
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
        f"\U0001f501 *\u041d\u0430\u0447\u0438\u043d\u0430\u0435\u043c \u0437\u0430\u043d\u043e\u0432\u043e*\n_{data.get('level_name', 'Challenge')}_\n\n"
        f"\u0412\u043e\u043f\u0440\u043e\u0441\u043e\u0432: {len(questions)}",
        parse_mode="Markdown",
    )
    await quiz.send_challenge_question(context.bot, user_id)
