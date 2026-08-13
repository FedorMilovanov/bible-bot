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
        await query.edit_message_text("Unknown Challenge.")
        return ConversationHandler.END
    try:
        questions = pick_competitive_challenge_questions(mode)
    except ValueError:
        logger.exception("competitive Challenge question selection failed")
        await query.edit_message_text("Challenge questions are temporarily unavailable.")
        return ConversationHandler.END

    time_limit = 10 if mode == "hardcore20" else None
    mode_name = "Random Challenge" if mode == "random20" else "Hardcore Random"
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
        f"{mode_name}\n\n{len(questions)} questions. Start!",
    )
    await quiz.send_challenge_question(context.bot, update.effective_user.id)
    return quiz.ANSWERING


async def restart_session_handler(update, context):
    query = update.callback_query
    user_id = query.from_user.id
    try:
        resolved = resolve_session_action(query.data, "rst", user_id)
    except LegacySessionActionUnavailable:
        await query.answer("Session store unavailable.", show_alert=True)
        return
    except LegacySessionActionStale:
        await query.answer("This action is stale.", show_alert=True)
        return

    session = resolved.session
    mode = session.get("mode")
    if mode not in _COMPETITIVE_MODES:
        return await quiz.restart_session_handler(update, context)

    try:
        if persisted_is_retry(session):
            await query.answer("Retry practice cannot be restarted.", show_alert=True)
            return
    except LegacyRetryPolicyInvalid:
        await query.answer("Stored retry policy is invalid.", show_alert=True)
        return

    try:
        questions = pick_competitive_challenge_questions(mode)
    except ValueError:
        logger.exception("competitive Challenge restart selection failed")
        await query.answer("Challenge questions are unavailable.", show_alert=True)
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
        await query.answer("Session store unavailable.", show_alert=True)
        return
    except QuizSessionLifecycleConflict:
        await query.answer("Attempt changed; open /status.", show_alert=True)
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
        f"Restarted {data.get('level_name', 'Challenge')}: {len(questions)} questions"
    )
    await quiz.send_challenge_question(context.bot, user_id)
