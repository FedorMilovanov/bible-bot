# ruff: noqa: RUF001
"""Bot-free production Telegram quiz controller.

Mongo owns durable quiz attempts. This module owns only the live Telegram quiz
state machine and its process-local projection; presentation/catalog helpers are
resolved through focused canonical modules instead of ``bot.py``.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Callable
from typing import Any, TypeVar

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import ConversationHandler

import achievement_catalog
import question_identity
import quiz_answer_history as answer_history
import telegram_answer_animation as answer_animation
import telegram_main_menu as main_menu
import telegram_quiz_result_menu as result_menu
import telegram_quiz_runtime_state as quiz_runtime
from config import (
    FEEDBACK_DELAY_CORRECT,
    FEEDBACK_DELAY_WRONG,
    QUIZ_TIMEOUT,
    SPEED_MODE_TIMEOUT,
    TIMED_MODE_TIMEOUT,
)
from course_catalog import (
    SURFACE_TELEGRAM,
    CourseCatalogError,
    resolve_course,
    resolve_course_pool,
)
from database import (
    format_time,
    get_user_position,
    init_user_stats,
    record_question_stat,
    touch_user_activity,
)
from legacy_live_answer import (
    LegacyLiveAnswerStale,
    LegacyLiveStateInvalid,
    apply_live_answer_once,
    apply_live_timeout_once,
    build_live_answer_callback,
)
from legacy_live_finalize import LegacyLiveFinalizationPending, finalize_live_persisted_attempt
from legacy_live_question import (
    LegacyLiveQuestionStateInvalid,
    capture_live_question_target,
    mark_live_question_sent,
)
from legacy_question_timer import (
    LegacyQuestionTimerConflict,
    LegacyQuestionTimerUnavailable,
    question_is_timed_out,
)
from legacy_restart_policy import (
    LegacyRestartStateInvalid,
    classify_restart_session,
    restart_timeout_route,
)
from legacy_retry_policy import LegacyRetryPolicyInvalid, persisted_is_retry
from legacy_session_access import (
    QuizSessionAccessSchemaInvalid,
    QuizSessionAccessUnavailable,
    get_active_quiz_session_strict,
)
from legacy_session_action import (
    LegacySessionActionStale,
    LegacySessionActionUnavailable,
    resolve_session_action,
    session_action_payloads,
)
from legacy_session_control import (
    LegacySessionControlConflict,
    LegacySessionControlUnavailable,
    LegacySessionResultPending,
    cancel_current_incomplete_session,
)
from legacy_session_launch import (
    LegacySessionLaunchActiveAttempt,
    LegacySessionLaunchConflict,
    LegacySessionLaunchResultPending,
    LegacySessionLaunchUnavailable,
    launch_quiz_attempt,
)
from legacy_session_lifecycle import (
    QuizSessionLifecycleConflict,
    QuizSessionLifecycleUnavailable,
    cancel_owned_incomplete_quiz_attempt,
    restart_owned_quiz_attempt,
)
from legacy_session_recovery import (
    LegacyPersistedSessionModeInvalid,
    LegacyPersistedSessionStateInvalid,
    recovery_fields,
)
from questions import get_pool_by_key, pick_competitive_challenge_questions
from session_integrity import QuizSessionAnswerConflict, QuizSessionStoreUnavailable
from telegram_conversation_states import ANSWERING, BATTLE_ANSWERING
from utils import safe_edit

logger = logging.getLogger(__name__)
_T = TypeVar("_T")

# Canonical process-local mirrors exist before legacy bootstrap and are rebound
# into bot.py by telegram_production. There is exactly one runtime mapping.
user_data = quiz_runtime.get_user_data()


async def _run_blocking_io(
    function: Callable[..., _T],
    /,
    *args,
    **kwargs,
) -> _T:
    """Run one synchronous persistence/network boundary outside the PTB loop."""
    if kwargs:
        return await asyncio.to_thread(lambda: function(*args, **kwargs))
    return await asyncio.to_thread(function, *args)


async def _touch_activity(user_id: int) -> None:
    data = user_data.get(user_id)
    if isinstance(data, dict):
        data["last_activity"] = time.time()
    await _run_blocking_io(touch_user_activity, user_id)


def _achievement_rewards() -> dict[str, int]:
    return {
        key: max(0, int(meta.get("reward", 0) or 0))
        for key, meta in achievement_catalog.ACHIEVEMENTS.items()
    }


def _hydrate_session(
    user_id: int,
    session: dict,
    *,
    chat_id: int | None = None,
    username: str | None = None,
    first_name: str | None = None,
) -> dict:
    """Rebuild runtime/UI fields from one validated durable session snapshot."""
    fields = recovery_fields(session)
    try:
        is_retry = persisted_is_retry(session)
    except LegacyRetryPolicyInvalid as exc:
        raise LegacyPersistedSessionStateInvalid(
            "persisted retry policy is invalid"
        ) from exc
    resolved_chat = chat_id if chat_id is not None else fields.get("quiz_chat_id")
    data = quiz_runtime.create_session_data(
        user_id=user_id,
        session_id=fields["session_id"],
        attempt_id=fields["attempt_id"],
        questions=fields["questions"],
        level_name=fields["level_name"],
        chat_id=resolved_chat,
        current_question=fields["current_question"],
        answered_questions=fields["answered_questions"],
        level_key=fields["level_key"],
        correct_answers=fields["correct_answers"],
        start_time=fields["start_time"],
        last_activity=time.time(),
        is_battle=False,
        battle_points=0,
        is_retry=is_retry,
        is_challenge=fields["is_challenge"],
        challenge_mode=fields["challenge_mode"],
        challenge_time_limit=fields["challenge_time_limit"],
        quiz_mode=fields["quiz_mode"],
        score_multiplier=fields["score_multiplier"],
        quiz_time_limit=fields["quiz_time_limit"],
        current_streak=fields["current_streak"],
        max_streak=fields["max_streak"],
        fastest_answer=fields["fastest_answer"],
        result_pending=fields["result_pending"],
        persisted_result_time=fields["persisted_result_time"],
        persisted_completed_at=fields["persisted_completed_at"],
        username=username,
        first_name=first_name or "Игрок",
    )
    data["question_sent_at"] = session.get("question_sent_at")
    data["session_id"] = fields["session_id"]
    data["attempt_id"] = fields["attempt_id"]
    data["user_id"] = user_id
    user_data[user_id] = data
    return data


def _lifecycle_keyboard(session: dict, *, include_cancel: bool = True) -> InlineKeyboardMarkup:
    payloads = session_action_payloads(session)
    rows = [[InlineKeyboardButton("▶️ Продолжить", callback_data=payloads["res"])]]
    if session.get("is_retry") is not True:
        rows.append([InlineKeyboardButton("🔁 Начать заново", callback_data=payloads["rst"])])
    if include_cancel:
        rows.append([InlineKeyboardButton("❌ Отменить", callback_data=payloads["can"])])
    return InlineKeyboardMarkup(rows)


def _session_progress(session: dict) -> tuple[int, int, str]:
    questions = session.get("questions_data", [])
    total = len(questions) if isinstance(questions, list) else 0
    current = session.get("current_index", 0)
    if isinstance(current, bool) or not isinstance(current, int):
        current = 0
    return current, total, str(session.get("level_name") or "тест")


async def _show_active_attempt(target: Any, session: dict, *, edit: bool) -> None:
    current, total, level_name = _session_progress(session)
    if session.get("is_retry") is True:
        actions = "Продолжить или отменить повторение ошибок?"
    else:
        actions = "Продолжить, начать заново или отменить текущую попытку?"
    text = (
        f"⏸ *Тест уже идёт: {min(current + 1, max(total, 1))}/{max(total, 1)}*\n"
        f"_{level_name}_\n\n"
        f"{actions}"
    )
    keyboard = _lifecycle_keyboard(session)
    if edit:
        await target.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)
    else:
        await target.reply_text(text, parse_mode="Markdown", reply_markup=keyboard)


async def _send_claimed_achievements(bot, chat_id: int, keys: list[str]) -> None:
    for key in keys:
        meta = achievement_catalog.ACHIEVEMENTS.get(key)
        if not meta:
            continue
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    "🏆 *ДОСТИЖЕНИЕ РАЗБЛОКИРОВАНО!*\n\n"
                    f"{meta.get('icon', '🏅')} *{meta.get('name', key)}*\n"
                    f"_{meta.get('description', '')}_\n\n"
                    f"🎁 Награда: *+{int(meta.get('reward', 0) or 0)} баллов*"
                ),
                parse_mode="Markdown",
            )
        except Exception:
            logger.warning("achievement UI delivery failed for user chat %s", chat_id, exc_info=True)


async def _render_result(bot, user_id: int, outcome, *, retry_drill: bool = False) -> None:
    data = user_data[user_id]
    score = int(outcome.score)
    total = int(outcome.total)
    time_seconds = float(outcome.time_seconds)
    result = outcome.result if isinstance(outcome.result, dict) else {}
    chat_id = data.get("quiz_chat_id")
    if not chat_id:
        logger.error("cannot render quiz result without chat id for user %s", user_id)
        return

    data["correct_answers"] = score
    data["result_pending"] = False
    data["user_id"] = user_id
    percentage = round(score / max(total, 1) * 100)
    position, _entry = await _run_blocking_io(get_user_position, user_id)
    position_text = f"#{position}" if position else "—"

    wrong = []
    for item in data.get("answered_questions", []):
        try:
            if answer_history.is_wrong(item):
                wrong.append(item)
        except Exception:
            continue
    data["wrong_answers"] = wrong

    if result.get("practice"):
        retry_drill = True
        text = (
            "🔁 *ПОВТОРЕНИЕ ОШИБОК ЗАВЕРШЕНО*\n\n"
            f"*Правильно:* {score}/{total} ({percentage}%)\n"
            f"*Время:* {format_time(time_seconds)}\n\n"
            "Это тренировочный режим — баллы, бонусы и достижения не начисляются."
        )
    elif outcome.is_challenge:
        mode = data.get("challenge_mode") or data.get("level_key")
        mode_name = "🎲 Random Challenge" if mode == "random20" else "💀 Hardcore Random"
        base = int(result.get("earned_base", 0) or 0)
        bonus_state = result.get("bonus") if isinstance(result.get("bonus"), dict) else {}
        bonus = int(bonus_state.get("bonus", 0) or 0)
        data["challenge_bonus"] = bonus
        if percentage == 100:
            grade = "🌟 Идеально!"
        elif percentage >= 90:
            grade = "🔥 Отлично!"
        elif percentage >= 75:
            grade = "👍 Хорошо"
        else:
            grade = "📚 Нужно повторить"
        text = (
            f"━━━━━━━━━━━━━━━━\n{mode_name}\n━━━━━━━━━━━━━━━━\n"
            f"📊 *{score}/{total}* ({percentage}%) {grade}\n"
            f"⏱ Время: *{format_time(time_seconds)}*\n"
            f"🏅 Позиция: *{position_text}*\n"
            f"💎 Очки: *+{base}*"
        )
        if bonus_state.get("eligible"):
            text += f"\n🎁 Бонус: *+{bonus}*"
        else:
            text += "\n🎁 Бонус: _уже использован_"
        badge_messages = result.get("new_challenge_badges", [])
        if badge_messages:
            text += "\n\n" + "\n".join(str(item) for item in badge_messages)
    else:
        base = int(result.get("earned_base", 0) or 0)
        daily = result.get("daily_bonus") if isinstance(result.get("daily_bonus"), dict) else {}
        if percentage >= 90:
            grade = "Отлично! 🌟"
        elif percentage >= 70:
            grade = "Хорошо! 👍"
        elif percentage >= 50:
            grade = "Удовлетворительно 📖"
        else:
            grade = "Нужно повторить 📚"
        text = (
            "🏆 *РЕЗУЛЬТАТЫ*\n\n"
            f"*Категория:* {data.get('level_name', 'Тест')}\n"
            f"*Правильно:* {score}/{total} ({percentage}%)\n"
            f"*Баллы:* +{base} 💎\n"
            f"*Время:* {format_time(time_seconds)}\n"
            f"*Позиция:* {position_text}\n"
            f"*Оценка:* {grade}"
        )
        if int(daily.get("bonus", 0) or 0) > 0:
            text += f"\n🌅 Дневной бонус: *+{int(daily['bonus'])}*"

    try:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
    except Exception:
        logger.error("result card delivery failed for user %s", user_id, exc_info=True)

    if not retry_drill:
        await _send_claimed_achievements(
            bot,
            chat_id,
            list(result.get("new_achievements", []) or []),
        )

    try:
        await result_menu.send_final_results_menu(bot, chat_id, data)
    except Exception:
        logger.error("final result menu delivery failed for user %s", user_id, exc_info=True)


class _MemoryResultOutcome:
    def __init__(self, data: dict):
        self.score = int(data.get("correct_answers", 0) or 0)
        self.total = len(data.get("questions", []))
        self.time_seconds = max(0.0, time.time() - float(data.get("start_time", time.time())))
        self.is_challenge = False
        self.result = {
            "scored": False,
            "earned_base": 0,
            "daily_bonus": {"bonus": 0, "eligible": False, "claimed_now": False},
            "new_achievements": [],
        }


async def show_results(bot, user_id: int):
    data = user_data.get(user_id)
    if not data:
        return
    if data.get("is_retry") and not data.get("session_id"):
        await _render_result(bot, user_id, _MemoryResultOutcome(data), retry_drill=True)
        return
    try:
        outcome = await _run_blocking_io(
            lambda: finalize_live_persisted_attempt(
                user_id=user_id,
                data=data,
                username=data.get("username"),
                first_name=data.get("first_name"),
                achievement_rewards=_achievement_rewards(),
            )
        )
    except LegacyLiveFinalizationPending:
        data["result_pending"] = True
        chat_id = data.get("quiz_chat_id")
        if chat_id:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    "⚠️ Результат уже завершён, но база временно не подтвердила всю финализацию.\n"
                    "Ничего не потеряно: нажми /status через несколько секунд для безопасного повтора."
                ),
            )
        return
    await _render_result(bot, user_id, outcome)


async def show_challenge_results(bot, user_id: int):
    data = user_data.get(user_id)
    if not data:
        return
    try:
        outcome = await _run_blocking_io(
            lambda: finalize_live_persisted_attempt(
                user_id=user_id,
                data=data,
                username=data.get("username"),
                first_name=data.get("first_name"),
                achievement_rewards=_achievement_rewards(),
            )
        )
    except LegacyLiveFinalizationPending:
        data["result_pending"] = True
        chat_id = data.get("quiz_chat_id")
        if chat_id:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    "⚠️ Challenge завершён, но финализация пока не подтверждена.\n"
                    "Результат сохранён как pending; /status безопасно повторит операцию."
                ),
            )
        return
    await _render_result(bot, user_id, outcome)


async def _finalize_active_session(bot, user, session: dict, chat_id: int) -> bool:
    try:
        data = _hydrate_session(
            user.id,
            session,
            chat_id=chat_id,
            username=user.username,
            first_name=user.first_name,
        )
    except (LegacyPersistedSessionModeInvalid, LegacyPersistedSessionStateInvalid, ValueError):
        await bot.send_message(
            chat_id=chat_id,
            text="⚠️ Сохранённая сессия противоречива. Новую попытку не запускаю; требуется проверка состояния.",
        )
        return False
    if data.get("is_challenge"):
        await show_challenge_results(bot, user.id)
    else:
        await show_results(bot, user.id)
    return not bool(user_data.get(user.id, {}).get("result_pending"))


async def _launch_attempt(
    *,
    user,
    bot,
    chat_id: int,
    mode: str,
    questions: list[dict],
    level_key: str,
    level_name: str,
    time_limit: int | None,
    is_retry: bool = False,
    retry_after_finalize: bool = True,
) -> dict | None:
    question_ids = [question_identity.get_qid(question) for question in questions]
    try:
        outcome = await _run_blocking_io(
            lambda: launch_quiz_attempt(
                user_id=user.id,
                mode=mode,
                question_ids=question_ids,
                questions_data=questions,
                level_key=level_key,
                level_name=level_name,
                time_limit=time_limit,
                chat_id=chat_id,
                is_retry=is_retry,
            )
        )
    except LegacySessionLaunchActiveAttempt as exc:
        await bot.send_message(
            chat_id=chat_id,
            text="⏸ У тебя уже есть незавершённый тест.",
            reply_markup=_lifecycle_keyboard(exc.session),
        )
        return None
    except LegacySessionLaunchResultPending:
        if not retry_after_finalize:
            await bot.send_message(
                chat_id=chat_id,
                text="⚠️ Предыдущий результат ещё финализируется. Открой /status и повтори запуск после завершения.",
            )
            return None
        try:
            active = await _run_blocking_io(get_active_quiz_session_strict, user.id)
        except (QuizSessionAccessUnavailable, QuizSessionAccessSchemaInvalid):
            await bot.send_message(chat_id=chat_id, text="⚠️ База сессий временно недоступна.")
            return None
        if active is None or not await _finalize_active_session(bot, user, active, chat_id):
            return None
        return await _launch_attempt(
            user=user,
            bot=bot,
            chat_id=chat_id,
            mode=mode,
            questions=questions,
            level_key=level_key,
            level_name=level_name,
            time_limit=time_limit,
            is_retry=is_retry,
            retry_after_finalize=False,
        )
    except LegacySessionLaunchUnavailable:
        await bot.send_message(chat_id=chat_id, text="⚠️ База сессий временно недоступна. Попробуй ещё раз.")
        return None
    except LegacySessionLaunchConflict:
        await bot.send_message(
            chat_id=chat_id,
            text="⚠️ Состояние активной попытки изменилось. Открой /status перед новым запуском.",
        )
        return None

    try:
        return _hydrate_session(
            user.id,
            outcome.session,
            chat_id=chat_id,
            username=user.username,
            first_name=user.first_name,
        )
    except (LegacyPersistedSessionModeInvalid, LegacyPersistedSessionStateInvalid, ValueError):
        logger.exception("new durable quiz session could not be hydrated")
        await bot.send_message(
            chat_id=chat_id,
            text="⚠️ Новая попытка записана, но её состояние не удалось безопасно прочитать. Используй /status.",
        )
        return None


async def _launch_level_from_query(
    update: Update,
    context,
    level_key: str,
    quiz_mode: str,
    time_limit: int | None,
) -> None:
    query = update.callback_query
    try:
        entry = resolve_course(level_key, surface=SURFACE_TELEGRAM, mode=quiz_mode)
        pool = resolve_course_pool(entry)
    except (CourseCatalogError, KeyError):
        await query.edit_message_text("⚠️ Уровень не найден.")
        return
    count = min(entry.default_question_count, len(pool))
    if count <= 0:
        await query.edit_message_text("⚠️ Вопросы не найдены.")
        return
    questions = random.sample(pool, count)
    data = await _launch_attempt(
        user=update.effective_user,
        bot=context.bot,
        chat_id=query.message.chat_id,
        mode="level",
        questions=questions,
        level_key=entry.pool_key,
        level_name=entry.title,
        time_limit=time_limit,
    )
    if data is None:
        return
    labels = {
        "relaxed": "🧘 Без таймера",
        "timed": f"⏱ {TIMED_MODE_TIMEOUT} сек / ×1.5",
        "speed": f"⚡ {SPEED_MODE_TIMEOUT} сек / ×2",
    }
    await query.edit_message_text(
        f"*{entry.title}*\n\n📝 Вопросов: {len(questions)} · {labels.get(quiz_mode, '')}\nНачинаем!",
        parse_mode="Markdown",
    )
    await send_question(context.bot, update.effective_user.id)


async def relaxed_mode_handler(update: Update, context):
    await update.callback_query.answer()
    await _launch_level_from_query(
        update,
        context,
        update.callback_query.data.replace("relaxed_mode_", ""),
        "relaxed",
        None,
    )


async def timed_mode_handler(update: Update, context):
    await update.callback_query.answer()
    await _launch_level_from_query(
        update,
        context,
        update.callback_query.data.replace("timed_mode_", ""),
        "timed",
        TIMED_MODE_TIMEOUT,
    )


async def speed_mode_handler(update: Update, context):
    await update.callback_query.answer()
    await _launch_level_from_query(
        update,
        context,
        update.callback_query.data.replace("speed_mode_", ""),
        "speed",
        SPEED_MODE_TIMEOUT,
    )


async def random_all_start_handler(update: Update, context):
    query = update.callback_query
    await query.answer()
    pool = get_pool_by_key("random_all")
    questions = random.sample(pool, min(10, len(pool))) if pool else []
    if not questions:
        await query.edit_message_text("⚠️ Вопросы не найдены.")
        return
    data = await _launch_attempt(
        user=update.effective_user,
        bot=context.bot,
        chat_id=query.message.chat_id,
        mode="level",
        questions=questions,
        level_key="random_all",
        level_name="🎲 Случайный режим (все темы)",
        time_limit=None,
    )
    if data is None:
        return
    await query.edit_message_text(
        f"🎲 *Случайный режим*\n\n📝 Вопросов: {len(questions)} · 🧘 Без таймера\nНачинаем!",
        parse_mode="Markdown",
    )
    await send_question(context.bot, update.effective_user.id)


async def random_command(update: Update, context):
    user = update.effective_user
    pool = get_pool_by_key("random_all")
    questions = random.sample(pool, min(10, len(pool))) if pool else []
    if not questions:
        await update.message.reply_text("⚠️ Вопросы не найдены.", reply_markup=main_menu.main_keyboard())
        return
    data = await _launch_attempt(
        user=user,
        bot=context.bot,
        chat_id=update.effective_chat.id,
        mode="level",
        questions=questions,
        level_key="random_all",
        level_name="🎲 Случайный режим (все темы)",
        time_limit=None,
    )
    if data is None:
        return
    await update.message.reply_text(
        f"🎲 *Случайный тест*\n{len(questions)} вопросов из всех тем\n\nНачинаем!",
        parse_mode="Markdown",
    )
    await send_question(context.bot, user.id)


async def intro_start_handler(update: Update, context):
    await update.callback_query.answer()
    await _launch_level_from_query(
        update,
        context,
        update.callback_query.data.replace("intro_start_", ""),
        "relaxed",
        None,
    )


async def challenge_start(update: Update, context):
    query = update.callback_query
    await query.answer()
    mode = query.data.replace("challenge_start_", "")
    if mode not in {"random20", "hardcore20"}:
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
    data = await _launch_attempt(
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
    await send_challenge_question(context.bot, update.effective_user.id)
    return ANSWERING


def _time_limit(data: dict) -> int | None:
    if data.get("is_challenge"):
        return data.get("challenge_time_limit")
    return data.get("quiz_time_limit")


def _cancel_runtime_timer(data: dict) -> None:
    task = data.get("timer_task")
    if task and not task.done():
        task.cancel()
    data["timer_task"] = None
    countdown = data.get("countdown_task")
    if countdown and not countdown.done():
        countdown.cancel()
    data["countdown_task"] = None


async def _disable_question_keyboard(bot, data: dict) -> None:
    chat_id = data.get("quiz_chat_id")
    message_id = data.get("quiz_message_id")
    if chat_id and message_id:
        try:
            await bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=None,
            )
        except Exception:
            pass


async def _send_current_question(bot, user_id: int, prefix: str) -> None:
    data = user_data.get(user_id)
    if not data:
        return
    index = data.get("current_question", 0)
    questions = data.get("questions", [])
    if not isinstance(index, int) or isinstance(index, bool) or index < 0:
        return
    if index >= len(questions):
        if data.get("is_challenge"):
            await show_challenge_results(bot, user_id)
        else:
            await show_results(bot, user_id)
        return

    question = questions[index]
    canonical = list(question.get("options", []))
    if not canonical:
        await bot.send_message(
            chat_id=data.get("quiz_chat_id"),
            text="⚠️ У вопроса нет вариантов ответа.",
        )
        return
    shuffled = canonical[:]
    random.shuffle(shuffled)
    data["current_options"] = shuffled
    correct_index = question.get("correct")
    if (
        not isinstance(correct_index, int)
        or isinstance(correct_index, bool)
        or not 0 <= correct_index < len(canonical)
    ):
        await bot.send_message(
            chat_id=data.get("quiz_chat_id"),
            text="⚠️ Вопрос повреждён. Продолжение остановлено.",
        )
        return
    data["current_correct_text"] = canonical[correct_index]

    try:
        target = capture_live_question_target(data)
        callbacks = [
            build_live_answer_callback(prefix, data, index, option_index)
            for option_index in range(len(shuffled))
        ]
    except (LegacyLiveAnswerStale, LegacyLiveStateInvalid, LegacyLiveQuestionStateInvalid, ValueError):
        logger.error("cannot build live question target for user %s", user_id, exc_info=True)
        await bot.send_message(
            chat_id=data.get("quiz_chat_id"),
            text="⚠️ Состояние вопроса изменилось. Используй /status.",
        )
        return

    options_text = "\n\n" + "\n".join(
        f"*{i + 1}.* {option}" for i, option in enumerate(shuffled)
    )
    buttons = [[
        InlineKeyboardButton(str(i + 1), callback_data=callback)
        for i, callback in enumerate(callbacks)
    ]]
    buttons.append([
        InlineKeyboardButton("⚠️ Неточность?", callback_data=f"report_inaccuracy_{index}"),
        InlineKeyboardButton("↩️ выйти", callback_data="cancel_quiz"),
    ])
    limit = _time_limit(data)
    timer_text = f" • ⏱ {limit} сек" if limit else ""
    progress = answer_history.build_progress_bar(
        index + 1,
        len(questions),
        data.get("answered_questions", []),
    )
    text = (
        f"*Вопрос {index + 1}/{len(questions)}*{timer_text}\n"
        f"{progress}\n\n{question['question']}{options_text}"
    )
    chat_id = data.get("quiz_chat_id")
    if not chat_id:
        logger.error("quiz chat id missing for user %s", user_id)
        return

    message_id = data.get("quiz_message_id")
    delivered = None
    if message_id:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=InlineKeyboardMarkup(buttons),
                parse_mode="Markdown",
            )
            delivered = message_id
        except Exception as exc:
            if "not modified" in str(exc).lower():
                delivered = message_id
    if delivered is None:
        try:
            message = await bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=InlineKeyboardMarkup(buttons),
                parse_mode="Markdown",
            )
            data["quiz_message_id"] = message.message_id
        except Exception:
            logger.error("question Telegram delivery failed for user %s", user_id, exc_info=True)
            return

    sent_at = time.time()
    try:
        canonical_sent_at = await _run_blocking_io(
            lambda: mark_live_question_sent(
                user_id,
                data,
                target,
                sent_at=sent_at,
            )
        )
    except (
        LegacyLiveAnswerStale,
        LegacyLiveQuestionStateInvalid,
        LegacyQuestionTimerConflict,
        LegacyQuestionTimerUnavailable,
    ):
        logger.warning(
            "question delivered but durable timer marker failed for user %s",
            user_id,
            exc_info=True,
        )
        await _disable_question_keyboard(bot, data)
        await bot.send_message(
            chat_id=chat_id,
            text=(
                "⚠️ Вопрос показан, но база не подтвердила его состояние. "
                "Ответы отключены; используй /status для безопасного продолжения."
            ),
        )
        return

    _cancel_runtime_timer(data)
    if limit:
        elapsed = max(0.0, time.time() - canonical_sent_at)
        remaining = max(0.0, float(limit) - elapsed)
        data["timer_task"] = asyncio.create_task(
            _handle_question_timeout(
                bot,
                user_id,
                target.attempt_id,
                target.question_index,
                remaining,
                int(limit),
            )
        )


async def send_question(bot, user_id: int, time_limit=None):
    del time_limit
    await _send_current_question(bot, user_id, "qa")


async def send_challenge_question(bot, user_id: int):
    await _send_current_question(bot, user_id, "cha")


async def _after_answer(bot, user_id: int, outcome) -> None:
    data = user_data.get(user_id)
    if not data:
        return
    if outcome.current_index < len(data.get("questions", [])):
        if data.get("is_challenge"):
            await send_challenge_question(bot, user_id)
        else:
            await send_question(bot, user_id)
    elif data.get("is_challenge"):
        await show_challenge_results(bot, user_id)
    else:
        await show_results(bot, user_id)


async def _handle_inline_answer(update: Update, context, prefix: str):
    query = update.callback_query
    user_id = query.from_user.id
    data = user_data.get(user_id)
    if not data:
        await query.answer(
            "⚠️ Сессия не загружена. Используй /status.",
            show_alert=True,
        )
        return

    lock = quiz_runtime.get_user_lock(user_id)
    async with lock:
        answer_now = time.time()
        try:
            outcome = await _run_blocking_io(
                lambda: apply_live_answer_once(
                    user_id,
                    data,
                    query.data,
                    prefix,
                    now=answer_now,
                )
            )
        except LegacyLiveAnswerStale:
            await query.answer("Эта кнопка уже устарела.")
            return
        except (QuizSessionStoreUnavailable, QuizSessionAnswerConflict):
            await query.answer(
                "⚠️ Ответ не сохранён. Повтори через несколько секунд.",
                show_alert=True,
            )
            return
        except LegacyLiveStateInvalid:
            logger.error("live answer state invalid for user %s", user_id, exc_info=True)
            await query.answer(
                "⚠️ Состояние теста изменилось. Используй /status.",
                show_alert=True,
            )
            return

        _cancel_runtime_timer(data)
        await query.answer()
        quiz_runtime.reset_bad_input(user_id)

        shuffled = data.get("current_options", [])
        option_index = outcome.option_index if outcome.option_index is not None else 0
        try:
            correct_slot = shuffled.index(outcome.correct_text)
        except ValueError:
            correct_slot = option_index
        try:
            is_numeric = bool(
                query.message.reply_markup
                and query.message.reply_markup.inline_keyboard
                and len(query.message.reply_markup.inline_keyboard[0]) > 1
            )
            await answer_animation.animate_answer_buttons(
                query,
                option_index,
                correct_slot,
                is_numeric,
                shuffled,
            )
        except Exception:
            logger.debug("answer animation failed", exc_info=True)

        if outcome.applied:
            try:
                await _run_blocking_io(
                    record_question_stat,
                    outcome.question_id,
                    data.get("level_key"),
                    outcome.is_correct,
                    float(outcome.latency_seconds or 0.0),
                )
            except Exception:
                logger.warning("question analytics failed after durable answer", exc_info=True)

        if outcome.is_correct:
            suffix = f" 🔥×{outcome.current_streak}" if outcome.current_streak >= 2 else ""
            feedback = f"✅ *Верно!*{suffix}\n\n_{outcome.correct_text}_"
            delay = FEEDBACK_DELAY_CORRECT
        else:
            feedback = f"❌ *Неверно*\n\n✅ Правильно: *{outcome.correct_text}*"
            delay = FEEDBACK_DELAY_WRONG

        chat_id = data.get("quiz_chat_id")
        message_id = data.get("quiz_message_id")
        if chat_id and message_id:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=feedback,
                    parse_mode="Markdown",
                )
            except Exception:
                pass
        await asyncio.sleep(delay)
        await _after_answer(context.bot, user_id, outcome)


async def quiz_inline_answer(update: Update, context):
    await _handle_inline_answer(update, context, "qa")


async def challenge_inline_answer(update: Update, context):
    await _handle_inline_answer(update, context, "cha")


async def _handle_question_timeout(
    bot,
    user_id: int,
    expected_attempt_id: str,
    expected_index: int,
    delay_seconds: float,
    timeout_seconds: int,
):
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    data = user_data.get(user_id)
    if not data:
        return
    lock = quiz_runtime.get_user_lock(user_id)
    async with lock:
        timeout_now = time.time()
        try:
            outcome = await _run_blocking_io(
                lambda: apply_live_timeout_once(
                    user_id,
                    data,
                    expected_index,
                    expected_attempt_id=expected_attempt_id,
                    now=timeout_now,
                )
            )
        except LegacyLiveAnswerStale:
            return
        except (QuizSessionStoreUnavailable, QuizSessionAnswerConflict):
            logger.warning(
                "timeout could not be durably recorded for user %s",
                user_id,
                exc_info=True,
            )
            await _disable_question_keyboard(bot, data)
            chat_id = data.get("quiz_chat_id")
            if chat_id:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        "⚠️ Время истекло, но база временно недоступна. "
                        "Ответы отключены; /status продолжит безопасно."
                    ),
                )
            return
        except LegacyLiveStateInvalid:
            logger.error("timeout state invalid for user %s", user_id, exc_info=True)
            return

        if outcome.applied:
            try:
                await _run_blocking_io(
                    record_question_stat,
                    outcome.question_id,
                    data.get("level_key"),
                    False,
                    float(outcome.latency_seconds or timeout_seconds),
                )
            except Exception:
                logger.warning("timeout analytics failed after durable answer", exc_info=True)

        chat_id = data.get("quiz_chat_id")
        message_id = data.get("quiz_message_id")
        text = (
            f"⏱ *Время вышло ({timeout_seconds} сек)*\n"
            f"✅ Правильный ответ: *{outcome.correct_text}*"
        )
        if chat_id and message_id:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=text,
                    parse_mode="Markdown",
                )
            except Exception:
                pass
        data["quiz_message_id"] = None
        await asyncio.sleep(FEEDBACK_DELAY_WRONG)
        await _after_answer(bot, user_id, outcome)


async def auto_timeout(bot, user_id, q_num_at_send):
    data = user_data.get(user_id)
    if not data:
        return
    target = capture_live_question_target(data)
    if target.question_index != q_num_at_send:
        return
    timeout_seconds = int(_time_limit(data) or QUIZ_TIMEOUT)
    await _handle_question_timeout(
        bot,
        user_id,
        target.attempt_id,
        target.question_index,
        float(timeout_seconds),
        timeout_seconds,
    )


async def challenge_timeout(bot, user_id, q_num_at_send):
    await auto_timeout(bot, user_id, q_num_at_send)


async def _resume_resolved(query, context, resolved) -> None:
    data = _hydrate_session(
        query.from_user.id,
        resolved.session,
        chat_id=query.message.chat_id,
        username=query.from_user.username,
        first_name=query.from_user.first_name,
    )
    try:
        timed_out = question_is_timed_out(resolved.session, now=time.time())
        restart_timeout_route(resolved.session)
    except (LegacyQuestionTimerConflict, LegacyRestartStateInvalid):
        await query.edit_message_text(
            "⚠️ Таймер сохранённой попытки противоречив. Продолжение остановлено."
        )
        return

    await query.edit_message_text(
        f"▶️ *Продолжаем!*\n_{data.get('level_name', 'Тест')}_\n"
        f"Вопрос {data.get('current_question', 0) + 1}/{len(data.get('questions', []))}",
        parse_mode="Markdown",
    )
    if timed_out:
        target = capture_live_question_target(data)
        await _handle_question_timeout(
            context.bot,
            query.from_user.id,
            target.attempt_id,
            target.question_index,
            0.0,
            int(_time_limit(data) or 1),
        )
        return
    if data.get("is_challenge"):
        await send_challenge_question(context.bot, query.from_user.id)
    else:
        await send_question(context.bot, query.from_user.id)


async def resume_session_handler(update: Update, context):
    query = update.callback_query
    user_id = query.from_user.id
    try:
        resolved = await _run_blocking_io(
            lambda: resolve_session_action(query.data, "res", user_id)
        )
    except LegacySessionActionUnavailable:
        await query.answer("⚠️ База сессий временно недоступна.", show_alert=True)
        return
    except LegacySessionActionStale:
        await query.answer("Эта кнопка уже устарела.", show_alert=True)
        return
    await query.answer()
    try:
        await _resume_resolved(query, context, resolved)
    except (LegacyPersistedSessionModeInvalid, LegacyPersistedSessionStateInvalid, ValueError):
        logger.error("resume hydration failed", exc_info=True)
        await query.edit_message_text("⚠️ Сохранённая попытка повреждена. Новую не создаю.")


async def restart_session_handler(update: Update, context):
    query = update.callback_query
    user_id = query.from_user.id
    try:
        resolved = await _run_blocking_io(
            lambda: resolve_session_action(query.data, "rst", user_id)
        )
    except LegacySessionActionUnavailable:
        await query.answer("⚠️ База сессий временно недоступна.", show_alert=True)
        return
    except LegacySessionActionStale:
        await query.answer("Эта кнопка уже устарела.", show_alert=True)
        return

    session = resolved.session
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

    mode = session.get("mode")
    total = len(session.get("questions_data", []))
    try:
        if mode in {"random20", "hardcore20"}:
            questions = pick_competitive_challenge_questions(mode)
        else:
            pool = get_pool_by_key(session.get("level_key"))
            questions = random.sample(pool, min(total, len(pool))) if pool else []
    except (KeyError, ValueError):
        questions = []
    if not questions:
        await query.answer(
            "⚠️ Не удалось собрать вопросы для перезапуска.",
            show_alert=True,
        )
        return

    try:
        result = await _run_blocking_io(
            lambda: restart_owned_quiz_attempt(
                resolved.session_id,
                user_id,
                expected_attempt_id=resolved.attempt_id,
                mode=mode,
                question_ids=[question_identity.get_qid(item) for item in questions],
                questions_data=questions,
                level_key=session.get("level_key"),
                level_name=session.get("level_name"),
                time_limit=session.get("time_limit"),
                chat_id=query.message.chat_id,
            )
        )
    except QuizSessionLifecycleUnavailable:
        await query.answer("⚠️ База сессий временно недоступна.", show_alert=True)
        return
    except QuizSessionLifecycleConflict:
        await query.answer("Попытка уже изменилась. Открой /status.", show_alert=True)
        return

    await query.answer()
    data = _hydrate_session(
        user_id,
        result["session"],
        chat_id=query.message.chat_id,
        username=query.from_user.username,
        first_name=query.from_user.first_name,
    )
    await query.edit_message_text(
        f"🔁 *Начинаем заново*\n_{data.get('level_name', 'Тест')}_\n\n"
        f"Вопросов: {len(questions)}",
        parse_mode="Markdown",
    )
    if data.get("is_challenge"):
        await send_challenge_question(context.bot, user_id)
    else:
        await send_question(context.bot, user_id)


async def cancel_session_handler(update: Update, context):
    query = update.callback_query
    user_id = query.from_user.id
    try:
        resolved = await _run_blocking_io(
            lambda: resolve_session_action(query.data, "can", user_id)
        )
    except LegacySessionActionUnavailable:
        await query.answer("⚠️ База сессий временно недоступна.", show_alert=True)
        return
    except LegacySessionActionStale:
        await query.answer("Эта кнопка уже устарела.", show_alert=True)
        return
    try:
        await _run_blocking_io(
            lambda: cancel_owned_incomplete_quiz_attempt(
                resolved.session_id,
                user_id,
                expected_attempt_id=resolved.attempt_id,
            )
        )
    except QuizSessionLifecycleUnavailable:
        await query.answer("⚠️ База сессий временно недоступна.", show_alert=True)
        return
    except QuizSessionLifecycleConflict:
        await query.answer("Попытка уже изменилась. Открой /status.", show_alert=True)
        return
    await query.answer()
    local = user_data.get(user_id)
    if local and local.get("attempt_id") == resolved.attempt_id:
        _cancel_runtime_timer(local)
        user_data.pop(user_id, None)
    await query.edit_message_text(
        "❌ Тест отменён.",
        reply_markup=main_menu.main_keyboard(),
    )


async def _cancel_current(user_id: int) -> tuple[bool, str]:
    try:
        result = await _run_blocking_io(
            lambda: cancel_current_incomplete_session(user_id)
        )
    except LegacySessionResultPending:
        return False, "result_pending"
    except LegacySessionControlUnavailable:
        return False, "unavailable"
    except LegacySessionControlConflict:
        return False, "conflict"
    local = user_data.get(user_id)
    if local:
        _cancel_runtime_timer(local)
    user_data.pop(user_id, None)
    return result.had_active_session, "cancelled"


async def cancel_quiz_handler(update: Update, context):
    query = update.callback_query
    await query.answer()
    _had, status = await _cancel_current(query.from_user.id)
    if status == "result_pending":
        await query.edit_message_text(
            "⏳ Тест уже завершён; результат нельзя удалить. Используй /status для финализации."
        )
        return ConversationHandler.END
    if status in {"unavailable", "conflict"}:
        await query.edit_message_text(
            "⚠️ Не удалось безопасно отменить попытку. Используй /status и повтори позже."
        )
        return ConversationHandler.END
    await query.edit_message_text(
        "❌ *Тест отменён.* Выбери действие:",
        reply_markup=main_menu.main_keyboard(),
        parse_mode="Markdown",
    )
    return ConversationHandler.END


async def cancel(update: Update, context):
    _had, status = await _cancel_current(update.effective_user.id)
    if status == "result_pending":
        await update.message.reply_text(
            "⏳ Тест уже завершён; результат сохранён для финализации. Используй /status."
        )
    elif status in {"unavailable", "conflict"}:
        await update.message.reply_text(
            "⚠️ База временно не подтверждает безопасную отмену. Ничего не удалено."
        )
    else:
        await update.message.reply_text("❌ Отменено.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END


async def reset_command(update: Update, context):
    _had, status = await _cancel_current(update.effective_user.id)
    if status == "result_pending":
        await update.message.reply_text(
            "⏳ Завершённый результат нельзя сбросить. Используй /status для финализации."
        )
        return ConversationHandler.END
    if status in {"unavailable", "conflict"}:
        await update.message.reply_text(
            "⚠️ Безопасный сброс сейчас не подтверждён. Данные не удалены."
        )
        return ConversationHandler.END
    await update.message.reply_text("🆘 Тест сброшен.", reply_markup=ReplyKeyboardRemove())
    await update.message.reply_text(
        "📖 *Главное меню*",
        reply_markup=main_menu.main_keyboard(),
        parse_mode="Markdown",
    )
    return ConversationHandler.END


async def reset_session_inline(update: Update, context):
    query = update.callback_query
    await query.answer()
    _had, status = await _cancel_current(query.from_user.id)
    if status == "result_pending":
        await safe_edit(
            query,
            "⏳ Завершённый результат нельзя удалить. Используй /status.",
            reply_markup=main_menu.main_keyboard(),
        )
        return
    if status in {"unavailable", "conflict"}:
        await safe_edit(
            query,
            "⚠️ Безопасный сброс сейчас не подтверждён.",
            reply_markup=main_menu.main_keyboard(),
        )
        return
    await safe_edit(
        query,
        "🆘 Тест сброшен.",
        reply_markup=main_menu.main_keyboard(),
    )


async def _status_session(user_id: int):
    try:
        session = await _run_blocking_io(
            lambda: get_active_quiz_session_strict(user_id)
        )
    except QuizSessionAccessUnavailable:
        return None, "unavailable"
    except QuizSessionAccessSchemaInvalid:
        return None, "conflict"
    if session is None:
        return None, "none"
    try:
        decision = classify_restart_session(session)
    except LegacyRestartStateInvalid:
        return session, "conflict"
    return session, decision.action


async def status_command(update: Update, context):
    user = update.effective_user
    session, status = await _status_session(user.id)
    if status == "none":
        await update.message.reply_text(
            "📌 Нет активного теста.",
            reply_markup=main_menu.main_keyboard(),
        )
        return
    if status == "unavailable":
        await update.message.reply_text("⚠️ База сессий временно недоступна.")
        return
    if status == "conflict":
        await update.message.reply_text(
            "⚠️ Состояние активной сессии противоречиво; новая попытка не запускается."
        )
        return
    if status == "finalize":
        await _finalize_active_session(
            context.bot,
            user,
            session,
            update.effective_chat.id,
        )
        return
    await _show_active_attempt(update.message, session, edit=False)


async def show_status_inline(update: Update, context):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    session, status = await _status_session(user.id)
    if status == "none":
        await safe_edit(
            query,
            "📌 *Статус:* нет активного теста",
            reply_markup=main_menu.main_keyboard(),
        )
        return
    if status == "unavailable":
        await safe_edit(query, "⚠️ База сессий временно недоступна.")
        return
    if status == "conflict":
        await safe_edit(
            query,
            "⚠️ Состояние сессии противоречиво; ничего не удалено.",
        )
        return
    if status == "finalize":
        await _finalize_active_session(
            context.bot,
            user,
            session,
            query.message.chat_id,
        )
        return
    await _show_active_attempt(query, session, edit=True)


async def start(update: Update, context):
    user = update.effective_user
    await _run_blocking_io(init_user_stats, user.id, user.username, user.first_name)
    await _touch_activity(user.id)

    try:
        if update.message:
            await update.message.delete()
    except Exception:
        pass

    session, status = await _status_session(user.id)
    if status == "unavailable":
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⚠️ База сессий временно недоступна. Новую попытку не запускаю.",
        )
        return
    if status == "conflict":
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⚠️ Активная сессия противоречива. Новую попытку не запускаю.",
        )
        return
    if status == "resume":
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⏸ *У тебя есть незавершённый тест.*",
            parse_mode="Markdown",
            reply_markup=_lifecycle_keyboard(session),
        )
        return
    if status == "finalize":
        if not await _finalize_active_session(
            context.bot,
            user,
            session,
            update.effective_chat.id,
        ):
            return

    if context.args:
        try:
            entry = resolve_course(context.args[0], surface=SURFACE_TELEGRAM)
            resolve_course_pool(entry)
        except (CourseCatalogError, KeyError):
            entry = None
        if entry is not None:
            rows = []
            for mode in entry.allowed_modes:
                if mode == "relaxed":
                    label = "🧘 Без ограничения времени"
                elif mode == "timed":
                    label = f"⏱ На время ({TIMED_MODE_TIMEOUT} сек)"
                else:
                    label = f"⚡ Скоростной ({SPEED_MODE_TIMEOUT} сек)"
                rows.append([
                    InlineKeyboardButton(
                        label,
                        callback_data=f"course_mode:{mode}:{entry.key}",
                    )
                ])
            rows.append([InlineKeyboardButton("↩️ В главное меню", callback_data="back_to_main")])
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"📚 *{entry.title}*\n\nВыбери режим прохождения:",
                reply_markup=InlineKeyboardMarkup(rows),
                parse_mode="Markdown",
            )
            return

    name = user.first_name or "друг"
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"👋 *Добро пожаловать, {name}!*\n\nВыбери действие:",
        reply_markup=main_menu.main_keyboard(),
        parse_mode="Markdown",
    )


async def test_command(update: Update, context):
    import telegram_course_surface as courses

    return await courses.choose_level(update, context, is_callback=False)


async def retry_errors(update: Update, context):
    query = update.callback_query
    user = query.from_user
    user_id = user.id
    try:
        target_id = int((query.data or "").replace("retry_errors_", "", 1))
    except (TypeError, ValueError):
        await query.answer("Некорректная кнопка.", show_alert=True)
        return ConversationHandler.END
    if target_id != user_id or target_id not in user_data:
        await query.answer("Данные сессии устарели.", show_alert=True)
        return ConversationHandler.END
    previous = user_data[target_id]
    wrong = []
    for item in previous.get("answered_questions", []):
        try:
            if answer_history.is_wrong(item):
                question = item.get("question_obj") if isinstance(item, dict) else None
                if isinstance(question, dict):
                    wrong.append(question)
        except Exception:
            continue
    if not wrong:
        await query.answer("Ошибок нет!", show_alert=True)
        return ConversationHandler.END

    await query.answer()
    data = await _launch_attempt(
        user=user,
        bot=context.bot,
        chat_id=query.message.chat_id,
        mode="level",
        questions=wrong,
        level_key="retry_errors",
        level_name=f"🔁 Повторение ошибок ({previous.get('level_name', 'Тест')})",
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
    await send_question(context.bot, user_id)
    return ANSWERING


async def text_answer_fallback(update: Update, context):
    del context
    user_id = update.effective_user.id
    data = user_data.get(user_id)
    if data and data.get("is_battle"):
        await update.message.reply_text("👆 В битве используй кнопки под вопросом.")
        return BATTLE_ANSWERING
    if data:
        await update.message.reply_text("👆 Используй кнопки под вопросом для ответа.")
        return ANSWERING
    await update.message.reply_text("Используй /status или /test.")
    return ConversationHandler.END


async def _general_message_fallback(update: Update, context):
    user = update.effective_user
    if update.effective_chat.type == "private":
        try:
            await update.message.delete()
        except Exception:
            pass
    data = user_data.get(user.id)
    if data and data.get("is_battle"):
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="👆 В битве используй кнопки под текущим вопросом.",
        )
        return
    if data:
        data["quiz_chat_id"] = update.effective_chat.id
        if data.get("is_challenge"):
            await send_challenge_question(context.bot, user.id)
        else:
            await send_question(context.bot, user.id)
        return

    session, status = await _status_session(user.id)
    if status == "resume":
        try:
            data = _hydrate_session(
                user.id,
                session,
                chat_id=update.effective_chat.id,
                username=user.username,
                first_name=user.first_name,
            )
        except (LegacyPersistedSessionModeInvalid, LegacyPersistedSessionStateInvalid, ValueError):
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="⚠️ Сессия повреждена. Используй /status.",
            )
            return
        try:
            timed_out = question_is_timed_out(session, now=time.time())
        except LegacyQuestionTimerConflict:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="⚠️ Таймер сессии противоречив. Используй /status.",
            )
            return
        if timed_out:
            target = capture_live_question_target(data)
            await _handle_question_timeout(
                context.bot,
                user.id,
                target.attempt_id,
                target.question_index,
                0.0,
                int(_time_limit(data) or 1),
            )
        elif data.get("is_challenge"):
            await send_challenge_question(context.bot, user.id)
        else:
            await send_question(context.bot, user.id)
        return
    if status == "finalize":
        await _finalize_active_session(
            context.bot,
            user,
            session,
            update.effective_chat.id,
        )
        return
    if status in {"unavailable", "conflict"}:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⚠️ Состояние сессии сейчас нельзя безопасно прочитать.",
        )
        return
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="📖 *Главное меню*\n\nВыбери действие:",
        reply_markup=main_menu.main_keyboard(),
        parse_mode="Markdown",
    )


async def remind_unfinished_tests_job(context):
    from database import get_stale_sessions

    try:
        stale = await _run_blocking_io(get_stale_sessions, max_age_hours=2)
    except Exception:
        logger.warning("stale-session reminder lookup failed", exc_info=True)
        return
    for session in stale:
        uid = session.get("user_id")
        if not uid:
            continue
        try:
            decision = classify_restart_session(session)
        except LegacyRestartStateInvalid:
            continue
        if decision.action != "resume":
            continue
        try:
            await context.bot.send_message(
                chat_id=int(uid),
                text=(
                    "📝 *У тебя есть незавершённый тест!*\n\n"
                    "Продолжить с того места, где остановился?"
                ),
                parse_mode="Markdown",
                reply_markup=_lifecycle_keyboard(session),
            )
        except Exception:
            logger.debug(
                "unfinished-session reminder delivery failed for %s",
                uid,
                exc_info=True,
            )


async def _save_all_sessions(_application=None):
    """Cancel process-local timers only; persisted Mongo state is authoritative."""
    cancelled = 0
    for data in list(user_data.values()):
        before = data.get("timer_task")
        _cancel_runtime_timer(data)
        if before:
            cancelled += 1
    logger.info(
        "Graceful shutdown: cancelled %d runtime quiz timers; Mongo unchanged",
        cancelled,
    )
