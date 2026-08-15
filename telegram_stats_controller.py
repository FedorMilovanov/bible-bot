# ruff: noqa: RUF001
"""Production stats and leaderboard presentation with non-blocking DB boundaries."""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from database import (
    calculate_accuracy,
    calculate_days_playing,
    format_time,
    get_category_leaderboard,
    get_context_leaderboard,
    get_current_week_id,
    get_leaderboard_page,
    get_total_users,
    get_user_history,
    get_user_position,
    get_weekly_leaderboard,
)
from utils import safe_edit


async def stats_command(update, context, *, main_keyboard_factory: Callable[[], InlineKeyboardMarkup]):
    """Render /stats while keeping synchronous PyMongo work off the PTB event loop."""
    del context
    user_id = update.effective_user.id
    position, entry = await asyncio.to_thread(get_user_position, user_id)
    if not entry:
        await update.message.reply_text(
            "📊 *МОЯ СТАТИСТИКА*\n\nВы ещё не проходили тесты.\nИспользуйте /menu чтобы начать!",
            parse_mode="Markdown",
            reply_markup=main_keyboard_factory(),
        )
        return

    total_tests = entry.get("total_tests", 0)
    total_questions = entry.get("total_questions_answered", 0)
    total_correct = entry.get("total_correct_answers", 0)
    avg_time = entry.get("total_time_spent", 0) / max(total_tests, 1)
    days_playing = calculate_days_playing(
        entry.get("first_play_date", datetime.now().strftime("%Y-%m-%d"))
    )
    battles_played = entry.get("battles_played", 0)
    battles_won = entry.get("battles_won", 0)
    daily_streak = entry.get("daily_activity_streak", 0)

    text = "📊 *МОЯ СТАТИСТИКА*\n\n"
    text += f"🏅 Позиция: *#{position}*\n"
    text += f"💎 Баллов: *{entry.get('total_points', 0)}*\n"
    text += f"📅 Дней в игре: *{days_playing}*\n"
    text += f"🎯 Тестов пройдено: *{total_tests}*\n"
    text += f"✅ Точность: *{calculate_accuracy(total_correct, total_questions)}%*\n"
    text += f"⏱ Среднее время: *{format_time(avg_time)}*\n"
    if daily_streak > 0:
        text += f"🔥 Серия дней: *{daily_streak}*\n"
    text += f"⚔️ Битв: {battles_played}, Побед: {battles_won}\n"

    await update.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=main_keyboard_factory(),
    )


async def show_my_stats(query):
    """Render the inline personal-statistics screen without blocking PTB."""
    user_id = query.from_user.id
    position, entry = await asyncio.to_thread(get_user_position, user_id)

    if not entry:
        await query.edit_message_text(
            "📊 *МОЯ СТАТИСТИКА*\n\nВы ещё не проходили тесты.\nИспользуйте /test чтобы начать!",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🎯 Начать тест", callback_data="start_test")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")],
                ]
            ),
            parse_mode="Markdown",
        )
        return

    total_tests = entry.get("total_tests", 0)
    total_questions = entry.get("total_questions_answered", 0)
    total_correct = entry.get("total_correct_answers", 0)
    avg_time = entry.get("total_time_spent", 0) / max(total_tests, 1)
    days_playing = calculate_days_playing(
        entry.get("first_play_date", datetime.now().strftime("%Y-%m-%d"))
    )
    battles_played = entry.get("battles_played", 0)
    battles_won = entry.get("battles_won", 0)

    text = "📊 *МОЯ СТАТИСТИКА*\n\n"
    text += f"🏅 Позиция: *#{position}*\n"
    text += f"💎 Баллов: *{entry.get('total_points', 0)}*\n"
    text += f"📅 Дней в игре: *{days_playing}*\n"
    text += f"🎯 Тестов пройдено: *{total_tests}*\n"
    text += f"✅ Точность: *{calculate_accuracy(total_correct, total_questions)}%*\n"
    text += f"⏱ Среднее время: *{format_time(avg_time)}*\n"

    daily_streak = entry.get("daily_activity_streak", 0)
    if daily_streak > 0:
        text += f"🔥 Серия дней: *{daily_streak}*\n"

    max_streak = entry.get("max_streak_ever", 0)
    if max_streak > 0:
        text += f"⚡ Лучшая серия: *{max_streak}* правильных подряд\n"

    perfect_count = entry.get("perfect_count", 0)
    if perfect_count > 0:
        text += f"💎 Идеальных тестов: *{perfect_count}*\n"

    text += f"\n⚔️ Битв: *{battles_played}*"
    if battles_played > 0:
        text += f", Побед: *{battles_won}* ({round(battles_won / battles_played * 100)}%)"
    text += "\n"

    achievements = entry.get("achievements", {})
    if achievements:
        text += f"🏅 Достижений: *{len(achievements)}*\n"

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🎯 Начать тест", callback_data="start_test")],
                [InlineKeyboardButton("🏅 Достижения", callback_data="achievements")],
                [InlineKeyboardButton("📜 История", callback_data="my_history")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")],
            ]
        ),
        parse_mode="Markdown",
    )


async def show_general_leaderboard(query, page: int = 0):
    """Render the paged general leaderboard with all Mongo reads off-loop."""
    users = await asyncio.to_thread(get_leaderboard_page, page)
    total_users = await asyncio.to_thread(get_total_users)
    user_id = query.from_user.id

    if not users:
        text = "🏆 *ТАБЛИЦА ЛИДЕРОВ*\n\nПока никто не проходил тесты."
    else:
        text = f"🏆 *ТАБЛИЦА ЛИДЕРОВ* (Стр. {page + 1})\n"
        start_rank = page * 10 + 1
        for i, entry in enumerate(users, start_rank):
            name = entry.get("first_name", "Unknown")[:15]
            pts = entry.get("total_points", 0)
            tests = entry.get("total_tests", 0)
            medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
            text += f"\n{medal} *{name}* — 💎{pts} • 🎯{tests}\n"

    position, my_entry = await asyncio.to_thread(get_user_position, user_id)
    if my_entry and position:
        text += f"\n━━━━━━━━━━━━\n👤 *Ваше место:* #{position}"

    nav = []
    if page > 0:
        nav.append(
            InlineKeyboardButton("⬅️", callback_data=f"leaderboard_page_{page - 1}")
        )
    if (page + 1) * 10 < total_users:
        nav.append(
            InlineKeyboardButton("➡️", callback_data=f"leaderboard_page_{page + 1}")
        )

    keyboard = []
    if nav:
        keyboard.append(nav)
    keyboard.append(
        [
            InlineKeyboardButton("🏛 Контекст", callback_data="cat_lb_context"),
            InlineKeyboardButton("🔴 Богословы", callback_data="cat_lb_hard"),
        ]
    )
    keyboard.append([InlineKeyboardButton("⬅️ В меню", callback_data="back_to_main")])

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )


async def show_history(update, context):
    """Render recent quiz history while keeping the durable read off-loop."""
    del context
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    try:
        sessions = await asyncio.to_thread(get_user_history, user_id, limit=10)
    except Exception:
        sessions = []

    if sessions:
        text = "📜 *ИСТОРИЯ ПРОХОЖДЕНИЙ*\n\n"
        for session in sessions:
            end_time = session.get("end_time")
            dt = end_time.strftime("%d.%m %H:%M") if hasattr(end_time, "strftime") else "—"
            score = session.get("correct_count", 0)
            total = session.get("total_questions", len(session.get("questions_data", [])))
            name = session.get("level_name", "?")
            pct = round(score / max(total, 1) * 100)
            text += f"• {dt} — _{name}_: *{score}/{total}* ({pct}%)\n"
    else:
        text = "📜 *ИСТОРИЯ*\n\nПока пусто — пройди первый тест!"

    await safe_edit(
        query,
        text,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Назад", callback_data="my_stats")]]
        ),
    )


async def show_category_leaderboard(query, category_key: str):
    """Render one category leaderboard with the blocking database read off-loop."""
    category_names = {
        "easy": "🟢 Основы",
        "medium": "🟡 Контекст",
        "hard": "🔴 Богословие",
        "nero": "👑 Нерон",
        "geography": "🌍 География",
        "context": "🏛 Знатоки контекста",
    }
    cat_name = category_names.get(category_key, category_key)
    if category_key == "context":
        users = await asyncio.to_thread(get_context_leaderboard)
    else:
        users = await asyncio.to_thread(get_category_leaderboard, category_key)

    if not users:
        text = f"{cat_name}\n\nПока никто не проходил этот тест."
    else:
        text = f"🏆 *РЕЙТИНГ: {cat_name}*\n\n"
        for i, entry in enumerate(users, 1):
            name = entry.get("first_name", "?")[:15]
            medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
            if category_key == "context":
                text += f"{medal} *{name}* — {entry.get('_context_correct', 0)} верных\n"
            else:
                text += f"{medal} *{name}* — {entry.get(f'{category_key}_correct', 0)} верных\n"

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⬅️ Общий рейтинг", callback_data="leaderboard")],
                [InlineKeyboardButton("⬅️ В меню", callback_data="back_to_main")],
            ]
        ),
        parse_mode="Markdown",
    )


async def category_leaderboard_handler(update, context):
    """Dispatch category leaderboard presentation without legacy bot.py routing."""
    del context
    query = update.callback_query
    await query.answer()
    await show_category_leaderboard(query, query.data.replace("cat_lb_", ""))


async def show_weekly_leaderboard(update, context):
    """Render weekly challenge leaderboard with its durable read off-loop."""
    del context
    query = update.callback_query
    await query.answer()
    mode = query.data.replace("weekly_lb_", "")
    users = await asyncio.to_thread(get_weekly_leaderboard, mode)
    mode_name = "🎲 Random Challenge" if mode == "random20" else "💀 Hardcore Random"
    week_id = get_current_week_id()

    if not users:
        text = f"🏆 *{mode_name}*\nНеделя {week_id}\n\nПока нет результатов."
    else:
        text = f"🏆 *{mode_name}*\nНеделя {week_id}\n\n"
        for i, entry in enumerate(users, 1):
            medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
            name = entry.get("first_name", "?")[:15]
            score = entry.get("best_score", 0)
            elapsed = format_time(entry.get("best_time", 0))
            text += f"{medal} *{name}* — {score}/20 • ⏱ {elapsed}\n"

    other_mode = "hardcore20" if mode == "random20" else "random20"
    other_mode_name = "💀 Hardcore" if mode == "random20" else "🎲 Normal"
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"→ {other_mode_name}", callback_data=f"weekly_lb_{other_mode}"
                    )
                ],
                [InlineKeyboardButton("🎲 Сыграть", callback_data=f"challenge_rules_{mode}")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="challenge_menu")],
            ]
        ),
        parse_mode="Markdown",
    )
