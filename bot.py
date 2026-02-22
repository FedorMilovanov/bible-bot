"""
Библейский тест-бот — 1 Петра
Точка входа.
"""
from keep_alive import keep_alive
keep_alive()

import os
import time
import random
import asyncio
from datetime import datetime

from telegram import (
    Update, ReplyKeyboardMarkup, ReplyKeyboardRemove,
    InlineKeyboardButton, InlineKeyboardMarkup,
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters,
    ConversationHandler, CallbackQueryHandler,
)

from database import (
    collection,
    init_user_stats, add_to_leaderboard, update_battle_stats,
    get_user_position, get_leaderboard_page, get_total_users,
    format_time, calculate_days_playing, calculate_accuracy,
    record_question_stat, get_question_stats,
    get_points_to_next_place, get_category_leaderboard,
    is_bonus_eligible, compute_bonus,
    update_challenge_stats, update_weekly_leaderboard,
    get_weekly_leaderboard, get_user_achievements, get_current_week_id,
)
from questions import (
    easy_questions, easy_questions_v17_25,
    medium_questions, medium_questions_v17_25,
    hard_questions, hard_questions_v17_25,
    nero_questions, geography_questions,
    practical_ch1_questions, practical_v17_25_questions,
    linguistics_ch1_questions, linguistics_ch1_questions_2,
    linguistics_v17_25_questions, all_chapter1_questions,
    intro_part1_questions, intro_part2_questions, intro_part3_questions,
)

# ─────────────────────────────────────────────
# КОНФИГУРАЦИЯ УРОВНЕЙ (единое место для правок)
# ─────────────────────────────────────────────
LEVEL_CONFIG = {
    "level_easy": {
        "pool":  easy_questions + easy_questions_v17_25,
        "name":  "🟢 Основы (1 Петра 1:1–25)",
        "key":   "easy",
        "points_per_q": 1,
    },
    "level_medium": {
        "pool":  medium_questions + medium_questions_v17_25,
        "name":  "🟡 Контекст (1 Петра 1:1–25)",
        "key":   "medium",
        "points_per_q": 2,
    },
    "level_hard": {
        "pool":  hard_questions + hard_questions_v17_25,
        "name":  "🔴 Богословие (1 Петра 1:1–25)",
        "key":   "hard",
        "points_per_q": 3,
    },
    "level_nero": {
        "pool":  nero_questions,
        "name":  "👑 Правление Нерона",
        "key":   "nero",
        "points_per_q": 2,
    },
    "level_geography": {
        "pool":  geography_questions,
        "name":  "🌍 География земли",
        "key":   "geography",
        "points_per_q": 2,
    },
    "level_practical_ch1": {
        "pool":  practical_ch1_questions + practical_v17_25_questions,
        "name":  "🙏 Применение (1 Петра 1:1–25)",
        "key":   "practical_ch1",
        "points_per_q": 2,
    },
    "level_linguistics_ch1": {
        "pool":  linguistics_ch1_questions,
        "name":  "🔬 Лингвистика: Избранные и странники (ч.1)",
        "key":   "linguistics_ch1",
        "points_per_q": 3,
    },
    "level_linguistics_ch1_2": {
        "pool":  linguistics_ch1_questions_2,
        "name":  "🔬 Лингвистика: Живая надежда (ч.2)",
        "key":   "linguistics_ch1_2",
        "points_per_q": 3,
    },
    "level_linguistics_ch1_3": {
        "pool":  linguistics_v17_25_questions,
        "name":  "🔬 Лингвистика: Искупление и истина (ч.3)",
        "key":   "linguistics_ch1_3",
        "points_per_q": 3,
    },
     "level_intro1": {
        "pool":  intro_part1_questions,
        "name":  "📜 Введение: Авторство ч.1",
        "key":   "intro1",
        "points_per_q": 2,
    },
    "level_intro2": {
        "pool":  intro_part2_questions,
        "name":  "📜 Введение: Авторство ч.2",
        "key":   "intro2",
        "points_per_q": 2,
    },
    "level_intro3": {
        "pool":  intro_part3_questions,
        "name":  "📜 Введение: Структура и цель",
        "key":   "intro3",
        "points_per_q": 2,
    },
}

# Состояния диалога
CHOOSING_LEVEL, ANSWERING, BATTLE_ANSWERING = range(3)

# Хранилище активных сессий (в памяти)
# TODO: перенести в MongoDB/Redis при необходимости
user_data: dict = {}
pending_battles: dict = {}


# ═══════════════════════════════════════════════
# ГЛАВНОЕ МЕНЮ
# ═══════════════════════════════════════════════

def _main_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 О боте",                callback_data="about")],
        [InlineKeyboardButton("🎯 Начать тест",           callback_data="start_test")],
        [InlineKeyboardButton("🎲 Челлендж (20) — бонус", callback_data="challenge_menu")],
        [InlineKeyboardButton("🏛 Исторический контекст", callback_data="historical_menu")],
        [InlineKeyboardButton("⚔️ Режим битвы",            callback_data="battle_menu")],
        [InlineKeyboardButton("🏆 Таблица лидеров",       callback_data="leaderboard")],
        [InlineKeyboardButton("📊 Моя статистика",        callback_data="my_stats")],
    ])


async def start(update: Update, context):
    user = update.effective_user
    is_new = init_user_stats(user.id, user.username, user.first_name)

    # Сбрасываем ReplyKeyboard если осталась от теста
    await update.message.reply_text("↩️", reply_markup=ReplyKeyboardRemove())

    name = user.first_name or "друг"

    welcome = (
        f"👋 *Добро пожаловать, {name}!*\n\n"
        "Здесь мы изучаем *1-е послание Петра* — "
        "один из ключевых текстов Нового Завета.\n\n"
        "📖 *Глава 1* — основной тест по тексту\n"
        "🔬 *Лингвистика* — глубокий разбор слов и смыслов\n"
        "🏛 *Исторический контекст* — Нерон, география, введение\n"
        "⚔️ *Битвы* — соревнование с другими игроками\n\n"
        "Нажми на кнопку ниже, чтобы начать! 👇"
    )

    await update.message.reply_text(
        welcome,
        reply_markup=_main_keyboard(),
        parse_mode="Markdown",
    )


async def back_to_main(update: Update, context):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "📖 *БИБЛЕЙСКИЙ ТЕСТ-БОТ*\n\n"
        "📖 Глава 1 • 🔬 Лингвистика • 🏛 Контекст • ⚔️ Битвы\n\n"
        "Выбери действие:",
        reply_markup=_main_keyboard(),
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════
# ВЫБОР УРОВНЯ
# ═══════════════════════════════════════════════

async def choose_level(update, context, is_callback=False):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🏛 Исторический контекст",      callback_data="historical_menu")],
        [InlineKeyboardButton("📖 1 Петра — Глава 1",          callback_data="chapter_1_menu")],
        [InlineKeyboardButton("📖 Глава 2 — скоро...",         callback_data="coming_soon")],
        [InlineKeyboardButton("⬅️ Назад",                       callback_data="back_to_main")],
    ])
    text = (
        "🎯 *ВЫБЕРИ КАТЕГОРИЮ*\n\n"
        "📖 *1 Петра по главам:*\nГлава 1 — 5 видов вопросов\n\n"
        "📜 *Тематические:*\n👑 Правление Нерона • 🌍 География\n\n"
        "⏱ На каждый вопрос — 7 секунд!"
    )
    if is_callback and hasattr(update, "callback_query"):
        await update.callback_query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")
    else:
        await update.message.reply_text(text, reply_markup=keyboard, parse_mode="Markdown")


async def chapter_1_menu(update: Update, context):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([

        [InlineKeyboardButton("🟢 Основы (1 балл)",                        callback_data="level_easy")],
        [InlineKeyboardButton("🟡 Контекст (2 балла)",                     callback_data="level_medium")],
        [InlineKeyboardButton("🔴 Богословие (3 балла)",                   callback_data="level_hard")],
        [InlineKeyboardButton("🙏 Применение (2 балла)",                   callback_data="level_practical_ch1")],
        [InlineKeyboardButton("🔬 Лингвистика: Избранные и странники ч.1 (3 балла)", callback_data="level_linguistics_ch1")],
        [InlineKeyboardButton("🔬 Лингвистика: Живая надежда ч.2 (3 балла)", callback_data="level_linguistics_ch1_2")],
        [InlineKeyboardButton("🔬 Лингвистика: Искупление и истина ч.3 (3 балла)", callback_data="level_linguistics_ch1_3")],

        [InlineKeyboardButton("⬅️ Назад",                                   callback_data="start_test")],
    ])
    await query.edit_message_text(
        "📖 *1 ПЕТРА — ГЛАВА 1 (ст. 1–25)*\n\n"
        
        "🟢 *Основы* — факты, даты, адресаты\n"
        "🟡 *Контекст* — исторический фон, символы\n"
        "🔴 *Богословие* — греческий, доктрины, Троица\n"
        "🙏 *Применение* — практические вопросы\n"
        "🔬 *Лингвистика ч.1* — прогноз, диаспора, защита...\n"
        "🔬 *Лингвистика ч.2* — святость, логос, рождение свыше...\n"
        "🔬 *Лингвистика ч.3* — выкуп, образ жизни, глагол...\n"
        "👑 *Нерон* — правление и гонения\n"
        "🌍 *География* — провинции и города",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )


async def historical_menu(update: Update, context):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📜 Введение: Авторство ч.1 (2 балла)",    callback_data="level_intro1")],
        [InlineKeyboardButton("📜 Введение: Авторство ч.2 (2 балла)",    callback_data="level_intro2")],
        [InlineKeyboardButton("📜 Введение: Структура и цель (2 балла)", callback_data="level_intro3")],
        [InlineKeyboardButton("━━━━━━━━━━━━━━━",                          callback_data="coming_soon")],
        [InlineKeyboardButton("👑 Правление Нерона (2 балла)",           callback_data="level_nero")],
        [InlineKeyboardButton("🌍 География земли (2 балла)",            callback_data="level_geography")],
        [InlineKeyboardButton("⬅️ Назад",                                 callback_data="back_to_main")],
    ])
    await query.edit_message_text(
        "🏛 *ИСТОРИЧЕСКИЙ КОНТЕКСТ*\n\n"
        "📜 *Введение в книгу* — основа:\n"
        "Авторство, датировка, структура и цели послания\n\n"
        "➕ *Дополнительно:*\n"
        "👑 Правление Нерона — исторический фон, гонения\n"
        "🌍 География — провинции и города малой Азии\n\n"
        "_Баллы за эти тесты не влияют на общий рейтинг._",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )


async def level_selected(update: Update, context):
    query = update.callback_query
    await query.answer()

    if query.data == "back_to_main":
        await back_to_main(update, context)
        return ConversationHandler.END

    cfg = LEVEL_CONFIG.get(query.data)
    if not cfg:
        return ConversationHandler.END

    user_id = update.effective_user.id
    questions = random.sample(cfg["pool"], min(10, len(cfg["pool"])))

    user_data[user_id] = {
        "questions":          questions,
        "level_name":         cfg["name"],
        "level_key":          cfg["key"],
        "current_question":   0,
        "correct_answers":    0,
        "answered_questions": [],
        "start_time":         time.time(),
        "is_battle":          False,
        "battle_points":      0,
    }

    await query.edit_message_text(
        f"*{cfg['name']}*\n\n📝 Вопросов: {len(questions)}\nНачинаем! ⏱",
        parse_mode="Markdown",
    )
    await send_question(query.message, user_id)
    return ANSWERING


# ═══════════════════════════════════════════════
# ВОПРОСЫ И ОТВЕТЫ
# ═══════════════════════════════════════════════

async def send_question(message, user_id):
    data = user_data[user_id]
    q_num = data["current_question"]
    total = len(data["questions"])

    if q_num >= total:
        await show_results(message, user_id)
        return ConversationHandler.END

    q = data["questions"][q_num]
    correct_text = q["options"][q["correct"]]
    shuffled = q["options"][:]
    random.shuffle(shuffled)

    data["current_options"]      = shuffled
    data["current_correct_text"] = correct_text
    data["question_sent_at"]     = time.time()

    # Отменяем предыдущий страховочный таймер
    old_task = data.get("timer_task")
    if old_task and not old_task.done():
        old_task.cancel()

    await message.reply_text(
        f"*Вопрос {q_num + 1}/{total}*\n\n{q['question']}",
        reply_markup=ReplyKeyboardMarkup(
            [[opt] for opt in shuffled],
            one_time_keyboard=True, resize_keyboard=True,
        ),
        parse_mode="Markdown",
    )

    # Страховочный таймер 60 сек (чтобы тест не завис навсегда)
    data["timer_task"] = asyncio.create_task(auto_timeout(message, user_id, q_num))


async def auto_timeout(message, user_id, q_num_at_send):
    """Страховочный таймер 60 сек для обычного теста — чтобы тест не завис."""
    await asyncio.sleep(60)

    if user_id not in user_data:
        return

    data = user_data[user_id]
    if data.get("current_question") != q_num_at_send or data.get("is_battle"):
        return

    q = data["questions"][q_num_at_send]
    correct_text = data.get("current_correct_text") or q["options"][q["correct"]]

    data["answered_questions"].append({
        "question_obj": q,
        "user_answer":  "⏱ Время вышло",
    })

    try:
        await message.reply_text(
            f"⏱ *60 секунд истекло*\n✅ Правильный ответ: *{correct_text}*",
            reply_markup=ReplyKeyboardRemove(),
            parse_mode="Markdown",
        )
    except Exception:
        return

    data["current_question"] += 1
    if data["current_question"] < len(data["questions"]):
        await send_question(message, user_id)
    else:
        await show_results(message, user_id)


async def answer(update: Update, context):
    user_id = update.effective_user.id

    if user_id not in user_data:
        await update.message.reply_text("Используй /test чтобы начать")
        return ConversationHandler.END

    data = user_data[user_id]

    if data.get("is_battle"):
        return await battle_answer(update, context)

    q_num       = data["current_question"]
    q           = data["questions"][q_num]
    user_answer = update.message.text

    correct_text    = data.get("current_correct_text") or q["options"][q["correct"]]
    current_options = data.get("current_options") or q["options"]

    if user_answer not in current_options:
        await update.message.reply_text("Выбери один из вариантов")
        return ANSWERING

    # Отмена таймера (если вдруг остался с предыдущей сессии)
    timer_task = data.get("timer_task")
    if timer_task and not timer_task.done():
        timer_task.cancel()

    if user_answer == correct_text:
        data["correct_answers"] += 1
        await update.message.reply_text("✅ Верно!", reply_markup=ReplyKeyboardRemove())
    else:
        await update.message.reply_text(
            f"❌ Неверно\n✅ {correct_text}",
            reply_markup=ReplyKeyboardRemove(),
        )

    # Записываем статистику по вопросу
    elapsed = time.time() - data.get("question_sent_at", time.time())
    q_id = str(q.get("id", hash(q["question"])))
    record_question_stat(q_id, data["level_key"], user_answer == correct_text, elapsed)

    data["answered_questions"].append({"question_obj": q, "user_answer": user_answer})
    data["current_question"] += 1

    if data["current_question"] < len(data["questions"]):
        await send_question(update.message, user_id)
        return ANSWERING
    else:
        await show_results(update.message, user_id)
        return ConversationHandler.END


async def show_results(message, user_id):
    data       = user_data[user_id]
    score      = data["correct_answers"]
    total      = len(data["questions"])
    percentage = (score / total) * 100
    time_taken = time.time() - data["start_time"]
    user       = message.from_user

    add_to_leaderboard(user_id, user.username, user.first_name, data["level_key"], score, total, time_taken)

    position, entry = get_user_position(user_id)

    # Очки берём из LEVEL_CONFIG
    cfg = next((v for v in LEVEL_CONFIG.values() if v["key"] == data["level_key"]), None)
    earned_points = score * (cfg["points_per_q"] if cfg else 1)

    if percentage >= 90:   grade = "Отлично! 🌟"
    elif percentage >= 70: grade = "Хорошо! 👍"
    elif percentage >= 50: grade = "Удовлетворительно 📖"
    else:                  grade = "Нужно повторить 📚"

    result_text = (
        f"🏆 *РЕЗУЛЬТАТЫ*\n\n"
        f"*Категория:* {data['level_name']}\n"
        f"*Правильно:* {score}/{total} ({percentage:.0f}%)\n"
        f"*Баллы:* +{earned_points} 💎\n"
        f"*Время:* {format_time(time_taken)}\n"
        f"*Позиция:* #{position}\n"
        f"*Оценка:* {grade}\n"
    )

    answered = data.get("answered_questions", [])
    wrong = [
        item for item in answered
        if item["user_answer"] != item["question_obj"]["options"][item["question_obj"]["correct"]]
    ]

    # Кнопка "Повторить ошибки" только если есть ошибки
    keyboard_rows = [
        [InlineKeyboardButton("🔄 Ещё раз",     callback_data="start_test")],
        [InlineKeyboardButton("⚔️ Битва",        callback_data="battle_menu")],
        [InlineKeyboardButton("📊 Статистика",   callback_data="my_stats")],
        [InlineKeyboardButton("⬅️ Меню",         callback_data="back_to_main")],
    ]
    if wrong:
        keyboard_rows.insert(1, [InlineKeyboardButton(
            f"🔁 Повторить ошибки ({len(wrong)})",
            callback_data=f"retry_errors_{user_id}"
        )])

    await message.reply_text(result_text, reply_markup=InlineKeyboardMarkup(keyboard_rows), parse_mode="Markdown")

    # Разбор ошибок с группировкой по стихам
    if wrong:
        # Собираем темы/стихи где ошибки (если есть поле verse)
        verse_errors = {}
        for item in wrong:
            verse = item["question_obj"].get("verse", "")
            if verse:
                verse_errors[verse] = verse_errors.get(verse, 0) + 1

        header = f"❌ *РАЗБОР ОШИБОК ({len(wrong)} из {len(answered)}):*"
        if verse_errors:
            sorted_verses = sorted(verse_errors.items(), key=lambda x: -x[1])
            verse_list = ", ".join(f"ст. {v} ({c})" for v, c in sorted_verses)
            header += f"\n\n📌 *Сложные места:* {verse_list}"
            header += "\n💡 _Рекомендуем перечитать эти стихи перед следующим тестом_"

        await message.reply_text(header, parse_mode="Markdown")

        for i, item in enumerate(wrong, 1):
            q            = item["question_obj"]
            user_ans     = item["user_answer"]
            correct_text = q["options"][q["correct"]]

            verse_tag = f"📖 ст. {q['verse']} | " if q.get("verse") else ""
            topic_tag = f"🏷 {q['topic']}" if q.get("topic") else ""

            breakdown = f"❌ *Ошибка {i}* {verse_tag}{topic_tag}\n_{q['question']}_\n\n"
            breakdown += f"Ваш ответ: *{user_ans}*\n"
            breakdown += f"Правильно: *{correct_text}*\n\n"

            if "options_explanations" in q:
                breakdown += "*Разбор вариантов:*\n"
                for j, opt in enumerate(q["options"]):
                    breakdown += f"• _{opt}_\n{q['options_explanations'][j]}\n\n"

            breakdown += f"💡 *Пояснение:*\n{q['explanation']}"

            if q.get("pdf_ref"):
                breakdown += f"\n\n📄 _Источник: {q['pdf_ref']}_"

            if len(breakdown) > 4000:
                breakdown = breakdown[:3990] + "..."

            await message.reply_text(breakdown, parse_mode="Markdown")
    else:
        await message.reply_text("🎯 *Все ответы верны — отличная работа!*", parse_mode="Markdown")


# ═══════════════════════════════════════════════
# КОМАНДЫ
# ═══════════════════════════════════════════════

async def test_command(update: Update, context):
    await choose_level(update, context, is_callback=False)
    return CHOOSING_LEVEL


async def cancel(update: Update, context):
    await update.message.reply_text("❌ Отменено.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END


# ═══════════════════════════════════════════════
# ПОВТОРЕНИЕ ОШИБОК
# ═══════════════════════════════════════════════

async def retry_errors(update: Update, context):
    """Запускает сессию повторения — ошибочные вопросы один раз."""
    query   = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    target_id = int(query.data.replace("retry_errors_", ""))

    if target_id not in user_data:
        await query.edit_message_text("⚠️ Данные сессии устарели. Начни новый тест.")
        return ConversationHandler.END

    prev_data = user_data[target_id]
    answered  = prev_data.get("answered_questions", [])
    wrong_questions = [
        item["question_obj"] for item in answered
        if item["user_answer"] != item["question_obj"]["options"][item["question_obj"]["correct"]]
    ]

    if not wrong_questions:
        await query.answer("Ошибок нет!", show_alert=True)
        return

    user_data[user_id] = {
        "questions":           wrong_questions,
        "level_name":          f"🔁 Повторение ошибок ({prev_data['level_name']})",
        "level_key":           prev_data["level_key"],
        "current_question":    0,
        "correct_answers":     0,
        "answered_questions":  [],
        "start_time":          time.time(),
        "is_battle":           False,
        "battle_points":       0,
        "is_retry":            True,
    }

    await query.edit_message_text(
        f"🔁 *ПОВТОРЕНИЕ ОШИБОК*\n\n"
        f"Вопросов: {len(wrong_questions)}\nПоехали! 💪",
        parse_mode="Markdown",
    )
    await send_question(query.message, user_id)
    return ANSWERING



async def button_handler(update: Update, context):
    query = update.callback_query
    await query.answer()

    if query.data.startswith("leaderboard_page_"):
        page = int(query.data.replace("leaderboard_page_", ""))
        await show_general_leaderboard(query, page)
        return

    if query.data == "about":
        await query.edit_message_text(
            "📚 *О БОТЕ*\n\n"
            "Этот бот поможет проверить знания по Первому посланию Петра.\n\n"
            "*📋 КАТЕГОРИИ ТЕСТОВ:*\n"
            "📜 Введение: Авторство ч.1 — 2 балла\n"
            "📜 Введение: Авторство ч.2 — 2 балла\n"
            "📜 Введение: Структура и цель — 2 балла\n"
            "🟢 Основы (1:1–25) — 1 балл\n"
            "🟡 Контекст (1:1–25) — 2 балла\n"
            "🔴 Богословие (1:1–25) — 3 балла\n"
            "🙏 Применение (1:1–25) — 2 балла\n"
            "🔬 Лингвистика: Избранные и странники ч.1 — 3 балла\n"
            "🔬 Лингвистика: Живая надежда ч.2 — 3 балла\n"
            "🔬 Лингвистика: Искупление и истина ч.3 — 3 балла\n"
            "👑 Нерон — 2 балла\n"
            "🌍 География — 2 балла\n\n"
            "*⚔️ РЕЖИМ БИТВЫ:*\n"
            "• Создай битву или присоединись\n"
            "• Отвечай на те же вопросы\n"
            "• Победитель получает +5 баллов!\n\n"
            "*🔁 РЕЖИМ ПОВТОРЕНИЯ:*\n"
            "• После теста — кнопка «Повторить ошибки»\n"
            "• Учишь до 2 правильных ответов подряд\n\n"
            "💡 Каждый тест — 10 случайных вопросов!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
            ]),
            parse_mode="Markdown",
        )
    elif query.data == "start_test":
        await choose_level(update, context, is_callback=True)
    elif query.data == "battle_menu":
        await show_battle_menu(query)
    elif query.data == "leaderboard":
        await show_general_leaderboard(query, 0)
    elif query.data == "my_stats":
        await show_my_stats(query)
    elif query.data == "historical_menu":
        await historical_menu(update, context)
    elif query.data == "challenge_menu":
        await challenge_menu(update, context)
    elif query.data == "achievements":
        await show_achievements(update, context)
    elif query.data == "coming_soon":
        await query.answer("🚧 Глава 2 в разработке — следи за обновлениями!", show_alert=True)


async def category_leaderboard_handler(update: Update, context):
    query = update.callback_query
    await query.answer()
    category_key = query.data.replace("cat_lb_", "")
    await show_category_leaderboard(query, category_key)



# ═══════════════════════════════════════════════
# РЕЖИМ БИТВЫ
# ═══════════════════════════════════════════════

async def show_battle_menu(query):
    # Очищаем устаревшие битвы (старше 10 минут) при каждом открытии меню
    cutoff = time.time() - 600
    stale  = [bid for bid, b in list(pending_battles.items()) if b.get("created_at", 0) < cutoff]
    for bid in stale:
        del pending_battles[bid]

    available = [
        (bid, b["creator_name"])
        for bid, b in pending_battles.items()
        if b["status"] == "waiting"
    ]

    keyboard = [[InlineKeyboardButton("🆕 Создать битву", callback_data="create_battle")]]
    for bid, creator_name in available[:5]:
        keyboard.append([InlineKeyboardButton(f"⚔️ vs {creator_name}", callback_data=f"join_battle_{bid}")])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")])

    text = "⚔️ *РЕЖИМ БИТВЫ*\n\n🎯 Соревнуйся с другими игроками!\n"
    text += "• Оба отвечают на одинаковые вопросы\n"
    text += "• Побеждает тот, кто ответит лучше\n"
    text += "• Победа = +5 баллов, ничья = +2\n\n"
    text += f"📋 *Доступных битв:* {len(available)}\n" if available else "📋 *Нет доступных битв*\nСоздай свою!\n"

    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def create_battle(update: Update, context):
    query    = update.callback_query
    await query.answer()
    user_id  = query.from_user.id
    user_name = query.from_user.first_name
    battle_id = f"battle_{user_id}_{int(time.time())}"

    pending_battles[battle_id] = {
        "creator_id":       user_id,
        "creator_name":     user_name,
        "questions":        random.sample(all_chapter1_questions, 10),
        "status":           "waiting",
        "creator_score":    0,
        "creator_answers":  [],
        "creator_time":     0,
        "opponent_id":      None,
        "opponent_name":    None,
        "opponent_score":   0,
        "opponent_answers": [],
        "opponent_time":    0,
        "created_at":       time.time(),
    }

    await query.edit_message_text(
        "⚔️ *БИТВА СОЗДАНА!*\n\n"
        f"🆔 ID: `{battle_id[-8:]}`\n\n"
        "⏳ Ожидание соперника...\nИли начни отвечать первым!\n\n"
        "_Битва автоматически удалится через 10 минут_",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Начать отвечать", callback_data=f"start_battle_{battle_id}_creator")],
            [InlineKeyboardButton("❌ Отменить битву",  callback_data=f"cancel_battle_{battle_id}")],
            [InlineKeyboardButton("⬅️ Назад",           callback_data="battle_menu")],
        ]),
        parse_mode="Markdown",
    )


async def join_battle(update: Update, context):
    query    = update.callback_query
    await query.answer()
    battle_id = query.data.replace("join_battle_", "")
    user_id   = query.from_user.id
    user_name = query.from_user.first_name

    if battle_id not in pending_battles:
        await query.edit_message_text(
            "❌ Битва не найдена или уже завершена.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="battle_menu")]]),
        )
        return

    battle = pending_battles[battle_id]
    if battle["creator_id"] == user_id:
        await query.answer("Нельзя присоединиться к своей битве!", show_alert=True)
        return
    if battle["opponent_id"] is not None:
        await query.answer("К этой битве уже присоединился другой игрок!", show_alert=True)
        return

    battle["opponent_id"]   = user_id
    battle["opponent_name"] = user_name
    battle["status"]        = "in_progress"

    await query.edit_message_text(
        f"⚔️ *БИТВА НАЧАЛАСЬ!*\n\n"
        f"👤 Ты vs 👤 {battle['creator_name']}\n\n"
        "📝 10 вопросов\n⏱ Время учитывается!\n\nНажми «Начать отвечать»",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Начать отвечать", callback_data=f"start_battle_{battle_id}_opponent")],
            [InlineKeyboardButton("⬅️ Назад",           callback_data="battle_menu")],
        ]),
        parse_mode="Markdown",
    )


async def start_battle_questions(update: Update, context):
    query = update.callback_query
    await query.answer()

    data_parts = query.data.replace("start_battle_", "").rsplit("_", 1)
    battle_id  = data_parts[0]
    role       = data_parts[1]

    if battle_id not in pending_battles:
        await query.edit_message_text("❌ Битва не найдена.")
        return

    user_id = query.from_user.id
    user_data[user_id] = {
        "battle_id":       battle_id,
        "role":            role,
        "questions":       pending_battles[battle_id]["questions"],
        "current_question": 0,
        "correct_answers": 0,
        "start_time":      time.time(),
        "is_battle":       True,
    }

    await query.edit_message_text(
        "⚔️ *БИТВА: Вопрос 1/10*\n\nНачинаем! Удачи! 🍀",
        parse_mode="Markdown",
    )
    await send_battle_question(query.message, user_id)
    return BATTLE_ANSWERING


async def send_battle_question(message, user_id):
    data  = user_data[user_id]
    q_num = data["current_question"]

    if q_num >= len(data["questions"]):
        await finish_battle_for_user(message, user_id)
        return

    q            = data["questions"][q_num]
    correct_text = q["options"][q["correct"]]
    shuffled     = q["options"][:]
    random.shuffle(shuffled)

    data["current_options"]      = shuffled
    data["current_correct_text"] = correct_text
    data["question_sent_at"]     = time.time()

    await message.reply_text(
        f"⚔️ *Вопрос {q_num + 1}/10* ⚡ Быстрее = больше очков!\n\n{q['question']}",
        reply_markup=ReplyKeyboardMarkup(
            [[opt] for opt in shuffled],
            one_time_keyboard=True, resize_keyboard=True,
        ),
        parse_mode="Markdown",
    )


async def battle_answer(update: Update, context):
    user_id = update.effective_user.id

    if user_id not in user_data or not user_data[user_id].get("is_battle"):
        return await answer(update, context)

    data        = user_data[user_id]
    q_num       = data["current_question"]
    q           = data["questions"][q_num]
    user_answer = update.message.text

    correct_text    = data.get("current_correct_text") or q["options"][q["correct"]]
    current_options = data.get("current_options") or q["options"]

    if user_answer not in current_options:
        await update.message.reply_text("Выбери вариант из списка")
        return BATTLE_ANSWERING

    sent_at  = data.get("question_sent_at", time.time())
    elapsed  = min(time.time() - sent_at, 7.0)

    if user_answer == correct_text:
        data["correct_answers"] += 1
        speed_bonus  = round((7.0 - elapsed) / 7.0 * 7)
        points       = 10 + speed_bonus
        data["battle_points"] = data.get("battle_points", 0) + points
        await update.message.reply_text(
            f"✅ +{points} очков (⚡{speed_bonus} бонус за скорость)",
            reply_markup=ReplyKeyboardRemove(),
        )
    else:
        await update.message.reply_text(f"❌ {correct_text}", reply_markup=ReplyKeyboardRemove())

    data["current_question"] += 1
    if data["current_question"] < len(data["questions"]):
        await send_battle_question(update.message, user_id)
        return BATTLE_ANSWERING
    else:
        await finish_battle_for_user(update.message, user_id)
        return ConversationHandler.END


async def finish_battle_for_user(message, user_id):
    data      = user_data[user_id]
    battle_id = data["battle_id"]
    role      = data["role"]
    time_taken = time.time() - data["start_time"]

    if battle_id not in pending_battles:
        await message.reply_text("❌ Битва не найдена.")
        return

    battle        = pending_battles[battle_id]
    battle_points = data.get("battle_points", 0)

    if role == "creator":
        battle.update({
            "creator_score":    data["correct_answers"],
            "creator_time":     time_taken,
            "creator_points":   battle_points,
            "creator_finished": True,
        })
    else:
        battle.update({
            "opponent_score":    data["correct_answers"],
            "opponent_time":     time_taken,
            "opponent_points":   battle_points,
            "opponent_finished": True,
        })

    if battle.get("creator_finished") and battle.get("opponent_finished"):
        await show_battle_results(message, battle_id)
    else:
        await message.reply_text(
            f"✅ *Ты закончил!*\n\n"
            f"📊 Твой результат: {data['correct_answers']}/10\n"
            f"⏱ Время: {format_time(time_taken)}\n\n"
            "⏳ Ожидание соперника...",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В меню", callback_data="back_to_main")]]),
        )


async def show_battle_results(message, battle_id):
    if battle_id not in pending_battles:
        return

    battle         = pending_battles[battle_id]
    creator_points = battle.get("creator_points", 0)
    opponent_points = battle.get("opponent_points", 0)

    if creator_points > opponent_points:
        winner, winner_name = "creator", battle["creator_name"]
    elif opponent_points > creator_points:
        winner, winner_name = "opponent", battle.get("opponent_name", "Соперник")
    else:
        winner, winner_name = "draw", None

    if winner == "creator":
        update_battle_stats(battle["creator_id"], "win")
        update_battle_stats(battle["opponent_id"], "lose")
    elif winner == "opponent":
        update_battle_stats(battle["creator_id"], "lose")
        update_battle_stats(battle["opponent_id"], "win")
    else:
        update_battle_stats(battle["creator_id"], "draw")
        update_battle_stats(battle["opponent_id"], "draw")

    text  = "⚔️ *РЕЗУЛЬТАТЫ БИТВЫ*\n\n"
    text += f"🏆 *Победитель: {winner_name}!*\n\n" if winner != "draw" else "🤝 *НИЧЬЯ!*\n\n"
    text += (f"👤 *{battle['creator_name']}*\n"
             f"   ✅ {battle['creator_score']}/10 • ⚡ {creator_points} очков • ⏱ {format_time(battle['creator_time'])}\n\n")
    text += (f"👤 *{battle.get('opponent_name', 'Соперник')}*\n"
             f"   ✅ {battle['opponent_score']}/10 • ⚡ {opponent_points} очков • ⏱ {format_time(battle['opponent_time'])}\n\n")
    text += "💎 *+5 баллов* победителю!\n" if winner != "draw" else "💎 *+2 балла* каждому!\n"

    await message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Новая битва", callback_data="battle_menu")],
            [InlineKeyboardButton("⬅️ В меню",       callback_data="back_to_main")],
        ]),
        parse_mode="Markdown",
    )
    del pending_battles[battle_id]


async def cancel_battle(update: Update, context):
    query = update.callback_query
    await query.answer()
    battle_id = query.data.replace("cancel_battle_", "")
    pending_battles.pop(battle_id, None)
    await query.edit_message_text(
        "❌ Битва отменена.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="battle_menu")]]),
    )


# ═══════════════════════════════════════════════
# СТАТИСТИКА И ЛИДЕРБОРД
# ═══════════════════════════════════════════════

async def show_my_stats(query):
    user_id  = query.from_user.id
    position, entry = get_user_position(user_id)

    if not entry:
        await query.edit_message_text(
            "📊 *МОЯ СТАТИСТИКА*\n\nВы ещё не проходили тесты.\nИспользуйте /test чтобы начать!",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎯 Начать тест", callback_data="start_test")],
                [InlineKeyboardButton("⬅️ Назад",       callback_data="back_to_main")],
            ]),
            parse_mode="Markdown",
        )
        return

    total_tests     = entry.get("total_tests", 0)
    total_questions = entry.get("total_questions_answered", 0)
    total_correct   = entry.get("total_correct_answers", 0)
    avg_time        = entry.get("total_time_spent", 0) / max(total_tests, 1)
    days_playing    = calculate_days_playing(entry.get("first_play_date", datetime.now().strftime("%Y-%m-%d")))

    battles_played = entry.get("battles_played", 0)
    battles_won    = entry.get("battles_won", 0)

    text  = "📊 *МОЯ СТАТИСТИКА*\n\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n👤 *ОБЩАЯ ИНФОРМАЦИЯ*\n━━━━━━━━━━━━━━━━━━━━\n"
    text += f"🏅 Позиция в рейтинге: *#{position}*\n"
    text += f"💎 Всего баллов: *{entry.get('total_points', 0)}*\n"
    text += f"📅 Дней в игре: *{days_playing}*\n"
    text += f"🎯 Тестов пройдено: *{total_tests}*\n"
    text += f"📝 Вопросов отвечено: *{total_questions}*\n"
    text += f"✅ Общая точность: *{calculate_accuracy(total_correct, total_questions)}%*\n"
    text += f"⏱ Среднее время теста: *{format_time(avg_time)}*\n\n"

    text += "━━━━━━━━━━━━━━━━━━━━\n⚔️ *БИТВЫ*\n━━━━━━━━━━━━━━━━━━━━\n"
    text += f"🎮 Сыграно: *{battles_played}*\n"
    text += f"🏆 Побед: *{battles_won}*\n"
    text += f"💔 Поражений: *{entry.get('battles_lost', 0)}*\n"
    text += f"🤝 Ничьих: *{entry.get('battles_draw', 0)}*\n"
    if battles_played > 0:
        text += f"📈 Винрейт: *{round(battles_won / battles_played * 100)}%*\n"
    text += "\n"

    text += "━━━━━━━━━━━━━━━━━━━━\n📚 *ПО КАТЕГОРИЯМ*\n━━━━━━━━━━━━━━━━━━━━\n"
    for key, name in [
        ("easy", "🟢 Основы"), ("medium", "🟡 Контекст"), ("hard", "🔴 Богословие"),
        ("nero", "👑 Нерон"), ("geography", "🌍 География"),
    ]:
        attempts = entry.get(f"{key}_attempts", 0)
        if attempts > 0:
            acc  = calculate_accuracy(entry.get(f"{key}_correct", 0), entry.get(f"{key}_total", 0))
            best = entry.get(f"{key}_best_score", 0)
            text += f"{name}: *{acc}%* (лучший: {best}/10)\n"
        else:
            text += f"{name}: _не пройдено_\n"

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎯 Начать тест",  callback_data="start_test")],
            [InlineKeyboardButton("🎲 Random",        callback_data="challenge_menu")],
            [InlineKeyboardButton("🏅 Достижения",    callback_data="achievements")],
            [InlineKeyboardButton("⚔️ Битва",          callback_data="battle_menu")],
            [InlineKeyboardButton("⬅️ Назад",           callback_data="back_to_main")],
        ]),
        parse_mode="Markdown",
    )


async def show_general_leaderboard(query, page=0):
    users       = get_leaderboard_page(page)
    total_users = get_total_users()
    user_id     = query.from_user.id

    if not users:
        text = "🏆 *ТАБЛИЦА ЛИДЕРОВ*\n\nПока никто не проходил тесты.\nБудь первым! 🚀"
    else:
        text = f"🏆 *ТАБЛИЦА ЛИДЕРОВ* (Стр. {page + 1} из {(total_users - 1) // 10 + 1}) • Всего: {total_users}\n"
        start_rank = page * 10 + 1

        for i, entry in enumerate(users, start_rank):
            name   = entry.get("first_name", "Unknown")[:15]
            pts    = entry.get("total_points", 0)
            tests  = entry.get("total_tests", 0)
            wins   = entry.get("battles_won", 0)

            if i == 1:
                text += f"\n🥇 *{name}*\n"
                text += f"    💎 {pts} очков • 🎯 {tests} тестов • ⚔️ {wins} побед\n"
            elif i == 2:
                text += f"\n🥈 *{name}*\n"
                text += f"    💎 {pts} очков • 🎯 {tests} тестов • ⚔️ {wins} побед\n"
            elif i == 3:
                text += f"\n🥉 *{name}*\n"
                text += f"    💎 {pts} очков • 🎯 {tests} тестов • ⚔️ {wins} побед\n"
            else:
                if i == 4:
                    text += "\n━━━━━━━━━━━━━━━━━━━━\n"
                text += f"*{i}.* {name} — 💎 {pts}\n"

    # Карточка "Я в рейтинге"
    position, my_entry = get_user_position(user_id)
    if my_entry and position:
        my_pts    = my_entry.get("total_points", 0)
        gap       = get_points_to_next_place(user_id)
        text += "\n━━━━━━━━━━━━━━━━━━━━\n"
        text += f"👤 *Ваше место:* #{position} из {total_users}\n"
        text += f"💎 У вас: *{my_pts} очков*\n"
        if gap is not None:
            text += f"🎯 До следующего места: *+{gap} очков*"
        else:
            text += "🏆 Вы на первом месте!"

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"leaderboard_page_{page-1}"))
    if (page + 1) * 10 < total_users:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"leaderboard_page_{page+1}"))

    keyboard = []
    if nav:
        keyboard.append(nav)
    keyboard.append([
        InlineKeyboardButton("🔬 Лингвисты",  callback_data="cat_lb_linguistics_ch1"),
        InlineKeyboardButton("🔴 Богословы",  callback_data="cat_lb_hard"),
    ])
    keyboard.append([
        InlineKeyboardButton("👑 Нерон",      callback_data="cat_lb_nero"),
        InlineKeyboardButton("🌍 География",  callback_data="cat_lb_geography"),
    ])
    keyboard.append([InlineKeyboardButton("🎯 Начать тест", callback_data="start_test")])
    keyboard.append([InlineKeyboardButton("⬅️ В меню",      callback_data="back_to_main")])

    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def show_category_leaderboard(query, category_key):
    CATEGORY_NAMES = {
        "easy":            "🟢 Основы",
        "medium":          "🟡 Контекст",
        "hard":            "🔴 Богословие",
        "nero":            "👑 Нерон",
        "geography":       "🌍 География",
        "practical_ch1":   "🙏 Применение",
        "linguistics_ch1": "🔬 Лингвистика",
        "intro1":          "📜 Введение ч.1",
        "intro2":          "📜 Введение ч.2",
        "intro3":          "📜 Введение ч.3",
    }
    cat_name = CATEGORY_NAMES.get(category_key, category_key)
    users    = get_category_leaderboard(category_key, limit=10)

    if not users:
        text = f"{cat_name}\n\nПока никто не проходил этот тест."
    else:
        text = f"🏆 *РЕЙТИНГ: {cat_name}*\n_(по числу верных ответов)_\n\n"
        for i, entry in enumerate(users, 1):
            name    = entry.get("first_name", "Unknown")[:15]
            correct = entry.get(f"{category_key}_correct", 0)
            total   = entry.get(f"{category_key}_total", 0)
            best    = entry.get(f"{category_key}_best_score", 0)
            acc     = calculate_accuracy(correct, total)
            medal   = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
            text   += f"{medal} *{name}* — {correct} верных ({acc}%) • лучший: {best}/10\n"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Общий рейтинг", callback_data="leaderboard")],
        [InlineKeyboardButton("⬅️ В меню",         callback_data="back_to_main")],
    ])
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")





# ═══════════════════════════════════════════════
# RANDOM CHALLENGE — ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════

def build_progress_bar(current, total=20, length=10):
    """Строит прогресс-бар: ▰▰▰▱▱▱▱▱▱▱"""
    filled = round(current / total * length)
    return "▰" * filled + "▱" * (length - filled)


def pick_challenge_questions(mode):
    """
    Умная выборка 20 вопросов по квотам.
    Normal:   6 easy, 6 medium, 6 hard, 1 practical, 1 linguistics
    Hardcore: 4 easy, 5 medium, 7 hard, 4 linguistics
    """
    pool_easy   = easy_questions + easy_questions_v17_25
    pool_medium = medium_questions + medium_questions_v17_25
    pool_hard   = hard_questions + hard_questions_v17_25
    pool_prac   = practical_ch1_questions + practical_v17_25_questions
    pool_ling   = linguistics_ch1_questions + linguistics_ch1_questions_2 + linguistics_v17_25_questions

    def safe_sample(pool, n):
        pool = list(pool)
        if len(pool) >= n:
            return random.sample(pool, n)
        return random.choices(pool, k=n)  # повторы если мало вопросов

    if mode == "random20":
        questions = (
            safe_sample(pool_easy,   6) +
            safe_sample(pool_medium, 6) +
            safe_sample(pool_hard,   6) +
            safe_sample(pool_prac,   1) +
            safe_sample(pool_ling,   1)
        )
    else:  # hardcore20
        questions = (
            safe_sample(pool_easy,   4) +
            safe_sample(pool_medium, 5) +
            safe_sample(pool_hard,   7) +
            safe_sample(pool_ling,   4)
        )

    random.shuffle(questions)
    return questions


def build_rules_card(mode, eligible):
    """Строит красивый экран правил."""
    today_status = "✅ доступен" if eligible else "❌ уже получен сегодня"

    if mode == "random20":
        title   = "🎲 *Random Challenge (20)*"
        rules   = "• 20 вопросов • умный рандом • без таймера"
        bonus_t = (
            "20/20 → +100 💎\n"
            "19/20 → +80 💎\n"
            "18/20 → +60 💎\n"
            "17/20 → +40 💎\n"
            "16/20 → +25 💎\n"
            "15/20 → +10 💎\n"
            "ниже 15 → 0"
        )
        ppq = 1
    else:
        title   = "💀 *Hardcore Random (20)*"
        rules   = "• 20 вопросов • уклон в hard/лингвистику • ⏱ 7 сек"
        bonus_t = (
            "20/20 → +200 💎\n"
            "19/20 → +150 💎\n"
            "18/20 → +110 💎\n"
            "17/20 → +80 💎\n"
            "16/20 → +50 💎\n"
            "15/20 → +25 💎\n"
            "ниже 15 → 0"
        )
        ppq = 2

    return (
        f"{title}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"*Правила:*\n"
        f"{rules}\n"
        f"• Очков за вопрос: {ppq}\n"
        f"• Подсказки: _выключены_\n"
        f"• Супер-бонус: _1 раз в день_\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"*Бонусы:*\n"
        f"{bonus_t}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"*Статус бонуса сегодня:* {today_status}"
    )


# ═══════════════════════════════════════════════
# RANDOM CHALLENGE — HANDLERS
# ═══════════════════════════════════════════════

async def challenge_menu(update: Update, context):
    """Меню выбора режима челленджа со статусом бонуса."""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    normal_ok   = is_bonus_eligible(user_id, "random20")
    hardcore_ok = is_bonus_eligible(user_id, "hardcore20")

    def badge(ok):
        return "✅ доступен" if ok else "❌ уже получен"

    text = (
        "🎲 *RANDOM CHALLENGE (20)*\n\n"
        "20 вопросов • умный рандом • подсказки выключены\n\n"
        "💎 *Очки:*\n"
        "• Normal: 1 за верный ответ + супер-бонус\n"
        "• Hardcore: 2 за верный ответ + супер-бонус\n\n"
        f"🎁 *Бонус сегодня:*\n"
        f"• 🎲 Normal:   {badge(normal_ok)}\n"
        f"• 💀 Hardcore: {badge(hardcore_ok)}\n\n"
        "Это лучший режим для обучения — вопросы\n"
        "покрывают ключевые темы и стихи.\n\n"
        "Выбери режим:"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎲 Normal (20) — без таймера", callback_data="challenge_rules_random20")],
        [InlineKeyboardButton("💀 Hardcore (20) — 7 сек",     callback_data="challenge_rules_hardcore20")],
        [InlineKeyboardButton("🏆 Лидерборд недели",          callback_data="weekly_lb_random20")],
        [InlineKeyboardButton("⬅️ Назад",                      callback_data="back_to_main")],
    ])
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")


async def challenge_rules(update: Update, context):
    """Экран правил перед стартом."""
    query  = update.callback_query
    await query.answer()
    mode   = query.data.replace("challenge_rules_", "")
    user_id = query.from_user.id
    eligible = is_bonus_eligible(user_id, mode)

    text = build_rules_card(mode, eligible)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Начать!", callback_data=f"challenge_start_{mode}")],
        [InlineKeyboardButton("⬅️ Назад",   callback_data="challenge_menu")],
    ])
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")


async def challenge_start(update: Update, context):
    """Запускает сессию челленджа."""
    query   = update.callback_query
    await query.answer()
    mode    = query.data.replace("challenge_start_", "")
    user_id = query.from_user.id
    eligible = is_bonus_eligible(user_id, mode)

    questions = pick_challenge_questions(mode)

    time_limit = 7 if mode == "hardcore20" else None
    mode_name  = "🎲 Random Challenge" if mode == "random20" else "💀 Hardcore Random"

    user_data[user_id] = {
        "questions":           questions,
        "level_name":          mode_name,
        "level_key":           mode,
        "current_question":    0,
        "correct_answers":     0,
        "answered_questions":  [],
        "start_time":          time.time(),
        "is_battle":           False,
        "battle_points":       0,
        "is_challenge":        True,
        "challenge_mode":      mode,
        "challenge_eligible":  eligible,
        "challenge_time_limit": time_limit,
    }

    bonus_status = "✅ бонус доступен" if eligible else "❌ бонус уже получен"
    await query.edit_message_text(
        f"{mode_name}\n\n"
        f"📋 20 вопросов • {bonus_status}\n\n"
        f"Поехали! 💪",
        parse_mode="Markdown",
    )
    await send_challenge_question(query.message, user_id)
    return ANSWERING


async def send_challenge_question(message, user_id):
    """Отправляет вопрос в режиме челленджа с прогресс-баром."""
    data  = user_data[user_id]
    q_num = data["current_question"]
    total = len(data["questions"])

    if q_num >= total:
        await show_challenge_results(message, user_id)
        return

    q            = data["questions"][q_num]
    correct_text = q["options"][q["correct"]]
    shuffled     = q["options"][:]
    random.shuffle(shuffled)

    data["current_options"]      = shuffled
    data["current_correct_text"] = correct_text
    data["question_sent_at"]     = time.time()

    # Отменяем предыдущий таймер
    old_task = data.get("timer_task")
    if old_task and not old_task.done():
        old_task.cancel()

    progress   = build_progress_bar(q_num, total)
    correct_so_far = data["correct_answers"]
    bonus_icon = "✅" if data["challenge_eligible"] else "❌"
    mode_name  = data["level_name"]
    time_limit = data.get("challenge_time_limit")
    timer_str  = f" • ⏱ {time_limit} сек" if time_limit else ""

    header = (
        f"{mode_name} • {bonus_icon} бонус\n"
        f"Вопрос *{q_num + 1}/{total}*{timer_str}\n"
        f"{progress}\n"
        f"✅ Правильно: {correct_so_far}/{q_num}\n\n"
    ) if q_num > 0 else (
        f"{mode_name} • {bonus_icon} бонус\n"
        f"Вопрос *{q_num + 1}/{total}*{timer_str}\n"
        f"{progress}\n\n"
    )

    await message.reply_text(
        f"{header}{q['question']}",
        reply_markup=ReplyKeyboardMarkup(
            [[opt] for opt in shuffled],
            one_time_keyboard=True, resize_keyboard=True,
        ),
        parse_mode="Markdown",
    )

    # Таймер только для Hardcore
    if time_limit:
        data["timer_task"] = asyncio.create_task(
            challenge_timeout(message, user_id, q_num)
        )


async def challenge_timeout(message, user_id, q_num_at_send):
    """Таймер для Hardcore режима."""
    data = user_data.get(user_id)
    if not data:
        return
    time_limit = data.get("challenge_time_limit", 7)
    await asyncio.sleep(time_limit)

    if user_id not in user_data:
        return
    data = user_data[user_id]
    if data.get("current_question") != q_num_at_send:
        return

    q            = data["questions"][q_num_at_send]
    correct_text = data.get("current_correct_text") or q["options"][q["correct"]]

    data["answered_questions"].append({
        "question_obj": q,
        "user_answer":  "⏱ Время вышло",
    })
    try:
        await message.reply_text(
            f"⏱ *Время вышло!*\n✅ {correct_text}",
            reply_markup=ReplyKeyboardRemove(),
            parse_mode="Markdown",
        )
    except Exception:
        return

    data["current_question"] += 1
    if data["current_question"] < len(data["questions"]):
        await send_challenge_question(message, user_id)
    else:
        await show_challenge_results(message, user_id)


async def challenge_answer(update: Update, context):
    """Обработчик ответов в режиме челленджа."""
    user_id = update.effective_user.id
    data    = user_data.get(user_id)

    if not data or not data.get("is_challenge"):
        return await answer(update, context)

    q_num       = data["current_question"]
    q           = data["questions"][q_num]
    user_answer = update.message.text
    correct_text    = data.get("current_correct_text") or q["options"][q["correct"]]
    current_options = data.get("current_options") or q["options"]

    if user_answer not in current_options:
        await update.message.reply_text("Выбери вариант из списка")
        return ANSWERING

    # Отменяем таймер
    timer_task = data.get("timer_task")
    if timer_task and not timer_task.done():
        timer_task.cancel()

    is_correct = (user_answer == correct_text)
    if is_correct:
        data["correct_answers"] += 1
        await update.message.reply_text("✅ Верно!", reply_markup=ReplyKeyboardRemove())
    else:
        await update.message.reply_text(
            f"❌ Неверно\n✅ {correct_text}",
            reply_markup=ReplyKeyboardRemove(),
        )

    # Статистика по вопросу
    elapsed = time.time() - data.get("question_sent_at", time.time())
    q_id = str(q.get("id", hash(q["question"])))
    record_question_stat(q_id, data["level_key"], is_correct, elapsed)

    data["answered_questions"].append({"question_obj": q, "user_answer": user_answer})
    data["current_question"] += 1

    if data["current_question"] < len(data["questions"]):
        await send_challenge_question(update.message, user_id)
        return ANSWERING
    else:
        await show_challenge_results(update.message, user_id)
        return ConversationHandler.END


async def show_challenge_results(message, user_id):
    """Красивый экран результатов с анимацией подсчёта."""
    data       = user_data[user_id]
    score      = data["correct_answers"]
    total      = len(data["questions"])
    mode       = data["challenge_mode"]
    eligible   = data["challenge_eligible"]
    time_taken = time.time() - data["start_time"]
    user       = message.from_user

    # Анимация подсчёта
    anim_msg = await message.reply_text("📊 Подсчитываю результат…")
    try:
        await asyncio.sleep(0.4)
        await anim_msg.edit_text("📊 Подсчитываю результат… ▰▱▱")
        await asyncio.sleep(0.4)
        await anim_msg.edit_text("📊 Подсчитываю результат… ▰▰▱")
        await asyncio.sleep(0.4)
        await anim_msg.edit_text("📊 Готово! ✨")
    except Exception:
        pass  # Telegram может отклонить если текст не изменился

    # Считаем очки
    points_per_q = 1 if mode == "random20" else 2
    earned_base  = score * points_per_q
    bonus        = compute_bonus(score, mode, eligible)
    total_earned = earned_base + bonus

    # Записываем в БД
    total_credited, new_achievements = update_challenge_stats(
        user.id, user.username, user.first_name,
        mode, score, total, time_taken, eligible
    )
    if eligible:
        update_weekly_leaderboard(
            user.id, user.username, user.first_name,
            mode, score, time_taken
        )

    # Оценка
    pct = round(score / total * 100)
    if pct == 100:   grade = "🌟 Идеально!"
    elif pct >= 90:  grade = "🔥 Отлично!"
    elif pct >= 75:  grade = "👍 Хорошо"
    elif pct >= 60:  grade = "📖 Неплохо"
    else:            grade = "📚 Нужно повторить"

    mode_name = "🎲 Random Challenge" if mode == "random20" else "💀 Hardcore Random"
    position, _ = get_user_position(user.id)

    result = (
        f"━━━━━━━━━━━━━━━━\n"
        f"{mode_name}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📊 Результат: *{score}/{total}* ({pct}%) {grade}\n"
        f"⏱ Время: *{format_time(time_taken)}*\n"
        f"🏅 Позиция: *#{position}*\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💎 Обычные очки: +{earned_base} ({score} × {points_per_q})\n"
    )

    if eligible:
        if bonus > 0:
            result += f"🎁 Супер-бонус: *+{bonus}*\n"
        else:
            result += f"🎁 Супер-бонус: 0 (нужно 15+)\n"
        result += f"✨ Итого начислено: *+{total_earned}*\n"
    else:
        result += f"🎁 Бонус: _недоступен (уже получен сегодня)_\n"
        result += f"✨ Начислено: *+{earned_base}*\n"

    # Достижения
    if new_achievements:
        result += "━━━━━━━━━━━━━━━━\n"
        result += "🏅 *Новые достижения:*\n"
        for ach in new_achievements:
            result += f"  {ach}\n"

    result += "━━━━━━━━━━━━━━━━"

    # Кнопки
    answered  = data.get("answered_questions", [])
    wrong     = [i for i in answered
                 if i["user_answer"] != i["question_obj"]["options"][i["question_obj"]["correct"]]]
    kb_rows = [
        [InlineKeyboardButton(f"🔁 Сыграть ещё раз", callback_data=f"challenge_rules_{mode}")],
        [InlineKeyboardButton("🏆 Лидерборд недели",  callback_data=f"weekly_lb_{mode}")],
        [InlineKeyboardButton("🏅 Достижения",         callback_data="achievements")],
        [InlineKeyboardButton("⬅️ Меню",               callback_data="back_to_main")],
    ]
    if wrong:
        kb_rows.insert(1, [InlineKeyboardButton(
            f"📌 Повторить ошибки ({len(wrong)})",
            callback_data=f"retry_errors_{user_id}"
        )])

    await message.reply_text(result, reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode="Markdown")

    # Разбор ошибок (как в обычном тесте)
    if wrong:
        await message.reply_text(f"❌ *РАЗБОР ОШИБОК ({len(wrong)} из {total}):*", parse_mode="Markdown")
        for i, item in enumerate(wrong, 1):
            q            = item["question_obj"]
            correct_text = q["options"][q["correct"]]
            breakdown    = f"❌ *Ошибка {i}*\n_{q['question']}_\n\n"
            breakdown   += f"Ваш ответ: *{item['user_answer']}*\n"
            breakdown   += f"Правильно: *{correct_text}*\n\n"
            breakdown   += f"💡 {q.get('explanation', '')}"
            if len(breakdown) > 4000:
                breakdown = breakdown[:3990] + "..."
            await message.reply_text(breakdown, parse_mode="Markdown")
    else:
        await message.reply_text("🎯 *Все ответы верны!*", parse_mode="Markdown")


# ═══════════════════════════════════════════════
# ДОСТИЖЕНИЯ
# ═══════════════════════════════════════════════

async def show_achievements(update: Update, context):
    """Экран достижений."""
    query   = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    achievements, streak_count, streak_last = get_user_achievements(user_id)

    def ach_status(key, name, desc):
        if key in achievements:
            return f"✅ *{name}*\n   _{desc}_\n   📅 Получено: {achievements[key]}\n"
        return f"🔒 *{name}*\n   _{desc}_\n"

    text = (
        "🏅 *МОИ ДОСТИЖЕНИЯ*\n"
        "━━━━━━━━━━━━━━━━\n\n"
        + ach_status("perfect_20",  "Perfect 20",         "Ответить на все 20 вопросов правильно")
        + "\n"
        + ach_status("streak_3",    "Серия 18+ (3 дня)",  "3 дня подряд набирать 18+ в Random Challenge")
        + "\n"
        "━━━━━━━━━━━━━━━━\n"
        f"🔥 *Текущая серия:* {streak_count} дн."
    )
    if streak_last:
        text += f"\n📅 Последний раз: {streak_last}"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
    ])
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")


# ═══════════════════════════════════════════════
# ЕЖЕНЕДЕЛЬНЫЙ ЛИДЕРБОРД
# ═══════════════════════════════════════════════

async def show_weekly_leaderboard(update: Update, context):
    """Еженедельный лидерборд по режиму."""
    query  = update.callback_query
    await query.answer()
    mode   = query.data.replace("weekly_lb_", "")
    users  = get_weekly_leaderboard(mode)

    mode_name = "🎲 Random Challenge" if mode == "random20" else "💀 Hardcore Random"
    week_id   = get_current_week_id()

    if not users:
        text = f"🏆 *{mode_name}*\nНеделя {week_id}\n\nПока нет результатов.\nБудь первым! 🚀"
    else:
        text = f"🏆 *{mode_name}*\nНеделя {week_id}\n\n"
        for i, entry in enumerate(users, 1):
            medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
            name  = entry.get("first_name", "?")[:15]
            score = entry.get("best_score", 0)
            t     = format_time(entry.get("best_time", 0))
            text += f"{medal} *{name}* — {score}/20 • ⏱ {t}\n"

    other_mode      = "hardcore20" if mode == "random20" else "random20"
    other_mode_name = "💀 Hardcore" if mode == "random20" else "🎲 Normal"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Переключить → {other_mode_name}", callback_data=f"weekly_lb_{other_mode}")],
        [InlineKeyboardButton("🎲 Сыграть",  callback_data=f"challenge_rules_{mode}")],
        [InlineKeyboardButton("⬅️ Назад",    callback_data="challenge_menu")],
    ])
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")


# ═══════════════════════════════════════════════
# ОЧИСТКА УСТАРЕВШИХ БИТВ (JobQueue)
# ═══════════════════════════════════════════════

async def cleanup_old_battles(context):
    """Удаляет битвы старше 10 минут. Вызывается автоматически каждые 5 мин."""
    cutoff = time.time() - 600
    stale  = [bid for bid, b in pending_battles.items() if b.get("created_at", 0) < cutoff]
    for bid in stale:
        del pending_battles[bid]
    if stale:
        print(f"🧹 Удалено устаревших битв: {len(stale)}")


# ═══════════════════════════════════════════════
# ЗАПУСК
# ═══════════════════════════════════════════════

def main():
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    if not BOT_TOKEN:
        raise ValueError("❌ Не задана переменная окружения BOT_TOKEN.")

    app = Application.builder().token(BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("test", test_command),
            CallbackQueryHandler(level_selected,        pattern="^level_"),
            CallbackQueryHandler(start_battle_questions, pattern="^start_battle_"),
            CallbackQueryHandler(retry_errors,           pattern="^retry_errors_"),
            CallbackQueryHandler(challenge_start,         pattern="^challenge_start_"),
        ],
        states={
            CHOOSING_LEVEL:  [CallbackQueryHandler(level_selected)],
            ANSWERING:       [MessageHandler(filters.TEXT & ~filters.COMMAND, challenge_answer)],
            BATTLE_ANSWERING: [MessageHandler(filters.TEXT & ~filters.COMMAND, battle_answer)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(back_to_main, pattern="^back_to_main$"),
        ],
        allow_reentry=True,
    )

    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("start", start))

    # Битвы
    app.add_handler(CallbackQueryHandler(create_battle,  pattern="^create_battle$"))
    app.add_handler(CallbackQueryHandler(join_battle,    pattern="^join_battle_"))
    app.add_handler(CallbackQueryHandler(cancel_battle,  pattern="^cancel_battle_"))

    # Общие
    app.add_handler(CallbackQueryHandler(chapter_1_menu,   pattern="^chapter_1_menu$"))
    app.add_handler(CallbackQueryHandler(historical_menu,   pattern="^historical_menu$"))
    app.add_handler(CallbackQueryHandler(
        button_handler,
        pattern=r"^(about|start_test|battle_menu|leaderboard|my_stats|leaderboard_page_\d+|historical_menu|coming_soon|challenge_menu|achievements)$",
    ))
    app.add_handler(CallbackQueryHandler(back_to_main, pattern="^back_to_main$"))
    app.add_handler(CallbackQueryHandler(category_leaderboard_handler, pattern="^cat_lb_"))
    app.add_handler(CallbackQueryHandler(challenge_rules,   pattern="^challenge_rules_"))
    app.add_handler(CallbackQueryHandler(show_weekly_leaderboard, pattern="^weekly_lb_"))
    # challenge_start — через ConversationHandler entry_points ниже

    # JobQueue — подключаем только если доступен (требует pip install python-telegram-bot[job-queue])
    if app.job_queue is not None:
        app.job_queue.run_repeating(cleanup_old_battles, interval=300, first=300)
        print("🧹 Автоочистка битв активна (JobQueue)")
    else:
        print("⚠️  JobQueue недоступен — очистка битв встроена в show_battle_menu")

    print("🤖 Бот запущен!")
    print("📚 Вопросы — 1 Петра (Введение + Глава 1, ст. 1–25)")
    print("⚔️ Режим битвы включён")
    print("🔁 Режим повторения ошибок включён")
    print("📊 Статистика сохраняется в MongoDB")

    app.run_polling()


if __name__ == "__main__":
    main()

