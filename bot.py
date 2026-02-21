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
)
from questions import (
    easy_questions, easy_questions_v17_25,
    medium_questions, medium_questions_v17_25,
    hard_questions, hard_questions_v17_25,
    nero_questions, geography_questions,
    practical_ch1_questions, practical_v17_25_questions,
    linguistics_ch1_questions, linguistics_ch1_questions_2,
    linguistics_v17_25_questions, all_chapter1_questions,
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
        "name":  "🔬 Лингвистика ч.1 (ст. 1–16)",
        "key":   "linguistics_ch1",
        "points_per_q": 3,
    },
    "level_linguistics_ch1_2": {
        "pool":  linguistics_ch1_questions_2,
        "name":  "🔬 Лингвистика ч.2 (ст. 1–16)",
        "key":   "linguistics_ch1_2",
        "points_per_q": 3,
    },
    "level_linguistics_ch1_3": {
        "pool":  linguistics_v17_25_questions,
        "name":  "🔬 Лингвистика ч.3 (ст. 17–25)",
        "key":   "linguistics_ch1_3",
        "points_per_q": 3,
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
        [InlineKeyboardButton("📖 О боте",           callback_data="about")],
        [InlineKeyboardButton("🎯 Начать тест",      callback_data="start_test")],
        [InlineKeyboardButton("⚔️ Режим битвы",       callback_data="battle_menu")],
        [InlineKeyboardButton("🏆 Таблица лидеров",  callback_data="leaderboard")],
        [InlineKeyboardButton("📊 Моя статистика",   callback_data="my_stats")],
    ])


async def start(update: Update, context):
    user = update.effective_user
    init_user_stats(user.id, user.username, user.first_name)
    await update.message.reply_text(
        "📖 *БИБЛЕЙСКИЙ ТЕСТ-БОТ*\n\n"
        "*Тема:* 1 Петра — Глава 1 (ст. 1–25)\n\n"
        "📚 *Категории тестов:*\n"
        "🟢 Основы • 🟡 Контекст • 🔴 Богословие\n"
        "🙏 Применение • 🔬 Лингвистика (3 части)\n"
        "👑 Правление Нерона • 🌍 География\n\n"
        "⚔️ *Новый режим:* Битва с другими игроками!\n\n"
        "Выбери действие:",
        reply_markup=_main_keyboard(),
        parse_mode="Markdown",
    )


async def back_to_main(update: Update, context):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "📖 *БИБЛЕЙСКИЙ ТЕСТ-БОТ*\n\nВыбери действие:",
        reply_markup=_main_keyboard(),
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════
# ВЫБОР УРОВНЯ
# ═══════════════════════════════════════════════

async def choose_level(update, context, is_callback=False):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 1 Петра — Глава 1",          callback_data="chapter_1_menu")],
        [InlineKeyboardButton("👑 Правление Нерона (2 балла)", callback_data="level_nero")],
        [InlineKeyboardButton("🌍 География земли (2 балла)",  callback_data="level_geography")],
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
        [InlineKeyboardButton("🟢 Основы (1 балл)",                      callback_data="level_easy")],
        [InlineKeyboardButton("🟡 Контекст (2 балла)",                   callback_data="level_medium")],
        [InlineKeyboardButton("🔴 Богословие (3 балла)",                 callback_data="level_hard")],
        [InlineKeyboardButton("🙏 Применение (2 балла)",                  callback_data="level_practical_ch1")],
        [InlineKeyboardButton("🔬 Лингвистический разбор — ч.1 (3 балла)", callback_data="level_linguistics_ch1")],
        [InlineKeyboardButton("🔬 Лингвистический разбор — ч.2 (3 балла)", callback_data="level_linguistics_ch1_2")],
        [InlineKeyboardButton("🔬 Лингвистический разбор — ч.3 (3 балла)", callback_data="level_linguistics_ch1_3")],
        [InlineKeyboardButton("👑 История: Нерон (2 балла)",              callback_data="level_nero")],
        [InlineKeyboardButton("🌍 История: География (2 балла)",          callback_data="level_geography")],
        [InlineKeyboardButton("⬅️ Назад",                                  callback_data="start_test")],
    ])
    await query.edit_message_text(
        "📖 *1 ПЕТРА — ГЛАВА 1 (ст. 1–25)*\n\n"
        "🟢 *Основы* — факты, даты, адресаты\n"
        "🟡 *Контекст* — исторический фон, символы\n"
        "🔴 *Богословие* — греческий, доктрины, Троица\n"
        "🙏 *Применение* — практические вопросы\n"
        "🔬 *Лингвистика ч.1* — πρόγνωσις, παρεπίδημος, φρουρέω...\n"
        "🔬 *Лингвистика ч.2* — ἁγιασμός, ζῶσα ἐλπίς, λόγος...\n"
        "🔬 *Лингвистика ч.3* — λυτρόω, ἀναστρέφω, ῥῆμα...\n"
        "👑 *Нерон* — правление и гонения\n"
        "🌍 *География* — провинции и города\n\n"
        "⏱ 7 секунд на вопрос!",
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

    # Отменяем предыдущий таймер
    old_task = data.get("timer_task")
    if old_task and not old_task.done():
        old_task.cancel()

    await message.reply_text(
        f"*Вопрос {q_num + 1}/{total}*  ⏱ 7 сек\n\n{q['question']}",
        reply_markup=ReplyKeyboardMarkup(
            [[opt] for opt in shuffled],
            one_time_keyboard=True, resize_keyboard=True,
        ),
        parse_mode="Markdown",
    )

    data["timer_task"] = asyncio.create_task(auto_timeout(message, user_id, q_num))


async def auto_timeout(message, user_id, q_num_at_send):
    await asyncio.sleep(7)

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
            f"⏱ *Время вышло!*\n✅ Правильный ответ: *{correct_text}*",
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

    sent_at = data.get("question_sent_at", time.time())
    if time.time() - sent_at > 7:
        correct_text = data.get("current_correct_text") or q["options"][q["correct"]]
        data["answered_questions"].append({"question_obj": q, "user_answer": "⏱ Время вышло"})
        await update.message.reply_text(
            f"⏱ *Время вышло!*\n✅ Правильный ответ: {correct_text}",
            reply_markup=ReplyKeyboardRemove(),
            parse_mode="Markdown",
        )
        data["current_question"] += 1
        if data["current_question"] < len(data["questions"]):
            await send_question(update.message, user_id)
            return ANSWERING
        else:
            await show_results(update.message, user_id)
            return ConversationHandler.END

    correct_text    = data.get("current_correct_text") or q["options"][q["correct"]]
    current_options = data.get("current_options") or q["options"]

    if user_answer not in current_options:
        await update.message.reply_text("Выбери один из вариантов")
        return ANSWERING

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

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Ещё раз",     callback_data="start_test")],
        [InlineKeyboardButton("⚔️ Битва",        callback_data="battle_menu")],
        [InlineKeyboardButton("📊 Статистика",   callback_data="my_stats")],
        [InlineKeyboardButton("⬅️ Меню",         callback_data="back_to_main")],
    ])

    await message.reply_text(result_text, reply_markup=keyboard, parse_mode="Markdown")

    # Разбор ошибок
    answered     = data.get("answered_questions", [])
    wrong = [
        item for item in answered
        if item["user_answer"] != item["question_obj"]["options"][item["question_obj"]["correct"]]
    ]

    if wrong:
        await message.reply_text(
            f"❌ *РАЗБОР ОШИБОК ({len(wrong)} из {len(answered)}):*",
            parse_mode="Markdown",
        )
        for i, item in enumerate(wrong, 1):
            q            = item["question_obj"]
            user_ans     = item["user_answer"]
            correct_text = q["options"][q["correct"]]

            breakdown = f"❌ *Ошибка {i}*\n_{q['question']}_\n\n"
            breakdown += f"Ваш ответ: *{'⏱ Время вышло' if user_ans == '⏱ Время вышло' else user_ans}*\n"
            breakdown += f"Правильно: *{correct_text}*\n\n"

            if "options_explanations" in q:
                breakdown += "*Разбор вариантов:*\n"
                for j, opt in enumerate(q["options"]):
                    breakdown += f"• _{opt}_\n{q['options_explanations'][j]}\n\n"

            breakdown += f"💡 *Пояснение:*\n{q['explanation']}"

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
# КНОПКИ
# ═══════════════════════════════════════════════

async def button_handler(update: Update, context):
    query = update.callback_query
    await query.answer()

    if query.data.startswith("leaderboard_page_"):
        page = int(query.data.split("_")[2])
        await show_general_leaderboard(query, page)
        return

    if query.data == "about":
        await query.edit_message_text(
            "📚 *О БОТЕ*\n\n"
            "Этот бот поможет проверить знания по Первому посланию Петра.\n\n"
            "*📋 КАТЕГОРИИ ТЕСТОВ:*\n"
            "🟢 Основы (1:1–25) — 1 балл\n"
            "🟡 Контекст (1:1–25) — 2 балла\n"
            "🔴 Богословие (1:1–25) — 3 балла\n"
            "🙏 Применение (1:1–25) — 2 балла\n"
            "🔬 Лингвистика ч.1 — 3 балла\n"
            "🔬 Лингвистика ч.2 — 3 балла\n"
            "🔬 Лингвистика ч.3 (ст. 17–25) — 3 балла\n"
            "👑 Нерон — 2 балла\n"
            "🌍 География — 2 балла\n\n"
            "*⚔️ РЕЖИМ БИТВЫ:*\n"
            "• Создай битву или присоединись\n"
            "• Отвечай на те же вопросы\n"
            "• Победитель получает +5 баллов!\n\n"
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


# ═══════════════════════════════════════════════
# РЕЖИМ БИТВЫ
# ═══════════════════════════════════════════════

async def show_battle_menu(query):
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
            [InlineKeyboardButton("🎯 Начать тест", callback_data="start_test")],
            [InlineKeyboardButton("⚔️ Битва",        callback_data="battle_menu")],
            [InlineKeyboardButton("⬅️ Назад",         callback_data="back_to_main")],
        ]),
        parse_mode="Markdown",
    )


async def show_general_leaderboard(query, page=0):
    users       = get_leaderboard_page(page)
    total_users = get_total_users()

    if not users:
        text = "🏆 *ТАБЛИЦА ЛИДЕРОВ*\n\nПока никто не проходил тесты.\nБудь первым! 🚀"
    else:
        text       = f"🏆 *ТАБЛИЦА ЛИДЕРОВ* (Стр. {page + 1})\n\n"
        start_rank = page * 10 + 1
        for i, entry in enumerate(users, start_rank):
            medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, "")
            name  = entry.get("first_name", "Unknown")[:15]
            text += f"{medal} *{i}.* {name}\n"
            text += f"   💎 {entry.get('total_points',0)} • 🎯 {entry.get('total_tests',0)} тестов • ⚔️ {entry.get('battles_won',0)} побед\n\n"

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"leaderboard_page_{page-1}"))
    if (page + 1) * 10 < total_users:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"leaderboard_page_{page+1}"))

    keyboard = []
    if nav:
        keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("🎯 Начать тест", callback_data="start_test")])
    keyboard.append([InlineKeyboardButton("⬅️ В меню",      callback_data="back_to_main")])

    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


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
        ],
        states={
            CHOOSING_LEVEL:  [CallbackQueryHandler(level_selected)],
            ANSWERING:       [MessageHandler(filters.TEXT & ~filters.COMMAND, answer)],
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
    app.add_handler(CallbackQueryHandler(chapter_1_menu, pattern="^chapter_1_menu$"))
    app.add_handler(CallbackQueryHandler(
        button_handler,
        pattern=r"^(about|start_test|battle_menu|leaderboard|my_stats|leaderboard_page_\d+)$",
    ))
    app.add_handler(CallbackQueryHandler(back_to_main, pattern="^back_to_main$"))

    # ✅ Автоматическая очистка устаревших битв каждые 5 минут
    app.job_queue.run_repeating(cleanup_old_battles, interval=300, first=300)

    print("🤖 Бот запущен!")
    print("📚 190 вопросов — 1 Петра, глава 1 (ст. 1–25)")
    print("⚔️ Режим битвы включён")
    print("⏱ Реальный таймер на вопросы активен")
    print("📊 Статистика сохраняется в MongoDB")
    print("🧹 Автоочистка битв активна (каждые 5 мин)")

    app.run_polling()


if __name__ == "__main__":
    main()
