"""
bot.py — Библейский тест-бот (1 Петра)
v4.0: финальное меню, объединённый выбор режима, мобильный прогресс-бар.
"""
from keep_alive import keep_alive
keep_alive()

import os
import io
import time
import random
import hashlib
import asyncio
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

from config import (
    QUIZ_TIMEOUT, CHALLENGE_TIMEOUT,
    TIMED_MODE_TIMEOUT, SPEED_MODE_TIMEOUT,
    FEEDBACK_DELAY_CORRECT, FEEDBACK_DELAY_WRONG,
    BATTLE_QUESTIONS, CHALLENGE_QUESTIONS, QUIZ_QUESTIONS,
    MAX_BTN_LEN, CALLBACK_DEBOUNCE,
    BAD_INPUT_LIMIT, GC_INTERVAL, GC_STALE_THRESHOLD,
    BATTLE_EXPIRY, BATTLE_CLEANUP_INTERVAL, BROADCAST_SLEEP,
)

from telegram import (
    Update, ReplyKeyboardMarkup, ReplyKeyboardRemove,
    InlineKeyboardButton, InlineKeyboardMarkup,
    InlineQueryResultArticle, InputTextMessageContent,
    InputFile,
)
from telegram.error import NetworkError, TimedOut, RetryAfter, BadRequest, ChatMigrated
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters,
    ConversationHandler, CallbackQueryHandler, InlineQueryHandler,
)

from database import (
    collection,
    init_user_stats, add_to_leaderboard, update_battle_stats, update_daily_streak,
    get_user_position, get_leaderboard_page, get_total_users,
    format_time, calculate_days_playing, calculate_accuracy,
    record_question_stat,
    get_points_to_next_place, get_category_leaderboard, get_context_leaderboard,
    is_bonus_eligible, compute_bonus,
    update_challenge_stats, update_weekly_leaderboard,
    get_weekly_leaderboard, get_user_achievements, get_current_week_id,
    # Session management
    create_quiz_session, get_active_quiz_session, get_quiz_session,
    update_quiz_session, advance_quiz_session, set_question_sent_at,
    finish_quiz_session, cancel_quiz_session, cancel_active_quiz_session,
    is_question_timed_out,
    # Battles in MongoDB
    create_battle_doc, get_battle, update_battle, get_waiting_battles,
    delete_battle, cleanup_stale_battles as db_cleanup_stale_battles,
    # Admin
    get_admin_stats, get_all_user_ids, get_hardest_questions, get_user_stats,
    update_achievement_stats, check_daily_bonus,
    # Reports
    can_submit_report, seconds_until_next_report, insert_report, mark_report_delivered,
    touch_user_activity,
    # History
    get_user_history,
)
from utils import safe_send, safe_edit, safe_truncate, generate_result_image, get_rank_name, create_result_gif
from questions import get_pool_by_key, BATTLE_POOL

# ── Вопросы Введения (для Random20, Hardcore20, Битв) ────────────────────────
try:
    from intro import (
        intro_part1_questions,
        intro_part2_questions,
        intro_part3_questions,
    )
    INTRO_POOL = intro_part1_questions + intro_part2_questions + intro_part3_questions
except ImportError:
    INTRO_POOL = []
    logger.warning("intro.py не найден — вопросы Введения не включены в Random20/Битвы")

# ─────────────────────────────────────────────
# КОНФИГУРАЦИЯ
# ─────────────────────────────────────────────
_admin_raw = os.getenv("ADMIN_USER_ID")
if not _admin_raw:
    raise ValueError("❌ ADMIN_USER_ID не задан в переменных окружения")
ADMIN_USER_ID = int(_admin_raw)

# Состояния диалога
CHOOSING_LEVEL, ANSWERING, BATTLE_ANSWERING = range(3)
REPORT_TYPE, REPORT_TEXT, REPORT_PHOTO, REPORT_CONFIRM = range(10, 14)

# ─────────────────────────────────────────────
# ТИПИЗАЦИЯ СЕССИИ
# ─────────────────────────────────────────────
from typing import Optional

# QuizSession dataclass удалён — сессии хранятся в user_data: dict[int, dict]


# Хранилище активных сессий (в памяти)
user_data: dict = {}
user_locks: dict = {}  # {user_id: asyncio.Lock} для thread-safe обработки ответов

# Пользовательские настройки (в памяти; сбрасываются при рестарте)
# Структура: {user_id: {"typewriter": bool, ...}}
USER_PREFS: dict = {}

def get_pref(user_id: int, key: str, default=True) -> bool:
    """Читает настройку пользователя. По умолчанию все фичи включены."""
    return USER_PREFS.get(user_id, {}).get(key, default)

def set_pref(user_id: int, key: str, value: bool) -> None:
    """Сохраняет настройку пользователя."""
    if user_id not in USER_PREFS:
        USER_PREFS[user_id] = {}
    USER_PREFS[user_id][key] = value

# Счётчик неверных вводов подряд
_bad_input_count: dict = {}
_BAD_INPUT_LIMIT = BAD_INPUT_LIMIT

def stable_question_id(q: dict) -> str:
    """Стабильный детерминированный ID вопроса (не зависит от перезапуска)."""
    text = q.get("question", "")
    return hashlib.md5(text.encode()).hexdigest()[:12]

def get_qid(question_obj: dict) -> str:
    """Генерирует уникальный ID вопроса на основе SHA256."""
    text = question_obj.get("question", "") + "".join(question_obj.get("options", []))
    # Используем SHA256 но оставляем длину 12 символов для обратной совместимости
    return hashlib.sha256(text.encode('utf-8')).hexdigest()[:12]

def _create_session_data(
    user_id: int,
    session_id: str,
    questions: list,
    level_name: str,
    chat_id: int,
    **extra_fields
) -> dict:
    """
    Фабрика для создания единообразной структуры user_data[user_id].
    Использовать везде где инициализируется новая сессия.
    """
    base_data = {
        "session_id": session_id,
        "questions": questions,
        "current_question": 0,
        "answered_questions": [],
        "level_name": level_name,
        "quiz_chat_id": chat_id,
        "quiz_message_id": None,
        "processing_answer": False,
        "timer_task": None,
        "countdown_task": None,   # живой визуальный таймер (отдельная задача)
        "question_sent_at": None,
        "current_streak": 0,   # Текущая серия правильных ответов подряд
        "max_streak": 0,       # Максимальная серия за тест
    }
    base_data.update(extra_fields)
    return base_data

REPORT_TYPE_LABELS = {
    "bug":      "🐞 Баг",
    "idea":     "💡 Идея",
    "question": "❓ Вопрос по материалу",
}
report_drafts: dict = {}
# _report_last_sent и REPORT_COOLDOWN_SECONDS управляются в database.py

import re

def sanitize_report_text(text: str) -> str:
    """Убираем Markdown-инъекции и ограничиваем длину."""
    text = text[:2000]
    # Экранируем спецсимволы Markdown
    text = re.sub(r'([*_`\[\]])', r'\\\1', text)
    return text.strip()

# ─────────────────────────────────────────────
# DEBOUNCE ДЛЯ CALLBACK-КНОПОК
# ─────────────────────────────────────────────
_last_callback: dict[int, float] = {}
_CALLBACK_DEBOUNCE = CALLBACK_DEBOUNCE  # секунд

async def _debounce_callback(update: Update) -> bool:
    """
    Возвращает True, если запрос нужно игнорировать (слишком быстрое повторное нажатие).
    Вызывай в начале каждого callback-обработчика.
    """
    user_id = update.callback_query.from_user.id
    now = time.time()
    if now - _last_callback.get(user_id, 0) < _CALLBACK_DEBOUNCE:
        await update.callback_query.answer()
        return True
    _last_callback[user_id] = now
    return False

_STUCK_KB = InlineKeyboardMarkup([
    [InlineKeyboardButton("🆘 Сброс",    callback_data="reset_session"),
     InlineKeyboardButton("🐞 Сообщить", callback_data="report_start_bug_direct")],
    [InlineKeyboardButton("⬅️ Меню",     callback_data="back_to_main")],
])

# ─────────────────────────────────────────────
# КОНФИГУРАЦИЯ УРОВНЕЙ
# pool_key → get_pool_by_key(pool_key) возвращает список вопросов
# ─────────────────────────────────────────────
LEVEL_CONFIG = {
    # ── Легкий ──────────────────────────────────────────────────────────────
    "level_easy":              {"pool_key": "easy",             "name": "🟢 Легкий уровень (ст. 1–25)",                      "points_per_q": 1, "num_questions": 10},
    "level_easy_p1":           {"pool_key": "easy_p1",          "name": "🟢 Легкий (ст. 1–16)",                              "points_per_q": 1, "num_questions": 10},
    "level_easy_p2":           {"pool_key": "easy_p2",          "name": "🟢 Легкий (ст. 17–25)",                             "points_per_q": 1, "num_questions": 10},
    # ── Средний ─────────────────────────────────────────────────────────────
    "level_medium":            {"pool_key": "medium",           "name": "🟡 Средний (ст. 1–25)",                             "points_per_q": 2, "num_questions": 10},
    "level_medium_p1":         {"pool_key": "medium_p1",        "name": "🟡 Средний (ст. 1–16)",                             "points_per_q": 2, "num_questions": 10},
    "level_medium_p2":         {"pool_key": "medium_p2",        "name": "🟡 Средний (ст. 17–25)",                            "points_per_q": 2, "num_questions": 10},
    # ── Сложный ─────────────────────────────────────────────────────────────
    "level_hard":              {"pool_key": "hard",             "name": "🔴 Сложный (ст. 1–25)",                             "points_per_q": 3, "num_questions": 10},
    "level_hard_p1":           {"pool_key": "hard_p1",          "name": "🔴 Сложный (ст. 1–16)",                             "points_per_q": 3, "num_questions": 10},
    "level_hard_p2":           {"pool_key": "hard_p2",          "name": "🔴 Сложный (ст. 17–25)",                            "points_per_q": 3, "num_questions": 10},
    # ── Применение ──────────────────────────────────────────────────────────
    "level_practical_ch1":     {"pool_key": "practical_ch1",    "name": "🙏 Применение (ст. 1–25)",                          "points_per_q": 2, "num_questions": 10},
    "level_practical_p1":      {"pool_key": "practical_p1",     "name": "🙏 Применение (ст. 1–16)",                          "points_per_q": 2, "num_questions": 10},
    "level_practical_p2":      {"pool_key": "practical_p2",     "name": "🙏 Применение (ст. 17–25)",                         "points_per_q": 2, "num_questions": 10},
    # ── Лингвистика ─────────────────────────────────────────────────────────
    "level_linguistics_ch1":   {"pool_key": "linguistics_ch1",  "name": "🔬 Лингвистика: Избранные и странники (ч.1)",       "points_per_q": 3, "num_questions": 10},
    "level_linguistics_ch1_2": {"pool_key": "linguistics_ch1_2","name": "🔬 Лингвистика: Живая надежда (ч.2)",               "points_per_q": 3, "num_questions": 10},
    "level_linguistics_ch1_3": {"pool_key": "linguistics_ch1_3","name": "🔬 Лингвистика: Искупление и истина (ч.3)",         "points_per_q": 3, "num_questions": 10},
    # ── Исторический контекст ───────────────────────────────────────────────
    "level_nero":              {"pool_key": "nero",             "name": "👑 Правление Нерона",                               "points_per_q": 2, "num_questions": 10},
    "level_geography":         {"pool_key": "geography",        "name": "🌍 География земли",                                "points_per_q": 2, "num_questions": 10},
    "level_intro1":            {"pool_key": "intro1",           "name": "📜 Введение: Авторство ч.1",                        "points_per_q": 2, "num_questions": 10},
    "level_intro2":            {"pool_key": "intro2",           "name": "📜 Введение: Авторство ч.2",                        "points_per_q": 2, "num_questions": 10},
    "level_intro3":            {"pool_key": "intro3",           "name": "📜 Введение: Структура и цель",                     "points_per_q": 2, "num_questions": 10},
}

# ═══════════════════════════════════════════════
# ДОСТИЖЕНИЯ
# ═══════════════════════════════════════════════

ACHIEVEMENTS = {
    # ═══ Первые шаги ═══
    "first_steps": {
        "name": "Первые шаги",
        "icon": "⭐",
        "description": "Пройди свой первый тест",
        "reward": 10,
    },
    # ═══ Перфекционист (уровни) ═══
    "perfectionist_1": {
        "name": "Перфекционист I",
        "icon": "💎",
        "description": "100% в любом тесте",
        "reward": 25,
        "requirement": {"perfect_count": 1},
    },
    "perfectionist_2": {
        "name": "Перфекционист II",
        "icon": "💎💎",
        "description": "100% в 5 тестах",
        "reward": 50,
        "requirement": {"perfect_count": 5},
    },
    "perfectionist_3": {
        "name": "Перфекционист III",
        "icon": "💎💎💎",
        "description": "100% в 15 тестах",
        "reward": 100,
        "requirement": {"perfect_count": 15},
    },
    # ═══ Огненная серия (уровни) ═══
    "streak_5": {
        "name": "Огненная серия",
        "icon": "🔥",
        "description": "5 правильных подряд",
        "reward": 15,
        "requirement": {"max_streak": 5},
    },
    "streak_10": {
        "name": "Снайпер",
        "icon": "🎯",
        "description": "10 правильных подряд",
        "reward": 30,
        "requirement": {"max_streak": 10},
    },
    "streak_20": {
        "name": "Легенда",
        "icon": "👑",
        "description": "20 правильных подряд",
        "reward": 75,
        "requirement": {"max_streak": 20},
    },
    # ═══ Марафонец (уровни) ═══
    "marathoner_10": {
        "name": "Бегун",
        "icon": "🏃",
        "description": "Пройди 10 тестов",
        "reward": 20,
        "requirement": {"total_tests": 10},
    },
    "marathoner_50": {
        "name": "Марафонец",
        "icon": "🏅",
        "description": "Пройди 50 тестов",
        "reward": 50,
        "requirement": {"total_tests": 50},
    },
    "marathoner_100": {
        "name": "Ультрамарафонец",
        "icon": "🏆",
        "description": "Пройди 100 тестов",
        "reward": 100,
        "requirement": {"total_tests": 100},
    },
    # ═══ Молния ═══
    "lightning": {
        "name": "Молния",
        "icon": "⚡",
        "description": "Ответь за 3 сек в скоростном режиме",
        "reward": 20,
    },
    # ═══ Ежедневная активность ═══
    "daily_streak_7": {
        "name": "Неделя знаний",
        "icon": "📅",
        "description": "Проходи тесты 7 дней подряд",
        "reward": 30,
        "requirement": {"daily_streak": 7},
    },
    "daily_streak_30": {
        "name": "Месяц мудрости",
        "icon": "📆",
        "description": "Проходи тесты 30 дней подряд",
        "reward": 100,
        "requirement": {"daily_streak": 30},
    },
}

# ─────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ─────────────────────────────────────────────

def _inc_bad_input(user_id: int) -> int:
    _bad_input_count[user_id] = _bad_input_count.get(user_id, 0) + 1
    return _bad_input_count[user_id]

def _reset_bad_input(user_id: int):
    _bad_input_count.pop(user_id, None)

def _touch(user_id: int):
    """Обновляет last_activity в памяти и в БД."""
    if user_id in user_data:
        user_data[user_id]["last_activity"] = time.time()
    touch_user_activity(user_id)

def _main_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 О боте",                callback_data="about")],
        [InlineKeyboardButton("🎯 Начать тест",           callback_data="start_test")],
        [InlineKeyboardButton("🎲 Челлендж (20) — бонус", callback_data="challenge_menu")],
        [InlineKeyboardButton("🏛 Исторический контекст", callback_data="historical_menu")],
        [InlineKeyboardButton("⚔️ Режим битвы",            callback_data="battle_menu")],
        [InlineKeyboardButton("🏆 Таблица лидеров",       callback_data="leaderboard")],
        [InlineKeyboardButton("📊 Моя статистика",        callback_data="my_stats")],
        [InlineKeyboardButton("📌 Мой статус",            callback_data="my_status")],
        [InlineKeyboardButton("✉️ Обратная связь",        callback_data="report_menu")],
        [InlineKeyboardButton("⚙️ Настройки",             callback_data="user_settings")],
    ])


# ═══════════════════════════════════════════════
# СТАРТ
# ═══════════════════════════════════════════════

async def start(update: Update, context):
    user = update.effective_user
    init_user_stats(user.id, user.username, user.first_name)
    _touch(user.id)

    # Удаляем сообщение /start пользователя, чтобы не засорять чат
    try:
        await update.message.delete()
    except Exception:
        pass

    # Убираем ReplyKeyboard, если была — отправляем невидимый пузырь и сразу удаляем
    try:
        stub = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="↩️",
            reply_markup=ReplyKeyboardRemove(),
        )
        await asyncio.sleep(0.3)
        await stub.delete()
    except Exception:
        pass

    active_session = get_active_quiz_session(user.id)
    if active_session:
        questions_data = active_session.get("questions_data", [])
        total_q = len(questions_data)
        current = active_session.get("current_index", 0)
        if current >= total_q:
            cancel_quiz_session(active_session["_id"])
            active_session = None
        else:
            level_name = active_session.get("level_name", "тест")
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=(
                    f"⏸ *Тест прерван на вопросе {current + 1}/{total_q}*\n"
                    f"_{level_name}_\n\nЧто хочешь сделать?"
                ),
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶️ Продолжить", callback_data=f"resume_session_{active_session['_id']}")],
                    [InlineKeyboardButton("🔁 Начать заново", callback_data=f"restart_session_{active_session['_id']}")],
                    [InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_session_{active_session['_id']}")],
                ]),
            )
            return

    # ── Deep link: /start level_easy, /start random20, и т.д. ──────────────────
    if context.args and len(context.args) > 0:
        level_key = context.args[0]
        if level_key in LEVEL_CONFIG:
            cfg = LEVEL_CONFIG[level_key]
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🧘 Без ограничения времени", callback_data=f"relaxed_mode_{level_key}")],
                [InlineKeyboardButton(f"⏱ На время ({TIMED_MODE_TIMEOUT} сек)  ×1.5 баллов", callback_data=f"timed_mode_{level_key}")],
                [InlineKeyboardButton(f"⚡ Скоростной ({SPEED_MODE_TIMEOUT} сек)  ×2 баллов", callback_data=f"speed_mode_{level_key}")],
                [InlineKeyboardButton("↩️ В главное меню", callback_data="back_to_main")],
            ])
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"📚 *{cfg['name']}*\n\nВыбери режим прохождения:",
                reply_markup=keyboard,
                parse_mode="Markdown",
            )
            return
        # Если level_key не распознан — показываем обычное меню

    name = user.first_name or "друг"
    streak = update_daily_streak(user.id)
    _, entry = get_user_position(user.id)

    welcome = (
        f"👋 *Добро пожаловать, {name}!*\n\n"
        "Здесь мы изучаем *1-е послание Петра*.\n\n"
        "📖 *Глава 1* — основной тест\n"
        "🔬 *Лингвистика* — глубокий разбор\n"
        "🏛 *Исторический контекст* — Нерон, география\n"
        "⚔️ *Битвы* — соревнование с другими\n\n"
        "Нажми на кнопку ниже! 👇"
    )
    if streak > 0:
        welcome += f"\n\n🔥 *Серия: {streak} дней подряд!*"
    else:
        welcome += "\n\n💡 _Заходи каждый день для серии!_"
    # Всегда новое сообщение — меню "прыгает" вниз
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=welcome,
        reply_markup=_main_keyboard(),
        parse_mode="Markdown",
    )


async def back_to_main(update: Update, context):
    query = update.callback_query
    await query.answer()
    # Отправляем новое сообщение внизу чата — Telegram автоматически
    # прокручивает экран к нему. Старое сообщение остаётся в истории.
    await context.bot.send_message(
        chat_id=query.message.chat_id,
        text="📖 *БИБЛЕЙСКИЙ ТЕСТ-БОТ*\n\n"
             "📖 Глава 1 • 🔬 Лингвистика • 🏛 Контекст • ⚔️ Битвы\n\n"
             "Выбери действие:",
        reply_markup=_main_keyboard(),
        parse_mode="Markdown",
    )


def _settings_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Строит клавиатуру настроек с текущими состояниями переключателей."""
    tw_on = get_pref(user_id, "typewriter", default=True)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"⌨️ Печатная машинка: {'✅ вкл' if tw_on else '❌ выкл'}",
            callback_data="toggle_typewriter",
        )],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")],
    ])


async def user_settings_handler(update: Update, context):
    """Показывает меню пользовательских настроек."""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    await query.edit_message_text(
        "⚙️ *НАСТРОЙКИ*\n\n"
        "Здесь можно включить или выключить визуальные эффекты.\n"
        "_Настройки сохраняются до перезапуска бота._",
        reply_markup=_settings_keyboard(user_id),
        parse_mode="Markdown",
    )


async def toggle_typewriter_handler(update: Update, context):
    """Переключает эффект печатной машинки."""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    current = get_pref(user_id, "typewriter", default=True)
    set_pref(user_id, "typewriter", not current)
    state = "включена ✅" if not current else "выключена ❌"
    await query.edit_message_text(
        f"⚙️ *НАСТРОЙКИ*\n\n"
        f"⌨️ Печатная машинка {state}\n\n"
        "_Настройки сохраняются до перезапуска бота._",
        reply_markup=_settings_keyboard(user_id),
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════
# МЕНЮ УРОВНЕЙ
# ═══════════════════════════════════════════════

async def choose_level(update, context, is_callback=False):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🏛 Исторический контекст", callback_data="historical_menu")],
        [InlineKeyboardButton("📖 1 Петра — Глава 1",     callback_data="chapter_1_menu")],
        [InlineKeyboardButton("📖 Глава 2 — скоро...",    callback_data="coming_soon")],
        [InlineKeyboardButton("⬅️ Назад",                  callback_data="back_to_main")],
    ])
    text = "🎯 *ВЫБЕРИ КАТЕГОРИЮ*\n\n📖 *1 Петра по главам:*\nГлава 1 — 5 видов вопросов\n\nВыбери уровень и режим прохождения 👇"
    if is_callback and hasattr(update, "callback_query"):
        await update.callback_query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")
    else:
        await update.message.reply_text(text, reply_markup=keyboard, parse_mode="Markdown")


async def chapter_1_menu(update: Update, context):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🟢 Легкий (1–16)",   callback_data="level_easy_p1"),
            InlineKeyboardButton("🟢 Легкий (17–25)",  callback_data="level_easy_p2"),
        ],
        [
            InlineKeyboardButton("🟡 Средний (1–16)",  callback_data="level_medium_p1"),
            InlineKeyboardButton("🟡 Средний (17–25)", callback_data="level_medium_p2"),
        ],
        [
            InlineKeyboardButton("🔴 Сложный (1–16)",  callback_data="level_hard_p1"),
            InlineKeyboardButton("🔴 Сложный (17–25)", callback_data="level_hard_p2"),
        ],
        [
            InlineKeyboardButton("🙏 Примен. (1–16)",  callback_data="level_practical_p1"),
            InlineKeyboardButton("🙏 Примен. (17–25)", callback_data="level_practical_p2"),
        ],
        [
            InlineKeyboardButton("🔬 Лингвистика ч.1", callback_data="level_linguistics_ch1"),
            InlineKeyboardButton("🔬 Лингвистика ч.2", callback_data="level_linguistics_ch1_2"),
        ],
        [InlineKeyboardButton("🔬 Лингвистика ч.3",    callback_data="level_linguistics_ch1_3")],
        [InlineKeyboardButton("⬅️ Назад",               callback_data="start_test")],
    ])
    await query.edit_message_text(
        "📖 *1 ПЕТРА — ГЛАВА 1 (ст. 1–25)*\n\n"
        "🟢 Легкий (1 балл) • 🟡 Средний (2 балла) • 🔴 Сложный (3 балла)\n"
        "🙏 Применение (2 балла) • 🔬 Лингвистика (3 балла)",
        reply_markup=keyboard, parse_mode="Markdown",
    )


async def historical_menu(update: Update, context):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📜 Введение: Авторство ч.1 (2 балла)",    callback_data="level_intro1")],
        [InlineKeyboardButton("📜 Введение: Авторство ч.2 (2 балла)",    callback_data="level_intro2")],
        [InlineKeyboardButton("📜 Введение: Структура и цель (2 балла)", callback_data="level_intro3")],
        [InlineKeyboardButton("👑 Правление Нерона (2 балла)",           callback_data="level_nero")],
        [InlineKeyboardButton("🌍 География земли (2 балла)",            callback_data="level_geography")],
        [InlineKeyboardButton("🎲 Случайный факт",                        callback_data="random_fact_intro")],
        [InlineKeyboardButton("⬅️ Назад",                                 callback_data="back_to_main")],
    ])
    await query.edit_message_text(
        "🏛 *ИСТОРИЧЕСКИЙ КОНТЕКСТ*\n\n"
        "📜 Введение — баллы засчитываются в общий рейтинг!\n"
        "💡 Перед тестами Введения можно нажать кнопку и получить *справку*.",
        reply_markup=keyboard, parse_mode="Markdown",
    )


# ─────────────────────────────────────────────
# СПРАВКА ДЛЯ ТЕСТОВ ВВЕДЕНИЯ
# ─────────────────────────────────────────────

def _get_intro_pool(level_callback: str):
    """Возвращает пул вопросов по callback-имени уровня Введение."""
    cfg = LEVEL_CONFIG.get(level_callback)
    return get_pool_by_key(cfg["pool_key"]) if cfg else []


async def intro_hint_handler(update: Update, context):
    """Показывает 3 случайных факта из пула вопросов выбранного теста Введения."""
    query = update.callback_query
    await query.answer()
    level_cb = query.data.replace("intro_hint_", "")  # e.g. "level_intro1"
    pool = _get_intro_pool(level_cb)
    cfg = LEVEL_CONFIG.get(level_cb, {})
    level_name = cfg.get("name", "Введение")

    facts = []
    sample = random.sample(pool, min(3, len(pool))) if pool else []
    for q in sample:
        facts.append(f"💡 _{q['explanation']}_")

    hint_text = f"📖 *Справка: {level_name}*\n\n" + "\n\n".join(facts) if facts else "Нет данных."

    await query.edit_message_text(
        hint_text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Начать тест", callback_data=f"intro_start_{level_cb}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="historical_menu")],
        ]),
    )


async def intro_start_handler(update: Update, context):
    """Запускает тест Введения напрямую (минуя экран справки)."""
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    _touch(user_id)

    level_cb = query.data.replace("intro_start_", "")  # e.g. "level_intro1"
    cfg = LEVEL_CONFIG.get(level_cb)
    if not cfg:
        await query.edit_message_text("⚠️ Уровень не найден.")
        return

    questions = random.sample(get_pool_by_key(cfg["pool_key"]), min(cfg.get("num_questions", 10), len(get_pool_by_key(cfg["pool_key"]))))
    cancel_active_quiz_session(user_id)

    question_ids = [get_qid(q) for q in questions]
    session_id = create_quiz_session(
        user_id=user_id, mode="level", question_ids=question_ids,
        questions_data=questions, level_key=cfg["pool_key"],
        level_name=cfg["name"], time_limit=None,
        chat_id=query.message.chat_id,
    )

    user_data[user_id] = _create_session_data(
        user_id=user_id,
        session_id=session_id,
        questions=questions,
        level_name=cfg["name"],
        chat_id=query.message.chat_id,
        level_key=cfg["pool_key"],
        correct_answers=0,
        start_time=time.time(),
        last_activity=time.time(),
        is_battle=False,
        battle_points=0,
        username=update.effective_user.username,
        first_name=update.effective_user.first_name,
    )

    await query.edit_message_text(
        f"*{cfg['name']}*\n\n📝 Вопросов: {len(questions)} • 💎 2 балла за ответ\nНачинаем! ⏱",
        parse_mode="Markdown",
    )
    await send_question(context.bot, user_id)


async def random_fact_handler(update: Update, context):
    """Отправляет один случайный факт из всех вопросов категории Введение."""
    query = update.callback_query
    await query.answer()

    all_intro = (get_pool_by_key("intro1") + get_pool_by_key("intro2") + get_pool_by_key("intro3"))
    q = random.choice(all_intro)
    fact = q["explanation"]

    await query.edit_message_text(
        f"🎲 *А вы знали?*\n\n_{fact}_",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎲 Ещё факт",  callback_data="random_fact_intro")],
            [InlineKeyboardButton("⬅️ Назад",      callback_data="historical_menu")],
        ]),
    )


# ═══════════════════════════════════════════════
# ВЫБОР УРОВНЯ → СТАРТ СЕССИИ
# ═══════════════════════════════════════════════

async def _show_level_mode_selection(query, level_key: str):
    """Показывает объединённый экран выбора режима для уровня (один шаг вместо двух)."""
    cfg = LEVEL_CONFIG.get(level_key)
    if not cfg:
        return

    pool_size = len(get_pool_by_key(cfg["pool_key"]))
    num_q = min(cfg.get("num_questions", 10), pool_size)
    ppq = cfg.get("points_per_q", 1)

    text = (
        f"📝 *{cfg['name']}*\n\n"
        f"📋 Вопросов: {num_q}\n"
        f"⭐ Баллов за ответ: {ppq}\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"*Выбери режим:*\n\n"
        f"🧘 *Спокойный* — без ограничения времени\n"
        f"   _Баллы: ×1.0_\n\n"
        f"⏱ *На время* — {TIMED_MODE_TIMEOUT} сек на вопрос\n"
        f"   _Баллы: ×1.5_\n\n"
        f"⚡ *Скоростной* — {SPEED_MODE_TIMEOUT} сек на вопрос\n"
        f"   _Баллы: ×2.0_"
    )

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🧘 Спокойный",                callback_data=f"relaxed_mode_{level_key}"),
            InlineKeyboardButton(f"⏱ {TIMED_MODE_TIMEOUT} сек", callback_data=f"timed_mode_{level_key}"),
            InlineKeyboardButton(f"⚡ {SPEED_MODE_TIMEOUT} сек", callback_data=f"speed_mode_{level_key}"),
        ],
        [InlineKeyboardButton("⬅️ Назад", callback_data="start_test")],
    ])

    await query.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)


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
    _touch(user_id)

    # Для тестов «Введение» предлагаем справку перед стартом
    if cfg["pool_key"] in ("intro1", "intro2", "intro3"):
        await query.edit_message_text(
            f"📜 *{cfg['name']}*\n\n"
            "Это вопросы по введению к 1 Петра: авторство, датировка, структура.\n\n"
            "Хочешь получить краткую *💡 справку* перед тестом?",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💡 Справка (3 факта)", callback_data=f"intro_hint_{query.data}")],
                [InlineKeyboardButton("▶️ Начать без справки", callback_data=f"intro_start_{query.data}")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="historical_menu")],
            ]),
        )
        return ConversationHandler.END

    # Сразу показываем объединённый экран выбора режима
    await _show_level_mode_selection(query, query.data)
    return ConversationHandler.END


async def confirm_level_handler(update: Update, context):
    """Обратная совместимость: confirm_level_ → экран выбора режима."""
    query = update.callback_query
    await query.answer()
    level_key = query.data.replace("confirm_level_", "")
    await _show_level_mode_selection(query, level_key)



async def _launch_level_test(query, update, level_key: str, quiz_mode: str,
                              time_limit: int | None, score_multiplier: float):
    """Общая логика запуска теста после выбора режима."""
    cfg = LEVEL_CONFIG.get(level_key)
    if not cfg:
        return

    user_id = update.effective_user.id
    _touch(user_id)

    # Случайная выборка ТОЛЬКО из пула этого уровня
    pool = get_pool_by_key(cfg["pool_key"])
    num_to_select = min(cfg.get("num_questions", 10), len(pool))
    questions = random.sample(pool, num_to_select)
    cancel_active_quiz_session(user_id)

    question_ids = [get_qid(q) for q in questions]
    session_id = create_quiz_session(
        user_id=user_id, mode="level", question_ids=question_ids,
        questions_data=questions, level_key=cfg["pool_key"],
        level_name=cfg["name"], time_limit=time_limit,
        chat_id=query.message.chat_id,
    )

    chat_id = query.message.chat_id

    user_data[user_id] = _create_session_data(
        user_id=user_id,
        session_id=session_id,
        questions=questions,
        level_name=cfg["name"],
        chat_id=chat_id,
        level_key=cfg["pool_key"],
        correct_answers=0,
        start_time=time.time(),
        last_activity=time.time(),
        is_battle=False,
        battle_points=0,
        username=update.effective_user.username,
        first_name=update.effective_user.first_name,
        quiz_mode=quiz_mode,
        score_multiplier=score_multiplier,
        quiz_time_limit=time_limit,
    )

    mode_label = {"relaxed": "🧘 Без таймера", "timed": f"⏱ {TIMED_MODE_TIMEOUT} сек / ×1.5", "speed": f"⚡ {SPEED_MODE_TIMEOUT} сек / ×2"}.get(quiz_mode, "")
    await query.edit_message_text(
        f"*{cfg['name']}*\n\n📝 Вопросов: {len(questions)} · {mode_label}\nНачинаем!",
        parse_mode="Markdown",
    )
    await send_question(query.message.get_bot(), user_id, time_limit=time_limit)


async def relaxed_mode_handler(update: Update, context):
    """Режим без таймера, обычные баллы ×1."""
    query = update.callback_query
    await query.answer()
    level_key = query.data.replace("relaxed_mode_", "")
    await _launch_level_test(query, update, level_key, "relaxed", None, 1.0)


async def timed_mode_handler(update: Update, context):
    """Режим с таймером TIMED_MODE_TIMEOUT сек, баллы ×1.5."""
    query = update.callback_query
    await query.answer()
    level_key = query.data.replace("timed_mode_", "")
    await _launch_level_test(query, update, level_key, "timed", TIMED_MODE_TIMEOUT, 1.5)


async def speed_mode_handler(update: Update, context):
    """Скоростной режим SPEED_MODE_TIMEOUT сек, баллы ×2."""
    query = update.callback_query
    await query.answer()
    level_key = query.data.replace("speed_mode_", "")
    await _launch_level_test(query, update, level_key, "speed", SPEED_MODE_TIMEOUT, 2.0)


# ═══════════════════════════════════════════════
# ВОПРОСЫ И ОТВЕТЫ
# ═══════════════════════════════════════════════

async def send_question(bot, user_id, time_limit=None):
    """
    Отправляет или редактирует сообщение с вопросом.
    Первый вопрос — новое сообщение; последующие — редактирование того же «пузыря».
    """
    data = user_data[user_id]
    q_num = data["current_question"]
    total = len(data["questions"])

    if q_num >= total:
        await _finalize_quiz_bubble(bot, user_id)
        await show_results(bot, user_id)
        return

    q = data["questions"][q_num]
    correct_text = q["options"][q["correct"]]
    shuffled = q["options"][:]
    random.shuffle(shuffled)

    data["current_options"]      = shuffled
    data["current_correct_text"] = correct_text
    data["processing_answer"]    = False
    sent_at = time.time()
    data["question_sent_at"]     = sent_at

    session_id = data.get("session_id")
    if session_id:
        set_question_sent_at(session_id, sent_at)

    # Всегда: варианты текстом, кнопки с цифрами
    options_text = "\n\n" + "\n".join(f"*{i+1}.* {opt}" for i, opt in enumerate(shuffled))
    buttons = [[InlineKeyboardButton(str(i + 1), callback_data=f"qa_{i}") for i in range(len(shuffled))]]

    buttons.append([
        InlineKeyboardButton("⚠️ Неточность?", callback_data=f"report_inaccuracy_{q_num}"),
        InlineKeyboardButton("↩️ выйти", callback_data="cancel_quiz"),
    ])
    keyboard = InlineKeyboardMarkup(buttons)
    progress = build_progress_bar(q_num + 1, total, data.get("answered_questions", []))
    # Используем time_limit из аргумента или из сохранённых данных сессии
    effective_limit = time_limit if time_limit is not None else data.get("quiz_time_limit")
    timer_str = f" • ⏱ {effective_limit} сек" if effective_limit else ""
    text = f"*Вопрос {q_num + 1}/{total}*{timer_str}\n{progress}\n\n{q['question']}{options_text}"

    quiz_message_id = data.get("quiz_message_id")
    quiz_chat_id    = data.get("quiz_chat_id")

    if not quiz_chat_id:
        logger.error("send_question: quiz_chat_id is None for user %s, cannot send question!", user_id)
        return

    if quiz_message_id and quiz_chat_id:
        # Последующие вопросы — редактируем пузырь мгновенно (typewriter здесь не нужен)
        try:
            await bot.edit_message_text(
                chat_id=quiz_chat_id, message_id=quiz_message_id,
                text=text, reply_markup=keyboard, parse_mode="Markdown",
            )
        except Exception as e:
            err_str = str(e).lower()
            if "not modified" not in err_str:
                # Если редактирование не удалось по другой причине — шлём новым сообщением
                try:
                    msg = await bot.send_message(
                        chat_id=quiz_chat_id, text=text, reply_markup=keyboard, parse_mode="Markdown",
                    )
                    data["quiz_message_id"] = msg.message_id
                    data["quiz_chat_id"]    = msg.chat.id
                except Exception as e2:
                    logger.error("send_question: fallback send_message failed for user %s: %s", user_id, e2)
                    return
    else:
        # Первый вопрос — отправляем сразу без анимации
        try:
            await _typing_pause(bot, quiz_chat_id, has_timer=bool(effective_limit))

            msg = await bot.send_message(
                chat_id=quiz_chat_id, text=text, reply_markup=keyboard, parse_mode="Markdown",
            )
            data["quiz_message_id"] = msg.message_id
            data["quiz_chat_id"]    = msg.chat.id
        except Exception as e:
            logger.error("send_question: send_message failed for user %s: %s", user_id, e)
            return

    # Запускаем таймер если задан time_limit (для timed/speed режимов)
    if effective_limit:
        old_task = data.get("timer_task")
        if old_task and not old_task.done():
            old_task.cancel()
        # Отменяем предыдущий визуальный countdown
        _cancel_countdown(user_id)
        data["timer_task"] = asyncio.create_task(
            _handle_question_timeout(bot, user_id, q_num, effective_limit)
        )
        # Запускаем живой визуальный countdown параллельно
        data["countdown_task"] = asyncio.create_task(
            _live_countdown(bot, quiz_chat_id, effective_limit, user_id, q_num)
        )


async def _finalize_quiz_bubble(bot, user_id, text="✅ *Тест завершён!*"):
    """Финально редактирует «пузырь» вопроса — убирает кнопки."""
    data = user_data.get(user_id, {})
    qmid  = data.get("quiz_message_id")
    qcid  = data.get("quiz_chat_id")
    if qmid and qcid:
        try:
            await bot.edit_message_text(
                chat_id=qcid, message_id=qmid,
                text=text, parse_mode="Markdown",
            )
        except Exception:
            pass


def _cancel_countdown(user_id: int):
    """
    Отменяет живой countdown-таймер пользователя, если он запущен.
    Вызывать при: ответе, отмене теста, таймауте вопроса.
    """
    data = user_data.get(user_id)
    if not data:
        return
    task = data.get("countdown_task")
    if task and not task.done():
        task.cancel()
    data["countdown_task"] = None


async def _live_countdown(bot, chat_id: int, seconds: int, user_id: int, q_num: int):
    """
    Живой визуальный таймер: отдельное сообщение обновляется каждую секунду.

    Жизненный цикл:
    - Отправляет новое сообщение под вопросом
    - Каждую секунду меняет текст: 🟢 → 🟡 (≤10) → 🔴 (≤5)
    - При истечении времени удаляет сообщение
    - При отмене (cancel()) — молча удаляет сообщение
    - Привязан к конкретному q_num: если вопрос уже сменился — останавливается
    """
    msg = None
    try:
        msg = await bot.send_message(chat_id=chat_id, text=f"🟢 {seconds} сек")

        for i in range(seconds, 0, -1):
            await asyncio.sleep(1)

            # Прерываем если вопрос уже сменился или сессия закрыта
            data = user_data.get(user_id)
            if not data or data.get("current_question") != q_num:
                return

            emoji = "🔴" if i <= 5 else "🟡" if i <= 10 else "🟢"
            bar_filled = max(0, round(i / seconds * 8))
            bar = "█" * bar_filled + "░" * (8 - bar_filled)
            try:
                await msg.edit_text(f"{emoji} {i} сек  {bar}")
            except Exception:
                return  # сообщение удалено или недоступно — останавливаемся

    except asyncio.CancelledError:
        pass  # нормальная отмена — просто удаляем сообщение
    except Exception as e:
        logger.debug("_live_countdown error for user %s: %s", user_id, e)
    finally:
        # Всегда удаляем сообщение таймера при выходе
        if msg:
            try:
                await msg.delete()
            except Exception:
                pass


# ═══════════════════════════════════════════════════════════════
# АНИМАЦИИ: печатная машинка, typing-пауза, конфетти
# ═══════════════════════════════════════════════════════════════

async def _typing_pause(bot, chat_id: int, has_timer: bool) -> None:
    """
    Показывает индикатор «печатает…» перед отправкой вопроса.
    При наличии таймера — пропускаем (каждая секунда на счету).
    """
    if has_timer:
        return
    try:
        await bot.send_chat_action(chat_id=chat_id, action="typing")
        await asyncio.sleep(0.8)
    except Exception:
        pass


async def typewriter_send(bot, chat_id: int, text: str,
                           reply_markup=None, delay: float = None) -> object:
    """
    Отправляет сообщение с эффектом печатной машинки.

    Используется ТОЛЬКО для первого вопроса (новое сообщение, не edit).
    Печатаем только текст вопроса — без вариантов ответа и Markdown-разметки,
    чтобы избежать битого парсинга на промежуточных шагах.

    Финальный вызов edit_text передаёт полный text + reply_markup + parse_mode.
    """
    # Шаг 1: отправляем заглушку-курсор
    try:
        msg = await bot.send_message(chat_id=chat_id, text="▌")
    except Exception as e:
        logger.warning("typewriter_send: initial send failed: %s", e)
        # Fallback — обычная отправка
        return await bot.send_message(
            chat_id=chat_id, text=text,
            reply_markup=reply_markup, parse_mode="Markdown",
        )

    # Шаг 2: находим только строку вопроса (до вариантов ответа)
    # text имеет вид "*Вопрос N/M* ... \n\n<вопрос>\n\n1. вариант..."
    # Печатаем только до первого двойного переноса после заголовка
    parts = text.split("\n\n", 2)
    # parts[0] = заголовок, parts[1] = тело вопроса, parts[2] = варианты (если есть)
    header   = parts[0] if len(parts) > 0 else ""
    question = parts[1] if len(parts) > 1 else text

    # Адаптивная задержка: короткий текст — 0.03 сек, длинный — 0.02 сек
    if delay is None:
        _delay = 0.02 if len(question) > 50 else 0.03
    else:
        _delay = delay

    displayed = header + "\n\n"
    for char in question:
        displayed += char
        # Обновляем не при каждом символе — Telegram лимитирует edit до ~20/сек
        # Обновляем каждые 2 символа для плавности без флуда
        if len(displayed) % 2 == 0:
            try:
                await msg.edit_text(displayed + "▌")
            except Exception:
                break
        await asyncio.sleep(_delay)

    # Шаг 3: финальный вариант — полный текст с разметкой и кнопками
    try:
        await msg.edit_text(text, reply_markup=reply_markup, parse_mode="Markdown")
    except Exception as e:
        logger.warning("typewriter_send: final edit failed: %s", e)
        # Если финальный edit упал — шлём заново
        try:
            await msg.delete()
        except Exception:
            pass
        msg = await bot.send_message(
            chat_id=chat_id, text=text,
            reply_markup=reply_markup, parse_mode="Markdown",
        )

    return msg


async def animate_confetti(bot, chat_id: int) -> None:
    """
    Анимация конфетти при идеальном результате (score == total).
    Запускать ДО отправки карточки результатов.
    """
    frames = [
        "       🎊       ",
        "    🎉  🎊  🎉    ",
        " 🎊  🎉  🎊  🎉  🎊 ",
        "🎉🎊🎉🎊🎉🎊🎉🎊🎉",
        " 🎊  🎉  🎊  🎉  🎊 ",
        "    🎉  🎊  🎉    ",
        "       🎊       ",
    ]
    msg = None
    try:
        # Дартс — символ точности и результата
        dice_msg = await bot.send_dice(chat_id=chat_id, emoji="🎯")
        await asyncio.sleep(1.5)
        try:
            await dice_msg.delete()
        except Exception:
            pass

        msg = await bot.send_message(chat_id=chat_id, text=frames[0])
        for frame in frames[1:]:
            await asyncio.sleep(0.18)
            try:
                await msg.edit_text(frame)
            except Exception:
                break

        await asyncio.sleep(0.18)
        try:
            await msg.edit_text("🎉 *ИДЕАЛЬНЫЙ РЕЗУЛЬТАТ!* 🎉", parse_mode="Markdown")
        except Exception:
            pass

        await asyncio.sleep(2.0)
    except Exception as e:
        logger.debug("animate_confetti error: %s", e)
    finally:
        if msg:
            try:
                await msg.delete()
            except Exception:
                pass


async def report_inaccuracy_handler(update: Update, context):
    """
    Обрабатывает нажатие «⚠️ Неточность?» во время теста.
    Автоматически определяет текущий вопрос и отправляет сообщение админу в директ.
    """
    query = update.callback_query
    await query.answer("✅ Сообщение отправлено автору. Спасибо!", show_alert=False)

    user = update.effective_user
    user_id = user.id
    data = user_data.get(user_id, {})

    q_num = data.get("current_question", 0)
    q_list = data.get("questions", [])
    level_name = data.get("level_name", "—")
    username = f"@{user.username}" if user.username else f"{user.first_name} (id: {user_id})"

    if q_list and q_num < len(q_list):
        q = q_list[q_num]
        q_text = q.get("question", "—")
        options = q.get("options", [])
        correct_idx = q.get("correct", 0)
        correct_ans = options[correct_idx] if options else "—"
        options_str = "\n".join(f"  {i+1}. {opt}" for i, opt in enumerate(options))
        msg = (
            f"⚠️ *СООБЩЕНИЕ О НЕТОЧНОСТИ*\n\n"
            f"👤 От: {username}\n"
            f"📚 Тест: _{level_name}_\n"
            f"❓ Вопрос {q_num + 1}: _{q_text}_\n\n"
            f"📋 Варианты:\n{options_str}\n\n"
            f"✅ Правильный ответ в базе: _{correct_ans}_"
        )
    else:
        msg = (
            f"⚠️ *СООБЩЕНИЕ О НЕТОЧНОСТИ*\n\n"
            f"👤 От: {username}\n"
            f"📚 Тест: _{level_name}_\n"
            f"❓ Вопрос: {q_num + 1} (детали недоступны)"
        )

    try:
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=msg,
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.warning("report_inaccuracy: не удалось отправить сообщение админу: %s", e)


async def _handle_question_timeout(bot, user_id: int, q_num_at_send: int, timeout_seconds: int):
    """
    Универсальный таймаут — одна реализация для обычного теста и challenge.
    Вызывается из auto_timeout / challenge_timeout.
    """
    await asyncio.sleep(timeout_seconds)

    if user_id not in user_data:
        return
    data = user_data[user_id]

    if (data.get("processing_answer")
            or data.get("current_question") != q_num_at_send
            or data.get("session_id") is None):
        return

    data["processing_answer"] = True
    # Countdown больше не нужен — время вышло, удаляем визуальный таймер
    _cancel_countdown(user_id)
    try:
        q            = data["questions"][q_num_at_send]
        correct_text = data.get("current_correct_text") or q["options"][q["correct"]]
        q_id         = get_qid(q)

        session_id = data.get("session_id")
        if session_id:
            advance_quiz_session(session_id, q_id, "⏱ Время вышло", False, q)

        data["answered_questions"].append({"question_obj": q, "user_answer": "⏱ Время вышло"})
        data["current_question"] += 1

        qmid, qcid = data.get("quiz_message_id"), data.get("quiz_chat_id")
        timeout_text = f"⏱ *Время вышло ({timeout_seconds} сек)*\n✅ Правильный ответ: *{correct_text}*"
        if qmid and qcid:
            try:
                await bot.edit_message_text(
                    chat_id=qcid, message_id=qmid,
                    text=timeout_text,
                    parse_mode="Markdown",
                )
            except Exception:
                # edit не удался — шлём новым сообщением
                try:
                    await bot.send_message(chat_id=qcid, text=timeout_text, parse_mode="Markdown")
                except Exception:
                    pass
        elif qcid:
            try:
                await bot.send_message(chat_id=qcid, text=timeout_text, parse_mode="Markdown")
            except Exception:
                pass

        # Сбрасываем quiz_message_id, чтобы следующий send_question
        # создал свежее сообщение с кнопками, а не редактировал пузырь таймаута
        data["quiz_message_id"] = None

        # Пауза всегда — вне зависимости от наличия qmid
        await asyncio.sleep(FEEDBACK_DELAY_WRONG)

        # Перечитываем актуальное состояние (на случай гонки)
        if user_id not in user_data:
            return
        data = user_data[user_id]

        is_challenge = data.get("is_challenge", False)
        try:
            if data["current_question"] < len(data["questions"]):
                if is_challenge:
                    await send_challenge_question(bot, user_id)
                else:
                    await send_question(bot, user_id)
            else:
                # Тест завершён по таймауту — показываем результаты
                await _finalize_quiz_bubble(bot, user_id)
                if is_challenge:
                    await show_challenge_results(bot, user_id)
                else:
                    await show_results(bot, user_id)
        except Exception:
            logger.error("_handle_question_timeout: error sending next question for user %s", user_id, exc_info=True)
    finally:
        if user_id in user_data:
            user_data[user_id]["processing_answer"] = False


async def auto_timeout(bot, user_id, q_num_at_send):
    """Страховочный таймер для обычного теста."""
    await _handle_question_timeout(bot, user_id, q_num_at_send, QUIZ_TIMEOUT)


async def answer(update: Update, context):
    user_id = update.effective_user.id
    _touch(user_id)

    if user_id not in user_data:
        db_session = get_active_quiz_session(user_id)
        if db_session and db_session.get("mode") == "level":
            await _restore_session_to_memory(user_id, db_session)
        else:
            await update.message.reply_text("Используй /test чтобы начать")
            return ConversationHandler.END

    data = user_data[user_id]

    if data.get("is_battle"):
        return await battle_answer(update, context)

    # Для обычного теста ответы теперь через Inline-кнопки (quiz_inline_answer).
    # Этот обработчик остаётся для совместимости (например, вбит текст вручную).
    if data.get("processing_answer"):
        return ANSWERING

    await update.message.reply_text(
        "👆 Используй кнопки под вопросом для ответа.",
        reply_markup=_STUCK_KB,
    )
    return ANSWERING


def _correct_text(q: dict) -> str:
    """Возвращает текст правильного ответа по индексу из оригинального вопроса."""
    return q["options"][q["correct"]]

def _is_wrong(item: dict) -> bool:
    """True если ответ пользователя не совпадает с правильным текстом."""
    return item["user_answer"] != _correct_text(item["question_obj"])


def _suggest_next_level(current_key: str) -> dict | None:
    """Возвращает следующий уровень сложности для текущего ключа, или None."""
    progression = {
        "easy_p1":        {"name": "🟡 Средний (1–16)",         "callback": "level_medium_p1"},
        "easy_p2":        {"name": "🟡 Средний (17–25)",        "callback": "level_medium_p2"},
        "easy":           {"name": "🟡 Средний (1–25)",         "callback": "level_medium"},
        "medium_p1":      {"name": "🔴 Сложный (1–16)",         "callback": "level_hard_p1"},
        "medium_p2":      {"name": "🔴 Сложный (17–25)",        "callback": "level_hard_p2"},
        "medium":         {"name": "🔴 Сложный (1–25)",         "callback": "level_hard"},
        "hard_p1":        {"name": "🙏 Применение (1–16)",      "callback": "level_practical_p1"},
        "hard_p2":        {"name": "🙏 Применение (17–25)",     "callback": "level_practical_p2"},
        "hard":           {"name": "🙏 Применение (1–25)",      "callback": "level_practical_ch1"},
        "practical_p1":   {"name": "🔬 Лингвистика ч.1",       "callback": "level_linguistics_ch1"},
        "practical_p2":   {"name": "🔬 Лингвистика ч.2",       "callback": "level_linguistics_ch1_2"},
        "practical_ch1":  {"name": "🔬 Лингвистика ч.1",       "callback": "level_linguistics_ch1"},
        "linguistics_ch1":   {"name": "🔬 Лингвистика ч.2",    "callback": "level_linguistics_ch1_2"},
        "linguistics_ch1_2": {"name": "🔬 Лингвистика ч.3",    "callback": "level_linguistics_ch1_3"},
    }
    return progression.get(current_key)



async def send_final_results_menu(bot, chat_id: int, data: dict):
    """
    Отправляет финальное меню после результатов и достижений.
    Всегда внизу чата — легко найти.
    """
    # Берём уже подсчитанный результат из data (он записывается в show_results / show_challenge_results)
    # Fallback: считаем вручную через сравнение user_answer с правильным вариантом
    total = len(data.get("questions", []))
    correct_count = data.get("correct_answers", None)
    if correct_count is None:
        answered = data.get("answered_questions", [])
        correct_count = sum(
            1 for a in answered
            if isinstance(a, dict) and not _is_wrong(a)
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
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"{emoji} *{level_name}*\n"
        f"Результат: *{correct_count}/{total}* ({percentage}%)\n"
        f"_{comment}_\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📋 *Выбери действие:*"
    )

    keyboard = []

    # Главная кнопка — посмотреть ответы
    keyboard.append([
        InlineKeyboardButton("📖 ПОСМОТРЕТЬ ОТВЕТЫ И РАЗБОР", callback_data="review_test_0"),
    ])

    # Пересдать ошибки (если есть)
    if wrong_count > 0:
        user_id = data.get("user_id")
        if user_id:
            keyboard.append([
                InlineKeyboardButton(
                    f"🔄 Пересдать ошибки ({wrong_count} шт.)",
                    callback_data=f"retry_errors_{user_id}",
                ),
            ])

    # Пройти заново
    if level_key:
        keyboard.append([
            InlineKeyboardButton("🔁 Пройти этот тест заново", callback_data=f"level_{level_key}"),
        ])

    # Поделиться — формируем красивый текст
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
    empty  = "⬜" * (10 - percentage // 10)
    progress_bar = filled + empty

    try:
        bot_info = await bot.get_me()
        bot_username = bot_info.username or "milovanovaibot"
    except Exception:
        bot_username = "milovanovaibot"

    challenge_mode = data.get("challenge_mode")
    if challenge_mode:
        mode_name = "🎲 Random Challenge" if challenge_mode == "random20" else "💀 Hardcore Challenge"
        share_text = (
            f"⚡ Challenge по 1 Посланию Петра\n\n"
            f"{mode_name}\n"
            f"{progress_bar} {percentage}%\n\n"
            f"{result_emoji} {correct_count}/{total} — {result_comment}\n"
        )
        bonus = data.get("challenge_bonus", 0)
        if bonus and bonus > 0:
            share_text += f"🎁 Бонус: +{bonus} баллов!\n"
        share_text += f"\nПримешь вызов? 👉 @{bot_username}"
    else:
        share_text = (
            f"📖 Тест по 1 Посланию Петра\n\n"
            f"{level_name}\n"
            f"{progress_bar} {percentage}%\n\n"
            f"{result_emoji} {correct_count}/{total} — {result_comment}\n"
        )
        max_streak = data.get("max_streak", 0)
        if max_streak >= 3:
            share_text += f"🔥 Серия: {max_streak} подряд!\n"
        share_text += f"\nПроверь свои знания 👉 @{bot_username}"

    keyboard.append([
        InlineKeyboardButton(
            "📤 Поделиться результатом",
            switch_inline_query=share_text,
        ),
    ])

    # Навигация
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
    except Exception as e:
        logger.error("send_final_results_menu: ошибка отправки: %s", e)


async def show_results(bot, user_id):
    """Показывает результаты: редактирует пузырь вопроса, затем фото с кнопками."""
    data       = user_data[user_id]
    score      = data["correct_answers"]
    total      = len(data["questions"])
    percentage = (score / total) * 100
    time_taken = time.time() - data["start_time"]
    chat_id    = data.get("quiz_chat_id")
    quiz_mid   = data.get("quiz_message_id")
    username   = data.get("username")
    first_name = data.get("first_name", "Игрок")

    session_id = data.get("session_id")
    if session_id:
        finish_quiz_session(session_id)

    add_to_leaderboard(user_id, username, first_name, data["level_key"], score, total, time_taken,
                       score_multiplier=data.get("score_multiplier", 1.0))
    position, entry = get_user_position(user_id)

    cfg = next((v for v in LEVEL_CONFIG.values() if v["pool_key"] == data["level_key"]), None)
    base_points = score * (cfg["points_per_q"] if cfg else 1)
    score_multiplier = data.get("score_multiplier", 1.0)
    earned_points = round(base_points * score_multiplier)
    multiplier_label = {1.5: " ×1.5 ⏱", 2.0: " ×2 ⚡"}.get(score_multiplier, "")

    if percentage >= 90:   grade = "Отлично! 🌟"
    elif percentage >= 70: grade = "Хорошо! 👍"
    elif percentage >= 50: grade = "Удовлетворительно 📖"
    else:                  grade = "Нужно повторить 📚"

    max_streak = data.get("max_streak", 0)
    streak_line = f"\n🔥 *Лучшая серия:* {max_streak} подряд!" if max_streak >= 3 else ""
    result_text = (
        f"🏆 *РЕЗУЛЬТАТЫ*\n\n"
        f"*Категория:* {data['level_name']}\n"
        f"*Правильно:* {score}/{total} ({percentage:.0f}%)\n"
        f"*Баллы:* +{earned_points} 💎{multiplier_label}\n"
        f"*Время:* {format_time(time_taken)}\n"
        f"*Позиция:* #{position}\n"
        f"*Оценка:* {grade}{streak_line}\n"
    )

    answered = data.get("answered_questions", [])
    wrong = [item for item in answered if _is_wrong(item)]

    # Сохраняем ошибки в user_data для пагинации
    if user_id in user_data:
        user_data[user_id]["wrong_answers"] = wrong
        user_data[user_id]["user_id"] = user_id  # для финального меню

    # Основная карточка результатов — без кнопок (будут в финальном меню)
    # Шаг 0: конфетти при идеальном результате — до карточки результатов
    if score == total and chat_id:
        await animate_confetti(bot, chat_id)

    # Шаг 1: редактируем пузырь вопроса — показываем заглушку "Генерирую..."
    stub_deleted = False
    if quiz_mid and chat_id:
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=quiz_mid,
                text="⏳ *Тест завершён! Генерирую результат...*",
                parse_mode="Markdown",
            )
        except Exception:
            quiz_mid = None  # редактирование не удалось — забудем об этом пузыре

    # Шаг 2: пробуем генерировать GIF (анимированный), fallback → PNG → текст
    photo_sent = False
    try:
        rank_name = get_rank_name(percentage)

        # Попытка 1: анимированный GIF
        gif_buf = await create_result_gif(
            score=score,
            total=total,
            rank_name=rank_name,
            time_seconds=time_taken,
            first_name=first_name,
        )
        if gif_buf:
            caption = (
                f"🏆 *{score}/{total}* ({percentage:.0f}%) • {rank_name}\n"
                f"⏱ {format_time(time_taken)} • 💎 +{earned_points} • #{position}"
            )
            await bot.send_animation(
                chat_id=chat_id,
                animation=InputFile(gif_buf, filename="result.gif"),
                caption=caption,
                parse_mode="Markdown",
            )
            photo_sent = True

        # Fallback: PNG если GIF не вышел
        if not photo_sent:
            img_bytes = await generate_result_image(
                bot=bot,
                user_id=user_id,
                first_name=first_name,
                score=score,
                total=total,
                rank_name=rank_name,
            )
            if img_bytes:
                bio = io.BytesIO(img_bytes)
                bio.name = "result.png"
                bio.seek(0)
                caption = (
                    f"🏆 *{score}/{total}* ({percentage:.0f}%) • {rank_name}\n"
                    f"⏱ {format_time(time_taken)} • 💎 +{earned_points} • #{position}"
                )
                await bot.send_photo(
                    chat_id=chat_id,
                    photo=InputFile(bio, filename="result.png"),
                    caption=caption,
                    parse_mode="Markdown",
                )
                photo_sent = True

    except Exception as e:
        logger.error("Result image error", exc_info=True)

    # Шаг 3: удаляем заглушку "Генерирую..." если картинка ушла
    if photo_sent and quiz_mid and chat_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=quiz_mid)
        except Exception:
            pass
    elif not photo_sent:
        # Картинки нет — редактируем заглушку в финальный текст с кнопками
        if quiz_mid and chat_id:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=quiz_mid,
                    text=result_text,
                    parse_mode="Markdown",
                )
            except Exception:
                # Совсем не вышло — шлём новым сообщением
                try:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=result_text,
                        parse_mode="Markdown",
                    )
                except Exception:
                    logger.error("show_results: не удалось отправить результаты", exc_info=True)
        else:
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=result_text,
                    parse_mode="Markdown",
                )
            except Exception:
                logger.error("show_results: не удалось отправить результаты (no quiz_mid)", exc_info=True)

    # Ежедневный бонус за первый тест дня
    _daily_bonus = check_daily_bonus(user_id)
    if _daily_bonus > 0 and chat_id:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🌅 *ЕЖЕДНЕВНЫЙ БОНУС!*\n\n"
                    f"Это твой первый тест сегодня!\n"
                    f"🎁 *+{_daily_bonus} баллов*"
                ),
                parse_mode="Markdown",
            )
        except Exception:
            pass

    # Проверяем и выдаём достижения после показа результатов
    await check_and_award_achievements(bot, user_id, data)

    # Финальное меню — всегда внизу чата, легко найти
    await asyncio.sleep(0.5)
    if chat_id:
        await send_final_results_menu(bot, chat_id, data)

    # (идеальный результат отмечается конфетти выше, до карточки)


# ═══════════════════════════════════════════════
# INLINE-ОТВЕТ НА ВОПРОС (основной тест)
# ═══════════════════════════════════════════════

async def _animate_answer_buttons(
    query,
    btn_index: int,
    correct_index: int,
    is_numeric_mode: bool,
    shuffled: list[str],
) -> None:
    """
    Визуальная анимация кнопок после ответа.

    Два режима кнопок (определяется is_numeric_mode):
      - Числовой: кнопки "1","2","3","4" — меняем только текст, callback не трогаем
      - Текстовый: кнопки с полным текстом варианта

    Структура клавиатуры:
      rows[0..N-1] — варианты ответа (каждая строка = 1 вариант или ряд чисел)
      rows[-1]     — [⚠️ Неточность?, ↩️ выйти] — не трогаем

    is_correct  → анимация: ✅ → 🎉 → ⭐ → ✅ (по 0.3 сек)
    is_incorrect → подсвечиваем нажатую ❌ и правильную ✅, без анимации
    """
    is_correct = (btn_index == correct_index)

    try:
        # Получаем текущую клавиатуру из сообщения
        rows = [
            [InlineKeyboardButton(btn.text, callback_data=btn.callback_data)
             for btn in row]
            for row in query.message.reply_markup.inline_keyboard
        ]
        # Последняя строка — служебные кнопки, не трогаем
        answer_rows = rows[:-1]
        service_row = rows[-1]

        if is_numeric_mode:
            # Числовой режим: все варианты в одной строке [1][2][3][4]
            # answer_rows[0] содержит все кнопки-цифры
            num_row = answer_rows[0]

            if is_correct:
                for emoji in ["✅", "🎉", "⭐", "✅"]:
                    num_row[btn_index].text = f"{emoji}"
                    try:
                        await query.edit_message_reply_markup(
                            reply_markup=InlineKeyboardMarkup(answer_rows + [service_row])
                        )
                    except Exception:
                        return
                    await asyncio.sleep(0.3)
            else:
                num_row[btn_index].text  = "❌"
                num_row[correct_index].text = "✅"
                try:
                    await query.edit_message_reply_markup(
                        reply_markup=InlineKeyboardMarkup(answer_rows + [service_row])
                    )
                except Exception:
                    pass

        else:
            # Текстовый режим: каждый вариант — отдельная строка
            # answer_rows[i] = [InlineKeyboardButton(текст, callback_data=...)]
            correct_text = shuffled[correct_index]
            user_text    = shuffled[btn_index]

            if is_correct:
                for emoji in ["✅", "🎉", "⭐", "✅"]:
                    answer_rows[btn_index][0].text = f"{emoji} {user_text}"
                    try:
                        await query.edit_message_reply_markup(
                            reply_markup=InlineKeyboardMarkup(answer_rows + [service_row])
                        )
                    except Exception:
                        return
                    await asyncio.sleep(0.3)
            else:
                answer_rows[btn_index][0].text   = f"❌ {user_text}"
                answer_rows[correct_index][0].text = f"✅ {correct_text}"
                try:
                    await query.edit_message_reply_markup(
                        reply_markup=InlineKeyboardMarkup(answer_rows + [service_row])
                    )
                except Exception:
                    pass

    except Exception as e:
        logger.debug("_animate_answer_buttons error: %s", e)


async def _handle_inline_answer(update: Update, context, prefix: str):
    """
    Общая логика обработки inline-ответа.
    prefix = 'qa'  → обычный тест   (send_question / show_results)
    prefix = 'cha' → challenge-режим (send_challenge_question / show_challenge_results)
    """
    query   = update.callback_query
    user_id = query.from_user.id
    _touch(user_id)

    if user_id not in user_data:
        await query.answer("⚠️ Сессия не найдена. Начни тест заново.", show_alert=True)
        return

    data = user_data[user_id]

    if data.get("processing_answer"):
        await query.answer()
        return

    q_num = data["current_question"]
    if q_num >= len(data["questions"]):
        await query.answer()
        return

    try:
        btn_index = int(query.data.replace(f"{prefix}_", ""))
    except ValueError:
        await query.answer()
        return

    shuffled = data.get("current_options", [])
    if btn_index >= len(shuffled):
        await query.answer()
        return

    user_answer         = shuffled[btn_index]
    q                   = data["questions"][q_num]
    correct_text        = data.get("current_correct_text") or q["options"][q["correct"]]
    session_id_at_start = data.get("session_id")
    is_challenge        = data.get("is_challenge", False)

    data["processing_answer"] = True
    await query.answer()

    try:
        timer_task = data.get("timer_task")
        if timer_task and not timer_task.done():
            timer_task.cancel()
        # Отменяем живой countdown — ответ уже получен
        _cancel_countdown(user_id)

        is_correct = (user_answer == correct_text)
        _reset_bad_input(user_id)

        # ── Анимация кнопок ──────────────────────────────────────────────────
        # Находим индекс правильного ответа в перемешанном списке
        try:
            correct_index = shuffled.index(correct_text)
        except ValueError:
            correct_index = btn_index  # fallback: считаем ответ верным визуально

        # Числовой режим: кнопки "1"/"2"... — когда хотя бы один вариант длинный
        is_numeric_mode = (
            query.message.reply_markup is not None
            and len(query.message.reply_markup.inline_keyboard) > 0
            and len(query.message.reply_markup.inline_keyboard[0]) > 1
        )
        await _animate_answer_buttons(query, btn_index, correct_index, is_numeric_mode, shuffled)
        # ─────────────────────────────────────────────────────────────────────

        if is_correct:
            data["correct_answers"] += 1
            data["current_streak"] = data.get("current_streak", 0) + 1
            streak = data["current_streak"]
            if data["current_streak"] > data.get("max_streak", 0):
                data["max_streak"] = data["current_streak"]
            if streak >= 10:   streak_text = f"🔥×{streak} НЕВЕРОЯТНО!"
            elif streak >= 5:  streak_text = f"🔥×{streak} ГОРЯЧО!"
            elif streak >= 2:  streak_text = f"🔥×{streak}"
            else:              streak_text = ""
            streak_suffix = f" {streak_text}" if streak_text else ""
            feedback = f"✅ *Верно!*{streak_suffix}\n\n_{correct_text}_"
        else:
            data["current_streak"] = 0
            feedback = f"❌ *Неверно*\n\n✅ Правильно: *{correct_text}*"

        elapsed = time.time() - data.get("question_sent_at", time.time())
        # Отслеживаем самый быстрый ответ (для достижения "Молния")
        if elapsed < data.get("fastest_answer", 9999):
            data["fastest_answer"] = elapsed
        q_id    = get_qid(q)
        record_question_stat(q_id, data["level_key"], is_correct, elapsed)

        data["answered_questions"].append({"question_obj": q, "user_answer": user_answer})
        data["current_question"] += 1

        if session_id_at_start:
            advance_quiz_session(session_id_at_start, q_id, user_answer, is_correct, q)

        qmid, qcid = data.get("quiz_message_id"), data.get("quiz_chat_id")
        if qmid and qcid:
            try:
                await context.bot.edit_message_text(
                    chat_id=qcid, message_id=qmid,
                    text=feedback, parse_mode="Markdown",
                )
            except Exception as e:
                if "not modified" not in str(e).lower():
                    logger.warning("%s_inline_answer edit error: %s", prefix, e)

        delay = FEEDBACK_DELAY_CORRECT if is_correct else FEEDBACK_DELAY_WRONG
        await asyncio.sleep(delay)

        if user_id not in user_data:
            return
        current_data = user_data[user_id]
        if current_data.get("session_id") != session_id_at_start:
            return

        if current_data["current_question"] < len(current_data["questions"]):
            if is_challenge:
                await send_challenge_question(context.bot, user_id)
            else:
                await send_question(context.bot, user_id)
        else:
            if is_challenge:
                await show_challenge_results(context.bot, user_id)
            else:
                await _finalize_quiz_bubble(context.bot, user_id)
                await show_results(context.bot, user_id)

    except Exception:
        logger.error("%s_inline_answer unexpected error", prefix, exc_info=True)
    finally:
        if user_id in user_data and user_data[user_id].get("session_id") == session_id_at_start:
            user_data[user_id]["processing_answer"] = False


async def quiz_inline_answer(update: Update, context):
    """Обрабатывает нажатие кнопки ответа в обычном тесте."""
    await _handle_inline_answer(update, context, "qa")


# ═══════════════════════════════════════════════
# ПОВТОРЕНИЕ ОШИБОК
# ═══════════════════════════════════════════════

async def retry_errors(update: Update, context):
    query   = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    target_id = int(query.data.replace("retry_errors_", ""))

    # Защита: нельзя повторять чужие ошибки
    if target_id != user_id:
        await query.answer("⚠️ Это не ваша сессия.", show_alert=True)
        return ConversationHandler.END

    if target_id not in user_data:
        await query.edit_message_text("⚠️ Данные сессии устарели. Начни новый тест.")
        return ConversationHandler.END

    prev_data = user_data[target_id]
    answered  = prev_data.get("answered_questions", [])
    wrong_questions = [
        item["question_obj"] for item in answered
        if _is_wrong(item)
    ]

    if not wrong_questions:
        await query.answer("Ошибок нет!", show_alert=True)
        return

    user_data[user_id] = _create_session_data(
        user_id=user_id,
        session_id=None,
        questions=wrong_questions,
        level_name=f"🔁 Повторение ошибок ({prev_data['level_name']})",
        chat_id=query.message.chat_id,
        level_key=prev_data["level_key"],
        correct_answers=0,
        start_time=time.time(),
        last_activity=time.time(),
        is_battle=False,
        battle_points=0,
        is_retry=True,
        username=query.from_user.username,
        first_name=query.from_user.first_name,
    )

    await query.edit_message_text(
        f"🔁 *ПОВТОРЕНИЕ ОШИБОК*\n\nВопросов: {len(wrong_questions)}\nПоехали! 💪",
        parse_mode="Markdown",
    )
    await send_question(context.bot, user_id)
    return ANSWERING


# ═══════════════════════════════════════════════
# ПАГИНАЦИЯ РАЗБОРА ОШИБОК
# ═══════════════════════════════════════════════

def _build_error_page(wrong: list, index: int) -> tuple:
    """Формирует текст и клавиатуру для одной страницы разбора ошибок."""
    total = len(wrong)
    item  = wrong[index]
    q     = item["question_obj"]
    user_ans     = item["user_answer"]
    correct_text = _correct_text(q)

    verse_tag = f"📖 ст. {q['verse']} | " if q.get("verse") else ""
    topic_tag = f"🏷 {q['topic']}" if q.get("topic") else ""

    text  = f"🔴 *Ошибка {index + 1} из {total}* {verse_tag}{topic_tag}\n\n"
    text += f"*Вопрос:* _{q['question']}_\n\n"
    text += f"*Ваш ответ:* {user_ans}\n"
    text += f"*Правильно:* {correct_text}\n\n"
    if "options_explanations" in q:
        text += "*Разбор вариантов:*\n"
        for j, opt in enumerate(q["options"]):
            text += f"• _{opt}_\n{q['options_explanations'][j]}\n\n"
    text += f"💡 *Пояснение:* {q['explanation']}"
    if q.get("pdf_ref"):
        text += f"\n\n📄 _Источник: {q['pdf_ref']}_"

    left_cb  = f"review_nav_{index - 1}" if index > 0 else "review_nav_noop"
    right_cb = f"review_nav_{index + 1}" if index < total - 1 else "review_nav_noop"

    nav_buttons = [
        InlineKeyboardButton("⬅️" if index > 0 else "·", callback_data=left_cb),
        InlineKeyboardButton(f"{index + 1}/{total}", callback_data="review_nav_noop"),
        InlineKeyboardButton("➡️" if index < total - 1 else "·", callback_data=right_cb),
    ]

    keyboard = InlineKeyboardMarkup([
        nav_buttons,
        [InlineKeyboardButton("🔙 Вернуться в Меню", callback_data="back_to_main")],
    ])
    return safe_truncate(text, 4000), keyboard



async def noop_handler(update: Update, context):
    """Кнопка-счётчик — ничего не делает."""
    await update.callback_query.answer()


async def review_test_handler(update: Update, context):
    """Листание вопросов теста с правильными ответами после завершения."""
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    data    = user_data.get(user_id, {})

    q_index  = int(query.data.split("_")[-1])
    answered = data.get("answered_questions", [])

    if not answered or q_index >= len(answered):
        await query.edit_message_text("❌ Данные теста не найдены. Пройди тест заново.")
        return

    total       = len(answered)
    answer_data = answered[q_index]
    q           = answer_data.get("question_obj", {})
    user_answer = answer_data.get("user_answer", "—")
    correct_answer = _correct_text(q)
    is_correct  = (user_answer == correct_answer)
    status      = "✅" if is_correct else "❌"

    text = (
        f"📖 *Просмотр теста* ({q_index + 1}/{total})\n\n"
        f"*Вопрос:*\n{q.get('question', '—')}\n\n"
        "*Варианты:*\n"
    )
    for i, opt in enumerate(q.get("options", [])):
        if i == q.get("correct"):
            marker = "✅"
        elif opt == user_answer and not is_correct:
            marker = "❌"
        else:
            marker = "⬜"
        arrow = " ← твой ответ" if opt == user_answer and not is_correct else ""
        text += f"{marker} {i+1}. {opt}{arrow}\n"

    text += f"\n*Твой ответ:* {user_answer} {status}"

    explanation = q.get("explanation") or q.get("fun_fact")
    if explanation:
        text += f"\n\n💡 *Пояснение:*\n_{explanation}_"
    nav_row = []
    if q_index > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Пред.", callback_data=f"review_test_{q_index - 1}"))
    nav_row.append(InlineKeyboardButton(f"{q_index + 1}/{total}", callback_data="noop"))
    if q_index < total - 1:
        nav_row.append(InlineKeyboardButton("➡️ След.", callback_data=f"review_test_{q_index + 1}"))

    buttons = [nav_row, [InlineKeyboardButton("🏠 В меню", callback_data="back_to_main")]]

    await query.edit_message_text(
        text=text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def review_errors_handler(update: Update, context):
    """Показывает/листает ошибки внутри одного сообщения."""
    query   = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data_cb = query.data

    if data_cb.startswith("review_errors_"):
        # Первый вход: review_errors_{uid}_{idx}
        parts     = data_cb.split("_")
        target_id = int(parts[2])
        index     = int(parts[3])
    elif data_cb.startswith("review_nav_"):
        suffix = data_cb.replace("review_nav_", "")
        if suffix == "noop":
            return
        index     = int(suffix)
        target_id = user_id
    else:
        return

    if target_id not in user_data:
        await query.edit_message_text("⚠️ Данные устарели. Начни новый тест.")
        return

    wrong = user_data[target_id].get("wrong_answers", [])
    if not wrong:
        await query.edit_message_text("✅ Ошибок нет!")
        return

    index = max(0, min(index, len(wrong) - 1))
    text, keyboard = _build_error_page(wrong, index)

    try:
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode="Markdown")
    except Exception as e:
        if "not modified" not in str(e).lower():
            raise


# ═══════════════════════════════════════════════
# ВОССТАНОВЛЕНИЕ СЕССИИ ПОСЛЕ РЕСТАРТА
# ═══════════════════════════════════════════════

async def _restore_session_to_memory(user_id: int, db_session: dict):
    mode = db_session.get("mode", "level")
    questions = db_session.get("questions_data", [])
    current_index = db_session.get("current_index", 0)
    correct_count = db_session.get("correct_count", 0)
    answered = db_session.get("answered_questions", [])
    start_time_val = db_session.get("start_time", time.time())
    is_challenge = mode in ("random20", "hardcore20")
    time_limit = db_session.get("time_limit")

    user_data[user_id] = _create_session_data(
        user_id=user_id,
        session_id=db_session["_id"],
        questions=questions,
        level_name=db_session.get("level_name", "Тест"),
        chat_id=db_session.get("chat_id"),
        current_question=current_index,
        answered_questions=answered,
        is_challenge=is_challenge,
        level_key=db_session.get("level_key", mode),
        correct_answers=correct_count,
        start_time=start_time_val,
        last_activity=time.time(),
        is_battle=False,
        battle_points=0,
        challenge_mode=mode if is_challenge else None,
        challenge_eligible=is_bonus_eligible(user_id, mode) if is_challenge else False,
        challenge_time_limit=time_limit,
        username=None,
        first_name="Игрок",
    )


async def _handle_timeout_after_restart(message, user_id: int, db_session: dict):
    await _restore_session_to_memory(user_id, db_session)
    data = user_data[user_id]
    data["quiz_chat_id"] = message.chat_id
    q_num = data["current_question"]
    q = data["questions"][q_num]
    correct_text = q["options"][q["correct"]]
    q_id = get_qid(q)
    session_id = data["session_id"]
    advance_quiz_session(session_id, q_id, "⏱ Время вышло", False, q)
    data["answered_questions"].append({"question_obj": q, "user_answer": "⏱ Время вышло"})
    data["current_question"] += 1
    bot = message.get_bot()
    try:
        await bot.send_message(
            chat_id=message.chat_id,
            text=f"⏱ *Время вышло!*\n✅ {correct_text}",
            parse_mode="Markdown",
        )
    except Exception:
        pass
    if data["current_question"] < len(data["questions"]):
        await send_challenge_question(bot, user_id)
    else:
        await show_challenge_results(bot, user_id)


async def resume_session_handler(update: Update, context):
    query = update.callback_query
    await query.answer()
    session_id = query.data.replace("resume_session_", "")
    user_id = query.from_user.id
    _touch(user_id)

    db_session = get_quiz_session(session_id)
    if not db_session or db_session.get("status") != "in_progress":
        await query.edit_message_text("⚠️ Сессия не найдена или уже завершена.")
        return

    await _restore_session_to_memory(user_id, db_session)
    data = user_data[user_id]
    # Store user info and chat_id for the new inline flow
    data["username"]    = query.from_user.username
    data["first_name"]  = query.from_user.first_name or "Игрок"
    data["quiz_chat_id"] = query.message.chat_id
    mode = db_session.get("mode", "level")

    if is_question_timed_out(db_session):
        await query.edit_message_text("▶️ Продолжаем тест...")
        await _handle_timeout_after_restart(query.message, user_id, db_session)
        return ANSWERING

    level_name = data["level_name"]
    current = data["current_question"]
    total = len(data["questions"])
    await query.edit_message_text(
        f"▶️ *Продолжаем!*\n_{level_name}_\nВопрос {current + 1}/{total}",
        parse_mode="Markdown",
    )
    if mode in ("random20", "hardcore20"):
        await send_challenge_question(context.bot, user_id)
    else:
        await send_question(context.bot, user_id)
    return ANSWERING


async def restart_session_handler(update: Update, context):
    query = update.callback_query
    await query.answer()
    session_id = query.data.replace("restart_session_", "")
    user_id = query.from_user.id
    _touch(user_id)

    db_session = get_quiz_session(session_id)
    cancel_quiz_session(session_id)

    if not db_session:
        await query.edit_message_text("⚠️ Сессия не найдена.")
        return

    mode = db_session.get("mode", "level")
    if mode in ("random20", "hardcore20"):
        eligible = is_bonus_eligible(user_id, mode)
        questions = pick_challenge_questions(mode)
        time_limit = 10 if mode == "hardcore20" else None
        mode_name = "🎲 Random Challenge" if mode == "random20" else "💀 Hardcore Random"
        question_ids = [get_qid(q) for q in questions]
        new_session_id = create_quiz_session(
            user_id=user_id, mode=mode, question_ids=question_ids,
            questions_data=questions, level_key=mode, level_name=mode_name,
            time_limit=time_limit,
            chat_id=query.message.chat_id,
        )
        user_data[user_id] = _create_session_data(
            user_id=user_id,
            session_id=new_session_id,
            questions=questions,
            level_name=mode_name,
            chat_id=query.message.chat_id,
            level_key=mode,
            correct_answers=0,
            start_time=time.time(),
            last_activity=time.time(),
            is_battle=False,
            battle_points=0,
            is_challenge=True,
            challenge_mode=mode,
            challenge_eligible=eligible,
            challenge_time_limit=time_limit,
            username=query.from_user.username,
            first_name=query.from_user.first_name or "Игрок",
        )
        await query.edit_message_text(f"{mode_name}\n\n📋 20 вопросов\nПоехали! 💪", parse_mode="Markdown")
        await send_challenge_question(context.bot, user_id)
    else:
        level_key = db_session.get("level_key")
        cfg = next((v for v in LEVEL_CONFIG.values() if v["pool_key"] == level_key), None)
        if not cfg:
            await query.edit_message_text("⚠️ Уровень не найден.")
            return
        questions = random.sample(get_pool_by_key(cfg["pool_key"]), min(cfg.get("num_questions", 10), len(get_pool_by_key(cfg["pool_key"]))))
        question_ids = [get_qid(q) for q in questions]
        new_session_id = create_quiz_session(
            user_id=user_id, mode="level", question_ids=question_ids,
            questions_data=questions, level_key=cfg["pool_key"],
            level_name=cfg["name"], time_limit=None,
            chat_id=query.message.chat_id,
        )
        user_data[user_id] = _create_session_data(
            user_id=user_id,
            session_id=new_session_id,
            questions=questions,
            level_name=cfg["name"],
            chat_id=query.message.chat_id,
            level_key=cfg["pool_key"],
            correct_answers=0,
            start_time=time.time(),
            last_activity=time.time(),
            is_battle=False,
            battle_points=0,
            username=query.from_user.username,
            first_name=query.from_user.first_name or "Игрок",
        )
        await query.edit_message_text(
            f"🔁 *Начинаем заново*\n{cfg['name']}\n\n📝 Вопросов: {len(questions)}",
            parse_mode="Markdown",
        )
        await send_question(context.bot, user_id)
    return ANSWERING


async def cancel_session_handler(update: Update, context):
    query = update.callback_query
    await query.answer()
    session_id = query.data.replace("cancel_session_", "")
    cancel_quiz_session(session_id)
    await query.edit_message_text("❌ Тест отменён.", reply_markup=_main_keyboard())


# ═══════════════════════════════════════════════
# РЕЖИМ БИТВЫ — MongoDB-backed (задание 1.2)
# ═══════════════════════════════════════════════

async def show_battle_menu(query):
    available = get_waiting_battles(limit=5)
    keyboard = [[InlineKeyboardButton("🆕 Создать битву", callback_data="create_battle")]]
    for b in available:
        keyboard.append([InlineKeyboardButton(
            f"⚔️ vs {b['creator_name']}", callback_data=f"join_battle_{b['_id']}"
        )])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")])

    text = "⚔️ *РЕЖИМ БИТВЫ*\n\n🎯 Соревнуйся с другими!\n"
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

    battle_doc = create_battle_doc(
        battle_id=battle_id,
        creator_id=user_id,
        creator_name=user_name,
        questions=random.sample(BATTLE_POOL + INTRO_POOL, min(10, len(BATTLE_POOL + INTRO_POOL))),
    )
    if not battle_doc:
        await query.edit_message_text("❌ Ошибка создания битвы. Попробуй позже.")
        return

    await query.edit_message_text(
        f"⚔️ *БИТВА СОЗДАНА!*\n\n"
        f"🆔 ID: `{battle_id[-8:]}`\n\n"
        "⏳ Ожидание соперника...\n\n"
        "_Битва автоматически удалится через 10 минут_",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Начать отвечать", callback_data=f"start_battle_{battle_id}_creator")],
            [InlineKeyboardButton("❌ Отменить",         callback_data=f"cancel_battle_{battle_id}")],
            [InlineKeyboardButton("⬅️ Назад",            callback_data="battle_menu")],
        ]),
        parse_mode="Markdown",
    )


async def join_battle(update: Update, context):
    query    = update.callback_query
    await query.answer()
    battle_id = query.data.replace("join_battle_", "")
    user_id   = query.from_user.id
    user_name = query.from_user.first_name

    battle = get_battle(battle_id)
    if not battle or battle.get("status") != "waiting":
        await query.edit_message_text(
            "❌ Битва не найдена или уже началась.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="battle_menu")]]),
        )
        return

    if battle["creator_id"] == user_id:
        await query.answer("Нельзя присоединиться к своей битве!", show_alert=True)
        return
    if battle["opponent_id"] is not None:
        await query.answer("К этой битве уже присоединился другой игрок!", show_alert=True)
        return

    update_battle(battle_id, {
        "opponent_id":   user_id,
        "opponent_name": user_name,
        "status":        "in_progress",
    })

    await query.edit_message_text(
        f"⚔️ *БИТВА НАЧАЛАСЬ!*\n\n"
        f"👤 Ты vs 👤 {battle['creator_name']}\n\n"
        "📝 10 вопросов\n⏱ Время учитывается!\nНажми «Начать»",
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
    user_id    = query.from_user.id

    battle = get_battle(battle_id)
    if not battle:
        await query.edit_message_text("❌ Битва не найдена.")
        return

    user_data[user_id] = _create_session_data(
        user_id=user_id,
        session_id=battle_id,
        questions=battle["questions"],
        level_name="⚔️ PvP Битва",
        chat_id=query.message.chat_id,
        battle_id=battle_id,
        role=role,
        correct_answers=0,
        start_time=time.time(),
        last_activity=time.time(),
        is_battle=True,
        battle_points=0,
        battle_chat_id=query.message.chat_id,
        battle_role=role,
    )

    await query.edit_message_text("⚔️ *БИТВА: Вопрос 1/10*\n\nНачинаем! 🍀", parse_mode="Markdown")
    await send_battle_question(context.bot, query.message.chat_id, user_id)
    return BATTLE_ANSWERING


async def send_battle_question(bot, chat_id: int, user_id: int):
    """Отправляет или редактирует вопрос битвы. bot передаётся явно — всегда context.bot."""
    data  = user_data[user_id]
    q_num = data["current_question"]

    if q_num >= len(data["questions"]):
        await finish_battle_for_user(bot, chat_id, user_id)
        return

    q            = data["questions"][q_num]
    correct_text = q["options"][q["correct"]]
    shuffled     = q["options"][:]
    random.shuffle(shuffled)
    data["current_options"]      = shuffled
    data["current_correct_text"] = correct_text
    data["question_sent_at"]     = time.time()

    progress = build_progress_bar(q_num + 1, len(data["questions"]))
    options_text = ""
    if any(len(opt) > MAX_BTN_LEN for opt in shuffled):
        options_text = "\n\n" + "\n".join(f"*{i+1}.* {opt}" for i, opt in enumerate(shuffled))
        buttons = [[InlineKeyboardButton(str(i + 1), callback_data=f"ba_{i}") for i in range(len(shuffled))]]
    else:
        buttons = [[InlineKeyboardButton(opt, callback_data=f"ba_{i}")] for i, opt in enumerate(shuffled)]
    buttons.append([InlineKeyboardButton("❌ Выйти", callback_data=f"cancel_battle_{data['battle_id']}")])
    keyboard = InlineKeyboardMarkup(buttons)

    text = (
        f"⚔️ *Вопрос {q_num + 1}/{len(data['questions'])}* {progress}\n"
        f"⚡ Быстрее = больше очков!\n\n{q['question']}{options_text}"
    )

    battle_msg_id = data.get("battle_message_id")
    if battle_msg_id:
        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=battle_msg_id,
                text=text, reply_markup=keyboard, parse_mode="Markdown",
            )
            return
        except Exception as e:
            if "not modified" in str(e).lower():
                return
            # редактирование не удалось — шлём новым сообщением ниже

    sent = await bot.send_message(
        chat_id=chat_id, text=text, reply_markup=keyboard, parse_mode="Markdown",
    )
    data["battle_message_id"] = sent.message_id
    data["battle_chat_id"]    = chat_id


async def battle_answer(update: Update, context):
    """Обрабатывает нажатие inline-кнопки ответа в битве (callback_data=ba_<index>)."""
    query   = update.callback_query
    user_id = query.from_user.id

    if user_id not in user_data or not user_data[user_id].get("is_battle"):
        await query.answer()
        return

    data = user_data[user_id]

    # Защита от двойного нажатия
    if data.get("processing_answer"):
        await query.answer()
        return
    data["processing_answer"] = True

    # chat_id: предпочитаем зафиксированный при старте, fallback — текущий апдейт
    chat_id = data.get("battle_chat_id") or query.message.chat_id

    try:
        idx = int(query.data.replace("ba_", ""))
        current_options = data.get("current_options", [])
        if idx >= len(current_options):
            await query.answer()
            return

        q_num        = data["current_question"]
        q            = data["questions"][q_num]
        user_answer  = current_options[idx]
        correct_text = data.get("current_correct_text") or q["options"][q["correct"]]

        sent_at     = data.get("question_sent_at", time.time())
        elapsed     = min(time.time() - sent_at, 7.0)

        if user_answer == correct_text:
            data["correct_answers"] += 1
            speed_bonus = round((7.0 - elapsed) / 7.0 * 7)
            points = 10 + speed_bonus
            data["battle_points"] = data.get("battle_points", 0) + points
            await query.answer(f"✅ +{points} очков (⚡{speed_bonus} бонус)", show_alert=False)
        else:
            await query.answer(f"❌ Верно: {correct_text}", show_alert=True)

        data["current_question"] += 1
    finally:
        data["processing_answer"] = False

    if data["current_question"] < len(data["questions"]):
        await send_battle_question(context.bot, chat_id, user_id)
    else:
        await finish_battle_for_user(context.bot, chat_id, user_id)


async def finish_battle_for_user(bot, chat_id: int, user_id: int):
    """Записывает результат игрока в Mongo. Если оба закончили — рассылает итоги."""
    data          = user_data[user_id]
    battle_id     = data["battle_id"]
    role          = data["role"]
    time_taken    = time.time() - data["start_time"]
    battle_points = data.get("battle_points", 0)

    battle = get_battle(battle_id)
    if not battle:
        await bot.send_message(chat_id=chat_id, text="❌ Битва не найдена.")
        return

    if role == "creator":
        update_battle(battle_id, {
            "creator_score":    data["correct_answers"],
            "creator_time":     time_taken,
            "creator_points":   battle_points,
            "creator_finished": True,
        })
    else:
        update_battle(battle_id, {
            "opponent_score":    data["correct_answers"],
            "opponent_time":     time_taken,
            "opponent_points":   battle_points,
            "opponent_finished": True,
        })

    # Перечитываем актуальное состояние из БД
    battle = get_battle(battle_id)
    if battle.get("creator_finished") and battle.get("opponent_finished"):
        await show_battle_results(bot, battle_id)
    else:
        await bot.send_message(
            chat_id=chat_id,
            text=(
                f"✅ *Ты закончил!*\n\n"
                f"📊 Твой результат: {data['correct_answers']}/10\n"
                f"⏱ Время: {format_time(time_taken)}\n\n"
                "⏳ Ожидание соперника..."
            ),
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В меню", callback_data="back_to_main")]]),
        )


async def show_battle_results(bot, battle_id: str):
    """
    Формирует итоговый текст и отправляет его ОБОИМ участникам через context.bot.
    Вызывается только когда creator_finished и opponent_finished == True.
    """
    battle = get_battle(battle_id)
    if not battle:
        return

    creator_points  = battle.get("creator_points", 0)
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
    text += (
        f"👤 *{battle['creator_name']}*\n"
        f"   ✅ {battle['creator_score']}/10 • ⚡ {creator_points} очков"
        f" • ⏱ {format_time(battle['creator_time'])}\n\n"
    )
    text += (
        f"👤 *{battle.get('opponent_name', 'Соперник')}*\n"
        f"   ✅ {battle['opponent_score']}/10 • ⚡ {opponent_points} очков"
        f" • ⏱ {format_time(battle['opponent_time'])}\n\n"
    )
    text += "💎 *+5 баллов* победителю!\n" if winner != "draw" else "💎 *+2 балла* каждому!\n"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Новая битва", callback_data="battle_menu")],
        [InlineKeyboardButton("⬅️ В меню",       callback_data="back_to_main")],
    ])

    # Гарантированно отправляем обоим — каждый получит в свой личный чат
    for uid in (battle["creator_id"], battle["opponent_id"]):
        try:
            await bot.send_message(
                chat_id=uid,
                text=text,
                reply_markup=keyboard,
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.warning("Battle result delivery to %s failed: %s", uid, e)

    delete_battle(battle_id)


async def cancel_battle(update: Update, context):
    query = update.callback_query
    await query.answer()
    battle_id = query.data.replace("cancel_battle_", "")
    delete_battle(battle_id)
    await query.edit_message_text(
        "❌ Битва отменена.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="battle_menu")]]),
    )


# ═══════════════════════════════════════════════
# INLINE MODE — Вызов на дуэль (задание 4.1)
# ═══════════════════════════════════════════════

async def inline_query_handler(update: Update, context):
    """Inline mode: пользователь пишет @BotName → появляется «Вызвать на дуэль»."""
    query = update.inline_query
    results = [
        InlineQueryResultArticle(
            id="duel",
            title="⚔️ Вызвать на дуэль",
            description="Отправить вызов на библейский поединок!",
            input_message_content=InputTextMessageContent(
                message_text=(
                    "⚔️ *Вызов на библейскую дуэль!*\n\n"
                    "Кто лучше знает Первое послание Петра?\n\n"
                    "Нажми кнопку ниже, чтобы принять вызов!"
                ),
                parse_mode="Markdown",
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    "⚔️ Принять вызов!",
                    url=f"https://t.me/{context.bot.username}?start=battle_inline_{query.from_user.id}"
                )]
            ]),
        )
    ]
    await query.answer(results, cache_time=10)


# ═══════════════════════════════════════════════
# ADMIN ПАНЕЛЬ (задание 4.3)
# ═══════════════════════════════════════════════

async def help_command(update: Update, context):
    """Команда /help — справка по боту."""
    text = (
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
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=_main_keyboard())


async def admin_command(update: Update, context):
    """Команда /admin — только для администратора."""
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID:
        await update.message.reply_text("❌ У тебя нет доступа к этой команде.")
        return

    stats = get_admin_stats()
    text = (
        "🛡 *ПАНЕЛЬ АДМИНИСТРАТОРА*\n\n"
        f"👥 Всего пользователей: *{stats.get('total_users', 0)}*\n"
        f"🟢 Онлайн за 24ч: *{stats.get('online_24h', 0)}*\n"
        f"🆕 Новых сегодня: *{stats.get('new_today', 0)}*\n"
        f"💬 Активных сессий в памяти: *{len(user_data)}*\n"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 Сложные вопросы",  callback_data="admin_hard_questions")],
        [InlineKeyboardButton("👥 Активные сессии",   callback_data="admin_active_sessions")],
        [InlineKeyboardButton("🧹 Очистка данных",    callback_data="admin_cleanup")],
        [InlineKeyboardButton("📢 Рассылка",          callback_data="admin_broadcast_prompt")],
    ])
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=keyboard)


async def admin_callback_handler(update: Update, context):
    """Обрабатывает inline-кнопки admin-панели."""
    query   = update.callback_query
    user_id = query.from_user.id

    if user_id != ADMIN_USER_ID:
        await query.answer("❌ Нет доступа.", show_alert=True)
        return

    await query.answer()
    action = query.data

    if action == "admin_hard_questions":
        hard  = get_hardest_questions(limit=10)
        text  = "🔍 *Самые сложные вопросы (топ-10):*\n\n"
        for s in hard:
            attempts = s.get("total_attempts", 0)
            correct  = s.get("correct_attempts", 0)
            pct      = round(correct / max(attempts, 1) * 100)
            qid      = s.get("_id", "?")
            text     += f"• *{pct}%* верных ({correct}/{attempts}) — `{qid}`\n"
        await query.edit_message_text(
            text or "Статистика пока пуста.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="admin_back")]]),
        )

    elif action == "admin_active_sessions":
        lines = []
        for uid, data in list(user_data.items())[:20]:
            name   = data.get("first_name", "?")
            q_num  = data.get("current_question", 0)
            total  = len(data.get("questions", []))
            mode   = "⚔️" if data.get("is_battle") else ("🎲" if data.get("is_challenge") else "📖")
            lines.append(f"{mode} {name} ({uid}) — {q_num}/{total}")
        text = "👥 *Активные сессии в памяти:*\n\n" + ("\n".join(lines) if lines else "Пусто")
        await query.edit_message_text(
            text, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="admin_back")]]),
        )

    elif action == "admin_cleanup":
        deleted_battles = db_cleanup_stale_battles()
        now   = time.time()
        stale = [uid for uid, d in list(user_data.items())
                 if now - d.get("last_activity", now) > GC_STALE_THRESHOLD]
        for uid in stale:
            user_data.pop(uid, None)
        text = (
            f"🧹 *Очистка выполнена*\n\n"
            f"⚔️ Удалено устаревших битв: *{deleted_battles}*\n"
            f"🧠 Удалено записей user_data: *{len(stale)}*"
        )
        await query.edit_message_text(
            text, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="admin_back")]]),
        )

    elif action == "admin_broadcast_prompt":
        await query.edit_message_text(
            "📢 *Рассылка*\n\nОтправь команду:\n`/broadcast Текст сообщения`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="admin_back")]]),
        )

    elif action == "admin_back":
        stats = get_admin_stats()
        text = (
            "🛡 *ПАНЕЛЬ АДМИНИСТРАТОРА*\n\n"
            f"👥 Всего пользователей: *{stats.get('total_users', 0)}*\n"
            f"🟢 Онлайн за 24ч: *{stats.get('online_24h', 0)}*\n"
            f"🆕 Новых сегодня: *{stats.get('new_today', 0)}*\n"
            f"💬 Активных сессий в памяти: *{len(user_data)}*\n"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Сложные вопросы",  callback_data="admin_hard_questions")],
            [InlineKeyboardButton("👥 Активные сессии",   callback_data="admin_active_sessions")],
            [InlineKeyboardButton("🧹 Очистка данных",    callback_data="admin_cleanup")],
            [InlineKeyboardButton("📢 Рассылка",          callback_data="admin_broadcast_prompt")],
        ])
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=keyboard)


async def broadcast_command(update: Update, context):
    """Команда /broadcast Текст — рассылка всем пользователям."""
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID:
        await update.message.reply_text("❌ Нет доступа.")
        return

    text = update.message.text.replace("/broadcast", "", 1).strip()
    if not text:
        await update.message.reply_text("Использование: `/broadcast Текст сообщения`", parse_mode="Markdown")
        return

    all_ids = get_all_user_ids()
    sent = 0
    failed = 0
    status_msg = await update.message.reply_text(f"📢 Рассылка... 0/{len(all_ids)}")

    for i, uid in enumerate(all_ids):
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=f"📢 *Сообщение от автора бота:*\n\n{text}",
                parse_mode="Markdown",
            )
            sent += 1
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=f"📢 *Сообщение от автора бота:*\n\n{text}",
                    parse_mode="Markdown",
                )
                sent += 1
            except Exception:
                failed += 1
        except Exception:
            failed += 1
        # Обновляем статус каждые 20 пользователей
        if (i + 1) % 20 == 0:
            try:
                await status_msg.edit_text(f"📢 Рассылка... {i + 1}/{len(all_ids)}")
            except Exception:
                pass
        await asyncio.sleep(BROADCAST_SLEEP)  # ~28 msg/sec — в пределах лимита Telegram

    await status_msg.edit_text(
        f"✅ Рассылка завершена!\n"
        f"✉️ Отправлено: {sent}\n"
        f"❌ Ошибок: {failed}"
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
    battles_played  = entry.get("battles_played", 0)
    battles_won     = entry.get("battles_won", 0)

    text  = "📊 *МОЯ СТАТИСТИКА*\n\n"
    text += f"🏅 Позиция: *#{position}*\n"
    text += f"💎 Баллов: *{entry.get('total_points', 0)}*\n"
    text += f"📅 Дней в игре: *{days_playing}*\n"
    text += f"🎯 Тестов: *{total_tests}*\n"
    text += f"✅ Точность: *{calculate_accuracy(total_correct, total_questions)}%*\n"
    text += f"⏱ Среднее время: *{format_time(avg_time)}*\n\n"
    text += f"⚔️ Битв: *{battles_played}*, Побед: *{battles_won}*\n"
    if battles_played > 0:
        text += f"📈 Винрейт: *{round(battles_won / battles_played * 100)}%*\n"



    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎯 Начать тест",  callback_data="start_test")],
            [InlineKeyboardButton("🏅 Достижения",   callback_data="achievements")],
            [InlineKeyboardButton("📜 История",       callback_data="my_history")],
            [InlineKeyboardButton("⬅️ Назад",         callback_data="back_to_main")],
        ]),
        parse_mode="Markdown",
    )


async def show_history(update: Update, context):
    """Показывает историю последних 10 прохождений пользователя (6.3)."""
    query   = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    try:
        sessions = get_user_history(user_id, limit=10)
    except Exception:
        sessions = []

    if sessions:
        text = "📜 *ИСТОРИЯ ПРОХОЖДЕНИЙ*\n\n"
        for s in sessions:
            end_time = s.get("end_time")
            dt = end_time.strftime("%d.%m %H:%M") if hasattr(end_time, "strftime") else "—"
            score = s.get("correct_count", 0)
            total = s.get("total_questions", len(s.get("questions_data", [])))
            name  = s.get("level_name", "?")
            pct   = round(score / max(total, 1) * 100)
            text += f"• {dt} — _{name}_: *{score}/{total}* ({pct}%)\n"
    else:
        text = "📜 *ИСТОРИЯ*\n\nПока пусто — пройди первый тест!"

    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Назад", callback_data="my_stats")],
    ]))


async def show_general_leaderboard(query, page=0):
    users       = get_leaderboard_page(page)
    total_users = get_total_users()
    user_id     = query.from_user.id

    if not users:
        text = "🏆 *ТАБЛИЦА ЛИДЕРОВ*\n\nПока никто не проходил тесты."
    else:
        text = f"🏆 *ТАБЛИЦА ЛИДЕРОВ* (Стр. {page + 1})\n"
        start_rank = page * 10 + 1
        for i, entry in enumerate(users, start_rank):
            name  = entry.get("first_name", "Unknown")[:15]
            pts   = entry.get("total_points", 0)
            tests = entry.get("total_tests", 0)
            medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
            text += f"\n{medal} *{name}* — 💎{pts} • 🎯{tests}\n"

    position, my_entry = get_user_position(user_id)
    if my_entry and position:
        text += f"\n━━━━━━━━━━━━\n👤 *Ваше место:* #{position}"

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"leaderboard_page_{page-1}"))
    if (page + 1) * 10 < total_users:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"leaderboard_page_{page+1}"))

    keyboard = []
    if nav:
        keyboard.append(nav)
    keyboard.append([
        InlineKeyboardButton("🏛 Контекст", callback_data="cat_lb_context"),
        InlineKeyboardButton("🔴 Богословы", callback_data="cat_lb_hard"),
    ])
    keyboard.append([InlineKeyboardButton("⬅️ В меню", callback_data="back_to_main")])
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def show_category_leaderboard(query, category_key):
    CATEGORY_NAMES = {
        "easy": "🟢 Основы", "medium": "🟡 Контекст", "hard": "🔴 Богословие",
        "nero": "👑 Нерон", "geography": "🌍 География",
        "context": "🏛 Знатоки контекста",
    }
    cat_name = CATEGORY_NAMES.get(category_key, category_key)
    users = get_context_leaderboard() if category_key == "context" else get_category_leaderboard(category_key)

    if not users:
        text = f"{cat_name}\n\nПока никто не проходил этот тест."
    else:
        text = f"🏆 *РЕЙТИНГ: {cat_name}*\n\n"
        for i, entry in enumerate(users, 1):
            name  = entry.get("first_name", "?")[:15]
            medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
            if category_key == "context":
                text += f"{medal} *{name}* — {entry.get('_context_correct', 0)} верных\n"
            else:
                text += f"{medal} *{name}* — {entry.get(f'{category_key}_correct', 0)} верных\n"

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Общий рейтинг", callback_data="leaderboard")],
            [InlineKeyboardButton("⬅️ В меню",         callback_data="back_to_main")],
        ]),
        parse_mode="Markdown",
    )


async def category_leaderboard_handler(update: Update, context):
    query = update.callback_query
    await query.answer()
    await show_category_leaderboard(query, query.data.replace("cat_lb_", ""))


# ═══════════════════════════════════════════════
# RANDOM CHALLENGE
# ═══════════════════════════════════════════════

def build_progress_bar(current: int, total: int, answered_questions: list = None) -> str:
    """
    Цветной прогресс-бар:
    🟩 = правильный ответ  🟥 = неправильный  🟨 = текущий вопрос  ⬜ = не отвечал
    """
    bar = ""
    for i in range(total):
        if answered_questions and i < len(answered_questions):
            item = answered_questions[i]
            user_ans = item.get("user_answer", "")
            correct = _correct_text(item["question_obj"])
            bar += "🟩" if user_ans == correct else "🟥"
        elif i == current - 1:
            bar += "🟨"
        else:
            bar += "⬜"
    return bar


def pick_challenge_questions(mode):
    pool_ling = (get_pool_by_key("linguistics_ch1") +
                 get_pool_by_key("linguistics_ch1_2") +
                 get_pool_by_key("linguistics_ch1_3"))

    def safe_sample(pool, n):
        """Безопасная выборка — не падает на пустом или коротком пуле."""
        pool = list(pool)
        if not pool or n <= 0:
            return []
        return random.sample(pool, min(n, len(pool)))

    if mode == "random20":
        # Целевое: 6+5+6+1+1+1 = 20
        # Если INTRO_POOL пустой — компенсируем +1 из medium
        intro = safe_sample(INTRO_POOL, 1)
        medium_n = 5 if intro else 6
        questions = (safe_sample(get_pool_by_key("easy"),         6) +
                     safe_sample(get_pool_by_key("medium"),        medium_n) +
                     safe_sample(get_pool_by_key("hard"),          6) +
                     safe_sample(get_pool_by_key("practical_ch1"), 1) +
                     safe_sample(pool_ling,                        1) +
                     intro)
    else:
        # Целевое: 4+4+7+4+1 = 20
        # Если INTRO_POOL пустой — компенсируем +1 из hard
        intro = safe_sample(INTRO_POOL, 1)
        hard_n = 7 if intro else 8
        questions = (safe_sample(get_pool_by_key("easy"),   4) +
                     safe_sample(get_pool_by_key("medium"),  4) +
                     safe_sample(get_pool_by_key("hard"),    hard_n) +
                     safe_sample(pool_ling,                  4) +
                     intro)

    random.shuffle(questions)
    return questions


async def challenge_menu(update: Update, context):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    normal_ok   = is_bonus_eligible(user_id, "random20")
    hardcore_ok = is_bonus_eligible(user_id, "hardcore20")
    badge = lambda ok: "✅ доступен" if ok else "❌ уже получен"
    text = (
        "🎲 *RANDOM CHALLENGE (20)*\n\n"
        f"🎁 Бонус сегодня:\n"
        f"• 🎲 Normal:   {badge(normal_ok)}\n"
        f"• 💀 Hardcore: {badge(hardcore_ok)}\n\n"
        "Выбери режим:"
    )
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🎲 Normal (20) — без таймера", callback_data="challenge_rules_random20")],
            [InlineKeyboardButton("💀 Hardcore (20) — 10 сек",     callback_data="challenge_rules_hardcore20")],
            [InlineKeyboardButton("🏆 Лидерборд недели",          callback_data="weekly_lb_random20")],
            [InlineKeyboardButton("⬅️ Назад",                      callback_data="back_to_main")],
        ]),
        parse_mode="Markdown",
    )


async def challenge_rules(update: Update, context):
    query  = update.callback_query
    await query.answer()
    mode   = query.data.replace("challenge_rules_", "")
    user_id = query.from_user.id
    eligible = is_bonus_eligible(user_id, mode)
    today_status = "✅ доступен" if eligible else "❌ уже получен сегодня"
    title = "🎲 *Random Challenge (20)*" if mode == "random20" else "💀 *Hardcore Random (20)*"
    timer_info = "• без таймера" if mode == "random20" else "• ⏱ 10 сек на вопрос"
    await query.edit_message_text(
        f"{title}\n━━━━━━━━━━━━━━━━\n{timer_info}\n"
        f"*Статус бонуса:* {today_status}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Начать!", callback_data=f"challenge_start_{mode}")],
            [InlineKeyboardButton("⬅️ Назад",   callback_data="challenge_menu")],
        ]),
        parse_mode="Markdown",
    )


async def challenge_start(update: Update, context):
    query   = update.callback_query
    await query.answer()
    mode    = query.data.replace("challenge_start_", "")
    user_id = query.from_user.id
    _touch(user_id)
    eligible = is_bonus_eligible(user_id, mode)
    questions = pick_challenge_questions(mode)
    time_limit = 10 if mode == "hardcore20" else None
    mode_name  = "🎲 Random Challenge" if mode == "random20" else "💀 Hardcore Random"

    cancel_active_quiz_session(user_id)
    question_ids = [get_qid(q) for q in questions]
    session_id = create_quiz_session(
        user_id=user_id, mode=mode, question_ids=question_ids,
        questions_data=questions, level_key=mode, level_name=mode_name,
        time_limit=time_limit,
        chat_id=query.message.chat_id,
    )

    cfg = {"name": mode_name}
    chat_id = query.message.chat_id
    user_data[user_id] = _create_session_data(
        user_id=user_id,
        session_id=session_id,
        questions=questions,
        level_name=f"⚡ Challenge: {cfg['name']}",
        chat_id=chat_id,
        is_challenge=True,
        challenge_time_limit=time_limit,
        level_key=mode,
        correct_answers=0,
        start_time=time.time(),
        last_activity=time.time(),
        is_battle=False,
        battle_points=0,
        challenge_mode=mode,
        challenge_eligible=eligible,
        username=query.from_user.username,
        first_name=query.from_user.first_name or "Игрок",
    )

    await query.edit_message_text(
        f"{mode_name}\n\n📋 20 вопросов • {'✅ бонус доступен' if eligible else '❌ бонус уже получен'}\n\nПоехали! 💪",
        parse_mode="Markdown",
    )
    await send_challenge_question(context.bot, user_id)
    return ANSWERING


async def send_challenge_question(bot, user_id):
    """
    Отправляет или редактирует вопрос в challenge-режиме.
    bot — всегда telegram.Bot; chat_id берётся из user_data.
    """
    data  = user_data[user_id]
    q_num = data["current_question"]
    total = len(data["questions"])

    if q_num >= total:
        await show_challenge_results(bot, user_id)
        return

    chat_id = data.get("quiz_chat_id")
    if not chat_id:
        logger.warning("send_challenge_question: no chat_id for user %s", user_id)
        return

    q = data["questions"][q_num]
    correct_text = q["options"][q["correct"]]
    shuffled = q["options"][:]
    random.shuffle(shuffled)

    data["current_options"]      = shuffled
    data["current_correct_text"] = correct_text
    data["processing_answer"]    = False
    sent_at = time.time()
    data["question_sent_at"]     = sent_at

    old_task = data.get("timer_task")
    if old_task and not old_task.done():
        old_task.cancel()
    # Отменяем предыдущий визуальный countdown
    _cancel_countdown(user_id)

    session_id = data.get("session_id")
    if session_id:
        set_question_sent_at(session_id, sent_at)

    progress = build_progress_bar(q_num + 1, total, data.get("answered_questions", []))
    time_limit = data.get("challenge_time_limit")
    timer_str  = f" • ⏱ {time_limit} сек" if time_limit else ""

    # Всегда: варианты текстом, кнопки с цифрами
    options_text = "\n\n" + "\n".join(f"*{i+1}.* {opt}" for i, opt in enumerate(shuffled))
    buttons = [[InlineKeyboardButton(str(i + 1), callback_data=f"cha_{i}") for i in range(len(shuffled))]]

    buttons.append([
        InlineKeyboardButton("⚠️ Неточность?", callback_data=f"report_inaccuracy_{q_num}"),
        InlineKeyboardButton("↩️ выйти", callback_data="cancel_quiz"),
    ])
    keyboard = InlineKeyboardMarkup(buttons)
    text = (
        f"*Вопрос {q_num + 1}/{total}*{timer_str}\n{progress}\n\n"
        f"{q['question']}{options_text}"
    )

    quiz_message_id = data.get("quiz_message_id")
    quiz_chat_id    = data.get("quiz_chat_id")

    if quiz_message_id and quiz_chat_id:
        try:
            await bot.edit_message_text(
                chat_id=quiz_chat_id, message_id=quiz_message_id,
                text=text, reply_markup=keyboard, parse_mode="Markdown",
            )
        except Exception as e:
            err_str = str(e).lower()
            if "not modified" not in err_str:
                try:
                    msg = await bot.send_message(
                        chat_id=chat_id, text=text,
                        reply_markup=keyboard, parse_mode="Markdown",
                    )
                    data["quiz_message_id"] = msg.message_id
                    data["quiz_chat_id"]    = msg.chat.id
                except Exception as e2:
                    logger.error("send_challenge_question: fallback send_message failed for user %s: %s", user_id, e2)
                    return
    else:
        # Первый вопрос challenge — отправляем сразу без анимации
        try:
            await _typing_pause(bot, chat_id, has_timer=bool(time_limit))

            msg = await bot.send_message(
                chat_id=chat_id, text=text, reply_markup=keyboard, parse_mode="Markdown",
            )
            data["quiz_message_id"] = msg.message_id
            data["quiz_chat_id"]    = msg.chat.id
        except Exception as e:
            logger.error("send_challenge_question: send_message failed for user %s: %s", user_id, e)
            return

    if time_limit:
        data["timer_task"] = asyncio.create_task(
            challenge_timeout(bot, user_id, q_num)
        )
        # Запускаем живой визуальный countdown параллельно
        data["countdown_task"] = asyncio.create_task(
            _live_countdown(bot, chat_id, time_limit, user_id, q_num)
        )


async def challenge_timeout(bot, user_id, q_num_at_send):
    """Таймер для challenge-режима. Лимит читается из данных сессии."""
    data = user_data.get(user_id)
    timeout = data.get("challenge_time_limit", CHALLENGE_TIMEOUT) if data else CHALLENGE_TIMEOUT
    await _handle_question_timeout(bot, user_id, q_num_at_send, timeout)


async def challenge_answer(update: Update, context):
    """Fallback — для совместимости при текстовом вводе."""
    user_id = update.effective_user.id
    _touch(user_id)
    data    = user_data.get(user_id)

    if not data or not data.get("is_challenge"):
        db_session = get_active_quiz_session(user_id)
        if db_session and db_session.get("mode") in ("random20", "hardcore20"):
            if is_question_timed_out(db_session):
                await _handle_timeout_after_restart(update.message, user_id, db_session)
                return ANSWERING
            await _restore_session_to_memory(user_id, db_session)
            data = user_data.get(user_id)
        elif not data or not data.get("is_challenge"):
            return await answer(update, context)

    if data.get("processing_answer"):
        return ANSWERING

    await update.message.reply_text(
        "👆 Используй кнопки под вопросом для ответа.",
        reply_markup=_STUCK_KB,
    )
    return ANSWERING


async def challenge_inline_answer(update: Update, context):
    """Обрабатывает нажатие кнопки ответа в режиме Challenge."""
    await _handle_inline_answer(update, context, "cha")


async def show_challenge_results(bot, user_id):
    data       = user_data[user_id]
    score      = data["correct_answers"]
    total      = len(data["questions"])
    mode       = data["challenge_mode"]
    eligible   = data["challenge_eligible"]
    time_taken = time.time() - data["start_time"]
    chat_id    = data.get("quiz_chat_id")
    username   = data.get("username")
    first_name = data.get("first_name", "Игрок")

    session_id = data.get("session_id")
    if session_id:
        finish_quiz_session(session_id)

    # Анимация подсчёта
    anim_msg = await bot.send_message(chat_id=chat_id, text="📊 Подсчитываю результат…")
    for step in ("📊 Подсчитываю… ▰▱▱", "📊 Подсчитываю… ▰▰▱", "📊 Готово! ✨"):
        try:
            await asyncio.sleep(0.4)
            await anim_msg.edit_text(step)
        except Exception:
            pass

    points_per_q = 1 if mode == "random20" else 2
    earned_base  = score * points_per_q
    bonus        = compute_bonus(score, mode, eligible)
    total_earned = earned_base + bonus

    total_credited, new_achievements = update_challenge_stats(
        user_id, username, first_name,
        mode, score, total, time_taken, eligible
    )
    if eligible:
        update_weekly_leaderboard(user_id, username, first_name, mode, score, time_taken)

    pct = round(score / total * 100)
    grade = "🌟 Идеально!" if pct == 100 else "🔥 Отлично!" if pct >= 90 else "👍 Хорошо" if pct >= 75 else "📚 Нужно повторить"
    mode_name = "🎲 Random Challenge" if mode == "random20" else "💀 Hardcore Random"
    position, _ = get_user_position(user_id)

    result = (
        f"━━━━━━━━━━━━━━━━\n{mode_name}\n━━━━━━━━━━━━━━━━\n"
        f"📊 *{score}/{total}* ({pct}%) {grade}\n"
        f"⏱ Время: *{format_time(time_taken)}*\n"
        f"🏅 Позиция: *#{position}*\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💎 Очки: +{earned_base}"
    )
    if eligible:
        result += f"\n🎁 Бонус: *+{bonus}*\n✨ Итого: *+{total_earned}*"
    else:
        result += "\n🎁 Бонус: _недоступен_"

    if new_achievements:
        result += "\n━━━━━━━━━━━━━━━━\n🏅 *Новые достижения:*\n"
        for ach in new_achievements:
            result += f"  {ach}\n"
    result += "\n━━━━━━━━━━━━━━━━"

    answered = data.get("answered_questions", [])
    wrong = [i for i in answered if i["user_answer"] != i["question_obj"]["options"][i["question_obj"]["correct"]]]

    # Сохраняем ошибки и user_id для финального меню
    user_data[user_id]["wrong_answers"] = wrong
    user_data[user_id]["user_id"] = user_id

    # Основные результаты — без кнопок (будут в финальном меню)
    await bot.send_message(
        chat_id=chat_id, text=result,
        parse_mode="Markdown",
    )

    # Картинка результатов: GIF → PNG → тихий fallback
    try:
        rank_name = get_rank_name(pct)
        time_taken_ch = time.time() - data.get("start_time", time.time())

        gif_buf = await create_result_gif(
            score=score,
            total=total,
            rank_name=rank_name,
            time_seconds=time_taken_ch,
            first_name=first_name,
        )
        if gif_buf:
            await bot.send_animation(
                chat_id=chat_id,
                animation=InputFile(gif_buf, filename="result.gif"),
                caption=f"🏆 {score}/{total} • {rank_name}",
            )
        else:
            # Fallback: статичный PNG
            img_bytes = await generate_result_image(
                bot=bot,
                user_id=user_id,
                first_name=first_name,
                score=score, total=total, rank_name=rank_name,
            )
            if img_bytes:
                bio = io.BytesIO(img_bytes)
                bio.name = "result.png"
                bio.seek(0)
                await bot.send_photo(
                    chat_id=chat_id,
                    photo=InputFile(bio, filename="result.png"),
                    caption=f"🏆 {score}/{total} • {rank_name}",
                )
    except Exception as e:
        logger.error("Challenge result image error", exc_info=True)

    if not wrong:
        await bot.send_message(chat_id=chat_id, text="🎯 *Все ответы верны!*", parse_mode="Markdown")

    # Проверяем и выдаём достижения
    await check_and_award_achievements(bot, user_id, data)

    # Финальное меню — всегда внизу чата, легко найти
    await asyncio.sleep(0.5)
    await send_final_results_menu(bot, chat_id, data)


# ═══════════════════════════════════════════════
# ДОСТИЖЕНИЯ И ЕЖЕНЕДЕЛЬНЫЙ ЛИДЕРБОРД
# ═══════════════════════════════════════════════


async def check_and_award_achievements(bot, user_id: int, data: dict) -> list:
    """Проверяет условия достижений и выдаёт награды. Возвращает список новых ключей."""
    chat_id = data.get("quiz_chat_id")
    if not chat_id:
        return []

    user_doc          = get_user_stats(user_id) or {}
    user_achievements = user_doc.get("achievements", {})

    answered  = data.get("answered_questions", [])
    questions = data.get("questions", [])
    total     = len(questions)
    score     = sum(1 for item in answered
                    if item.get("user_answer") == _correct_text(item["question_obj"]))
    is_perfect  = total > 0 and score == total
    max_streak  = data.get("max_streak", 0)

    # Обновляем статистику и получаем актуальные значения
    stats = update_achievement_stats(user_id, is_perfect, max_streak)
    stats["total_tests"] = user_doc.get("total_tests", 0) + 1  # текущий тест ещё не записан

    new_achievements = []

    # Первые шаги
    if "first_steps" not in user_achievements and stats["total_tests"] >= 1:
        new_achievements.append("first_steps")

    # Перфекционист (уровни)
    pc = stats.get("perfect_count", 0)
    for key, threshold in [("perfectionist_1", 1), ("perfectionist_2", 5), ("perfectionist_3", 15)]:
        if key not in user_achievements and pc >= threshold:
            new_achievements.append(key)

    # Огненная серия (уровни)
    ms = stats.get("max_streak_ever", 0)
    for key, threshold in [("streak_5", 5), ("streak_10", 10), ("streak_20", 20)]:
        if key not in user_achievements and ms >= threshold:
            new_achievements.append(key)

    # Марафонец (уровни)
    tt = stats.get("total_tests", 0)
    for key, threshold in [("marathoner_10", 10), ("marathoner_50", 50), ("marathoner_100", 100)]:
        if key not in user_achievements and tt >= threshold:
            new_achievements.append(key)

    # Ежедневная активность
    ds = stats.get("daily_streak", 0)
    for key, threshold in [("daily_streak_7", 7), ("daily_streak_30", 30)]:
        if key not in user_achievements and ds >= threshold:
            new_achievements.append(key)

    # Молния
    if "lightning" not in user_achievements:
        if data.get("fastest_answer", 9999) <= 3 and data.get("quiz_mode") == "speed":
            new_achievements.append("lightning")

    if new_achievements:
        now_str      = datetime.now().strftime("%d.%m.%Y")
        ach_update   = {f"achievements.{k}": now_str for k in new_achievements}
        total_reward = sum(ACHIEVEMENTS[k]["reward"] for k in new_achievements)
        try:
            collection.update_one(
                {"_id": str(user_id)},
                {"$set": ach_update, "$inc": {"total_points": total_reward}},
                upsert=True,
            )
        except Exception as e:
            logger.error("check_and_award_achievements DB error: %s", e)

        for ach_key in new_achievements:
            ach = ACHIEVEMENTS[ach_key]
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"🏆 *ДОСТИЖЕНИЕ РАЗБЛОКИРОВАНО!*\n\n"
                        f"{ach['icon']} *{ach['name']}*\n"
                        f"_{ach['description']}_\n\n"
                        f"🎁 Награда: *+{ach['reward']} баллов*"
                    ),
                    parse_mode="Markdown",
                )
                await asyncio.sleep(1.5)
            except Exception as e:
                logger.warning("check_and_award_achievements: send failed: %s", e)

    return new_achievements

async def show_achievements(update: Update, context):
    """Показывает все достижения с прогрессом и статистикой."""
    query   = update.callback_query
    await query.answer()
    user_id    = query.from_user.id
    user_stats = get_user_stats(user_id) or {}
    achievements = user_stats.get("achievements", {})

    perfect_count = user_stats.get("perfect_count", 0)
    max_streak    = user_stats.get("max_streak_ever", 0)
    total_tests   = user_stats.get("total_tests", 0)
    daily_streak  = user_stats.get("daily_activity_streak", 0)

    text     = "🏅 *МОИ ДОСТИЖЕНИЯ*\n━━━━━━━━━━━━━━━━\n\n"
    unlocked = 0

    for key, ach in ACHIEVEMENTS.items():
        if key in achievements:
            unlocked += 1
            text += f"✅ {ach['icon']} *{ach['name']}*\n"
            text += f"   _{ach['description']}_\n"
            text += f"   📅 {achievements[key]}\n\n"
        else:
            req      = ach.get("requirement", {})
            progress = ""
            if "perfect_count" in req:
                progress = f" ({perfect_count}/{req['perfect_count']})"
            elif "max_streak" in req:
                progress = f" ({max_streak}/{req['max_streak']})"
            elif "total_tests" in req:
                progress = f" ({total_tests}/{req['total_tests']})"
            elif "daily_streak" in req:
                progress = f" ({daily_streak}/{req['daily_streak']})"
            text += f"🔒 {ach['icon']} *{ach['name']}*{progress}\n"
            text += f"   _{ach['description']}_\n"
            text += f"   🎁 +{ach['reward']} баллов\n\n"

    text += (
        f"━━━━━━━━━━━━━━━━\n"
        f"✅ Разблокировано: {unlocked}/{len(ACHIEVEMENTS)}\n"
        f"📊 Тестов пройдено: {total_tests}\n"
        f"💎 Идеальных тестов: {perfect_count}\n"
        f"🔥 Лучшая серия: {max_streak}\n"
        f"📅 Дней подряд: {daily_streak}"
    )

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]]),
        parse_mode="Markdown",
    )

async def show_weekly_leaderboard(update: Update, context):
    query  = update.callback_query
    await query.answer()
    mode   = query.data.replace("weekly_lb_", "")
    users  = get_weekly_leaderboard(mode)
    mode_name = "🎲 Random Challenge" if mode == "random20" else "💀 Hardcore Random"
    week_id   = get_current_week_id()

    if not users:
        text = f"🏆 *{mode_name}*\nНеделя {week_id}\n\nПока нет результатов."
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
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"→ {other_mode_name}", callback_data=f"weekly_lb_{other_mode}")],
            [InlineKeyboardButton("🎲 Сыграть",  callback_data=f"challenge_rules_{mode}")],
            [InlineKeyboardButton("⬅️ Назад",    callback_data="challenge_menu")],
        ]),
        parse_mode="Markdown",
    )


# ═══════════════════════════════════════════════
# КОМАНДЫ
# ═══════════════════════════════════════════════

async def test_command(update: Update, context):
    await choose_level(update, context, is_callback=False)
    return CHOOSING_LEVEL


async def cancel_quiz_handler(update: Update, context):
    """Обработчик кнопки ❌ Выйти в меню — отменяет тест и показывает главное меню."""
    query   = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    # Отменяем таймер, если есть
    data = user_data.get(user_id, {})
    timer_task = data.get("timer_task")
    if timer_task and not timer_task.done():
        timer_task.cancel()
    # Отменяем живой countdown
    _cancel_countdown(user_id)

    # Отменяем сессию в БД и чистим память
    cancel_active_quiz_session(user_id)
    user_data.pop(user_id, None)

    # Редактируем тот же «пузырь» с вопросом
    await query.edit_message_text(
        "❌ *Тест отменён.* Выбери действие:",
        reply_markup=_main_keyboard(),
        parse_mode="Markdown",
    )
    return ConversationHandler.END


async def cancel(update: Update, context):
    user_id = update.effective_user.id
    cancel_active_quiz_session(user_id)
    user_data.pop(user_id, None)
    await update.message.reply_text("❌ Отменено.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END


async def reset_command(update: Update, context):
    user_id = update.effective_user.id
    cancel_active_quiz_session(user_id)
    user_data.pop(user_id, None)
    await update.message.reply_text("🆘 Тест сброшен.", reply_markup=ReplyKeyboardRemove())
    await update.message.reply_text("📖 *Главное меню*", reply_markup=_main_keyboard(), parse_mode="Markdown")
    return ConversationHandler.END


async def status_command(update: Update, context):
    user_id = update.effective_user.id
    session = get_active_quiz_session(user_id)
    mem = user_data.get(user_id)
    if not session and not mem:
        await update.message.reply_text("📌 Нет активного теста.", reply_markup=_main_keyboard())
        return
    if session:
        total_q = len(session.get("questions_data", []))
        current = session.get("current_index", 0)
        level = session.get("level_name", "?")
        sid = session["_id"]
    else:
        total_q = len(mem.get("questions", []))
        current = mem.get("current_question", 0)
        level = mem.get("level_name", "?")
        sid = mem.get("session_id", "")
    text = f"📌 *Активный тест*\nРежим: _{level}_\nВопрос: *{current + 1}/{total_q}*"
    await update.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Продолжить", callback_data=f"resume_session_{sid}")],
            [InlineKeyboardButton("🆘 Сбросить",   callback_data="reset_session")],
        ]) if sid else None,
        parse_mode="Markdown",
    )


async def reset_session_inline(update: Update, context):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    cancel_active_quiz_session(user_id)
    user_data.pop(user_id, None)
    try:
        await query.message.reply_text("✅", reply_markup=ReplyKeyboardRemove())
    except Exception:
        pass
    await safe_edit(query, "🆘 Тест сброшен.", reply_markup=_main_keyboard())


async def show_status_inline(update: Update, context):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    session = get_active_quiz_session(user_id)
    mem = user_data.get(user_id)
    if not session and not mem:
        await safe_edit(query, "📌 *Статус:* нет активного теста", reply_markup=_main_keyboard())
        return
    if session:
        total_q = len(session.get("questions_data", []))
        current = session.get("current_index", 0)
        level = session.get("level_name", "?")
        sid = session["_id"]
    else:
        total_q = len(mem.get("questions", []))
        current = mem.get("current_question", 0)
        level = mem.get("level_name", "?")
        sid = mem.get("session_id", "")
    await safe_edit(
        query,
        f"📌 *Активный тест*\nРежим: _{level}_\nВопрос: *{current + 1}/{total_q}*",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Продолжить", callback_data=f"resume_session_{sid}")],
            [InlineKeyboardButton("🆘 Сбросить",   callback_data="reset_session")],
            [InlineKeyboardButton("⬅️ Меню",        callback_data="back_to_main")],
        ]),
    )


# ═══════════════════════════════════════════════
# ОБЩИЙ BUTTON HANDLER
# ═══════════════════════════════════════════════

async def button_handler(update: Update, context):
    query = update.callback_query
    if await _debounce_callback(update):
        return
    await query.answer()
    _touch(query.from_user.id)

    if query.data.startswith("leaderboard_page_"):
        page = int(query.data.replace("leaderboard_page_", ""))
        await show_general_leaderboard(query, page)
        return

    dispatch = {
        "about":         lambda: query.edit_message_text(
            "📚 *БИБЛЕЙСКИЙ ТЕСТ-БОТ: 1 ПЕТРА*\n"
            "Интерактивный инструмент для глубокого изучения Писания.\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🎯 *ЦЕЛЬ ПРОЕКТА*\n"
            "Погружение в контекст, язык и богословие Первого послания Петра — "
            "не просто проверка памяти, а осмысленное изучение текста.\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🧩 *СТРУКТУРА ТЕСТОВ*\n\n"
            "📖 *Глава 1* — по частям (ст. 1–16 и ст. 17–25)\n"
            "• 🟢 Лёгкий (1 балл) — факты, имена, даты\n"
            "• 🟡 Средний (2 балла) — контекст и связи\n"
            "• 🔴 Сложный (3 балла) — богословие и толкование\n"
            "• 🙏 Применение (2 балла) — практика и жизнь\n\n"
            "🔬 *Лингвистика* (3 балла за вопрос)\n"
            "• Ч.1 — Избранные и странники\n"
            "• Ч.2 — Живая надежда\n"
            "• Ч.3 — Искупление и истина (ст. 17–25)\n\n"
            "🏛 *Исторический контекст* (2 балла)\n"
            "• 📜 Введение: авторство (ч.1 и ч.2)\n"
            "• 📜 Введение: структура и цель\n"
            "• 👑 Правление Нерона\n"
            "• 🌍 География провинций\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "⚡️ *РЕЖИМЫ ПРОХОЖДЕНИЯ*\n\n"
            "🧘 *Спокойный* — без таймера, баллы ×1.0\n"
            "⏱ *На время* — 30 сек, баллы ×1.5\n"
            "⚡ *Скоростной* — 15 сек, баллы ×2.0\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🎮 *ДОПОЛНИТЕЛЬНЫЕ РЕЖИМЫ*\n\n"
            "🎲 *Random Challenge (20)* — случайные вопросы из всех категорий, бонус раз в день\n"
            "💀 *Hardcore Challenge (20)* — 10 сек на вопрос, повышенная сложность\n"
            "⚔️ *PvP Битва* — соревнование с другим игроком в реальном времени\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "📊 *СИСТЕМА ПРОГРЕССА*\n\n"
            "💎 *Баллы* — зависят от сложности и режима\n"
            "🏆 *Достижения* — разблокируй награды за успехи\n"
            "🔥 *Серии* — бонусы за правильные ответы подряд\n"
            "📅 *Ежедневный бонус* — награда за первый тест дня\n"
            "🏅 *Лидерборд* — общий и по категориям\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "📖 *ПОСЛЕ ТЕСТА*\n\n"
            "✅ Просмотр всех ответов с разбором\n"
            "🔄 Пересдача только ошибок\n"
            "📤 Поделиться результатом с друзьями\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🏆 *ДОСТИЖЕНИЯ*\n\n"
            "⭐ Первые шаги — пройди первый тест\n"
            "💎 Перфекционист I/II/III — 100% в тестах\n"
            "🔥 Огненная серия — 5/10/20 правильных подряд\n"
            "🏃 Марафонец — пройди 10/50/100 тестов\n"
            "⚡ Молния — ответь за 3 сек\n"
            "📅 Неделя знаний — 7 дней подряд\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "📬 *ОБРАТНАЯ СВЯЗЬ*\n\n"
            "Нашёл ошибку в вопросе? Нажми \"⚠️ Неточность\" прямо во время теста.\n\n"
            "_v4.0 • Soli Deo Gloria_",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_main")]]),
            parse_mode="Markdown",
        ),
        "start_test":    lambda: choose_level(update, context, is_callback=True),
        "battle_menu":   lambda: show_battle_menu(query),
        "leaderboard":   lambda: show_general_leaderboard(query, 0),
        "my_stats":      lambda: show_my_stats(query),
        "historical_menu": lambda: historical_menu(update, context),
        "challenge_menu":  lambda: challenge_menu(update, context),
        "achievements":    lambda: show_achievements(update, context),
        "my_status":       lambda: show_status_inline(update, context),
        "reset_session":   lambda: reset_session_inline(update, context),
        "coming_soon":     lambda: query.answer("🚧 В разработке!", show_alert=True),
    }

    handler = dispatch.get(query.data)
    if handler:
        await handler()


# ═══════════════════════════════════════════════
# СИСТЕМА РЕПОРТОВ
# ═══════════════════════════════════════════════

async def report_menu(update: Update, context):
    query = update.callback_query
    await query.answer()
    await safe_edit(
        query,
        "✉️ *Написать автору*\n\nВыбери тип сообщения:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🐞 Сообщить о баге",     callback_data="report_start_bug")],
            [InlineKeyboardButton("💡 Предложение",          callback_data="report_start_idea")],
            [InlineKeyboardButton("❓ Вопрос по материалу",  callback_data="report_start_question")],
            [InlineKeyboardButton("⬅️ Назад",                callback_data="back_to_main")],
        ]),
    )


async def report_start(update: Update, context):
    query = update.callback_query
    await query.answer()
    report_type = query.data.replace("report_start_", "")
    if report_type == "bug_direct":
        report_type = "bug"
    user_id = query.from_user.id

    if not can_submit_report(user_id):
        remaining = seconds_until_next_report(user_id)
        await query.answer(f"⏳ Слишком часто. Попробуй через {remaining} сек.", show_alert=True)
        return

    report_drafts[user_id] = {"type": report_type, "text": None, "photo_file_id": None}
    label = REPORT_TYPE_LABELS.get(report_type, report_type)
    await safe_edit(query, f"{label}\n\n✏️ Напиши своё сообщение.\n\nДля отмены: /cancelreport")
    return REPORT_TEXT


async def report_receive_text(update: Update, context):
    user_id = update.effective_user.id
    if user_id not in report_drafts:
        return ConversationHandler.END
    text = sanitize_report_text(update.message.text.strip())
    if not text:
        await safe_send(update.message, "Пожалуйста, напиши текст.")
        return REPORT_TEXT
    report_drafts[user_id]["text"] = text
    await safe_send(
        update.message,
        "📎 Хочешь приложить скриншот?\n\nПришли *фото* или нажми кнопку ниже.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➡️ Пропустить", callback_data="report_skip_photo")],
            [InlineKeyboardButton("❌ Отмена",      callback_data="report_cancel")],
        ]),
    )
    return REPORT_PHOTO


async def report_receive_photo(update: Update, context):
    user_id = update.effective_user.id
    if user_id not in report_drafts:
        return ConversationHandler.END
    if update.message.photo:
        report_drafts[user_id]["photo_file_id"] = update.message.photo[-1].file_id
    draft = report_drafts[user_id]
    label = REPORT_TYPE_LABELS.get(draft["type"], draft["type"])
    has_photo = "✅ фото приложено" if draft.get("photo_file_id") else "нет фото"
    await safe_send(
        update.message,
        f"📋 *Подтверждение*\n\nТип: {label}\nТекст: _{draft['text'][:200]}_\nФото: {has_photo}\n\nОтправить?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Отправить", callback_data="report_confirm")],
            [InlineKeyboardButton("❌ Отмена",    callback_data="report_cancel")],
        ]),
    )
    return REPORT_CONFIRM


async def report_skip_photo(update: Update, context):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if user_id not in report_drafts:
        return ConversationHandler.END
    draft = report_drafts[user_id]
    label = REPORT_TYPE_LABELS.get(draft["type"], draft["type"])
    await safe_edit(
        query,
        f"📋 *Подтверждение*\n\nТип: {label}\nТекст: _{draft['text'][:200]}_\nФото: нет\n\nОтправить?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Отправить", callback_data="report_confirm")],
            [InlineKeyboardButton("❌ Отмена",    callback_data="report_cancel")],
        ]),
    )
    return REPORT_CONFIRM


async def report_confirm(update: Update, context):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user = query.from_user

    if user_id not in report_drafts:
        await safe_edit(query, "⚠️ Данные устарели. Начни заново.", reply_markup=_main_keyboard())
        return ConversationHandler.END

    draft = report_drafts.pop(user_id)
    ctx = {}
    mem = user_data.get(user_id)
    if mem:
        ctx = {"mode": mem.get("level_key"), "level": mem.get("level_name"), "q": mem.get("current_question")}

    label = REPORT_TYPE_LABELS.get(draft["type"], draft["type"])
    uname_plain = user.username if user.username else f"id={user_id}"
    uname_link = f"@{user.username}" if user.username else f"id={user_id}"
    ctx_str = ", ".join(f"{k}={v}" for k, v in ctx.items() if v is not None) or "нет"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    admin_card = f"{label}\nОт: {uname_plain} (id={user_id})\nВремя: {ts}\nКонтекст: {ctx_str}\n\n{draft['text'][:1500]}"

    # Сохраняем в MongoDB и обновляем кулдаун
    report_id = insert_report(
        user_id=user_id,
        username=user.username,
        first_name=user.first_name,
        report_type=draft["type"],
        text=draft["text"] or "",
        context=ctx,
    )

    admin_delivered = False
    try:
        if draft.get("photo_file_id"):
            await context.bot.send_photo(
                chat_id=ADMIN_USER_ID,
                photo=draft["photo_file_id"],
                caption=f"{label} от {uname_link} • {ts}",
                parse_mode="Markdown",
            )
        await context.bot.send_message(chat_id=ADMIN_USER_ID, text=safe_truncate(admin_card))
        admin_delivered = True
        if report_id:
            mark_report_delivered(report_id)
    except Exception as e:
        logger.error("[REPORT] Could not deliver to admin: %s", e)

    msg = "✅ *Спасибо! Сообщение отправлено автору.*" if admin_delivered else "⚠️ Не удалось доставить прямо сейчас."
    await safe_edit(query, msg, reply_markup=_main_keyboard())
    return ConversationHandler.END


async def report_cancel(update: Update, context):
    query = update.callback_query
    await query.answer()
    report_drafts.pop(query.from_user.id, None)
    await safe_edit(query, "❌ Репорт отменён.", reply_markup=_main_keyboard())
    return ConversationHandler.END


async def cancel_report_command(update: Update, context):
    user_id = update.effective_user.id
    report_drafts.pop(user_id, None)
    await update.message.reply_text("❌ Репорт отменён.", reply_markup=ReplyKeyboardRemove())
    await update.message.reply_text("Главное меню:", reply_markup=_main_keyboard())
    return ConversationHandler.END


# ═══════════════════════════════════════════════
# FALLBACK + JOB QUEUE TASKS
# ═══════════════════════════════════════════════

async def _general_message_fallback(update: Update, context):
    """
    Резервный обработчик текстовых сообщений.
    Удаляет случайный текст пользователя, переотправляет вопрос/меню вниз.
    """
    user_id = update.effective_user.id
    is_private = update.effective_chat.type == "private"

    # Удаляем сообщение пользователя — только в приватных чатах
    if is_private:
        try:
            await update.message.delete()
        except Exception:
            pass

    # Если идёт обычный тест или challenge (inline-кнопки)
    if user_id in user_data:
        data = user_data[user_id]
        if data.get("is_battle"):
            return  # битвы используют текстовый ввод — не трогаем

        # Удаляем старый пузырь с вопросом, чтобы переотправить вниз
        old_mid = data.get("quiz_message_id")
        old_cid = data.get("quiz_chat_id")
        if old_mid and old_cid:
            try:
                await context.bot.delete_message(chat_id=old_cid, message_id=old_mid)
            except Exception:
                pass
            data["quiz_message_id"] = None  # сбрасываем, чтобы send_question отправил новым

        # Переотправляем вопрос вниз
        if data.get("is_challenge"):
            await send_challenge_question(context.bot, user_id)
        else:
            await send_question(context.bot, user_id)
        return

    # Нет сессии в памяти — пробуем восстановить из БД
    db_session = get_active_quiz_session(user_id)
    if db_session:
        mode = db_session.get("mode", "level")
        await _restore_session_to_memory(user_id, db_session)

        if user_id in user_data:
            user_data[user_id]["quiz_chat_id"]  = update.message.chat_id
            user_data[user_id]["username"]      = update.effective_user.username
            user_data[user_id]["first_name"]    = update.effective_user.first_name or "Игрок"

        if is_question_timed_out(db_session):
            await _handle_timeout_after_restart(update.message, user_id, db_session)
            return

        if mode in ("random20", "hardcore20"):
            await send_challenge_question(context.bot, user_id)
        else:
            await send_question(context.bot, user_id)
        return

    # Совсем нет сессии — отправляем главное меню вниз
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="📖 *Главное меню*\n\nВыбери действие:",
        reply_markup=_main_keyboard(),
        parse_mode="Markdown",
    )


async def cleanup_old_battles_job(context):
    """JobQueue: удаляет устаревшие битвы из MongoDB."""
    deleted = db_cleanup_stale_battles()
    if deleted:
        logger.info("🧹 Удалено устаревших битв: %d", deleted)


async def cleanup_stale_userdata_job(context):
    """
    JobQueue (каждый час): удаляет из user_data записи с активностью >24ч.
    Реализует требование задания 2.1.
    """
    now = time.time()
    stale = [
        uid for uid, data in list(user_data.items())
        if now - data.get("last_activity", now) > GC_STALE_THRESHOLD
    ]
    for uid in stale:
        user_data.pop(uid, None)
    if stale:
        logger.info("🧹 GC: удалено %d устаревших записей user_data", len(stale))


async def remind_unfinished_tests_job(context):
    """JobQueue (каждые 2 часа): напоминает о брошенных сессиях (6.4)."""
    from database import get_stale_sessions
    try:
        stale = get_stale_sessions(max_age_hours=2)
    except Exception:
        return  # функция может быть не реализована — молча пропускаем
    for session in stale:
        uid = session.get("user_id")
        if not uid:
            continue
        try:
            await context.bot.send_message(
                chat_id=int(uid),
                text="📝 *У тебя есть незавершённый тест!*\n\nПродолжить с того места, где остановился?",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("▶️ Продолжить",
                        callback_data=f"resume_session_{session['_id']}")],
                    [InlineKeyboardButton("❌ Отменить",
                        callback_data=f"cancel_session_{session['_id']}")],
                ]),
            )
        except Exception:
            pass


# ─────────────────────────────────────────────
# GRACEFUL SHUTDOWN (6.6)
# ─────────────────────────────────────────────
import signal

async def _save_all_sessions():
    """Сохраняет все in-memory сессии в MongoDB при корректной остановке."""
    saved = 0
    for uid, data in list(user_data.items()):
        sid = data.get("session_id")
        if sid:
            try:
                update_quiz_session(sid, {
                    "current_index": data.get("current_question", 0),
                    "correct_count": data.get("correct_answers", 0),
                })
                saved += 1
            except Exception:
                pass
    logger.info("💾 Graceful shutdown: сохранено %d сессий", saved)


def _handle_shutdown(signum, frame):
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_save_all_sessions())
    else:
        loop.run_until_complete(_save_all_sessions())


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT,  _handle_shutdown)


async def on_error(update: object, context):
    """Глобальный обработчик ошибок."""
    import traceback

    err = context.error

    # 1. Фильтруем сетевые ошибки (Render часто рвет соединение, это норма)
    if isinstance(err, (NetworkError, TimedOut)):
        logger.debug("Network noise ignored: %s", err)
        return

    # 2. Фильтруем RetryAfter — Telegram просит подождать, не спамим
    if isinstance(err, RetryAfter):
        logger.warning("RetryAfter: retry in %ss", err.retry_after)
        return

    # 3. Фильтруем "Message is not modified" (юзер жмет кнопку дважды)
    if isinstance(err, BadRequest) and "not modified" in str(err).lower():
        return

    # 4. Фильтруем ChatMigrated (группа перешла в супергруппу)
    if isinstance(err, ChatMigrated):
        logger.info("ChatMigrated: new_chat_id=%s", err.new_chat_id)
        return

    tb = "".join(traceback.format_exception(type(err), err, err.__traceback__))
    logger.error("Unhandled exception:\n%s", tb)

    # 5. Фоновые ошибки (polling/getUpdates) — нет реального пользователя.
    #    Только логируем, не беспокоим ни пользователя, ни админа.
    if not (isinstance(update, Update) and update.effective_user):
        return

    # Реальная ошибка с пользователем — уведомляем его и админа
    try:
        msg_target = (update.message or
                      (update.callback_query.message if update.callback_query else None))
        if msg_target:
            await msg_target.reply_text(
                "⚠️ Произошла ошибка. Нажми /reset или сообщи автору.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🆘 Сброс",     callback_data="reset_session"),
                     InlineKeyboardButton("🐞 Сообщить",  callback_data="report_start_bug_direct")],
                ]),
            )
    except Exception:
        pass

    try:
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=safe_truncate(f"🚨 ОШИБКА\n\n{tb[:1500]}"),
        )
    except Exception:
        pass


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
            CHOOSING_LEVEL:   [CallbackQueryHandler(level_selected)],
            ANSWERING:        [
                CallbackQueryHandler(cancel_quiz_handler, pattern="^cancel_quiz$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, challenge_answer),
            ],
            BATTLE_ANSWERING: [
                CallbackQueryHandler(cancel_quiz_handler, pattern="^cancel_quiz$"),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(cancel_quiz_handler, pattern="^cancel_quiz$"),
            CallbackQueryHandler(back_to_main, pattern="^back_to_main$"),
        ],
        allow_reentry=True,
    )
    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("start", start))

    app.add_handler(CallbackQueryHandler(report_inaccuracy_handler, pattern=r"^report_inaccuracy_"))

    # Inline-ответы на вопросы (основной тест, challenge и битвы)
    app.add_handler(CallbackQueryHandler(quiz_inline_answer,       pattern=r"^qa_\d+$"))
    app.add_handler(CallbackQueryHandler(challenge_inline_answer,  pattern=r"^cha_\d+$"))
    app.add_handler(CallbackQueryHandler(battle_answer,            pattern=r"^ba_\d+$"))
    app.add_handler(CallbackQueryHandler(cancel_quiz_handler,      pattern="^cancel_quiz$"))
    # Подтверждение уровня перед стартом (4.5)
    app.add_handler(CallbackQueryHandler(confirm_level_handler,    pattern=r"^confirm_level_"))
    app.add_handler(CallbackQueryHandler(relaxed_mode_handler,    pattern=r"^relaxed_mode_"))
    app.add_handler(CallbackQueryHandler(timed_mode_handler,      pattern=r"^timed_mode_"))
    app.add_handler(CallbackQueryHandler(speed_mode_handler,      pattern=r"^speed_mode_"))

    # Session recovery
    app.add_handler(CallbackQueryHandler(resume_session_handler,  pattern="^resume_session_"))
    app.add_handler(CallbackQueryHandler(restart_session_handler, pattern="^restart_session_"))
    app.add_handler(CallbackQueryHandler(cancel_session_handler,  pattern="^cancel_session_"))

    # Команды
    app.add_handler(CommandHandler("reset",       reset_command))
    app.add_handler(CommandHandler("status",      status_command))
    app.add_handler(CommandHandler("cancelreport", cancel_report_command))
    app.add_handler(CommandHandler("admin",        admin_command))
    app.add_handler(CommandHandler("broadcast",    broadcast_command))
    app.add_handler(CommandHandler("help",         help_command))  # 6.1

    # Admin inline-панель (6.5)
    app.add_handler(CallbackQueryHandler(
        admin_callback_handler,
        pattern=r"^admin_(hard_questions|active_sessions|cleanup|broadcast_prompt|back)$",
    ))

    # Репорты
    report_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(report_start, pattern="^report_start_")],
        states={
            REPORT_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, report_receive_text)],
            REPORT_PHOTO: [
                MessageHandler(filters.PHOTO, report_receive_photo),
                CallbackQueryHandler(report_skip_photo, pattern="^report_skip_photo$"),
                CallbackQueryHandler(report_cancel,     pattern="^report_cancel$"),
            ],
            REPORT_CONFIRM: [
                CallbackQueryHandler(report_confirm, pattern="^report_confirm$"),
                CallbackQueryHandler(report_cancel,  pattern="^report_cancel$"),
            ],
        },
        fallbacks=[
            CommandHandler("cancelreport", cancel_report_command),
            CommandHandler("reset",        reset_command),
        ],
        allow_reentry=True,
    )
    app.add_handler(report_conv)

    # Битвы
    app.add_handler(CallbackQueryHandler(create_battle,  pattern="^create_battle$"))
    app.add_handler(CallbackQueryHandler(join_battle,    pattern="^join_battle_"))
    app.add_handler(CallbackQueryHandler(cancel_battle,  pattern="^cancel_battle_"))

    # Inline mode (задание 4.1)
    app.add_handler(InlineQueryHandler(inline_query_handler))

    # Общие кнопки
    app.add_handler(CallbackQueryHandler(chapter_1_menu,   pattern="^chapter_1_menu$"))
    app.add_handler(CallbackQueryHandler(historical_menu,  pattern="^historical_menu$"))
    app.add_handler(CallbackQueryHandler(intro_hint_handler,  pattern=r"^intro_hint_"))
    app.add_handler(CallbackQueryHandler(intro_start_handler, pattern=r"^intro_start_"))
    app.add_handler(CallbackQueryHandler(random_fact_handler, pattern="^random_fact_intro$"))
    app.add_handler(CallbackQueryHandler(report_menu,      pattern="^report_menu$"))
    app.add_handler(CallbackQueryHandler(challenge_rules,  pattern="^challenge_rules_"))
    app.add_handler(CallbackQueryHandler(show_weekly_leaderboard, pattern="^weekly_lb_"))
    app.add_handler(CallbackQueryHandler(category_leaderboard_handler, pattern="^cat_lb_"))
    app.add_handler(CallbackQueryHandler(back_to_main,     pattern="^back_to_main$"))
    app.add_handler(CallbackQueryHandler(user_settings_handler,     pattern="^user_settings$"))
    app.add_handler(CallbackQueryHandler(toggle_typewriter_handler, pattern="^toggle_typewriter$"))
    app.add_handler(CallbackQueryHandler(
        button_handler,
        pattern=r"^(about|start_test|battle_menu|leaderboard|my_stats|leaderboard_page_\d+|"
                r"historical_menu|coming_soon|challenge_menu|achievements|my_status|reset_session)$",
    ))

    # История прохождений (6.3)
    app.add_handler(CallbackQueryHandler(show_history, pattern="^my_history$"))

    # Разбор ошибок (пагинация)
    app.add_handler(CallbackQueryHandler(review_errors_handler, pattern=r"^review_errors_"))
    app.add_handler(CallbackQueryHandler(review_errors_handler, pattern=r"^review_nav_"))
    app.add_handler(CallbackQueryHandler(review_test_handler,   pattern=r"^review_test_\d+$"))
    app.add_handler(CallbackQueryHandler(noop_handler,           pattern="^noop$"))

    # Fallback для сообщений (восстановление после рестарта)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _general_message_fallback))

    # JobQueue
    if app.job_queue is not None:
        app.job_queue.run_repeating(cleanup_old_battles_job,    interval=BATTLE_CLEANUP_INTERVAL, first=BATTLE_CLEANUP_INTERVAL)
        app.job_queue.run_repeating(cleanup_stale_userdata_job, interval=GC_INTERVAL,             first=GC_INTERVAL)
        app.job_queue.run_repeating(remind_unfinished_tests_job, interval=7200, first=7200)  # 6.4
        logger.info("🧹 Автоочистка активна (битвы + user_data GC + напоминания)")
    else:
        logger.warning("JobQueue недоступен — автоочистка отключена")

    app.add_error_handler(on_error)

    logger.info("🤖 Бот запущен! (v4.0)")
    logger.info("🛡 Admin ID: %s", ADMIN_USER_ID)
    app.run_polling()


if __name__ == "__main__":
    main()
