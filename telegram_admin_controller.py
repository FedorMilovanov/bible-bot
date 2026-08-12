"""Production-only admin actions that must respect durable state authority."""
from __future__ import annotations

import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

import bot as legacy
from legacy_battle_cleanup import (
    LegacyBattleCleanupUnavailable,
    cleanup_stale_waiting_battles,
)


_ADMIN_READ_ACTIONS = {
    "admin_hard_questions",
    "admin_active_sessions",
    "admin_broadcast_prompt",
    "admin_back",
}


def _admin_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔍 Сложные вопросы", callback_data="admin_hard_questions")],
            [InlineKeyboardButton("👥 Активные сессии", callback_data="admin_active_sessions")],
            [InlineKeyboardButton("🧹 Очистка данных", callback_data="admin_cleanup")],
            [InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast_prompt")],
        ]
    )


def _admin_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅️ Назад", callback_data="admin_back")]]
    )


def _stale_ram_users(*, now: float) -> list[int]:
    stale: list[int] = []
    for user_id, data in list(legacy.user_data.items()):
        if not isinstance(data, dict):
            stale.append(user_id)
            continue
        last_activity = data.get("last_activity", now)
        if isinstance(last_activity, bool) or not isinstance(last_activity, (int, float)):
            stale.append(user_id)
            continue
        if now - float(last_activity) > legacy.GC_STALE_THRESHOLD:
            stale.append(user_id)
    return stale


def _hard_questions_text() -> str:
    rows = legacy.get_hardest_questions(limit=10) or []
    if not isinstance(rows, list):
        rows = list(rows)
    lines = ["Самые сложные вопросы (топ-10):"]
    for index, item in enumerate(rows[:10], start=1):
        if not isinstance(item, dict):
            continue
        question = str(
            item.get("question")
            or item.get("question_text")
            or item.get("_id")
            or "Вопрос"
        ).replace("\n", " ")[:140]
        attempts = item.get("total_attempts", 0)
        correct = item.get("correct_attempts", item.get("correct_count", 0))
        lines.append(f"{index}. {question}\nПопыток: {attempts}; верных: {correct}")
    if len(lines) == 1:
        lines.append("Статистика пока пуста.")
    return "\n\n".join(lines)


def _active_sessions_text() -> str:
    lines = [f"Активных сессий в памяти: {len(legacy.user_data)}"]
    for user_id, data in list(legacy.user_data.items())[:20]:
        if not isinstance(data, dict):
            lines.append(f"{user_id}: поврежденная запись")
            continue
        name = str(data.get("first_name", "?"))[:60]
        current = data.get("current_question", 0)
        questions = data.get("questions", [])
        total = len(questions) if isinstance(questions, list) else 0
        lines.append(f"{user_id} | {name} | {current}/{total}")
    return "\n".join(lines)


async def admin_read_callback(update, context):
    """Serve only read/presentation admin callbacks; destructive cleanup is separate."""
    del context
    query = update.callback_query
    user_id = query.from_user.id
    if user_id != legacy.ADMIN_USER_ID:
        await query.answer("Нет доступа.", show_alert=True)
        return

    action = query.data
    if action not in _ADMIN_READ_ACTIONS:
        await query.answer("Недопустимое действие.", show_alert=True)
        return

    await query.answer()
    if action == "admin_hard_questions":
        await query.edit_message_text(
            _hard_questions_text(),
            reply_markup=_admin_back_keyboard(),
        )
        return

    if action == "admin_active_sessions":
        await query.edit_message_text(
            _active_sessions_text(),
            reply_markup=_admin_back_keyboard(),
        )
        return

    if action == "admin_broadcast_prompt":
        await query.edit_message_text(
            "Рассылка\n\nОтправь команду: /broadcast Текст сообщения",
            reply_markup=_admin_back_keyboard(),
        )
        return

    await query.edit_message_text(
        "Админ-панель",
        reply_markup=_admin_menu_keyboard(),
    )


async def admin_cleanup(update, context):
    """Run only recovery-safe durable cleanup, then clear stale process-local RAM."""
    del context
    query = update.callback_query
    user_id = query.from_user.id
    if user_id != legacy.ADMIN_USER_ID:
        await query.answer("Нет доступа.", show_alert=True)
        return

    try:
        deleted_battles = cleanup_stale_waiting_battles(max_age_minutes=10)
    except LegacyBattleCleanupUnavailable:
        await query.answer("База битв временно недоступна.", show_alert=True)
        return

    now = time.time()
    stale = _stale_ram_users(now=now)
    for stale_user_id in stale:
        legacy.user_data.pop(stale_user_id, None)

    await query.answer()
    await query.edit_message_text(
        "🧹 *Очистка выполнена*\n\n"
        f"⚔️ Безопасно удалено pre-progress битв: *{deleted_battles}*\n"
        f"🧠 Удалено записей user_data: *{len(stale)}*",
        parse_mode="Markdown",
        reply_markup=_admin_back_keyboard(),
    )
