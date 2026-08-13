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
            [InlineKeyboardButton("Hard questions", callback_data="admin_hard_questions")],
            [InlineKeyboardButton("Active sessions", callback_data="admin_active_sessions")],
            [InlineKeyboardButton("Cleanup", callback_data="admin_cleanup")],
            [InlineKeyboardButton("Broadcast", callback_data="admin_broadcast_prompt")],
        ]
    )


def _admin_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Back", callback_data="admin_back")]]
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
    lines = ["Hardest questions (top 10):"]
    for index, item in enumerate(rows[:10], start=1):
        if not isinstance(item, dict):
            continue
        question = str(
            item.get("question")
            or item.get("question_text")
            or item.get("_id")
            or "Question"
        ).replace("\n", " ")[:140]
        attempts = item.get("total_attempts", 0)
        correct = item.get("correct_attempts", item.get("correct_count", 0))
        lines.append(f"{index}. {question}\nAttempts: {attempts}; correct: {correct}")
    if len(lines) == 1:
        lines.append("No statistics yet.")
    return "\n\n".join(lines)


def _active_sessions_text() -> str:
    lines = [f"Active in-memory sessions: {len(legacy.user_data)}"]
    for user_id, data in list(legacy.user_data.items())[:20]:
        if not isinstance(data, dict):
            lines.append(f"{user_id}: malformed record")
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
        await query.answer("Access denied.", show_alert=True)
        return

    action = query.data
    if action not in _ADMIN_READ_ACTIONS:
        await query.answer("Unsupported action.", show_alert=True)
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
            "Broadcast\n\nSend: /broadcast MESSAGE",
            reply_markup=_admin_back_keyboard(),
        )
        return

    await query.edit_message_text(
        "Admin panel",
        reply_markup=_admin_menu_keyboard(),
    )


async def admin_cleanup(update, context):
    """Run only recovery-safe durable cleanup, then clear stale process-local RAM."""
    del context
    query = update.callback_query
    user_id = query.from_user.id
    if user_id != legacy.ADMIN_USER_ID:
        await query.answer("Access denied.", show_alert=True)
        return

    try:
        deleted_battles = cleanup_stale_waiting_battles(max_age_minutes=10)
    except LegacyBattleCleanupUnavailable:
        await query.answer("Battle storage is temporarily unavailable.", show_alert=True)
        return

    now = time.time()
    stale = _stale_ram_users(now=now)
    for stale_user_id in stale:
        legacy.user_data.pop(stale_user_id, None)

    await query.answer()
    await query.edit_message_text(
        "Cleanup completed\n\n"
        f"Safely deleted pre-progress battles: {deleted_battles}\n"
        f"Removed user_data records: {len(stale)}",
        reply_markup=_admin_back_keyboard(),
    )
