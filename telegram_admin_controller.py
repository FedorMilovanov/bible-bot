"""Production-only admin actions that must respect durable state authority."""
from __future__ import annotations

import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

import bot as legacy
from legacy_battle_cleanup import (
    LegacyBattleCleanupUnavailable,
    cleanup_stale_waiting_battles,
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
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Назад", callback_data="admin_back")]]
        ),
    )
