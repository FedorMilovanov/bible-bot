"""Production achievement presentation backed by the canonical catalog."""
from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from achievement_catalog import ACHIEVEMENTS, validate_achievement_catalog
from database import get_user_stats, touch_user_activity


def _achievement_catalog() -> Mapping[str, Mapping]:
    """Return the validated canonical achievement catalog authority."""
    return validate_achievement_catalog(ACHIEVEMENTS)


def _touch_memory(user_data, user_id: int) -> None:
    """Preserve the process-local activity timestamp without doing DB I/O."""
    if not isinstance(user_data, dict):
        return
    entry = user_data.get(user_id)
    if isinstance(entry, dict):
        entry["last_activity"] = time.time()


async def show_achievements(update, context, *, user_data):
    """Render achievements without running Mongo work on the PTB event loop."""
    del context
    query = update.callback_query
    user_id = query.from_user.id
    catalog = _achievement_catalog()

    await query.answer()
    _touch_memory(user_data, user_id)
    await asyncio.to_thread(touch_user_activity, user_id)
    user_stats = await asyncio.to_thread(get_user_stats, user_id) or {}
    unlocked_achievements = user_stats.get("achievements", {})

    perfect_count = user_stats.get("perfect_count", 0)
    max_streak = user_stats.get("max_streak_ever", 0)
    total_tests = user_stats.get("total_tests", 0)
    daily_streak = user_stats.get("daily_activity_streak", 0)

    text = "🏅 *МОИ ДОСТИЖЕНИЯ*\n━━━━━━━━━━━━━━━━\n\n"
    unlocked = 0

    for key, achievement in catalog.items():
        if key in unlocked_achievements:
            unlocked += 1
            text += f"✅ {achievement['icon']} *{achievement['name']}*\n"
            text += f"   _{achievement['description']}_\n"
            text += f"   📅 {unlocked_achievements[key]}\n\n"
            continue

        requirement = achievement.get("requirement", {})
        progress = ""
        if "perfect_count" in requirement:
            progress = f" ({perfect_count}/{requirement['perfect_count']})"
        elif "max_streak" in requirement:
            progress = f" ({max_streak}/{requirement['max_streak']})"
        elif "total_tests" in requirement:
            progress = f" ({total_tests}/{requirement['total_tests']})"
        elif "daily_streak" in requirement:
            progress = f" ({daily_streak}/{requirement['daily_streak']})"

        text += f"🔒 {achievement['icon']} *{achievement['name']}*{progress}\n"
        text += f"   _{achievement['description']}_\n"
        text += f"   🎁 +{achievement['reward']} баллов\n\n"

    text += (
        "━━━━━━━━━━━━━━━━\n"
        f"✅ Разблокировано: {unlocked}/{len(catalog)}\n"
        f"📊 Тестов пройдено: {total_tests}\n"
        f"💎 Идеальных тестов: {perfect_count}\n"
        f"🔥 Лучшая серия: {max_streak}\n"
        f"📅 Дней подряд: {daily_streak}"
    )

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]]
        ),
        parse_mode="Markdown",
    )
