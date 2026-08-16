"""Production achievement presentation with explicit legacy catalog injection."""
from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from database import get_user_stats, touch_user_activity

_REQUIRED_ACHIEVEMENT_FIELDS = {"name", "icon", "description", "reward"}


def _achievement_catalog(legacy_module) -> Mapping[str, Mapping]:
    """Resolve and validate the single existing achievement catalog authority."""
    catalog = getattr(legacy_module, "ACHIEVEMENTS", None)
    if not isinstance(catalog, Mapping) or not catalog:
        raise RuntimeError("Legacy achievement catalog is unavailable")

    for key, achievement in catalog.items():
        if not isinstance(key, str) or not key:
            raise RuntimeError("Achievement catalog contains an invalid key")
        if not isinstance(achievement, Mapping):
            raise RuntimeError(f"Achievement {key!r} is not a mapping")
        missing = _REQUIRED_ACHIEVEMENT_FIELDS - set(achievement)
        if missing:
            raise RuntimeError(
                f"Achievement {key!r} is missing required fields: {sorted(missing)}"
            )
        if not isinstance(achievement["reward"], int) or achievement["reward"] < 0:
            raise RuntimeError(f"Achievement {key!r} has an invalid reward")
    return catalog


def _touch_legacy_memory(legacy_module, user_id: int) -> None:
    """Preserve the legacy in-memory activity timestamp without doing DB I/O."""
    user_data = getattr(legacy_module, "user_data", None)
    if not isinstance(user_data, dict):
        return
    entry = user_data.get(user_id)
    if isinstance(entry, dict):
        entry["last_activity"] = time.time()


async def show_achievements(update, context, *, legacy_module):
    """Render achievements without running Mongo work on the PTB event loop."""
    del context
    query = update.callback_query
    user_id = query.from_user.id
    catalog = _achievement_catalog(legacy_module)

    await query.answer()
    _touch_legacy_memory(legacy_module, user_id)
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
