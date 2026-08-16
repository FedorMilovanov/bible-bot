"""Idempotent production callbacks for process-local presentation settings."""
from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


_USER_PREFS: dict[int, dict[str, bool]] = {}


def get_pref(user_id: int, key: str, default: bool = True) -> bool:
    """Read one process-local preference with the historical default semantics."""
    return _USER_PREFS.get(user_id, {}).get(key, default)


def set_pref(user_id: int, key: str, value: bool) -> None:
    """Set one process-local preference until process restart."""
    _USER_PREFS.setdefault(user_id, {})[key] = bool(value)


def _settings_keyboard(user_id: int) -> InlineKeyboardMarkup:
    current = bool(get_pref(user_id, "typewriter", default=True))
    desired = not current
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"⌨️ Печатная машинка: {'✅ вкл' if current else '❌ выкл'}",
                    callback_data=f"typewriter_set:{1 if desired else 0}",
                )
            ],
            [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")],
        ]
    )


async def _render_settings(query, *, user_id: int, changed: bool = False) -> None:
    current = bool(get_pref(user_id, "typewriter", default=True))
    if changed:
        state = "включена ✅" if current else "выключена ❌"
        text = (
            f"⚙️ *НАСТРОЙКИ*\n\n"
            f"⌨️ Печатная машинка {state}\n\n"
            "_Настройки сохраняются до перезапуска бота._"
        )
    else:
        text = (
            "⚙️ *НАСТРОЙКИ*\n\n"
            "Здесь можно включить или выключить визуальные эффекты.\n"
            "_Настройки сохраняются до перезапуска бота._"
        )
    await query.edit_message_text(
        text,
        reply_markup=_settings_keyboard(user_id),
        parse_mode="Markdown",
    )


async def user_settings_handler(update, context):
    """Render the settings menu using target-specific callback payloads."""
    del context
    query = update.callback_query
    if query is None or query.from_user is None:
        return
    await query.answer()
    await _render_settings(query, user_id=query.from_user.id)


async def set_typewriter_handler(update, context):
    """Set, rather than toggle, the local typewriter preference."""
    del context
    query = update.callback_query
    if query is None or query.from_user is None:
        return
    data = query.data or ""
    if data not in {"typewriter_set:0", "typewriter_set:1"}:
        await query.answer()
        return
    desired = data == "typewriter_set:1"
    set_pref(query.from_user.id, "typewriter", desired)
    await query.answer()
    await _render_settings(query, user_id=query.from_user.id, changed=True)


async def legacy_toggle_upgrade_handler(update, context):
    """Upgrade an old toggle button to the idempotent menu without toggling."""
    del context
    query = update.callback_query
    if query is None or query.from_user is None:
        return
    await query.answer()
    await _render_settings(query, user_id=query.from_user.id)
