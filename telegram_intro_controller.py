# ruff: noqa: RUF001
"""Catalog-backed Telegram presentation for legacy Introduction callbacks."""
from __future__ import annotations

import random

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from course_catalog import (
    SURFACE_TELEGRAM,
    CourseCatalogError,
    resolve_course,
    resolve_course_pool,
)

_INTRO_COURSE_KEYS = (
    "level_intro1",
    "level_intro2",
    "level_intro3",
)


async def _show_unavailable(query) -> None:
    try:
        await query.answer("Курс устарел или сейчас недоступен.", show_alert=True)
    except Exception:
        pass
    try:
        await query.edit_message_text(
            "⚠️ Этот учебный модуль больше недоступен. Обнови меню.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data="historical_menu")]]
            ),
        )
    except Exception:
        pass


def _resolve_intro_course(course_key: str):
    if course_key not in _INTRO_COURSE_KEYS:
        raise CourseCatalogError(f"unsupported introduction course: {course_key!r}")
    entry = resolve_course(course_key, surface=SURFACE_TELEGRAM)
    pool = resolve_course_pool(entry)
    return entry, pool


async def intro_hint_handler(update, context) -> None:
    """Show three facts for one canonical Introduction course."""
    del context
    query = update.callback_query
    data = str(query.data or "")
    prefix = "intro_hint_"
    if not data.startswith(prefix):
        await _show_unavailable(query)
        return

    course_key = data[len(prefix) :]
    try:
        entry, pool = _resolve_intro_course(course_key)
    except (CourseCatalogError, KeyError):
        await _show_unavailable(query)
        return

    await query.answer()
    sample = random.sample(pool, min(3, len(pool))) if pool else []
    facts = [f"💡 _{question['explanation']}_" for question in sample]
    hint_text = (
        f"📖 *Справка: {entry.title}*\n\n" + "\n\n".join(facts)
        if facts
        else "Нет данных."
    )

    await query.edit_message_text(
        hint_text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "▶️ Начать тест",
                        callback_data=f"intro_start_{course_key}",
                    )
                ],
                [InlineKeyboardButton("⬅️ Назад", callback_data="historical_menu")],
            ]
        ),
    )


async def random_fact_handler(update, context) -> None:
    """Show one fact sampled from all available canonical Introduction courses."""
    del context
    query = update.callback_query

    all_intro = []
    for course_key in _INTRO_COURSE_KEYS:
        try:
            _, pool = _resolve_intro_course(course_key)
        except (CourseCatalogError, KeyError):
            continue
        all_intro.extend(pool)

    if not all_intro:
        await _show_unavailable(query)
        return

    await query.answer()
    fact = random.choice(all_intro)["explanation"]
    await query.edit_message_text(
        f"🎲 *А вы знали?*\n\n_{fact}_",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🎲 Ещё факт",
                        callback_data="random_fact_intro",
                    )
                ],
                [InlineKeyboardButton("⬅️ Назад", callback_data="historical_menu")],
            ]
        ),
    )
