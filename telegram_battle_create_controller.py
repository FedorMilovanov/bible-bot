# ruff: noqa: RUF001
"""Replay-safe production adapter for creating durable shared PvP battles."""
from __future__ import annotations

import asyncio
import logging
import random

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

import telegram_battle_controller as battles
import telegram_battle_share_controller as sharing
from legacy_battle_session import (
    LegacyBattleSessionConflict,
    LegacyBattleSessionUnavailable,
    create_durable_battle,
    get_owned_open_durable_battle,
)
from questions import BATTLE_POOL

logger = logging.getLogger(__name__)
_MAX_UPDATE_ID = (1 << 64) - 1


def battle_id_for_update(update_id: int) -> str:
    """Map one Telegram callback update to one exact 16-hex durable battle id."""
    if (
        isinstance(update_id, bool)
        or not isinstance(update_id, int)
        or update_id < 0
        or update_id > _MAX_UPDATE_ID
    ):
        raise ValueError("Telegram update id is outside the durable battle id range")
    return f"battle_{update_id:016x}"


def _owned_created_battle(battle_id: str, creator_id: int) -> dict | None:
    battle = get_owned_open_durable_battle(battle_id, creator_id)
    if battle is None:
        return None
    if battle.get("creator_id") != creator_id:
        raise LegacyBattleSessionConflict(
            "deterministic battle id is not owned by the creating user"
        )
    return battle


def _competitive_battle_questions() -> list[dict]:
    """Return exactly 10 questions from the ranking-eligible bank."""
    pool = list(BATTLE_POOL)
    if len(pool) < 10:
        raise ValueError("battle questions are unavailable")
    return random.sample(pool, 10)


def create_or_recover_battle(update, user) -> tuple[dict, bool]:
    """Create once by update id or recover an already accepted/ambiguous write."""
    battle_id = battle_id_for_update(update.update_id)
    existing = _owned_created_battle(battle_id, user.id)
    if existing is not None:
        return existing, False

    questions = _competitive_battle_questions()
    try:
        created = create_durable_battle(
            battle_id=battle_id,
            creator_id=user.id,
            creator_name=user.first_name or "Игрок",
            questions=questions,
        )
        return created, True
    except (LegacyBattleSessionUnavailable, LegacyBattleSessionConflict) as exc:
        # Mongo may have committed the deterministic insert even if the client
        # lost its acknowledgement, or this exact Telegram update may be replayed.
        # Read back only the exact id owned by this creator; never issue a second
        # random-id create attempt.
        try:
            recovered = _owned_created_battle(battle_id, user.id)
        except LegacyBattleSessionUnavailable as lookup_exc:
            raise exc from lookup_exc
        if recovered is None:
            raise exc from None
        return recovered, False


async def create_battle(update, context):
    """Create/recover one durable battle and render a share picker for its exact id."""
    query = update.callback_query
    user = query.from_user
    try:
        battle, _created = await asyncio.to_thread(create_or_recover_battle, update, user)
    except ValueError as exc:
        if "questions" in str(exc):
            await query.answer("⚠️ Вопросы для битвы не найдены.", show_alert=True)
        else:
            await query.answer("⚠️ Запрос на создание битвы повреждён.", show_alert=True)
        return
    except (LegacyBattleSessionUnavailable, LegacyBattleSessionConflict):
        logger.warning("replay-safe battle creation failed for user %s", user.id, exc_info=True)
        await query.answer("⚠️ Не удалось создать битву. Попробуй ещё раз.", show_alert=True)
        return

    battle_id = battle["_id"]
    rows = []
    try:
        share_url = sharing.build_battle_share_url(
            context.bot.username,
            battle_id,
            str(battle.get("creator_name") or user.first_name or "Игрок"),
        )
    except ValueError:
        logger.info("battle share URL is unavailable", exc_info=True)
    else:
        rows.append([InlineKeyboardButton("📤 Поделиться вызовом", url=share_url)])
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    "❌ Отменить ожидание",
                    callback_data=battles._cancel_payload(battle_id),
                )
            ],
            [InlineKeyboardButton("⬅️ К битвам", callback_data="battle_menu")],
        ]
    )

    await query.answer()
    await query.edit_message_text(
        "⚔️ *БИТВА СОЗДАНА!*\n\n"
        "📤 Отправь точную ссылку сопернику или дождись игрока из общего списка.\n"
        "⏳ После присоединения бот пришлёт обоим кнопку Start.\n\n"
        "_Незапущенная битва автоматически очищается после окна ожидания._",
        reply_markup=InlineKeyboardMarkup(rows),
        parse_mode="Markdown",
    )
