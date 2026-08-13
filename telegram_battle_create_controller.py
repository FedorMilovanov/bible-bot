# ruff: noqa: RUF001
"""Replay-safe production adapter for creating durable shared PvP battles."""
from __future__ import annotations

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
            creator_name=user.first_name or "\u0418\u0433\u0440\u043e\u043a",
            questions=questions,
        )
        return created, True
    except (LegacyBattleSessionUnavailable, LegacyBattleSessionConflict) as exc:
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
        battle, _created = create_or_recover_battle(update, user)
    except ValueError as exc:
        if "questions" in str(exc):
            await query.answer("\u26a0\ufe0f \u0412\u043e\u043f\u0440\u043e\u0441\u044b \u0434\u043b\u044f \u0431\u0438\u0442\u0432\u044b \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u044b.", show_alert=True)
        else:
            await query.answer("\u26a0\ufe0f \u0417\u0430\u043f\u0440\u043e\u0441 \u043d\u0430 \u0441\u043e\u0437\u0434\u0430\u043d\u0438\u0435 \u0431\u0438\u0442\u0432\u044b \u043f\u043e\u0432\u0440\u0435\u0436\u0434\u0451\u043d.", show_alert=True)
        return
    except (LegacyBattleSessionUnavailable, LegacyBattleSessionConflict):
        logger.warning("replay-safe battle creation failed for user %s", user.id, exc_info=True)
        await query.answer("\u26a0\ufe0f \u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0441\u043e\u0437\u0434\u0430\u0442\u044c \u0431\u0438\u0442\u0432\u0443. \u041f\u043e\u043f\u0440\u043e\u0431\u0443\u0439 \u0435\u0449\u0451 \u0440\u0430\u0437.", show_alert=True)
        return

    battle_id = battle["_id"]
    rows = []
    try:
        share_url = sharing.build_battle_share_url(
            context.bot.username,
            battle_id,
            str(battle.get("creator_name") or user.first_name or "\u0418\u0433\u0440\u043e\u043a"),
        )
    except ValueError:
        logger.info("battle share URL is unavailable", exc_info=True)
    else:
        rows.append([InlineKeyboardButton("\U0001f4e4 \u041f\u043e\u0434\u0435\u043b\u0438\u0442\u044c\u0441\u044f \u0432\u044b\u0437\u043e\u0432\u043e\u043c", url=share_url)])
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    "\u274c \u041e\u0442\u043c\u0435\u043d\u0438\u0442\u044c \u043e\u0436\u0438\u0434\u0430\u043d\u0438\u0435",
                    callback_data=battles._cancel_payload(battle_id),
                )
            ],
            [InlineKeyboardButton("\u2b05\ufe0f \u041a \u0431\u0438\u0442\u0432\u0430\u043c", callback_data="battle_menu")],
        ]
    )

    await query.answer()
    await query.edit_message_text(
        "\u2694\ufe0f *\u0411\u0418\u0422\u0412\u0410 \u0421\u041e\u0417\u0414\u0410\u041d\u0410!*\n\n"
        "\U0001f4e4 \u041e\u0442\u043f\u0440\u0430\u0432\u044c \u0442\u043e\u0447\u043d\u0443\u044e \u0441\u0441\u044b\u043b\u043a\u0443 \u0441\u043e\u043f\u0435\u0440\u043d\u0438\u043a\u0443 \u0438\u043b\u0438 \u0434\u043e\u0436\u0434\u0438\u0441\u044c \u0438\u0433\u0440\u043e\u043a\u0430 \u0438\u0437 \u043e\u0431\u0449\u0435\u0433\u043e \u0441\u043f\u0438\u0441\u043a\u0430.\n"
        "\u23f3 \u041f\u043e\u0441\u043b\u0435 \u043f\u0440\u0438\u0441\u043e\u0435\u0434\u0438\u043d\u0435\u043d\u0438\u044f \u0431\u043e\u0442 \u043f\u0440\u0438\u0448\u043b\u0451\u0442 \u043e\u0431\u043e\u0438\u043c \u043a\u043d\u043e\u043f\u043a\u0443 Start.\n\n"
        "_\u041d\u0435\u0437\u0430\u043f\u0443\u0449\u0435\u043d\u043d\u0430\u044f \u0431\u0438\u0442\u0432\u0430 \u0430\u0432\u0442\u043e\u043c\u0430\u0442\u0438\u0447\u0435\u0441\u043a\u0438 \u043e\u0447\u0438\u0449\u0430\u0435\u0442\u0441\u044f \u043f\u043e\u0441\u043b\u0435 \u043e\u043a\u043d\u0430 \u043e\u0436\u0438\u0434\u0430\u043d\u0438\u044f._",
        reply_markup=InlineKeyboardMarkup(rows),
        parse_mode="Markdown",
    )
