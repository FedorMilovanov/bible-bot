# ruff: noqa: RUF001
"""Durable PvP sharing and Telegram start-deep-link adapter."""
from __future__ import annotations

import asyncio
import logging
import re
import uuid
from urllib.parse import urlencode

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

import telegram_battle_controller as battles
import telegram_battle_ready_delivery as ready_delivery
from legacy_battle_session import (
    LegacyBattleSessionConflict,
    LegacyBattleSessionUnavailable,
    claim_durable_battle_opponent,
    create_durable_battle,
)

logger = logging.getLogger(__name__)
_DEEP_LINK_PREFIX = "duel_"
_BATTLE_ID_RE = re.compile(r"battle_[0-9a-f]{16}\Z")


def _deep_link_payload(battle_id: str) -> str:
    if not isinstance(battle_id, str) or _BATTLE_ID_RE.fullmatch(battle_id) is None:
        raise ValueError("invalid durable battle id")
    payload = f"{_DEEP_LINK_PREFIX}{battle_id}"
    if len(payload.encode("utf-8")) > 64:
        raise ValueError("battle deep-link payload exceeds Telegram limit")
    return payload


def _bot_username(value: str | None) -> str:
    if not isinstance(value, str):
        raise ValueError("bot username is unavailable")
    username = value.strip().lstrip("@")
    if not username or not re.fullmatch(r"[A-Za-z0-9_]+", username):
        raise ValueError("bot username is invalid")
    return username


def build_battle_deep_link(bot_username: str, battle_id: str) -> str:
    username = _bot_username(bot_username)
    return f"https://t.me/{username}?start={_deep_link_payload(battle_id)}"


def build_battle_share_url(
    bot_username: str,
    battle_id: str,
    creator_name: str,
) -> str:
    deep_link = build_battle_deep_link(bot_username, battle_id)
    name = str(creator_name or "Игрок").strip() or "Игрок"
    query = urlencode(
        {
            "url": deep_link,
            "text": f"⚔️ {name} вызывает тебя на битву знаний!",
        }
    )
    return f"https://t.me/share/url?{query}"


def parse_battle_deep_link(args) -> str | None:
    """Return exact durable battle id, or None when /start is unrelated."""
    if not args:
        return None
    if not isinstance(args, (list, tuple)):
        raise ValueError("start arguments are invalid")
    first = args[0]
    if not isinstance(first, str) or not first.startswith(_DEEP_LINK_PREFIX):
        return None
    if len(args) != 1:
        raise ValueError("battle deep link has unexpected arguments")
    battle_id = first[len(_DEEP_LINK_PREFIX) :]
    if _BATTLE_ID_RE.fullmatch(battle_id) is None:
        raise ValueError("battle deep link is malformed")
    return battle_id


def _opponent_start_markup(battle_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "▶️ Начать",
                    callback_data=battles._start_payload(battle_id, "opponent"),
                )
            ],
            [InlineKeyboardButton("⬅️ К битвам", callback_data="battle_menu")],
        ]
    )


async def _notify_creator_ready(bot, battle: dict, opponent_name: str) -> None:
    del opponent_name
    battle_id = battle.get("_id") if isinstance(battle, dict) else None
    if not isinstance(battle_id, str) or not battle_id:
        logger.warning("creator durable-share notification has no battle id")
        return
    try:
        await ready_delivery.deliver_creator_ready_once(
            bot,
            battle_id,
            start_payload_builder=battles._start_payload,
        )
    except Exception:
        # The opponent claim already staged the durable marker. A transient
        # failure remains recoverable by battle_maintenance_job.
        logger.warning("creator durable-share notification remains pending", exc_info=True)


async def create_battle(update, context):
    """Create exact durable battle and expose a share-picker URL for that id."""
    query = update.callback_query
    user = query.from_user
    questions = battles._battle_pool()
    if not questions:
        await query.answer("⚠️ Вопросы для битвы не найдены.", show_alert=True)
        return

    battle_id = f"battle_{uuid.uuid4().hex[:16]}"
    try:
        await asyncio.to_thread(
            create_durable_battle,
            battle_id=battle_id,
            creator_id=user.id,
            creator_name=user.first_name or "Игрок",
            questions=questions,
        )
    except (LegacyBattleSessionUnavailable, LegacyBattleSessionConflict, ValueError):
        logger.warning("durable shared battle creation failed for user %s", user.id, exc_info=True)
        await query.answer("⚠️ Не удалось создать битву. Попробуй ещё раз.", show_alert=True)
        return

    rows = []
    try:
        share_url = build_battle_share_url(
            context.bot.username,
            battle_id,
            user.first_name or "Игрок",
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


async def handle_start_deep_link(update, context) -> bool:
    """Claim exact shared battle; return False for ordinary /start payloads."""
    try:
        battle_id = parse_battle_deep_link(getattr(context, "args", None))
    except ValueError:
        message = update.effective_message
        if message is not None:
            await message.reply_text("⚠️ Ссылка на битву повреждена или устарела.")
        return True
    if battle_id is None:
        return False

    user = update.effective_user
    message = update.effective_message
    if user is None or message is None:
        return True
    opponent_name = user.first_name or "Игрок"
    try:
        battle = await asyncio.to_thread(
            claim_durable_battle_opponent,
            battle_id,
            user.id,
            opponent_name,
        )
    except (LegacyBattleSessionUnavailable, ValueError):
        await message.reply_text("⚠️ База битв временно недоступна. Попробуй позже.")
        return True

    if battle is None:
        await message.reply_text(
            "⚠️ Эта битва уже занята, принадлежит тебе или окно ожидания истекло."
        )
        return True

    await message.reply_text(
        f"⚔️ *БИТВА НАЧАЛАСЬ!*\n\nТы vs {battle.get('creator_name', 'Игрок')}\n"
        "Каждый проходит свой durable progress независимо.",
        reply_markup=_opponent_start_markup(battle_id),
        parse_mode="Markdown",
    )
    await _notify_creator_ready(context.bot, battle, opponent_name)
    return True