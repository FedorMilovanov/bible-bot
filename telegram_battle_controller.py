# ruff: noqa: RUF001
"""Production Telegram adapter for Mongo-authoritative durable PvP battles."""
from __future__ import annotations

import asyncio
import logging
import random
import uuid

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ConversationHandler

import bot as legacy
from battle_integrity import (
    BATTLE_DELIVERY_PROTOCOL_OUTBOX,
    BattleStoreUnavailable,
    battle_role_for_user,
    claim_final_battle,
    record_battle_result,
)
from legacy_battle_callback_protocol import (
    LegacyBattleCallbackInvalid,
    build_battle_answer_callback,
    parse_battle_answer_callback,
    resolve_battle_option,
)
from legacy_battle_cancel import LegacyBattleCancelUnavailable, cancel_unstarted_battle
from legacy_battle_cleanup import LegacyBattleCleanupUnavailable, cleanup_stale_waiting_battles
from legacy_battle_delivery_drain import drain_pending_battles
from legacy_battle_finalization_drain import finalize_ready_battles
from legacy_battle_progress import (
    LegacyBattleProgressConflict,
    LegacyBattleProgressInvalid,
    LegacyBattleProgressUnavailable,
    completed_battle_result_inputs,
    ensure_battle_progress,
    mark_battle_question_sent,
    record_battle_answer_once,
)
from legacy_battle_recovery import (
    LegacyBattleRecoveryUnavailable,
    get_open_durable_battles_for_user,
)
from legacy_battle_session import (
    LegacyBattleSessionConflict,
    LegacyBattleSessionUnavailable,
    claim_durable_battle_opponent,
    create_durable_battle,
    get_owned_open_durable_battle,
    get_waiting_durable_battles,
    resolve_owned_open_battle_callback,
)

logger = logging.getLogger(__name__)
BATTLE_ANSWERING = legacy.BATTLE_ANSWERING


def _role_label(battle: dict, role: str) -> str:
    other = "opponent" if role == "creator" else "creator"
    return str(battle.get(f"{other}_name") or "соперник")


def _start_payload(battle_id: str, role: str) -> str:
    payload = f"start_battle_{battle_id}_{role}"
    if len(payload.encode()) > 64:
        raise ValueError("battle start callback exceeds Telegram limit")
    return payload


def _cancel_payload(battle_id: str) -> str:
    payload = f"cancel_battle_{battle_id}"
    if len(payload.encode()) > 64:
        raise ValueError("battle cancel callback exceeds Telegram limit")
    return payload


def _battle_pool() -> list[dict]:
    pool = list(legacy.BATTLE_POOL) + list(legacy.INTRO_POOL)
    if not pool:
        return []
    return random.sample(pool, min(10, len(pool)))


def _menu_markup(user_id: int, active: list[dict], waiting: list[dict]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("🆕 Создать битву", callback_data="create_battle")]]
    seen: set[str] = set()
    for battle in active:
        battle_id = battle.get("_id")
        role = battle_role_for_user(battle, user_id)
        if not isinstance(battle_id, str) or role is None:
            continue
        seen.add(battle_id)
        if battle.get("opponent_id") is None:
            rows.append([
                InlineKeyboardButton(
                    "⏳ Моя битва ждёт соперника",
                    callback_data="noop",
                ),
                InlineKeyboardButton("❌", callback_data=_cancel_payload(battle_id)),
            ])
        else:
            rows.append([
                InlineKeyboardButton(
                    f"▶️ Продолжить vs {_role_label(battle, role)[:18]}",
                    callback_data=_start_payload(battle_id, role),
                )
            ])
    for battle in waiting:
        battle_id = battle.get("_id")
        creator_id = battle.get("creator_id")
        if not isinstance(battle_id, str) or battle_id in seen or creator_id == user_id:
            continue
        rows.append([
            InlineKeyboardButton(
                f"⚔️ vs {str(battle.get('creator_name') or 'Игрок')[:18]}",
                callback_data=f"join_battle_{battle_id}",
            )
        ])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(rows)


async def show_battle_menu(update, context):
    del context
    query = update.callback_query
    user_id = query.from_user.id
    try:
        active = await asyncio.to_thread(
            get_open_durable_battles_for_user,
            user_id,
            limit=10,
        )
        waiting = await asyncio.to_thread(get_waiting_durable_battles, limit=10)
    except (LegacyBattleRecoveryUnavailable, LegacyBattleSessionUnavailable, ValueError):
        await query.answer("⚠️ База битв временно недоступна.", show_alert=True)
        return
    await query.answer()
    await query.edit_message_text(
        "⚔️ *РЕЖИМ БИТВЫ*\n\n"
        "🎯 10 одинаковых вопросов каждому участнику.\n"
        "⚡ За быстрые правильные ответы начисляется больше battle-очков.\n"
        "🏆 Победа = +5 рейтинговых баллов, ничья = +2.\n\n"
        "Durable progress можно продолжить после перезапуска бота.",
        reply_markup=_menu_markup(user_id, active, waiting),
        parse_mode="Markdown",
    )


async def create_battle(update, context):
    del context
    query = update.callback_query
    user = query.from_user
    questions = _battle_pool()
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
        logger.warning("durable battle creation failed for user %s", user.id, exc_info=True)
        await query.answer("⚠️ Не удалось создать битву. Попробуй ещё раз.", show_alert=True)
        return
    await query.answer()
    await query.edit_message_text(
        "⚔️ *БИТВА СОЗДАНА!*\n\n"
        "⏳ Сначала дождись соперника. Когда он присоединится, бот пришлёт кнопку Start.\n\n"
        "_Незапущенная битва автоматически очищается после окна ожидания._",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Отменить ожидание", callback_data=_cancel_payload(battle_id))],
            [InlineKeyboardButton("⬅️ К битвам", callback_data="battle_menu")],
        ]),
        parse_mode="Markdown",
    )


async def join_battle(update, context):
    query = update.callback_query
    user = query.from_user
    battle_id = (query.data or "").replace("join_battle_", "", 1)
    try:
        battle = await asyncio.to_thread(
            claim_durable_battle_opponent,
            battle_id,
            user.id,
            user.first_name or "Игрок",
        )
    except (LegacyBattleSessionUnavailable, ValueError):
        await query.answer("⚠️ База битв временно недоступна.", show_alert=True)
        return
    if battle is None:
        await query.answer("Эту битву уже занял другой игрок или она истекла.", show_alert=True)
        return
    await query.answer()
    await query.edit_message_text(
        f"⚔️ *БИТВА НАЧАЛАСЬ!*\n\nТы vs {battle.get('creator_name', 'Игрок')}\n"
        "Каждый проходит свой durable progress независимо.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Начать", callback_data=_start_payload(battle_id, "opponent"))],
            [InlineKeyboardButton("⬅️ К битвам", callback_data="battle_menu")],
        ]),
        parse_mode="Markdown",
    )
    try:
        await context.bot.send_message(
            chat_id=int(battle["creator_id"]),
            text=f"⚔️ *Соперник найден:* {user.first_name or 'Игрок'}\nМожно начинать битву.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("▶️ Начать", callback_data=_start_payload(battle_id, "creator"))]
            ]),
            parse_mode="Markdown",
        )
    except Exception:
        logger.info("creator battle-ready notification was not delivered", exc_info=True)


def _parse_start(payload: str | None) -> tuple[str, str]:
    value = (payload or "").replace("start_battle_", "", 1)
    parts = value.rsplit("_", 1)
    if len(parts) != 2 or parts[1] not in {"creator", "opponent"}:
        raise ValueError("invalid battle start callback")
    return parts[0], parts[1]


async def start_battle_questions(update, context):
    query = update.callback_query
    user_id = query.from_user.id
    try:
        battle_id, requested_role = _parse_start(query.data)
        battle = await asyncio.to_thread(get_owned_open_durable_battle, battle_id, user_id)
    except (LegacyBattleSessionUnavailable, ValueError):
        await query.answer("⚠️ База битв временно недоступна.", show_alert=True)
        return ConversationHandler.END
    role = battle_role_for_user(battle, user_id)
    if battle is None or role != requested_role:
        await query.answer("Эта кнопка устарела или принадлежит другому участнику.", show_alert=True)
        return ConversationHandler.END
    if battle.get("opponent_id") is None:
        await query.answer("⏳ Сначала дождись соперника.", show_alert=True)
        return ConversationHandler.END
    try:
        state = await asyncio.to_thread(ensure_battle_progress, battle_id, user_id, role)
    except (LegacyBattleProgressUnavailable, LegacyBattleProgressConflict, LegacyBattleProgressInvalid):
        await query.answer("⚠️ Durable progress сейчас нельзя восстановить.", show_alert=True)
        return ConversationHandler.END

    await query.answer()
    progress = state["progress"]
    questions = state["battle"].get("questions", [])
    if progress["current_index"] >= len(questions):
        await query.edit_message_text("⏳ Восстанавливаю финализацию битвы…")
        await finish_battle_for_user(context.bot, query.message.chat_id, user_id, battle_id, role)
        return ConversationHandler.END
    await query.edit_message_text(
        f"⚔️ *Продолжаем битву*\nВопрос {progress['current_index'] + 1}/{len(questions)}",
        parse_mode="Markdown",
    )
    await send_battle_question(context.bot, query.message.chat_id, user_id, battle_id, role)
    return BATTLE_ANSWERING


async def _disable_message_keyboard(bot, chat_id: int, message_id: int) -> None:
    try:
        await bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=None)
    except Exception:
        pass


async def send_battle_question(bot, chat_id: int, user_id: int, battle_id: str, role: str):
    try:
        state = await asyncio.to_thread(ensure_battle_progress, battle_id, user_id, role)
    except (LegacyBattleProgressUnavailable, LegacyBattleProgressConflict, LegacyBattleProgressInvalid):
        await bot.send_message(chat_id=chat_id, text="⚠️ Не удалось восстановить durable progress битвы.")
        return
    battle = state["battle"]
    progress = state["progress"]
    questions = battle.get("questions", [])
    index = progress["current_index"]
    if index >= len(questions):
        await finish_battle_for_user(bot, chat_id, user_id, battle_id, role)
        return
    question = questions[index]
    options = list(question.get("options", []))
    shuffled = options[:]
    random.shuffle(shuffled)
    try:
        callbacks = [build_battle_answer_callback(battle_id, index, option) for option in shuffled]
    except ValueError:
        logger.error("battle callback generation failed for %s", battle_id, exc_info=True)
        await bot.send_message(chat_id=chat_id, text="⚠️ Вопрос битвы повреждён.")
        return

    if any(len(option) > legacy.MAX_BTN_LEN for option in shuffled):
        options_text = "\n\n" + "\n".join(f"*{i + 1}.* {option}" for i, option in enumerate(shuffled))
        rows = [[InlineKeyboardButton(str(i + 1), callback_data=cb) for i, cb in enumerate(callbacks)]]
    else:
        options_text = ""
        rows = [
            [InlineKeyboardButton(option, callback_data=cb)]
            for option, cb in zip(shuffled, callbacks, strict=True)
        ]
    rows.append([InlineKeyboardButton("⬅️ В меню", callback_data="battle_menu")])
    text = (
        f"⚔️ *Вопрос {index + 1}/{len(questions)}* {legacy.build_progress_bar(index + 1, len(questions))}\n"
        f"⚡ Быстрее = больше очков!\n\n{question.get('question', '')}{options_text}"
    )
    try:
        sent = await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=InlineKeyboardMarkup(rows),
            parse_mode="Markdown",
        )
    except Exception:
        logger.warning("battle question Telegram delivery failed", exc_info=True)
        return
    try:
        await asyncio.to_thread(
            mark_battle_question_sent,
            battle_id,
            user_id,
            role,
            expected_index=index,
        )
    except (LegacyBattleProgressUnavailable, LegacyBattleProgressConflict, LegacyBattleProgressInvalid):
        await _disable_message_keyboard(bot, chat_id, sent.message_id)
        await bot.send_message(
            chat_id=chat_id,
            text="⚠️ Вопрос показан, но Mongo не подтвердил timer marker. Ответы отключены.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("▶️ Восстановить", callback_data=_start_payload(battle_id, role))]
            ]),
        )


async def battle_answer(update, context):
    query = update.callback_query
    user_id = query.from_user.id
    try:
        callback_token, question_index, option_token = parse_battle_answer_callback(query.data)
        battle = await asyncio.to_thread(
            resolve_owned_open_battle_callback,
            user_id,
            callback_token,
        )
        role = battle_role_for_user(battle, user_id)
        if role is None:
            raise LegacyBattleSessionConflict("callback owner is not a participant")
        questions = battle.get("questions", [])
        if question_index < 0 or question_index >= len(questions):
            raise LegacyBattleCallbackInvalid("battle question callback is stale")
        user_answer = resolve_battle_option(questions[question_index].get("options", []), option_token)
        outcome = await asyncio.to_thread(
            record_battle_answer_once,
            battle["_id"],
            user_id,
            role,
            expected_index=question_index,
            user_answer=user_answer,
        )
    except LegacyBattleCallbackInvalid:
        await query.answer("Эта кнопка битвы устарела.", show_alert=True)
        return
    except (LegacyBattleSessionUnavailable, LegacyBattleProgressUnavailable):
        await query.answer("⚠️ База битв временно недоступна. Ответ не потерян намеренно — повтори позже.", show_alert=True)
        return
    except (LegacyBattleSessionConflict, LegacyBattleProgressConflict, LegacyBattleProgressInvalid, ValueError):
        await query.answer("Состояние битвы уже изменилось. Открой меню битв.", show_alert=True)
        return

    answer = outcome["answer"]
    if answer.get("is_correct") is True:
        points = int(answer.get("points", 0) or 0)
        await query.answer(f"✅ +{points} battle-очков")
        feedback = f"✅ *Верно!* +{points}"
    else:
        correct = questions[question_index]["options"][questions[question_index]["correct"]]
        await query.answer(f"❌ Верно: {correct}", show_alert=True)
        feedback = f"❌ *Неверно*\n✅ Правильно: *{correct}*"
    try:
        await query.edit_message_text(feedback, parse_mode="Markdown", reply_markup=None)
    except Exception:
        pass

    progress = outcome["progress"]
    battle_id = outcome["battle"]["_id"]
    if progress["current_index"] >= len(questions):
        await finish_battle_for_user(context.bot, query.message.chat_id, user_id, battle_id, role)
        return
    await asyncio.sleep(0.4)
    await send_battle_question(context.bot, query.message.chat_id, user_id, battle_id, role)


async def finish_battle_for_user(bot, chat_id: int, user_id: int, battle_id: str, role: str):
    try:
        result = await asyncio.to_thread(
            completed_battle_result_inputs,
            battle_id,
            user_id,
            role,
        )
        battle = await asyncio.to_thread(
            record_battle_result,
            battle_id,
            user_id,
            role,
            score=result["score"],
            time_seconds=result["time_seconds"],
            points=result["points"],
        )
    except (LegacyBattleProgressUnavailable, LegacyBattleProgressConflict, LegacyBattleProgressInvalid, BattleStoreUnavailable, ValueError):
        logger.warning("battle participant finalization pending for %s", battle_id, exc_info=True)
        await bot.send_message(
            chat_id=chat_id,
            text="⚠️ Результат вопросов сохранён, но participant finalization пока не подтверждена. Открой «Битвы» и нажми «Продолжить» для безопасного повтора.",
        )
        return
    if not isinstance(battle, dict):
        await bot.send_message(chat_id=chat_id, text="⚠️ Итог битвы сейчас нельзя подтвердить.")
        return

    if battle.get("creator_finished") and battle.get("opponent_finished"):
        try:
            await asyncio.to_thread(
                claim_final_battle,
                battle_id,
                delivery_protocol=BATTLE_DELIVERY_PROTOCOL_OUTBOX,
            )
        except BattleStoreUnavailable:
            logger.warning("shared battle finalization deferred for %s", battle_id, exc_info=True)
        await drain_battle_outbox(bot, limit=10)
        return

    await bot.send_message(
        chat_id=chat_id,
        text=(
            "✅ *Ты закончил!*\n\n"
            f"📊 {result['score']}/{result['total']}\n"
            f"⚡ Battle-очки: {result['points']}\n"
            f"⏱ Время: {legacy.format_time(result['time_seconds'])}\n\n"
            "⏳ Ожидание результата соперника…"
        ),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ К битвам", callback_data="battle_menu")]]),
        parse_mode="Markdown",
    )


def _result_text(battle: dict) -> str:
    creator_points = int(battle.get("creator_points", 0) or 0)
    opponent_points = int(battle.get("opponent_points", 0) or 0)
    if creator_points > opponent_points:
        headline = f"🏆 *Победитель: {battle.get('creator_name', 'Игрок')}!*"
    elif opponent_points > creator_points:
        headline = f"🏆 *Победитель: {battle.get('opponent_name', 'Игрок')}!*"
    else:
        headline = "🤝 *НИЧЬЯ!*"
    total = len(battle.get("questions", []))
    return (
        "⚔️ *РЕЗУЛЬТАТЫ БИТВЫ*\n\n"
        f"{headline}\n\n"
        f"👤 *{battle.get('creator_name', 'Игрок')}* — "
        f"{battle.get('creator_score', 0)}/{total} • ⚡ {creator_points} • ⏱ {legacy.format_time(battle.get('creator_time', 0))}\n"
        f"👤 *{battle.get('opponent_name', 'Игрок')}* — "
        f"{battle.get('opponent_score', 0)}/{total} • ⚡ {opponent_points} • ⏱ {legacy.format_time(battle.get('opponent_time', 0))}\n\n"
        + ("💎 Победителю +5 рейтинговых баллов." if creator_points != opponent_points else "💎 Каждому +2 рейтинговых балла.")
    )


async def _send_final_result(bot, battle: dict, role: str):
    user_id = battle.get(f"{role}_id")
    if isinstance(user_id, bool) or not isinstance(user_id, int):
        raise ValueError("battle result recipient is invalid")
    return await bot.send_message(
        chat_id=user_id,
        text=_result_text(battle),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Новая битва", callback_data="battle_menu")],
            [InlineKeyboardButton("⬅️ В меню", callback_data="back_to_main")],
        ]),
        parse_mode="Markdown",
    )


async def drain_battle_outbox(bot, *, limit: int = 50):
    async def sender(battle: dict, role: str):
        return await _send_final_result(bot, battle, role)

    summary = await drain_pending_battles(sender=sender, limit=limit)
    if summary.errors:
        logger.warning("battle outbox drain completed with errors: %s", summary.errors)
    return summary


async def cancel_battle(update, context):
    del context
    query = update.callback_query
    battle_id = (query.data or "").replace("cancel_battle_", "", 1)
    try:
        deleted = await asyncio.to_thread(
            cancel_unstarted_battle,
            battle_id,
            query.from_user.id,
        )
    except (LegacyBattleCancelUnavailable, ValueError):
        await query.answer("⚠️ База битв временно недоступна.", show_alert=True)
        return
    if not deleted:
        await query.answer(
            "Битва уже началась или содержит durable progress. Удалять её нельзя — продолжи через меню битв.",
            show_alert=True,
        )
        return
    await query.answer()
    await query.edit_message_text(
        "❌ Ожидание битвы отменено.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ К битвам", callback_data="battle_menu")]]),
    )


async def battle_maintenance_job(context):
    finalization = await asyncio.to_thread(finalize_ready_battles, limit=50)
    if finalization.errors:
        logger.warning("battle finalization sweep errors: %s", finalization.errors)
    try:
        await drain_battle_outbox(context.bot, limit=50)
    except Exception:
        logger.exception("battle outbox maintenance failed")
    try:
        await asyncio.to_thread(
            cleanup_stale_waiting_battles,
            max_age_minutes=10,
        )
    except LegacyBattleCleanupUnavailable:
        logger.warning("battle stale cleanup unavailable", exc_info=True)
