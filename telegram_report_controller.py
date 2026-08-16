# ruff: noqa: RUF001
"""Production Telegram UI adapter for crash-safe user reports.

The historical report handlers in ``bot.py`` remove their only RAM draft before
Mongo acceptance and send directly to the administrator. This adapter keeps the
mature UI vocabulary but moves authority to the durable report receipt/outbox:

* a stable report id is allocated when the draft starts;
* confirmation persists the immutable draft before RAM cleanup;
* question-inaccuracy clicks use an attempt/question-bound deterministic id;
* administrator delivery happens only through the leased durable outbox;
* a lifecycle job drains pending reports after process restarts.

Nothing starts a background task on import.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ConversationHandler

import telegram_main_menu as main_menu
from database import can_submit_report, seconds_until_next_report
from legacy_inaccuracy_report import (
    LegacyInaccuracyReportInvalid,
    accept_inaccuracy_report_once,
)
from legacy_report_delivery_drain import drain_pending_reports
from legacy_report_submit import (
    LegacyReportDraftInvalid,
    accept_report_draft_once,
    new_report_draft,
    set_report_draft_photo,
    set_report_draft_text,
)
from legacy_session_access import (
    QuizSessionAccessSchemaInvalid,
    QuizSessionAccessUnavailable,
    get_active_quiz_session_strict,
)
from report_integrity import ReportStoreUnavailable
from telegram_report_state import (
    REPORT_CONFIRM,
    REPORT_PHOTO,
    REPORT_TEXT,
    REPORT_TYPE_LABELS,
    report_drafts,
)
from utils import safe_edit, safe_send

logger = logging.getLogger(__name__)


def _admin_user_id() -> int:
    raw = os.getenv("ADMIN_USER_ID")
    if not raw:
        raise ValueError("ADMIN_USER_ID is required for report delivery")
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError("ADMIN_USER_ID must be an integer") from exc


def _sanitize_report_text(text: str) -> str:
    text = text[:2000]
    text = re.sub(r"([*_`\[\]])", r"\\\1", text)
    return text.strip()


def _confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Отправить", callback_data="report_confirm")],
        [InlineKeyboardButton("❌ Отмена", callback_data="report_cancel")],
    ])


def _photo_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➡️ Без фото", callback_data="report_skip_photo")],
        [InlineKeyboardButton("❌ Отмена", callback_data="report_cancel")],
    ])


def _report_type(query_data: str | None) -> str | None:
    value = (query_data or "").replace("report_start_", "", 1)
    if value == "bug_direct":
        value = "bug"
    return value if value in REPORT_TYPE_LABELS else None


def _session_context(session: dict | None) -> dict:
    if not isinstance(session, dict):
        return {}
    return {
        "mode": session.get("level_key"),
        "level": session.get("level_name"),
        "q": session.get("current_index"),
        "attempt_id": session.get("attempt_id"),
    }


def _durable_draft_context(user_id: int) -> dict:
    """Best-effort context enrichment; report acceptance must not depend on it."""
    try:
        return _session_context(get_active_quiz_session_strict(user_id))
    except (QuizSessionAccessUnavailable, QuizSessionAccessSchemaInvalid):
        logger.warning("report context session lookup unavailable for user %s", user_id)
        return {}


def _draft_for(user_id: int) -> dict | None:
    draft = report_drafts.get(user_id)
    return draft if isinstance(draft, dict) else None


def _confirmation_text(draft: dict) -> str:
    label = REPORT_TYPE_LABELS.get(
        draft.get("type"),
        str(draft.get("type") or "Сообщение"),
    )
    has_photo = "✅ фото приложено" if draft.get("photo_file_id") else "нет фото"
    return (
        "📋 *Подтверждение*\n\n"
        f"*Тип:* {label}\n"
        f"*Фото:* {has_photo}\n\n"
        f"{draft.get('text') or ''}"
    )


async def report_start(update, context):
    del context
    query = update.callback_query
    report_type = _report_type(query.data)
    if report_type is None:
        await query.answer("Некорректный тип сообщения.", show_alert=True)
        return ConversationHandler.END

    user_id = query.from_user.id
    if not await asyncio.to_thread(can_submit_report, user_id):
        remaining = await asyncio.to_thread(seconds_until_next_report, user_id)
        await query.answer(
            f"⏳ Слишком часто. Попробуй через {remaining} сек.",
            show_alert=True,
        )
        return ConversationHandler.END

    try:
        draft = new_report_draft(report_type)
    except (ValueError, RuntimeError):
        logger.exception("could not allocate stable report draft id for user %s", user_id)
        await query.answer("⚠️ Не удалось начать сообщение. Попробуй позже.", show_alert=True)
        return ConversationHandler.END

    report_drafts[user_id] = draft
    await query.answer()
    label = REPORT_TYPE_LABELS[report_type]
    await safe_edit(
        query,
        f"{label}\n\n✏️ Напиши своё сообщение.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Отмена", callback_data="report_cancel")]
        ]),
    )
    return REPORT_TEXT


async def report_receive_text(update, context):
    del context
    user_id = update.effective_user.id
    draft = _draft_for(user_id)
    if draft is None:
        return ConversationHandler.END

    text = _sanitize_report_text((update.message.text or "").strip())
    if not text:
        await safe_send(update.message, "Пожалуйста, напиши текст.")
        return REPORT_TEXT
    try:
        set_report_draft_text(draft, text)
    except LegacyReportDraftInvalid:
        logger.warning("report draft text rejected for user %s", user_id, exc_info=True)
        await safe_send(update.message, "⚠️ Черновик повреждён. Начни сообщение заново.")
        return ConversationHandler.END

    await safe_send(
        update.message,
        "📎 Хочешь приложить скриншот?\n\nПришли *фото* или нажми кнопку ниже.",
        reply_markup=_photo_keyboard(),
        parse_mode="Markdown",
    )
    return REPORT_PHOTO


async def report_receive_photo(update, context):
    del context
    user_id = update.effective_user.id
    draft = _draft_for(user_id)
    if draft is None:
        return ConversationHandler.END
    if not update.message.photo:
        await safe_send(update.message, "Пришли фото или выбери «Без фото».")
        return REPORT_PHOTO
    try:
        set_report_draft_photo(draft, update.message.photo[-1].file_id)
    except LegacyReportDraftInvalid:
        logger.warning("report photo rejected for user %s", user_id, exc_info=True)
        await safe_send(update.message, "⚠️ Фото не удалось привязать к черновику.")
        return REPORT_PHOTO

    await safe_send(
        update.message,
        _confirmation_text(draft),
        reply_markup=_confirm_keyboard(),
        parse_mode="Markdown",
    )
    return REPORT_CONFIRM


async def report_skip_photo(update, context):
    del context
    query = update.callback_query
    await query.answer()
    draft = _draft_for(query.from_user.id)
    if draft is None:
        return ConversationHandler.END
    try:
        set_report_draft_photo(draft, None)
    except LegacyReportDraftInvalid:
        await safe_edit(query, "⚠️ Черновик повреждён. Начни сообщение заново.")
        return ConversationHandler.END
    await safe_edit(
        query,
        _confirmation_text(draft),
        reply_markup=_confirm_keyboard(),
    )
    return REPORT_CONFIRM


async def _send_report_photo(bot, report: dict) -> Any:
    photo_file_id = report.get("photo_file_id")
    if not isinstance(photo_file_id, str) or not photo_file_id:
        raise ValueError("durable report photo id is missing")
    return await bot.send_photo(
        chat_id=_admin_user_id(),
        photo=photo_file_id,
        caption=f"📎 Report {report.get('report_id') or report.get('_id')}",
    )


def _plain_context(report: dict) -> str:
    context = report.get("context")
    if not isinstance(context, dict) or not context:
        return "—"
    parts = []
    for key in ("kind", "level", "mode", "q", "attempt_id"):
        value = context.get(key)
        if value not in (None, ""):
            parts.append(f"{key}={value}")
    return ", ".join(parts) or "—"


async def _send_report_text(bot, report: dict) -> Any:
    report_type = report.get("type")
    label = REPORT_TYPE_LABELS.get(report_type, str(report_type or "report"))
    username = report.get("username") or "—"
    first_name = report.get("first_name") or "—"
    text = str(report.get("text") or "")
    body = (
        f"📨 {label}\n"
        f"Report: {report.get('report_id') or report.get('_id')}\n"
        f"User: {first_name} (@{username}), id={report.get('user_id')}\n"
        f"Context: {_plain_context(report)}\n\n"
        f"{text}"
    )
    return await bot.send_message(chat_id=_admin_user_id(), text=body[:4096])


async def drain_report_outbox(bot, *, limit: int = 50):
    async def photo_sender(report: dict):
        return await _send_report_photo(bot, report)

    async def text_sender(report: dict):
        return await _send_report_text(bot, report)

    summary = await drain_pending_reports(
        photo_sender=photo_sender,
        text_sender=text_sender,
        limit=limit,
    )
    if summary.errors:
        logger.warning("report outbox drain completed with errors: %s", summary.errors)
    return summary


async def report_delivery_job(context):
    try:
        await drain_report_outbox(context.bot)
    except Exception:
        logger.exception("unexpected report outbox drain failure")


async def report_confirm(update, context):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    user_id = user.id
    draft = _draft_for(user_id)
    if draft is None:
        await safe_edit(
            query,
            "⚠️ Данные устарели. Начни сообщение заново.",
            reply_markup=main_menu.main_keyboard(),
        )
        return ConversationHandler.END

    report_id = draft.get("report_id")
    try:
        accepted = await asyncio.to_thread(
            lambda: accept_report_draft_once(
                user_id=user_id,
                username=user.username,
                first_name=user.first_name,
                draft=draft,
                context=_durable_draft_context(user_id),
            )
        )
    except (LegacyReportDraftInvalid, ReportStoreUnavailable, ValueError):
        logger.warning("durable report acceptance failed for user %s", user_id, exc_info=True)
        await safe_edit(
            query,
            "⚠️ База не подтвердила сохранение. Черновик не удалён — нажми «Отправить» ещё раз.",
            reply_markup=_confirm_keyboard(),
        )
        return REPORT_CONFIRM

    accepted_id = (
        accepted.get("_id") or accepted.get("report_id")
        if isinstance(accepted, dict)
        else None
    )
    if not isinstance(accepted_id, str) or accepted_id != report_id:
        logger.error("durable report acceptance returned mismatched identity for user %s", user_id)
        await safe_edit(
            query,
            "⚠️ База вернула противоречивый идентификатор. Черновик сохранён локально для повтора.",
            reply_markup=_confirm_keyboard(),
        )
        return REPORT_CONFIRM

    current = _draft_for(user_id)
    if current is draft or (current and current.get("report_id") == report_id):
        report_drafts.pop(user_id, None)

    try:
        await drain_report_outbox(context.bot, limit=10)
    except Exception:
        logger.warning("accepted report remains queued for admin delivery", exc_info=True)

    await safe_edit(
        query,
        "✅ Сообщение сохранено. Спасибо!",
        reply_markup=main_menu.main_keyboard(),
    )
    return ConversationHandler.END


async def report_cancel(update, context):
    del context
    query = update.callback_query
    await query.answer()
    report_drafts.pop(query.from_user.id, None)
    await safe_edit(
        query,
        "❌ Репорт отменён.",
        reply_markup=main_menu.main_keyboard(),
    )
    return ConversationHandler.END


async def cancel_report_command(update, context):
    del context
    user_id = update.effective_user.id
    report_drafts.pop(user_id, None)
    await update.message.reply_text("❌ Репорт отменён.", reply_markup=ReplyKeyboardRemove())
    await update.message.reply_text("Главное меню:", reply_markup=main_menu.main_keyboard())
    return ConversationHandler.END


async def report_inaccuracy_handler(update, context):
    query = update.callback_query
    user = update.effective_user
    user_id = user.id
    try:
        session = await asyncio.to_thread(get_active_quiz_session_strict, user_id)
    except QuizSessionAccessUnavailable:
        await query.answer("База сессий временно недоступна.", show_alert=True)
        return
    except QuizSessionAccessSchemaInvalid:
        await query.answer("Состояние сессии противоречиво. Используй /status.", show_alert=True)
        return
    if not isinstance(session, dict):
        await query.answer("Сессия уже не активна. Используй /status.", show_alert=True)
        return

    try:
        question_index = int((query.data or "").replace("report_inaccuracy_", "", 1))
    except (TypeError, ValueError):
        await query.answer("Некорректная кнопка.", show_alert=True)
        return

    questions = session.get("questions_data")
    if (
        not isinstance(questions, list)
        or question_index < 0
        or question_index >= len(questions)
        or not isinstance(questions[question_index], dict)
    ):
        await query.answer("Этот вопрос уже недоступен.", show_alert=True)
        return
    attempt_id = session.get("attempt_id")
    if not isinstance(attempt_id, str) or not attempt_id:
        await query.answer("Попытка не подтверждена. Используй /status.", show_alert=True)
        return

    try:
        await asyncio.to_thread(
            lambda: accept_inaccuracy_report_once(
                user_id=user_id,
                username=user.username,
                first_name=user.first_name,
                attempt_id=attempt_id,
                question_index=question_index,
                question=questions[question_index],
                level_name=session.get("level_name"),
            )
        )
    except (LegacyInaccuracyReportInvalid, ReportStoreUnavailable, ValueError):
        logger.warning("inaccuracy report acceptance failed for user %s", user_id, exc_info=True)
        await query.answer("⚠️ Не удалось сохранить сообщение. Попробуй ещё раз.", show_alert=True)
        return

    await query.answer("✅ Неточность сохранена.")
    try:
        await drain_report_outbox(context.bot, limit=10)
    except Exception:
        logger.warning("accepted inaccuracy report remains queued", exc_info=True)
