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

import logging
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ConversationHandler

import bot as legacy
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
from report_integrity import ReportStoreUnavailable

logger = logging.getLogger(__name__)

REPORT_TEXT = legacy.REPORT_TEXT
REPORT_PHOTO = legacy.REPORT_PHOTO
REPORT_CONFIRM = legacy.REPORT_CONFIRM


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
    return value if value in legacy.REPORT_TYPE_LABELS else None


def _draft_context(user_id: int) -> dict:
    data = legacy.user_data.get(user_id)
    if not isinstance(data, dict):
        return {}
    return {
        "mode": data.get("level_key"),
        "level": data.get("level_name"),
        "q": data.get("current_question"),
        "attempt_id": data.get("attempt_id"),
    }


def _draft_for(user_id: int) -> dict | None:
    draft = legacy.report_drafts.get(user_id)
    return draft if isinstance(draft, dict) else None


def _confirmation_text(draft: dict) -> str:
    label = legacy.REPORT_TYPE_LABELS.get(draft.get("type"), str(draft.get("type") or "Сообщение"))
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
    if not legacy.can_submit_report(user_id):
        remaining = legacy.seconds_until_next_report(user_id)
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

    legacy.report_drafts[user_id] = draft
    await query.answer()
    label = legacy.REPORT_TYPE_LABELS[report_type]
    await legacy.safe_edit(
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

    text = legacy.sanitize_report_text((update.message.text or "").strip())
    if not text:
        await legacy.safe_send(update.message, "Пожалуйста, напиши текст.")
        return REPORT_TEXT
    try:
        set_report_draft_text(draft, text)
    except LegacyReportDraftInvalid:
        logger.warning("report draft text rejected for user %s", user_id, exc_info=True)
        await legacy.safe_send(update.message, "⚠️ Черновик повреждён. Начни сообщение заново.")
        return ConversationHandler.END

    await legacy.safe_send(
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
        await legacy.safe_send(update.message, "Пришли фото или выбери «Без фото».")
        return REPORT_PHOTO
    try:
        set_report_draft_photo(draft, update.message.photo[-1].file_id)
    except LegacyReportDraftInvalid:
        logger.warning("report photo rejected for user %s", user_id, exc_info=True)
        await legacy.safe_send(update.message, "⚠️ Фото не удалось привязать к черновику.")
        return REPORT_PHOTO

    await legacy.safe_send(
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
        await legacy.safe_edit(query, "⚠️ Черновик повреждён. Начни сообщение заново.")
        return ConversationHandler.END
    await legacy.safe_edit(
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
        chat_id=legacy.ADMIN_USER_ID,
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
    label = legacy.REPORT_TYPE_LABELS.get(report_type, str(report_type or "report"))
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
    return await bot.send_message(chat_id=legacy.ADMIN_USER_ID, text=body[:4096])


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
        await legacy.safe_edit(
            query,
            "⚠️ Данные устарели. Начни сообщение заново.",
            reply_markup=legacy._main_keyboard(),
        )
        return ConversationHandler.END

    report_id = draft.get("report_id")
    try:
        accepted = accept_report_draft_once(
            user_id=user_id,
            username=user.username,
            first_name=user.first_name,
            draft=draft,
            context=_draft_context(user_id),
        )
    except (LegacyReportDraftInvalid, ReportStoreUnavailable, ValueError):
        logger.warning("durable report acceptance failed for user %s", user_id, exc_info=True)
        await legacy.safe_edit(
            query,
            "⚠️ База не подтвердила сохранение. Черновик не удалён — нажми «Отправить» ещё раз.",
            reply_markup=_confirm_keyboard(),
        )
        return REPORT_CONFIRM

    accepted_id = accepted.get("_id") or accepted.get("report_id") if isinstance(accepted, dict) else None
    if not isinstance(accepted_id, str) or accepted_id != report_id:
        logger.error("durable report acceptance returned mismatched identity for user %s", user_id)
        await legacy.safe_edit(
            query,
            "⚠️ База вернула противоречивый идентификатор. Черновик сохранён локально для повтора.",
            reply_markup=_confirm_keyboard(),
        )
        return REPORT_CONFIRM

    current = _draft_for(user_id)
    if current is draft or (current and current.get("report_id") == report_id):
        legacy.report_drafts.pop(user_id, None)

    try:
        await drain_report_outbox(context.bot, limit=10)
    except Exception:
        # Acceptance is already durable. Delivery remains retryable by the job.
        logger.warning("accepted report remains queued for admin delivery", exc_info=True)

    await legacy.safe_edit(
        query,
        "✅ Сообщение сохранено. Спасибо!",
        reply_markup=legacy._main_keyboard(),
    )
    return ConversationHandler.END


async def report_cancel(update, context):
    del context
    query = update.callback_query
    await query.answer()
    legacy.report_drafts.pop(query.from_user.id, None)
    await legacy.safe_edit(
        query,
        "❌ Репорт отменён.",
        reply_markup=legacy._main_keyboard(),
    )
    return ConversationHandler.END


async def cancel_report_command(update, context):
    del context
    user_id = update.effective_user.id
    legacy.report_drafts.pop(user_id, None)
    await update.message.reply_text("❌ Репорт отменён.", reply_markup=ReplyKeyboardRemove())
    await update.message.reply_text("Главное меню:", reply_markup=legacy._main_keyboard())
    return ConversationHandler.END


async def report_inaccuracy_handler(update, context):
    query = update.callback_query
    user = update.effective_user
    user_id = user.id
    data = legacy.user_data.get(user_id)
    if not isinstance(data, dict):
        await query.answer("Сессия уже не загружена. Используй /status.", show_alert=True)
        return

    try:
        question_index = int((query.data or "").replace("report_inaccuracy_", "", 1))
    except (TypeError, ValueError):
        await query.answer("Некорректная кнопка.", show_alert=True)
        return

    questions = data.get("questions")
    if (
        not isinstance(questions, list)
        or question_index < 0
        or question_index >= len(questions)
        or not isinstance(questions[question_index], dict)
    ):
        await query.answer("Этот вопрос уже недоступен.", show_alert=True)
        return
    attempt_id = data.get("attempt_id")
    if not isinstance(attempt_id, str) or not attempt_id:
        await query.answer("Попытка не подтверждена. Используй /status.", show_alert=True)
        return

    try:
        accept_inaccuracy_report_once(
            user_id=user_id,
            username=user.username,
            first_name=user.first_name,
            attempt_id=attempt_id,
            question_index=question_index,
            question=questions[question_index],
            level_name=data.get("level_name"),
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
