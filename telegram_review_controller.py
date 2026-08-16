# ruff: noqa: RUF001
"""Process-local post-quiz review presentation detached from the legacy bot module."""
from __future__ import annotations

from collections.abc import Mapping

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from utils import safe_truncate


def _correct_text(question: Mapping) -> str:
    options = question.get("options", [])
    correct = question.get("correct")
    if (
        not isinstance(options, list)
        or isinstance(correct, bool)
        or not isinstance(correct, int)
        or correct < 0
        or correct >= len(options)
    ):
        return "—"
    return str(options[correct])


def _build_error_page(wrong: list, index: int) -> tuple[str, InlineKeyboardMarkup]:
    """Build one historical wrong-answer review page."""
    total = len(wrong)
    item = wrong[index]
    question = item["question_obj"]
    user_answer = item["user_answer"]
    correct_text = _correct_text(question)

    verse_tag = f"📖 ст. {question['verse']} | " if question.get("verse") else ""
    topic_tag = f"🏷 {question['topic']}" if question.get("topic") else ""

    text = f"🔴 *Ошибка {index + 1} из {total}* {verse_tag}{topic_tag}\n\n"
    text += f"*Вопрос:* _{question['question']}_\n\n"
    text += f"*Ваш ответ:* {user_answer}\n"
    text += f"*Правильно:* {correct_text}\n\n"
    if "options_explanations" in question:
        text += "*Разбор вариантов:*\n"
        for option_index, option in enumerate(question["options"]):
            text += (
                f"• _{option}_\n"
                f"{question['options_explanations'][option_index]}\n\n"
            )
    text += f"💡 *Пояснение:* {question['explanation']}"
    if question.get("pdf_ref"):
        text += f"\n\n📄 _Источник: {question['pdf_ref']}_"

    left_payload = f"review_nav_{index - 1}" if index > 0 else "review_nav_noop"
    right_payload = (
        f"review_nav_{index + 1}" if index < total - 1 else "review_nav_noop"
    )
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⬅️" if index > 0 else "·",
                    callback_data=left_payload,
                ),
                InlineKeyboardButton(
                    f"{index + 1}/{total}",
                    callback_data="review_nav_noop",
                ),
                InlineKeyboardButton(
                    "➡️" if index < total - 1 else "·",
                    callback_data=right_payload,
                ),
            ],
            [
                InlineKeyboardButton(
                    "🔙 Вернуться в Меню",
                    callback_data="back_to_main",
                )
            ],
        ]
    )
    return safe_truncate(text, 4000), keyboard


async def review_test_handler(update, context, *, user_data: Mapping) -> None:
    """Page through the completed in-memory attempt with answer markers."""
    del context
    query = update.callback_query
    user_id = query.from_user.id
    data = user_data.get(user_id, {})

    try:
        question_index = int((query.data or "").rsplit("_", 1)[-1])
    except (TypeError, ValueError):
        await query.answer("Некорректная кнопка.", show_alert=True)
        return

    answered = data.get("answered_questions", [])
    if not answered or question_index < 0 or question_index >= len(answered):
        await query.answer()
        await query.edit_message_text(
            "❌ Данные теста не найдены. Пройди тест заново."
        )
        return

    await query.answer()
    total = len(answered)
    answer_data = answered[question_index]
    question = answer_data.get("question_obj", {})
    user_answer = answer_data.get("user_answer", "—")
    correct_answer = _correct_text(question)
    is_correct = user_answer == correct_answer
    status = "✅" if is_correct else "❌"

    text = (
        f"📖 *Просмотр теста* ({question_index + 1}/{total})\n\n"
        f"*Вопрос:*\n{question.get('question', '—')}\n\n"
        "*Варианты:*\n"
    )
    for option_index, option in enumerate(question.get("options", [])):
        if option_index == question.get("correct"):
            marker = "✅"
        elif option == user_answer and not is_correct:
            marker = "❌"
        else:
            marker = "⬜"
        arrow = " ← твой ответ" if option == user_answer and not is_correct else ""
        text += f"{marker} {option_index + 1}. {option}{arrow}\n"

    text += f"\n*Твой ответ:* {user_answer} {status}"
    explanation = question.get("explanation") or question.get("fun_fact")
    if explanation:
        text += f"\n\n💡 *Пояснение:*\n_{explanation}_"

    navigation = []
    if question_index > 0:
        navigation.append(
            InlineKeyboardButton(
                "⬅️ Пред.",
                callback_data=f"review_test_{question_index - 1}",
            )
        )
    navigation.append(
        InlineKeyboardButton(
            f"{question_index + 1}/{total}",
            callback_data="noop",
        )
    )
    if question_index < total - 1:
        navigation.append(
            InlineKeyboardButton(
                "➡️ След.",
                callback_data=f"review_test_{question_index + 1}",
            )
        )

    await query.edit_message_text(
        text=text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [
                navigation,
                [InlineKeyboardButton("🏠 В меню", callback_data="back_to_main")],
            ]
        ),
    )


async def review_errors_handler(update, context, *, user_data: Mapping) -> None:
    """Render only the current user's process-local wrong-answer review."""
    del context
    query = update.callback_query
    user_id = query.from_user.id
    callback_data = query.data or ""

    if callback_data.startswith("review_errors_"):
        parts = callback_data.split("_")
        if len(parts) != 4:
            await query.answer("Некорректная кнопка.", show_alert=True)
            return
        try:
            target_id = int(parts[2])
            index = int(parts[3])
        except (TypeError, ValueError):
            await query.answer("Некорректная кнопка.", show_alert=True)
            return
        if target_id != user_id:
            await query.answer(
                "Нет доступа к чужому разбору ошибок.",
                show_alert=True,
            )
            return
    elif callback_data.startswith("review_nav_"):
        suffix = callback_data.removeprefix("review_nav_")
        if suffix == "noop":
            await query.answer()
            return
        try:
            index = int(suffix)
        except (TypeError, ValueError):
            await query.answer("Некорректная кнопка.", show_alert=True)
            return
        target_id = user_id
    else:
        await query.answer()
        return

    await query.answer()
    if target_id not in user_data:
        await query.edit_message_text("⚠️ Данные устарели. Начни новый тест.")
        return

    wrong = user_data[user_id].get("wrong_answers", [])
    if not wrong:
        await query.edit_message_text("✅ Ошибок нет!")
        return

    index = max(0, min(index, len(wrong) - 1))
    text, keyboard = _build_error_page(wrong, index)
    try:
        await query.edit_message_text(
            text,
            reply_markup=keyboard,
            parse_mode="Markdown",
        )
    except Exception as exc:
        if "not modified" not in str(exc).lower():
            raise
