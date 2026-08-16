"""Canonical Telegram answer-button feedback animation."""
from __future__ import annotations

import asyncio
import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

logger = logging.getLogger(__name__)


def _replace_button_text(row: list[InlineKeyboardButton], index: int, text: str) -> None:
    """Replace one immutable PTB button while preserving its callback payload."""
    button = row[index]
    row[index] = InlineKeyboardButton(text, callback_data=button.callback_data)


async def animate_answer_buttons(
    query,
    btn_index: int,
    correct_index: int,
    is_numeric_mode: bool,
    shuffled: list[str],
) -> None:
    """Render the intended deployed post-answer inline-keyboard animation."""
    is_correct = btn_index == correct_index

    try:
        rows = [
            [
                InlineKeyboardButton(btn.text, callback_data=btn.callback_data)
                for btn in row
            ]
            for row in query.message.reply_markup.inline_keyboard
        ]
        answer_rows = rows[:-1]
        service_row = rows[-1]

        if is_numeric_mode:
            num_row = answer_rows[0]

            if is_correct:
                for emoji in ["✅", "🎉", "⭐", "✅"]:
                    _replace_button_text(num_row, btn_index, emoji)
                    try:
                        await query.edit_message_reply_markup(
                            reply_markup=InlineKeyboardMarkup([*answer_rows, service_row])
                        )
                    except Exception:
                        return
                    await asyncio.sleep(0.3)
            else:
                _replace_button_text(num_row, btn_index, "❌")
                _replace_button_text(num_row, correct_index, "✅")
                try:
                    await query.edit_message_reply_markup(
                        reply_markup=InlineKeyboardMarkup([*answer_rows, service_row])
                    )
                except Exception:
                    pass

        else:
            correct_text = shuffled[correct_index]
            user_text = shuffled[btn_index]

            if is_correct:
                for emoji in ["✅", "🎉", "⭐", "✅"]:
                    _replace_button_text(
                        answer_rows[btn_index],
                        0,
                        f"{emoji} {user_text}",
                    )
                    try:
                        await query.edit_message_reply_markup(
                            reply_markup=InlineKeyboardMarkup([*answer_rows, service_row])
                        )
                    except Exception:
                        return
                    await asyncio.sleep(0.3)
            else:
                _replace_button_text(
                    answer_rows[btn_index],
                    0,
                    f"❌ {user_text}",
                )
                _replace_button_text(
                    answer_rows[correct_index],
                    0,
                    f"✅ {correct_text}",
                )
                try:
                    await query.edit_message_reply_markup(
                        reply_markup=InlineKeyboardMarkup([*answer_rows, service_row])
                    )
                except Exception:
                    pass

    except Exception as exc:
        logger.debug("animate_answer_buttons error: %s", exc)


__all__ = ["animate_answer_buttons"]
