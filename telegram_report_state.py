"""Canonical process-local state for in-progress Telegram reports."""
from __future__ import annotations


REPORT_TYPE, REPORT_TEXT, REPORT_PHOTO, REPORT_CONFIRM = range(10, 14)
REPORT_TYPE_LABELS = {
    "bug": "🐞 Баг",
    "idea": "💡 Идея",
    "question": "❓ Вопрос по материалу",
}
report_drafts: dict[int, dict] = {}
