"""Narrow adapter around canonical process-local report draft state."""
from __future__ import annotations

from telegram_report_state import report_drafts


def drop_report_draft(user_id: int) -> bool:
    """Drop one process-local report draft; return whether one existed."""
    return report_drafts.pop(user_id, None) is not None
