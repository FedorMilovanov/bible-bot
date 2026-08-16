"""Canonical process-local state for in-progress Telegram reports."""
from __future__ import annotations


REPORT_TYPE, REPORT_TEXT, REPORT_PHOTO, REPORT_CONFIRM = range(10, 14)
REPORT_TYPE_LABELS = {
    "bug": "🐞 Баг",
    "idea": "💡 Идея",
    "question": "❓ Вопрос по материалу",
}
report_drafts: dict[int, dict] = {}


def install_legacy_bridge(legacy_module) -> None:
    """Point transitional legacy report state at the canonical definitions."""
    states = (
        getattr(legacy_module, "REPORT_TYPE", None),
        getattr(legacy_module, "REPORT_TEXT", None),
        getattr(legacy_module, "REPORT_PHOTO", None),
        getattr(legacy_module, "REPORT_CONFIRM", None),
    )
    if states != (REPORT_TYPE, REPORT_TEXT, REPORT_PHOTO, REPORT_CONFIRM):
        raise RuntimeError("legacy report conversation states diverged")
    if getattr(legacy_module, "REPORT_TYPE_LABELS", None) != REPORT_TYPE_LABELS:
        raise RuntimeError("legacy report type labels diverged")

    current = getattr(legacy_module, "report_drafts", None)
    if not isinstance(current, dict):
        raise TypeError("legacy module must expose a report_drafts dict")
    if current is not report_drafts:
        if current:
            report_drafts.update(current)
        legacy_module.report_drafts = report_drafts

    # The values are immutable integers, but assigning them after parity makes
    # the ownership explicit before transitional consumers copy the constants.
    legacy_module.REPORT_TYPE = REPORT_TYPE
    legacy_module.REPORT_TEXT = REPORT_TEXT
    legacy_module.REPORT_PHOTO = REPORT_PHOTO
    legacy_module.REPORT_CONFIRM = REPORT_CONFIRM
    legacy_module.REPORT_TYPE_LABELS = REPORT_TYPE_LABELS
