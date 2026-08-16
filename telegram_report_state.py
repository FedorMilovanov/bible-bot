"""Canonical process-local state for in-progress Telegram report drafts."""
from __future__ import annotations


report_drafts: dict[int, dict] = {}


def install_legacy_bridge(legacy_module) -> None:
    """Point transitional legacy report handlers at the canonical draft mapping."""
    current = getattr(legacy_module, "report_drafts", None)
    if not isinstance(current, dict):
        raise TypeError("legacy module must expose a report_drafts dict")
    if current is report_drafts:
        return
    if current:
        report_drafts.update(current)
    legacy_module.report_drafts = report_drafts
