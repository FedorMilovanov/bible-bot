"""Draft-to-durable submission boundary for legacy user reports.

A stable ``report_id`` is allocated when the draft starts and survives retries.
Confirmation persists immutable text/photo/context through the crash-safe report
store. This module never removes the caller's RAM draft and never sends Telegram
messages; those are explicit controller/outbox concerns.
"""
from __future__ import annotations

from copy import deepcopy

from report_integrity import accept_report_once, new_report_id

_REPORT_TYPES = frozenset({"bug", "idea", "question"})


class LegacyReportDraftInvalid(RuntimeError):
    """The in-memory report draft cannot be submitted safely."""


def _text(value, field: str, *, required: bool, max_length: int) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str):
        raise LegacyReportDraftInvalid(f"report draft {field} is invalid")
    value = value.strip()
    if required and not value:
        raise LegacyReportDraftInvalid(f"report draft {field} is missing")
    if len(value) > max_length:
        raise LegacyReportDraftInvalid(f"report draft {field} is too long")
    return value or None


def new_report_draft(report_type: str) -> dict:
    if report_type not in _REPORT_TYPES:
        raise ValueError("unsupported report type")
    return {"report_id": new_report_id(), "type": report_type, "text": None, "photo_file_id": None}


def set_report_draft_text(draft: dict, text: str) -> dict:
    _validated_identity(draft)
    draft["text"] = _text(text, "text", required=True, max_length=2000)
    return draft


def set_report_draft_photo(draft: dict, photo_file_id: str | None) -> dict:
    _validated_identity(draft)
    draft["photo_file_id"] = _text(photo_file_id, "photo_file_id", required=False, max_length=1024)
    return draft


def _validated_identity(draft: dict) -> tuple[str, str]:
    if not isinstance(draft, dict):
        raise LegacyReportDraftInvalid("report draft is invalid")
    report_id = _text(draft.get("report_id"), "report_id", required=True, max_length=128)
    report_type = draft.get("type")
    if report_type not in _REPORT_TYPES:
        raise LegacyReportDraftInvalid("report draft type is invalid")
    return report_id, report_type


def immutable_report_draft_snapshot(draft: dict, *, context: dict | None = None) -> dict:
    report_id, report_type = _validated_identity(draft)
    text = _text(draft.get("text"), "text", required=True, max_length=2000)
    photo_file_id = _text(draft.get("photo_file_id"), "photo_file_id", required=False, max_length=1024)
    if context is None:
        context = {}
    if not isinstance(context, dict):
        raise LegacyReportDraftInvalid("report draft context is invalid")
    return {
        "report_id": report_id,
        "report_type": report_type,
        "text": text,
        "photo_file_id": photo_file_id,
        "context": deepcopy(context),
    }


def accept_report_draft_once(
    *,
    user_id: int,
    username: str | None,
    first_name: str | None,
    draft: dict,
    context: dict | None = None,
) -> dict:
    snapshot = immutable_report_draft_snapshot(draft, context=context)
    return accept_report_once(
        report_id=snapshot["report_id"],
        user_id=user_id,
        username=username,
        first_name=first_name,
        report_type=snapshot["report_type"],
        text=snapshot["text"],
        photo_file_id=snapshot["photo_file_id"],
        context=snapshot["context"],
    )
