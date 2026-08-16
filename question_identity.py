"""Canonical question identity algorithms."""
from __future__ import annotations

import hashlib


def stable_question_id(question: dict) -> str:
    """Return the historical text-only 12-character MD5 compatibility id."""
    text = question.get("question", "")
    return hashlib.md5(text.encode()).hexdigest()[:12]


def get_qid(question: dict) -> str:
    """Return the persisted 12-character SHA256 question identity."""
    text = question.get("question", "") + "".join(question.get("options", []))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


__all__ = ["get_qid", "stable_question_id"]
