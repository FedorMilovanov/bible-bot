"""Canonical helpers for interpreting recorded quiz answer history."""
from __future__ import annotations

from collections.abc import Callable


_PROBE_QUESTION = {
    "question": "Кого называет автор?",
    "options": ["A", "Б", "C"],
    "correct": 1,
}


def correct_text(question: dict) -> str:
    """Return the canonical correct option text using the historical semantics."""
    return question["options"][question["correct"]]


def is_wrong(item: dict) -> bool:
    """Return whether one recorded user answer differs from the correct text."""
    return item["user_answer"] != correct_text(item["question_obj"])


def _assert_parity(
    legacy_correct: Callable[[dict], str],
    legacy_wrong: Callable[[dict], bool],
) -> None:
    question_for_legacy = {
        "question": _PROBE_QUESTION["question"],
        "options": list(_PROBE_QUESTION["options"]),
        "correct": _PROBE_QUESTION["correct"],
    }
    question_for_canonical = {
        "question": _PROBE_QUESTION["question"],
        "options": list(_PROBE_QUESTION["options"]),
        "correct": _PROBE_QUESTION["correct"],
    }
    try:
        legacy_correct_text = legacy_correct(question_for_legacy)
        canonical_correct_text = correct_text(question_for_canonical)
    except Exception as exc:
        raise RuntimeError("Legacy _correct_text parity probe failed") from exc
    if legacy_correct_text != canonical_correct_text:
        raise RuntimeError("Legacy _correct_text diverged from canonical answer history")

    for user_answer in (canonical_correct_text, "другой ответ"):
        legacy_item = {
            "user_answer": user_answer,
            "question_obj": {
                "question": _PROBE_QUESTION["question"],
                "options": list(_PROBE_QUESTION["options"]),
                "correct": _PROBE_QUESTION["correct"],
            },
        }
        canonical_item = {
            "user_answer": user_answer,
            "question_obj": {
                "question": _PROBE_QUESTION["question"],
                "options": list(_PROBE_QUESTION["options"]),
                "correct": _PROBE_QUESTION["correct"],
            },
        }
        try:
            legacy_value = legacy_wrong(legacy_item)
            canonical_value = is_wrong(canonical_item)
        except Exception as exc:
            raise RuntimeError("Legacy _is_wrong parity probe failed") from exc
        if legacy_value != canonical_value:
            raise RuntimeError("Legacy _is_wrong diverged from canonical answer history")


def install_legacy_bridge(legacy_module) -> None:
    """Replace transitional answer-history helpers after atomic parity checks."""
    legacy_correct = getattr(legacy_module, "_correct_text", None)
    legacy_wrong = getattr(legacy_module, "_is_wrong", None)
    if not callable(legacy_correct):
        raise TypeError("legacy module must expose callable _correct_text")
    if not callable(legacy_wrong):
        raise TypeError("legacy module must expose callable _is_wrong")

    _assert_parity(legacy_correct, legacy_wrong)
    legacy_module._correct_text = correct_text
    legacy_module._is_wrong = is_wrong


__all__ = ["correct_text", "install_legacy_bridge", "is_wrong"]
