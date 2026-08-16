"""Canonical helpers for interpreting recorded quiz answer history."""
from __future__ import annotations


def correct_text(question: dict) -> str:
    """Return the canonical correct option text using the historical semantics."""
    return question["options"][question["correct"]]


def is_wrong(item: dict) -> bool:
    """Return whether one recorded user answer differs from the correct text."""
    return item["user_answer"] != correct_text(item["question_obj"])


def build_progress_bar(
    current: int,
    total: int,
    answered_questions: list | None = None,
) -> str:
    """Render the historical answer-aware quiz progress bar exactly."""
    bar = ""
    for index in range(total):
        if answered_questions and index < len(answered_questions):
            item = answered_questions[index]
            user_answer = item.get("user_answer", "")
            correct = correct_text(item["question_obj"])
            bar += "🟩" if user_answer == correct else "🟥"
        elif index == current - 1:
            bar += "🟨"
        else:
            bar += "⬜"
    return bar


__all__ = ["build_progress_bar", "correct_text", "is_wrong"]
