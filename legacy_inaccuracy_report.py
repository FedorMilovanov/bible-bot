"""Durable acceptance for the inline “question inaccuracy” report path.

The legacy Telegram handler sends the report directly to the administrator and
therefore loses the event if the process dies around the send. This module gives
that ingress a deterministic idempotency key and persists it through the same
report outbox used by normal user reports. It contains no Telegram calls.
"""
from __future__ import annotations

import hashlib
from copy import deepcopy

from report_integrity import accept_report_once

_MAX_REPORT_TEXT = 2000


class LegacyInaccuracyReportInvalid(RuntimeError):
    """Raised when the clicked question cannot form stable report evidence."""


def _database():
    import database

    return database


def _required_attempt_id(value) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LegacyInaccuracyReportInvalid("inaccuracy report attempt id is invalid")
    return value.strip()


def _question_index(value) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LegacyInaccuracyReportInvalid(
            "inaccuracy report question index is invalid"
        )
    return value


def _question_snapshot(question: dict) -> tuple[str, list[str], int, str]:
    if not isinstance(question, dict):
        raise LegacyInaccuracyReportInvalid("inaccuracy report question is invalid")
    text = question.get("question")
    options = question.get("options")
    correct = question.get("correct")
    if not isinstance(text, str) or not text.strip():
        raise LegacyInaccuracyReportInvalid("inaccuracy report question text is invalid")
    if (
        not isinstance(options, list)
        or not options
        or any(not isinstance(option, str) for option in options)
    ):
        raise LegacyInaccuracyReportInvalid("inaccuracy report options are invalid")
    if (
        isinstance(correct, bool)
        or not isinstance(correct, int)
        or correct < 0
        or correct >= len(options)
    ):
        raise LegacyInaccuracyReportInvalid(
            "inaccuracy report correct-answer index is invalid"
        )
    question_id = hashlib.sha256(
        (text + "".join(options)).encode("utf-8")
    ).hexdigest()[:12]
    return text, options, correct, question_id


def stable_inaccuracy_report_id(
    *,
    user_id: int | str,
    attempt_id: str,
    question_index: int,
    question_id: str,
) -> str:
    """Return one stable report id for one user/attempt/question position."""
    attempt_id = _required_attempt_id(attempt_id)
    question_index = _question_index(question_index)
    if not isinstance(question_id, str) or not question_id:
        raise LegacyInaccuracyReportInvalid("inaccuracy report question id is invalid")
    owner = _database()._uid(user_id)
    digest = hashlib.sha256(
        f"{owner}\x1f{attempt_id}\x1f{question_index}\x1f{question_id}".encode()
    ).hexdigest()
    return f"inaccuracy-{digest[:40]}"


def _report_text(
    *,
    level_name: str,
    question_index: int,
    question_text: str,
    options: list[str],
    correct_index: int,
) -> str:
    safe_level = level_name.strip() if isinstance(level_name, str) and level_name.strip() else "—"
    options_text = "\n".join(
        f"  {index + 1}. {option}" for index, option in enumerate(options)
    )
    text = (
        "Неточность в вопросе\n\n"
        f"Тест: {safe_level}\n"
        f"Вопрос {question_index + 1}: {question_text}\n\n"
        f"Варианты:\n{options_text}\n\n"
        f"Правильный ответ в базе: {options[correct_index]}"
    )
    return text[:_MAX_REPORT_TEXT]


def accept_inaccuracy_report_once(
    *,
    user_id: int | str,
    username: str | None,
    first_name: str | None,
    attempt_id: str,
    question_index: int,
    question: dict,
    level_name: str | None = None,
) -> dict:
    """Persist one clicked inaccuracy report exactly once for its attempt/question."""
    attempt_id = _required_attempt_id(attempt_id)
    question_index = _question_index(question_index)
    question_text, options, correct_index, question_id = _question_snapshot(question)
    report_id = stable_inaccuracy_report_id(
        user_id=user_id,
        attempt_id=attempt_id,
        question_index=question_index,
        question_id=question_id,
    )
    context = {
        "kind": "question_inaccuracy",
        "attempt_id": attempt_id,
        "question_index": question_index,
        "question_id": question_id,
        "level_name": level_name or "",
        "question": deepcopy(question),
    }
    return accept_report_once(
        report_id=report_id,
        user_id=user_id,
        username=username,
        first_name=first_name,
        report_type="bug",
        text=_report_text(
            level_name=level_name or "",
            question_index=question_index,
            question_text=question_text,
            options=options,
            correct_index=correct_index,
        ),
        photo_file_id=None,
        context=context,
        update_cooldown=False,
    )
