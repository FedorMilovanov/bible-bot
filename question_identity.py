"""Canonical question identity algorithms and transitional legacy bridge."""
from __future__ import annotations

import hashlib
from collections.abc import Callable


_PROBE_QUESTIONS = (
    {"question": "", "options": []},
    {
        "question": "Кто написал 1 Петра?",
        "options": ["Пётр", "Павел", "Иоанн", "Иаков"],
    },
    {"question": "Grace & truth", "options": ["A", "B"]},
)


def stable_question_id(question: dict) -> str:
    """Return the historical text-only 12-character MD5 compatibility id."""
    text = question.get("question", "")
    return hashlib.md5(text.encode()).hexdigest()[:12]


def get_qid(question: dict) -> str:
    """Return the persisted 12-character SHA256 question identity."""
    text = question.get("question", "") + "".join(question.get("options", []))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def _assert_parity(
    legacy_function: Callable[[dict], str],
    canonical_function: Callable[[dict], str],
    *,
    name: str,
) -> None:
    for probe in _PROBE_QUESTIONS:
        legacy_input = {
            "question": probe["question"],
            "options": list(probe["options"]),
        }
        canonical_input = {
            "question": probe["question"],
            "options": list(probe["options"]),
        }
        try:
            legacy_value = legacy_function(legacy_input)
            canonical_value = canonical_function(canonical_input)
        except Exception as exc:
            raise RuntimeError(f"Legacy {name} parity probe failed") from exc
        if legacy_value != canonical_value:
            raise RuntimeError(f"Legacy {name} diverged from canonical question identity")


def install_legacy_bridge(legacy_module) -> None:
    """Replace transitional question-id helpers only after behavior matches."""
    legacy_stable = getattr(legacy_module, "stable_question_id", None)
    legacy_qid = getattr(legacy_module, "get_qid", None)
    if not callable(legacy_stable):
        raise TypeError("legacy module must expose callable stable_question_id")
    if not callable(legacy_qid):
        raise TypeError("legacy module must expose callable get_qid")

    # Validate every helper before mutating either legacy attribute so bridge
    # installation is atomic/fail-closed from the composition root's view.
    _assert_parity(
        legacy_stable,
        stable_question_id,
        name="stable_question_id",
    )
    _assert_parity(legacy_qid, get_qid, name="get_qid")

    legacy_module.stable_question_id = stable_question_id
    legacy_module.get_qid = get_qid


__all__ = ["get_qid", "install_legacy_bridge", "stable_question_id"]
