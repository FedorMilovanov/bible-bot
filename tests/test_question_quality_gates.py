"""Pedagogical/editorial gates for the canonical question bank.

Hard content-truth checks live next to each reviewed chapter.  This module
guards properties that belong to the product's reader-facing voice (see
docs/QUESTION_BANK_AUDIT_2026-09.md):

* internal authoring/verification vocabulary must never appear in user-facing
  question text, options or explanations;
* every card must remain structurally valid after option shuffling;
* application, contested and project-position cards must never be promoted to
  competitive ranking.

A fixed grandfather set records the cards that still leaked pipeline language
at the 2026-09 audit.  It is a shrink-only list: fixing a card requires removing
its id here, and any new leak fails the gate.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from questions import POOL_REGISTRY

_REPO_ROOT = Path(__file__).resolve().parent.parent
_AUDIT_PATH = _REPO_ROOT / "scripts" / "audit_question_quality.py"
_spec = importlib.util.spec_from_file_location("audit_question_quality", _AUDIT_PATH)
audit = importlib.util.module_from_spec(_spec)
sys.modules["audit_question_quality"] = audit
_spec.loader.exec_module(audit)

REVIEWED_CHAPTER_POOLS = ("chapter2", "chapter3", "chapter4", "chapter5")

# Cards still containing internal pipeline vocabulary at the 2026-09 audit.
# Shrink-only: delete an id when the card is localized/released, never add one
# without a tracked editorial release (see the audit report).
PIPELINE_JARGON_GRANDFATHER: frozenset[str] = frozenset()  # retired 2026-09-14


def _user_text(card: dict) -> str:
    return "\n".join(
        [str(card.get("question", ""))]
        + [str(option) for option in card.get("options", [])]
        + [str(card.get("explanation", ""))]
    )


def _pipeline_leak_ids() -> dict[str, str]:
    leaks: dict[str, str] = {}
    for pool_key in REVIEWED_CHAPTER_POOLS:
        for card in POOL_REGISTRY[pool_key]:
            text = _user_text(card)
            import re as _re
            for name, pattern in audit.PIPELINE_MARKERS.items():
                if _re.search(pattern, text, _re.IGNORECASE):
                    leaks[str(card["id"])] = name
                    break
    return leaks


def test_pipeline_jargon_has_no_new_leaks_and_shrinks_only():
    leaks = _pipeline_leak_ids()
    new_leaks = sorted(set(leaks) - PIPELINE_JARGON_GRANDFATHER)
    assert not new_leaks, (
        "New internal-pipeline vocabulary in user-facing cards; localize it or "
        f"move it to private research metadata: {[(i, leaks[i]) for i in new_leaks]}"
    )
    repaired = sorted(PIPELINE_JARGON_GRANDFATHER - set(leaks))
    assert not repaired, (
        "These cards no longer leak pipeline vocabulary; remove them from "
        f"PIPELINE_JARGON_GRANDFATHER so the debt set shrinks: {repaired}"
    )


def test_every_card_has_four_options_and_valid_correct_index():
    for pool_key, pool in POOL_REGISTRY.items():
        ids = [card.get("id") for card in pool]
        assert len(ids) == len(set(ids)), f"duplicate ids in {pool_key}"
        for card in pool:
            options = card.get("options", [])
            correct = card.get("correct_index", card.get("correct"))
            assert isinstance(correct, int) and 0 <= correct < len(options) == 4, (
                pool_key,
                card.get("id"),
            )
            assert len(set(options)) == 4, (pool_key, card.get("id"))


def test_application_contested_and_project_never_competitive():
    forbidden = []
    for pool_key, pool in POOL_REGISTRY.items():
        for card in pool:
            if card.get("competitive") is True and (
                card.get("claim_type") == "application"
                or card.get("confidence") == "contested"
                or card.get("position") == "project"
            ):
                forbidden.append((pool_key, card.get("id")))
    assert not forbidden, forbidden
