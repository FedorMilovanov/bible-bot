"""Quality gates for the production question bank.

These tests enforce the checks that structural content tests cannot see: whether
an answer can be guessed from option shape, whether learner-facing text leaks
internal research vocabulary, and whether the recorded quality debt is exactly
the debt the project has reviewed.

The audit engine itself lives in ``scripts/audit_question_quality.py``; the
ratchet baseline lives in ``data/question-quality-budget.json``.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from questions import POOL_REGISTRY
from questions.intro_balance_review import BALANCED_DISTRACTORS, apply_intro_balance
from questions.option_balance_review import BALANCED_OPTIONS
from questions.content_truth_review import REVIEW_OVERRIDES
from questions.review_composition_2026_09 import REVIEW_LAYERS

ROOT = Path(__file__).resolve().parents[1]
BUDGET_PATH = ROOT / "data" / "question-quality-budget.json"
AUDIT_SCRIPT = ROOT / "scripts" / "audit_question_quality.py"
AUDIT_REPORT_PATH = ROOT / "docs" / "QUESTION_BANK_AUDIT.md"

# Accepted blocker debt. The previous Chapter-5 release-repin list
# (ch5_w3q_050/075/111/125/127/143) was closed by the reviewed option/stem
# realignment repin recorded in docs/CHAPTER5_RELEASE_AUDIT.md, and no new
# blocker debt exists. The set is deliberately explicit and cannot grow
# silently.
CHAPTER5_REPIN_DEBT = frozenset()

LENGTH_LEAK_RATIO = 1.35
LENGTH_LEAK_MIN_DELTA = 12


def _run_audit(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(AUDIT_SCRIPT), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


def _bank_by_id() -> dict[str, dict]:
    return {
        item["id"]: item
        for pool in POOL_REGISTRY.values()
        for item in pool
    }


def test_question_quality_budget_is_respected():
    """The audit must pass its ratchet: no counter may grow past the baseline."""
    result = _run_audit("--check")
    assert result.returncode == 0, result.stdout + result.stderr


def test_tracked_question_bank_audit_is_fresh(tmp_path: Path):
    generated = tmp_path / "QUESTION_BANK_AUDIT.md"
    result = _run_audit("--report", str(generated))
    assert result.returncode == 0, result.stdout + result.stderr
    assert generated.read_text(encoding="utf-8") == AUDIT_REPORT_PATH.read_text(encoding="utf-8"), (
        "docs/QUESTION_BANK_AUDIT.md is stale; regenerate it with "
        "python scripts/audit_question_quality.py --report docs/QUESTION_BANK_AUDIT.md"
    )


def test_blocker_debt_is_exactly_the_reviewed_chapter5_repin_list(tmp_path: Path):
    report_path = tmp_path / "audit.json"
    result = _run_audit("--json", str(report_path))
    assert result.returncode == 0, result.stdout + result.stderr

    report = json.loads(report_path.read_text(encoding="utf-8"))
    blockers = {
        finding["item_id"]
        for finding in report["findings"]
        if finding["severity"] == "blocker"
    }
    assert blockers == CHAPTER5_REPIN_DEBT

    budget = json.loads(BUDGET_PATH.read_text(encoding="utf-8"))
    assert set(budget["accepted_blockers"]) == CHAPTER5_REPIN_DEBT


def test_review_layers_never_drop_fields_an_earlier_layer_reviewed():
    """A partial evidence top-up must not erase earlier wording fixes."""
    union: dict[str, set[str]] = {}
    for layer in REVIEW_LAYERS:
        for question_id, fields in layer.items():
            union.setdefault(question_id, set()).update(fields)

    for question_id, fields in union.items():
        missing = fields - set(REVIEW_OVERRIDES[question_id])
        assert not missing, f"{question_id} lost fields {sorted(missing)}"


def test_contested_intro_card_keeps_its_hedged_wording():
    """The 'noble lie' card is contested; its answer must stay a hedge."""
    item = next(q for q in POOL_REGISTRY["intro2"] if q["id"] == "intro2_04")
    keyed = item["options"][item["correct"]]
    assert "могли знать" in keyed
    assert "невозможно" not in keyed
    assert "не делает позднее авторство логически невозможным" in item["explanation"]
    assert len(item["sources"]) >= 2


def test_option_balance_replaces_only_distractors():
    """Every balanced card keeps the reviewed key and loses the length cue."""
    bank = _bank_by_id()
    assert BALANCED_OPTIONS, "balance table must not be empty"
    for question_id, replacements in BALANCED_OPTIONS.items():
        item = bank[question_id]
        # apply_option_balance refuses to rewrite the reviewed key slot when it
        # runs; afterwards option_order_review rotates the slots, so the table is
        # verified by content: every rewritten distractor is present and none of
        # them is the keyed option.
        keyed = item["options"][item["correct"]]
        for index, text in replacements.items():
            assert text in item["options"], f"{question_id}: option {index} text missing"
            assert text != keyed, f"{question_id}: override names the key"

        lengths = [len(option) for option in item["options"]]
        correct = lengths[item["correct"]]
        longest_other = max(
            length for index, length in enumerate(lengths) if index != item["correct"]
        )
        assert correct - longest_other < LENGTH_LEAK_MIN_DELTA or (
            correct < longest_other * LENGTH_LEAK_RATIO
        ), f"{question_id}: the key is still the long way to the answer"


def test_project_positions_are_visibly_labelled():
    """A project-position card must show the course position to the learner."""
    for pool_name, items in POOL_REGISTRY.items():
        for item in items:
            if item.get("position") != "project":
                continue
            stem = item["question"]
            assert stem.startswith("[Позиция курса]") or "курс" in stem.casefold(), (
                f"{pool_name}/{item['id']}: project position is invisible"
            )


def test_intro_balance_replaces_only_distractors():
    """The introduction courses keep their reviewed key and lose the cue."""
    bank = _bank_by_id()
    assert BALANCED_DISTRACTORS, "intro balance table must not be empty"
    for question_id, replacements in BALANCED_DISTRACTORS.items():
        item = bank[question_id]
        distractors = [
            option
            for index, option in enumerate(item["options"])
            if index != item["correct"]
        ]
        # option_order_review rotates options, so compare content, not slot order.
        assert sorted(distractors) == sorted(replacements), f"{question_id}: distractors drifted"
        assert item["options"][item["correct"]] not in replacements
        assert apply_intro_balance  # the pass is registered in questions._canonical
