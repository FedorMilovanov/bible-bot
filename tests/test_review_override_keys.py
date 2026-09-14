"""Guard the reviewed-override tables against duplicate keys.

A duplicate key inside a Python dict literal keeps only the last entry and drops
the earlier one silently. That happened twice during this review: a two-key
evidence top-up erased the reviewed wording of ``intro2_04``, and a second
``ch3_disp_003`` entry erased its plain-language explanation, so a learner-facing
card fell back to internal workflow English without any test failing.

The override tables are plain literals, so the cheapest reliable guard is a
syntax-level check: no dict literal under ``questions/`` or ``scripts/`` may
repeat a key.
"""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCANNED_ROOTS = ("questions", "scripts")


def _duplicate_keys(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    duplicates: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        seen: set[str] = set()
        for key in node.keys:
            if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
                continue
            if key.value in seen:
                duplicates.append(f"{key.value} (line {key.lineno})")
            seen.add(key.value)
    return duplicates


def test_no_duplicate_keys_in_review_tables():
    offenders: list[str] = []
    for root_name in SCANNED_ROOTS:
        for path in sorted((ROOT / root_name).rglob("*.py")):
            duplicates = _duplicate_keys(path)
            if duplicates:
                offenders.append(f"{path.relative_to(ROOT)}: {', '.join(duplicates)}")
    assert offenders == [], "duplicate dict keys drop earlier review fields:\n" + "\n".join(
        offenders
    )
