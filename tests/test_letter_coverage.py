"""Every verse of 1 Peter is asked about somewhere in the bank.

A course on the letter can only teach what its questions touch, so coverage is part
of depth: 105 verses (25 + 25 + 22 + 19 + 14) and, measured from the cards' verse
anchors, all of them are covered. The anchor is the machine-readable half of
coverage - a card may also discuss a verse in its explanation - so a missing anchor
is worth a deliberate decision rather than a silent drift. Anchors that name
another book (a Psalm, Isaiah, Proverbs) are not coverage of 1 Peter, and a
reference past the end of a chapter is an anchor error worth seeing.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from questions import get_pool_by_key  # noqa: E402
from scripts.audit_question_quality import (  # noqa: E402
    LEAF_POOLS,
    LETTER_CHAPTER_VERSES,
    letter_coverage,
)


def _pools() -> dict[str, list[dict]]:
    return {pool: list(get_pool_by_key(pool)) for pool in LEAF_POOLS}


def test_the_bank_covers_every_verse_of_first_peter():
    """105/105 today; a drop means adding a card or recording the gap."""
    coverage = letter_coverage(_pools())
    assert coverage["total"] == 105
    assert sum(LETTER_CHAPTER_VERSES) == coverage["total"]
    assert coverage["missing"] == {}, (
        "anchors no longer cover "
        f"{coverage['missing']}: add a card for the verse or say in the audit why it stays out"
    )
    assert coverage["covered"] == coverage["total"]


def test_no_anchor_points_past_the_end_of_its_chapter():
    """A verse reference the chapter does not have would be a data error."""
    assert letter_coverage(_pools())["out_of_range"] == []


# The bank writes verse ranges with an en dash in the anchors; the tests build
# them from the escape so the file stays free of characters linters read as typos.
EN_DASH = "\u2013"


def test_verse_ranges_count_every_verse_they_span():
    """3:10-12 covers three verses; 1:24-30 stops at the chapter's last verse."""
    pools = {
        "synthetic": [
            {"id": "a", "verse": f"1 Пет. 3:10{EN_DASH}12"},
            {"id": "b", "verse": "4:18"},
            {"id": "c", "verse": "1:24-30"},
        ]
    }
    coverage = letter_coverage(pools)
    per_chapter = coverage["per_chapter"]
    assert per_chapter[3] == 3
    assert per_chapter[4] == 1
    assert per_chapter[1] == 2  # 24 and 25 exist, 26-30 are clamped away
    assert coverage["out_of_range"] == ["c: 1:24-30"]
    assert coverage["missing"][2] == list(range(1, 26))


def test_cross_chapter_ranges_cover_both_chapters():
    """1:23-2:2 runs to the end of chapter 1 and through the start of chapter 2."""
    coverage = letter_coverage({"synthetic": [{"id": "a", "verse": "1 Пет. 1:23-2:2"}]})
    assert coverage["per_chapter"][1] == 3  # 23, 24, 25
    assert coverage["per_chapter"][2] == 2  # 1, 2
    assert coverage["out_of_range"] == []


def test_coverage_ignores_anchors_of_other_books():
    """Chapter:verse of Isaiah or Proverbs must not count as 1 Peter coverage."""
    pools = {
        "synthetic": [
            {"id": "a", "verse": "Ис. 53:5"},
            {"id": "b", "verse": "Притч. 3:25"},
            {"id": "c", "verse": "1 Пет. 3:6 / Притч. 3:25"},
            {"id": "d", "verse": f"Исх. 24:3{EN_DASH}8; Чис. 19"},
        ]
    }
    coverage = letter_coverage(pools)
    assert coverage["per_chapter"][3] == 1  # only 3:6 from the mixed anchor
    assert coverage["covered"] == 1
    assert coverage["out_of_range"] == []
