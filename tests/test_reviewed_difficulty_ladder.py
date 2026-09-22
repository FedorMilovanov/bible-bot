"""Regression contract for the human-reviewed cognitive difficulty ladder."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import questions
from questions.level_policy import SOURCE_REVIEWED, level_for
from questions.reviewed_difficulty_2026_09 import (
    LEVEL_REVIEW_ID,
    REVIEWED_LEVEL_BY_ID,
    REVIEWED_LEVEL_POOLS,
    reviewed_level_counts,
)


ROOT = Path(__file__).resolve().parents[1]
AUDIT_SCRIPT = ROOT / "scripts" / "audit_question_quality.py"


def _audit_module():
    name = "_difficulty_audit"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, AUDIT_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _cards(pool: str) -> list[dict]:
    return questions.POOL_REGISTRY[pool]


def test_review_map_is_complete_for_every_exposed_difficulty_pool():
    assert LEVEL_REVIEW_ID == "chapter1-practical-cognitive-levels-2026-09-v3"
    ids = {
        card["id"]
        for pool in REVIEWED_LEVEL_POOLS
        for card in _cards(pool)
    }
    assert ids == set(REVIEWED_LEVEL_BY_ID)
    assert len(ids) == 392
    assert reviewed_level_counts() == {"base": 113, "core": 192, "advanced": 87}


def test_production_cards_carry_reviewed_level_and_provenance():
    for pool in REVIEWED_LEVEL_POOLS:
        for card in _cards(pool):
            expected = REVIEWED_LEVEL_BY_ID[card["id"]]
            assert card["level"] == expected
            level, source = level_for(pool, card)
            assert level == expected
            assert source == SOURCE_REVIEWED


def test_easy_courses_are_true_entry_level():
    for pool in ("easy_p1", "easy_p2"):
        assert {card["level"] for card in _cards(pool)} == {"base"}


def test_legacy_medium_and_hard_names_do_not_override_cognitive_review():
    medium_levels = {
        card["level"]
        for pool in ("medium_p1", "medium_p2")
        for card in _cards(pool)
    }
    hard_levels = {
        card["level"]
        for pool in ("hard_p1", "hard_p2")
        for card in _cards(pool)
    }
    assert medium_levels == {"base", "core", "advanced"}
    assert hard_levels == {"base", "core", "advanced"}

    by_id = {
        card["id"]: card
        for pool in ("hard_p1", "hard_p2")
        for card in _cards(pool)
    }
    # These are useful textual checks, but they are not advanced just because
    # their historical pool was named "hard".
    assert by_id["hard_04"]["level"] == "base"
    assert by_id["hard_05"]["level"] == "base"
    assert by_id["hard_07"]["level"] == "base"

    # These genuinely require intertextual, historical, lexical, or multi-step
    # exegetical discrimination.
    for question_id in (
        "hard_13",
        "hard_es2_08",
        "hard17_04",
        "hard17_09",
        "hard_deep_18",
        "hard_deep_20",
    ):
        assert by_id[question_id]["level"] == "advanced"


def test_tms_sublevels_match_the_reviewed_cognitive_task():
    cards = {card["id"]: card for card in _cards("tms_deep")}
    for number in range(1, 4):
        assert cards[f"tms1_easy_0{number}"]["level"] == "base"
    for number in range(1, 5):
        assert cards[f"tms1_med_0{number}"]["level"] == "core"
    for number in range(1, 6):
        assert cards[f"tms1_hard_0{number}"]["level"] == "advanced"
    for number in range(1, 4):
        assert cards[f"tms1_app_0{number}"]["level"] == "core"


def test_context_and_greek_labels_do_not_fake_difficulty():
    nero = {card["id"]: card for card in _cards("nero")}
    geography = {card["id"]: card for card in _cards("geography")}
    linguistics = {
        card["id"]: card
        for pool in ("linguistics_ch1", "linguistics_ch1_2", "linguistics_ch1_3")
        for card in _cards(pool)
    }
    intro = {
        card["id"]: card
        for pool in ("intro1", "intro2", "intro3")
        for card in _cards(pool)
    }

    # Names, dates and direct lexical recognition remain base even when the
    # subject sounds scholarly.
    for question_id in ("nero_01", "nero_03", "nero_07", "geo_03", "ling1_02", "ling2_07"):
        source = nero | geography | linguistics
        assert source[question_id]["level"] == "base"

    # Advanced means evidence discrimination or multi-step interpretation.
    for question_id in (
        "ling1_07", "ling2_06", "ling3_04",
        "intro1_12", "intro2_15", "intro2_16",
        "intro3_04", "intro3_08", "intro3_13", "intro3_16",
    ):
        source = linguistics | intro
        assert source[question_id]["level"] == "advanced"


def test_practical_courses_are_not_mistaken_for_advanced_by_topic():
    cards = {
        card["id"]: card
        for pool in ("practical_p1", "practical_p2")
        for card in _cards(pool)
    }
    assert len(cards) == 90
    assert sum(card["level"] == "base" for card in cards.values()) == 1
    assert sum(card["level"] == "core" for card in cards.values()) == 74
    assert sum(card["level"] == "advanced" for card in cards.values()) == 15

    assert cards["prac_06"]["level"] == "base"
    for question_id in ("prac_01", "pracSit_01", "prac17_22", "prac17_es2_10"):
        assert cards[question_id]["level"] == "core"
    for question_id in (
        "prac_08",
        "prac13_12",
        "prac_es2_07",
        "prac17_11",
        "pracSit_13",
        "prac17_es2_08",
    ):
        assert cards[question_id]["level"] == "advanced"


def test_no_reviewed_advanced_card_is_a_recall_only_audit_finding():
    audit = _audit_module().audit()
    advanced_ids = {
        card["id"]
        for pool in REVIEWED_LEVEL_POOLS
        for card in _cards(pool)
        if card["level"] == "advanced"
    }
    recall_ids = {
        finding.item_id
        for finding in audit.findings
        if finding.check_id == "depth.recall_only"
    }
    assert not (advanced_ids & recall_ids), sorted(advanced_ids & recall_ids)


def test_unreviewed_chapter_courses_remain_honest_proxy_not_fake_levels():
    for pool in ("chapter2", "chapter3", "chapter4", "chapter5"):
        assert all("level" not in card for card in _cards(pool))
