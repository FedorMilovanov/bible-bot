"""Chapter 4 third content-pass release: plain-Russian localization (ch4prv3_).

The sealed Chapter 4 bank could not be edited in place.  The localization
release instead issues new immutable review-record IDs and content digests for
the twenty-two cards flagged by the 2026-09 reader-voice audit.  These tests
fail closed: the data artifact must resolve card-by-card to the final runtime
bank and to the final review registry, no reader-voice English/jargon may
remain, and the pass-two keyed answers must be preserved byte-position-wise.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

from questions.chapter4.authoring import CHAPTER4_STAGING_QUESTIONS
from questions.chapter4.localization_pass import (
    CARD_REVISIONS,
    LOCALIZATION_BASE_EXACT_HEAD,
    LOCALIZATION_PASS_REVIEW_ID,
    REVIEW_RECORD_REVISIONS_3,
)
from questions.chapter4.review_registry import (
    PRODUCT_REVIEW_BY_CARD_ID,
    product_card_content_digest,
)
from questions.chapter4.reviewed import CHAPTER4_REVIEWED_QUESTIONS

ROOT = Path(__file__).resolve().parents[1]
AUDIT = json.loads(
    (ROOT / "data" / "chapter4-localization-pass-v3.json").read_text(encoding="utf-8")
)

_AUDIT_PATH = ROOT / "scripts" / "audit_question_quality.py"
_spec = importlib.util.spec_from_file_location("audit_question_quality", _AUDIT_PATH)
audit = importlib.util.module_from_spec(_spec)
sys.modules["audit_question_quality"] = audit
_spec.loader.exec_module(audit)

# Keyed answer indices of the twenty-one revised cards as sealed at the close
# of the second adversarial pass; localization must never reshuffle answers.
EXPECTED_CORRECT_INDEX = {
    "ch4_gr_001": 0,
    "ch4_gr_002": 1,
    "ch4_ot_001": 2,
    "ch4_course_002": 0,
    "ch4_syn_001": 3,
    "ch4_hist_001": 1,
    "ch4_course_003": 2,
    "ch4_disputed_006": 3,
    "ch4_tc_001": 1,
    "ch4_tc_002": 3,
    "ch4_tc_003": 3,
    "ch4_ot_003": 2,
    "ch4_ot_004": 0,
    "ch4_gr_004": 0,
    "ch4_text_011": 1,
    "ch4_gr_005": 3,
    "ch4_gr_006": 3,
    "ch4_hist_003": 0,
    "ch4_lex_002": 2,
    "ch4_text_006": 3,
    "ch4_theol_001": 3,
    "ch4_disputed_001": 1,
}


def test_artifact_metadata_and_coverage():
    assert AUDIT["schema_version"] == 3
    assert AUDIT["audit_id"] == "CHAPTER4-PLAIN-RUSSIAN-LOCALIZATION-RELEASE-V3"
    assert AUDIT["pass_review_id"] == LOCALIZATION_PASS_REVIEW_ID
    assert AUDIT["base_exact_head"] == LOCALIZATION_BASE_EXACT_HEAD
    assert AUDIT["cards_reviewed"] == 52
    assert AUDIT["finding_cards_before_fix"] == 22
    assert AUDIT["open_findings"] == 0
    assert AUDIT["finding_categories"] == {
        "ENGLISH_GRAMMAR_ABBREVIATION": 5,
        "PIPELINE_JARGON": 5,
        "THIN_EXPLANATION": 2,
        "UNTRANSLATED_ENGLISH_TERM": 13,
    }
    assert len(AUDIT["records"]) == 52
    assert len({row["product_card_id"] for row in AUDIT["records"]}) == 52
    assert len({row["research_claim_id"] for row in AUDIT["records"]}) == 52


def test_revised_set_matches_seal_and_artifact():
    assert set(REVIEW_RECORD_REVISIONS_3) == set(CARD_REVISIONS)
    assert set(EXPECTED_CORRECT_INDEX) == set(CARD_REVISIONS)
    revised_artifact = {
        row["product_card_id"]
        for row in AUDIT["records"]
        if row["decision"] == "PASS_AFTER_REVISION"
    }
    assert revised_artifact == set(CARD_REVISIONS)
    assert len(revised_artifact) == 22


def test_records_resolve_to_final_runtime_and_registry():
    cards = {card["id"]: card for card in CHAPTER4_STAGING_QUESTIONS}
    for row in AUDIT["records"]:
        card = cards[row["product_card_id"]]
        review = PRODUCT_REVIEW_BY_CARD_ID[row["product_card_id"]]
        assert row["product_review_record_id"] == card["review_record_id"]
        assert row["product_review_record_id"] == review["product_review_record_id"]
        assert row["product_card_content_digest_sha256"] == review[
            "product_card_content_digest_sha256"
        ]
        assert (
            row["product_card_content_digest_sha256"]
            == product_card_content_digest(card)
        )
        assert row["research_claim_id"] == review["research_claim_id"]
        assert review["review_decision"] == "APPROVE_NORMAL_LEARNING_ONLY"


def test_record_id_generation_marks_third_pass_cards():
    for card_id, (record_id, digest) in REVIEW_RECORD_REVISIONS_3.items():
        assert record_id == f"ch4prv3_{card_id}_{digest[:12]}"
    for card in CHAPTER4_STAGING_QUESTIONS:
        if card["id"] in CARD_REVISIONS:
            assert card["review_record_id"].startswith("ch4prv3_")
        else:
            assert card["review_record_id"].startswith("ch4prv2_")


def test_keyed_answer_indices_are_frozen():
    cards = {card["id"]: card for card in CHAPTER4_STAGING_QUESTIONS}
    for card_id, expected in EXPECTED_CORRECT_INDEX.items():
        assert cards[card_id]["correct"] == expected
    # Every revised card still has four distinct options.
    for card_id in CARD_REVISIONS:
        options = cards[card_id]["options"]
        assert len(options) == 4
        assert len({option.casefold() for option in options}) == 4


# English parsing vocabulary that a plain-Russian bank must never display.
_ENGLISH_GRAMMAR_WORDS = {
    "aorist", "present", "perfect", "future", "imperfect", "active", "passive",
    "middle", "participle", "imperative", "indicative", "subjunctive",
    "optative", "infinitive", "nominative", "genitive", "dative",
    "accusative", "vocative", "plural", "singular", "deponent",
    "application", "project", "stewardship", "consensus", "scholarly",
    "cessationism", "preterist", "datum", "qualifier", "family",
    "forgiving", "forbearing", "concealment", "impunity", "postmortem",
    "alternative", "morphology", "prophetic", "background",
}
_LATIN_TOKEN = __import__("re").compile(r"[A-Za-zÀ-ɏ][A-Za-zÀ-ɏ'-]+")


def _latin_tokens(text):
    return {token.casefold() for token in _LATIN_TOKEN.findall(text)}


def test_no_pipeline_jargon_or_untranslated_english_remains():
    # The canonical audit script exposes pipeline vocabulary as regex markers;
    # every sealed staged card must be free of it.
    import re
    for card_id, row in ((c["id"], c) for c in CHAPTER4_STAGING_QUESTIONS):
        text = "\n".join(
            (row["question"], *[str(o) for o in row["options"]], row["explanation"])
        )
        for name, pattern in audit.PIPELINE_MARKERS.items():
            assert not re.search(pattern, text), f"{card_id} leaks pipeline term {name!r}"
    # The localization pass must remove English parsing/scholarly vocabulary
    # from every card it sealed (keyed answer position preserved).
    for card_id in CARD_REVISIONS:
        row = next(c for c in CHAPTER4_STAGING_QUESTIONS if c["id"] == card_id)
        text = "\n".join(
            (row["question"], *[str(o) for o in row["options"]], row["explanation"])
        )
        leftover = sorted(_latin_tokens(text) & _ENGLISH_GRAMMAR_WORDS)
        assert not leftover, f"{card_id} leaks English terms: {leftover}"


def test_runtime_chapter4_has_no_untranslated_english_learner_wording():
    # Covers wording added after the sealed localization pass (learner
    # clarifications and option balance): the product the learner sees is fully
    # Russian apart from names, editions and standard abbreviations.
    allowed = {
        "morphgnt", "lxx", "na", "na28", "ecm", "sblgnt", "cbgm", "ubs", "tms",
        "sinaiticus", "codex", "atkinson", "horrell", "williams", "byrley",
        "nero", "domitian", "trajan", "stanojevic", "net", "lsj", "jts",
        "christians",
    }
    for card in CHAPTER4_REVIEWED_QUESTIONS:
        text = "\n".join(
            (card["question"], *[str(o) for o in card["options"]], card["explanation"])
        )
        leftover = sorted(_latin_tokens(text) & _ENGLISH_GRAMMAR_WORDS)
        assert not leftover, f"{card['id']} runtime English terms: {leftover}"
        for token in _latin_tokens(text) - allowed:
            assert any(ch.isupper() for ch in token) or len(token) <= 3, (
                f"{card['id']} lowercase Latin token {token!r}"
            )


def test_revised_explanations_are_teaching_and_absent_from_artifact_findings():
    for thin_card_id in ("ch4_text_006", "ch4_theol_001"):
        card = next(c for c in CHAPTER4_STAGING_QUESTIONS if c["id"] == thin_card_id)
        assert len(card["explanation"]) >= 120
    for row in AUDIT["records"]:
        assert row["finding_codes_before_fix"] == [] or row["decision"] == "PASS_AFTER_REVISION"


def test_cueing_and_learning_only_invariants_hold_after_release():
    invariants = AUDIT["invariants_preserved"]
    assert invariants["severe_answer_length_cueing_cases"] == 0
    assert invariants["correct_option_longest_after"] == 29
    severe = []
    longest = 0
    for card in CHAPTER4_STAGING_QUESTIONS:
        lengths = [len(option) for option in card["options"]]
        if lengths[card["correct"]] == max(lengths):
            longest += 1
        mean_wrong = sum(
            length for index, length in enumerate(lengths) if index != card["correct"]
        ) / 3
        if lengths[card["correct"]] / max(1, mean_wrong) > 1.8:
            severe.append(card["id"])
    assert severe == []
    assert longest == 29
    assert all(card["competitive"] is False for card in CHAPTER4_STAGING_QUESTIONS)
    assert len(CHAPTER4_REVIEWED_QUESTIONS) == 52


def test_project_and_tc_001_reviewed_decorations_survive():
    cards = {card["id"]: card for card in CHAPTER4_REVIEWED_QUESTIONS}
    assert cards["ch4_course_003"]["question"].startswith("[Позиция курса]")
    assert "SBLGNT" in cards["ch4_tc_001"]["question"]
    assert "ECM/NA28" in cards["ch4_tc_001"]["question"]
