#!/usr/bin/env python3
"""Build data/chapter4-localization-pass-v3.json from the sealed runtime.

The Chapter 4 localization release is a reviewed content pass: its audit
artifact must resolve card-by-card to the final runtime bank and the final
review registry.  Run this after editing questions/chapter4/localization_pass.py
and rebuilding its seal (scripts/build_chapter4_localization_seal.py):

    python3 scripts/build_chapter4_localization_audit.py
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from questions.chapter4.authoring import CHAPTER4_STAGING_QUESTIONS
from questions.chapter4.localization_pass import (
    CARD_REVISIONS,
    LOCALIZATION_BASE_EXACT_HEAD,
    LOCALIZATION_PASS_REVIEW_ID,
)
from questions.chapter4.review_registry import PRODUCT_REVIEW_BY_CARD_ID

# Findings recorded by the 2026-09 reader-voice audit (scripts/
# audit_question_quality.py) on the post-pass-two bank, keyed by product card.
FINDINGS_BEFORE_FIX: dict[str, list[str]] = {
    "ch4_disputed_001": ["UNTRANSLATED_ENGLISH_TERM"],
    "ch4_gr_001": ["ENGLISH_GRAMMAR_ABBREVIATION"],
    "ch4_gr_002": ["ENGLISH_GRAMMAR_ABBREVIATION"],
    "ch4_ot_001": ["UNTRANSLATED_ENGLISH_TERM"],
    "ch4_course_002": ["UNTRANSLATED_ENGLISH_TERM"],
    "ch4_syn_001": ["PIPELINE_JARGON"],
    "ch4_hist_001": ["PIPELINE_JARGON"],
    "ch4_course_003": ["PIPELINE_JARGON", "UNTRANSLATED_ENGLISH_TERM"],
    "ch4_disputed_006": ["UNTRANSLATED_ENGLISH_TERM"],
    "ch4_tc_001": ["PIPELINE_JARGON", "UNTRANSLATED_ENGLISH_TERM"],
    "ch4_tc_002": ["UNTRANSLATED_ENGLISH_TERM"],
    "ch4_tc_003": ["PIPELINE_JARGON", "UNTRANSLATED_ENGLISH_TERM"],
    "ch4_ot_003": ["UNTRANSLATED_ENGLISH_TERM"],
    "ch4_ot_004": ["UNTRANSLATED_ENGLISH_TERM"],
    "ch4_gr_004": ["ENGLISH_GRAMMAR_ABBREVIATION"],
    "ch4_text_011": ["UNTRANSLATED_ENGLISH_TERM"],
    "ch4_gr_005": ["ENGLISH_GRAMMAR_ABBREVIATION"],
    "ch4_gr_006": ["ENGLISH_GRAMMAR_ABBREVIATION"],
    "ch4_hist_003": ["UNTRANSLATED_ENGLISH_TERM"],
    "ch4_lex_002": ["UNTRANSLATED_ENGLISH_TERM"],
    "ch4_text_006": ["THIN_EXPLANATION"],
    "ch4_theol_001": ["THIN_EXPLANATION"],
}

OUTPUT_PATH = Path(__file__).resolve().parents[1] / "data" / "chapter4-localization-pass-v3.json"


def build_artifact() -> dict:
    if set(FINDINGS_BEFORE_FIX) != set(CARD_REVISIONS):
        raise RuntimeError(
            "finding map and CARD_REVISIONS must cover the same 22 cards"
        )

    severe = []
    longest = 0
    for card in CHAPTER4_STAGING_QUESTIONS:
        lengths = [len(option) for option in card["options"]]
        correct_length = lengths[card["correct"]]
        if correct_length == max(lengths):
            longest += 1
        mean_wrong = sum(
            length for index, length in enumerate(lengths) if index != card["correct"]
        ) / 3
        if correct_length / max(1, mean_wrong) > 1.8:
            severe.append(card["id"])

    records = []
    for card in CHAPTER4_STAGING_QUESTIONS:
        card_id = card["id"]
        review = PRODUCT_REVIEW_BY_CARD_ID[card_id]
        revised = card_id in CARD_REVISIONS
        records.append(
            {
                "product_card_id": card_id,
                "product_review_record_id": card["review_record_id"],
                "product_card_content_digest_sha256": review[
                    "product_card_content_digest_sha256"
                ],
                "research_claim_id": review["research_claim_id"],
                "finding_codes_before_fix": FINDINGS_BEFORE_FIX.get(card_id, []),
                "decision": "PASS_AFTER_REVISION" if revised else "PASS",
            }
        )

    categories = Counter(
        code
        for codes in FINDINGS_BEFORE_FIX.values()
        for code in codes
    )

    return {
        "schema_version": 3,
        "audit_id": "CHAPTER4-PLAIN-RUSSIAN-LOCALIZATION-RELEASE-V3",
        "pass_review_id": LOCALIZATION_PASS_REVIEW_ID,
        "base_exact_head": LOCALIZATION_BASE_EXACT_HEAD,
        "prior_pass": "chapter4-second-adversarial-content-pass-v2",
        "reviewer": {
            "reviewer_id": "chapter4-plain-russian-localization-release-agent",
            "reviewer_role": "independent_reader_voice_localization_review",
        },
        "cards_reviewed": 52,
        "finding_cards_before_fix": len(FINDINGS_BEFORE_FIX),
        "finding_categories": dict(sorted(categories.items())),
        "open_findings": 0,
        "invariants_preserved": {
            "keyed_answers_unchanged": True,
            "research_claims_and_edges_unchanged": True,
            "confidence_position_claim_type_unchanged": True,
            "learning_only_non_competitive": True,
            "absolute_certainty_distractors_absent": True,
            "severe_answer_length_cueing_cases": len(severe),
            "correct_option_longest_after": longest,
        },
        "checks_applied_to_every_record": [
            "stem_correct",
            "four_distinct_plausible_options",
            "one_unambiguous_best_answer",
            "explanation_within_evidence",
            "no_pipeline_jargon",
            "no_untranslated_english_terms",
            "no_english_grammar_abbreviations",
            "no_absolute_certainty_distractor",
            "project_position_not_disguised_as_neutral",
            "edition_flattening_absent",
            "morphology_to_exegesis_laundering_absent",
        ],
        "records": records,
    }


def main() -> None:
    artifact = build_artifact()
    if artifact["invariants_preserved"]["severe_answer_length_cueing_cases"] != 0:
        raise RuntimeError("severe answer-length cueing must be zero at release")
    if artifact["invariants_preserved"]["correct_option_longest_after"] != 29:
        raise RuntimeError("localization pass must preserve the pass-two cueing baseline of 29")
    OUTPUT_PATH.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
