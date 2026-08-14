import json
from pathlib import Path

from questions.chapter4.authoring import CHAPTER4_STAGING_QUESTIONS
from questions.chapter4.review_registry import PRODUCT_REVIEW_BY_CARD_ID


ROOT = Path(__file__).resolve().parents[1]
AUDIT = json.loads(
    (ROOT / "data" / "chapter4-second-adversarial-pass-v2.json").read_text(
        encoding="utf-8"
    )
)


def _severe_length_ratio(card):
    correct = len(card["options"][card["correct"]])
    wrong_mean = sum(
        len(option)
        for index, option in enumerate(card["options"])
        if index != card["correct"]
    ) / 3
    return correct / max(1, wrong_mean)


def test_second_pass_is_post_first_green_and_covers_52_of_52():
    assert AUDIT["schema_version"] == 2
    assert AUDIT["first_green_exact_head"] == "2f9ae1cb03b5d6817efefe0988e7e619c2fd5afc"
    assert AUDIT["cards_reviewed"] == 52
    assert AUDIT["open_findings"] == 0
    assert len(AUDIT["records"]) == 52
    assert len({row["product_card_id"] for row in AUDIT["records"]}) == 52
    assert len({row["research_claim_id"] for row in AUDIT["records"]}) == 52


def test_second_pass_records_resolve_to_final_runtime_and_review_registry():
    cards = {card["id"]: card for card in CHAPTER4_STAGING_QUESTIONS}
    assert set(cards) == {row["product_card_id"] for row in AUDIT["records"]}
    for row in AUDIT["records"]:
        card = cards[row["product_card_id"]]
        review = PRODUCT_REVIEW_BY_CARD_ID[row["product_card_id"]]
        assert row["product_review_record_id"] == card["review_record_id"]
        assert row["product_review_record_id"] == review["product_review_record_id"]
        assert row["research_claim_id"] == review["research_claim_id"]
        assert row["decision"] in {"PASS", "PASS_AFTER_REVISION"}


def test_second_pass_cueing_finding_is_resolved_to_zero_severe_cases():
    assert AUDIT["finding_cards_before_fix"] == 16
    assert AUDIT["finding_categories"] == {
        "ANSWER_LENGTH_OR_OPTION_SHAPE_CUEING": 16
    }
    assert AUDIT["correct_option_longest_before"] == 36
    assert AUDIT["correct_option_longest_after"] == 29
    assert AUDIT["severe_correct_length_ratio_gt_1_8_before"] == 13
    assert AUDIT["severe_correct_length_ratio_gt_1_8_after"] == 0

    cards = CHAPTER4_STAGING_QUESTIONS
    longest_now = sum(
        len(card["options"][card["correct"]])
        == max(len(option) for option in card["options"])
        for card in cards
    )
    severe_now = [card["id"] for card in cards if _severe_length_ratio(card) > 1.8]
    assert longest_now == 29
    assert severe_now == []


def test_revised_cards_received_new_immutable_review_records():
    revised = {
        row["product_card_id"]
        for row in AUDIT["records"]
        if row["decision"] == "PASS_AFTER_REVISION"
    }
    assert len(revised) == 16
    assert revised == {
        "ch4_gr_003",
        "ch4_syn_001",
        "ch4_disputed_002",
        "ch4_course_001",
        "ch4_disputed_003",
        "ch4_disputed_004",
        "ch4_hist_001",
        "ch4_disputed_006",
        "ch4_text_012",
        "ch4_gr_005",
        "ch4_gr_006",
        "ch4_hist_003",
        "ch4_lex_002",
        "ch4_app_002",
        "ch4_app_004",
        "ch4_app_005",
    }
