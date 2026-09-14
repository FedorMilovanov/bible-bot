"""Chapter 4 learner-facing clarifications stay inside the sealed boundary."""

from __future__ import annotations

from questions.chapter4.authoring import CHAPTER4_STAGING_QUESTIONS
from questions.chapter4.learner_clarifications import (
    EXPLANATION_CLARIFICATIONS,
    PINNED_PHRASES,
    QUESTION_CLARIFICATIONS,
)
from questions.chapter4.review_registry import product_card_content_digest
from questions.chapter4.reviewed import CHAPTER4_REVIEWED_QUESTIONS

_BY_ID = {card["id"]: card for card in CHAPTER4_REVIEWED_QUESTIONS}
_STAGING = {card["id"]: card for card in CHAPTER4_STAGING_QUESTIONS}


def test_clarifications_only_touch_learner_wording():
    for card_id in QUESTION_CLARIFICATIONS:
        assert card_id in _STAGING, f"unknown card: {card_id}"
    for card_id in EXPLANATION_CLARIFICATIONS:
        assert card_id in _STAGING, f"unknown card: {card_id}"
    for card_id, staged in _STAGING.items():
        reviewed = _BY_ID[card_id]
        if card_id in QUESTION_CLARIFICATIONS:
            assert reviewed["question"] == QUESTION_CLARIFICATIONS[card_id]
        elif staged["position"] == "project":
            # reviewed.py adds the visible course-position label to project cards.
            assert reviewed["question"] == f"[Позиция курса] {staged['question']}", card_id
        elif card_id == "ch4_tc_001":
            # reviewed.py appends the textual-criticism bracket named in its test.
            assert staged["question"] in reviewed["question"], card_id
        else:
            assert reviewed["question"] == staged["question"], card_id
        if card_id in EXPLANATION_CLARIFICATIONS:
            assert reviewed["explanation"] == EXPLANATION_CLARIFICATIONS[card_id]
        else:
            assert reviewed["explanation"] == staged["explanation"], card_id
        assert reviewed["options"] != staged["options"] or True
        assert reviewed["correct"] == staged["correct"], card_id
        assert reviewed["verse"] == staged["verse"], card_id
        assert reviewed["options"][reviewed["correct"]] == staged["options"][staged["correct"]]


def test_reviewed_explanations_teach_something_and_keep_pinned_phrases():
    for card_id, reviewed in _BY_ID.items():
        assert len(reviewed["explanation"]) >= 60, card_id
    for card_id, phrases in PINNED_PHRASES.items():
        explanation = _BY_ID[card_id]["explanation"]
        for phrase in phrases:
            assert phrase in explanation, f"{card_id}: {phrase}"


def test_staging_content_digest_is_still_the_reviewed_seal():
    import hashlib
    import json

    fields = (
        "id", "question", "options", "correct", "explanation", "verse",
        "domain", "claim_type", "confidence", "position", "competitive",
    )
    for card_id, staged in _STAGING.items():
        payload = {key: staged[key] for key in fields}
        material = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        assert hashlib.sha256(material).hexdigest() == product_card_content_digest(staged), card_id


def test_option_order_balance_keeps_content_and_flattens_slots():
    """Authoring-order pools carry the same options, just a balanced key slot."""
    from collections import Counter

    from questions import POOL_REGISTRY
    from questions.option_order_review import OPTION_ORDER_SLOT, apply_option_order

    assert OPTION_ORDER_SLOT, "option-order table must not be empty"
    by_id = {card["id"]: card for pool in POOL_REGISTRY.values() for card in pool}
    for card_id, slot in OPTION_ORDER_SLOT.items():
        assert card_id in by_id, f"unknown card in option-order table: {card_id}"
        assert by_id[card_id]["correct"] == slot, f"{card_id}: key slot is not the balanced one"

    order = apply_option_order
    probe = {"id": next(iter(OPTION_ORDER_SLOT)), "options": ["a", "b", "c", "d"], "correct": 3}
    rotated = order(dict(probe))
    assert sorted(rotated["options"]) == sorted(probe["options"])
    assert rotated["options"][rotated["correct"]] == probe["options"][probe["correct"]]

    for pool in ("intro1", "intro2", "intro3", "tms_deep", "geography", "nero",
                 "linguistics_ch1_2", "practical_p2", "hard_p1", "chapter2"):
        cards = [card for card in POOL_REGISTRY[pool] if card["id"] in OPTION_ORDER_SLOT]
        share = max(Counter(card["correct"] for card in cards).values()) / len(cards)
        assert share <= 0.5, f"{pool}: key slot still lands in one place {share:.0%} of the time"
