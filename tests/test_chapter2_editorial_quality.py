import re
from difflib import SequenceMatcher

from questions.chapter2.reviewed import CHAPTER2_REVIEWED_QUESTIONS


def _normalize(text: str) -> str:
    return " ".join(re.findall(r"[\w\u0370-\u03ff\u1f00-\u1fff]+", text.casefold()))


def test_reviewed_options_are_nonempty_and_unique_after_normalization():
    for item in CHAPTER2_REVIEWED_QUESTIONS:
        options = item["options"]
        assert len(options) == 4, item["id"]
        normalized = [_normalize(option) for option in options]
        assert all(normalized), item["id"]
        assert len(normalized) == len(set(normalized)), item["id"]
        assert 0 <= item["correct"] < len(options), item["id"]


def test_reviewed_questions_have_no_exact_or_extreme_near_duplicates():
    normalized = [
        (item["id"], _normalize(item["question"]))
        for item in CHAPTER2_REVIEWED_QUESTIONS
    ]
    for index, (left_id, left) in enumerate(normalized):
        for right_id, right in normalized[index + 1 :]:
            assert left != right, (left_id, right_id)
            ratio = SequenceMatcher(None, left, right).ratio()
            assert ratio < 0.96, (left_id, right_id, ratio)


def test_noncompetitive_review_layers_cannot_enter_ranking_by_metadata():
    for item in CHAPTER2_REVIEWED_QUESTIONS:
        if item["claim_type"] in {"greek", "history", "application"}:
            assert item["competitive"] is False, item["id"]
        if item["position"] == "project":
            assert item["competitive"] is False, item["id"]
