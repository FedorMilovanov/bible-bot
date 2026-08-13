from questions.chapter2.reviewed import (
    CHAPTER2_REVIEWED_QUESTIONS,
    CHAPTER2_REVIEW_QUARANTINE_IDS,
)


_EXPECTED_EDITORIAL_QUARANTINE = {
    "ch2_hist_001",
    "ch2_hist_003",
    "ch2_hist_004",
    "ch2_theol_010",
}


def test_reviewed_chapter2_ids_are_unique_and_quarantine_is_explicit():
    ids = [item["id"] for item in CHAPTER2_REVIEWED_QUESTIONS]
    assert CHAPTER2_REVIEWED_QUESTIONS
    assert len(ids) == len(set(ids))
    assert "ch2_theol_002" in ids
    assert set(CHAPTER2_REVIEW_QUARANTINE_IDS) == _EXPECTED_EDITORIAL_QUARANTINE
    assert set(ids).isdisjoint(CHAPTER2_REVIEW_QUARANTINE_IDS)
