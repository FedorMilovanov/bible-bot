from questions.chapter2.reviewed import (
    CHAPTER2_REVIEWED_QUESTIONS,
    CHAPTER2_REVIEW_QUARANTINE_IDS,
)


_RECENTLY_REVIEWED_IDS = {
    "ch2_hist_001",
    "ch2_hist_003",
    "ch2_hist_004",
    "ch2_theol_002",
    "ch2_theol_010",
}


def test_reviewed_chapter2_ids_are_unique_and_quarantine_is_empty():
    ids = [item["id"] for item in CHAPTER2_REVIEWED_QUESTIONS]
    assert CHAPTER2_REVIEWED_QUESTIONS
    assert len(ids) == len(set(ids))
    assert _RECENTLY_REVIEWED_IDS <= set(ids)
    assert not CHAPTER2_REVIEW_QUARANTINE_IDS
