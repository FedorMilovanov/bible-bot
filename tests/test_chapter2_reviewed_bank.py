from questions.chapter2.reviewed import (
    CHAPTER2_REVIEWED_QUESTIONS,
    CHAPTER2_REVIEW_QUARANTINE_IDS,
)


def test_reviewed_chapter2_ids_are_unique():
    ids = [item["id"] for item in CHAPTER2_REVIEWED_QUESTIONS]
    assert CHAPTER2_REVIEWED_QUESTIONS
    assert len(ids) == len(set(ids))
    assert "ch2_theol_002" in ids
    assert not CHAPTER2_REVIEW_QUARANTINE_IDS
