from questions.chapter2.reviewed import CHAPTER2_REVIEWED_QUESTIONS


def test_reviewed_chapter2_ids_are_unique():
    ids = [item["id"] for item in CHAPTER2_REVIEWED_QUESTIONS]
    assert CHAPTER2_REVIEWED_QUESTIONS
    assert len(ids) == len(set(ids))
    assert "ch2_theol_002" not in ids
