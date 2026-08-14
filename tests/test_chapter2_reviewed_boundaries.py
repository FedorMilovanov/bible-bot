from questions.chapter2.reviewed import CHAPTER2_REVIEWED_QUESTIONS


def test_reviewed_chapter2_epistemic_boundaries_hold():
    for item in CHAPTER2_REVIEWED_QUESTIONS:
        if item["position"] == "project":
            assert item["question"].startswith("[Позиция курса]")
            assert item["competitive"] is False
        if item["confidence"] == "contested":
            assert item["competitive"] is False
        if item["claim_type"] in {"history", "application"}:
            assert item["competitive"] is False
