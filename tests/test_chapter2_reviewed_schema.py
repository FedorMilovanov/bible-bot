from questions.chapter2.reviewed import CHAPTER2_REVIEWED_QUESTIONS


def test_reviewed_chapter2_schema_is_valid():
    for item in CHAPTER2_REVIEWED_QUESTIONS:
        assert item["id"].startswith("ch2_")
        assert item["question"].strip()
        assert len(item["options"]) == 4
        assert len(item["options"]) == len(set(item["options"]))
        assert isinstance(item["correct"], int)
        assert 0 <= item["correct"] < 4
        assert item["explanation"].strip()
        assert item["claim_type"] in {"text", "greek", "history", "interpretation", "application"}
        assert item["confidence"] in {"high", "medium", "contested"}
        assert item["position"] in {"neutral", "project"}
        assert isinstance(item["competitive"], bool)
