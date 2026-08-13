from questions.chapter2.reviewed import CHAPTER2_REVIEWED_QUESTIONS
from questions.source_registry import SOURCE_CATALOG


def test_reviewed_chapter2_sources_resolve():
    for item in CHAPTER2_REVIEWED_QUESTIONS:
        assert item["sources"]
        assert not (set(item["sources"]) - set(SOURCE_CATALOG)), item["id"]
