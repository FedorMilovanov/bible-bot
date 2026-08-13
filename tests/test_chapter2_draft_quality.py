from questions import POOL_REGISTRY
from questions.chapter2 import (
    CHAPTER2_DRAFT_QUESTIONS,
    GREEK_2_1_10,
    INTERTEXT_2_1_10,
    TEXT_2_1_10,
)
from questions.source_registry import SOURCE_CATALOG


def test_chapter2_draft_has_stable_unique_ids_and_valid_schema():
    ids = []
    for item in CHAPTER2_DRAFT_QUESTIONS:
        qid = item.get("id")
        assert isinstance(qid, str) and qid.startswith("ch2_")
        ids.append(qid)
        assert isinstance(item.get("question"), str) and item["question"].strip()
        assert isinstance(item.get("options"), list) and len(item["options"]) == 4
        assert len(set(item["options"])) == 4
        assert isinstance(item.get("correct"), int)
        assert 0 <= item["correct"] < len(item["options"])
        assert isinstance(item.get("explanation"), str) and item["explanation"].strip()
        assert item.get("claim_type") in {"text", "greek", "history", "interpretation", "application"}
        assert item.get("confidence") in {"high", "medium", "contested"}
        assert item.get("position") in {"neutral", "project"}
        assert isinstance(item.get("competitive"), bool)
        assert isinstance(item.get("sources"), list) and item["sources"]

    assert len(ids) == len(set(ids))


def test_chapter2_draft_sources_resolve():
    for item in CHAPTER2_DRAFT_QUESTIONS:
        unresolved = set(item["sources"]) - set(SOURCE_CATALOG)
        assert not unresolved, (item["id"], unresolved)


def test_chapter2_greek_requires_text_and_morphology_and_is_not_ranked_yet():
    assert GREEK_2_1_10
    for item in GREEK_2_1_10:
        assert item["claim_type"] == "greek"
        assert {"sblgnt", "morphgnt_1peter"} <= set(item["sources"])
        assert item["competitive"] is False


def test_chapter2_direct_text_slice_is_explicitly_rankable_only_when_high_confidence():
    assert TEXT_2_1_10
    for item in TEXT_2_1_10:
        if item["competitive"]:
            assert item["claim_type"] == "text"
            assert item["confidence"] == "high"
            assert item["position"] == "neutral"


def test_chapter2_intertexts_have_lxx_or_text_evidence_and_rank_only_direct_links():
    assert INTERTEXT_2_1_10
    for item in INTERTEXT_2_1_10:
        assert "sblgnt" in item["sources"]
        assert "septuagint_bible" in item["sources"]
        if item["competitive"]:
            assert item["claim_type"] == "text"
            assert item["confidence"] == "high"
            assert item["position"] == "neutral"


def test_incomplete_chapter2_is_not_exposed_as_a_production_pool():
    assert "chapter2" not in POOL_REGISTRY
    assert not any(key.startswith("ch2_") for key in POOL_REGISTRY)
