import questions
from questions.chapter3.disputed_18_22 import DISPUTED_3_18_22
from questions.chapter3.greek_18_22 import GREEK_3_18_22
from questions.chapter3.sources import SOURCE_CATALOG as CHAPTER3_SOURCES


def _all_source_ids():
    return set(questions.SOURCE_CATALOG) | set(CHAPTER3_SOURCES)


def test_chapter3_foundation_is_not_in_production_registry():
    assert "chapter3" not in questions.POOL_REGISTRY
    assert not any(key.startswith("ch3_") for key in questions.POOL_REGISTRY)


def test_chapter3_foundation_ids_are_unique_and_sources_resolve():
    items = GREEK_3_18_22 + DISPUTED_3_18_22
    ids = [item["id"] for item in items]
    assert items
    assert len(ids) == len(set(ids))
    for item in items:
        assert set(item["sources"]) <= _all_source_ids(), item["id"]


def test_chapter3_greek_is_morphgnt_backed_and_noncompetitive():
    for item in GREEK_3_18_22:
        assert item["claim_type"] == "greek"
        assert item["confidence"] == "high"
        assert item["position"] == "neutral"
        assert item["competitive"] is False
        assert {"sblgnt", "morphgnt_1peter"} <= set(item["sources"])


def test_chapter3_disputed_map_cannot_enter_ranking():
    for item in DISPUTED_3_18_22:
        assert item["claim_type"] == "interpretation"
        assert item["confidence"] == "contested"
        assert item["competitive"] is False


def test_spirits_question_preserves_competing_conservative_readings():
    item = next(item for item in DISPUTED_3_18_22 if item["id"] == "ch3_disp_001")
    assert {"gty_1p3_18_20", "grudem_noah_1p3_19"} <= set(item["sources"])


def test_eperotema_question_requires_primary_greek_and_peer_reviewed_control():
    item = next(item for item in DISPUTED_3_18_22 if item["id"] == "ch3_disp_003")
    assert {"sblgnt", "morphgnt_1peter", "jts_crawford_1p3_21"} <= set(item["sources"])
