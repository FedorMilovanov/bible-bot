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


def test_stable_foundation_ids_remain_present():
    greek_ids = {item["id"] for item in GREEK_3_18_22}
    disputed_ids = {item["id"] for item in DISPUTED_3_18_22}
    assert {f"ch3_gr_{n:03d}" for n in range(1, 7)} <= greek_ids
    assert {f"ch3_disp_{n:03d}" for n in range(1, 5)} <= disputed_ids


def test_chapter3_greek_is_morphgnt_backed_and_noncompetitive():
    for item in GREEK_3_18_22:
        assert item["claim_type"] == "greek"
        assert item["confidence"] == "high"
        assert item["position"] == "neutral"
        assert item["competitive"] is False
        assert set(item["sources"]) == {"sblgnt", "morphgnt_1peter"}
        assert "morphgnt" in item


def test_chapter3_disputed_map_cannot_enter_ranking():
    for item in DISPUTED_3_18_22:
        assert item["claim_type"] == "interpretation"
        assert item["confidence"] == "contested"
        assert item["position"] == "neutral"
        assert item["competitive"] is False
        assert len(item.get("readings", [])) >= 2


def test_spirits_question_preserves_materially_different_readings_and_inspected_quorum():
    item = next(item for item in DISPUTED_3_18_22 if item["id"] == "ch3_disp_001")
    assert {
        "fallen_spirits_watchers",
        "christ_through_noah",
        "human_dead_descensus_reception",
    } <= set(item["readings"])
    assert {
        "gty_1p3_18_20",
        "tgc_storms_1p3_18_22",
        "grudem_noah_1p3_19",
        "lei_descensus_2025",
    } <= set(item["sources"])
    assert CHAPTER3_SOURCES["gty_1p3_18_20"]["inspection_scope"] == "relevant_section_inspected"
    assert CHAPTER3_SOURCES["tgc_storms_1p3_18_22"]["inspection_scope"] == "relevant_section_inspected"
    assert CHAPTER3_SOURCES["grudem_noah_1p3_19"]["inspection_scope"] == "relevant_section_inspected"
    assert CHAPTER3_SOURCES["lei_descensus_2025"]["inspection_scope"] == "publisher_abstract_inspected"


def test_eperotema_question_requires_morphology_lexical_history_and_bounded_peer_review():
    item = next(item for item in DISPUTED_3_18_22 if item["id"] == "ch3_disp_003")
    assert {"appeal_request", "pledge_stipulation", "confession_response_related"} <= set(item["readings"])
    assert {
        "sblgnt",
        "morphgnt_1peter",
        "lsj_eperotema",
        "ubs_handbook_1p3_21",
        "jts_crawford_1p3_21",
    } <= set(item["sources"])
    assert CHAPTER3_SOURCES["lsj_eperotema"]["inspection_scope"] == "relevant_section_inspected"
    assert CHAPTER3_SOURCES["ubs_handbook_1p3_21"]["inspection_scope"] == "relevant_section_inspected"
    assert CHAPTER3_SOURCES["jts_crawford_1p3_21"]["inspection_scope"] == "publisher_abstract_inspected"


def test_baptism_dispute_keeps_multiple_systematic_readings_without_catalog_only_sources():
    item = next(item for item in DISPUTED_3_18_22 if item["id"] == "ch3_disp_004")
    assert {
        "sacramental_efficacy",
        "faith_appeal_or_pledge_instrumentality",
        "sign_confession_resurrection_relation",
    } <= set(item["readings"])
    assert {
        "jts_crawford_1p3_21",
        "ubs_handbook_1p3_21",
        "gty_1p3_20_22",
        "tgc_storms_1p3_18_22",
    } <= set(item["sources"])
    assert not {"westfall_baptism_1999", "schreiner_1peter_nac", "horrell_williams_icc_v2"} & set(item["sources"])
