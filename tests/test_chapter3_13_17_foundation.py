from collections import Counter

import questions

from questions.chapter3.application_13_17 import APPLICATION_3_13_17
from questions.chapter3.greek_13_17 import GREEK_3_13_17
from questions.chapter3.intertext_13_17 import INTERTEXT_3_13_17
from questions.chapter3.reviewed import CHAPTER3_REVIEWED_QUESTIONS
from questions.chapter3.sources_13_17 import SOURCE_CATALOG as LANE_SOURCES
from questions.chapter3.text_13_17 import TEXT_3_13_17
from questions.chapter3.theology_13_17 import DISPUTED_3_13_17, THEOLOGY_3_13_17

ALLOWED_CLAIM_TYPES = {"text", "greek", "history", "interpretation", "application"}
ALLOWED_POSITIONS = {"neutral", "project"}
ALLOWED_CONFIDENCE = {"high", "medium", "contested"}
NON_CLAIM_EVIDENCE_STATUSES = {
    "bibliographic_control_only",
    "metadata_preview_only_not_claim_evidence",
    "edition_metadata_inspected",
    "abstract_only",
}
CLAIM_EVIDENCE_STATUSES = {
    "relevant_article_text_inspected",
    "relevant_case_study_inspected_not_author_independent_from_2002",
    "relevant_3_15_comment_inspected",
    "entry_inspected",
}
BANNED_ABSURD_DISTRACTORS = {"Имя Пётр", "Иерусалим", "Средневековая глосса", "Христос назван ангелом"}

LANE_ITEMS = (
    TEXT_3_13_17
    + GREEK_3_13_17
    + INTERTEXT_3_13_17
    + DISPUTED_3_13_17
    + THEOLOGY_3_13_17
    + APPLICATION_3_13_17
)


def _all_source_ids():
    return set(questions.SOURCE_CATALOG) | set(LANE_SOURCES)


def test_chapter3_13_17_lane_crosses_product_only_through_reviewed_copy():
    assert "chapter3" in questions.POOL_REGISTRY
    assert not any(key.startswith("ch3_") for key in questions.POOL_REGISTRY)

    root_by_id = {item["id"]: item for item in questions.get_pool_by_key("chapter3")}
    reviewed_ids = {item["id"] for item in CHAPTER3_REVIEWED_QUESTIONS}
    assert set(root_by_id) == reviewed_ids
    for raw in LANE_ITEMS:
        assert raw["id"] in root_by_id
        assert root_by_id[raw["id"]] is not raw


def test_chapter3_13_17_ids_are_unique_and_reserved():
    ids = [item["id"] for item in LANE_ITEMS]
    assert LANE_ITEMS
    assert len(ids) == len(set(ids))
    prefixes = {"ch3_text_", "ch3_gr_", "ch3_ot_", "ch3_theol_", "ch3_disp_", "ch3_app_"}
    for item_id in ids:
        prefix = next((p for p in prefixes if item_id.startswith(p)), None)
        assert prefix is not None, item_id
        assert int(item_id.removeprefix(prefix)) >= 301, item_id


def test_chapter3_13_17_metadata_uses_only_canonical_enums():
    for item in LANE_ITEMS:
        assert item["claim_type"] in ALLOWED_CLAIM_TYPES, item["id"]
        assert item["position"] in ALLOWED_POSITIONS, item["id"]
        assert item["confidence"] in ALLOWED_CONFIDENCE, item["id"]
        assert item["competitive"] is False, item["id"]


def test_chapter3_13_17_options_are_valid_unique_and_not_absurd():
    for item in LANE_ITEMS:
        options = item["options"]
        assert len(options) == 4, item["id"]
        assert len(set(options)) == 4, item["id"]
        assert isinstance(item["correct"], int), item["id"]
        assert 0 <= item["correct"] < 4, item["id"]
        assert all(option.strip() for option in options), item["id"]
        assert all(len(option.strip()) >= 12 for option in options), item["id"]
        assert not (set(options) & BANNED_ABSURD_DISTRACTORS), item["id"]
        lengths = [len(option.strip()) for option in options]
        assert max(lengths) <= 3 * min(lengths), item["id"]


def test_chapter3_13_17_answer_positions_are_balanced_without_simple_leakage():
    positions = [item["correct"] for item in LANE_ITEMS]
    counts = Counter(positions)
    assert set(counts) == {0, 1, 2, 3}
    assert sorted(counts.values()) == [6, 7, 7, 7]
    assert all(not (positions[i] == positions[i + 1] == positions[i + 2]) for i in range(len(positions) - 2))
    for period in range(1, 5):
        assert any(positions[i] != positions[i % period] for i in range(period, len(positions)))


def test_chapter3_13_17_lane_source_inspection_statuses_are_classified():
    known_statuses = NON_CLAIM_EVIDENCE_STATUSES | CLAIM_EVIDENCE_STATUSES
    for source_id, source in LANE_SOURCES.items():
        assert source.get("inspection_status") in known_statuses, source_id


def test_chapter3_13_17_sources_resolve_and_nonclaim_controls_do_not_leak_into_claims():
    known = _all_source_ids()
    for item in LANE_ITEMS:
        assert item["sources"], item["id"]
        assert set(item["sources"]) <= known, item["id"]
        for source_id in item["sources"]:
            if source_id not in LANE_SOURCES:
                continue
            status = LANE_SOURCES[source_id]["inspection_status"]
            assert status not in NON_CLAIM_EVIDENCE_STATUSES, (item["id"], source_id, status)


def test_chapter3_13_17_apologia_uses_bounded_inspected_lexical_control():
    source = LANE_SOURCES["abbott_smith_apologia_1922"]
    assert source["inspection_status"] == "entry_inspected"
    assert "ἀπολογία" in source["inspection_scope"]
    interpretation = next(item for item in THEOLOGY_3_13_17 if item["id"] == "ch3_theol_302")
    application = next(item for item in APPLICATION_3_13_17 if item["id"] == "ch3_app_302")
    assert "abbott_smith_apologia_1922" in interpretation["sources"]
    assert "abbott_smith_apologia_1922" in application["sources"]


def test_chapter3_13_17_greek_has_exact_morphgnt_backing():
    markers = {
        "ch3_gr_301": "2AAD-P--",
        "ch3_gr_302": "----ASM-",
        "ch3_gr_303": "----NPM-",
        "ch3_gr_304": "----ASF-",
        "ch3_gr_305": "-PAPDSM-",
        "ch3_gr_306": "----GSF-",
        "ch3_gr_307": "----GSM-",
        "ch3_gr_308": "-PAPNPM-",
        "ch3_gr_309": "3PAO-S--",
    }
    assert set(markers) == {item["id"] for item in GREEK_3_13_17}
    for item in GREEK_3_13_17:
        assert item["claim_type"] == "greek"
        assert item["confidence"] == "high"
        assert item["position"] == "neutral"
        assert {"sblgnt", "morphgnt_1peter"} <= set(item["sources"])
        assert markers[item["id"]] in item["explanation"], item["id"]


def test_chapter3_13_17_isaiah_observation_is_separate_from_interpretation():
    observable = {"ch3_ot_301", "ch3_ot_302"}
    analytical = {"ch3_ot_303", "ch3_ot_304"}
    peer_reviewed = {"scriptura_vanrensburg_moyise_1p3", "verbum_moyise_2005_1p3"}
    for item in INTERTEXT_3_13_17:
        assert "sblgnt" in item["sources"]
        assert "septuagint_bible" in item["sources"]
        assert set(item["sources"]) & peer_reviewed, item["id"]
        if item["id"] in observable:
            assert item["claim_type"] == "text"
            assert item["confidence"] == "high"
        elif item["id"] in analytical:
            assert item["claim_type"] == "interpretation"
            assert item["confidence"] == "medium"
        else:
            raise AssertionError(item["id"])


def test_chapter3_13_17_contested_and_project_claims_cannot_rank():
    for item in LANE_ITEMS:
        if item["confidence"] == "contested" or item["position"] == "project":
            assert item["competitive"] is False, item["id"]
    disputed = DISPUTED_3_13_17[0]
    assert disputed["id"] == "ch3_disp_301"
    assert disputed["claim_type"] == "interpretation"
    assert disputed["confidence"] == "contested"


def test_chapter3_13_17_apologia_guardrail():
    greek = next(item for item in GREEK_3_13_17 if item["id"] == "ch3_gr_304")
    interpretation = next(item for item in THEOLOGY_3_13_17 if item["id"] == "ch3_theol_302")
    application = next(item for item in APPLICATION_3_13_17 if item["id"] == "ch3_app_302")
    assert "не выводятся из parsing code" in greek["explanation"]
    assert "не выбирая одну современную школу" in interpretation["options"][interpretation["correct"]]
    assert "требует дополнительных аргументов" in application["explanation"]


def test_chapter3_13_17_christology_is_interpretation_with_independent_control():
    greek = next(item for item in GREEK_3_13_17 if item["id"] == "ch3_gr_302")
    christology = next(item for item in THEOLOGY_3_13_17 if item["id"] == "ch3_theol_301")
    assert "Морфология не решает" in greek["explanation"]
    assert christology["claim_type"] == "interpretation"
    assert christology["confidence"] == "medium"
    assert christology["position"] == "project"
    assert "scriptura_vanrensburg_moyise_1p3" in christology["sources"]
    assert "blenkin_cambridge_1peter_1914" in christology["sources"]
    assert christology["competitive"] is False


def test_chapter3_13_17_application_confidence_is_item_specific():
    confidence = {item["id"]: item["confidence"] for item in APPLICATION_3_13_17}
    assert confidence == {
        "ch3_app_301": "medium",
        "ch3_app_302": "medium",
        "ch3_app_303": "high",
        "ch3_app_304": "medium",
    }
    assert all(item["claim_type"] == "application" for item in APPLICATION_3_13_17)
    assert all(item["position"] == "project" for item in APPLICATION_3_13_17)
