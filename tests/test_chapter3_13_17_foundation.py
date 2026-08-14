import questions

from questions.chapter3.application_13_17 import APPLICATION_3_13_17
from questions.chapter3.greek_13_17 import GREEK_3_13_17
from questions.chapter3.intertext_13_17 import INTERTEXT_3_13_17
from questions.chapter3.sources_13_17 import SOURCE_CATALOG as LANE_SOURCES
from questions.chapter3.text_13_17 import TEXT_3_13_17
from questions.chapter3.theology_13_17 import DISPUTED_3_13_17, THEOLOGY_3_13_17

LANE_ITEMS = TEXT_3_13_17 + GREEK_3_13_17 + INTERTEXT_3_13_17 + DISPUTED_3_13_17 + THEOLOGY_3_13_17 + APPLICATION_3_13_17


def _all_source_ids():
    return set(questions.SOURCE_CATALOG) | set(LANE_SOURCES)


def test_chapter3_13_17_lane_stays_out_of_production_registry():
    assert "chapter3" not in questions.POOL_REGISTRY
    assert not any(key.startswith("ch3_") for key in questions.POOL_REGISTRY)


def test_chapter3_13_17_ids_are_unique_and_reserved():
    ids = [item["id"] for item in LANE_ITEMS]
    assert LANE_ITEMS
    assert len(ids) == len(set(ids))
    prefixes = {"ch3_text_", "ch3_gr_", "ch3_ot_", "ch3_theol_", "ch3_disp_", "ch3_app_"}
    for item_id in ids:
        prefix = next((p for p in prefixes if item_id.startswith(p)), None)
        assert prefix is not None, item_id
        assert int(item_id.removeprefix(prefix)) >= 301, item_id


def test_chapter3_13_17_sources_resolve_and_all_items_are_noncompetitive():
    known = _all_source_ids()
    for item in LANE_ITEMS:
        assert item["sources"], item["id"]
        assert set(item["sources"]) <= known, item["id"]
        assert item["competitive"] is False, item["id"]


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


def test_chapter3_13_17_intertext_has_lxx_and_peer_reviewed_control():
    peer_reviewed = {"scriptura_vanrensburg_moyise_1p3", "verbum_moyise_2005_1p3"}
    for item in INTERTEXT_3_13_17:
        assert item["claim_type"] == "intertext"
        assert "sblgnt" in item["sources"]
        assert "septuagint_bible" in item["sources"]
        assert set(item["sources"]) & peer_reviewed, item["id"]
        assert item["competitive"] is False


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
    theology = next(item for item in THEOLOGY_3_13_17 if item["id"] == "ch3_theol_302")
    application = next(item for item in APPLICATION_3_13_17 if item["id"] == "ch3_app_302")
    assert "не выбирает" in greek["explanation"]
    assert "не выбирает одну современную" in theology["options"][0]
    assert "требует дополнительных аргументов" in application["explanation"]


def test_chapter3_13_17_christology_is_not_morphology():
    greek = next(item for item in GREEK_3_13_17 if item["id"] == "ch3_gr_302")
    theology = next(item for item in THEOLOGY_3_13_17 if item["id"] == "ch3_theol_301")
    assert "Морфология не решает" in greek["explanation"]
    assert theology["claim_type"] == "theology"
    assert theology["position"] == "project"
    assert {"scriptura_vanrensburg_moyise_1p3", "verbum_moyise_2005_1p3"} <= set(theology["sources"])
    assert theology["competitive"] is False
