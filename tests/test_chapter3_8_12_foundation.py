from questions.chapter3.application_8_12 import APPLICATION_3_8_12
from questions.chapter3.greek_8_12 import GREEK_3_8_12, MORPHGNT_3_8_12
from questions.chapter3.intertext_8_12 import FIRST_PETER_3_10_12, INTERTEXT_3_8_12, LXX_PS33_13_17
from questions.chapter3.sources_8_12 import SOURCE_CATALOG
from questions.chapter3.text_8_12 import TEXT_3_8_12
from questions.chapter3.theology_8_12 import DISPUTED_3_8_12, THEOLOGY_3_8_12

POOLS = [TEXT_3_8_12, GREEK_3_8_12, INTERTEXT_3_8_12, THEOLOGY_3_8_12, DISPUTED_3_8_12, APPLICATION_3_8_12]
ALL_ITEMS = [item for pool in POOLS for item in pool]


def test_lane_has_unique_ids_in_reserved_range():
    ids = [item["id"] for item in ALL_ITEMS]
    assert len(ids) == len(set(ids))
    allowed_prefixes = {"ch3_text", "ch3_gr", "ch3_ot", "ch3_theol", "ch3_disp", "ch3_app"}
    for item_id in ids:
        prefix, number = item_id.rsplit("_", 1)
        assert prefix in allowed_prefixes
        assert int(number) >= 201


def test_all_items_have_required_metadata_and_are_quarantined_from_competitive_play():
    for item in ALL_ITEMS:
        assert item["claim_type"] in {"text", "greek", "history", "interpretation", "application"}
        assert item["confidence"] in {"high", "medium", "contested"}
        assert item["position"] in {"neutral", "project"}
        assert item["competitive"] is False
        assert item["sources"]
        assert set(item["sources"]).issubset(SOURCE_CATALOG)
        assert len(item["options"]) == 4
        assert 0 <= item["correct"] < len(item["options"])
        assert len(set(item["options"])) == len(item["options"])


def test_claim_type_separation_by_pool():
    assert all(item["claim_type"] == "text" for item in TEXT_3_8_12)
    assert all(item["claim_type"] == "greek" for item in GREEK_3_8_12)
    assert all(item["claim_type"] == "application" for item in APPLICATION_3_8_12)
    assert all(item["confidence"] == "contested" for item in DISPUTED_3_8_12)
    assert all(item["competitive"] is False for item in DISPUTED_3_8_12)


def test_morphgnt_anchor_forms_are_exact():
    assert MORPHGNT_3_8_12["\u1f41\u03bc\u03cc\u03c6\u03c1\u03bf\u03bd\u03b5\u03c2"] == ("\u1f41\u03bc\u03cc\u03c6\u03c1\u03c9\u03bd", "A- ----NPM-")
    assert MORPHGNT_3_8_12["\u03c3\u03c5\u03bc\u03c0\u03b1\u03b8\u03b5\u1fd6\u03c2"] == ("\u03c3\u03c5\u03bc\u03c0\u03b1\u03b8\u03ae\u03c2", "A- ----NPM-")
    assert MORPHGNT_3_8_12["\u03c6\u03b9\u03bb\u03ac\u03b4\u03b5\u03bb\u03c6\u03bf\u03b9"] == ("\u03c6\u03b9\u03bb\u03ac\u03b4\u03b5\u03bb\u03c6\u03bf\u03c2", "A- ----NPM-")
    assert MORPHGNT_3_8_12["\u03b5\u1f54\u03c3\u03c0\u03bb\u03b1\u03b3\u03c7\u03bd\u03bf\u03b9"] == ("\u03b5\u1f54\u03c3\u03c0\u03bb\u03b1\u03b3\u03c7\u03bd\u03bf\u03c2", "A- ----NPM-")
    assert MORPHGNT_3_8_12["\u03c4\u03b1\u03c0\u03b5\u03b9\u03bd\u03cc\u03c6\u03c1\u03bf\u03bd\u03b5\u03c2"] == ("\u03c4\u03b1\u03c0\u03b5\u03b9\u03bd\u03cc\u03c6\u03c1\u03c9\u03bd", "A- ----NPM-")
    assert MORPHGNT_3_8_12["\u1f00\u03c0\u03bf\u03b4\u03b9\u03b4\u03cc\u03bd\u03c4\u03b5\u03c2"] == ("\u1f00\u03c0\u03bf\u03b4\u03af\u03b4\u03c9\u03bc\u03b9", "V- -PAPNPM-")
    assert MORPHGNT_3_8_12["\u03b5\u1f50\u03bb\u03bf\u03b3\u03bf\u1fe6\u03bd\u03c4\u03b5\u03c2"] == ("\u03b5\u1f50\u03bb\u03bf\u03b3\u03ad\u03c9", "V- -PAPNPM-")
    assert MORPHGNT_3_8_12["\u1f10\u03ba\u03bb\u03ae\u03b8\u03b7\u03c4\u03b5"] == ("\u03ba\u03b1\u03bb\u03ad\u03c9", "V- 2API-P--")
    assert MORPHGNT_3_8_12["\u03ba\u03bb\u03b7\u03c1\u03bf\u03bd\u03bf\u03bc\u03ae\u03c3\u03b7\u03c4\u03b5"] == ("\u03ba\u03bb\u03b7\u03c1\u03bf\u03bd\u03bf\u03bc\u03ad\u03c9", "V- 2AAS-P--")
    for form in ("\u03c0\u03b1\u03c5\u03c3\u03ac\u03c4\u03c9", "\u1f10\u03ba\u03ba\u03bb\u03b9\u03bd\u03ac\u03c4\u03c9", "\u03c0\u03bf\u03b9\u03b7\u03c3\u03ac\u03c4\u03c9", "\u03b6\u03b7\u03c4\u03b7\u03c3\u03ac\u03c4\u03c9", "\u03b4\u03b9\u03c9\u03be\u03ac\u03c4\u03c9"):
        assert MORPHGNT_3_8_12[form][1] == "V- 3AAD-S--"


def test_lxx_quote_extent_and_observable_differences_are_pinned():
    assert LXX_PS33_13_17[13].startswith("\u03c4\u03af\u03c2 \u1f10\u03c3\u03c4\u03b9\u03bd \u1f04\u03bd\u03b8\u03c1\u03c9\u03c0\u03bf\u03c2")
    assert FIRST_PETER_3_10_12[10].startswith("\u1f41 \u03b3\u1f70\u03c1 \u03b8\u03ad\u03bb\u03c9\u03bd")
    assert "\u03b3\u03bb\u1ff6\u03c3\u03c3\u03ac\u03bd " + "\u03c3\u03bf\u03c5" in LXX_PS33_13_17[14]
    assert "\u03b3\u03bb\u1ff6\u03c3\u03c3\u03b1\u03bd \u1f00\u03c0\u1f78" in FIRST_PETER_3_10_12[10]
    assert "\u03c0\u03b1\u1fe6\u03c3\u03bf\u03bd" in LXX_PS33_13_17[14]
    assert "\u03c0\u03b1\u03c5\u03c3\u03ac\u03c4\u03c9" in FIRST_PETER_3_10_12[10]
    assert "\u03c4\u03bf\u1fe6 \u1f10\u03be\u03bf\u03bb\u03b5\u03b8\u03c1\u03b5\u1fe6\u03c3\u03b1\u03b9 \u1f10\u03ba \u03b3\u1fc6\u03c2 \u03c4\u1f78 \u03bc\u03bd\u03b7\u03bc\u03cc\u03c3\u03c5\u03bd\u03bf\u03bd \u03b1\u1f50\u03c4\u1ff6\u03bd" in LXX_PS33_13_17[17]
    assert "\u1f10\u03be\u03bf\u03bb\u03b5\u03b8\u03c1\u03b5\u1fe6\u03c3\u03b1\u03b9" not in FIRST_PETER_3_10_12[12]


def test_nontrivial_intertext_items_meet_scholarly_control_quorum():
    scholarly = {"green_1peter_ot_ethics", "greaux_ps34_1peter", "christensen_ps34_1peter"}
    for item in INTERTEXT_3_8_12:
        if item["claim_type"] == "interpretation":
            assert len(scholarly.intersection(item["sources"])) >= 2


def test_project_theology_has_two_evangelical_witnesses_where_used():
    evangelical = {"tgc_storms_1peter", "gty_1p3_8", "gty_1p3_9", "gty_1p3_10_12"}
    for item in THEOLOGY_3_8_12:
        if item["position"] == "project":
            assert len(evangelical.intersection(item["sources"])) >= 2


def test_eis_touto_syntax_is_not_promoted_to_closed_fact():
    item = DISPUTED_3_8_12[0]
    assert item["id"] == "ch3_disp_201"
    assert item["confidence"] == "contested"
    assert item["position"] == "neutral"
    assert {"cambridge_greek_1p3_9", "meyer_1p3_9"}.issubset(item["sources"])
