from difflib import SequenceMatcher
import re
import unicodedata

from questions.chapter3.application_8_12 import APPLICATION_3_8_12
from questions.chapter3.greek_8_12 import GREEK_3_8_12, MORPHGNT_3_8_12
from questions.chapter3.intertext_8_12 import FIRST_PETER_3_10_12, INTERTEXT_3_8_12, LXX_PS33_13_17
from questions.chapter3.sources_8_12 import SOURCE_CATALOG
from questions.chapter3.text_8_12 import TEXT_3_8_12
from questions.chapter3.theology_8_12 import DISPUTED_3_8_12, THEOLOGY_3_8_12

POOLS = [
    TEXT_3_8_12,
    GREEK_3_8_12,
    INTERTEXT_3_8_12,
    THEOLOGY_3_8_12,
    DISPUTED_3_8_12,
    APPLICATION_3_8_12,
]
ALL_ITEMS = [item for pool in POOLS for item in pool]

CLAIM_TYPES = {"text", "greek", "history", "interpretation", "application"}
POSITIONS = {"neutral", "project"}
CONFIDENCE_LEVELS = {"high", "medium", "contested"}
NEAR_DUPLICATE_THRESHOLD = 0.92


def _normalize(value: str) -> str:
    value = unicodedata.normalize("NFKC", value).casefold()
    value = re.sub(r"\s+", " ", value).strip()
    return value


def test_lane_question_count_and_reserved_unique_ids():
    assert len(ALL_ITEMS) == 37
    ids = [item["id"] for item in ALL_ITEMS]
    assert len(ids) == len(set(ids))
    allowed_prefixes = {"ch3_text", "ch3_gr", "ch3_ot", "ch3_theol", "ch3_disp", "ch3_app"}
    for item_id in ids:
        prefix, number = item_id.rsplit("_", 1)
        assert prefix in allowed_prefixes
        assert int(number) >= 201


def test_metadata_enums_source_resolution_and_competitive_quarantine():
    for item in ALL_ITEMS:
        assert item["claim_type"] in CLAIM_TYPES
        assert item["position"] in POSITIONS
        assert item["confidence"] in CONFIDENCE_LEVELS
        assert item["competitive"] is False
        assert item["sources"]
        assert set(item["sources"]).issubset(SOURCE_CATALOG)


def test_editorial_option_integrity_and_valid_correct_index():
    for item in ALL_ITEMS:
        options = item["options"]
        assert len(options) == 4
        assert all(isinstance(option, str) and _normalize(option) for option in options)
        normalized = [_normalize(option) for option in options]
        assert len(normalized) == len(set(normalized))
        lengths = [len(option) for option in options]
        assert max(lengths) / min(lengths) <= 2.2, f"option-length cue in {item['id']}: {lengths}"
        assert isinstance(item["correct"], int)
        assert 0 <= item["correct"] < len(options)

    banned_absurdity_fragments = {
        "храмовая архитектура",
        "одинаково одет",
        "физически креп",
        "ритуально чист",
        "юридически безупреч",
    }
    all_option_text = "\n".join(_normalize(option) for item in ALL_ITEMS for option in item["options"])
    assert all(fragment not in all_option_text for fragment in banned_absurdity_fragments)


def test_no_exact_or_near_duplicate_question_stems():
    normalized = [(item["id"], _normalize(item["question"])) for item in ALL_ITEMS]
    stems = [stem for _, stem in normalized]
    assert len(stems) == len(set(stems))

    for index, (left_id, left) in enumerate(normalized):
        for right_id, right in normalized[index + 1 :]:
            ratio = SequenceMatcher(None, left, right).ratio()
            assert ratio < NEAR_DUPLICATE_THRESHOLD, (
                f"near-duplicate question stems: {left_id} / {right_id} = {ratio:.3f}"
            )


def test_claim_type_separation_by_pool():
    assert all(item["claim_type"] == "text" for item in TEXT_3_8_12)
    assert all(item["claim_type"] == "greek" for item in GREEK_3_8_12)
    assert all(item["claim_type"] == "application" for item in APPLICATION_3_8_12)
    assert all(item["confidence"] == "contested" for item in DISPUTED_3_8_12)
    assert all(item["competitive"] is False for item in DISPUTED_3_8_12)


def test_morphgnt_anchor_forms_are_exact():
    assert MORPHGNT_3_8_12["ὁμόφρονες"] == ("ὁμόφρων", "A- ----NPM-")
    assert MORPHGNT_3_8_12["συμπαθεῖς"] == ("συμπαθής", "A- ----NPM-")
    assert MORPHGNT_3_8_12["φιλάδελφοι"] == ("φιλάδελφος", "A- ----NPM-")
    assert MORPHGNT_3_8_12["εὔσπλαγχνοι"] == ("εὔσπλαγχνος", "A- ----NPM-")
    assert MORPHGNT_3_8_12["ταπεινόφρονες"] == ("ταπεινόφρων", "A- ----NPM-")
    assert MORPHGNT_3_8_12["ἀποδιδόντες"] == ("ἀποδίδωμι", "V- -PAPNPM-")
    assert MORPHGNT_3_8_12["εὐλογοῦντες"] == ("εὐλογέω", "V- -PAPNPM-")
    assert MORPHGNT_3_8_12["ἐκλήθητε"] == ("καλέω", "V- 2API-P--")
    assert MORPHGNT_3_8_12["κληρονομήσητε"] == ("κληρονομέω", "V- 2AAS-P--")
    for form in ("παυσάτω", "ἐκκλινάτω", "ποιησάτω", "ζητησάτω", "διωξάτω"):
        assert MORPHGNT_3_8_12[form][1] == "V- 3AAD-S--"


def test_psalm_quote_is_sustained_adaptation_and_exact_differences_are_pinned():
    classification = next(item for item in INTERTEXT_3_8_12 if item["id"] == "ch3_ot_201")
    correct = classification["options"][classification["correct"]]
    assert "sustained quotation/adaptation" in correct.casefold()
    assert "Пс. 33:13\u201317 LXX" in correct
    assert "Пс. 34:12\u201316 MT/common English numbering" in correct
    assert "verbatim" not in correct.casefold()
    assert "дослов" not in correct.casefold()

    assert LXX_PS33_13_17[13].startswith("τίς ἐστιν ἄνθρωπος")
    assert FIRST_PETER_3_10_12[10].startswith("ὁ γὰρ θέλων")
    assert "γλῶσσάν \u03c3\u03bf\u03c5" in LXX_PS33_13_17[14]
    assert "χείλη \u03c3\u03bf\u03c5" in LXX_PS33_13_17[14]
    assert "γλῶσσαν ἀπὸ" in FIRST_PETER_3_10_12[10]
    assert "χείλη τοῦ" in FIRST_PETER_3_10_12[10]
    assert "παῦσον" in LXX_PS33_13_17[14]
    assert "παυσάτω" in FIRST_PETER_3_10_12[10]
    assert "τοῦ ἐξολεθρεῦσαι ἐκ γῆς τὸ μνημόσυνον αὐτῶν" in LXX_PS33_13_17[17]
    assert "ἐξολεθρεῦσαι" not in FIRST_PETER_3_10_12[12]


def test_psalm_function_separates_local_text_fact_from_broader_interpretation():
    local_link = next(item for item in INTERTEXT_3_8_12 if item["id"] == "ch3_ot_208")
    broader = next(item for item in INTERTEXT_3_8_12 if item["id"] == "ch3_ot_207")
    assert local_link["claim_type"] == "text"
    assert local_link["confidence"] == "high"
    assert broader["claim_type"] == "interpretation"
    assert broader["confidence"] == "medium"


def test_nontrivial_intertext_interpretations_meet_scholarly_control_quorum():
    scholarly = {"green_1peter_ot_ethics", "greaux_ps34_1peter", "christensen_ps34_1peter"}
    for item in INTERTEXT_3_8_12:
        if item["claim_type"] == "interpretation":
            assert len(scholarly.intersection(item["sources"])) >= 2


def test_source_inspection_levels_bound_claims():
    green = SOURCE_CATALOG["green_1peter_ot_ethics"]
    greaux = SOURCE_CATALOG["greaux_ps34_1peter"]
    christensen = SOURCE_CATALOG["christensen_ps34_1peter"]

    assert green["inspection_level"] == "full_text_official_pdf"
    assert greaux["inspection_level"] == "publisher_abstract_only"
    assert christensen["inspection_level"] == "full_text_official_pdf"
    for source in (green, greaux, christensen):
        assert source["claim_limit"].strip()


def test_project_theology_has_two_evangelical_witnesses_where_used():
    evangelical = {"tgc_storms_1peter", "gty_1p3_8", "gty_1p3_9", "gty_1p3_10_12"}
    for item in THEOLOGY_3_8_12:
        if item["position"] == "project":
            assert len(evangelical.intersection(item["sources"])) >= 2


def test_eis_touto_syntax_remains_contested_with_multiple_controls():
    item = DISPUTED_3_8_12[0]
    assert item["id"] == "ch3_disp_201"
    assert item["confidence"] == "contested"
    assert item["position"] == "neutral"
    assert {"christensen_ps34_1peter", "cambridge_greek_1p3_9", "meyer_1p3_9"}.issubset(item["sources"])
