from collections import Counter
from difflib import SequenceMatcher
from itertools import combinations
import re
import unicodedata

from questions.chapter3.application_18_22 import APPLICATION_3_18_22 as A
from questions.chapter3.disputed_18_22 import DISPUTED_3_18_22 as D
from questions.chapter3.greek_18_22 import GREEK_3_18_22 as G
from questions.chapter3.intertext_18_22 import INTERTEXT_3_18_22 as I
from questions.chapter3.sources import SOURCE_CATALOG as S
from questions.chapter3.text_18_22 import TEXT_3_18_22 as T
from questions.chapter3.theology_18_22 import THEOLOGY_3_18_22 as H

ALL = T + G + D + I + H + A
CLAIM_TYPES = {"text", "greek", "history", "interpretation", "application"}
POSITIONS = {"neutral", "project"}
CONFIDENCES = {"high", "medium", "contested"}
INSPECTION_SCOPES = {
    "primary_text_inspected",
    "full_relevant_text_inspected",
    "relevant_section_inspected",
    "full_article_inspected",
    "publisher_abstract_inspected",
    "metadata_only",
    "bibliographic_only",
}
CARD_INELIGIBLE_SCOPES = {"metadata_only", "bibliographic_only"}
PASSAGE_LEVEL_SCOPES = {
    "full_relevant_text_inspected",
    "relevant_section_inspected",
    "full_article_inspected",
}
GROUP_PREFIXES = (
    (T, "ch3_text_"),
    (G, "ch3_gr_"),
    (D, "ch3_disp_"),
    (I, "ch3_ot_"),
    (H, "ch3_theol_"),
    (A, "ch3_app_"),
)


def _normalize_option(value):
    value = unicodedata.normalize("NFKC", value).casefold()
    value = re.sub(r"[^\w\u0370-\u03ff]+", " ", value, flags=re.UNICODE)
    return " ".join(value.split())


def test_completion_boundary_and_source_resolution():
    ids = [x["id"] for x in ALL]
    assert len(ids) == len(set(ids)) == 45
    for x in ALL:
        assert x["competitive"] is False
        assert set(x["sources"]) <= set(S), x["id"]
        assert "какая школа права" not in " ".join([x["question"], *x["options"]]).lower()


def test_correct_answer_positions_are_balanced_and_non_leaking():
    positions = [x["correct"] for x in ALL]
    counts = Counter(positions)

    assert set(counts) == {0, 1, 2, 3}
    assert sorted(counts.values()) == [11, 11, 11, 12]
    assert counts == Counter({0: 12, 1: 11, 2: 11, 3: 11})

    for start in range(len(positions) - 2):
        assert len(set(positions[start:start + 3])) > 1, (start, positions[start:start + 3])

    for offset in range(4):
        mechanical = [(index + offset) % 4 for index in range(len(positions))]
        assert positions != mechanical


def test_canonical_metadata_enums_and_id_prefixes():
    for group, prefix in GROUP_PREFIXES:
        assert group
        for x in group:
            assert x["claim_type"] in CLAIM_TYPES, x["id"]
            assert x["position"] in POSITIONS, x["id"]
            assert x["confidence"] in CONFIDENCES, x["id"]
            assert x["competitive"] is False, x["id"]
            assert re.fullmatch(re.escape(prefix) + r"\d{3}", x["id"]), x["id"]

    assert {x["id"] for x in I} == {f"ch3_ot_{n:03d}" for n in range(1, 6)}
    assert not any(x["id"].startswith("ch3_int_") for x in ALL)


def test_four_unique_options_valid_correct_and_near_duplicate_guard():
    for x in ALL:
        options = x["options"]
        assert len(options) == 4, x["id"]
        assert isinstance(x["correct"], int), x["id"]
        assert 0 <= x["correct"] < len(options), x["id"]
        assert all(isinstance(option, str) and option.strip() for option in options), x["id"]
        assert len(options) == len(set(options)), x["id"]

        normalized = [_normalize_option(option) for option in options]
        assert len(normalized) == len(set(normalized)), x["id"]

        for left, right in combinations(normalized, 2):
            if min(len(left), len(right)) < 24:
                continue
            assert left not in right and right not in left, x["id"]
            assert SequenceMatcher(None, left, right).ratio() < 0.985, x["id"]


def test_source_catalog_has_generic_inspection_contract():
    assert S
    for source_id, metadata in S.items():
        assert metadata.get("inspection_scope") in INSPECTION_SCOPES, source_id
        assert isinstance(metadata.get("claim_limit"), str) and metadata["claim_limit"].strip(), source_id


def test_referenced_sources_are_card_eligible_under_inspection_contract():
    referenced = {source_id for item in ALL for source_id in item["sources"]}
    for source_id in referenced:
        metadata = S[source_id]
        assert metadata["inspection_scope"] not in CARD_INELIGIBLE_SCOPES, source_id
        assert metadata["claim_limit"].strip(), source_id

    former_catalog_only = {
        "davids_1peter_nicnt",
        "schreiner_1peter_nac",
        "elliott_1peter_ayb",
        "horrell_williams_icc_v2",
        "westfall_baptism_1999",
    }
    assert not (former_catalog_only & referenced)
    assert all(S[source_id]["inspection_scope"] in CARD_INELIGIBLE_SCOPES for source_id in former_catalog_only)


def test_project_items_have_two_inspected_passage_level_evangelical_witnesses():
    project_items = [item for item in ALL if item["position"] == "project"]
    assert project_items

    for item in project_items:
        witnesses = {
            source_id
            for source_id in item["sources"]
            if S[source_id].get("project_passage_witness") is True
        }
        assert len(witnesses) >= 2, (item["id"], witnesses)
        assert all(S[source_id]["inspection_scope"] in PASSAGE_LEVEL_SCOPES for source_id in witnesses)


def test_bounded_abstract_sources_state_their_limits():
    bounded = {
        "jts_crawford_1p3_21",
        "pierce_spirits_2011",
        "grindheim_spirits_2024",
        "marcar_noah_2017",
        "lei_descensus_2025",
    }
    for source_id in bounded:
        metadata = S[source_id]
        assert metadata["inspection_scope"] == "publisher_abstract_inspected"
        limit = metadata["claim_limit"].casefold()
        assert "not" in limit or "only" in limit


def test_direct_text_is_uninterpreted():
    for x in T:
        assert x["sources"] == ["sblgnt"]
        assert (x["claim_type"], x["position"], x["evidence_layer"]) == ("text", "neutral", "text")


def test_3_18_sblgnt_suffered_guard_and_separate_death_participle():
    suffered = next(x for x in T if x["id"] == "ch3_text_001")
    killed = next(x for x in T if x["id"] == "ch3_text_010")
    correct_suffered = suffered["options"][suffered["correct"]]
    correct_killed = killed["options"][killed["correct"]]

    assert "ἔπαθεν" in correct_suffered
    assert "пострад" in correct_suffered.casefold()
    assert "ἀπέθανεν" not in correct_suffered
    assert "умер" not in correct_suffered.casefold()
    assert "θανατωθεὶς" in correct_killed
    assert "рукопис" in suffered["explanation"].casefold()


def test_morphgnt_snapshot():
    got = {(x["morphgnt"]["form"], x["morphgnt"]["parse"]) for x in G}
    want = {
        ("ἔπαθεν", "3AAI-S--"), ("προσαγάγῃ", "3AAS-S--"), ("ἐκήρυξεν", "3AAI-S--"),
        ("διεσώθησαν", "3API-P--"), ("ἀντίτυπον", "A- ----NSN-"), ("ἐπερώτημα", "N- ----NSN-"),
        ("ἅπαξ", "D- --------"), ("θανατωθεὶς", "-APPNSM-"), ("ζῳοποιηθεὶς", "-APPNSM-"),
        ("ᾧ", "RR ----DSN-"), ("πνεύμασιν", "N- ----DPN-"), ("πορευθεὶς", "-APPNSM-"),
        ("ἀπειθήσασίν", "-AAPDPM-"), ("σῴζει", "3PAI-S--"), ("ὑποταγέντων", "-APPGPM-")
    }
    assert want <= got
    assert all(set(x["sources"]) == {"sblgnt", "morphgnt_1peter"} for x in G)


def test_intertext_has_bounded_primary_and_scholarship_control():
    e = next(x for x in I if x["id"] == "ch3_ot_003")
    assert {"enoch_10_14_charles", "pierce_spirits_2011", "tgc_storms_1p3_18_22"} <= set(e["sources"])
    assert e["relationship"] == "probable_second_temple_background"


def test_crawford_and_enoch_evidence_scopes_are_fail_closed():
    crawford = S["jts_crawford_1p3_21"]
    enoch = S["enoch_10_14_charles"]

    assert crawford["inspection_scope"] == "publisher_abstract_inspected"
    assert "not the full jts article" in crawford["claim_limit"].casefold()
    assert "lexical certainty" in crawford["claim_limit"].casefold()

    assert enoch["kind"] == "primary_second_temple_translation"
    assert enoch["inspection_scope"] == "primary_text_inspected"
    assert "not a critical textual edition" in enoch["claim_limit"].casefold()
    assert "cannot prove direct literary dependence" in enoch["claim_limit"].casefold()
