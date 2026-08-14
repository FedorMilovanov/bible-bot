import re
import unicodedata
from difflib import SequenceMatcher
from itertools import combinations

import questions
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
    known = set(questions.SOURCE_CATALOG) | set(S)
    ids = [x["id"] for x in ALL]
    assert len(ids) == len(set(ids)) == 45
    for x in ALL:
        assert x["competitive"] is False
        assert set(x["sources"]) <= known, x["id"]
        assert "какая школа права" not in " ".join([x["question"], *x["options"]]).lower()


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
    assert all({"sblgnt", "morphgnt_1peter"} <= set(x["sources"]) for x in G)


def test_intertext_and_project_quorum():
    e = next(x for x in I if x["id"] == "ch3_ot_003")
    assert {"enoch_10_14_charles", "pierce_spirits_2011", "grindheim_spirits_2024"} <= set(e["sources"])
    assert e["relationship"] == "probable_second_temple_background"

    for x in [x for x in H if x["position"] == "project"]:
        src = set(x["sources"])
        assert any(v.startswith("gty_") for v in src)
        assert "schreiner_1peter_nac" in src


def test_crawford_and_enoch_evidence_scopes_are_fail_closed():
    crawford = S["jts_crawford_1p3_21"]
    enoch = S["enoch_10_14_charles"]

    assert crawford["inspection_scope"] == "publisher_abstract_inspected"
    assert "full jts article was not inspected" in crawford["limits"].casefold()
    assert "lexical certainty" in crawford["limits"].casefold()

    assert enoch["kind"] == "primary_second_temple_translation"
    assert enoch["inspection_scope"] == "public_domain_translation_passages_inspected"
    assert "not a critical textual edition" in enoch["limits"].casefold()
    assert "cannot prove direct literary dependence" in enoch["limits"].casefold()
