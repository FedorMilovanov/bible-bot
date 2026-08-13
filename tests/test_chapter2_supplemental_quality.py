from questions.chapter2.application_growth import APPLICATION_GROWTH_2_1_3
from questions.chapter2.application_identity import APPLICATION_IDENTITY_2_4_10
from questions.chapter2.application_suffering import APPLICATION_SUFFERING_2_18_25
from questions.chapter2.application_witness import APPLICATION_WITNESS_2_11_12
from questions.chapter2.history_bodily_suffering import HISTORY_BODILY_2_18_25
from questions.chapter2.history_oiketai import HISTORY_OIKETAI_2_18
from questions.chapter2.theology_civil import THEOLOGY_CIVIL_2_13_17
from questions.source_registry import SOURCE_CATALOG


SUPPLEMENTAL = (
    HISTORY_OIKETAI_2_18
    + HISTORY_BODILY_2_18_25
    + APPLICATION_GROWTH_2_1_3
    + APPLICATION_IDENTITY_2_4_10
    + APPLICATION_WITNESS_2_11_12
    + APPLICATION_SUFFERING_2_18_25
    + THEOLOGY_CIVIL_2_13_17
)


def test_supplemental_items_have_unique_ids_and_resolved_sources():
    ids = [item["id"] for item in SUPPLEMENTAL]
    assert len(ids) == len(set(ids))
    for item in SUPPLEMENTAL:
        assert not (set(item["sources"]) - set(SOURCE_CATALOG)), item["id"]


def test_history_and_application_never_enter_ranking():
    for item in HISTORY_OIKETAI_2_18 + HISTORY_BODILY_2_18_25:
        assert item["claim_type"] == "history"
        assert item["competitive"] is False
    for item in (
        APPLICATION_GROWTH_2_1_3
        + APPLICATION_IDENTITY_2_4_10
        + APPLICATION_WITNESS_2_11_12
        + APPLICATION_SUFFERING_2_18_25
    ):
        assert item["claim_type"] == "application"
        assert item["competitive"] is False


def test_project_theology_is_visible_and_noncompetitive():
    for item in THEOLOGY_CIVIL_2_13_17:
        assert item["claim_type"] == "interpretation"
        assert item["competitive"] is False
        if item["position"] == "project":
            assert item["question"].startswith("[Позиция курса]")
