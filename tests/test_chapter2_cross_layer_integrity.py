from questions.chapter2 import CHAPTER2_DRAFT_QUESTIONS
from questions.chapter2.application_growth import APPLICATION_GROWTH_2_1_3
from questions.chapter2.application_identity import APPLICATION_IDENTITY_2_4_10
from questions.chapter2.application_suffering import APPLICATION_SUFFERING_2_18_25
from questions.chapter2.application_witness import APPLICATION_WITNESS_2_11_12
from questions.chapter2.disputed_2_8 import DISPUTED_2_8
from questions.chapter2.disputed_2_12 import DISPUTED_2_12
from questions.chapter2.history_bodily_suffering import HISTORY_BODILY_2_18_25
from questions.chapter2.history_exiles_2_11 import HISTORY_EXILES_2_11
from questions.chapter2.history_oiketai import HISTORY_OIKETAI_2_18
from questions.chapter2.theology_civil import THEOLOGY_CIVIL_2_13_17
from questions.chapter2.theology_people_text import THEOLOGY_PEOPLE_TEXT
from questions.source_registry import SOURCE_CATALOG


ALL_CHAPTER2_REVIEWED_ITEMS = (
    CHAPTER2_DRAFT_QUESTIONS
    + APPLICATION_GROWTH_2_1_3
    + APPLICATION_IDENTITY_2_4_10
    + APPLICATION_WITNESS_2_11_12
    + APPLICATION_SUFFERING_2_18_25
    + HISTORY_OIKETAI_2_18
    + HISTORY_BODILY_2_18_25
    + HISTORY_EXILES_2_11
    + THEOLOGY_CIVIL_2_13_17
    + THEOLOGY_PEOPLE_TEXT
    + DISPUTED_2_8
    + DISPUTED_2_12
)


def test_chapter2_ids_are_unique_across_all_review_layers():
    ids = [item["id"] for item in ALL_CHAPTER2_REVIEWED_ITEMS]
    assert len(ids) == len(set(ids))


def test_chapter2_sources_resolve_across_all_review_layers():
    for item in ALL_CHAPTER2_REVIEWED_ITEMS:
        unresolved = set(item["sources"]) - set(SOURCE_CATALOG)
        assert not unresolved, (item["id"], unresolved)


def test_chapter2_project_positions_are_visible_everywhere():
    for item in ALL_CHAPTER2_REVIEWED_ITEMS:
        if item["position"] == "project":
            assert item["competitive"] is False
            assert item["question"].startswith("[Позиция курса]")


def test_chapter2_nonfactual_review_domains_never_rank():
    for item in ALL_CHAPTER2_REVIEWED_ITEMS:
        if item["claim_type"] in {"application", "history"}:
            assert item["competitive"] is False
        if item["confidence"] == "contested":
            assert item["competitive"] is False
