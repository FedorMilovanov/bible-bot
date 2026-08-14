from questions.chapter2.disputed_2_8 import DISPUTED_2_8
from questions.chapter2.disputed_2_12 import DISPUTED_2_12
from questions.chapter2.history_exiles_2_11 import HISTORY_EXILES_2_11
from questions.chapter2.theology_people_text import THEOLOGY_PEOPLE_TEXT
from questions.source_registry import SOURCE_CATALOG


ITEMS = DISPUTED_2_8 + DISPUTED_2_12 + HISTORY_EXILES_2_11 + THEOLOGY_PEOPLE_TEXT


def test_new_review_modules_have_unique_ids_and_sources():
    ids = [item["id"] for item in ITEMS]
    assert len(ids) == len(set(ids))
    for item in ITEMS:
        assert item["sources"]
        assert not (set(item["sources"]) - set(SOURCE_CATALOG)), item["id"]


def test_new_review_modules_never_enter_ranking():
    assert all(item["competitive"] is False for item in ITEMS)


def test_disputed_project_positions_are_visible():
    for item in DISPUTED_2_8 + DISPUTED_2_12:
        assert item["claim_type"] == "interpretation"
        assert item["confidence"] in {"medium", "contested"}
        if item["position"] == "project":
            assert item["question"].startswith("[Позиция курса]")


def test_exile_context_is_history_not_application():
    item = HISTORY_EXILES_2_11[0]
    assert item["claim_type"] == "history"
    assert item["position"] == "neutral"
