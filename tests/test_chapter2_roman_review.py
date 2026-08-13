from questions.chapter2.history_roman_2_13_14 import HISTORY_ROMAN_2_13_14
from questions.source_registry import SOURCE_CATALOG


def test_roman_context_is_later_comparison_not_ranking_fact():
    item = HISTORY_ROMAN_2_13_14[0]
    assert item["claim_type"] == "history"
    assert item["position"] == "neutral"
    assert item["competitive"] is False
    assert not (set(item["sources"]) - set(SOURCE_CATALOG))
    assert "начале II века" in item["explanation"]
