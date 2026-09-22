# ruff: noqa: RUF001\n"""Regression contract for the third exegesis-precision wave."""
from __future__ import annotations

import questions


def _card(pool: str, question_id: str) -> dict:
    return next(card for card in questions.POOL_REGISTRY[pool] if card["id"] == question_id)


def _key(card: dict) -> str:
    return str(card["options"][card["correct"]])


def test_girded_mind_teaches_readiness_without_forcing_exodus_12():
    card = _card("hard_p1", "hard_03")
    assert card["claim_type"] == "interpretation"
    assert card["confidence"] == "medium"
    assert card["competitive"] is False
    assert {"sblgnt", "septuagint_bible", "schreiner_nac_1peter"} <= set(card["sources"])

    keyed = _key(card)
    assert "готовност" in keyed and "собранност" in keyed
    assert "может" in keyed and "Исх. 12:11" in keyed
    assert "прямо его не цитирует" in keyed  # noqa: RUF001
    assert "возможную интертекстуальную связь" in card["explanation"]


def test_nero_silence_is_secondary_dating_evidence_not_calendar_proof():
    card = _card("hard_p1", "hard_11")
    assert card["question"].startswith("[Позиция курса]")
    assert card["claim_type"] == "interpretation"
    assert card["confidence"] == "contested"
    assert card["position"] == "project"
    assert card["competitive"] is False
    assert {"oxford_1peter_contested", "tacitus_annals_15_44"} <= set(card["sources"])

    keyed = _key(card)
    assert "может" in keyed and "поддерживать" in keyed
    assert "само по себе" in keyed and "не доказывает" in keyed  # noqa: RUF001
    assert "аргументе от молчания" in card["explanation"]


def test_exodus_24_is_strong_intertext_not_fake_direct_quotation():
    card = _card("hard_p1", "hard_13")
    assert card["claim_type"] == "interpretation"
    assert card["confidence"] == "medium"
    assert card["competitive"] is False
    assert {"sblgnt", "septuagint_bible", "schreiner_nac_1peter"} <= set(card["sources"])

    keyed = _key(card)
    assert "сильным вероятным фоном" in keyed
    assert "не маркирует" in keyed and "прямую цитату" in keyed
    assert "послушание" in card["explanation"] and "окропляет" in card["explanation"]


def test_perfect_participle_1_22_is_not_a_tense_shortcut():
    card = _card("hard_p2", "hard17_06")
    assert card["claim_type"] == "greek"
    assert card["confidence"] == "medium"
    assert card["competitive"] is False
    assert "cambridge_greek_perfect_aspect" in card["sources"]
    assert "cambridge_greek_perfect_aspect" in questions.SOURCE_CATALOG

    keyed = _key(card)
    assert "нынешнего призыва" in keyed
    assert "синтаксис" in keyed and "контекст" in keyed
    assert "перфектное действительное причастие" in card["explanation"]
    surface = " ".join([keyed, card["explanation"]])
    assert "завершённое действие с продолжающимся результатом" not in surface  # noqa: RUF001
    assert "сделано однажды" in card["explanation"]


def test_perfect_passive_1_23_separates_morphology_agent_and_theology():
    card = _card("hard_p2", "hard17_08")
    assert card["claim_type"] == "greek"
    assert card["confidence"] == "medium"
    assert card["competitive"] is False
    assert "cambridge_greek_perfect_aspect" in card["sources"]

    keyed = _key(card)
    assert "получив" in keyed and "новое рождение" in keyed
    assert "контекст" in keyed and "формой" in keyed
    explanation = card["explanation"]
    assert "перфектное страдательное причастие" in explanation
    assert "получателей действия" in explanation
    assert "имя действующего лица нужно брать из контекста" in explanation
    assert "не является грамматическим доказательством" in explanation


def test_wave_keeps_all_five_cards_out_of_ranking():
    for pool, question_id in (
        ("hard_p1", "hard_03"),
        ("hard_p1", "hard_11"),
        ("hard_p1", "hard_13"),
        ("hard_p2", "hard17_06"),
        ("hard_p2", "hard17_08"),
    ):
        assert _card(pool, question_id)["competitive"] is False
