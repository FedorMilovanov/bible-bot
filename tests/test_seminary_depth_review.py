"""Regression contract for the seminary-depth review layer.

These tests intentionally inspect the final learner surface from the questions
package: the review must survive curation, option balancing, project labelling
and option rotation. They guard epistemic boundaries and assessment quality
rather than pinning raw authoring files.
"""
from __future__ import annotations

import questions


def _card(pool: str, question_id: str) -> dict:
    return next(card for card in questions.POOL_REGISTRY[pool] if card["id"] == question_id)


def _wrong_options(card: dict) -> list[str]:
    return [
        str(option)
        for index, option in enumerate(card["options"])
        if index != int(card["correct"])
    ]


def test_intro_dating_and_authorship_are_not_presented_as_settled_history():
    date_card = _card("intro2", "intro2_14")
    silence_card = _card("intro2", "intro2_15")
    authorship = _card("intro3", "intro3_16")

    assert date_card["position"] == "project"
    assert date_card["competitive"] is False
    assert "точный год 62 или 63 нельзя доказать" in date_card["options"][date_card["correct"]]
    assert "аргумент от молчания" in date_card["explanation"]

    assert silence_card["competitive"] is False
    assert "вторичный аргумент" in silence_card["options"][silence_card["correct"]]
    assert "не устанавливает" in silence_card["explanation"]

    assert authorship["position"] == "project"
    assert authorship["confidence"] == "contested"
    assert authorship["competitive"] is False
    surface = " ".join(
        [authorship["question"], authorship["explanation"], *authorship["options"]]
    )
    assert "единодушно поддерживает Петра" not in surface
    assert "секретарск" in authorship["explanation"]


def test_intro_audience_and_israel_language_keep_exegetical_boundaries_visible():
    audience = _card("intro2", "intro2_16")
    people = _card("intro3", "intro3_08")

    assert "смешанный состав вероятен" in audience["options"][audience["correct"]]
    assert "пропорция известна" in audience["explanation"]
    assert audience["competitive"] is False

    assert "ветхозаветные титулы" in people["question"]
    assert "Применяет к христианским адресатам" in people["options"][people["correct"]]
    assert "полное тождество церкви и Израиля" in " ".join(people["options"])
    assert "систематической богословской модели" in people["explanation"]


def test_intro_route_and_babylon_are_reconstruction_not_lexical_fact():
    route = _card("intro3", "intro3_12")
    babylon = _card("intro3", "intro3_13")

    assert route["competitive"] is False
    assert "правдоподобная, но не доказанная реконструкция" in route["options"][route["correct"]]
    assert "не даёт дорожного журнала" in route["explanation"]

    assert babylon["competitive"] is False
    assert "Наиболее распространённая" in babylon["options"][babylon["correct"]]
    assert "не словарная тождественность" in babylon["options"][babylon["correct"]]
    assert "скорее всего Рим" in babylon["explanation"]


def test_literary_hypotheses_are_taught_before_being_evaluated():
    baptismal = _card("intro3", "intro3_02")
    cross = _card("intro3", "intro3_03")
    partition = _card("intro3", "intro3_04")
    counter = _card("intro3", "intro3_05")

    for card in (baptismal, cross, partition, counter):
        assert card["competitive"] is False
        assert card["confidence"] == "medium"

    assert "не является необходимым объяснением" in baptismal["options"][baptismal["correct"]]
    assert "истории исследования" in cross["explanation"]
    assert "наблюдения реальны" in partition["explanation"]
    assert "не математическое опровержение" in counter["explanation"]


def test_tms_hard_items_require_evidence_discrimination_not_cartoon_rejection():
    cards = [_card("tms_deep", f"tms1_hard_0{number}") for number in range(1, 6)]

    banned_caricatures = (
        "Христос сотворён",
        "вера — это эмоция",
        "Истина мешает любви",
        "случайное совпадение слов без связи",
        "страх и упование для разных групп",
    )
    for card in cards:
        assert card["competitive"] is False
        assert card["confidence"] == "medium"
        wrong = _wrong_options(card)
        # Advanced distractors must be developed enough to encode a real
        # competing inference. The repository-wide wiseness audit separately
        # guards option-length cues, so this test only prevents one-clause jokes.
        assert min(len(option.split()) for option in wrong) >= 9
        surface = " ".join(wrong)
        assert not any(marker in surface for marker in banned_caricatures)


def test_tms_hard_explanations_mark_scope_of_inference():
    by_id = {
        card["id"]: card
        for card in questions.POOL_REGISTRY["tms_deep"]
        if card["id"].startswith("tms1_hard_")
    }
    assert set(by_id) >= {
        "tms1_hard_01",
        "tms1_hard_02",
        "tms1_hard_03",
        "tms1_hard_04",
        "tms1_hard_05",
    }

    assert "не заменяет" in by_id["tms1_hard_01"]["explanation"]
    assert "не делает страдание заслугой" in by_id["tms1_hard_02"]["explanation"]
    assert "не из одного образа" in by_id["tms1_hard_03"]["explanation"]
    assert "не противопоставляет доктрину и любовь" in by_id["tms1_hard_04"]["explanation"]
    assert by_id["tms1_hard_05"]["position"] == "project"
    assert "не означает сомнение" in by_id["tms1_hard_05"]["explanation"]
