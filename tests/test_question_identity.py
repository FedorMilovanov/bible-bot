from pathlib import Path

import question_identity


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
RUNTIME_SOURCE = (ROOT / "telegram_quiz_runtime_controller.py").read_text(encoding="utf-8")
QUESTION_IDENTITY_SOURCE = (ROOT / "question_identity.py").read_text(encoding="utf-8")


def test_known_question_identity_vectors_are_stable():
    cases = [
        ({"question": "", "options": []}, "d41d8cd98f00", "e3b0c44298fc"),
        (
            {
                "question": "Кто написал 1 Петра?",
                "options": ["Пётр", "Павел", "Иоанн", "Иаков"],
            },
            "0b476ad5ddf3",
            "43ddc179f338",
        ),
        (
            {"question": "Grace & truth", "options": ["A", "B"]},
            "7604e42999d9",
            "adbc57afe553",
        ),
    ]

    for question, stable_id, persisted_id in cases:
        assert question_identity.stable_question_id(question) == stable_id
        assert question_identity.get_qid(question) == persisted_id


def test_options_affect_persisted_qid_but_not_historical_stable_id():
    first = {"question": "same", "options": ["A", "B"]}
    second = {"question": "same", "options": ["B", "A"]}

    assert question_identity.stable_question_id(first) == question_identity.stable_question_id(second)
    assert question_identity.get_qid(first) != question_identity.get_qid(second)


def test_question_identity_is_canonical_and_has_no_monolith_bridge():
    assert "import bot" not in QUESTION_IDENTITY_SOURCE
    assert "from bot" not in QUESTION_IDENTITY_SOURCE
    assert "install_legacy_bridge" not in QUESTION_IDENTITY_SOURCE
    assert "user_data" not in QUESTION_IDENTITY_SOURCE
    assert "Mongo" not in QUESTION_IDENTITY_SOURCE


def test_production_runtime_uses_question_identity_directly():
    assert "from question_identity import get_qid" in RUNTIME_SOURCE
    assert "question_identity.install_legacy_bridge" not in PRODUCTION_SOURCE
    assert "legacy =" not in PRODUCTION_SOURCE
