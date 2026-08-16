from pathlib import Path
from types import SimpleNamespace

import pytest

import quiz_answer_history


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
ANSWER_HISTORY_SOURCE = (ROOT / "quiz_answer_history.py").read_text(encoding="utf-8")


def _legacy_correct(question: dict) -> str:
    return question["options"][question["correct"]]


def _legacy_wrong(item: dict) -> bool:
    return item["user_answer"] != _legacy_correct(item["question_obj"])


def _legacy_namespace(**overrides):
    values = {"_correct_text": _legacy_correct, "_is_wrong": _legacy_wrong}
    values.update(overrides)
    return SimpleNamespace(**values)


def test_correct_text_and_wrong_semantics_match_recorded_history_contract():
    question = {
        "question": "Какой вариант верный?",
        "options": ["первый", "второй", "третий"],
        "correct": 1,
    }
    correct = {"user_answer": "второй", "question_obj": question}
    wrong = {"user_answer": "первый", "question_obj": question}

    assert quiz_answer_history.correct_text(question) == "второй"
    assert quiz_answer_history.is_wrong(correct) is False
    assert quiz_answer_history.is_wrong(wrong) is True


def test_helpers_preserve_fail_fast_shape_errors():
    with pytest.raises(KeyError):
        quiz_answer_history.correct_text({"options": ["A"]})
    with pytest.raises(IndexError):
        quiz_answer_history.correct_text({"options": ["A"], "correct": 3})
    with pytest.raises(KeyError):
        quiz_answer_history.is_wrong({"question_obj": {"options": ["A"], "correct": 0}})


def test_bridge_replaces_both_legacy_helpers_by_identity_after_parity():
    legacy = _legacy_namespace()

    quiz_answer_history.install_legacy_bridge(legacy)

    assert legacy._correct_text is quiz_answer_history.correct_text
    assert legacy._is_wrong is quiz_answer_history.is_wrong

    quiz_answer_history.install_legacy_bridge(legacy)
    assert legacy._correct_text is quiz_answer_history.correct_text
    assert legacy._is_wrong is quiz_answer_history.is_wrong


def test_bridge_rejects_correct_text_drift_without_partial_replacement():
    def drifted_correct(_question: dict) -> str:
        return "A"

    original_wrong = _legacy_wrong
    legacy = _legacy_namespace(_correct_text=drifted_correct)

    with pytest.raises(RuntimeError, match="_correct_text diverged"):
        quiz_answer_history.install_legacy_bridge(legacy)

    assert legacy._correct_text is drifted_correct
    assert legacy._is_wrong is original_wrong


def test_bridge_rejects_wrong_semantics_drift_without_partial_replacement():
    def drifted_wrong(_item: dict) -> bool:
        return False

    original_correct = _legacy_correct
    legacy = _legacy_namespace(_is_wrong=drifted_wrong)

    with pytest.raises(RuntimeError, match="_is_wrong diverged"):
        quiz_answer_history.install_legacy_bridge(legacy)

    assert legacy._correct_text is original_correct
    assert legacy._is_wrong is drifted_wrong


@pytest.mark.parametrize(
    "legacy",
    [
        SimpleNamespace(_is_wrong=_legacy_wrong),
        SimpleNamespace(_correct_text=_legacy_correct),
        SimpleNamespace(_correct_text=None, _is_wrong=_legacy_wrong),
        SimpleNamespace(_correct_text=_legacy_correct, _is_wrong=None),
    ],
)
def test_bridge_rejects_missing_or_noncallable_helpers(legacy):
    with pytest.raises(TypeError):
        quiz_answer_history.install_legacy_bridge(legacy)


def test_answer_history_module_is_pure_and_does_not_import_legacy_or_database():
    assert "import bot" not in ANSWER_HISTORY_SOURCE
    assert "from bot" not in ANSWER_HISTORY_SOURCE
    assert "database" not in ANSWER_HISTORY_SOURCE
    assert "telegram" not in ANSWER_HISTORY_SOURCE


def test_production_composition_root_installs_answer_history_bridge():
    assert "import quiz_answer_history as answer_history" in PRODUCTION_SOURCE
    assert "answer_history.install_legacy_bridge(legacy)" in PRODUCTION_SOURCE
