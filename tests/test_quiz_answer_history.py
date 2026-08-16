from pathlib import Path

import pytest

import quiz_answer_history


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_SOURCE = (ROOT / "telegram_production.py").read_text(encoding="utf-8")
RUNTIME_SOURCE = (ROOT / "telegram_quiz_runtime_controller.py").read_text(encoding="utf-8")
ANSWER_HISTORY_SOURCE = (ROOT / "quiz_answer_history.py").read_text(encoding="utf-8")


def _question() -> dict:
    return {
        "question": "Какой вариант верный?",
        "options": ["первый", "второй", "третий"],
        "correct": 1,
    }


def test_correct_text_and_wrong_semantics_match_recorded_history_contract():
    question = _question()
    correct = {"user_answer": "второй", "question_obj": question}
    wrong = {"user_answer": "первый", "question_obj": question}

    assert quiz_answer_history.correct_text(question) == "второй"
    assert quiz_answer_history.is_wrong(correct) is False
    assert quiz_answer_history.is_wrong(wrong) is True


def test_progress_bar_preserves_answer_and_current_question_semantics():
    question = _question()
    correct = {"user_answer": "второй", "question_obj": question}
    wrong = {"user_answer": "первый", "question_obj": question}

    assert quiz_answer_history.build_progress_bar(2, 4) == "⬜🟨⬜⬜"
    assert quiz_answer_history.build_progress_bar(3, 4, [correct, wrong]) == "🟩🟥🟨⬜"
    assert quiz_answer_history.build_progress_bar(1, 2, [{"question_obj": question}]) == "🟥⬜"
    assert quiz_answer_history.build_progress_bar(0, 0, []) == ""


def test_helpers_preserve_fail_fast_shape_errors():
    with pytest.raises(KeyError):
        quiz_answer_history.correct_text({"options": ["A"]})
    with pytest.raises(IndexError):
        quiz_answer_history.correct_text({"options": ["A"], "correct": 3})
    with pytest.raises(KeyError):
        quiz_answer_history.is_wrong({"question_obj": {"options": ["A"], "correct": 0}})
    with pytest.raises(KeyError):
        quiz_answer_history.build_progress_bar(
            1,
            1,
            [{"user_answer": "A", "question_obj": {"options": ["A"]}}],
        )


def test_answer_history_module_is_pure_and_has_no_monolith_bridge():
    assert "import bot" not in ANSWER_HISTORY_SOURCE
    assert "from bot" not in ANSWER_HISTORY_SOURCE
    assert "install_legacy_bridge" not in ANSWER_HISTORY_SOURCE
    assert "database" not in ANSWER_HISTORY_SOURCE
    assert "telegram" not in ANSWER_HISTORY_SOURCE


def test_production_runtime_uses_answer_history_directly():
    assert "from quiz_answer_history import build_progress_bar, is_wrong" in RUNTIME_SOURCE
    assert "answer_history.install_legacy_bridge" not in PRODUCTION_SOURCE
    assert "legacy =" not in PRODUCTION_SOURCE
