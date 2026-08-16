from pathlib import Path
import ast

from question_identity import get_qid


ROOT = Path(__file__).resolve().parents[1]
CONTROLLER_SOURCE = (ROOT / "telegram_challenge_controller.py").read_text(encoding="utf-8")


def _restart_function() -> ast.AsyncFunctionDef:
    tree = ast.parse(CONTROLLER_SOURCE)
    return next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "restart_session_handler"
    )


def test_challenge_restart_imports_canonical_question_identity_directly():
    assert "from question_identity import get_qid" in CONTROLLER_SOURCE
    assert "quiz.legacy.get_qid" not in CONTROLLER_SOURCE


def test_restart_question_ids_preserve_question_order_through_canonical_get_qid():
    questions = [
        {"question": "Первый?", "options": ["alpha", "beta"], "correct": 0},
        {"question": "Второй?", "options": ["gamma", "delta"], "correct": 1},
        {"question": "Третий?", "options": ["epsilon", "zeta"], "correct": 0},
    ]

    assert [get_qid(item) for item in questions] == [
        get_qid(questions[0]),
        get_qid(questions[1]),
        get_qid(questions[2]),
    ]


def test_restart_wires_ordered_get_qid_comprehension_into_durable_restart():
    restart = _restart_function()
    keyword_values = {
        keyword.arg: keyword.value
        for node in ast.walk(restart)
        if isinstance(node, ast.Call)
        for keyword in node.keywords
        if keyword.arg is not None
    }
    question_ids = keyword_values["question_ids"]

    assert isinstance(question_ids, ast.ListComp)
    assert len(question_ids.generators) == 1
    generator = question_ids.generators[0]
    assert isinstance(generator.target, ast.Name)
    assert generator.target.id == "item"
    assert isinstance(generator.iter, ast.Name)
    assert generator.iter.id == "questions"
    assert isinstance(question_ids.elt, ast.Call)
    assert isinstance(question_ids.elt.func, ast.Name)
    assert question_ids.elt.func.id == "get_qid"
    assert len(question_ids.elt.args) == 1
    assert isinstance(question_ids.elt.args[0], ast.Name)
    assert question_ids.elt.args[0].id == "item"
