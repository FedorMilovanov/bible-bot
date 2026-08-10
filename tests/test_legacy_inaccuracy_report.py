from copy import deepcopy

import pytest

import legacy_inaccuracy_report as inaccuracy


def _question(text="Who?", *, correct=1):
    return {
        "question": text,
        "options": ["Peter", "Paul", "John"],
        "correct": correct,
    }


def test_stable_report_id_is_attempt_and_question_position_bound():
    first = inaccuracy.stable_inaccuracy_report_id(
        user_id=42,
        attempt_id="attempt-1",
        question_index=3,
        question_id="qid-1",
    )
    replay = inaccuracy.stable_inaccuracy_report_id(
        user_id="42",
        attempt_id="attempt-1",
        question_index=3,
        question_id="qid-1",
    )
    other_attempt = inaccuracy.stable_inaccuracy_report_id(
        user_id=42,
        attempt_id="attempt-2",
        question_index=3,
        question_id="qid-1",
    )
    other_position = inaccuracy.stable_inaccuracy_report_id(
        user_id=42,
        attempt_id="attempt-1",
        question_index=4,
        question_id="qid-1",
    )

    assert first == replay
    assert first.startswith("inaccuracy-")
    assert first != other_attempt
    assert first != other_position


def test_acceptance_uses_stable_id_and_copies_question_snapshot(monkeypatch):
    captured = {}
    question = _question()
    original = deepcopy(question)

    def accept(**kwargs):
        captured.update(kwargs)
        return {"_id": kwargs["report_id"], "context": deepcopy(kwargs["context"])}

    monkeypatch.setattr(inaccuracy, "accept_report_once", accept)

    result = inaccuracy.accept_inaccuracy_report_once(
        user_id=42,
        username="tester",
        first_name="Test",
        attempt_id="attempt-1",
        question_index=2,
        question=question,
        level_name="Easy",
    )
    question["question"] = "mutated later"

    assert result["_id"] == captured["report_id"]
    assert captured["report_type"] == "bug"
    assert captured["photo_file_id"] is None
    assert captured["context"]["kind"] == "question_inaccuracy"
    assert captured["context"]["attempt_id"] == "attempt-1"
    assert captured["context"]["question_index"] == 2
    assert captured["context"]["question"] == original
    assert "Вопрос 3: Who?" in captured["text"]
    assert "Правильный ответ в базе: Paul" in captured["text"]


def test_profile_changes_do_not_change_stable_report_identity(monkeypatch):
    seen = []

    def accept(**kwargs):
        seen.append(kwargs["report_id"])
        return {"_id": kwargs["report_id"]}

    monkeypatch.setattr(inaccuracy, "accept_report_once", accept)
    common = {
        "user_id": 42,
        "attempt_id": "attempt-1",
        "question_index": 0,
        "question": _question(),
        "level_name": "Easy",
    }
    inaccuracy.accept_inaccuracy_report_once(
        username="old",
        first_name="Old",
        **common,
    )
    inaccuracy.accept_inaccuracy_report_once(
        username="new",
        first_name="New",
        **common,
    )

    assert seen[0] == seen[1]


def test_question_content_change_changes_report_identity(monkeypatch):
    seen = []
    monkeypatch.setattr(
        inaccuracy,
        "accept_report_once",
        lambda **kwargs: seen.append(kwargs["report_id"]) or {"_id": kwargs["report_id"]},
    )

    common = {
        "user_id": 42,
        "username": None,
        "first_name": "User",
        "attempt_id": "attempt-1",
        "question_index": 0,
        "level_name": "Easy",
    }
    inaccuracy.accept_inaccuracy_report_once(question=_question("Who?"), **common)
    inaccuracy.accept_inaccuracy_report_once(question=_question("Where?"), **common)

    assert seen[0] != seen[1]


def test_long_admin_text_is_bounded_before_report_store(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        inaccuracy,
        "accept_report_once",
        lambda **kwargs: captured.update(kwargs) or {"_id": kwargs["report_id"]},
    )
    question = {
        "question": "Q" * 1800,
        "options": ["A" * 500, "B" * 500],
        "correct": 0,
    }

    inaccuracy.accept_inaccuracy_report_once(
        user_id=42,
        username=None,
        first_name=None,
        attempt_id="attempt-1",
        question_index=0,
        question=question,
    )

    assert 0 < len(captured["text"]) <= 2000


@pytest.mark.parametrize(
    "overrides",
    [
        {"attempt_id": ""},
        {"question_index": -1},
        {"question": None},
        {"question": {"question": "Q", "options": [], "correct": 0}},
        {"question": {"question": "Q", "options": ["A"], "correct": True}},
    ],
)
def test_invalid_inaccuracy_evidence_fails_before_store(monkeypatch, overrides):
    monkeypatch.setattr(
        inaccuracy,
        "accept_report_once",
        lambda **_kwargs: pytest.fail("invalid evidence must not reach report store"),
    )
    params = {
        "user_id": 42,
        "username": None,
        "first_name": None,
        "attempt_id": "attempt-1",
        "question_index": 0,
        "question": _question(),
    }
    params.update(overrides)

    with pytest.raises(inaccuracy.LegacyInaccuracyReportInvalid):
        inaccuracy.accept_inaccuracy_report_once(**params)
