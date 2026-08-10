from copy import deepcopy

import pytest

import database
from session_integrity import QuizSessionAnswerConflict, record_owned_quiz_answer


class ReplayCollection:
    def __init__(self, session):
        self.session = deepcopy(session)

    def find_one_and_update(self, query, update, return_document=None):
        return None

    def find_one(self, query):
        return deepcopy(self.session)


class FakeNow:
    def isoformat(self):
        return "2026-08-10T12:00:10"


def _record(monkeypatch, session, *, expected_index=0):
    monkeypatch.setattr(database, "quiz_sessions_collection", ReplayCollection(session))
    monkeypatch.setattr(database, "_now_utc", lambda: FakeNow())
    return record_owned_quiz_answer(
        "s1",
        42,
        expected_index=expected_index,
        question_id=f"q{expected_index}",
        user_answer="A",
        is_correct=True,
        question_obj={"id": f"q{expected_index}"},
        latency_seconds=2.5,
    )


def _answer(index):
    return {
        "index": index,
        "qid": f"q{index}",
        "user_answer": "A",
        "is_correct": True,
        "latency_seconds": 2.5,
        "ts": f"2026-08-10T12:00:{index + 1:02d}",
    }


def test_exact_immediately_preceding_transition_is_replayable(monkeypatch):
    session = {
        "_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "current_index": 1,
        "answered_questions": [_answer(0)],
    }

    result = _record(monkeypatch, session)

    assert result["applied"] is False
    assert result["answer"] == session["answered_questions"][0]


def test_old_answer_after_later_progress_is_not_treated_as_lost_response(monkeypatch):
    session = {
        "_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "current_index": 3,
        "answered_questions": [_answer(0), _answer(1), _answer(2)],
    }

    with pytest.raises(QuizSessionAnswerConflict, match="immediately preceding"):
        _record(monkeypatch, session, expected_index=0)


def test_terminal_session_cannot_replay_old_answer(monkeypatch):
    for status in ("finished", "cancelled"):
        session = {
            "_id": "s1",
            "user_id": "42",
            "status": status,
            "current_index": 1,
            "answered_questions": [_answer(0)],
        }
        with pytest.raises(QuizSessionAnswerConflict, match="not in progress"):
            _record(monkeypatch, session)


def test_ledger_length_must_match_durable_index_for_replay(monkeypatch):
    session = {
        "_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "current_index": 1,
        "answered_questions": [_answer(0), _answer(1)],
    }

    with pytest.raises(QuizSessionAnswerConflict, match="ledger is inconsistent"):
        _record(monkeypatch, session)
