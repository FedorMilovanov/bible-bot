from copy import deepcopy
from datetime import datetime

import pytest

import database
from legacy_session_close import (
    QuizSessionCompletionInvalid,
    finish_completed_owned_quiz_session,
    validate_completed_owned_quiz_session,
)


class SessionCollection:
    def __init__(self, doc):
        self.doc = deepcopy(doc) if doc is not None else None
        self.update_filter = None
        self.update_doc = None

    def find_one(self, query):
        if self.doc is None:
            return None
        for key, value in query.items():
            if self.doc.get(key) != value:
                return None
        return deepcopy(self.doc)

    def find_one_and_update(self, query, update, return_document=None):
        self.update_filter = deepcopy(query)
        self.update_doc = deepcopy(update)
        if self.doc is None:
            return None
        for key, value in query.items():
            if self.doc.get(key) != value:
                return None
        for key, value in update.get("$set", {}).items():
            self.doc[key] = deepcopy(value)
        return deepcopy(self.doc)


def _session(*, status="in_progress", current=2, answered=None, correct_count=1):
    question_ids = ["q1", "q2"]
    if answered is None:
        answered = [
            {"index": 0, "qid": "q1", "is_correct": True},
            {"index": 1, "qid": "q2", "is_correct": False},
        ]
    return {
        "_id": "s1",
        "user_id": "42",
        "status": status,
        "current_index": current,
        "correct_count": correct_count,
        "question_ids": question_ids,
        "answered_questions": answered,
    }


def _install(monkeypatch, doc):
    collection = SessionCollection(doc)
    now = datetime(2026, 8, 10, 12, 0, 0)
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: now)
    return collection, now


def test_read_only_completion_proof_accepts_exact_owned_ledger(monkeypatch):
    collection, _now = _install(monkeypatch, _session())

    result = validate_completed_owned_quiz_session("s1", 42)

    assert result["correct_count"] == 1
    assert collection.update_filter is None


def test_exact_complete_owned_session_is_closed_atomically(monkeypatch):
    collection, now = _install(monkeypatch, _session())

    result = finish_completed_owned_quiz_session("s1", 42)

    assert result["status"] == "finished"
    assert result["end_time"] == now
    assert collection.update_filter == {
        "_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "current_index": 2,
        "correct_count": 1,
        "question_ids": ["q1", "q2"],
        "answered_questions": [
            {"index": 0, "qid": "q1", "is_correct": True},
            {"index": 1, "qid": "q2", "is_correct": False},
        ],
    }


def test_incomplete_current_index_cannot_be_finished(monkeypatch):
    collection, _now = _install(
        monkeypatch,
        _session(current=1, answered=[{"index": 0, "qid": "q1", "is_correct": True}]),
    )

    with pytest.raises(QuizSessionCompletionInvalid, match="exact completed"):
        finish_completed_owned_quiz_session("s1", 42)

    assert collection.update_filter is None


def test_complete_index_with_short_answer_ledger_is_rejected(monkeypatch):
    collection, _now = _install(
        monkeypatch,
        _session(current=2, answered=[{"index": 0, "qid": "q1", "is_correct": True}]),
    )

    with pytest.raises(QuizSessionCompletionInvalid, match="exact completed"):
        finish_completed_owned_quiz_session("s1", 42)

    assert collection.update_filter is None


def test_answer_qid_and_index_must_match_durable_order(monkeypatch):
    _install(
        monkeypatch,
        _session(
            answered=[
                {"index": 0, "qid": "q2", "is_correct": True},
                {"index": 1, "qid": "q1", "is_correct": False},
            ]
        ),
    )
    with pytest.raises(QuizSessionCompletionInvalid, match="answer order"):
        validate_completed_owned_quiz_session("s1", 42)

    _install(
        monkeypatch,
        _session(
            answered=[
                {"index": 1, "qid": "q1", "is_correct": True},
                {"index": 0, "qid": "q2", "is_correct": False},
            ]
        ),
    )
    with pytest.raises(QuizSessionCompletionInvalid, match="answer index"):
        validate_completed_owned_quiz_session("s1", 42)


def test_correct_count_must_match_boolean_answer_ledger(monkeypatch):
    _install(monkeypatch, _session(correct_count=2))

    with pytest.raises(QuizSessionCompletionInvalid, match="correct_count"):
        validate_completed_owned_quiz_session("s1", 42)


def test_non_boolean_correctness_is_rejected(monkeypatch):
    _install(
        monkeypatch,
        _session(
            answered=[
                {"index": 0, "qid": "q1", "is_correct": "true"},
                {"index": 1, "qid": "q2", "is_correct": False},
            ]
        ),
    )

    with pytest.raises(QuizSessionCompletionInvalid, match="correctness"):
        validate_completed_owned_quiz_session("s1", 42)


def test_already_finished_exact_session_is_idempotent(monkeypatch):
    collection, _now = _install(monkeypatch, _session(status="finished"))

    result = finish_completed_owned_quiz_session("s1", 42)

    assert result["status"] == "finished"
    assert collection.update_filter is None


def test_finished_but_corrupt_session_is_not_accepted_as_idempotent(monkeypatch):
    _install(
        monkeypatch,
        _session(
            status="finished",
            current=1,
            answered=[{"index": 0, "qid": "q1", "is_correct": True}],
        ),
    )

    with pytest.raises(QuizSessionCompletionInvalid, match="exact completed"):
        finish_completed_owned_quiz_session("s1", 42)


def test_cancelled_or_missing_session_is_nonrecoverable_noop(monkeypatch):
    collection, _now = _install(monkeypatch, _session(status="cancelled"))
    assert validate_completed_owned_quiz_session("s1", 42) is None
    assert finish_completed_owned_quiz_session("s1", 42) is None
    assert collection.update_filter is None

    _install(monkeypatch, None)
    assert validate_completed_owned_quiz_session("s1", 42) is None
    assert finish_completed_owned_quiz_session("s1", 42) is None


def test_other_user_cannot_prove_or_close_session(monkeypatch):
    collection, _now = _install(monkeypatch, _session())

    assert validate_completed_owned_quiz_session("s1", 99) is None
    assert finish_completed_owned_quiz_session("s1", 99) is None
    assert collection.update_filter is None
