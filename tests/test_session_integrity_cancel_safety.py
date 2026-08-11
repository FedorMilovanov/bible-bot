from copy import deepcopy
from datetime import datetime

import pytest
from pymongo.errors import AutoReconnect

import database
from session_integrity import QuizSessionStoreUnavailable, cancel_owned_quiz_session


def _question():
    return {
        "id": "q1",
        "question": "Question",
        "options": ["A", "B"],
        "correct": 0,
    }


def _partial(*, legacy=False):
    doc = {
        "_id": "container-1",
        "user_id": "42",
        "status": "in_progress",
        "mode": "level",
        "level_key": "easy",
        "level_name": "Easy",
        "question_ids": ["q1"],
        "questions_data": [_question()],
        "current_index": 0,
        "correct_count": 0,
        "answered_questions": [],
        "time_limit": None,
        "start_time": datetime(2026, 8, 10, 12, 0, 0).timestamp(),
    }
    if not legacy:
        doc["attempt_id"] = "attempt-1"
    return doc


def _completed():
    doc = _partial()
    doc["current_index"] = 1
    doc["correct_count"] = 1
    doc["answered_questions"] = [
        {
            "index": 0,
            "qid": "q1",
            "user_answer": "A",
            "is_correct": True,
            "question_obj": _question(),
            "ts": "2026-08-10T12:00:05",
        }
    ]
    return doc


def _matches(doc, query):
    for key, expected in query.items():
        if key == "$or":
            if not any(_matches(doc, branch) for branch in expected):
                return False
            continue
        if isinstance(expected, dict) and "$exists" in expected:
            exists = key in doc
            if exists != expected["$exists"]:
                return False
            continue
        if doc.get(key) != expected:
            return False
    return True


class CancelCollection:
    def __init__(self, doc, *, advance_before_update=False, error=None):
        self.doc = deepcopy(doc)
        self.advance_before_update = advance_before_update
        self.error = error
        self.find_filter = None
        self.update_filter = None
        self.update_calls = 0

    def find_one(self, query):
        self.find_filter = deepcopy(query)
        if self.error is not None:
            raise self.error
        if self.doc is None or not _matches(self.doc, query):
            return None
        return deepcopy(self.doc)

    def find_one_and_update(self, query, update, return_document=None):
        self.update_calls += 1
        self.update_filter = deepcopy(query)
        if self.error is not None:
            raise self.error
        if self.advance_before_update:
            self.doc = _completed()
        if self.doc is None or not _matches(self.doc, query):
            return None
        before = deepcopy(self.doc)
        self.doc.update(deepcopy(update.get("$set", {})))
        return before


def _install(monkeypatch, collection):
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)


def test_partial_owner_cancel_is_attempt_and_state_bound(monkeypatch):
    collection = CancelCollection(_partial())
    _install(monkeypatch, collection)

    result = cancel_owned_quiz_session("container-1", 42)

    assert result is not None
    assert result["status"] == "in_progress"
    assert collection.doc["status"] == "cancelled"
    assert collection.update_filter == {
        "_id": "container-1",
        "user_id": "42",
        "status": "in_progress",
        "current_index": 0,
        "correct_count": 0,
        "question_ids": ["q1"],
        "answered_questions": [],
        "$or": [
            {"attempt_id": "attempt-1"},
            {"attempt_id": {"$exists": False}, "_id": "attempt-1"},
        ],
    }


def test_completed_owner_session_cannot_be_cancelled(monkeypatch):
    collection = CancelCollection(_completed())
    _install(monkeypatch, collection)

    assert cancel_owned_quiz_session("container-1", 42) is None
    assert collection.update_calls == 0
    assert collection.doc["status"] == "in_progress"


def test_last_answer_wins_race_against_legacy_cancel(monkeypatch):
    collection = CancelCollection(_partial(), advance_before_update=True)
    _install(monkeypatch, collection)

    assert cancel_owned_quiz_session("container-1", 42) is None
    assert collection.update_calls == 1
    assert collection.doc["current_index"] == 1
    assert collection.doc["answered_questions"][0]["qid"] == "q1"
    assert collection.doc["status"] == "in_progress"


def test_contradictory_owner_session_fails_closed_without_write(monkeypatch):
    doc = _partial()
    doc["question_ids"] = []
    collection = CancelCollection(doc)
    _install(monkeypatch, collection)

    assert cancel_owned_quiz_session("container-1", 42) is None
    assert collection.update_calls == 0
    assert collection.doc["status"] == "in_progress"


def test_legacy_attempt_identity_uses_container_fallback(monkeypatch):
    collection = CancelCollection(_partial(legacy=True))
    _install(monkeypatch, collection)

    result = cancel_owned_quiz_session("container-1", 42)

    assert result is not None
    assert collection.update_filter["$or"] == [
        {"attempt_id": "container-1"},
        {"attempt_id": {"$exists": False}, "_id": "container-1"},
    ]


def test_owner_cancel_storage_failure_is_explicit(monkeypatch):
    collection = CancelCollection(_partial(), error=AutoReconnect("mongo down"))
    _install(monkeypatch, collection)

    with pytest.raises(QuizSessionStoreUnavailable, match="cancellation failed"):
        cancel_owned_quiz_session("container-1", 42)
