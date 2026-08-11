from copy import deepcopy

import pytest
from pymongo.errors import AutoReconnect

import database
from session_integrity import (
    QuizSessionAnswerConflict,
    QuizSessionStoreUnavailable,
    cancel_owned_quiz_session,
    get_owned_quiz_session,
    record_owned_quiz_answer,
)


class FakeQuizSessionCollection:
    def __init__(self):
        self.find_filter = None
        self.claim_filter = None
        self.claim_update = None
        self.session = None
        self.claimed_session = None
        self.claim_error = None
        self.find_error = None

    def find_one(self, query):
        self.find_filter = deepcopy(query)
        if self.find_error is not None:
            raise self.find_error
        return deepcopy(self.session)

    def find_one_and_update(self, query, update, return_document=None):
        self.claim_filter = deepcopy(query)
        self.claim_update = deepcopy(update)
        if self.claim_error is not None:
            raise self.claim_error
        return deepcopy(self.claimed_session)


def _resumable_session(*, current_index=0):
    answers = []
    correct_count = 0
    if current_index:
        answers = [
            {
                "index": 0,
                "qid": "q1",
                "user_answer": "A",
                "is_correct": True,
                "question_obj": {"id": "q1"},
                "ts": "2026-08-10T12:00:01",
            }
        ]
        correct_count = 1
    return {
        "_id": "s1",
        "attempt_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "mode": "level",
        "level_key": "easy",
        "question_ids": ["q1", "q2"],
        "questions_data": [{"id": "q1"}, {"id": "q2"}],
        "current_index": current_index,
        "correct_count": correct_count,
        "answered_questions": answers,
        "time_limit": None,
        "start_time": 0.0,
    }


def _answer(collection, *, expected_attempt_id="s1", expected_index=0, **overrides):
    kwargs = {
        "question_id": "q1",
        "user_answer": "A",
        "is_correct": True,
        "question_obj": {"id": "q1", "question": "Q"},
        "latency_seconds": 2.5,
    }
    kwargs.update(overrides)
    return record_owned_quiz_answer(
        "s1",
        42,
        expected_attempt_id=expected_attempt_id,
        expected_index=expected_index,
        **kwargs,
    )


def test_owned_session_lookup_scopes_by_session_and_canonical_user_id(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.session = _resumable_session(current_index=1)
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert get_owned_quiz_session("s1", 42) == collection.session
    assert collection.find_filter == {"_id": "s1", "user_id": "42"}


def test_owned_resume_refuses_exact_completed_session(monkeypatch):
    collection = FakeQuizSessionCollection()
    session = _resumable_session(current_index=1)
    session["question_ids"] = ["q1"]
    session["questions_data"] = [{"id": "q1"}]
    collection.session = session
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert get_owned_quiz_session("s1", 42) is None


def test_owned_resume_refuses_contradictory_session(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.session = {"_id": "s1", "user_id": "42", "status": "in_progress"}
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert get_owned_quiz_session("s1", 42) is None


def test_owned_answer_first_apply_uses_owner_attempt_index_and_question_cas(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.claimed_session = {
        "_id": "s1",
        "attempt_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "current_index": 1,
    }
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: _FakeNow("2026-08-10T12:00:03"))

    result = _answer(collection)

    assert result["applied"] is True
    assert result["session"] == collection.claimed_session
    assert collection.claim_filter == {
        "_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "current_index": 0,
        "question_ids.0": "q1",
        "answered_questions": {"$type": "array"},
        "answered_questions.0": {"$exists": False},
        "correct_count": 0,
        "$or": [
            {"attempt_id": "s1"},
            {"attempt_id": {"$exists": False}, "_id": "s1"},
        ],
    }
    assert collection.claim_update["$inc"] == {
        "current_index": 1,
        "correct_count": 1,
    }
    stored = collection.claim_update["$push"]["answered_questions"]
    assert stored["index"] == 0
    assert stored["qid"] == "q1"
    assert stored["user_answer"] == "A"
    assert stored["is_correct"] is True
    assert stored["latency_seconds"] == 2.5
    assert stored["ts"] == "2026-08-10T12:00:03"
    assert collection.claim_update["$set"]["attempt_id"] == "s1"
    assert collection.claim_update["$set"]["question_sent_at"] is None


def test_owned_answer_lost_response_retry_returns_exact_ledger_entry(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.claimed_session = None
    collection.session = {
        "_id": "s1",
        "attempt_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "current_index": 1,
        "answered_questions": [
            {
                "index": 0,
                "qid": "q1",
                "user_answer": "A",
                "is_correct": True,
                "latency_seconds": 2.5,
                "ts": "2026-08-10T12:00:03",
            }
        ],
    }
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: _FakeNow("2026-08-10T12:00:10"))

    result = _answer(collection, latency_seconds=9.0)

    assert result["applied"] is False
    assert result["answer"] == collection.session["answered_questions"][0]
    assert collection.claim_filter["current_index"] == 0


def test_old_attempt_cannot_mutate_restarted_container(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.claimed_session = None
    collection.session = {
        "_id": "s1",
        "attempt_id": "attempt-new",
        "user_id": "42",
        "status": "in_progress",
        "current_index": 0,
        "answered_questions": [],
    }
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: _FakeNow("2026-08-10T12:00:10"))

    with pytest.raises(QuizSessionAnswerConflict, match="another attempt"):
        _answer(collection, expected_attempt_id="attempt-old")


def test_owned_answer_conflicting_replay_is_rejected(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.claimed_session = None
    collection.session = {
        "_id": "s1",
        "attempt_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "current_index": 1,
        "answered_questions": [
            {
                "index": 0,
                "qid": "q1",
                "user_answer": "A",
                "is_correct": True,
                "ts": "2026-08-10T12:00:03",
            }
        ],
    }
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: _FakeNow("2026-08-10T12:00:10"))

    with pytest.raises(QuizSessionAnswerConflict, match="conflicting quiz answer"):
        _answer(collection, user_answer="B", is_correct=False)


def test_owned_answer_same_qid_on_next_index_is_not_ambiguous(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.claimed_session = {
        "_id": "s1",
        "attempt_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "current_index": 2,
    }
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: _FakeNow("2026-08-10T12:00:20"))

    result = _answer(
        collection,
        expected_index=1,
        question_id="same-qid",
        user_answer="B",
        is_correct=False,
        question_obj={"id": "same-qid"},
        latency_seconds=None,
    )

    assert result["applied"] is True
    assert collection.claim_filter["current_index"] == 1
    assert collection.claim_filter["question_ids.1"] == "same-qid"
    assert collection.claim_filter["answered_questions"] == {"$type": "array"}
    assert collection.claim_filter["answered_questions.0"] == {"$exists": True}
    assert collection.claim_filter["answered_questions.1"] == {"$exists": False}
    assert collection.claim_update["$push"]["answered_questions"]["index"] == 1


def test_owned_answer_missing_or_wrong_index_is_conflict(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.claimed_session = None
    collection.session = {
        "_id": "s1",
        "attempt_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "current_index": 0,
        "answered_questions": [],
    }
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: _FakeNow("2026-08-10T12:00:03"))

    with pytest.raises(
        QuizSessionAnswerConflict,
        match="not the immediately preceding durable transition",
    ):
        _answer(
            collection,
            expected_index=1,
            question_id="q2",
            question_obj={"id": "q2"},
        )


def test_owned_answer_mongo_failure_is_explicit(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.claim_error = AutoReconnect("mongo unavailable")
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: _FakeNow("2026-08-10T12:00:03"))

    with pytest.raises(QuizSessionStoreUnavailable, match="quiz answer write failed"):
        _answer(collection)


def test_owned_answer_validates_attempt_index_and_latency_before_store(monkeypatch):
    collection = FakeQuizSessionCollection()
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    with pytest.raises(ValueError, match="expected_attempt_id"):
        _answer(collection, expected_attempt_id="")
    assert collection.claim_filter is None

    with pytest.raises(ValueError, match="expected_index"):
        _answer(collection, expected_index=-1)
    assert collection.claim_filter is None

    with pytest.raises(ValueError, match="latency_seconds"):
        _answer(collection, latency_seconds=float("inf"))
    assert collection.claim_filter is None


def test_owned_session_cancel_refuses_unprovable_snapshot(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.session = {"_id": "s1", "user_id": "42", "status": "in_progress"}
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert cancel_owned_quiz_session("s1", 42) is None
    assert collection.claim_filter is None


def test_database_uid_contract_matches_session_owner_storage():
    assert database._uid(42) == "42"


class _FakeNow:
    def __init__(self, iso: str):
        self._iso = iso

    def isoformat(self):
        return self._iso
