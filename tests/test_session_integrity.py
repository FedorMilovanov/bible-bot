from copy import deepcopy

import pytest
from pymongo.errors import AutoReconnect

import database
from session_integrity import (
    QuizSessionAnswerConflict,
    QuizSessionStoreUnavailable,
    cancel_owned_quiz_session,
    finish_owned_quiz_session,
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


def test_owned_session_lookup_scopes_by_session_and_canonical_user_id(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.session = {"_id": "s1", "user_id": "42", "status": "in_progress"}
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert get_owned_quiz_session("s1", 42) == collection.session
    assert collection.find_filter == {"_id": "s1", "user_id": "42"}


def test_owned_answer_first_apply_uses_owner_index_and_question_cas(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.claimed_session = {
        "_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "current_index": 1,
    }
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: _FakeNow("2026-08-10T12:00:03"))

    result = record_owned_quiz_answer(
        "s1",
        42,
        expected_index=0,
        question_id="q1",
        user_answer="A",
        is_correct=True,
        question_obj={"id": "q1", "question": "Q"},
        latency_seconds=2.5,
    )

    assert result["applied"] is True
    assert result["session"] == collection.claimed_session
    assert collection.claim_filter == {
        "_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "current_index": 0,
        "question_ids.0": "q1",
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
    assert collection.claim_update["$set"]["question_sent_at"] is None


def test_owned_answer_lost_response_retry_returns_exact_ledger_entry(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.claimed_session = None
    collection.session = {
        "_id": "s1",
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

    result = record_owned_quiz_answer(
        "s1",
        42,
        expected_index=0,
        question_id="q1",
        user_answer="A",
        is_correct=True,
        question_obj={"id": "q1"},
        latency_seconds=9.0,
    )

    assert result["applied"] is False
    assert result["answer"] == collection.session["answered_questions"][0]
    # The retry attempted only the guarded CAS. It did not issue any blind
    # second increment/push after discovering that index 0 was already durable.
    assert collection.claim_filter["current_index"] == 0


def test_owned_answer_conflicting_replay_is_rejected(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.claimed_session = None
    collection.session = {
        "_id": "s1",
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
        record_owned_quiz_answer(
            "s1",
            42,
            expected_index=0,
            question_id="q1",
            user_answer="B",
            is_correct=False,
            question_obj={"id": "q1"},
        )


def test_owned_answer_same_qid_on_next_index_is_not_ambiguous(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.claimed_session = {
        "_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "current_index": 2,
    }
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: _FakeNow("2026-08-10T12:00:20"))

    result = record_owned_quiz_answer(
        "s1",
        42,
        expected_index=1,
        question_id="same-qid",
        user_answer="B",
        is_correct=False,
        question_obj={"id": "same-qid"},
    )

    assert result["applied"] is True
    assert collection.claim_filter["current_index"] == 1
    assert collection.claim_filter["question_ids.1"] == "same-qid"
    assert collection.claim_update["$push"]["answered_questions"]["index"] == 1


def test_owned_answer_missing_or_wrong_index_is_conflict(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.claimed_session = None
    collection.session = {
        "_id": "s1",
        "user_id": "42",
        "status": "in_progress",
        "current_index": 0,
        "answered_questions": [],
    }
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: _FakeNow("2026-08-10T12:00:03"))

    with pytest.raises(QuizSessionAnswerConflict, match="index does not match"):
        record_owned_quiz_answer(
            "s1",
            42,
            expected_index=1,
            question_id="q2",
            user_answer="A",
            is_correct=True,
            question_obj={"id": "q2"},
        )


def test_owned_answer_mongo_failure_is_explicit(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.claim_error = AutoReconnect("mongo unavailable")
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: _FakeNow("2026-08-10T12:00:03"))

    with pytest.raises(QuizSessionStoreUnavailable, match="quiz answer write failed"):
        record_owned_quiz_answer(
            "s1",
            42,
            expected_index=0,
            question_id="q1",
            user_answer="A",
            is_correct=True,
            question_obj={"id": "q1"},
        )


def test_owned_answer_validates_index_and_latency_before_store(monkeypatch):
    collection = FakeQuizSessionCollection()
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    with pytest.raises(ValueError, match="expected_index"):
        record_owned_quiz_answer(
            "s1",
            42,
            expected_index=-1,
            question_id="q1",
            user_answer="A",
            is_correct=True,
            question_obj={"id": "q1"},
        )
    assert collection.claim_filter is None

    with pytest.raises(ValueError, match="latency_seconds"):
        record_owned_quiz_answer(
            "s1",
            42,
            expected_index=0,
            question_id="q1",
            user_answer="A",
            is_correct=True,
            question_obj={"id": "q1"},
            latency_seconds=float("inf"),
        )
    assert collection.claim_filter is None


def test_owned_session_cancel_is_atomic_and_returns_original_snapshot(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.claimed_session = {"_id": "s1", "user_id": "42", "status": "in_progress"}
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert cancel_owned_quiz_session("s1", 42) == collection.claimed_session
    assert collection.claim_filter == {
        "_id": "s1",
        "user_id": "42",
        "status": "in_progress",
    }
    assert collection.claim_update == {"$set": {"status": "cancelled"}}


def test_owned_session_finish_is_atomic_and_owner_scoped(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.claimed_session = {"_id": "s1", "user_id": "42", "status": "finished"}
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: "NOW")

    assert finish_owned_quiz_session("s1", 42) == collection.claimed_session
    assert collection.claim_filter == {
        "_id": "s1",
        "user_id": "42",
        "status": "in_progress",
    }
    assert collection.claim_update == {
        "$set": {"status": "finished", "end_time": "NOW"}
    }


def test_owned_session_finish_is_idempotent_when_already_finished(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.claimed_session = None
    collection.session = {"_id": "s1", "user_id": "42", "status": "finished"}
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: "NOW")

    assert finish_owned_quiz_session("s1", 42) == collection.session
    assert collection.find_filter == {"_id": "s1", "user_id": "42"}


def test_database_uid_contract_matches_session_owner_storage():
    assert database._uid(42) == "42"


class _FakeNow:
    def __init__(self, iso: str):
        self._iso = iso

    def isoformat(self):
        return self._iso
