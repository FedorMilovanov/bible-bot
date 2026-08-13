import uuid

import pytest

import database


class RecordingQuizSessions:
    def __init__(self, *, fail=False, find_result=None, fail_find=False):
        self.fail = fail
        self.find_result = find_result
        self.fail_find = fail_find
        self.inserted = []
        self.find_queries = []

    def insert_one(self, doc):
        if self.fail:
            raise RuntimeError("mongo unavailable")
        self.inserted.append(doc)

    def find_one(self, query):
        self.find_queries.append(query)
        if self.fail_find:
            raise RuntimeError("mongo unavailable")
        return self.find_result


def create_session():
    return database.create_quiz_session(
        user_id=42,
        mode="level",
        question_ids=["q1"],
        questions_data=[{"id": "q1", "question": "Q", "options": ["A"], "correct": 0}],
        level_key="easy",
        level_name="Easy",
        time_limit=None,
        chat_id=100,
    )


def test_disabled_session_store_fails_before_ram_can_use_phantom_id(monkeypatch):
    monkeypatch.setattr(database, "quiz_sessions_collection", None)

    with pytest.raises(
        database.LegacyQuizSessionPersistenceUnavailable,
        match="storage is unavailable",
    ):
        create_session()


def test_failed_insert_is_explicit_and_never_returns_phantom_id(monkeypatch):
    collection = RecordingQuizSessions(fail=True)
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    with pytest.raises(
        database.LegacyQuizSessionPersistenceUnavailable,
        match="insert failed",
    ):
        create_session()
    assert collection.inserted == []


def test_successful_insert_returns_the_persisted_uuid(monkeypatch):
    collection = RecordingQuizSessions()
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    session_id = create_session()

    assert str(uuid.UUID(session_id)) == session_id
    assert len(collection.inserted) == 1
    assert collection.inserted[0]["_id"] == session_id
    assert collection.inserted[0]["session_id"] == session_id
    assert collection.inserted[0]["user_id"] == "42"


def test_active_session_true_absence_stays_distinct_from_store_failure(monkeypatch):
    collection = RecordingQuizSessions(find_result=None)
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert database.get_active_quiz_session(42) is None
    assert collection.find_queries == [
        {"user_id": "42", "status": "in_progress"}
    ]


def test_disabled_active_session_lookup_is_explicit(monkeypatch):
    monkeypatch.setattr(database, "quiz_sessions_collection", None)

    with pytest.raises(
        database.LegacyQuizSessionPersistenceUnavailable,
        match="storage is unavailable",
    ):
        database.get_active_quiz_session(42)


def test_failed_active_session_lookup_is_not_false_absence(monkeypatch):
    collection = RecordingQuizSessions(fail_find=True)
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    with pytest.raises(
        database.LegacyQuizSessionPersistenceUnavailable,
        match="lookup failed",
    ):
        database.get_active_quiz_session(42)
