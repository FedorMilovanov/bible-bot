import uuid

import database


class RecordingQuizSessions:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.inserted = []

    def insert_one(self, doc):
        if self.fail:
            raise RuntimeError("mongo unavailable")
        self.inserted.append(doc)


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


def test_disabled_session_store_returns_no_recovery_id(monkeypatch):
    monkeypatch.setattr(database, "quiz_sessions_collection", None)

    assert create_session() is None


def test_failed_insert_returns_no_phantom_recovery_id(monkeypatch):
    collection = RecordingQuizSessions(fail=True)
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert create_session() is None
    assert collection.inserted == []


def test_successful_insert_returns_the_persisted_uuid(monkeypatch):
    collection = RecordingQuizSessions()
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    session_id = create_session()

    assert session_id is not None
    assert str(uuid.UUID(session_id)) == session_id
    assert len(collection.inserted) == 1
    assert collection.inserted[0]["_id"] == session_id
    assert collection.inserted[0]["session_id"] == session_id
    assert collection.inserted[0]["user_id"] == "42"
