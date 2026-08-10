from types import SimpleNamespace

import database
from session_integrity import cancel_owned_quiz_session, get_owned_quiz_session


class FakeQuizSessionCollection:
    def __init__(self):
        self.find_filter = None
        self.update_filter = None
        self.update_doc = None
        self.session = None
        self.modified_count = 0

    def find_one(self, query):
        self.find_filter = query
        return self.session

    def update_one(self, query, update):
        self.update_filter = query
        self.update_doc = update
        return SimpleNamespace(modified_count=self.modified_count)


def test_owned_session_lookup_scopes_by_session_and_user(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.session = {"_id": "s1", "user_id": 42, "status": "in_progress"}
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert get_owned_quiz_session("s1", 42) == collection.session
    assert collection.find_filter == {"_id": "s1", "user_id": 42}


def test_owned_session_cancel_is_atomic_and_requires_in_progress(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.modified_count = 1
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert cancel_owned_quiz_session("s1", 42) is True
    assert collection.update_filter == {
        "_id": "s1",
        "user_id": 42,
        "status": "in_progress",
    }
    assert collection.update_doc == {"$set": {"status": "cancelled"}}
