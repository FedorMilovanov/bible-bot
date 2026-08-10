import database
from session_integrity import cancel_owned_quiz_session, get_owned_quiz_session


class FakeQuizSessionCollection:
    def __init__(self):
        self.find_filter = None
        self.claim_filter = None
        self.claim_update = None
        self.session = None
        self.cancelled_session = None

    def find_one(self, query):
        self.find_filter = query
        return self.session

    def find_one_and_update(self, query, update, return_document=None):
        self.claim_filter = query
        self.claim_update = update
        return self.cancelled_session


def test_owned_session_lookup_scopes_by_session_and_user(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.session = {"_id": "s1", "user_id": 42, "status": "in_progress"}
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert get_owned_quiz_session("s1", 42) == collection.session
    assert collection.find_filter == {"_id": "s1", "user_id": 42}


def test_owned_session_cancel_is_atomic_and_returns_original_snapshot(monkeypatch):
    collection = FakeQuizSessionCollection()
    collection.cancelled_session = {"_id": "s1", "user_id": 42, "status": "in_progress"}
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert cancel_owned_quiz_session("s1", 42) == collection.cancelled_session
    assert collection.claim_filter == {
        "_id": "s1",
        "user_id": 42,
        "status": "in_progress",
    }
    assert collection.claim_update == {"$set": {"status": "cancelled"}}
