from __future__ import annotations

from copy import deepcopy
from threading import Barrier, Lock, Thread

from pymongo.errors import AutoReconnect

import database
import session_boundaries


class UpdateResult:
    def __init__(self, modified_count: int):
        self.modified_count = modified_count


class FakeSessionCollection:
    def __init__(self, doc: dict | None, *, fail: bool = False):
        self.doc = deepcopy(doc)
        self.fail = fail
        self.lock = Lock()
        self.last_predicate = None

    def _raise_if_needed(self):
        if self.fail:
            raise AutoReconnect("temporary Mongo outage")

    @staticmethod
    def _matches(doc, predicate):
        return doc is not None and all(doc.get(key) == value for key, value in predicate.items())

    def find_one(self, predicate):
        self._raise_if_needed()
        self.last_predicate = deepcopy(predicate)
        with self.lock:
            return deepcopy(self.doc) if self._matches(self.doc, predicate) else None

    def find_one_and_update(self, predicate, update, return_document=None):
        self._raise_if_needed()
        self.last_predicate = deepcopy(predicate)
        with self.lock:
            if not self._matches(self.doc, predicate):
                return None
            before = deepcopy(self.doc)
            self.doc.update(update["$set"])
            return before

    def update_one(self, predicate, update):
        self._raise_if_needed()
        self.last_predicate = deepcopy(predicate)
        with self.lock:
            if not self._matches(self.doc, predicate):
                return UpdateResult(0)
            self.doc.update(update["$set"])
            return UpdateResult(1)


def _session(*, owner="10", status="in_progress"):
    return {
        "_id": "session-abc",
        "user_id": owner,
        "status": status,
        "mode": "level",
        "questions_data": [],
    }


def test_owned_session_lookup_requires_user_id_and_status(monkeypatch):
    collection = FakeSessionCollection(_session(owner="10"))
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    owned = session_boundaries.get_owned_quiz_session("session-abc", 10)
    assert owned is not None
    assert collection.last_predicate == {
        "_id": "session-abc",
        "user_id": "10",
        "status": "in_progress",
    }

    assert session_boundaries.get_owned_quiz_session("session-abc", 11) is None


def test_owned_session_lookup_can_read_terminal_session_only_when_explicit(monkeypatch):
    collection = FakeSessionCollection(_session(owner="10", status="finished"))
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert session_boundaries.get_owned_quiz_session("session-abc", 10) is None
    assert session_boundaries.get_owned_quiz_session(
        "session-abc", 10, require_in_progress=False
    ) is not None


def test_restart_claim_is_exactly_once_under_concurrent_callbacks(monkeypatch):
    collection = FakeSessionCollection(_session(owner="10"))
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    barrier = Barrier(3)
    results = []

    def claim():
        barrier.wait()
        results.append(
            session_boundaries.claim_owned_quiz_session_restart("session-abc", 10)
        )

    first = Thread(target=claim)
    second = Thread(target=claim)
    first.start()
    second.start()
    barrier.wait()
    first.join(timeout=2)
    second.join(timeout=2)

    assert sum(result is not None for result in results) == 1
    assert collection.doc["status"] == "cancelled"
    assert collection.doc["restart_claimed_by"] == "10"


def test_restart_claim_rejects_other_user_without_mutation(monkeypatch):
    collection = FakeSessionCollection(_session(owner="10"))
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert session_boundaries.claim_owned_quiz_session_restart("session-abc", 99) is None
    assert collection.doc["status"] == "in_progress"


def test_cancel_owned_session_requires_owner_and_in_progress(monkeypatch):
    collection = FakeSessionCollection(_session(owner="10"))
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert session_boundaries.cancel_owned_quiz_session("session-abc", 11) is False
    assert collection.doc["status"] == "in_progress"

    assert session_boundaries.cancel_owned_quiz_session("session-abc", 10) is True
    assert collection.doc["status"] == "cancelled"
    assert session_boundaries.cancel_owned_quiz_session("session-abc", 10) is False


def test_mongo_failures_fail_closed(monkeypatch):
    collection = FakeSessionCollection(_session(), fail=True)
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    assert session_boundaries.get_owned_quiz_session("session-abc", 10) is None
    assert session_boundaries.claim_owned_quiz_session_restart("session-abc", 10) is None
    assert session_boundaries.cancel_owned_quiz_session("session-abc", 10) is False
