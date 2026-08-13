from copy import deepcopy
from datetime import datetime

import pytest
from pymongo.errors import AutoReconnect

import database
import legacy_question_timer as timer


def _get_path(doc, path):
    current = doc
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None, False
        current = current[part]
    return current, True


def _matches(doc, query):
    for key, expected in query.items():
        if key == "$or":
            if not any(_matches(doc, branch) for branch in expected):
                return False
            continue
        if key == "$and":
            if not all(_matches(doc, branch) for branch in expected):
                return False
            continue
        actual, exists = _get_path(doc, key)
        if isinstance(expected, dict) and "$exists" in expected:
            if exists != expected["$exists"]:
                return False
            continue
        if not exists or actual != expected:
            return False
    return True


def _set_path(doc, path, value):
    current = doc
    parts = path.split(".")
    for part in parts[:-1]:
        current = current.setdefault(part, {})
    current[parts[-1]] = deepcopy(value)


class FakeSessions:
    def __init__(self, doc):
        self.doc = deepcopy(doc) if doc is not None else None
        self.update_filter = None
        self.update_doc = None
        self.update_error = None
        self.find_error = None

    def find_one_and_update(self, query, update, return_document=None):
        self.update_filter = deepcopy(query)
        self.update_doc = deepcopy(update)
        if self.update_error is not None:
            raise self.update_error
        if self.doc is None or not _matches(self.doc, query):
            return None
        for key, value in update.get("$set", {}).items():
            _set_path(self.doc, key, value)
        return deepcopy(self.doc)

    def find_one(self, query):
        if self.find_error is not None:
            raise self.find_error
        if self.doc is None or not _matches(self.doc, query):
            return None
        return deepcopy(self.doc)


def _session(*, attempt_id="attempt-1", index=0, sent_at=None, legacy=False):
    doc = {
        "_id": "container-1",
        "user_id": "42",
        "status": "in_progress",
        "current_index": index,
        "question_sent_at": sent_at,
        "time_limit": 30,
    }
    if not legacy:
        doc["attempt_id"] = attempt_id
    return doc


def _install(monkeypatch, doc):
    collection = FakeSessions(doc)
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2026, 8, 10, 12, 0, 0))
    return collection


def _mark(**overrides):
    kwargs = {
        "session_id": "container-1",
        "user_id": 42,
        "expected_attempt_id": "attempt-1",
        "expected_index": 0,
        "sent_at": 100.0,
    }
    kwargs.update(overrides)
    return timer.mark_question_sent_once(**kwargs)


def test_first_question_marker_is_attempt_and_index_bound(monkeypatch):
    collection = _install(monkeypatch, _session())

    result = _mark()

    assert result["applied"] is True
    assert result["sent_at"] == 100.0
    assert result["session"]["question_sent_at"] == 100.0
    assert collection.update_filter["_id"] == "container-1"
    assert collection.update_filter["user_id"] == "42"
    assert collection.update_filter["status"] == "in_progress"
    assert collection.update_filter["current_index"] == 0
    assert collection.update_filter["$or"] == [
        {"attempt_id": "attempt-1"},
        {"attempt_id": {"$exists": False}, "_id": "attempt-1"},
    ]
    assert collection.update_doc["$set"]["question_sent_at"] == 100.0


def test_marker_retry_keeps_original_timestamp(monkeypatch):
    collection = _install(monkeypatch, _session(sent_at=100.0))

    result = _mark(sent_at=150.0)

    assert result["applied"] is False
    assert result["sent_at"] == 100.0
    assert collection.doc["question_sent_at"] == 100.0


def test_old_attempt_cannot_install_timer_after_restart(monkeypatch):
    collection = _install(monkeypatch, _session(attempt_id="attempt-new"))

    with pytest.raises(timer.LegacyQuestionTimerConflict, match="another attempt"):
        _mark(expected_attempt_id="attempt-old")

    assert collection.doc["question_sent_at"] is None


def test_stale_question_index_cannot_install_timer(monkeypatch):
    collection = _install(monkeypatch, _session(index=1))

    with pytest.raises(timer.LegacyQuestionTimerConflict, match="another question"):
        _mark(expected_index=0)

    assert collection.doc["question_sent_at"] is None


def test_legacy_session_can_back_compat_match_container_as_attempt(monkeypatch):
    collection = _install(monkeypatch, _session(legacy=True))
    collection.doc["_id"] = "container-1"

    result = _mark(expected_attempt_id="container-1")

    assert result["applied"] is True
    assert collection.doc["question_sent_at"] == 100.0


def test_mongo_write_or_read_failure_is_explicit(monkeypatch):
    collection = _install(monkeypatch, _session())
    collection.update_error = AutoReconnect("mongo down")
    with pytest.raises(timer.LegacyQuestionTimerUnavailable, match="write failed"):
        _mark()

    collection = _install(monkeypatch, _session(sent_at=100.0))
    collection.find_error = AutoReconnect("mongo down")
    with pytest.raises(timer.LegacyQuestionTimerUnavailable, match="write failed"):
        _mark(sent_at=150.0)


def test_invalid_inputs_fail_before_store(monkeypatch):
    collection = _install(monkeypatch, _session())

    for kwargs, message in (
        ({"expected_attempt_id": ""}, "expected_attempt_id"),
        ({"expected_index": -1}, "expected_index"),
        ({"sent_at": float("inf")}, "sent_at"),
        ({"sent_at": True}, "sent_at"),
    ):
        collection.update_filter = None
        with pytest.raises(ValueError, match=message):
            _mark(**kwargs)
        assert collection.update_filter is None


def test_timeout_uses_only_strict_durable_marker():
    session = _session(sent_at=100.0)
    assert timer.question_is_timed_out(session, now=129.99) is False
    assert timer.question_is_timed_out(session, now=130.0) is True

    session["question_sent_at"] = None
    assert timer.question_is_timed_out(session, now=999.0) is False

    session["time_limit"] = None
    assert timer.question_is_timed_out(session, now=999.0) is False


def test_timeout_rejects_malformed_or_future_durable_timer():
    for field, value in (
        ("time_limit", "30"),
        ("time_limit", 0),
        ("time_limit", True),
        ("question_sent_at", "100"),
        ("question_sent_at", -1),
        ("question_sent_at", True),
    ):
        session = _session(sent_at=100.0)
        session[field] = value
        with pytest.raises(timer.LegacyQuestionTimerConflict):
            timer.question_is_timed_out(session, now=130.0)

    with pytest.raises(timer.LegacyQuestionTimerConflict, match="future"):
        timer.question_is_timed_out(_session(sent_at=200.0), now=199.0)
