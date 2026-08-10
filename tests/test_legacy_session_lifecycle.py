from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace

import pytest
from pymongo.errors import AutoReconnect

import database
import legacy_session_lifecycle as lifecycle


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


def _unset_path(doc, path):
    current = doc
    parts = path.split(".")
    for part in parts[:-1]:
        current = current.get(part)
        if not isinstance(current, dict):
            return
    if isinstance(current, dict):
        current.pop(parts[-1], None)


class FakeSessions:
    def __init__(self, doc):
        self.doc = deepcopy(doc)
        self.find_error = None
        self.update_error = None
        self.force_update_miss = False
        self.update_calls = 0

    def find_one(self, query):
        if self.find_error is not None:
            raise self.find_error
        if self.doc is None or not _matches(self.doc, query):
            return None
        return deepcopy(self.doc)

    def find_one_and_update(self, query, update, return_document=None):
        self.update_calls += 1
        if self.update_error is not None:
            raise self.update_error
        if self.force_update_miss or self.doc is None or not _matches(self.doc, query):
            return None
        for key, value in update.get("$set", {}).items():
            _set_path(self.doc, key, value)
        for key, value in update.get("$inc", {}).items():
            current, exists = _get_path(self.doc, key)
            _set_path(self.doc, key, (current if exists else 0) + value)
        for key in update.get("$unset", {}):
            _unset_path(self.doc, key)
        return deepcopy(self.doc)


def _question(qid):
    return {
        "id": qid,
        "question": f"Question {qid}",
        "options": ["A", "B"],
        "correct": 0,
    }


def partial_session(*, attempt_id="attempt-1", legacy=False):
    q1 = _question("q1")
    q2 = _question("q2")
    doc = {
        "_id": "container-1",
        "session_id": "container-1",
        "user_id": "42",
        "status": "in_progress",
        "mode": "level",
        "level_key": "easy",
        "level_name": "Easy",
        "question_ids": ["q1", "q2"],
        "questions_data": [q1, q2],
        "current_index": 1,
        "correct_count": 1,
        "answered_questions": [
            {
                "index": 0,
                "qid": "q1",
                "user_answer": "A",
                "is_correct": True,
                "question_obj": q1,
                "latency_seconds": 2.0,
                "ts": "2026-08-10T12:00:02",
            }
        ],
        "time_limit": None,
        "chat_id": 100,
        "start_time": datetime(2026, 8, 10, 12, 0, 0).timestamp(),
        "restart_count": 0,
    }
    if not legacy:
        doc["attempt_id"] = attempt_id
    return doc


def completed_session():
    q = _question("q1")
    return {
        "_id": "container-1",
        "session_id": "container-1",
        "attempt_id": "attempt-1",
        "user_id": "42",
        "status": "in_progress",
        "mode": "level",
        "level_key": "easy",
        "level_name": "Easy",
        "question_ids": ["q1"],
        "questions_data": [q],
        "current_index": 1,
        "correct_count": 1,
        "answered_questions": [
            {
                "index": 0,
                "qid": "q1",
                "user_answer": "A",
                "is_correct": True,
                "question_obj": q,
                "latency_seconds": 2.0,
                "ts": "2026-08-10T12:00:02",
            }
        ],
        "time_limit": None,
        "chat_id": 100,
        "start_time": datetime(2026, 8, 10, 12, 0, 0).timestamp(),
    }


def _install(monkeypatch, doc):
    collection = FakeSessions(doc)
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2026, 8, 10, 12, 5, 0))
    monkeypatch.setattr(lifecycle.time, "time", lambda: 12345.0)
    return collection


def _restart(**overrides):
    kwargs = {
        "session_id": "container-1",
        "user_id": 42,
        "expected_attempt_id": "attempt-1",
        "mode": "level",
        "question_ids": ["n1", "n2"],
        "questions_data": [_question("n1"), _question("n2")],
        "level_key": "medium",
        "level_name": "Medium",
        "time_limit": None,
        "chat_id": 200,
    }
    kwargs.update(overrides)
    return lifecycle.restart_owned_quiz_attempt(**kwargs)


def test_restart_is_one_atomic_update_of_same_container(monkeypatch):
    collection = _install(monkeypatch, partial_session())

    result = _restart()

    assert result["applied"] is True
    assert result["session"]["_id"] == "container-1"
    assert result["attempt_id"] != "attempt-1"
    assert result["session"]["attempt_id"] == result["attempt_id"]
    assert result["session"]["previous_attempt_id"] == "attempt-1"
    assert result["session"]["current_index"] == 0
    assert result["session"]["correct_count"] == 0
    assert result["session"]["answered_questions"] == []
    assert result["session"]["question_ids"] == ["n1", "n2"]
    assert result["session"]["restart_count"] == 1
    assert result["session"]["start_time"] == 12345.0
    assert collection.update_calls == 1


def test_lost_restart_response_replays_without_second_reset(monkeypatch):
    collection = _install(monkeypatch, partial_session())
    first = _restart()
    first_attempt = first["attempt_id"]

    replay = _restart(
        question_ids=["different"],
        questions_data=[_question("different")],
        level_key="hard",
        level_name="Different",
    )

    assert replay["applied"] is False
    assert replay["attempt_id"] == first_attempt
    assert replay["session"]["question_ids"] == ["n1", "n2"]
    assert replay["session"]["restart_count"] == 1
    assert collection.update_calls == 1


def test_legacy_container_id_can_be_first_attempt_identity(monkeypatch):
    legacy = partial_session(legacy=True)
    legacy["_id"] = "container-1"
    collection = _install(monkeypatch, legacy)

    result = _restart(expected_attempt_id="container-1")

    assert result["applied"] is True
    assert result["session"]["previous_attempt_id"] == "container-1"
    assert result["session"]["attempt_id"] != "container-1"
    assert collection.update_calls == 1


def test_stale_restart_cannot_reset_new_attempt(monkeypatch):
    collection = _install(monkeypatch, partial_session(attempt_id="attempt-2"))

    with pytest.raises(lifecycle.QuizSessionLifecycleConflict, match="another quiz attempt"):
        _restart(expected_attempt_id="attempt-1")

    assert collection.update_calls == 0
    assert collection.doc["attempt_id"] == "attempt-2"


def test_completed_evidence_cannot_be_restarted_or_cancelled(monkeypatch):
    collection = _install(monkeypatch, completed_session())

    with pytest.raises(lifecycle.QuizSessionLifecycleConflict, match="must be finalized"):
        _restart()
    with pytest.raises(lifecycle.QuizSessionLifecycleConflict, match="must be finalized"):
        lifecycle.cancel_owned_incomplete_quiz_attempt(
            "container-1",
            42,
            expected_attempt_id="attempt-1",
        )

    assert collection.update_calls == 0
    assert collection.doc["status"] == "in_progress"


def test_cancel_is_attempt_bound_and_idempotent(monkeypatch):
    collection = _install(monkeypatch, partial_session())

    first = lifecycle.cancel_owned_incomplete_quiz_attempt(
        "container-1",
        42,
        expected_attempt_id="attempt-1",
    )
    replay = lifecycle.cancel_owned_incomplete_quiz_attempt(
        "container-1",
        42,
        expected_attempt_id="attempt-1",
    )

    assert first["applied"] is True
    assert replay["applied"] is False
    assert collection.doc["status"] == "cancelled"
    assert collection.update_calls == 1


def test_cancel_from_old_attempt_cannot_cancel_restarted_attempt(monkeypatch):
    collection = _install(monkeypatch, partial_session(attempt_id="attempt-2"))

    with pytest.raises(lifecycle.QuizSessionLifecycleConflict, match="another quiz attempt"):
        lifecycle.cancel_owned_incomplete_quiz_attempt(
            "container-1",
            42,
            expected_attempt_id="attempt-1",
        )

    assert collection.doc["status"] == "in_progress"
    assert collection.update_calls == 0


def test_concurrent_state_change_makes_restart_retryable_not_destructive(monkeypatch):
    collection = _install(monkeypatch, partial_session())
    collection.force_update_miss = True

    with pytest.raises(lifecycle.QuizSessionLifecycleConflict, match="changed during restart"):
        _restart()

    assert collection.doc["attempt_id"] == "attempt-1"
    assert collection.doc["current_index"] == 1


def test_storage_failure_is_explicit(monkeypatch):
    collection = _install(monkeypatch, partial_session())
    collection.find_error = AutoReconnect("mongo unavailable")

    with pytest.raises(lifecycle.QuizSessionLifecycleUnavailable, match="restart failed"):
        _restart()


def test_invalid_restart_spec_is_rejected_before_store(monkeypatch):
    collection = _install(monkeypatch, partial_session())

    with pytest.raises(ValueError, match="time_limit"):
        _restart(time_limit=0)

    assert collection.update_calls == 0
