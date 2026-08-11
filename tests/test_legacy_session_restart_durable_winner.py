from copy import deepcopy

import pytest

import database
import legacy_session_lifecycle as lifecycle


class LostResponseReplacement:
    def __init__(self):
        self.doc = {
            "_id": "container-1",
            "user_id": "42",
            "status": "in_progress",
            "attempt_id": "attempt-new",
            "previous_attempt_id": "attempt-old",
            "mode": "random20",
            "level_key": "random20",
            "level_name": "Random 20",
            "question_ids": ["winner-q1", "winner-q2"],
            "questions_data": [{"id": "winner-q1"}, {"id": "winner-q2"}],
            "current_index": 0,
            "correct_count": 0,
            "answered_questions": [],
            "question_sent_at": None,
            "time_limit": 20,
            "start_time": 100.0,
        }
        self.update_called = False

    def find_one(self, query):
        if query.get("_id") != self.doc["_id"] or query.get("user_id") != "42":
            return None
        return deepcopy(self.doc)

    def find_one_and_update(self, *_args, **_kwargs):
        self.update_called = True
        raise AssertionError("lost-response replay must not reset the durable winner")


def _restart(collection, monkeypatch):
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    return lifecycle.restart_owned_quiz_attempt(
        "container-1",
        42,
        expected_attempt_id="attempt-old",
        mode="random20",
        question_ids=["retry-q1", "retry-q2"],
        questions_data=[{"id": "retry-q1"}, {"id": "retry-q2"}],
        level_key="random20",
        level_name="Random 20",
        time_limit=20,
    )


def test_restart_replay_returns_durable_winner_when_retry_spec_differs(monkeypatch):
    collection = LostResponseReplacement()

    result = _restart(collection, monkeypatch)

    assert result["applied"] is False
    assert result["attempt_id"] == "attempt-new"
    assert result["previous_attempt_id"] == "attempt-old"
    assert result["session"]["question_ids"] == ["winner-q1", "winner-q2"]
    assert result["session"]["questions_data"] == [
        {"id": "winner-q1"},
        {"id": "winner-q2"},
    ]
    assert collection.update_called is False


def test_restart_replay_rejects_old_attempt_as_replacement(monkeypatch):
    collection = LostResponseReplacement()
    collection.doc["attempt_id"] = "attempt-old"

    with pytest.raises(lifecycle.QuizSessionLifecycleConflict, match="did not advance"):
        _restart(collection, monkeypatch)

    assert collection.update_called is False


def test_restart_replay_rejects_contradictory_durable_winner(monkeypatch):
    collection = LostResponseReplacement()
    collection.doc["mode"] = "unknown"

    with pytest.raises(lifecycle.QuizSessionLifecycleConflict, match="replacement state"):
        _restart(collection, monkeypatch)

    assert collection.update_called is False
