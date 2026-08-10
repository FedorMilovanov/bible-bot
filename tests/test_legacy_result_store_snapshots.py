from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace

import pytest

import database
import legacy_result_store as store


def _get_path(doc, path):
    current = doc
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None, False
        current = current[part]
    return current, True


def _set_path(doc, path, value):
    current = doc
    parts = path.split(".")
    for part in parts[:-1]:
        current = current.setdefault(part, {})
    current[parts[-1]] = deepcopy(value)


def _matches(doc, query):
    for key, expected in query.items():
        actual, exists = _get_path(doc, key)
        if isinstance(expected, dict):
            if "$exists" in expected and exists != expected["$exists"]:
                return False
            continue
        if not exists or actual != expected:
            return False
    return True


def _apply_update(doc, update):
    for key, value in update.get("$inc", {}).items():
        current, exists = _get_path(doc, key)
        _set_path(doc, key, (current if exists else 0) + value)
    for key, value in update.get("$set", {}).items():
        _set_path(doc, key, value)
    for key, value in update.get("$max", {}).items():
        current, exists = _get_path(doc, key)
        if not exists or value > current:
            _set_path(doc, key, value)


def _base_user():
    return {
        "_id": "42",
        "username": "old",
        "first_name": "Old",
        "total_tests": 0,
        "total_questions_answered": 0,
        "total_correct_answers": 0,
        "total_time_spent": 0,
        "total_points": 0,
        "easy_attempts": 0,
        "easy_correct": 0,
        "easy_total": 0,
        "easy_best_score": 0,
        "perfect_count": 0,
        "max_streak_ever": 0,
        "daily_activity_streak": 0,
        "daily_activity_last": "",
        "challenge_streak_count": 0,
        "challenge_streak_last_date": "",
        "achievements": {},
    }


class SnapshotUsers:
    def __init__(self, *, inject_one_race=False):
        self.doc = _base_user()
        self.inject_one_race = inject_one_race
        self.find_one_and_update_calls = 0
        self.update_one_calls = 0

    def find_one(self, query, projection=None):
        if not _matches(self.doc, query):
            return None
        return deepcopy(self.doc)

    def find_one_and_update(self, query, update, return_document=None):
        self.find_one_and_update_calls += 1
        if self.inject_one_race:
            self.inject_one_race = False
            # Simulate a distinct result committing after our pre-read. The
            # optimistic total_tests/date predicates must force a reload.
            self.doc["total_tests"] += 1
            self.doc["daily_activity_streak"] = 1
            self.doc["daily_activity_last"] = "2026-08-10"
        if not _matches(self.doc, query):
            return None
        _apply_update(self.doc, update)
        return deepcopy(self.doc)

    def update_one(self, query, update):
        self.update_one_calls += 1
        if not _matches(self.doc, query):
            return SimpleNamespace(modified_count=0)
        _apply_update(self.doc, update)
        return SimpleNamespace(modified_count=1)


def _apply(result_id, **overrides):
    params = {
        "result_id": result_id,
        "user_id": 42,
        "username": "u",
        "first_name": "User",
        "level_key": "easy",
        "score": 8,
        "total": 10,
        "time_seconds": 12.5,
        "score_multiplier": 1.5,
        "max_streak": 4,
        "quiz_mode": "timed",
        "fastest_answer": 2.8,
    }
    params.update(overrides)
    return store.apply_base_result_once(**params)


def test_replay_returns_original_result_inputs_not_retry_arguments(monkeypatch):
    users = SnapshotUsers()
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2026, 8, 10, 12, 0, 0))

    first = _apply("result-1")
    replay = _apply(
        "result-1",
        score=1,
        time_seconds=999,
        score_multiplier=0.25,
        max_streak=0,
        quiz_mode="relaxed",
        fastest_answer=99,
    )

    assert first["applied"] is True
    assert replay["applied"] is False
    assert replay["result"] == first["result"]
    assert replay["result"]["score"] == 8
    assert replay["result"]["time_seconds"] == 12.5
    assert replay["result"]["score_multiplier"] == 1.5
    assert replay["result"]["quiz_mode"] == "timed"
    assert replay["result"]["fastest_answer"] == 2.8
    assert replay["earned_base"] == first["earned_base"]


def test_receipt_achievement_state_does_not_follow_future_user_progress(monkeypatch):
    users = SnapshotUsers()
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2026, 8, 10, 12, 0, 0))

    first = _apply("result-1")
    original_state = deepcopy(first["receipt"]["achievement_state"])

    users.doc.update({
        "total_tests": 100,
        "perfect_count": 15,
        "max_streak_ever": 20,
        "daily_activity_streak": 30,
    })
    replay = _apply("result-1")

    assert replay["receipt"]["achievement_state"] == original_state
    assert replay["receipt"]["achievement_state"]["total_tests"] == 1
    assert replay["user"]["total_tests"] == 100


def test_base_result_reloads_after_concurrent_distinct_result(monkeypatch):
    users = SnapshotUsers(inject_one_race=True)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2026, 8, 10, 12, 0, 0))

    result = _apply("result-after-race")

    assert result["applied"] is True
    assert users.find_one_and_update_calls == 2
    assert users.doc["total_tests"] == 2
    assert result["receipt"]["achievement_state"]["total_tests"] == 2
    assert result["receipt"]["daily_streak"] == 1


def test_non_string_achievement_key_is_rejected_before_write(monkeypatch):
    users = SnapshotUsers()
    monkeypatch.setattr(database, "collection", users)

    with pytest.raises(ValueError, match="unsafe achievement key"):
        store.claim_achievement_once(
            42,
            ("streak_3", "legacy message"),
            reward=0,
            awarded_at="10.08.2026",
        )

    assert users.update_one_calls == 0
