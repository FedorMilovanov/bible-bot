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
    current[parts[-1]] = value


def _matches(doc, query):
    for key, expected in query.items():
        if key == "$or":
            if not any(_matches(doc, branch) for branch in expected):
                return False
            continue
        actual, exists = _get_path(doc, key)
        if isinstance(expected, dict):
            if "$ne" in expected:
                forbidden = expected["$ne"]
                if (isinstance(actual, list) and forbidden in actual) or actual == forbidden:
                    return False
            if "$exists" in expected and exists != expected["$exists"]:
                return False
            if "$lt" in expected and not (exists and actual < expected["$lt"]):
                return False
            if "$gt" in expected and not (exists and actual > expected["$gt"]):
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


class FakeUsers:
    def __init__(self, doc):
        self.doc = deepcopy(doc)
        self.find_one_and_update_calls = 0
        self.update_calls = 0

    def find_one(self, query, projection=None):
        if self.doc is None or not _matches(self.doc, query):
            return None
        if not projection:
            return deepcopy(self.doc)
        projected = {"_id": self.doc["_id"]}
        for key, include in projection.items():
            if not include:
                continue
            value, exists = _get_path(self.doc, key)
            if exists:
                _set_path(projected, key, deepcopy(value))
        return projected

    def find_one_and_update(self, query, update, return_document=None):
        self.find_one_and_update_calls += 1
        if self.doc is None or not _matches(self.doc, query):
            return None
        _apply_update(self.doc, update)
        return deepcopy(self.doc)

    def update_one(self, query, update):
        self.update_calls += 1
        if self.doc is None or not _matches(self.doc, query):
            return SimpleNamespace(modified_count=0)
        _apply_update(self.doc, update)
        return SimpleNamespace(modified_count=1)


class FakeWeekly:
    def __init__(self):
        self.docs = {}

    def find_one(self, query):
        doc = self.docs.get(query["_id"])
        return deepcopy(doc) if doc else None

    def insert_one(self, doc):
        self.docs[doc["_id"]] = deepcopy(doc)

    def update_one(self, query, update):
        doc = self.docs.get(query["_id"])
        if not doc or not _matches(doc, query):
            return SimpleNamespace(modified_count=0)
        _apply_update(doc, update)
        return SimpleNamespace(modified_count=1)


def base_user():
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
        "last_daily_bonus": "",
        "achievements": {},
        "challenge_streak_count": 0,
        "challenge_streak_last_date": "",
    }


def apply_easy_result(result_id):
    return store.apply_base_result_once(
        result_id=result_id,
        user_id=42,
        username="u",
        first_name="User",
        level_key="easy",
        score=8,
        total=10,
        time_seconds=12.5,
        max_streak=4,
    )


def test_base_result_requires_idempotency_key(monkeypatch):
    users = FakeUsers(base_user())
    monkeypatch.setattr(database, "collection", users)

    with pytest.raises(ValueError, match="result_id is required"):
        apply_easy_result("")

    assert users.doc["total_tests"] == 0


def test_base_result_receipt_prevents_duplicate_counters(monkeypatch):
    users = FakeUsers(base_user())
    monkeypatch.setattr(database, "collection", users)

    first = apply_easy_result("session-1")
    second = apply_easy_result("session-1")

    assert first["applied"] is True
    assert second["applied"] is False
    assert first["completed_at"] == second["completed_at"]
    assert users.doc["total_tests"] == 1
    assert users.doc["total_points"] == 8
    assert users.doc["easy_attempts"] == 1
    assert len(users.doc["legacy_result_receipts"]) == 1
    assert users.doc["daily_activity_streak"] == 1
    assert users.doc["max_streak_ever"] == 4


def test_base_result_receipts_do_not_expire_after_128_newer_results(monkeypatch):
    users = FakeUsers(base_user())
    monkeypatch.setattr(database, "collection", users)

    for index in range(140):
        assert apply_easy_result(f"session-{index}")["applied"] is True

    replay = apply_easy_result("session-0")

    assert replay["applied"] is False
    assert users.doc["total_tests"] == 140
    assert users.doc["easy_attempts"] == 140
    assert len(users.doc["legacy_result_receipts"]) == 140


def test_duplicate_result_keeps_original_completion_day_across_midnight(monkeypatch):
    users = FakeUsers(base_user())
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2026, 8, 10, 23, 59, 59))

    first = apply_easy_result("midnight-session")

    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2026, 8, 11, 0, 0, 1))
    retry = apply_easy_result("midnight-session")

    assert retry["applied"] is False
    assert retry["completed_at"] == first["completed_at"]
    assert store.result_day(retry["completed_at"]) == "2026-08-10"


def test_daily_bonus_receipt_survives_later_day_claim(monkeypatch):
    doc = base_user()
    doc["daily_activity_streak"] = 3
    users = FakeUsers(doc)
    monkeypatch.setattr(database, "collection", users)

    assert store.claim_daily_bonus_once(42, "2026-08-10") == 10
    assert store.claim_daily_bonus_once(42, "2026-08-11") == 10
    assert store.claim_daily_bonus_once(42, "2026-08-10") == 0
    assert users.doc["total_points"] == 20
    assert users.doc["last_daily_bonus"] == "2026-08-11"
    assert len(users.doc["daily_bonus_receipts"]) == 2


def test_challenge_bonus_receipt_survives_later_day_claim(monkeypatch):
    users = FakeUsers(base_user())
    monkeypatch.setattr(database, "collection", users)

    assert store.claim_challenge_bonus_once(42, "random20", 18, "2026-08-10") == 60
    assert store.claim_challenge_bonus_once(42, "random20", 18, "2026-08-11") == 60
    assert store.claim_challenge_bonus_once(42, "random20", 18, "2026-08-10") == 0
    assert users.doc["total_points"] == 120
    assert users.doc["random20_last_bonus_date"] == "2026-08-11"
    assert len(users.doc["challenge_bonus_receipts"]["random20"]) == 2


def test_achievement_reward_is_claimed_once(monkeypatch):
    users = FakeUsers(base_user())
    monkeypatch.setattr(database, "collection", users)

    assert store.claim_achievement_once(42, "perfectionist_1", reward=25, awarded_at="10.08.2026") is True
    assert store.claim_achievement_once(42, "perfectionist_1", reward=25, awarded_at="10.08.2026") is False
    assert users.doc["achievements"]["perfectionist_1"] == "10.08.2026"
    assert users.doc["total_points"] == 25


def test_result_week_id_uses_durable_completion_time():
    assert store.result_week_id("2021-01-01T00:00:00") == "2020-W53"


def test_weekly_best_is_idempotent_and_only_improves(monkeypatch):
    weekly = FakeWeekly()
    monkeypatch.setattr(database, "weekly_lb_collection", weekly)

    params = {
        "user_id": 42,
        "username": "u",
        "first_name": "User",
        "mode": "random20",
        "week_id": "2026-W33",
    }
    store.sync_weekly_best(score=18, time_seconds=50, **params)
    store.sync_weekly_best(score=17, time_seconds=10, **params)
    store.sync_weekly_best(score=18, time_seconds=40, **params)

    doc = weekly.docs["2026-W33_random20_42"]
    assert doc["best_score"] == 18
    assert doc["best_time"] == 40
