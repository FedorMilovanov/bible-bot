from copy import deepcopy
from datetime import datetime

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


class FakeUsers:
    def __init__(self, doc):
        self.doc = deepcopy(doc)

    def find_one(self, query, projection=None):
        if self.doc is None or not _matches(self.doc, query):
            return None
        return deepcopy(self.doc)

    def find_one_and_update(self, query, update, return_document=None):
        if self.doc is None or not _matches(self.doc, query):
            return None
        _apply_update(self.doc, update)
        return deepcopy(self.doc)


def _user():
    return {
        "_id": "42",
        "total_tests": 0,
        "total_questions_answered": 0,
        "total_correct_answers": 0,
        "total_time_spent": 0,
        "total_points": 0,
        "easy_attempts": 0,
        "easy_correct": 0,
        "easy_total": 0,
        "easy_best_score": 0,
        "random20_attempts": 0,
        "random20_correct": 0,
        "random20_total": 0,
        "random20_best_score": 0,
        "perfect_count": 0,
        "max_streak_ever": 0,
        "daily_activity_streak": 0,
        "daily_activity_last": "",
        "achievements": {},
        "challenge_streak_count": 0,
        "challenge_streak_last_date": "",
    }


def _apply(
    *,
    completed_at,
    challenge_mode=None,
    score=8,
    total=10,
    time_seconds=45.0,
    fastest_answer=None,
):
    level_key = challenge_mode or "easy"
    return store.apply_base_result_once(
        result_id="recovered-session",
        user_id=42,
        username="u",
        first_name="User",
        level_key=level_key,
        score=score,
        total=total,
        time_seconds=time_seconds,
        score_multiplier=1.0,
        max_streak=4,
        challenge_mode=challenge_mode,
        fastest_answer=fastest_answer,
        completed_at=completed_at,
    )


def test_recovered_result_keeps_original_day_while_last_activity_uses_write_time(monkeypatch):
    users = FakeUsers(_user())
    write_now = datetime(2026, 8, 11, 0, 5, 0)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "_now_utc", lambda: write_now)

    result = _apply(completed_at="2026-08-10T23:59:59")

    assert result["completed_at"] == "2026-08-10T23:59:59"
    assert result["receipt"]["completed_at"] == "2026-08-10T23:59:59"
    assert users.doc["daily_activity_last"] == "2026-08-10"
    assert users.doc["last_activity"] == write_now
    assert "20260810" in users.doc["normal_bonus_result_owners"]
    assert "20260811" not in users.doc["normal_bonus_result_owners"]


def test_recovered_challenge_keeps_original_iso_week(monkeypatch):
    users = FakeUsers(_user())
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2021, 1, 4, 0, 5, 0))

    result = _apply(
        completed_at="2021-01-03T23:59:59",
        challenge_mode="random20",
        score=18,
        total=20,
    )

    assert store.result_week_id(result["completed_at"]) == "2020-W53"
    assert users.doc["daily_activity_last"] == "2021-01-03"
    assert users.doc["challenge_streak_last_date"] == "2021-01-03"
    assert "20210103" in users.doc["challenge_bonus_result_owners"]["random20"]


def test_late_recovery_cannot_rewind_newer_daily_activity(monkeypatch):
    doc = _user()
    doc["daily_activity_last"] = "2026-08-11"
    doc["daily_activity_streak"] = 5
    users = FakeUsers(doc)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2026, 8, 12, 10, 0, 0))

    with pytest.raises(
        store.LegacyResultStoreUnavailable,
        match="predates newer daily activity",
    ):
        _apply(completed_at="2026-08-10T23:59:59")

    assert users.doc["total_tests"] == 0
    assert users.doc["daily_activity_last"] == "2026-08-11"
    assert users.doc["daily_activity_streak"] == 5


def test_late_challenge_recovery_cannot_rewind_newer_challenge_streak(monkeypatch):
    doc = _user()
    doc["daily_activity_last"] = "2026-08-10"
    doc["daily_activity_streak"] = 5
    doc["challenge_streak_last_date"] = "2026-08-11"
    doc["challenge_streak_count"] = 3
    users = FakeUsers(doc)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2026, 8, 12, 10, 0, 0))

    with pytest.raises(
        store.LegacyResultStoreUnavailable,
        match="predates newer Challenge streak",
    ):
        _apply(
            completed_at="2026-08-10T23:59:59",
            challenge_mode="random20",
            score=18,
            total=20,
        )

    assert users.doc["total_tests"] == 0
    assert users.doc["challenge_streak_last_date"] == "2026-08-11"
    assert users.doc["challenge_streak_count"] == 3


def test_invalid_authoritative_completion_time_is_retryable_before_write(monkeypatch):
    users = FakeUsers(_user())
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2026, 8, 11, 0, 5, 0))

    with pytest.raises(
        store.LegacyResultStoreUnavailable,
        match="completion timestamp is invalid",
    ):
        _apply(completed_at="not-a-timestamp")

    assert users.doc["total_tests"] == 0


def test_future_authoritative_completion_time_is_retryable_before_write(monkeypatch):
    users = FakeUsers(_user())
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2026, 8, 10, 12, 0, 0))

    with pytest.raises(
        store.LegacyResultStoreUnavailable,
        match="timestamp is in the future",
    ):
        _apply(completed_at="2026-08-10T12:00:01")

    assert users.doc["total_tests"] == 0


@pytest.mark.parametrize(
    ("field", "kwargs"),
    [
        ("time_seconds", {"time_seconds": float("inf")}),
        ("fastest_answer", {"fastest_answer": float("nan")}),
    ],
)
def test_nonfinite_result_numbers_are_rejected_before_write(monkeypatch, field, kwargs):
    users = FakeUsers(_user())
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2026, 8, 10, 12, 0, 0))

    with pytest.raises(ValueError, match=field):
        _apply(completed_at="2026-08-10T11:59:59", **kwargs)

    assert users.doc["total_tests"] == 0


def test_corrupt_existing_receipt_streak_is_retryable(monkeypatch):
    users = FakeUsers(_user())
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2026, 8, 10, 12, 0, 0))

    _apply(completed_at="2026-08-10T11:59:59")
    receipt_key = next(iter(users.doc["legacy_result_receipts"]))
    users.doc["legacy_result_receipts"][receipt_key]["daily_streak"] = "broken"

    with pytest.raises(
        store.LegacyResultStoreUnavailable,
        match="receipt daily_streak is invalid",
    ):
        _apply(completed_at="2026-08-10T11:59:59")


def test_invalid_result_day_never_falls_back_to_current_time():
    with pytest.raises(ValueError, match="completion timestamp is invalid"):
        store.result_day("not-a-timestamp")
