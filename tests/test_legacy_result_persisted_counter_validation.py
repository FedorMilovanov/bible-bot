from copy import deepcopy
from datetime import datetime

import pytest

import database
import legacy_result_store as store


class CorruptUsers:
    def __init__(self, doc):
        self.doc = deepcopy(doc)
        self.write_attempts = 0

    def find_one(self, query, projection=None):
        return deepcopy(self.doc)

    def find_one_and_update(self, query, update, return_document=None):
        self.write_attempts += 1
        pytest.fail("corrupt persisted counters must fail before Mongo mutation")


def _base_doc(**overrides):
    doc = {
        "_id": "42",
        "total_tests": 2,
        "perfect_count": 1,
        "max_streak_ever": 3,
        "daily_activity_streak": 2,
        "daily_activity_last": "2026-08-09",
        "challenge_streak_count": 1,
        "challenge_streak_last_date": "2026-08-09",
    }
    doc.update(overrides)
    return doc


def _apply(monkeypatch, doc, *, challenge=False):
    users = CorruptUsers(doc)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "_now_utc", lambda: datetime(2026, 8, 10, 12, 0, 0))
    kwargs = {
        "result_id": "counter-validation",
        "user_id": 42,
        "username": "u",
        "first_name": "User",
        "level_key": "random20" if challenge else "easy",
        "score": 18 if challenge else 1,
        "total": 20 if challenge else 2,
        "time_seconds": 10.0,
        "score_multiplier": 1.0,
        "max_streak": 1,
        "challenge_mode": "random20" if challenge else None,
        "quiz_mode": None if challenge else "relaxed",
    }
    with pytest.raises(store.LegacyResultStoreUnavailable, match="invalid persisted counter"):
        store.apply_base_result_once(**kwargs)
    assert users.write_attempts == 0


@pytest.mark.parametrize(
    "field",
    [
        "total_tests",
        "perfect_count",
        "max_streak_ever",
        "daily_activity_streak",
        "challenge_streak_count",
    ],
)
def test_string_persisted_counter_is_retryable_not_coerced(monkeypatch, field):
    _apply(monkeypatch, _base_doc(**{field: "3"}), challenge=field == "challenge_streak_count")


@pytest.mark.parametrize(
    "field",
    [
        "total_tests",
        "perfect_count",
        "max_streak_ever",
        "daily_activity_streak",
        "challenge_streak_count",
    ],
)
def test_negative_persisted_counter_is_retryable_not_reset(monkeypatch, field):
    _apply(monkeypatch, _base_doc(**{field: -1}), challenge=field == "challenge_streak_count")


def test_boolean_persisted_counter_is_not_treated_as_integer(monkeypatch):
    _apply(monkeypatch, _base_doc(total_tests=True))


def test_missing_legacy_counters_still_mean_zero(monkeypatch):
    entry = {"_id": "42"}
    for field in (
        "total_tests",
        "perfect_count",
        "max_streak_ever",
        "daily_activity_streak",
        "challenge_streak_count",
    ):
        assert store._persisted_nonnegative_int(entry, field) == 0
