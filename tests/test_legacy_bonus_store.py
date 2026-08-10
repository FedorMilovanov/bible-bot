from copy import deepcopy
from types import SimpleNamespace

import pytest

import database
import legacy_bonus_store as bonus_store
from legacy_result_store import LegacyResultStoreUnavailable


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
        _set_path(doc, key, deepcopy(value))
    for key, value in update.get("$max", {}).items():
        current, exists = _get_path(doc, key)
        if not exists or value > current:
            _set_path(doc, key, value)


class FakeUsers:
    def __init__(self):
        self.doc = {
            "_id": "42",
            "total_points": 0,
            "last_daily_bonus": "",
            "random20_last_bonus_date": "",
        }

    def find_one(self, query, projection=None):
        if not _matches(self.doc, query):
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

    def update_one(self, query, update):
        if not _matches(self.doc, query):
            return SimpleNamespace(modified_count=0)
        _apply_update(self.doc, update)
        return SimpleNamespace(modified_count=1)


def test_daily_bonus_retry_recovers_only_for_winning_result(monkeypatch):
    users = FakeUsers()
    monkeypatch.setattr(database, "collection", users)

    first = bonus_store.claim_daily_bonus_for_result(
        user_id=42,
        result_id="result-a",
        day="2026-08-10",
        daily_streak=3,
    )
    replay = bonus_store.claim_daily_bonus_for_result(
        user_id=42,
        result_id="result-a",
        day="2026-08-10",
        daily_streak=99,
    )
    other = bonus_store.claim_daily_bonus_for_result(
        user_id=42,
        result_id="result-b",
        day="2026-08-10",
        daily_streak=99,
    )

    assert first == {"bonus": 10, "eligible": True, "claimed_now": True}
    assert replay == {"bonus": 10, "eligible": True, "claimed_now": False}
    assert other == {"bonus": 0, "eligible": False, "claimed_now": False}
    assert users.doc["total_points"] == 10


def test_challenge_second_attempt_cannot_inherit_first_bonus(monkeypatch):
    users = FakeUsers()
    monkeypatch.setattr(database, "collection", users)

    first = bonus_store.claim_challenge_bonus_for_result(
        user_id=42,
        result_id="challenge-a",
        mode="random20",
        score=18,
        day="2026-08-10",
    )
    second = bonus_store.claim_challenge_bonus_for_result(
        user_id=42,
        result_id="challenge-b",
        mode="random20",
        score=20,
        day="2026-08-10",
    )
    replay = bonus_store.claim_challenge_bonus_for_result(
        user_id=42,
        result_id="challenge-a",
        mode="random20",
        score=20,
        day="2026-08-10",
    )

    assert first == {"bonus": 60, "eligible": True, "claimed_now": True}
    assert second == {"bonus": 0, "eligible": False, "claimed_now": False}
    assert replay == {"bonus": 60, "eligible": True, "claimed_now": False}
    assert users.doc["total_points"] == 60


def test_zero_challenge_bonus_is_still_owned_by_first_attempt(monkeypatch):
    users = FakeUsers()
    monkeypatch.setattr(database, "collection", users)

    first = bonus_store.claim_challenge_bonus_for_result(
        user_id=42,
        result_id="challenge-a",
        mode="random20",
        score=10,
        day="2026-08-10",
    )
    replay = bonus_store.claim_challenge_bonus_for_result(
        user_id=42,
        result_id="challenge-a",
        mode="random20",
        score=10,
        day="2026-08-10",
    )
    other = bonus_store.claim_challenge_bonus_for_result(
        user_id=42,
        result_id="challenge-b",
        mode="random20",
        score=20,
        day="2026-08-10",
    )

    assert first == {"bonus": 0, "eligible": True, "claimed_now": True}
    assert replay == {"bonus": 0, "eligible": True, "claimed_now": False}
    assert other == {"bonus": 0, "eligible": False, "claimed_now": False}
    assert users.doc["total_points"] == 0


def test_legacy_date_backfill_never_assigns_old_credit_to_new_result(monkeypatch):
    users = FakeUsers()
    users.doc["last_daily_bonus"] = "2026-08-10"
    monkeypatch.setattr(database, "collection", users)

    stage = bonus_store.claim_daily_bonus_for_result(
        user_id=42,
        result_id="new-result",
        day="2026-08-10",
        daily_streak=7,
    )

    assert stage == {"bonus": 0, "eligible": False, "claimed_now": False}
    assert users.doc["total_points"] == 0
    assert users.doc["daily_bonus_receipts"]["20260810"]["legacy"] is True


def test_owned_bonus_receipt_with_invalid_amount_is_retryable(monkeypatch):
    users = FakeUsers()
    users.doc["daily_bonus_receipts"] = {
        "20260810": {
            "bonus": "not-a-number",
            "eligible": True,
            "result_owner": bonus_store._owner("result-a"),
        }
    }
    monkeypatch.setattr(database, "collection", users)

    with pytest.raises(LegacyResultStoreUnavailable, match="amount is invalid"):
        bonus_store.claim_daily_bonus_for_result(
            user_id=42,
            result_id="result-a",
            day="2026-08-10",
            daily_streak=3,
        )


def test_owned_bonus_receipt_with_invalid_eligibility_is_retryable(monkeypatch):
    users = FakeUsers()
    users.doc["challenge_bonus_receipts"] = {
        "random20": {
            "20260810": {
                "bonus": 60,
                "eligible": "yes",
                "result_owner": bonus_store._owner("challenge-a"),
            }
        }
    }
    monkeypatch.setattr(database, "collection", users)

    with pytest.raises(LegacyResultStoreUnavailable, match="eligibility is invalid"):
        bonus_store.claim_challenge_bonus_for_result(
            user_id=42,
            result_id="challenge-a",
            mode="random20",
            score=18,
            day="2026-08-10",
        )
