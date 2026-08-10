from copy import deepcopy
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
        actual, exists = _get_path(doc, key)
        if isinstance(expected, dict) and "$exists" in expected:
            if exists != expected["$exists"]:
                return False
            continue
        if not exists or actual != expected:
            return False
    return True


class AchievementUsers:
    def __init__(self, doc, *, disappear_on_update=False, suppress_update=False):
        self.doc = deepcopy(doc)
        self.disappear_on_update = disappear_on_update
        self.suppress_update = suppress_update

    def update_one(self, query, update):
        if self.disappear_on_update:
            self.doc = None
            return SimpleNamespace(modified_count=0)
        if self.doc is None or not _matches(self.doc, query) or self.suppress_update:
            return SimpleNamespace(modified_count=0)
        for path, value in update.get("$set", {}).items():
            _set_path(self.doc, path, deepcopy(value))
        for path, value in update.get("$inc", {}).items():
            current, exists = _get_path(self.doc, path)
            _set_path(self.doc, path, (current if exists else 0) + value)
        return SimpleNamespace(modified_count=1)

    def find_one(self, query, projection=None):
        if self.doc is None or not _matches(self.doc, query):
            return None
        if not projection:
            return deepcopy(self.doc)
        result = {"_id": self.doc["_id"]}
        for path, include in projection.items():
            if not include:
                continue
            value, exists = _get_path(self.doc, path)
            if exists:
                _set_path(result, path, deepcopy(value))
        return result


def _user():
    return {"_id": "42", "total_points": 0, "achievements": {}}


def test_achievement_replay_is_the_only_zero_write_success(monkeypatch):
    users = AchievementUsers(_user())
    monkeypatch.setattr(database, "collection", users)

    assert store.claim_achievement_once(
        42,
        "first_steps",
        reward=10,
        awarded_at="10.08.2026",
    ) is True
    assert store.claim_achievement_once(
        42,
        "first_steps",
        reward=10,
        awarded_at="10.08.2026",
    ) is False
    assert users.doc["total_points"] == 10


def test_missing_user_after_zero_write_keeps_finalization_retryable(monkeypatch):
    users = AchievementUsers(_user(), disappear_on_update=True)
    monkeypatch.setattr(database, "collection", users)

    with pytest.raises(
        store.LegacyResultStoreUnavailable,
        match="user document disappeared",
    ):
        store.claim_achievement_once(
            42,
            "first_steps",
            reward=10,
            awarded_at="10.08.2026",
        )


def test_unexplained_zero_write_without_existing_achievement_is_retryable(monkeypatch):
    users = AchievementUsers(_user(), suppress_update=True)
    monkeypatch.setattr(database, "collection", users)

    with pytest.raises(
        store.LegacyResultStoreUnavailable,
        match="no prior claim exists",
    ):
        store.claim_achievement_once(
            42,
            "first_steps",
            reward=10,
            awarded_at="10.08.2026",
        )
