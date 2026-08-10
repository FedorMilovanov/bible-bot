from copy import deepcopy
from types import SimpleNamespace

import pytest

import battle_integrity as integrity
import database


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
            if "$ne" in expected:
                forbidden = expected["$ne"]
                if isinstance(actual, list):
                    if forbidden in actual:
                        return False
                elif exists and actual == forbidden:
                    return False
            continue
        if not exists or actual != expected:
            return False
    return True


class FakeUserCollection:
    def __init__(self):
        self.docs = {
            "10": {"_id": "10"},
            "20": {"_id": "20"},
        }

    def update_one(self, query, update):
        doc = self.docs.get(str(query["_id"]))
        if doc is None or not _matches(doc, query):
            return SimpleNamespace(modified_count=0)
        for path, value in update.get("$inc", {}).items():
            current, exists = _get_path(doc, path)
            _set_path(doc, path, (current if exists else 0) + value)
        for path, value in update.get("$set", {}).items():
            _set_path(doc, path, value)
        return SimpleNamespace(modified_count=1)

    def find_one(self, query, projection=None):
        doc = self.docs.get(str(query["_id"]))
        if doc is None or not _matches(doc, query):
            return None
        return deepcopy(doc)


class FakeBattleCollection:
    def __init__(self, battle):
        self.battle = deepcopy(battle)
        self.finalize_calls = 0

    def find_one(self, query, projection=None):
        if query.get("_id") != self.battle["_id"]:
            return None
        if query.get("creator_finished") is True and not self.battle.get("creator_finished"):
            return None
        if query.get("opponent_finished") is True and not self.battle.get("opponent_finished"):
            return None
        return deepcopy(self.battle)

    def find_one_and_update(self, query, update, return_document=None):
        if query.get("_id") != self.battle["_id"]:
            return None
        if query.get("final_claimed") == {"$ne": True} and self.battle.get("final_claimed") is True:
            return None
        self.finalize_calls += 1
        for path, value in update.get("$set", {}).items():
            _set_path(self.battle, path, value)
        return deepcopy(self.battle)


def _battle():
    return {
        "_id": "b1",
        "creator_id": 10,
        "creator_name": "Creator",
        "creator_finished": True,
        "creator_points": 15,
        "opponent_id": 20,
        "opponent_name": "Opponent",
        "opponent_finished": True,
        "opponent_points": 10,
    }


def test_retry_after_second_outcome_failure_does_not_double_credit_first_user(monkeypatch):
    battles = FakeBattleCollection(_battle())
    users = FakeUserCollection()
    monkeypatch.setattr(database, "battles_collection", battles)
    monkeypatch.setattr(database, "collection", users)

    real_apply = integrity._apply_battle_outcome_once
    failed_once = False

    def fail_on_first_opponent(battle_id, user_id, result, *, first_name):
        nonlocal failed_once
        if user_id == 20 and not failed_once:
            failed_once = True
            raise integrity.BattleStoreUnavailable("opponent outcome write failed")
        return real_apply(
            battle_id,
            user_id,
            result,
            first_name=first_name,
        )

    monkeypatch.setattr(integrity, "_apply_battle_outcome_once", fail_on_first_opponent)

    with pytest.raises(integrity.BattleStoreUnavailable, match="opponent outcome"):
        integrity.claim_final_battle("b1")

    assert users.docs["10"]["battles_played"] == 1
    assert users.docs["10"]["battles_won"] == 1
    assert users.docs["10"]["total_points"] == 5
    assert "battles_played" not in users.docs["20"]
    assert battles.finalize_calls == 0

    monkeypatch.setattr(integrity, "_apply_battle_outcome_once", real_apply)
    finalized = integrity.claim_final_battle("b1")

    assert finalized["final_claimed"] is True
    assert finalized["status"] == "finalized"
    assert battles.finalize_calls == 1
    assert users.docs["10"]["battles_played"] == 1
    assert users.docs["10"]["battles_won"] == 1
    assert users.docs["10"]["total_points"] == 5
    assert users.docs["20"]["battles_played"] == 1
    assert users.docs["20"]["battles_lost"] == 1
