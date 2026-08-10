from copy import deepcopy
from types import SimpleNamespace

import pytest

import database
from battle_integrity import (
    BattleStoreUnavailable,
    _apply_battle_outcome_once,
    _battle_receipt_digest,
    battle_role_for_user,
    claim_battle_opponent,
    claim_final_battle,
    delete_battle_for_participant,
    record_battle_result,
)


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


def _apply_update(doc, update):
    for key, value in update.get("$inc", {}).items():
        current, exists = _get_path(doc, key)
        _set_path(doc, key, (current if exists else 0) + value)
    for key, value in update.get("$set", {}).items():
        _set_path(doc, key, value)


class FakeBattleCollection:
    def __init__(self):
        self.claim_filter = None
        self.claim_update = None
        self.claim_result = None
        self.find_filter = None
        self.find_result = None
        self.delete_filter = None
        self.deleted_count = 0
        self.final_delete_filter = None
        self.final_delete_result = None

    def find_one_and_update(self, query, update, return_document=None):
        self.claim_filter = query
        self.claim_update = update
        return self.claim_result

    def find_one(self, query, projection=None):
        self.find_filter = query
        return self.find_result

    def find_one_and_delete(self, query):
        self.final_delete_filter = query
        return self.final_delete_result

    def delete_one(self, query):
        self.delete_filter = query
        return SimpleNamespace(deleted_count=self.deleted_count)


class FakeUserCollection:
    def __init__(self):
        self.update_calls = []
        self.docs = {}

    def add_user(self, uid, **fields):
        self.docs[str(uid)] = {"_id": str(uid), **deepcopy(fields)}

    def update_one(self, query, update):
        self.update_calls.append((deepcopy(query), deepcopy(update)))
        uid = str(query["_id"])
        doc = self.docs.get(uid)
        if doc is None or not _matches(doc, query):
            return SimpleNamespace(modified_count=0)
        _apply_update(doc, update)
        return SimpleNamespace(modified_count=1)

    def find_one(self, query, projection=None):
        uid = str(query["_id"])
        doc = self.docs.get(uid)
        if doc is None or not _matches(doc, query):
            return None
        return deepcopy(doc)


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


def test_battle_role_for_user():
    battle = {"creator_id": 10, "opponent_id": 20}
    assert battle_role_for_user(battle, 10) == "creator"
    assert battle_role_for_user(battle, 20) == "opponent"
    assert battle_role_for_user(battle, 30) is None


def test_claim_uses_single_compare_and_set(monkeypatch):
    collection = FakeBattleCollection()
    collection.claim_result = {"_id": "b1", "opponent_id": 20, "status": "in_progress"}
    monkeypatch.setattr(database, "battles_collection", collection)

    assert claim_battle_opponent("b1", 20, "Player") == collection.claim_result
    assert collection.claim_filter == {
        "_id": "b1",
        "status": "waiting",
        "opponent_id": None,
        "creator_id": {"$ne": 20},
    }
    assert collection.claim_update["$set"]["opponent_id"] == 20
    assert collection.claim_update["$set"]["status"] == "in_progress"


def test_participant_result_write_is_owner_scoped_and_once(monkeypatch):
    collection = FakeBattleCollection()
    collection.claim_result = {"_id": "b1", "creator_finished": True}
    monkeypatch.setattr(database, "battles_collection", collection)

    assert record_battle_result(
        "b1", 10, "creator", score=9, time_seconds=42.5, points=130
    ) == collection.claim_result
    assert collection.claim_filter == {
        "_id": "b1",
        "creator_id": 10,
        "creator_finished": {"$ne": True},
        "status": {"$in": ["waiting", "in_progress"]},
    }
    assert collection.claim_update["$set"] == {
        "creator_score": 9,
        "creator_time": 42.5,
        "creator_points": 130,
        "creator_finished": True,
    }


def test_participant_result_rejects_nonfinite_time_before_write(monkeypatch):
    collection = FakeBattleCollection()
    monkeypatch.setattr(database, "battles_collection", collection)

    with pytest.raises(ValueError, match="battle time"):
        record_battle_result(
            "b1", 10, "creator", score=9, time_seconds=float("inf"), points=130
        )

    assert collection.claim_filter is None


def test_participant_result_retry_returns_existing_snapshot_without_rewrite(monkeypatch):
    collection = FakeBattleCollection()
    collection.claim_result = None
    collection.find_result = {
        "_id": "b1",
        "creator_id": 10,
        "creator_finished": True,
        "creator_score": 9,
    }
    monkeypatch.setattr(database, "battles_collection", collection)

    result = record_battle_result(
        "b1", 10, "creator", score=10, time_seconds=1.0, points=999
    )

    assert result == collection.find_result
    assert collection.find_filter == {
        "_id": "b1",
        "creator_id": 10,
        "creator_finished": True,
    }


def test_final_battle_applies_each_user_marker_then_atomically_deletes(monkeypatch):
    battle_collection = FakeBattleCollection()
    battle = _battle()
    battle_collection.find_result = battle
    battle_collection.final_delete_result = battle
    user_collection = FakeUserCollection()
    user_collection.add_user(10)
    user_collection.add_user(20)
    monkeypatch.setattr(database, "battles_collection", battle_collection)
    monkeypatch.setattr(database, "collection", user_collection)

    assert claim_final_battle("b1") == battle
    assert battle_collection.final_delete_filter == {
        "_id": "b1",
        "creator_finished": True,
        "opponent_finished": True,
    }
    digest = _battle_receipt_digest("b1")
    assert user_collection.docs["10"]["battle_result_receipt_map"][digest] is True
    assert user_collection.docs["20"]["battle_result_receipt_map"][digest] is True
    creator_inc = user_collection.update_calls[0][1]["$inc"]
    opponent_inc = user_collection.update_calls[1][1]["$inc"]
    assert creator_inc == {"battles_played": 1, "battles_won": 1, "total_points": 5}
    assert opponent_inc == {"battles_played": 1, "battles_lost": 1}


def test_final_battle_retry_does_not_increment_existing_marker(monkeypatch):
    battle_collection = FakeBattleCollection()
    battle = _battle()
    battle["creator_points"] = 8
    battle["opponent_points"] = 8
    battle_collection.find_result = battle
    battle_collection.final_delete_result = battle
    user_collection = FakeUserCollection()
    digest = _battle_receipt_digest("b1")
    user_collection.add_user(10, battle_result_receipt_map={digest: True})
    user_collection.add_user(20, battle_result_receipt_map={digest: True})
    monkeypatch.setattr(database, "battles_collection", battle_collection)
    monkeypatch.setattr(database, "collection", user_collection)

    assert claim_final_battle("b1") == battle
    assert "battles_played" not in user_collection.docs["10"]
    assert "battles_played" not in user_collection.docs["20"]


def test_legacy_array_receipt_remains_valid_without_new_pushes(monkeypatch):
    battle_collection = FakeBattleCollection()
    battle = _battle()
    battle_collection.find_result = battle
    battle_collection.final_delete_result = battle
    user_collection = FakeUserCollection()
    user_collection.add_user(10, battle_result_receipts=["b1"])
    user_collection.add_user(20, battle_result_receipts=["b1"])
    monkeypatch.setattr(database, "battles_collection", battle_collection)
    monkeypatch.setattr(database, "collection", user_collection)

    assert claim_final_battle("b1") == battle
    assert user_collection.docs["10"]["battle_result_receipts"] == ["b1"]
    assert user_collection.docs["20"]["battle_result_receipts"] == ["b1"]
    assert "battle_result_receipt_map" not in user_collection.docs["10"]
    assert all("$push" not in update for _query, update in user_collection.update_calls)


def test_non_evicting_markers_keep_first_battle_idempotent_after_70_newer(monkeypatch):
    user_collection = FakeUserCollection()
    user_collection.add_user(10)
    monkeypatch.setattr(database, "collection", user_collection)

    for index in range(70):
        _apply_battle_outcome_once(
            f"battle-{index}",
            10,
            "draw",
            first_name="Player",
        )

    points_before = user_collection.docs["10"]["total_points"]
    played_before = user_collection.docs["10"]["battles_played"]
    _apply_battle_outcome_once("battle-0", 10, "draw", first_name="Player")

    assert played_before == 70
    assert user_collection.docs["10"]["battles_played"] == 70
    assert user_collection.docs["10"]["total_points"] == points_before
    assert len(user_collection.docs["10"]["battle_result_receipt_map"]) == 70
    assert "battle_result_receipts" not in user_collection.docs["10"]


def test_invalid_receipt_marker_is_retryable(monkeypatch):
    user_collection = FakeUserCollection()
    digest = _battle_receipt_digest("b1")
    user_collection.add_user(10, battle_result_receipt_map={digest: "broken"})
    monkeypatch.setattr(database, "collection", user_collection)

    with pytest.raises(BattleStoreUnavailable, match="receipt marker is invalid"):
        _apply_battle_outcome_once("b1", 10, "win", first_name="Player")


def test_delete_is_scoped_to_persisted_participant(monkeypatch):
    collection = FakeBattleCollection()
    collection.deleted_count = 1
    monkeypatch.setattr(database, "battles_collection", collection)

    assert delete_battle_for_participant("b1", 20) is True
    assert collection.delete_filter == {
        "_id": "b1",
        "$or": [{"creator_id": 20}, {"opponent_id": 20}],
    }
