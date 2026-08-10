from types import SimpleNamespace

import database
from battle_integrity import (
    battle_role_for_user,
    claim_battle_opponent,
    claim_final_battle,
    delete_battle_for_participant,
    record_battle_result,
)


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
        self.receipts = {}

    def update_one(self, query, update):
        uid = query["_id"]
        battle_id = query["battle_result_receipts"]["$ne"]
        self.update_calls.append((query, update))
        current = self.receipts.setdefault(uid, [])
        if battle_id in current:
            return SimpleNamespace(modified_count=0)
        current.append(battle_id)
        return SimpleNamespace(modified_count=1)

    def find_one(self, query, projection=None):
        uid = query["_id"]
        if uid not in self.receipts:
            return None
        return {"_id": uid, "battle_result_receipts": list(self.receipts[uid])}


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


def test_final_battle_applies_each_user_receipt_then_atomically_deletes(monkeypatch):
    battle_collection = FakeBattleCollection()
    battle = {
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
    battle_collection.find_result = battle
    battle_collection.final_delete_result = battle
    user_collection = FakeUserCollection()
    monkeypatch.setattr(database, "battles_collection", battle_collection)
    monkeypatch.setattr(database, "collection", user_collection)

    assert claim_final_battle("b1") == battle
    assert battle_collection.final_delete_filter == {
        "_id": "b1",
        "creator_finished": True,
        "opponent_finished": True,
    }
    assert user_collection.receipts == {"10": ["b1"], "20": ["b1"]}
    creator_inc = user_collection.update_calls[0][1]["$inc"]
    opponent_inc = user_collection.update_calls[1][1]["$inc"]
    assert creator_inc == {"battles_played": 1, "battles_won": 1, "total_points": 5}
    assert opponent_inc == {"battles_played": 1, "battles_lost": 1}


def test_final_battle_retry_does_not_increment_existing_user_receipts(monkeypatch):
    battle_collection = FakeBattleCollection()
    battle = {
        "_id": "b1",
        "creator_id": 10,
        "creator_name": "Creator",
        "creator_finished": True,
        "creator_points": 8,
        "opponent_id": 20,
        "opponent_name": "Opponent",
        "opponent_finished": True,
        "opponent_points": 8,
    }
    battle_collection.find_result = battle
    battle_collection.final_delete_result = battle
    user_collection = FakeUserCollection()
    user_collection.receipts = {"10": ["b1"], "20": ["b1"]}
    monkeypatch.setattr(database, "battles_collection", battle_collection)
    monkeypatch.setattr(database, "collection", user_collection)

    assert claim_final_battle("b1") == battle
    assert user_collection.receipts == {"10": ["b1"], "20": ["b1"]}
    assert all(call[0]["battle_result_receipts"] == {"$ne": "b1"} for call in user_collection.update_calls)


def test_delete_is_scoped_to_persisted_participant(monkeypatch):
    collection = FakeBattleCollection()
    collection.deleted_count = 1
    monkeypatch.setattr(database, "battles_collection", collection)

    assert delete_battle_for_participant("b1", 20) is True
    assert collection.delete_filter == {
        "_id": "b1",
        "$or": [{"creator_id": 20}, {"opponent_id": 20}],
    }
