from types import SimpleNamespace

import database
from battle_integrity import (
    battle_role_for_user,
    claim_battle_opponent,
    claim_battle_results,
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

    def find_one_and_update(self, query, update, return_document=None):
        self.claim_filter = query
        self.claim_update = update
        return self.claim_result

    def find_one(self, query):
        self.find_filter = query
        return self.find_result

    def delete_one(self, query):
        self.delete_filter = query
        return SimpleNamespace(deleted_count=self.deleted_count)


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


def test_shared_results_can_be_claimed_only_after_both_finish(monkeypatch):
    collection = FakeBattleCollection()
    collection.claim_result = {"_id": "b1", "results_processed": True, "status": "finished"}
    monkeypatch.setattr(database, "battles_collection", collection)

    assert claim_battle_results("b1") == collection.claim_result
    assert collection.claim_filter == {
        "_id": "b1",
        "creator_finished": True,
        "opponent_finished": True,
        "results_processed": {"$ne": True},
    }
    assert collection.claim_update == {
        "$set": {"results_processed": True, "status": "finished"}
    }


def test_delete_is_scoped_to_persisted_participant(monkeypatch):
    collection = FakeBattleCollection()
    collection.deleted_count = 1
    monkeypatch.setattr(database, "battles_collection", collection)

    assert delete_battle_for_participant("b1", 20) is True
    assert collection.delete_filter == {
        "_id": "b1",
        "$or": [{"creator_id": 20}, {"opponent_id": 20}],
    }
