from types import SimpleNamespace

import database
from battle_integrity import battle_role_for_user, claim_battle_opponent, delete_battle_for_participant


class FakeBattleCollection:
    def __init__(self):
        self.claim_filter = None
        self.claim_update = None
        self.claim_result = None
        self.delete_filter = None
        self.deleted_count = 0

    def find_one_and_update(self, query, update, return_document=None):
        self.claim_filter = query
        self.claim_update = update
        return self.claim_result

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


def test_delete_is_scoped_to_persisted_participant(monkeypatch):
    collection = FakeBattleCollection()
    collection.deleted_count = 1
    monkeypatch.setattr(database, "battles_collection", collection)

    assert delete_battle_for_participant("b1", 20) is True
    assert collection.delete_filter == {
        "_id": "b1",
        "$or": [{"creator_id": 20}, {"opponent_id": 20}],
    }
