from copy import deepcopy
from types import SimpleNamespace

import database
from battle_integrity import delete_battle_for_participant


def _matches_delete(doc: dict, query: dict) -> bool:
    if doc.get("_id") != query.get("_id"):
        return False
    participant = any(
        doc.get("creator_id") == clause.get("creator_id")
        or doc.get("opponent_id") == clause.get("opponent_id")
        for clause in query.get("$or", [])
    )
    if not participant:
        return False
    for field in ("creator_finished", "opponent_finished", "final_claimed"):
        condition = query.get(field, {})
        if condition.get("$ne") is True and doc.get(field) is True:
            return False
    return True


class FakeBattleCollection:
    def __init__(self, doc: dict):
        self.doc = deepcopy(doc)
        self.delete_filter = None

    def delete_one(self, query):
        self.delete_filter = deepcopy(query)
        if self.doc is not None and _matches_delete(self.doc, query):
            self.doc = None
            return SimpleNamespace(deleted_count=1)
        return SimpleNamespace(deleted_count=0)


def _battle(**overrides):
    doc = {
        "_id": "b1",
        "creator_id": 10,
        "opponent_id": 20,
        "creator_finished": False,
        "opponent_finished": False,
        "final_claimed": False,
    }
    doc.update(overrides)
    return doc


def test_participant_can_cancel_before_any_result_is_durable(monkeypatch):
    collection = FakeBattleCollection(_battle())
    monkeypatch.setattr(database, "battles_collection", collection)

    assert delete_battle_for_participant("b1", 20) is True
    assert collection.doc is None


def test_opponent_cannot_delete_creator_finished_result_evidence(monkeypatch):
    collection = FakeBattleCollection(_battle(creator_finished=True))
    monkeypatch.setattr(database, "battles_collection", collection)

    assert delete_battle_for_participant("b1", 20) is False
    assert collection.doc is not None


def test_creator_cannot_delete_opponent_finished_result_evidence(monkeypatch):
    collection = FakeBattleCollection(_battle(opponent_finished=True))
    monkeypatch.setattr(database, "battles_collection", collection)

    assert delete_battle_for_participant("b1", 10) is False
    assert collection.doc is not None


def test_nonparticipant_cannot_delete_unfinished_battle(monkeypatch):
    collection = FakeBattleCollection(_battle())
    monkeypatch.setattr(database, "battles_collection", collection)

    assert delete_battle_for_participant("b1", 99) is False
    assert collection.doc is not None
