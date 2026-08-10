from __future__ import annotations

from copy import deepcopy
from threading import Barrier, Lock, Thread

import pytest

import battle_consistency
import database


class UpdateResult:
    def __init__(self, modified_count: int):
        self.modified_count = modified_count


class FakeBattleCollection:
    def __init__(self, doc: dict):
        self.doc = deepcopy(doc)
        self.lock = Lock()

    def find_one_and_update(self, predicate, update, return_document=None):
        with self.lock:
            doc = self.doc
            if doc is None or doc.get("_id") != predicate.get("_id"):
                return None
            if doc.get("status") != predicate.get("status"):
                return None
            if doc.get("opponent_id") is not None:
                return None
            creator_guard = predicate.get("creator_id", {})
            if "$ne" in creator_guard and doc.get("creator_id") == creator_guard["$ne"]:
                return None
            doc.update(update["$set"])
            return deepcopy(doc)


class FakeUserCollection:
    def __init__(self, docs: dict[str, dict]):
        self.docs = deepcopy(docs)
        self.lock = Lock()

    def update_one(self, predicate, update):
        uid = predicate["_id"]
        receipt = predicate["battle_reward_receipts"]["$ne"]
        with self.lock:
            doc = self.docs.get(uid)
            if doc is None:
                return UpdateResult(0)
            receipts = doc.setdefault("battle_reward_receipts", [])
            if receipt in receipts:
                return UpdateResult(0)

            for key, value in update["$inc"].items():
                doc[key] = doc.get(key, 0) + value
            doc.update(update["$set"])

            push = update["$push"]["battle_reward_receipts"]
            receipts.extend(push["$each"])
            slice_value = push["$slice"]
            if slice_value < 0:
                doc["battle_reward_receipts"] = receipts[slice_value:]
            else:
                doc["battle_reward_receipts"] = receipts[:slice_value]
            return UpdateResult(1)

    def count_documents(self, predicate, limit=0):
        return int(predicate["_id"] in self.docs)


def _battle_doc():
    return {
        "_id": "battle_1_123",
        "creator_id": 1,
        "creator_name": "Creator",
        "status": "waiting",
        "opponent_id": None,
        "opponent_name": None,
    }


def _user_doc(uid: str):
    return {
        "_id": uid,
        "total_points": 0,
        "battles_played": 0,
        "battles_won": 0,
        "battles_lost": 0,
        "battles_draw": 0,
    }


def test_atomic_join_allows_only_one_opponent(monkeypatch):
    collection = FakeBattleCollection(_battle_doc())
    monkeypatch.setattr(database, "battles_collection", collection)

    barrier = Barrier(3)
    results = []

    def join(uid, name):
        barrier.wait()
        results.append(battle_consistency.join_battle_atomic("battle_1_123", uid, name))

    first = Thread(target=join, args=(2, "Two"))
    second = Thread(target=join, args=(3, "Three"))
    first.start()
    second.start()
    barrier.wait()
    first.join(timeout=2)
    second.join(timeout=2)

    winners = [item for item in results if item is not None]
    assert len(winners) == 1
    assert collection.doc["opponent_id"] in {2, 3}
    assert collection.doc["status"] == "in_progress"


def test_atomic_join_rejects_creator(monkeypatch):
    collection = FakeBattleCollection(_battle_doc())
    monkeypatch.setattr(database, "battles_collection", collection)

    assert battle_consistency.join_battle_atomic("battle_1_123", 1, "Creator") is None
    assert collection.doc["status"] == "waiting"
    assert collection.doc["opponent_id"] is None


def test_battle_reward_is_exactly_once_under_concurrent_finalizers(monkeypatch):
    users = FakeUserCollection({"1": _user_doc("1")})
    monkeypatch.setattr(database, "collection", users)

    barrier = Barrier(3)
    outcomes = []

    def reward():
        barrier.wait()
        outcomes.append(
            battle_consistency.apply_battle_reward_once(1, "battle_1_123", "win")
        )

    first = Thread(target=reward)
    second = Thread(target=reward)
    first.start()
    second.start()
    barrier.wait()
    first.join(timeout=2)
    second.join(timeout=2)

    doc = users.docs["1"]
    assert sum(result.applied for result in outcomes) == 1
    assert doc["battles_played"] == 1
    assert doc["battles_won"] == 1
    assert doc["total_points"] == 5
    assert doc["battle_reward_receipts"] == ["battle_1_123"]


def test_partial_battle_reward_retry_converges_without_duplicates(monkeypatch):
    users = FakeUserCollection({"1": _user_doc("1"), "2": _user_doc("2")})
    monkeypatch.setattr(database, "collection", users)

    assert battle_consistency.apply_battle_reward_once(1, "battle_1_123", "win").applied is True
    assert battle_consistency.apply_battle_reward_once(1, "battle_1_123", "win").applied is False
    assert battle_consistency.apply_battle_reward_once(2, "battle_1_123", "lose").applied is True

    assert users.docs["1"]["total_points"] == 5
    assert users.docs["1"]["battles_played"] == 1
    assert users.docs["2"]["battles_played"] == 1
    assert users.docs["2"]["battles_lost"] == 1


def test_draw_reward_is_idempotent(monkeypatch):
    users = FakeUserCollection({"1": _user_doc("1")})
    monkeypatch.setattr(database, "collection", users)

    first = battle_consistency.apply_battle_reward_once(1, "battle_draw", "draw")
    second = battle_consistency.apply_battle_reward_once(1, "battle_draw", "draw")

    assert first.applied is True
    assert second.applied is False
    assert users.docs["1"]["battles_draw"] == 1
    assert users.docs["1"]["total_points"] == 2


def test_reward_receipts_are_bounded(monkeypatch):
    users = FakeUserCollection({"1": _user_doc("1")})
    monkeypatch.setattr(database, "collection", users)

    for index in range(105):
        result = battle_consistency.apply_battle_reward_once(1, f"battle_{index}", "lose")
        assert result.applied is True

    doc = users.docs["1"]
    assert doc["battles_played"] == 105
    assert len(doc["battle_reward_receipts"]) == 100
    assert doc["battle_reward_receipts"][0] == "battle_5"
    assert doc["battle_reward_receipts"][-1] == "battle_104"


def test_reward_reports_missing_user(monkeypatch):
    users = FakeUserCollection({})
    monkeypatch.setattr(database, "collection", users)

    result = battle_consistency.apply_battle_reward_once(999, "battle_x", "win")
    assert result.applied is False
    assert result.missing_user is True


def test_invalid_battle_result_fails_closed(monkeypatch):
    users = FakeUserCollection({"1": _user_doc("1")})
    monkeypatch.setattr(database, "collection", users)

    with pytest.raises(ValueError, match="unsupported battle result"):
        battle_consistency.apply_battle_reward_once(1, "battle_x", "bonus")
