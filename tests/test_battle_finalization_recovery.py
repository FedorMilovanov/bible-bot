from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

from pymongo.errors import AutoReconnect

import battle_consistency
import database


class Cursor(list):
    def limit(self, count):
        return Cursor(self[:count])


class LeaseCollection:
    def __init__(self, docs):
        self.docs = {doc["_id"]: deepcopy(doc) for doc in docs}

    @staticmethod
    def _eligible(doc, predicate):
        if doc.get("creator_finished") is not True or doc.get("opponent_finished") is not True:
            return False
        clauses = predicate["$or"]
        state = doc.get("result_state")
        claimed = doc.get("result_claimed_at_dt")
        for clause in clauses:
            if clause == {"result_state": {"$exists": False}} and "result_state" not in doc:
                return True
            if clause == {"result_state": "pending"} and state == "pending":
                return True
            if clause.get("result_state") == "finalizing" and state == "finalizing":
                condition = clause.get("result_claimed_at_dt", {})
                if "$lt" in condition and claimed is not None and claimed < condition["$lt"]:
                    return True
                if condition == {"$exists": False} and claimed is None:
                    return True
        return False

    def find_one_and_update(self, predicate, update, return_document=None):
        doc = self.docs.get(predicate["_id"])
        if doc is None or not self._eligible(doc, predicate):
            return None
        doc.update(update["$set"])
        return deepcopy(doc)

    def find(self, predicate, projection=None):
        return Cursor(
            [{"_id": doc["_id"]} for doc in self.docs.values() if self._eligible(doc, predicate)]
        )


class Result:
    def __init__(self, *, modified_count=0, deleted_count=0):
        self.modified_count = modified_count
        self.deleted_count = deleted_count


class RewardCollection:
    def __init__(self, *, receipt_exists=False, user_exists=True, fail=False):
        self.receipt_exists = receipt_exists
        self.user_exists = user_exists
        self.fail = fail

    def update_one(self, predicate, update):
        if self.fail:
            raise AutoReconnect("temporary outage")
        return Result()

    def count_documents(self, predicate, limit=0):
        if self.fail:
            raise AutoReconnect("temporary outage")
        if "battle_reward_receipts" in predicate:
            return int(self.receipt_exists)
        return int(self.user_exists)


class DeliveryCollection:
    def __init__(self, doc):
        self.doc = deepcopy(doc)

    def update_one(self, predicate, update):
        doc = self.doc
        if doc is None or doc.get("_id") != predicate.get("_id"):
            return Result()
        role = "creator" if "creator_id" in predicate else "opponent"
        if doc.get(f"{role}_id") != predicate.get(f"{role}_id"):
            return Result()
        key = f"{role}_result_delivered"
        if doc.get(key) is True:
            return Result()
        doc.update(update["$set"])
        return Result(modified_count=1)

    def count_documents(self, predicate, limit=0):
        doc = self.doc
        if doc is None or doc.get("_id") != predicate.get("_id"):
            return 0
        for key, value in predicate.items():
            if key == "_id":
                continue
            if doc.get(key) != value:
                return 0
        return 1

    def delete_one(self, predicate):
        doc = self.doc
        if doc is None or doc.get("_id") != predicate.get("_id"):
            return Result()
        if not all(doc.get(key) is value for key, value in predicate.items() if key != "_id"):
            return Result()
        self.doc = None
        return Result(deleted_count=1)


def _finished(battle_id):
    return {
        "_id": battle_id,
        "creator_finished": True,
        "opponent_finished": True,
    }


def test_finalization_lease_blocks_duplicate_and_recovers_after_expiry(monkeypatch):
    clock = [datetime(2026, 8, 10, 2, 0, tzinfo=UTC)]
    collection = LeaseCollection([_finished("battle_a")])
    monkeypatch.setattr(database, "battles_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: clock[0])

    assert battle_consistency.get_battles_needing_finalization() == ["battle_a"]
    first = battle_consistency.claim_battle_finalization("battle_a")
    assert first is not None
    assert first["result_state"] == "finalizing"
    assert battle_consistency.claim_battle_finalization("battle_a") is None
    assert battle_consistency.get_battles_needing_finalization() == []

    clock[0] += timedelta(seconds=31)
    assert battle_consistency.get_battles_needing_finalization() == ["battle_a"]
    recovered = battle_consistency.claim_battle_finalization("battle_a")
    assert recovered is not None
    assert recovered["result_claimed_at_dt"] == clock[0]


def test_incomplete_battle_is_never_finalizable(monkeypatch):
    clock = datetime(2026, 8, 10, 2, 0, tzinfo=UTC)
    collection = LeaseCollection([
        {
            "_id": "battle_waiting",
            "creator_finished": True,
            "opponent_finished": False,
        }
    ])
    monkeypatch.setattr(database, "battles_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: clock)

    assert battle_consistency.get_battles_needing_finalization() == []
    assert battle_consistency.claim_battle_finalization("battle_waiting") is None


def test_reward_duplicate_is_distinguished_from_retryable_error(monkeypatch):
    duplicate = RewardCollection(receipt_exists=True)
    monkeypatch.setattr(database, "collection", duplicate)
    already = battle_consistency.apply_battle_reward_once(1, "battle_a", "win")
    assert already.already_applied is True
    assert already.retryable_error is False

    failing = RewardCollection(fail=True)
    monkeypatch.setattr(database, "collection", failing)
    retry = battle_consistency.apply_battle_reward_once(1, "battle_a", "win")
    assert retry.applied is False
    assert retry.retryable_error is True


def test_reward_missing_user_is_terminally_classified(monkeypatch):
    collection = RewardCollection(receipt_exists=False, user_exists=False)
    monkeypatch.setattr(database, "collection", collection)

    result = battle_consistency.apply_battle_reward_once(99, "battle_a", "lose")
    assert result.missing_user is True
    assert result.retryable_error is False


def test_result_delivery_receipts_are_idempotent_and_participant_bound(monkeypatch):
    clock = datetime(2026, 8, 10, 2, 0, tzinfo=UTC)
    collection = DeliveryCollection({
        "_id": "battle_a",
        "creator_id": 1,
        "opponent_id": 2,
    })
    monkeypatch.setattr(database, "battles_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: clock)

    assert battle_consistency.mark_battle_result_delivered("battle_a", 99, "creator") is False
    assert battle_consistency.mark_battle_result_delivered("battle_a", 1, "creator") is True
    assert battle_consistency.mark_battle_result_delivered("battle_a", 1, "creator") is True
    assert collection.doc["creator_result_delivered"] is True
    assert collection.doc["creator_result_delivered_at"] == clock


def test_battle_delete_requires_both_result_delivery_receipts(monkeypatch):
    collection = DeliveryCollection({
        "_id": "battle_a",
        "creator_id": 1,
        "opponent_id": 2,
        "creator_result_delivered": True,
        "opponent_result_delivered": False,
    })
    monkeypatch.setattr(database, "battles_collection", collection)

    assert battle_consistency.delete_battle_if_fully_delivered("battle_a") is False
    assert collection.doc is not None

    collection.doc["opponent_result_delivered"] = True
    assert battle_consistency.delete_battle_if_fully_delivered("battle_a") is True
    assert collection.doc is None
