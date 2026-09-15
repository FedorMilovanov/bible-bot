import pytest
from pymongo.errors import PyMongoError
from copy import deepcopy

import legacy_battle_finalization_drain as drain
from battle_integrity import BATTLE_DELIVERY_PROTOCOL_OUTBOX
from legacy_battle_protocols import BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE


class Cursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.limit_value = None

    def limit(self, value):
        self.limit_value = value
        return self

    def __iter__(self):
        return iter(deepcopy(self.rows[: self.limit_value]))


class Collection:
    def __init__(self, rows):
        self.rows = rows
        self.query = None
        self.projection = None

    def find(self, query, projection):
        self.query = deepcopy(query)
        self.projection = deepcopy(projection)
        return Cursor(self.rows)


def test_ready_sweep_is_durable_protocol_bound_and_uses_outbox(monkeypatch):
    collection = Collection([{"_id": "b1"}, {"_id": "b2"}])
    monkeypatch.setattr(drain, "_collection", lambda: collection)
    calls = []

    def claim(battle_id, *, delivery_protocol):
        calls.append((battle_id, delivery_protocol))
        return {"_id": battle_id}

    monkeypatch.setattr(drain, "claim_final_battle", claim)
    result = drain.finalize_ready_battles(limit=7)

    assert collection.query["question_progress_protocol"] == BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE
    assert collection.query["creator_finished"] is True
    assert collection.query["opponent_finished"] is True
    assert collection.query["final_claimed"] == {"$ne": True}
    assert calls == [
        ("b1", BATTLE_DELIVERY_PROTOCOL_OUTBOX),
        ("b2", BATTLE_DELIVERY_PROTOCOL_OUTBOX),
    ]
    assert result.battles_seen == 2
    assert result.finalized == 2
    assert result.errors == ()


def test_lost_finalization_race_is_deferred_not_error(monkeypatch):
    collection = Collection([{"_id": "b1"}])
    monkeypatch.setattr(drain, "_collection", lambda: collection)
    monkeypatch.setattr(drain, "claim_final_battle", lambda *_args, **_kwargs: None)

    result = drain.finalize_ready_battles()
    assert result.finalized == 0
    assert result.deferred == 1
    assert result.errors == ()


def test_one_finalization_failure_does_not_starve_next(monkeypatch):
    collection = Collection([{"_id": "battle-sensitive-id"}, {"_id": "good"}])
    monkeypatch.setattr(drain, "_collection", lambda: collection)

    def claim(battle_id, **_kwargs):
        if battle_id == "battle-sensitive-id":
            raise drain.BattleStoreUnavailable("provider-sensitive-marker")
        return {"_id": battle_id}

    monkeypatch.setattr(drain, "claim_final_battle", claim)
    result = drain.finalize_ready_battles()
    assert result.finalized == 1
    assert result.errors == ("battle-finalize:BattleStoreUnavailable",)
    assert "battle-sensitive-id" not in repr(result.errors)
    assert "provider-sensitive-marker" not in repr(result.errors)


def test_ready_lookup_suppresses_raw_mongo_cause(monkeypatch):
    class BrokenCollection:
        def find(self, *_args, **_kwargs):
            raise PyMongoError("mongo-sensitive-marker")

    monkeypatch.setattr(drain, "_collection", lambda: BrokenCollection())
    with pytest.raises(drain.LegacyBattleFinalizationQueueUnavailable) as raised:
        drain._ready_battle_ids(5)
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ is True
