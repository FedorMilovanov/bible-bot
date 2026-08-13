from copy import deepcopy

import pytest

import battle_integrity as integrity
import database


class Cursor:
    def __init__(self, rows):
        self.rows = deepcopy(rows)
        self.limit_value = None

    def limit(self, value):
        self.limit_value = value
        return self

    def __iter__(self):
        return iter(deepcopy(self.rows))


class ProtocolBattleCollection:
    def __init__(self, battle):
        self.battle = deepcopy(battle)
        self.finalize_query = None
        self.finalize_update = None
        self.pending_query = None

    def find_one(self, query, projection=None):
        if query.get("_id") != self.battle.get("_id"):
            return None
        if query.get("creator_finished") is True and self.battle.get("creator_finished") is not True:
            return None
        if query.get("opponent_finished") is True and self.battle.get("opponent_finished") is not True:
            return None
        return deepcopy(self.battle)

    def find_one_and_update(self, query, update, return_document=None):
        self.finalize_query = deepcopy(query)
        self.finalize_update = deepcopy(update)
        if self.battle.get("final_claimed") is True:
            return None
        self.battle.update(deepcopy(update["$set"]))
        return deepcopy(self.battle)

    def find(self, query):
        self.pending_query = deepcopy(query)
        matches = (
            self.battle.get("status") == query.get("status")
            and self.battle.get("result_delivery_protocol")
            == query.get("result_delivery_protocol")
        )
        return Cursor([self.battle] if matches else [])


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


def _disable_outcome_writes(monkeypatch):
    calls = []

    def apply(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(integrity, "_apply_battle_outcome_once", apply)
    return calls


def test_current_controller_default_marks_final_battle_as_legacy_direct(monkeypatch):
    collection = ProtocolBattleCollection(_battle())
    monkeypatch.setattr(database, "battles_collection", collection)
    _disable_outcome_writes(monkeypatch)

    finalized = integrity.claim_final_battle("b1")

    assert finalized["result_delivery_protocol"] == (
        integrity.BATTLE_DELIVERY_PROTOCOL_LEGACY_DIRECT
    )
    assert collection.finalize_update["$set"]["result_delivery_protocol"] == (
        integrity.BATTLE_DELIVERY_PROTOCOL_LEGACY_DIRECT
    )


def test_future_outbox_controller_must_opt_in_explicitly(monkeypatch):
    collection = ProtocolBattleCollection(_battle())
    monkeypatch.setattr(database, "battles_collection", collection)
    _disable_outcome_writes(monkeypatch)

    finalized = integrity.claim_final_battle(
        "b1",
        delivery_protocol=integrity.BATTLE_DELIVERY_PROTOCOL_OUTBOX,
    )

    assert finalized["result_delivery_protocol"] == integrity.BATTLE_DELIVERY_PROTOCOL_OUTBOX


def test_pending_listing_selects_only_outbox_protocol(monkeypatch):
    battle = _battle()
    battle.update(
        {
            "status": "finalized",
            "final_claimed": True,
            "result_delivery_protocol": integrity.BATTLE_DELIVERY_PROTOCOL_OUTBOX,
            "result_delivery": {
                "creator": {"delivered": False},
                "opponent": {"delivered": False},
            },
        }
    )
    collection = ProtocolBattleCollection(battle)
    monkeypatch.setattr(database, "battles_collection", collection)

    rows = integrity.get_pending_final_battles(limit=7)

    assert [row["_id"] for row in rows] == ["b1"]
    assert collection.pending_query["status"] == "finalized"
    assert collection.pending_query["result_delivery_protocol"] == (
        integrity.BATTLE_DELIVERY_PROTOCOL_OUTBOX
    )


def test_unknown_protocol_fails_before_outcome_or_finalization(monkeypatch):
    collection = ProtocolBattleCollection(_battle())
    monkeypatch.setattr(database, "battles_collection", collection)
    calls = _disable_outcome_writes(monkeypatch)

    with pytest.raises(ValueError, match="unsupported battle delivery protocol"):
        integrity.claim_final_battle("b1", delivery_protocol="guess-and-send")

    assert calls == []
    assert collection.finalize_query is None
