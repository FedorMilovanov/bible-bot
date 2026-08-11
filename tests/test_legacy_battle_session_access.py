from copy import deepcopy

import pytest
from pymongo.errors import PyMongoError

import legacy_battle_session as session
from legacy_battle_callback_protocol import battle_callback_token
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
        self.rows = list(rows)
        self.find_query = None
        self.find_one_query = None
        self.error = None

    def find(self, query):
        if self.error:
            raise self.error
        self.find_query = deepcopy(query)
        return Cursor(self.rows)

    def find_one(self, query):
        if self.error:
            raise self.error
        self.find_one_query = deepcopy(query)
        battle_id = query.get("_id")
        return next((deepcopy(row) for row in self.rows if row.get("_id") == battle_id), None)


def battle(battle_id="battle-1", *, creator_id=101, opponent_id=202):
    return {
        "_id": battle_id,
        "creator_id": creator_id,
        "opponent_id": opponent_id,
        "status": "in_progress",
        "final_claimed": False,
        "question_progress_protocol": BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE,
    }


def test_owned_open_lookup_is_participant_and_protocol_bound(monkeypatch):
    collection = Collection([battle()])
    monkeypatch.setattr(session, "_battle_collection", lambda: collection)

    row = session.get_owned_open_durable_battle("battle-1", 101)

    assert row["_id"] == "battle-1"
    query = collection.find_one_query
    assert query["question_progress_protocol"] == BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE
    assert query["status"] == {"$in": ["waiting", "in_progress"]}
    assert query["final_claimed"] == {"$ne": True}
    assert {"creator_id": 101} in query["$or"]
    assert {"opponent_id": 101} in query["$or"]


def test_callback_resolution_uses_durable_participant_set_not_ram(monkeypatch):
    rows = [battle("battle-a"), battle("battle-b")]
    collection = Collection(rows)
    monkeypatch.setattr(session, "_battle_collection", lambda: collection)

    resolved = session.resolve_owned_open_battle_callback(
        101,
        battle_callback_token("battle-b"),
    )

    assert resolved["_id"] == "battle-b"
    assert collection.find_query["question_progress_protocol"] == BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE
    assert collection.find_query["final_claimed"] == {"$ne": True}


def test_callback_resolution_rejects_stale_or_ambiguous_token(monkeypatch):
    duplicate = battle("battle-a")
    collection = Collection([duplicate, deepcopy(duplicate)])
    monkeypatch.setattr(session, "_battle_collection", lambda: collection)

    with pytest.raises(session.LegacyBattleSessionConflict, match="stale or ambiguous"):
        session.resolve_owned_open_battle_callback(101, battle_callback_token("battle-a"))

    collection.rows = [battle("battle-a")]
    with pytest.raises(session.LegacyBattleSessionConflict, match="stale or ambiguous"):
        session.resolve_owned_open_battle_callback(101, battle_callback_token("missing"))


def test_callback_resolution_ignores_malformed_candidate(monkeypatch):
    collection = Collection([{"creator_id": 101}, battle("battle-ok")])
    monkeypatch.setattr(session, "_battle_collection", lambda: collection)

    resolved = session.resolve_owned_open_battle_callback(
        101,
        battle_callback_token("battle-ok"),
    )
    assert resolved["_id"] == "battle-ok"


def test_callback_lookup_storage_outage_is_explicit(monkeypatch):
    collection = Collection([])
    collection.error = PyMongoError("down")
    monkeypatch.setattr(session, "_battle_collection", lambda: collection)

    with pytest.raises(session.LegacyBattleSessionUnavailable, match="callback lookup"):
        session.resolve_owned_open_battle_callback(101, battle_callback_token("battle-a"))
