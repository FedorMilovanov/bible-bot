from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace

import pytest
from pymongo.errors import DuplicateKeyError, PyMongoError

import legacy_battle_session as session
from legacy_battle_protocols import BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE


QUESTION = {"question": "Кто?", "options": ["Пётр", "Павел"], "correct": 0}


class FakeCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.sort_args = None
        self.limit_value = None

    def sort(self, *args):
        self.sort_args = args
        return self

    def limit(self, value):
        self.limit_value = value
        return self

    def __iter__(self):
        return iter(deepcopy(self.rows[: self.limit_value]))


class FakeCollection:
    def __init__(self):
        self.inserted = []
        self.insert_error = None
        self.find_query = None
        self.cursor = FakeCursor([])
        self.claim_call = None
        self.claim_result = None

    def insert_one(self, doc):
        if self.insert_error is not None:
            raise self.insert_error
        self.inserted.append(deepcopy(doc))

    def find(self, query):
        self.find_query = deepcopy(query)
        return self.cursor

    def find_one_and_update(self, query, update, return_document=None):
        self.claim_call = (deepcopy(query), deepcopy(update), return_document)
        return deepcopy(self.claim_result)


def install(monkeypatch, collection, now):
    monkeypatch.setattr(session, "_battle_collection", lambda: collection)
    monkeypatch.setattr(session, "_database", lambda: SimpleNamespace(_now_utc=lambda: now))


def test_create_battle_is_versioned_before_it_can_be_published(monkeypatch):
    now = datetime(2026, 8, 11, 12, 0, 0)
    collection = FakeCollection()
    install(monkeypatch, collection, now)

    doc = session.create_durable_battle(
        battle_id="battle-1",
        creator_id=101,
        creator_name="Creator",
        questions=[deepcopy(QUESTION)],
    )

    assert doc["question_progress_protocol"] == BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE
    assert doc["status"] == "waiting"
    assert doc["creator_finished"] is False
    assert doc["opponent_finished"] is False
    assert collection.inserted == [doc]


def test_create_duplicate_is_conflict_not_phantom_success(monkeypatch):
    collection = FakeCollection()
    collection.insert_error = DuplicateKeyError("duplicate")
    install(monkeypatch, collection, datetime(2026, 8, 11, 12, 0, 0))

    with pytest.raises(session.LegacyBattleSessionConflict, match="already exists"):
        session.create_durable_battle(
            battle_id="battle-1",
            creator_id=101,
            creator_name="Creator",
            questions=[deepcopy(QUESTION)],
        )


def test_create_store_outage_is_explicit(monkeypatch):
    collection = FakeCollection()
    collection.insert_error = PyMongoError("mongo unavailable")
    install(monkeypatch, collection, datetime(2026, 8, 11, 12, 0, 0))

    with pytest.raises(session.LegacyBattleSessionUnavailable, match="creation failed"):
        session.create_durable_battle(
            battle_id="battle-1",
            creator_id=101,
            creator_name="Creator",
            questions=[deepcopy(QUESTION)],
        )


def test_waiting_discovery_excludes_legacy_unversioned_battles(monkeypatch):
    now = datetime(2026, 8, 11, 12, 0, 0)
    collection = FakeCollection()
    collection.cursor = FakeCursor([{"_id": "battle-1"}])
    install(monkeypatch, collection, now)

    rows = session.get_waiting_durable_battles(limit=5)

    assert rows == [{"_id": "battle-1"}]
    assert collection.find_query["question_progress_protocol"] == BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE
    assert collection.find_query["final_claimed"] == {"$ne": True}
    assert collection.cursor.limit_value == 5


def test_opponent_claim_is_protocol_bound_owner_safe_and_records_join_time(monkeypatch):
    now = datetime(2026, 8, 11, 12, 0, 0)
    collection = FakeCollection()
    collection.claim_result = {
        "_id": "battle-1",
        "creator_id": 101,
        "opponent_id": 202,
        "question_progress_protocol": BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE,
        "status": "in_progress",
    }
    install(monkeypatch, collection, now)

    claimed = session.claim_durable_battle_opponent("battle-1", 202, "Opponent")

    assert claimed["opponent_id"] == 202
    query, update, _ = collection.claim_call
    assert query["question_progress_protocol"] == BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE
    assert query["creator_id"] == {"$ne": 202}
    assert query["opponent_id"] is None
    assert update["$set"]["status"] == "in_progress"
    assert update["$set"]["joined_at_dt"] == now
    assert update["$set"]["updated_at"] == now.isoformat()


@pytest.mark.parametrize("creator_id", [True, 0, -1, "101"])
def test_create_rejects_invalid_creator_before_mongo(monkeypatch, creator_id):
    collection = FakeCollection()
    install(monkeypatch, collection, datetime(2026, 8, 11, 12, 0, 0))

    with pytest.raises(ValueError):
        session.create_durable_battle(
            battle_id="battle-1",
            creator_id=creator_id,
            creator_name="Creator",
            questions=[deepcopy(QUESTION)],
        )
    assert collection.inserted == []
