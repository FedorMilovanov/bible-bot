from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from pymongo.errors import DuplicateKeyError

import broadcast_integrity as integrity


class Result:
    def __init__(self, modified_count=0):
        self.modified_count = modified_count


class Cursor:
    def __init__(self, docs):
        self.docs = list(docs)

    def sort(self, key, direction):
        del direction
        self.docs.sort(key=lambda item: item.get(key))
        return self

    def limit(self, limit):
        self.docs = self.docs[:limit]
        return self

    def __iter__(self):
        return iter(deepcopy(self.docs))


def _matches(doc, query):
    for key, expected in query.items():
        if key == "$or":
            if not any(_matches(doc, branch) for branch in expected):
                return False
            continue
        actual = doc.get(key)
        if isinstance(expected, dict):
            if "$ne" in expected and actual == expected["$ne"]:
                return False
            if "$exists" in expected and (key in doc) != expected["$exists"]:
                return False
            if "$lte" in expected and (actual is None or actual > expected["$lte"]):
                return False
            continue
        if actual != expected:
            return False
    return True


def _apply(doc, update):
    for key, value in update.get("$set", {}).items():
        doc[key] = value
    for key, value in update.get("$setOnInsert", {}).items():
        doc.setdefault(key, value)
    for key, value in update.get("$inc", {}).items():
        doc[key] = doc.get(key, 0) + value
    for key in update.get("$unset", {}):
        doc.pop(key, None)


class Broadcasts:
    def __init__(self):
        self.docs = {}
        self.indexes = []

    def create_index(self, keys, **kwargs):
        self.indexes.append((keys, kwargs))
        return kwargs.get("name")

    def insert_one(self, doc):
        if doc["_id"] in self.docs:
            raise DuplicateKeyError("duplicate")
        self.docs[doc["_id"]] = deepcopy(doc)
        return SimpleNamespace(inserted_id=doc["_id"])

    def find_one(self, query, projection=None):
        for doc in self.docs.values():
            if _matches(doc, query):
                if projection is None:
                    return deepcopy(doc)
                projected = {"_id": doc["_id"]}
                for key, include in projection.items():
                    if include and key in doc:
                        projected[key] = deepcopy(doc[key])
                return projected
        return None

    def update_one(self, query, update):
        for doc in self.docs.values():
            if _matches(doc, query):
                _apply(doc, update)
                return Result(1)
        return Result(0)

    def find(self, query):
        return Cursor(doc for doc in self.docs.values() if _matches(doc, query))


class Deliveries:
    def __init__(self):
        self.docs = {}
        self.indexes = []

    def create_index(self, keys, **kwargs):
        self.indexes.append((keys, kwargs))
        return kwargs.get("name")

    def bulk_write(self, operations, ordered=False):
        assert ordered is False
        for operation in operations:
            key = operation._filter["_id"]
            if key not in self.docs:
                doc = {}
                _apply(doc, operation._doc)
                self.docs[key] = doc
        return SimpleNamespace(upserted_count=0)

    def find_one_and_update(self, query, update, sort=None, return_document=None):
        del sort, return_document
        for key in sorted(self.docs):
            doc = self.docs[key]
            if _matches(doc, query):
                _apply(doc, update)
                return deepcopy(doc)
        return None

    def update_one(self, query, update):
        for doc in self.docs.values():
            if _matches(doc, query):
                _apply(doc, update)
                return Result(1)
        return Result(0)

    def find_one(self, query, projection=None):
        for doc in self.docs.values():
            if _matches(doc, query):
                if projection is None:
                    return deepcopy(doc)
                projected = {"_id": doc["_id"]}
                for key, include in projection.items():
                    if include and key in doc:
                        projected[key] = deepcopy(doc[key])
                return projected
        return None

    def count_documents(self, query, limit=0):
        count = sum(1 for doc in self.docs.values() if _matches(doc, query))
        return min(count, limit) if limit else count

    def update_many(self, query, update):
        count = 0
        for doc in self.docs.values():
            if _matches(doc, query):
                _apply(doc, update)
                count += 1
        return Result(count)


def install(monkeypatch):
    now = datetime(2026, 8, 12, 6, 0, 0)
    database = SimpleNamespace(_now_utc=lambda: now)
    broadcasts = Broadcasts()
    deliveries = Deliveries()
    monkeypatch.setattr(
        integrity,
        "_collections",
        lambda: (database, broadcasts, deliveries),
    )
    return broadcasts, deliveries


def test_broadcast_id_is_stable_and_validated():
    assert integrity.broadcast_id_for_update(42) == "telegram_update_42"
    for invalid in (True, -1, "42"):
        with pytest.raises(ValueError):
            integrity.broadcast_id_for_update(invalid)


def test_accept_broadcast_is_idempotent_before_recoverable_fanout(monkeypatch):
    broadcasts, deliveries = install(monkeypatch)

    first, created = integrity.accept_broadcast_once(
        broadcast_id="telegram_update_42",
        admin_id=1,
        admin_chat_id=1,
        text="Hello",
        recipient_ids=[3, 2, 3, "4"],
    )
    replay, replay_created = integrity.accept_broadcast_once(
        broadcast_id="telegram_update_42",
        admin_id=1,
        admin_chat_id=1,
        text="Hello",
        recipient_ids=[4, 3, 2],
    )

    assert created is True
    assert replay_created is False
    assert first["fanout_ready"] is False
    assert replay["recipient_ids"] == ["2", "3", "4"]
    assert deliveries.docs == {}
    assert broadcasts.docs["telegram_update_42"]["fanout_ready"] is False

    recovered = integrity.ensure_broadcast_fanout(replay)
    assert recovered["fanout_ready"] is True
    assert set(deliveries.docs) == {
        "telegram_update_42:2",
        "telegram_update_42:3",
        "telegram_update_42:4",
    }


def test_replayed_update_cannot_change_immutable_broadcast_content(monkeypatch):
    install(monkeypatch)
    integrity.accept_broadcast_once(
        broadcast_id="telegram_update_42",
        admin_id=1,
        admin_chat_id=1,
        text="Original",
        recipient_ids=[2],
    )

    with pytest.raises(integrity.BroadcastStoreUnavailable, match="different immutable content"):
        integrity.accept_broadcast_once(
            broadcast_id="telegram_update_42",
            admin_id=1,
            admin_chat_id=1,
            text="Changed",
            recipient_ids=[2],
        )


def test_delivery_lease_ack_and_completion_are_restart_safe(monkeypatch):
    broadcasts, deliveries = install(monkeypatch)
    parent, _created = integrity.accept_broadcast_once(
        broadcast_id="telegram_update_50",
        admin_id=1,
        admin_chat_id=1,
        text="News",
        recipient_ids=[10],
    )
    integrity.ensure_broadcast_fanout(parent)

    claimed = integrity.claim_next_broadcast_delivery(broadcast_id="telegram_update_50")
    assert claimed is not None
    assert claimed["user_id"] == "10"
    assert claimed["attempts"] == 1
    assert integrity.claim_next_broadcast_delivery(broadcast_id="telegram_update_50") is None

    assert integrity.mark_broadcast_delivery_delivered(
        claimed["_id"], claimed["claim_token"]
    ) is True
    state = integrity.sync_broadcast_completion("telegram_update_50")

    assert state == {"completed": True, "delivered": 1, "failed": 0}
    assert broadcasts.docs["telegram_update_50"]["completed"] is True
    assert "retention_at_dt" in broadcasts.docs["telegram_update_50"]
    assert "retention_at_dt" in deliveries.docs[claimed["_id"]]


def test_rate_limit_deferral_survives_restart_and_blocks_early_reclaim(monkeypatch):
    broadcasts, deliveries = install(monkeypatch)
    parent, _created = integrity.accept_broadcast_once(
        broadcast_id="telegram_update_55",
        admin_id=1,
        admin_chat_id=1,
        text="News",
        recipient_ids=[15],
    )
    integrity.ensure_broadcast_fanout(parent)

    now_ref = {"value": datetime(2026, 8, 12, 6, 0, 0)}
    database = SimpleNamespace(_now_utc=lambda: now_ref["value"])
    monkeypatch.setattr(
        integrity,
        "_collections",
        lambda: (database, broadcasts, deliveries),
    )

    claimed = integrity.claim_next_broadcast_delivery(broadcast_id="telegram_update_55")
    assert claimed is not None
    assert integrity.defer_broadcast_delivery(
        claimed["_id"],
        claimed["claim_token"],
        delay_seconds=300,
        error="RetryAfter",
    ) is True

    row = deliveries.docs[claimed["_id"]]
    assert "claim_token" not in row
    assert row["lease_until"] == now_ref["value"] + timedelta(seconds=300)
    assert integrity.claim_next_broadcast_delivery(broadcast_id="telegram_update_55") is None

    now_ref["value"] += timedelta(seconds=299)
    assert integrity.claim_next_broadcast_delivery(broadcast_id="telegram_update_55") is None

    now_ref["value"] += timedelta(seconds=2)
    reclaimed = integrity.claim_next_broadcast_delivery(broadcast_id="telegram_update_55")
    assert reclaimed is not None
    assert reclaimed["attempts"] == 2


def test_terminal_delivery_failure_completes_without_infinite_retry(monkeypatch):
    _broadcasts, deliveries = install(monkeypatch)
    parent, _created = integrity.accept_broadcast_once(
        broadcast_id="telegram_update_60",
        admin_id=1,
        admin_chat_id=1,
        text="News",
        recipient_ids=[20],
    )
    integrity.ensure_broadcast_fanout(parent)
    claimed = integrity.claim_next_broadcast_delivery(broadcast_id="telegram_update_60")
    assert claimed is not None

    assert integrity.mark_broadcast_delivery_terminal_failure(
        claimed["_id"],
        claimed["claim_token"],
        error="Forbidden",
    ) is True
    assert integrity.claim_next_broadcast_delivery(broadcast_id="telegram_update_60") is None
    assert deliveries.docs[claimed["_id"]]["delivered"] is False
    assert integrity.sync_broadcast_completion("telegram_update_60") == {
        "completed": True,
        "delivered": 0,
        "failed": 1,
    }


def test_index_bootstrap_includes_completion_scoped_retention(monkeypatch):
    broadcasts, deliveries = install(monkeypatch)
    integrity.ensure_broadcast_indexes()

    broadcast_names = {kwargs["name"] for _keys, kwargs in broadcasts.indexes}
    delivery_names = {kwargs["name"] for _keys, kwargs in deliveries.indexes}
    assert "ttl_broadcast_retention" in broadcast_names
    assert "ttl_broadcast_delivery_retention" in delivery_names
    assert "idx_broadcast_delivery_claim" in delivery_names
