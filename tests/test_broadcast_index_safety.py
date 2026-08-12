from copy import deepcopy
from types import SimpleNamespace

import pytest

import broadcast_index_safety as safety


class Collection:
    def __init__(self, indexes=None):
        self.indexes = deepcopy(indexes or {"_id_": {"key": [("_id", 1)]}})
        self.created = []

    def index_information(self):
        return deepcopy(self.indexes)

    def create_index(self, key, *, name, **options):
        self.created.append((name, deepcopy(key), deepcopy(options)))
        self.indexes[name] = {"key": deepcopy(key), **deepcopy(options)}
        return name


class DB:
    def __init__(self, broadcasts=None, deliveries=None):
        self.collections = {
            "broadcasts": broadcasts or Collection(),
            "broadcast_deliveries": deliveries or Collection(),
        }

    def __getitem__(self, name):
        return self.collections[name]


def install(monkeypatch, *, broadcasts=None, deliveries=None):
    db = DB(broadcasts=broadcasts, deliveries=deliveries)
    monkeypatch.setattr(safety, "_database", lambda: db)
    return db


def test_missing_indexes_are_created_then_reverified(monkeypatch):
    db = install(monkeypatch)

    safety.ensure_broadcast_indexes()

    broadcasts = db["broadcasts"]
    deliveries = db["broadcast_deliveries"]
    assert "ttl_broadcast_retention" in broadcasts.indexes
    assert broadcasts.indexes["ttl_broadcast_retention"] == {
        "key": [("retention_at_dt", 1)],
        "expireAfterSeconds": 7776000,
    }
    assert "ttl_broadcast_delivery_retention" in deliveries.indexes
    assert deliveries.indexes["ttl_broadcast_delivery_retention"] == {
        "key": [("retention_at_dt", 1)],
        "expireAfterSeconds": 7776000,
    }
    assert "idx_broadcast_pending" in broadcasts.indexes
    assert "idx_broadcast_delivery_claim" in deliveries.indexes
    assert "idx_broadcast_delivery_parent" in deliveries.indexes


def test_unrecognized_age_ttl_fails_before_any_index_write(monkeypatch):
    broadcasts = Collection(
        {
            "_id_": {"key": [("_id", 1)]},
            "ttl_old_created_at": {
                "key": [("created_at_dt", 1)],
                "expireAfterSeconds": 3600,
            },
        }
    )
    db = install(monkeypatch, broadcasts=broadcasts)

    with pytest.raises(
        safety.BroadcastIndexSafetyUnavailable,
        match="unrecognized TTL index ttl_old_created_at",
    ):
        safety.ensure_broadcast_indexes()

    assert db["broadcasts"].created == []
    assert db["broadcast_deliveries"].created == []


def test_incompatible_named_ttl_is_preserved_and_fails_closed(monkeypatch):
    deliveries = Collection(
        {
            "_id_": {"key": [("_id", 1)]},
            "ttl_broadcast_delivery_retention": {
                "key": [("created_at_dt", 1)],
                "expireAfterSeconds": 3600,
            },
        }
    )
    db = install(monkeypatch, deliveries=deliveries)
    before = deepcopy(deliveries.indexes)

    with pytest.raises(
        safety.BroadcastIndexSafetyUnavailable,
        match="TTL index ttl_broadcast_delivery_retention is incompatible",
    ):
        safety.ensure_broadcast_indexes()

    assert deliveries.indexes == before
    assert db["broadcasts"].created == []
    assert deliveries.created == []


def test_incompatible_lookup_index_fails_before_writes(monkeypatch):
    broadcasts = Collection(
        {
            "_id_": {"key": [("_id", 1)]},
            "idx_broadcast_pending": {
                "key": [("created_at_dt", 1)],
            },
        }
    )
    db = install(monkeypatch, broadcasts=broadcasts)

    with pytest.raises(
        safety.BroadcastIndexSafetyUnavailable,
        match="idx_broadcast_pending is incompatible",
    ):
        safety.ensure_broadcast_indexes()

    assert db["broadcasts"].created == []
    assert db["broadcast_deliveries"].created == []


def test_exact_existing_contract_performs_no_writes(monkeypatch):
    broadcasts = Collection(
        {
            "_id_": {"key": [("_id", 1)]},
            "ttl_broadcast_retention": {
                "key": [("retention_at_dt", 1)],
                "expireAfterSeconds": 7776000,
            },
            "idx_broadcast_pending": {
                "key": [("completed", 1), ("created_at_dt", 1)],
            },
        }
    )
    deliveries = Collection(
        {
            "_id_": {"key": [("_id", 1)]},
            "ttl_broadcast_delivery_retention": {
                "key": [("retention_at_dt", 1)],
                "expireAfterSeconds": 7776000,
            },
            "idx_broadcast_delivery_claim": {
                "key": [("done", 1), ("lease_until", 1), ("created_at_dt", 1)],
            },
            "idx_broadcast_delivery_parent": {
                "key": [("broadcast_id", 1), ("done", 1)],
            },
        }
    )
    db = install(monkeypatch, broadcasts=broadcasts, deliveries=deliveries)

    safety.ensure_broadcast_indexes()

    assert db["broadcasts"].created == []
    assert db["broadcast_deliveries"].created == []


def test_missing_database_is_distinct_fail_closed_boundary(monkeypatch):
    monkeypatch.setattr(
        safety,
        "_database",
        lambda: (_ for _ in ()).throw(
            safety.BroadcastIndexSafetyUnavailable("broadcast database is unavailable")
        ),
    )

    with pytest.raises(safety.BroadcastIndexSafetyUnavailable, match="database is unavailable"):
        safety.ensure_broadcast_indexes()
