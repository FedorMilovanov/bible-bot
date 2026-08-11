import inspect

import pytest
from pymongo.errors import AutoReconnect

from scripts import check_result_storage_growth as preflight


class _Admin:
    def __init__(self, owner):
        self.owner = owner

    def command(self, name):
        self.owner.commands.append(name)
        if name == "hello":
            return dict(self.owner.hello)
        return {"ok": 1}


class _Collection:
    def __init__(self, owner):
        self.owner = owner

    def aggregate(self, pipeline, **kwargs):
        self.owner.pipeline = pipeline
        self.owner.aggregate_kwargs = kwargs
        if self.owner.aggregate_error is not None:
            raise self.owner.aggregate_error
        return list(self.owner.rows)


class _Database:
    def __init__(self, owner):
        self.owner = owner

    def __getitem__(self, name):
        self.owner.collection_name = name
        return _Collection(self.owner)


class _Client:
    def __init__(self, *, hello=None, rows=(), aggregate_error=None):
        self.hello = dict(hello or {})
        self.rows = list(rows)
        self.aggregate_error = aggregate_error
        self.commands = []
        self.database_name = None
        self.collection_name = None
        self.pipeline = None
        self.aggregate_kwargs = None
        self.closed = False
        self.admin = _Admin(self)

    def __getitem__(self, name):
        self.database_name = name
        return _Database(self)

    def close(self):
        self.closed = True


def _row(*, size=1024, result_receipts=3, malformed=False):
    value = -1 if malformed else 0
    return {
        "_id": "42",
        "bson_size": size,
        "result_receipts": result_receipts,
        "daily_bonus_receipts": value,
        "normal_bonus_owners": 0,
        "random20_bonus_receipts": 0,
        "hardcore20_bonus_receipts": 0,
        "random20_bonus_owners": 0,
        "hardcore20_bonus_owners": 0,
    }


def test_preflight_source_never_imports_application_database_bootstrap():
    source = inspect.getsource(preflight)

    assert "import database" not in source
    assert "legacy_result_store" not in source
    assert "legacy_bonus_store" not in source


@pytest.mark.parametrize(
    ("hello", "expected", "transaction_candidate"),
    [
        ({"setName": "rs0"}, "replica_set", True),
        ({"msg": "isdbgrid"}, "sharded", True),
        ({"isWritablePrimary": True}, "standalone", False),
        (None, "unknown", False),
    ],
)
def test_topology_classification(hello, expected, transaction_candidate):
    assert preflight._topology(hello) == expected
    assert (expected in {"replica_set", "sharded"}) is transaction_candidate


def test_storage_snapshot_reads_size_counts_and_closes_client(monkeypatch):
    client = _Client(
        hello={"setName": "rs0"},
        rows=[_row(size=2 * 1024 * 1024, result_receipts=120)],
    )
    created = {}

    def mongo_client(url, **kwargs):
        created["url"] = url
        created["kwargs"] = kwargs
        return client

    monkeypatch.setenv("MONGO_URL", "mongodb://example.invalid")
    monkeypatch.setattr(preflight, "MongoClient", mongo_client)

    snapshot = preflight._load_storage_snapshot(limit=7)

    assert created == {
        "url": "mongodb://example.invalid",
        "kwargs": {"serverSelectionTimeoutMS": 5000},
    }
    assert client.commands == ["ping", "hello"]
    assert client.database_name == "bible_bot_db"
    assert client.collection_name == "leaderboard"
    assert client.aggregate_kwargs == {"allowDiskUse": False}
    assert client.pipeline[-1] == {"$limit": 7}
    projection = client.pipeline[0]["$project"]
    assert projection["bson_size"] == {"$bsonSize": "$$ROOT"}
    assert set(preflight._RECEIPT_MAPS).issubset(projection)
    assert snapshot["topology"] == "replica_set"
    assert snapshot["transaction_topology_candidate"] is True
    assert snapshot["users"][0]["result_receipts"] == 120
    assert snapshot["users"][0]["warning"] is False
    assert snapshot["malformed_receipt_maps"] == 0
    assert client.closed is True


def test_storage_snapshot_surfaces_warning_and_malformed_map(monkeypatch):
    client = _Client(
        rows=[
            _row(size=preflight._WARNING_BYTES, malformed=True),
        ]
    )
    monkeypatch.setenv("MONGO_URL", "mongodb://example.invalid")
    monkeypatch.setattr(preflight, "MongoClient", lambda *_args, **_kwargs: client)

    snapshot = preflight._load_storage_snapshot()

    assert snapshot["users"][0]["warning"] is True
    assert snapshot["users"][0]["daily_bonus_receipts"] == -1
    assert snapshot["malformed_receipt_maps"] == 1
    assert client.closed is True


def test_storage_snapshot_closes_client_on_mongo_error(monkeypatch):
    client = _Client(aggregate_error=AutoReconnect("mongo unavailable"))
    monkeypatch.setenv("MONGO_URL", "mongodb://example.invalid")
    monkeypatch.setattr(preflight, "MongoClient", lambda *_args, **_kwargs: client)

    with pytest.raises(
        preflight.ResultStoragePreflightUnavailable,
        match="growth preflight failed",
    ):
        preflight._load_storage_snapshot()

    assert client.closed is True


def test_missing_url_and_invalid_limit_fail_before_client(monkeypatch):
    monkeypatch.delenv("MONGO_URL", raising=False)
    monkeypatch.setattr(
        preflight,
        "MongoClient",
        lambda *_args, **_kwargs: pytest.fail("MongoClient must not be created"),
    )

    with pytest.raises(preflight.ResultStoragePreflightUnavailable, match="MONGO_URL"):
        preflight._load_storage_snapshot()

    monkeypatch.setenv("MONGO_URL", "mongodb://example.invalid")
    for value in (0, -1, True, 1.5):
        with pytest.raises(ValueError, match="positive integer"):
            preflight._load_storage_snapshot(limit=value)
