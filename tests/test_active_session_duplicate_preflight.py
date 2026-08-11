import inspect

import pytest
from pymongo.errors import AutoReconnect

from scripts import check_active_session_duplicates as preflight


class _Admin:
    def __init__(self, owner):
        self.owner = owner

    def command(self, name):
        self.owner.commands.append(name)
        return {"ok": 1}


class _Collection:
    def __init__(self, owner):
        self.owner = owner

    def aggregate(self, pipeline):
        self.owner.pipeline = pipeline
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
    def __init__(self, rows=(), aggregate_error=None):
        self.rows = list(rows)
        self.aggregate_error = aggregate_error
        self.commands = []
        self.database_name = None
        self.collection_name = None
        self.pipeline = None
        self.closed = False
        self.admin = _Admin(self)

    def __getitem__(self, name):
        self.database_name = name
        return _Database(self)

    def close(self):
        self.closed = True


def test_preflight_source_never_imports_application_database_bootstrap():
    source = inspect.getsource(preflight)

    assert "import database" not in source
    assert "legacy_session_access" not in source


def test_load_duplicates_is_read_only_and_closes_client(monkeypatch):
    client = _Client(
        rows=[
            {"_id": "42", "count": 3},
            {"_id": "99", "count": 2},
        ]
    )
    created = {}

    def mongo_client(url, **kwargs):
        created["url"] = url
        created["kwargs"] = kwargs
        return client

    monkeypatch.setenv("MONGO_URL", "mongodb://example.invalid")
    monkeypatch.setattr(preflight, "MongoClient", mongo_client)

    result = preflight._load_duplicates(limit=7)

    assert result == [
        {"user_id": "42", "count": 3},
        {"user_id": "99", "count": 2},
    ]
    assert created == {
        "url": "mongodb://example.invalid",
        "kwargs": {"serverSelectionTimeoutMS": 5000},
    }
    assert client.commands == ["ping"]
    assert client.database_name == "bible_bot_db"
    assert client.collection_name == "quiz_sessions"
    assert client.pipeline == [
        {"$match": {"status": "in_progress"}},
        {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1, "_id": 1}},
        {"$limit": 7},
    ]
    assert client.closed is True


def test_load_duplicates_closes_client_on_mongo_error(monkeypatch):
    client = _Client(aggregate_error=AutoReconnect("mongo unavailable"))
    monkeypatch.setenv("MONGO_URL", "mongodb://example.invalid")
    monkeypatch.setattr(preflight, "MongoClient", lambda *_args, **_kwargs: client)

    with pytest.raises(
        preflight.DuplicateSessionPreflightUnavailable,
        match="duplicate preflight failed",
    ):
        preflight._load_duplicates()

    assert client.closed is True


def test_missing_mongo_url_fails_before_client_creation(monkeypatch):
    monkeypatch.delenv("MONGO_URL", raising=False)
    monkeypatch.setattr(
        preflight,
        "MongoClient",
        lambda *_args, **_kwargs: pytest.fail("MongoClient must not be created"),
    )

    with pytest.raises(
        preflight.DuplicateSessionPreflightUnavailable,
        match="MONGO_URL",
    ):
        preflight._load_duplicates()


@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_load_duplicates_rejects_invalid_limit_before_store(monkeypatch, value):
    monkeypatch.setenv("MONGO_URL", "mongodb://example.invalid")
    monkeypatch.setattr(
        preflight,
        "MongoClient",
        lambda *_args, **_kwargs: pytest.fail("MongoClient must not be created"),
    )

    with pytest.raises(ValueError, match="positive integer"):
        preflight._load_duplicates(limit=value)
