import inspect
import json

import pytest
from pymongo.errors import AutoReconnect

import scripts.check_miniapp_session_duplicates as preflight
import web_api.db_hardening as hardening


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
        if self.owner.error is not None:
            raise self.owner.error
        return list(self.owner.rows)


class _Database:
    def __init__(self, owner):
        self.owner = owner

    def __getitem__(self, name):
        self.owner.collection_name = name
        return _Collection(self.owner)


class _Client:
    def __init__(self, *, rows=(), error=None):
        self.rows = list(rows)
        self.error = error
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


def test_preflight_status_contract_matches_miniapp_unique_index():
    assert preflight._OPEN_STATUSES == hardening.OPEN_STATUSES


def test_preflight_source_is_read_only_and_skips_application_bootstrap():
    source = inspect.getsource(preflight)
    assert "import database" not in source
    assert "import web_api" not in source
    for forbidden in (
        ".insert_one(",
        ".update_one(",
        ".update_many(",
        ".delete_one(",
        ".delete_many(",
        ".create_index(",
        ".drop_index(",
        ".replace_one(",
        ".find_one_and_update(",
    ):
        assert forbidden not in source


def test_preflight_reads_exact_open_statuses_and_closes_client(monkeypatch):
    client = _Client(rows=[{
        "_id": "42",
        "count": 2,
        "session_ids": ["session-a", "session-b"],
    }])
    created = {}

    def mongo_client(url, **kwargs):
        created["url"] = url
        created["kwargs"] = kwargs
        return client

    monkeypatch.setenv("MONGO_URL", "mongodb://example.invalid")
    monkeypatch.setattr(preflight, "MongoClient", mongo_client)

    duplicates = preflight._load_duplicates(limit=7)

    assert created == {
        "url": "mongodb://example.invalid",
        "kwargs": {"serverSelectionTimeoutMS": 5000},
    }
    assert client.commands == ["ping"]
    assert client.database_name == "bible_bot_db"
    assert client.collection_name == "miniapp_sessions"
    assert client.pipeline == [
        {"$match": {"status": {"$in": list(hardening.OPEN_STATUSES)}}},
        {
            "$group": {
                "_id": "$user_id",
                "session_ids": {"$push": "$_id"},
                "count": {"$sum": 1},
            }
        },
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1, "_id": 1}},
        {"$limit": 7},
    ]
    assert duplicates == [
        {
            "user_id": "42",
            "count": 2,
            "session_ids": ["session-a", "session-b"],
        }
    ]
    assert client.closed is True


def test_preflight_fails_before_connect_without_mongo_url(monkeypatch):
    monkeypatch.delenv("MONGO_URL", raising=False)
    monkeypatch.setattr(
        preflight,
        "MongoClient",
        lambda *_args, **_kwargs: pytest.fail("must fail before connecting"),
    )

    with pytest.raises(preflight.MiniAppDuplicatePreflightUnavailable, match="MONGO_URL"):
        preflight._load_duplicates()


@pytest.mark.parametrize("limit", [0, -1, True, 1.5, "5"])
def test_preflight_rejects_invalid_limit_before_connect(monkeypatch, limit):
    monkeypatch.setenv("MONGO_URL", "mongodb://example.invalid")
    monkeypatch.setattr(
        preflight,
        "MongoClient",
        lambda *_args, **_kwargs: pytest.fail("must validate before connecting"),
    )

    with pytest.raises(ValueError, match="positive integer"):
        preflight._load_duplicates(limit=limit)


def test_preflight_closes_client_on_mongo_error(monkeypatch):
    client = _Client(error=AutoReconnect("mongo unavailable"))
    monkeypatch.setenv("MONGO_URL", "mongodb://example.invalid")
    monkeypatch.setattr(preflight, "MongoClient", lambda *_args, **_kwargs: client)

    with pytest.raises(
        preflight.MiniAppDuplicatePreflightUnavailable,
        match="duplicate-session preflight failed",
    ):
        preflight._load_duplicates()
    assert client.closed is True


def test_cli_exit_codes_distinguish_safe_duplicate_and_unavailable(monkeypatch, capsys):
    monkeypatch.setattr(preflight, "_load_duplicates", lambda: [])
    assert preflight.main() == 0
    assert json.loads(capsys.readouterr().out) == {
        "ok": True,
        "duplicate_open_miniapp_sessions": 0,
    }

    monkeypatch.setattr(
        preflight,
        "_load_duplicates",
        lambda: [{"user_id": "42", "count": 2, "session_ids": ["a", "b"]}],
    )
    assert preflight.main() == 1
    duplicate_payload = json.loads(capsys.readouterr().out)
    assert duplicate_payload["error"] == "duplicate_open_miniapp_sessions"
    assert duplicate_payload["count"] == 1

    def unavailable():
        raise preflight.MiniAppDuplicatePreflightUnavailable("mongo down")

    monkeypatch.setattr(preflight, "_load_duplicates", unavailable)
    assert preflight.main() == 2
    assert json.loads(capsys.readouterr().err) == {
        "ok": False,
        "error": "preflight_unavailable",
        "detail": "mongo down",
    }
