import inspect
import json

import pytest
from pymongo.errors import AutoReconnect

import legacy_session_access as legacy_access
import scripts.check_session_unique_indexes as preflight
import web_api.db_hardening as miniapp_hardening


class _Admin:
    def __init__(self, owner):
        self.owner = owner

    def command(self, name):
        self.owner.commands.append(name)
        return {"ok": 1}


class _Collection:
    def __init__(self, owner, name):
        self.owner = owner
        self.name = name

    def index_information(self):
        if self.owner.error is not None:
            raise self.owner.error
        self.owner.collections.append(self.name)
        return self.owner.indexes[self.name]


class _Database:
    def __init__(self, owner):
        self.owner = owner

    def __getitem__(self, name):
        return _Collection(self.owner, name)


class _Client:
    def __init__(self, *, indexes, error=None):
        self.indexes = indexes
        self.error = error
        self.commands = []
        self.database_name = None
        self.collections = []
        self.closed = False
        self.admin = _Admin(self)

    def __getitem__(self, name):
        self.database_name = name
        return _Database(self)

    def close(self):
        self.closed = True


def _safe_indexes():
    return {
        "quiz_sessions": {
            legacy_access.ACTIVE_SESSION_INDEX: {
                "key": [("user_id", 1)],
                "unique": True,
                "partialFilterExpression": legacy_access.ACTIVE_SESSION_FILTER,
            }
        },
        "miniapp_sessions": {
            miniapp_hardening.UNIQUE_ACTIVE_NAME: {
                "key": [("user_id", 1)],
                "unique": True,
                "partialFilterExpression": miniapp_hardening.OPEN_FILTER,
            }
        },
    }


def test_preflight_contract_matches_runtime_session_guards():
    legacy_spec, miniapp_spec = preflight.EXPECTED
    assert legacy_spec == (
        "quiz_sessions",
        legacy_access.ACTIVE_SESSION_INDEX,
        [("user_id", 1)],
        True,
        legacy_access.ACTIVE_SESSION_FILTER,
    )
    assert miniapp_spec == (
        "miniapp_sessions",
        miniapp_hardening.UNIQUE_ACTIVE_NAME,
        [("user_id", 1)],
        True,
        miniapp_hardening.OPEN_FILTER,
    )


def test_preflight_source_is_read_only_and_skips_application_bootstrap():
    source = inspect.getsource(preflight)
    assert "import database" not in source
    assert "import web_api" not in source
    assert "import legacy_session_access" not in source
    for forbidden in (
        ".create_index(",
        ".drop_index(",
        ".insert_one(",
        ".update_one(",
        ".update_many(",
        ".delete_one(",
        ".delete_many(",
        ".find_one_and_update(",
    ):
        assert forbidden not in source


def test_preflight_reads_both_session_collections_and_closes_client(monkeypatch):
    indexes = _safe_indexes()
    client = _Client(indexes=indexes)
    created = {}

    def mongo_client(url, **kwargs):
        created["url"] = url
        created["kwargs"] = kwargs
        return client

    monkeypatch.setenv("MONGO_URL", "mongodb://example.invalid")
    monkeypatch.setattr(preflight, "MongoClient", mongo_client)

    assert preflight._load_index_information() == indexes
    assert created == {
        "url": "mongodb://example.invalid",
        "kwargs": {"serverSelectionTimeoutMS": 5000},
    }
    assert client.commands == ["ping"]
    assert client.database_name == "bible_bot_db"
    assert client.collections == ["quiz_sessions", "miniapp_sessions"]
    assert client.closed is True


def test_cli_reports_missing_and_incompatible_unique_contracts(monkeypatch, capsys):
    indexes = _safe_indexes()
    indexes["quiz_sessions"] = {}
    indexes["miniapp_sessions"][miniapp_hardening.UNIQUE_ACTIVE_NAME] = {
        "key": [("chat_id", 1)],
        "unique": False,
        "partialFilterExpression": {"status": "in_progress"},
    }
    monkeypatch.setattr(preflight, "_load_index_information", lambda: indexes)

    assert preflight.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["error"] == "session_unique_indexes_unsafe"
    errors = {(item["collection"], item["error"]) for item in payload["problems"]}
    assert ("quiz_sessions", "unique_session_index_missing") in errors
    assert ("miniapp_sessions", "unique_session_index_wrong_key") in errors
    assert ("miniapp_sessions", "unique_session_index_not_unique") in errors
    assert ("miniapp_sessions", "unique_session_index_wrong_filter") in errors


def test_cli_is_zero_for_exact_unique_contracts(monkeypatch, capsys):
    monkeypatch.setattr(preflight, "_load_index_information", _safe_indexes)

    assert preflight.main() == 0
    assert json.loads(capsys.readouterr().out) == {
        "ok": True,
        "session_unique_indexes": "safe",
    }


def test_preflight_requires_mongo_url_before_connect(monkeypatch):
    monkeypatch.delenv("MONGO_URL", raising=False)
    monkeypatch.setattr(
        preflight,
        "MongoClient",
        lambda *_args, **_kwargs: pytest.fail("must fail before connecting"),
    )

    with pytest.raises(preflight.SessionUniqueIndexPreflightUnavailable, match="MONGO_URL"):
        preflight._load_index_information()


def test_preflight_closes_client_on_mongo_error(monkeypatch):
    client = _Client(indexes=_safe_indexes(), error=AutoReconnect("mongo unavailable"))
    monkeypatch.setenv("MONGO_URL", "mongodb://example.invalid")
    monkeypatch.setattr(preflight, "MongoClient", lambda *_args, **_kwargs: client)

    with pytest.raises(
        preflight.SessionUniqueIndexPreflightUnavailable,
        match="unique-index lookup failed",
    ):
        preflight._load_index_information()
    assert client.closed is True


def test_cli_unavailable_is_distinct_failure(monkeypatch, capsys):
    def unavailable():
        raise preflight.SessionUniqueIndexPreflightUnavailable("mongo down")

    monkeypatch.setattr(preflight, "_load_index_information", unavailable)

    assert preflight.main() == 2
    assert json.loads(capsys.readouterr().err) == {
        "ok": False,
        "error": "preflight_unavailable",
        "detail": "mongo down",
    }
