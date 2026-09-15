from contextlib import contextmanager

import database


class _FakeAdmin:
    def command(self, name):
        assert name == "ping"
        return {"ok": 1}


class _FakeCluster:
    admin = _FakeAdmin()


def test_check_db_connection_uses_health_only_csot(monkeypatch):
    observed = []

    @contextmanager
    def fake_timeout(seconds):
        observed.append(seconds)
        yield

    monkeypatch.setattr(database, "collection", object())
    monkeypatch.setattr(database, "cluster", _FakeCluster(), raising=False)
    monkeypatch.setattr(database, "pymongo_timeout", fake_timeout)

    assert database.check_db_connection() is True
    assert observed == [database._DB_HEALTH_TIMEOUT_SECONDS]
    assert 0 < database._DB_HEALTH_TIMEOUT_SECONDS < 5


def test_check_db_connection_fails_closed_when_health_deadline_expires(monkeypatch):
    @contextmanager
    def expired_timeout(_seconds):
        raise TimeoutError("health deadline expired")
        yield

    monkeypatch.setattr(database, "collection", object())
    monkeypatch.setattr(database, "pymongo_timeout", expired_timeout)

    assert database.check_db_connection() is False
