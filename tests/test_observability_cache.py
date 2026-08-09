import database
from web_api import routes


def test_database_readiness_is_cached_briefly(monkeypatch):
    calls = []
    routes._DB_READY_CACHE.clear()

    def check_db_connection():
        calls.append(True)
        return True

    monkeypatch.setattr(database, "check_db_connection", check_db_connection)

    assert routes._database_ready() is True
    assert routes._database_ready() is True
    assert len(calls) == 1


def test_total_user_count_is_cached_briefly(monkeypatch):
    calls = []
    routes._TOTAL_USERS_CACHE.clear()

    def get_total_users():
        calls.append(True)
        return 42

    monkeypatch.setattr(database, "get_total_users", get_total_users)

    assert routes._total_users() == 42
    assert routes._total_users() == 42
    assert len(calls) == 1
