from datetime import UTC, datetime
import inspect
import warnings

import database


def test_database_clock_preserves_naive_utc_without_deprecation():
    before = datetime.now(UTC).replace(tzinfo=None)

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        value = database._now_utc()

    after = datetime.now(UTC).replace(tzinfo=None)

    assert value.tzinfo is None
    assert before <= value <= after


def test_database_today_and_days_playing_keep_existing_utc_semantics(monkeypatch):
    fixed = datetime(2026, 8, 15, 12, 0, 0)
    monkeypatch.setattr(database, "_now_utc", lambda: fixed)

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        assert database._today_utc() == "2026-08-15"
        assert database.calculate_days_playing("2026-08-15") == 1
        assert database.calculate_days_playing("2026-08-14") == 2
        assert database.calculate_days_playing("not-a-date") == 1


def test_database_module_no_longer_calls_deprecated_utcnow():
    source = inspect.getsource(database)

    assert ".utcnow(" not in source
