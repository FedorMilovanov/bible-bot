from datetime import UTC, datetime
import inspect
import warnings

from web_api import result_store


def test_result_store_clock_preserves_naive_utc_without_deprecation():
    before = datetime.now(UTC).replace(tzinfo=None)

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        value = result_store._now_utc_naive()

    after = datetime.now(UTC).replace(tzinfo=None)

    assert value.tzinfo is None
    assert before <= value <= after


def test_result_store_date_and_week_helpers_keep_existing_utc_formats(monkeypatch):
    fixed = datetime(2026, 1, 1, 12, 0, 0)
    monkeypatch.setattr(result_store, "_now_utc_naive", lambda: fixed)

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        assert result_store._today_utc() == "2026-01-01"
        assert result_store._week_id_utc() == "2026-W01"

    assert result_store._week_id_utc(datetime(2025, 12, 31, 23, 59, 59)) == "2026-W01"


def test_result_store_module_no_longer_calls_deprecated_utcnow():
    source = inspect.getsource(result_store)

    assert ".utcnow(" not in source
