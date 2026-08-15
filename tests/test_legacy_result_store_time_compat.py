from datetime import UTC, datetime
import warnings

import legacy_result_store as store


def test_numeric_completion_timestamp_uses_utc_without_deprecation():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        parsed = store._parse_completed_at(1704067200)

    assert parsed == datetime(2024, 1, 1, 0, 0, 0)
    assert parsed.tzinfo is None
    assert store.result_day(1704067200) == "2024-01-01"
    assert store.result_week_id(1704067200) == "2024-W01"


def test_aware_datetime_still_normalizes_to_naive_utc():
    value = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    parsed = store._parse_completed_at(value)

    assert parsed == datetime(2024, 1, 1, 0, 0, 0)
    assert parsed.tzinfo is None
