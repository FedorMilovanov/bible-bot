from datetime import UTC, datetime
import warnings

from web_api import quiz


def test_miniapp_quiz_clock_preserves_naive_utc_without_deprecation():
    before = datetime.now(UTC).replace(tzinfo=None)

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        value = quiz._now()

    after = datetime.now(UTC).replace(tzinfo=None)

    assert value.tzinfo is None
    assert before <= value <= after


def test_miniapp_quiz_clock_no_longer_uses_deprecated_utcnow():
    source = quiz._now.__code__.co_names

    assert "utcnow" not in source
