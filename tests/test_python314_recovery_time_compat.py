import warnings
from pathlib import Path

import legacy_session_recovery as recovery


ROOT = Path(__file__).resolve().parents[1]


def test_recovery_epoch_conversion_preserves_naive_utc_semantics_without_deprecation():
    session = {
        "start_time": 0,
        "answered_questions": [{"ts": "1970-01-01T00:00:05"}],
    }

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        assert recovery.persisted_result_time_seconds(session) == 5.0
        assert recovery.persisted_completed_at(session) == "1970-01-01T00:00:05"


def test_recovery_authority_does_not_use_removed_utc_constructor():
    source = (ROOT / "legacy_session_recovery.py").read_text(encoding="utf-8")

    assert "datetime.utcfromtimestamp" not in source
    assert "datetime.fromtimestamp(started_epoch, UTC).replace(tzinfo=None)" in source
