from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_FILES = (
    "tests/test_legacy_restart_policy.py",
    "tests/test_result_store.py",
    "tests/test_runtime_security_reconciliation.py",
    "tests/test_weekly_result_recovery.py",
)


def test_python314_timestamp_fixtures_use_supported_utc_constructors():
    for relative in FIXTURE_FILES:
        source = (ROOT / relative).read_text(encoding="utf-8")
        assert "datetime.utcnow(" not in source, relative
        assert "datetime.utcfromtimestamp(" not in source, relative
