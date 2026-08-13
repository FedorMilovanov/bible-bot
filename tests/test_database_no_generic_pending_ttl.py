from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATABASE = (ROOT / "database.py").read_text(encoding="utf-8")
SESSION_RETENTION = (ROOT / "legacy_session_retention.py").read_text(encoding="utf-8")
DELIVERY_RETENTION = (ROOT / "legacy_delivery_retention.py").read_text(encoding="utf-8")


def test_database_does_not_recreate_generic_pending_evidence_ttls():
    for obsolete_name in (
        'name="ttl_updated_at"',
        'name="ttl_battles_created_at"',
        'name="ttl_reports_created_at"',
    ):
        assert obsolete_name not in DATABASE

    # Weekly leaderboard expiry is unrelated to pending quiz/report/battle
    # recovery evidence and must not be removed by this migration cleanup.
    assert 'name="ttl_weekly_lb_updated_at"' in DATABASE


def test_state_aware_migrations_still_remove_indexes_from_older_deployments():
    # New startup no longer creates the unsafe indexes, but an upgraded MongoDB
    # may already contain them from an older release. The migration layer must
    # continue to drop those names before ensuring partial state-aware TTLs.
    assert '"ttl_updated_at"' in SESSION_RETENTION
    assert '"ttl_battles_created_at"' in DELIVERY_RETENTION
    assert '"ttl_reports_created_at"' in DELIVERY_RETENTION

    assert 'name=_TERMINAL_TTL_NAME' in SESSION_RETENTION
    assert 'target_name=_BATTLE_DELIVERED_TTL' in DELIVERY_RETENTION
    assert 'target_name=_REPORT_DELIVERED_TTL' in DELIVERY_RETENTION
