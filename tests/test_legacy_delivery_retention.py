from copy import deepcopy

import pytest
from pymongo.errors import PyMongoError

import database
from legacy_delivery_retention import (
    DeliveryRetentionUnavailable,
    ensure_state_aware_delivery_ttl,
)


class FakeIndexes:
    def __init__(self, info=None, *, fail=False):
        self.info = deepcopy(info or {})
        self.fail = fail
        self.dropped = []
        self.created = []

    def index_information(self):
        if self.fail:
            raise PyMongoError("index lookup failed")
        return deepcopy(self.info)

    def drop_index(self, name):
        self.dropped.append(name)
        self.info.pop(name, None)

    def create_index(self, keys, **kwargs):
        self.created.append((deepcopy(keys), deepcopy(kwargs)))
        self.info[kwargs["name"]] = {
            "key": list(keys),
            "expireAfterSeconds": kwargs.get("expireAfterSeconds"),
            "partialFilterExpression": deepcopy(kwargs.get("partialFilterExpression")),
        }
        return kwargs["name"]


def test_migration_replaces_generic_battle_and_report_ttls(monkeypatch):
    battles = FakeIndexes(
        {
            "ttl_battles_created_at": {
                "key": [("created_at_dt", 1)],
                "expireAfterSeconds": 2592000,
            }
        }
    )
    reports = FakeIndexes(
        {
            "ttl_reports_created_at": {
                "key": [("created_at_dt", 1)],
                "expireAfterSeconds": 7776000,
            }
        }
    )
    monkeypatch.setattr(database, "battles_collection", battles)
    monkeypatch.setattr(database, "reports_collection", reports)

    assert ensure_state_aware_delivery_ttl() is True

    assert battles.dropped == ["ttl_battles_created_at"]
    assert battles.created == [
        (
            [("created_at_dt", 1)],
            {
                "expireAfterSeconds": 2592000,
                "partialFilterExpression": {
                    "status": "finalized",
                    "result_delivery.creator.delivered": True,
                    "result_delivery.opponent.delivered": True,
                },
                "name": "ttl_battles_delivered_created_at",
                "background": True,
            },
        )
    ]

    assert reports.dropped == ["ttl_reports_created_at"]
    assert reports.created == [
        (
            [("created_at_dt", 1)],
            {
                "expireAfterSeconds": 7776000,
                "partialFilterExpression": {"admin_delivered": True},
                "name": "ttl_reports_delivered_created_at",
                "background": True,
            },
        )
    ]


def test_matching_partial_ttls_are_idempotent(monkeypatch):
    battles = FakeIndexes(
        {
            "ttl_battles_delivered_created_at": {
                "key": [("created_at_dt", 1)],
                "expireAfterSeconds": 2592000,
                "partialFilterExpression": {
                    "status": "finalized",
                    "result_delivery.creator.delivered": True,
                    "result_delivery.opponent.delivered": True,
                },
            }
        }
    )
    reports = FakeIndexes(
        {
            "ttl_reports_delivered_created_at": {
                "key": [("created_at_dt", 1)],
                "expireAfterSeconds": 7776000,
                "partialFilterExpression": {"admin_delivered": True},
            }
        }
    )
    monkeypatch.setattr(database, "battles_collection", battles)
    monkeypatch.setattr(database, "reports_collection", reports)

    assert ensure_state_aware_delivery_ttl() is True
    assert battles.dropped == []
    assert battles.created == []
    assert reports.dropped == []
    assert reports.created == []


def test_wrong_target_options_are_replaced(monkeypatch):
    battles = FakeIndexes(
        {
            "ttl_battles_delivered_created_at": {
                "key": [("created_at_dt", 1)],
                "expireAfterSeconds": 60,
                "partialFilterExpression": {"status": "finalized"},
            }
        }
    )
    reports = FakeIndexes()
    monkeypatch.setattr(database, "battles_collection", battles)
    monkeypatch.setattr(database, "reports_collection", reports)

    assert ensure_state_aware_delivery_ttl() is True
    assert battles.dropped == ["ttl_battles_delivered_created_at"]
    assert len(battles.created) == 1
    assert len(reports.created) == 1


def test_absent_collections_are_explicit_noop(monkeypatch):
    monkeypatch.setattr(database, "battles_collection", None)
    monkeypatch.setattr(database, "reports_collection", None)

    assert ensure_state_aware_delivery_ttl() is False


def test_one_configured_collection_can_migrate_independently(monkeypatch):
    reports = FakeIndexes(
        {
            "ttl_reports_created_at": {
                "key": [("created_at_dt", 1)],
                "expireAfterSeconds": 7776000,
            }
        }
    )
    monkeypatch.setattr(database, "battles_collection", None)
    monkeypatch.setattr(database, "reports_collection", reports)

    assert ensure_state_aware_delivery_ttl() is True
    assert reports.dropped == ["ttl_reports_created_at"]
    assert len(reports.created) == 1


def test_index_failure_is_not_silently_treated_as_safe(monkeypatch):
    monkeypatch.setattr(database, "battles_collection", FakeIndexes(fail=True))
    monkeypatch.setattr(database, "reports_collection", FakeIndexes())

    with pytest.raises(DeliveryRetentionUnavailable, match="retention migration failed"):
        ensure_state_aware_delivery_ttl()
