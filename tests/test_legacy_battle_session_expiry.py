from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

import legacy_battle_session as session
from legacy_battle_protocols import BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE


class ClaimCollection:
    def __init__(self):
        self.call = None

    def find_one_and_update(self, query, update, return_document=None):
        self.call = (query, update, return_document)
        return None


def install(monkeypatch, collection, now):
    monkeypatch.setattr(session, "_battle_collection", lambda: collection)
    monkeypatch.setattr(session, "_database", lambda: SimpleNamespace(_now_utc=lambda: now))


def test_direct_join_callback_cannot_claim_expired_waiting_battle(monkeypatch):
    now = datetime(2026, 8, 11, 12, 0, 0)
    collection = ClaimCollection()
    install(monkeypatch, collection, now)

    assert session.claim_durable_battle_opponent(
        "battle-1", 202, "Opponent", max_age_minutes=10
    ) is None

    query, _, _ = collection.call
    assert query["question_progress_protocol"] == BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE
    assert query["created_at_dt"] == {"$gte": now - timedelta(minutes=10)}


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "10"])
def test_join_rejects_invalid_expiry_before_mongo(monkeypatch, value):
    collection = ClaimCollection()
    install(monkeypatch, collection, datetime(2026, 8, 11, 12, 0, 0))

    with pytest.raises(ValueError, match="positive integer"):
        session.claim_durable_battle_opponent(
            "battle-1", 202, "Opponent", max_age_minutes=value
        )
    assert collection.call is None
