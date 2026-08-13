import asyncio
from types import SimpleNamespace

import pytest

import legacy_battle_delivery_drain as drain


def run(coro):
    return asyncio.run(coro)


async def sender(_battle, _role):
    return None


def test_battle_only_drain_delivers_outbox_finals(monkeypatch):
    monkeypatch.setattr(
        drain,
        "get_pending_final_battles",
        lambda limit: [{"_id": "b1"}, {"_id": "b2"}],
    )
    seen = []

    async def deliver(battle, _sender):
        seen.append(battle["_id"])
        return SimpleNamespace(
            creator_sent=True,
            opponent_sent=battle["_id"] == "b1",
            creator_pending=False,
            opponent_pending=battle["_id"] != "b1",
            errors=(),
        )

    monkeypatch.setattr(drain, "deliver_final_battle_once", deliver)
    result = run(drain.drain_pending_battles(sender=sender, limit=7))

    assert seen == ["b1", "b2"]
    assert result.battles_seen == 2
    assert result.recipient_sends == 3
    assert result.deferred == 1
    assert result.errors == ()


def test_battle_queue_outage_is_retryable_summary(monkeypatch):
    monkeypatch.setattr(
        drain,
        "get_pending_final_battles",
        lambda _limit: (_ for _ in ()).throw(drain.BattleStoreUnavailable("down")),
    )
    result = run(drain.drain_pending_battles(sender=sender))
    assert result.battles_seen == 0
    assert len(result.errors) == 1
    assert result.errors[0].startswith("battle-list:<queue>:BattleStoreUnavailable:")


def test_one_battle_failure_does_not_starve_next(monkeypatch):
    monkeypatch.setattr(
        drain,
        "get_pending_final_battles",
        lambda _limit: [{"_id": "bad"}, {"_id": "good"}],
    )
    seen = []

    async def deliver(battle, _sender):
        if battle["_id"] == "bad":
            raise RuntimeError("boom")
        seen.append("good")
        return SimpleNamespace(
            creator_sent=True,
            opponent_sent=False,
            creator_pending=False,
            opponent_pending=True,
            errors=("opponent:RuntimeError:telegram",),
        )

    monkeypatch.setattr(drain, "deliver_final_battle_once", deliver)
    result = run(drain.drain_pending_battles(sender=sender))
    assert seen == ["good"]
    assert result.recipient_sends == 1
    assert result.deferred == 1
    assert len(result.errors) == 2


def test_invalid_listing_shape_fails_closed(monkeypatch):
    monkeypatch.setattr(drain, "get_pending_final_battles", lambda _limit: {"_id": "b1"})
    with pytest.raises(drain.LegacyBattleDeliveryQueueInvalid, match="listing"):
        run(drain.drain_pending_battles(sender=sender))


def test_scope_does_not_import_report_queue():
    assert not hasattr(drain, "get_pending_reports")
    assert not hasattr(drain, "deliver_report_once")
