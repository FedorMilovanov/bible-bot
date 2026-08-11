import asyncio

import pytest

import legacy_battle_delivery_flow as flow


def run(coro):
    return asyncio.run(coro)


def _battle(**overrides):
    value = {
        "_id": "battle-1", "creator_id": 10, "opponent_id": 20,
        "final_claimed": True, "status": "finalized",
        "result_delivery_protocol": flow.BATTLE_DELIVERY_PROTOCOL_OUTBOX,
    }
    value.update(overrides)
    return value


async def _sender(_battle, _role):
    return None


def test_both_recipients_use_durable_delivery_worker(monkeypatch):
    calls = []
    async def deliver(battle_id, user_id, sender):
        calls.append((battle_id, user_id, sender))
        return True
    monkeypatch.setattr(flow, "deliver_battle_recipient_once", deliver)
    result = run(flow.deliver_final_battle_once(_battle(), _sender))
    assert [(bid, uid) for bid, uid, _ in calls] == [("battle-1", 10), ("battle-1", 20)]
    assert result.creator_sent and result.opponent_sent
    assert not result.creator_pending and not result.opponent_pending
    assert result.errors == ()


def test_one_recipient_failure_does_not_block_other(monkeypatch):
    calls = []
    async def deliver(_battle_id, user_id, _sender_fn):
        calls.append(user_id)
        if user_id == 10:
            raise RuntimeError("telegram down")
        return True
    monkeypatch.setattr(flow, "deliver_battle_recipient_once", deliver)
    result = run(flow.deliver_final_battle_once(_battle(), _sender))
    assert calls == [10, 20]
    assert result.creator_pending is True
    assert result.opponent_sent is True
    assert len(result.errors) == 1


def test_existing_lease_is_pending_not_success(monkeypatch):
    async def deliver(*_args):
        return False
    monkeypatch.setattr(flow, "deliver_battle_recipient_once", deliver)
    result = run(flow.deliver_final_battle_once(_battle(), _sender))
    assert result.creator_pending is True
    assert result.opponent_pending is True
    assert result.errors == ()


@pytest.mark.parametrize("battle,message", [
    ({}, "id"),
    (_battle(creator_id="10"), "creator_id"),
    (_battle(opponent_id=True), "opponent_id"),
    (_battle(opponent_id=10), "identical"),
    (_battle(final_claimed=False), "not a retained"),
    (_battle(status="completed"), "not a retained"),
    (_battle(result_delivery_protocol="legacy_direct_v1"), "not outbox-authoritative"),
    (_battle(result_delivery_protocol=None), "not outbox-authoritative"),
])
def test_invalid_final_snapshot_fails_before_delivery(monkeypatch, battle, message):
    monkeypatch.setattr(flow, "deliver_battle_recipient_once", lambda *_args, **_kwargs: pytest.fail("must not deliver"))
    with pytest.raises(flow.LegacyBattleDeliveryStateInvalid, match=message):
        run(flow.deliver_final_battle_once(battle, _sender))
