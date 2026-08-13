import asyncio

import pytest

import legacy_delivery_drain as drain


def run(coro):
    return asyncio.run(coro)


async def _noop_battle(_battle, _role):
    return None


async def _noop_report(_report):
    return None


def test_drain_delivers_reports_and_battle_recipients(monkeypatch):
    events = []
    monkeypatch.setattr(drain, "get_pending_reports", lambda limit: [{"_id": "r1"}])
    monkeypatch.setattr(
        drain,
        "get_pending_final_battles",
        lambda limit: [{"_id": "b1", "creator_id": 10, "opponent_id": 20}],
    )

    async def report_once(report_id, _photo, _text):
        events.append(("report", report_id))
        return True, True

    async def battle_once(battle_id, user_id, _sender):
        events.append(("battle", battle_id, user_id))
        return True

    monkeypatch.setattr(drain, "deliver_report_once", report_once)
    monkeypatch.setattr(drain, "deliver_battle_recipient_once", battle_once)
    result = run(
        drain.drain_pending_deliveries(
            battle_sender=_noop_battle,
            report_photo_sender=_noop_report,
            report_text_sender=_noop_report,
            limit=7,
        )
    )
    assert result.report_stage_sends == 2
    assert result.battle_recipient_sends == 2
    assert result.deferred == 0
    assert result.errors == ()
    assert events == [("report", "r1"), ("battle", "b1", 10), ("battle", "b1", 20)]


def test_item_failure_does_not_starve_later_queue_entries(monkeypatch):
    seen = []
    monkeypatch.setattr(
        drain,
        "get_pending_reports",
        lambda _limit: [{"_id": "bad"}, {"_id": "good"}],
    )
    monkeypatch.setattr(drain, "get_pending_final_battles", lambda _limit: [])

    async def report_once(report_id, _photo, _text):
        if report_id == "bad":
            raise RuntimeError("telegram down")
        seen.append(report_id)
        return False, True

    monkeypatch.setattr(drain, "deliver_report_once", report_once)
    result = run(
        drain.drain_pending_deliveries(
            battle_sender=_noop_battle,
            report_photo_sender=_noop_report,
            report_text_sender=_noop_report,
        )
    )
    assert seen == ["good"]
    assert result.report_stage_sends == 1
    assert len(result.errors) == 1


def test_existing_leases_are_deferred_not_errors(monkeypatch):
    monkeypatch.setattr(drain, "get_pending_reports", lambda _limit: [{"_id": "r1"}])
    monkeypatch.setattr(
        drain,
        "get_pending_final_battles",
        lambda _limit: [{"_id": "b1", "creator_id": 10, "opponent_id": 20}],
    )

    async def report_once(*_args):
        return False, False

    async def battle_once(*_args):
        return False

    monkeypatch.setattr(drain, "deliver_report_once", report_once)
    monkeypatch.setattr(drain, "deliver_battle_recipient_once", battle_once)
    result = run(
        drain.drain_pending_deliveries(
            battle_sender=_noop_battle,
            report_photo_sender=_noop_report,
            report_text_sender=_noop_report,
        )
    )
    assert result.deferred == 3
    assert result.errors == ()


def test_invalid_battle_does_not_block_valid_battle(monkeypatch):
    monkeypatch.setattr(drain, "get_pending_reports", lambda _limit: [])
    monkeypatch.setattr(
        drain,
        "get_pending_final_battles",
        lambda _limit: [
            {"_id": "bad", "creator_id": 10, "opponent_id": 10},
            {"_id": "good", "creator_id": 20, "opponent_id": 30},
        ],
    )
    delivered = []

    async def battle_once(battle_id, user_id, _sender):
        delivered.append((battle_id, user_id))
        return True

    monkeypatch.setattr(drain, "deliver_battle_recipient_once", battle_once)
    result = run(
        drain.drain_pending_deliveries(
            battle_sender=_noop_battle,
            report_photo_sender=_noop_report,
            report_text_sender=_noop_report,
        )
    )
    assert delivered == [("good", 20), ("good", 30)]
    assert len(result.errors) == 1


def test_report_store_listing_outage_does_not_starve_battle_queue(monkeypatch):
    monkeypatch.setattr(
        drain,
        "get_pending_reports",
        lambda _limit: (_ for _ in ()).throw(drain.ReportStoreUnavailable("reports down")),
    )
    monkeypatch.setattr(
        drain,
        "get_pending_final_battles",
        lambda _limit: [{"_id": "b1", "creator_id": 10, "opponent_id": 20}],
    )
    delivered = []

    async def battle_once(battle_id, user_id, _sender):
        delivered.append((battle_id, user_id))
        return True

    monkeypatch.setattr(drain, "deliver_battle_recipient_once", battle_once)
    result = run(
        drain.drain_pending_deliveries(
            battle_sender=_noop_battle,
            report_photo_sender=_noop_report,
            report_text_sender=_noop_report,
        )
    )

    assert result.reports_seen == 0
    assert result.battles_seen == 1
    assert result.battle_recipient_sends == 2
    assert delivered == [("b1", 10), ("b1", 20)]
    assert len(result.errors) == 1
    assert result.errors[0].startswith("report-list:<queue>:ReportStoreUnavailable:")


def test_battle_store_listing_outage_does_not_starve_report_queue(monkeypatch):
    monkeypatch.setattr(drain, "get_pending_reports", lambda _limit: [{"_id": "r1"}])
    monkeypatch.setattr(
        drain,
        "get_pending_final_battles",
        lambda _limit: (_ for _ in ()).throw(drain.BattleStoreUnavailable("battles down")),
    )
    delivered = []

    async def report_once(report_id, _photo, _text):
        delivered.append(report_id)
        return False, True

    monkeypatch.setattr(drain, "deliver_report_once", report_once)
    result = run(
        drain.drain_pending_deliveries(
            battle_sender=_noop_battle,
            report_photo_sender=_noop_report,
            report_text_sender=_noop_report,
        )
    )

    assert result.reports_seen == 1
    assert result.battles_seen == 0
    assert result.report_stage_sends == 1
    assert delivered == ["r1"]
    assert len(result.errors) == 1
    assert result.errors[0].startswith("battle-list:<queue>:BattleStoreUnavailable:")


def test_unknown_listing_failure_still_propagates_fail_closed(monkeypatch):
    monkeypatch.setattr(
        drain,
        "get_pending_reports",
        lambda _limit: (_ for _ in ()).throw(RuntimeError("programming failure")),
    )
    monkeypatch.setattr(drain, "get_pending_final_battles", lambda _limit: [])
    with pytest.raises(RuntimeError, match="programming failure"):
        run(
            drain.drain_pending_deliveries(
                battle_sender=_noop_battle,
                report_photo_sender=_noop_report,
                report_text_sender=_noop_report,
            )
        )


def test_limit_validation_happens_before_listing(monkeypatch):
    monkeypatch.setattr(drain, "get_pending_reports", lambda _limit: pytest.fail("must fail first"))
    with pytest.raises(ValueError, match="limit"):
        run(
            drain.drain_pending_deliveries(
                battle_sender=_noop_battle,
                report_photo_sender=_noop_report,
                report_text_sender=_noop_report,
                limit=0,
            )
        )
