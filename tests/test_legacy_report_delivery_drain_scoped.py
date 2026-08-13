import asyncio

import pytest

import legacy_report_delivery_drain as drain


def run(coro):
    return asyncio.run(coro)


async def _noop(_report):
    return None


@pytest.fixture(autouse=True)
def _default_no_aggregate_repair(monkeypatch):
    monkeypatch.setattr(drain, "repair_report_delivery_aggregate", lambda _report_id: False)


def test_report_only_drain_delivers_pending_reports(monkeypatch):
    monkeypatch.setattr(
        drain,
        "get_pending_reports",
        lambda limit: [{"_id": "r1"}, {"report_id": "r2"}],
    )
    seen = []

    async def deliver(report_id, _photo, _text):
        seen.append(report_id)
        return (report_id == "r1", True)

    monkeypatch.setattr(drain, "deliver_report_once", deliver)
    result = run(drain.drain_pending_reports(photo_sender=_noop, text_sender=_noop, limit=7))

    assert seen == ["r1", "r2"]
    assert result.reports_seen == 2
    assert result.stage_sends == 3
    assert result.deferred == 0
    assert result.errors == ()


def test_stuck_aggregate_is_repaired_before_any_sender_call(monkeypatch):
    monkeypatch.setattr(drain, "get_pending_reports", lambda _limit: [{"_id": "r1"}])
    repairs = []
    monkeypatch.setattr(
        drain,
        "repair_report_delivery_aggregate",
        lambda report_id: repairs.append(report_id) or True,
    )
    monkeypatch.setattr(
        drain,
        "deliver_report_once",
        lambda *_args: pytest.fail("terminal stage evidence must not be resent"),
    )

    result = run(drain.drain_pending_reports(photo_sender=_noop, text_sender=_noop))

    assert repairs == ["r1"]
    assert result.reports_seen == 1
    assert result.stage_sends == 0
    assert result.deferred == 0
    assert result.errors == ()


def test_worker_error_after_stage_settlement_is_repaired_without_retry_error(monkeypatch):
    monkeypatch.setattr(drain, "get_pending_reports", lambda _limit: [{"_id": "r1"}])
    repair_results = iter([False, True])
    monkeypatch.setattr(
        drain,
        "repair_report_delivery_aggregate",
        lambda _report_id: next(repair_results),
    )

    async def deliver(_report_id, _photo, _text):
        raise drain.ReportStoreUnavailable("aggregate ack lost")

    monkeypatch.setattr(drain, "deliver_report_once", deliver)

    result = run(drain.drain_pending_reports(photo_sender=_noop, text_sender=_noop))

    assert result.reports_seen == 1
    assert result.stage_sends == 0
    assert result.deferred == 0
    assert result.errors == ()


def test_report_only_drain_never_touches_battle_queue(monkeypatch):
    assert not hasattr(drain, "get_pending_final_battles")
    assert not hasattr(drain, "deliver_battle_recipient_once")


def test_report_store_outage_is_retryable_summary(monkeypatch):
    monkeypatch.setattr(
        drain,
        "get_pending_reports",
        lambda _limit: (_ for _ in ()).throw(drain.ReportStoreUnavailable("down")),
    )
    result = run(drain.drain_pending_reports(photo_sender=_noop, text_sender=_noop))

    assert result.reports_seen == 0
    assert result.stage_sends == 0
    assert len(result.errors) == 1
    assert result.errors[0].startswith("report-list:<queue>:ReportStoreUnavailable:")


def test_one_report_failure_does_not_starve_next(monkeypatch):
    monkeypatch.setattr(
        drain,
        "get_pending_reports",
        lambda _limit: [{"_id": "bad"}, {"_id": "good"}],
    )
    seen = []

    async def deliver(report_id, _photo, _text):
        if report_id == "bad":
            raise RuntimeError("telegram down")
        seen.append(report_id)
        return False, True

    monkeypatch.setattr(drain, "deliver_report_once", deliver)
    result = run(drain.drain_pending_reports(photo_sender=_noop, text_sender=_noop))

    assert seen == ["good"]
    assert result.stage_sends == 1
    assert len(result.errors) == 1


def test_existing_lease_is_deferred(monkeypatch):
    monkeypatch.setattr(drain, "get_pending_reports", lambda _limit: [{"_id": "r1"}])
    monkeypatch.setattr(
        drain,
        "deliver_report_once",
        lambda *_args: _async_result((False, False)),
    )
    result = run(drain.drain_pending_reports(photo_sender=_noop, text_sender=_noop))
    assert result.deferred == 1
    assert result.errors == ()


async def _async_result(value):
    return value


def test_invalid_listing_shape_fails_closed(monkeypatch):
    monkeypatch.setattr(drain, "get_pending_reports", lambda _limit: {"_id": "r1"})
    with pytest.raises(drain.LegacyReportDeliveryQueueInvalid, match="listing"):
        run(drain.drain_pending_reports(photo_sender=_noop, text_sender=_noop))


def test_limit_validation_precedes_store_lookup(monkeypatch):
    monkeypatch.setattr(
        drain,
        "get_pending_reports",
        lambda _limit: pytest.fail("must validate first"),
    )
    with pytest.raises(ValueError, match="limit"):
        run(drain.drain_pending_reports(photo_sender=_noop, text_sender=_noop, limit=0))
