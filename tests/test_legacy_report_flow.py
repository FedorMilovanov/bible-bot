import asyncio

import pytest

import legacy_report_flow as flow


def run(coro):
    return asyncio.run(coro)


async def _photo(_report):
    return None


async def _text(_report):
    return None


def test_acceptance_failure_propagates_before_delivery(monkeypatch):
    monkeypatch.setattr(flow, "accept_report_draft_once", lambda **_: (_ for _ in ()).throw(RuntimeError("mongo unavailable")))
    monkeypatch.setattr(flow, "deliver_report_once", lambda *_args, **_kwargs: pytest.fail("delivery must not run"))
    with pytest.raises(RuntimeError, match="mongo unavailable"):
        run(flow.submit_report_once(
            user_id=42, username="u", first_name="User", draft={}, context={},
            photo_sender=_photo, text_sender=_text,
        ))


def test_success_requires_durable_ack_of_both_stages(monkeypatch):
    monkeypatch.setattr(flow, "accept_report_draft_once", lambda **_: {"_id": "report-1"})
    async def deliver(*_args, **_kwargs):
        return True, True
    monkeypatch.setattr(flow, "deliver_report_once", deliver)
    monkeypatch.setattr(flow, "get_report_delivery_stage_state", lambda _rid, stage: {"stage": stage, "delivered": True})
    result = run(flow.submit_report_once(
        user_id=42, username="u", first_name="User", draft={}, context={},
        photo_sender=_photo, text_sender=_text,
    ))
    assert result.accepted is True
    assert result.delivered is True
    assert result.delivery_pending is False
    assert result.delivery_error is None


def test_telegram_failure_after_acceptance_is_pending_not_submission_failure(monkeypatch):
    monkeypatch.setattr(flow, "accept_report_draft_once", lambda **_: {"_id": "report-1"})
    async def fail(*_args, **_kwargs):
        raise RuntimeError("telegram down")
    monkeypatch.setattr(flow, "deliver_report_once", fail)
    monkeypatch.setattr(flow, "get_report_delivery_stage_state", lambda _rid, stage: {"stage": stage, "delivered": False})
    result = run(flow.submit_report_once(
        user_id=42, username=None, first_name="User", draft={}, context={},
        photo_sender=_photo, text_sender=_text,
    ))
    assert result.accepted is True
    assert result.delivered is False
    assert result.delivery_pending is True
    assert "telegram down" in result.delivery_error


def test_partial_ack_stays_pending(monkeypatch):
    monkeypatch.setattr(flow, "accept_report_draft_once", lambda **_: {"report_id": "report-1"})
    async def deliver(*_args, **_kwargs):
        return True, True
    monkeypatch.setattr(flow, "deliver_report_once", deliver)
    monkeypatch.setattr(flow, "get_report_delivery_stage_state", lambda _rid, stage: {"delivered": stage == "photo"})
    result = run(flow.submit_report_once(
        user_id=42, username="u", first_name="User", draft={}, context=None,
        photo_sender=_photo, text_sender=_text,
    ))
    assert result.accepted is True
    assert result.delivered is False
    assert result.delivery_pending is True


def test_ack_read_failure_after_acceptance_is_pending(monkeypatch):
    monkeypatch.setattr(flow, "accept_report_draft_once", lambda **_: {"_id": "report-1"})
    async def deliver(*_args, **_kwargs):
        return False, True
    monkeypatch.setattr(flow, "deliver_report_once", deliver)
    monkeypatch.setattr(flow, "get_report_delivery_stage_state", lambda *_: (_ for _ in ()).throw(RuntimeError("mongo read down")))
    result = run(flow.submit_report_once(
        user_id=42, username="u", first_name="User", draft={}, context={},
        photo_sender=_photo, text_sender=_text,
    ))
    assert result.accepted is True
    assert result.delivered is False
    assert result.delivery_pending is True
    assert "mongo read down" in result.delivery_error
