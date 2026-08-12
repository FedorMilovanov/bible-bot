import asyncio

import pytest

import legacy_delivery_worker as worker


def run(coro):
    return asyncio.run(coro)


def test_battle_permanent_failure_settles_without_release(monkeypatch):
    settled = []
    monkeypatch.setattr(
        worker,
        "claim_battle_result_delivery",
        lambda *_: {
            "battle": {"_id": "b1"},
            "role": "creator",
            "claim_token": "tok",
        },
    )
    monkeypatch.setattr(
        worker,
        "settle_battle_result_delivery_failure",
        lambda *args, **kwargs: settled.append((args, kwargs)) or True,
    )
    monkeypatch.setattr(
        worker,
        "release_battle_result_delivery",
        lambda *_args, **_kwargs: pytest.fail("permanent failure must not release"),
    )

    async def sender(_battle, _role):
        raise worker.LegacyDeliveryPermanentFailure("Forbidden: blocked")

    assert run(worker.deliver_battle_recipient_once("b1", 10, sender)) is False
    assert settled == [
        (("b1", 10, "tok"), {"error": "Forbidden: blocked"})
    ]


def test_report_photo_permanent_failure_allows_text_stage(monkeypatch):
    report = {"_id": "r1", "photo_file_id": "photo"}
    claims = {
        "photo": {"report": report, "stage": "photo", "claim_token": "photo-tok"},
        "text": {"report": report, "stage": "text", "claim_token": "text-tok"},
    }
    settled = []
    events = []

    monkeypatch.setattr(
        worker,
        "claim_report_delivery_stage",
        lambda _rid, stage: claims[stage],
    )
    monkeypatch.setattr(
        worker,
        "settle_report_delivery_stage_failure",
        lambda *args, **kwargs: settled.append((args, kwargs)) or True,
    )
    monkeypatch.setattr(
        worker,
        "get_report_delivery_stage_state",
        lambda _rid, _stage: {"delivered": True, "terminal_failed": True},
    )
    monkeypatch.setattr(
        worker,
        "mark_report_delivery_stage_delivered",
        lambda _rid, stage, _token: events.append(("ack", stage)) or True,
    )
    monkeypatch.setattr(
        worker,
        "release_report_delivery_stage",
        lambda *_args, **_kwargs: pytest.fail("permanent failure must not release"),
    )

    async def photo_sender(_report):
        events.append(("send", "photo"))
        raise worker.LegacyDeliveryPermanentFailure("BadRequest: invalid photo")

    async def text_sender(_report):
        events.append(("send", "text"))

    assert run(worker.deliver_report_once("r1", photo_sender, text_sender)) == (False, True)
    assert settled == [
        (("r1", "photo", "photo-tok"), {"error": "BadRequest: invalid photo"})
    ]
    assert events == [
        ("send", "photo"),
        ("send", "text"),
        ("ack", "text"),
    ]


def test_empty_permanent_failure_detail_is_rejected():
    with pytest.raises(ValueError):
        worker.LegacyDeliveryPermanentFailure("")
