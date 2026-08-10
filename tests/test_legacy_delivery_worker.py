import asyncio

import pytest

import legacy_delivery_worker as worker


def run(coro):
    return asyncio.run(coro)


def test_battle_delivery_claim_send_ack(monkeypatch):
    battle = {"_id": "b1", "creator_id": 10}
    events = []
    monkeypatch.setattr(
        worker,
        "claim_battle_result_delivery",
        lambda *_: {"battle": battle, "role": "creator", "claim_token": "tok"},
    )
    monkeypatch.setattr(
        worker,
        "mark_battle_result_delivered",
        lambda *args: events.append(("ack", args)) or True,
    )
    monkeypatch.setattr(
        worker,
        "release_battle_result_delivery",
        lambda *args, **kwargs: pytest.fail("success must not release lease"),
    )

    async def sender(value, role):
        events.append(("send", value, role))

    assert run(worker.deliver_battle_recipient_once("b1", 10, sender)) is True
    assert events[0] == ("send", battle, "creator")
    assert events[1][0] == "ack"


def test_battle_sender_failure_releases_for_retry(monkeypatch):
    released = []
    monkeypatch.setattr(
        worker,
        "claim_battle_result_delivery",
        lambda *_: {"battle": {"_id": "b1"}, "role": "opponent", "claim_token": "tok"},
    )
    monkeypatch.setattr(
        worker,
        "release_battle_result_delivery",
        lambda *args, **kwargs: released.append((args, kwargs)) or True,
    )

    async def sender(_battle, _role):
        raise RuntimeError("telegram down")

    with pytest.raises(RuntimeError, match="telegram down"):
        run(worker.deliver_battle_recipient_once("b1", 20, sender))
    assert released[0][0][:3] == ("b1", 20, "tok")
    assert "telegram down" in released[0][1]["error"]


def test_battle_ack_failure_does_not_immediately_release(monkeypatch):
    released = []
    monkeypatch.setattr(
        worker,
        "claim_battle_result_delivery",
        lambda *_: {"battle": {"_id": "b1"}, "role": "creator", "claim_token": "tok"},
    )
    monkeypatch.setattr(worker, "mark_battle_result_delivered", lambda *_: False)
    monkeypatch.setattr(
        worker,
        "release_battle_result_delivery",
        lambda *args, **kwargs: released.append((args, kwargs)),
    )

    async def sender(_battle, _role):
        return None

    with pytest.raises(worker.LegacyDeliveryAcknowledgementPending):
        run(worker.deliver_battle_recipient_once("b1", 10, sender))
    assert released == []


def test_report_delivers_photo_then_text_and_acks_each_stage(monkeypatch):
    report = {"_id": "r1", "photo_file_id": "photo"}
    events = []

    def claim(_report_id, stage):
        return {"report": report, "stage": stage, "claim_token": f"{stage}-tok"}

    monkeypatch.setattr(worker, "claim_report_delivery_stage", claim)
    monkeypatch.setattr(
        worker,
        "mark_report_delivery_stage_delivered",
        lambda _rid, stage, token: events.append(("ack", stage, token)) or True,
    )
    monkeypatch.setattr(
        worker,
        "release_report_delivery_stage",
        lambda *args, **kwargs: pytest.fail("success must not release lease"),
    )

    async def photo_sender(_report):
        events.append(("send", "photo"))

    async def text_sender(_report):
        events.append(("send", "text"))

    assert run(worker.deliver_report_once("r1", photo_sender, text_sender)) == (True, True)
    assert events == [
        ("send", "photo"),
        ("ack", "photo", "photo-tok"),
        ("send", "text"),
        ("ack", "text", "text-tok"),
    ]


def test_report_skips_durably_acknowledged_photo_and_sends_text(monkeypatch):
    events = []

    def claim(_report_id, stage):
        if stage == "photo":
            return None
        return {
            "report": {"_id": "r1", "photo_file_id": "photo"},
            "stage": "text",
            "claim_token": "text-tok",
        }

    monkeypatch.setattr(worker, "claim_report_delivery_stage", claim)
    monkeypatch.setattr(
        worker,
        "get_report_delivery_stage_state",
        lambda _rid, stage: {
            "report_id": "r1",
            "stage": stage,
            "delivered": True,
            "claim_token": None,
            "lease_until": None,
            "attempts": 1,
            "photo_file_id": "photo",
        },
    )
    monkeypatch.setattr(worker, "mark_report_delivery_stage_delivered", lambda *_: True)

    async def photo_sender(_report):
        pytest.fail("acknowledged photo must not be replayed")

    async def text_sender(_report):
        events.append("text")

    assert run(worker.deliver_report_once("r1", photo_sender, text_sender)) == (False, True)
    assert events == ["text"]


def test_report_does_not_overtake_photo_lease_owned_by_other_worker(monkeypatch):
    events = []

    def claim(_report_id, stage):
        if stage == "photo":
            return None
        pytest.fail("text stage must not be claimed before photo acknowledgement")

    monkeypatch.setattr(worker, "claim_report_delivery_stage", claim)
    monkeypatch.setattr(
        worker,
        "get_report_delivery_stage_state",
        lambda _rid, stage: {
            "report_id": "r1",
            "stage": stage,
            "delivered": False,
            "claim_token": "other-worker",
            "lease_until": "later",
            "attempts": 2,
            "photo_file_id": "photo",
        },
    )

    async def photo_sender(_report):
        events.append("photo")

    async def text_sender(_report):
        events.append("text")

    assert run(worker.deliver_report_once("r1", photo_sender, text_sender)) == (False, False)
    assert events == []


def test_report_sender_failure_releases_only_failed_stage(monkeypatch):
    released = []
    monkeypatch.setattr(
        worker,
        "claim_report_delivery_stage",
        lambda _rid, stage: {
            "report": {"_id": "r1", "photo_file_id": "photo"},
            "stage": stage,
            "claim_token": f"{stage}-tok",
        },
    )
    monkeypatch.setattr(
        worker,
        "release_report_delivery_stage",
        lambda *args, **kwargs: released.append((args, kwargs)) or True,
    )

    async def photo_sender(_report):
        raise RuntimeError("photo failed")

    async def text_sender(_report):
        pytest.fail("text must not run after photo failure")

    with pytest.raises(RuntimeError, match="photo failed"):
        run(worker.deliver_report_once("r1", photo_sender, text_sender))
    assert released[0][0][:3] == ("r1", "photo", "photo-tok")


def test_photo_stage_without_durable_file_id_fails_closed(monkeypatch):
    released = []
    monkeypatch.setattr(
        worker,
        "claim_report_delivery_stage",
        lambda *_: {"report": {"_id": "r1"}, "stage": "photo", "claim_token": "tok"},
    )
    monkeypatch.setattr(
        worker,
        "release_report_delivery_stage",
        lambda *args, **kwargs: released.append((args, kwargs)) or True,
    )

    async def sender(_report):
        pytest.fail("invalid photo stage must not send")

    with pytest.raises(worker.LegacyDeliveryStateInvalid, match="photo_file_id"):
        run(worker._deliver_report_stage_once("r1", "photo", sender))
    assert released[0][0][:3] == ("r1", "photo", "tok")
