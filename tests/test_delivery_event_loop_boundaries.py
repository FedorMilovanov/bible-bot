import asyncio
import inspect
import threading

import pytest

import legacy_report_delivery_drain as report_drain
import telegram_broadcast_controller as broadcast
import telegram_report_controller as reports


@pytest.mark.asyncio
async def test_report_queue_lookup_does_not_block_event_loop(monkeypatch):
    release = threading.Event()
    backup_fired = threading.Event()

    def blocking_lookup(limit):
        assert limit == 7
        release.wait(timeout=1.0)
        return []

    def emergency_release():
        backup_fired.set()
        release.set()

    monkeypatch.setattr(report_drain, "get_pending_reports", blocking_lookup)
    timer = threading.Timer(0.4, emergency_release)
    timer.start()
    try:
        task = asyncio.create_task(
            report_drain.drain_pending_reports(
                photo_sender=lambda report: asyncio.sleep(0),
                text_sender=lambda report: asyncio.sleep(0),
                limit=7,
            )
        )
        await asyncio.sleep(0.02)
        assert not backup_fired.is_set()
        assert not task.done()
        release.set()
        summary = await asyncio.wait_for(task, timeout=0.5)
        assert summary.reports_seen == 0
    finally:
        release.set()
        timer.cancel()


@pytest.mark.asyncio
async def test_broadcast_store_boundary_does_not_block_event_loop():
    release = threading.Event()
    backup_fired = threading.Event()

    def blocking_store(value):
        release.wait(timeout=1.0)
        return value

    def emergency_release():
        backup_fired.set()
        release.set()

    timer = threading.Timer(0.4, emergency_release)
    timer.start()
    try:
        task = asyncio.create_task(broadcast._store_call(blocking_store, "ok"))
        await asyncio.sleep(0.02)
        assert not backup_fired.is_set()
        assert not task.done()
        release.set()
        assert await asyncio.wait_for(task, timeout=0.5) == "ok"
    finally:
        release.set()
        timer.cancel()


def test_report_acceptance_and_cooldown_use_thread_boundaries():
    start_source = inspect.getsource(reports.report_start)
    confirm_source = inspect.getsource(reports.report_confirm)
    inaccuracy_source = inspect.getsource(reports.report_inaccuracy_handler)
    assert start_source.count("asyncio.to_thread(") >= 2
    assert "accept_report_draft_once" in confirm_source
    assert "asyncio.to_thread(" in confirm_source
    assert "accept_inaccuracy_report_once" in inaccuracy_source
    assert "asyncio.to_thread(" in inaccuracy_source


def test_report_drain_listing_and_repair_use_thread_boundaries():
    source = inspect.getsource(report_drain.drain_pending_reports)
    assert "asyncio.to_thread(get_pending_reports" in source
    assert source.count("asyncio.to_thread(") >= 4
    assert "repair_report_delivery_aggregate" in source


def test_broadcast_async_paths_use_store_boundary_not_direct_store_calls():
    drain_source = inspect.getsource(broadcast.drain_broadcast_outbox)
    command_source = inspect.getsource(broadcast.broadcast_command)
    assert drain_source.count("await _store_call(") >= 8
    assert "claim_next_broadcast_delivery" in drain_source
    assert "mark_broadcast_delivery_delivered" in drain_source
    assert "sync_broadcast_completion" in drain_source
    assert command_source.count("await _store_call(") >= 2
    assert "get_broadcast" in command_source
    assert "_accept_or_recover_new_broadcast" in command_source


def test_broadcast_store_boundary_adds_no_local_lock_or_retry_loop():
    source = inspect.getsource(broadcast._store_call)
    assert "asyncio.to_thread" in source
    assert "Lock(" not in source
    assert "while " not in source
    assert "sleep(" not in source
