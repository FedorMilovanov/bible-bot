from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTROLLER = (ROOT / "telegram_controller.py").read_text(encoding="utf-8")
REPORTS = (ROOT / "telegram_report_controller.py").read_text(encoding="utf-8")
SCOPED_DRAIN = (ROOT / "legacy_report_delivery_drain.py").read_text(encoding="utf-8")


def async_function(source: str, name: str) -> str:
    marker = f"async def {name}"
    start = source.index(marker)
    next_async = source.find("\nasync def ", start + len(marker))
    next_sync = source.find("\ndef ", start + len(marker))
    candidates = [item for item in (next_async, next_sync) if item != -1]
    end = min(candidates) if candidates else len(source)
    return source[start:end]


def test_production_report_handlers_do_not_delegate_to_legacy_writers():
    required = (
        "import telegram_report_controller as reports",
        "reports.report_start",
        "reports.report_receive_text",
        "reports.report_receive_photo",
        "reports.report_skip_photo",
        "reports.report_confirm",
        "reports.report_cancel",
        "reports.cancel_report_command",
        "reports.report_inaccuracy_handler",
        "reports.report_delivery_job",
    )
    for marker in required:
        assert marker in CONTROLLER

    forbidden = (
        "legacy.report_start",
        "legacy.report_receive_text",
        "legacy.report_receive_photo",
        "legacy.report_skip_photo",
        "legacy.report_confirm",
        "legacy.report_cancel",
        "legacy.cancel_report_command",
        "legacy.report_inaccuracy_handler",
    )
    for marker in forbidden:
        assert marker not in CONTROLLER


def test_report_confirmation_accepts_before_ram_cleanup_and_never_direct_sends():
    confirm = async_function(REPORTS, "report_confirm")
    assert "accept_report_draft_once(" in confirm
    assert "report_drafts.pop(" in confirm
    assert confirm.index("accept_report_draft_once(") < confirm.index("report_drafts.pop(")
    assert "insert_report(" not in confirm
    assert "mark_report_delivered(" not in confirm
    assert "ADMIN_USER_ID" not in confirm
    assert "drain_report_outbox(" in confirm


def test_report_draft_gets_stable_identity_at_start():
    start = async_function(REPORTS, "report_start")
    assert "new_report_draft(" in start
    assert "report_drafts[user_id] = draft" in start


def test_inaccuracy_acceptance_is_attempt_bound_and_not_direct_send():
    handler = async_function(REPORTS, "report_inaccuracy_handler")
    assert "get_active_quiz_session_strict" in handler
    assert 'attempt_id = session.get("attempt_id")' in handler
    assert "accept_inaccuracy_report_once(" in handler
    assert "ADMIN_USER_ID" not in handler
    assert "send_message(" not in handler
    assert "send_photo(" not in handler


def test_report_worker_is_scoped_away_from_pvp_until_battle_migration():
    assert "get_pending_final_battles" not in SCOPED_DRAIN
    assert "deliver_battle_recipient_once" not in SCOPED_DRAIN
    assert "drain_pending_reports(" in SCOPED_DRAIN
