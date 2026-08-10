from pathlib import Path


BOT = (Path(__file__).resolve().parents[1] / "bot.py").read_text(encoding="utf-8")


def async_function(name: str) -> str:
    marker = f"async def {name}"
    start = BOT.index(marker)
    next_async = BOT.find("\nasync def ", start + len(marker))
    return BOT[start:] if next_async == -1 else BOT[start:next_async]


def test_durable_report_migration_is_all_or_nothing():
    migration_markers = (
        "from legacy_report_submit import",
        "accept_report_draft_once(",
        "from legacy_delivery_drain import",
        "drain_pending_deliveries(",
    )
    if not any(marker in BOT for marker in migration_markers):
        # The controller is intentionally still on the historical report path.
        # Once durable report migration begins, all invariants below are required.
        return

    start = async_function("report_start")
    confirm = async_function("report_confirm")
    inaccuracy = async_function("report_inaccuracy_handler")

    assert "new_report_draft(" in start
    assert "accept_report_draft_once(" in confirm
    assert "insert_report(" not in confirm
    assert "mark_report_delivered(" not in confirm

    # Durable acceptance must happen before the only RAM copy is removed.
    if "report_drafts.pop(" in confirm:
        assert confirm.index("accept_report_draft_once(") < confirm.index("report_drafts.pop(")

    # Admin delivery must come from the durable outbox worker, not the request
    # handler, otherwise a process crash can lose which stage was delivered.
    assert "ADMIN_USER_ID" not in confirm
    assert "drain_pending_deliveries(" in BOT

    # The inline “inaccuracy” button is a second report ingress path. It must not
    # remain an ephemeral direct Telegram send after the main report path becomes
    # durable; use the generic store or a dedicated stable-id acceptance helper.
    assert "ADMIN_USER_ID" not in inaccuracy
    assert (
        "accept_report_once(" in inaccuracy
        or "accept_inaccuracy_report_once(" in inaccuracy
    )
