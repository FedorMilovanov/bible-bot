from pathlib import Path


SOURCE = (Path(__file__).resolve().parents[1] / "telegram_battle_controller.py").read_text(encoding="utf-8")


def async_function(name: str) -> str:
    marker = f"async def {name}"
    start = SOURCE.index(marker)
    next_async = SOURCE.find("\nasync def ", start + len(marker))
    next_sync = SOURCE.find("\ndef ", start + len(marker))
    candidates = [item for item in (next_async, next_sync) if item != -1]
    end = min(candidates) if candidates else len(SOURCE)
    return SOURCE[start:end]


def test_battle_adapter_has_no_ram_progress_authority():
    assert "legacy.user_data" not in SOURCE
    assert 'callback_data=f"ba_' not in SOURCE
    assert 'replace("ba_"' not in SOURCE


def test_question_delivery_precedes_durable_timer_marker():
    send = async_function("send_battle_question")
    assert "await bot.send_message(" in send
    assert "mark_battle_question_sent" in send
    assert "asyncio.to_thread(" in send
    assert send.index("await bot.send_message(") < send.index("mark_battle_question_sent")


def test_answer_cas_precedes_telegram_feedback():
    answer = async_function("battle_answer")
    assert "parse_battle_answer_callback(" in answer
    assert "resolve_owned_open_battle_callback" in answer
    assert "resolve_battle_option(" in answer
    assert "record_battle_answer_once" in answer
    assert answer.count("asyncio.to_thread(") >= 2
    assert "await query.answer(" in answer
    assert answer.index("record_battle_answer_once") < answer.index("await query.answer(")
    for mutation in (
        'correct_answers"] +=',
        'battle_points"] =',
        'current_question"] +=',
    ):
        assert mutation not in answer


def test_terminal_ledger_precedes_participant_receipt_and_outbox_final():
    finish = async_function("finish_battle_for_user")
    assert "completed_battle_result_inputs" in finish
    assert "record_battle_result" in finish
    assert finish.count("asyncio.to_thread(") >= 2
    assert finish.index("completed_battle_result_inputs") < finish.index("record_battle_result")
    assert "claim_final_battle" in finish
    assert "delivery_protocol=BATTLE_DELIVERY_PROTOCOL_OUTBOX" in "".join(finish.split())
    assert "show_battle_results(" not in finish


def test_creator_cannot_start_before_opponent_exists():
    start = async_function("start_battle_questions")
    assert 'battle.get("opponent_id") is None' in start
    assert "ensure_battle_progress" in start
    assert "asyncio.to_thread(" in start
    assert start.index('battle.get("opponent_id") is None') < start.index("ensure_battle_progress")


def test_cancel_never_uses_legacy_destructive_delete():
    cancel = async_function("cancel_battle")
    assert "cancel_unstarted_battle" in cancel
    assert "asyncio.to_thread(" in cancel
    assert "delete_battle_for_participant(" not in cancel


def test_join_routes_creator_ready_notice_through_durable_adapter():
    join = async_function("join_battle")
    assert "claim_durable_battle_opponent" in join
    assert "ready_delivery.deliver_creator_ready_once" in join
    assert "context.bot.send_message(" not in join
    assert join.index("claim_durable_battle_opponent") < join.index(
        "ready_delivery.deliver_creator_ready_once"
    )


def test_maintenance_drains_ready_outbox_before_shared_finalization():
    maintenance = async_function("battle_maintenance_job")
    assert "ready_delivery.drain_creator_ready_outbox" in maintenance
    assert "finalize_ready_battles" in maintenance
    assert maintenance.index("ready_delivery.drain_creator_ready_outbox") < maintenance.index(
        "finalize_ready_battles"
    )
