from pathlib import Path


BOT = (Path(__file__).resolve().parents[1] / "bot.py").read_text(encoding="utf-8")


def region(start: str, end: str) -> str:
    start_i = BOT.index(start)
    end_i = BOT.index(end, start_i)
    return BOT[start_i:end_i]


def test_random_command_includes_historical_leaf_pools():
    random_source = region("async def random_command", "async def admin_command")
    assert '"nero", "geography"' in random_source


def test_review_errors_never_uses_callback_user_as_authorization():
    source = region("async def review_errors_handler", "async def _restore_session_to_memory")
    assert "target_id != user_id" in source
    assert "wrong = user_data[user_id]" in source
    assert "wrong = user_data[target_id]" not in source


def test_resume_restart_cancel_are_owner_scoped():
    resume = region("async def resume_session_handler", "async def restart_session_handler")
    restart = region("async def restart_session_handler", "async def cancel_session_handler")
    cancel = region("async def cancel_session_handler", "async def show_battle_menu")

    assert "get_owned_quiz_session(session_id, user_id)" in resume
    assert "get_quiz_session(session_id)" not in resume
    assert "cancel_owned_quiz_session(session_id, user_id)" in restart
    assert "cancel_quiz_session(session_id)" not in restart
    assert "cancel_owned_quiz_session(session_id, user_id)" in cancel
    assert "cancel_quiz_session(session_id)" not in cancel


def test_battle_answer_has_stale_and_bounds_guards():
    source = region("async def battle_answer", "async def _retire_battle_message")
    assert 'current_question", 0) >= len(data.get("questions", []))' in source
    assert "idx < 0 or idx >= len(current_options)" in source
    assert "battle_result_pending" in source


def test_battle_finalization_uses_atomic_integrity_layer_only():
    source = region("async def finish_battle_for_user", "async def cancel_battle")
    assert "record_battle_result(" in source
    assert "claim_final_battle(battle_id)" in source
    assert "update_battle_stats(" not in source
    assert "delete_battle(battle_id)" not in source
    assert "await show_battle_results(bot, final_battle)" in source


def test_pending_battle_result_cannot_be_cancelled():
    source = region("async def cancel_battle", "async def inline_query_handler")
    assert "battle_result_pending" in source
    assert "delete_battle_for_participant" in source


def test_inaccuracy_report_uses_clicked_question_index_and_plain_delivery():
    source = region("async def report_inaccuracy_handler", "async def _handle_question_timeout")
    assert 'replace("report_inaccuracy_", "", 1)' in source
    assert "q_num < 0 or q_num >= len(q_list)" in source
    assert "data.get(\"current_question\"" not in source
    assert "parse_mode=\"Markdown\"" not in source
    assert "Принято, отправляю автору" in source


def test_retry_errors_has_single_branch_answer_and_safe_callback_parse():
    source = region("async def retry_errors", "def _build_error_page")
    assert 'replace("retry_errors_", "", 1)' in source
    assert "except (TypeError, ValueError)" in source
    assert "target_id != user_id" in source
    assert not source.lstrip().startswith("async def retry_errors(update: Update, context):\n    query   = update.callback_query\n    await query.answer()")


def test_review_test_rejects_malformed_and_negative_indexes():
    source = region("async def review_test_handler", "async def review_errors_handler")
    assert "except (TypeError, ValueError)" in source
    assert "q_index < 0 or q_index >= len(answered)" in source


def test_report_start_validates_type_before_answering_callback():
    source = region("async def report_start", "async def report_receive_text")
    assert "report_type not in REPORT_TYPE_LABELS" in source
    assert "if not can_submit_report(user_id)" in source
    assert source.index("await query.answer()") > source.index("if not can_submit_report(user_id)")
