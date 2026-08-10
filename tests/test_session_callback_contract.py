from pathlib import Path


BOT = (Path(__file__).resolve().parents[1] / "bot.py").read_text(encoding="utf-8")


def _function_body(name: str, next_name: str) -> str:
    start = BOT.index(f"async def {name}(")
    end = BOT.index(f"async def {next_name}(", start)
    return BOT[start:end]


def test_resume_callback_uses_owner_bound_lookup():
    body = _function_body("resume_session_handler", "restart_session_handler")
    assert "get_owned_quiz_session(session_id, user_id)" in body
    assert "get_quiz_session(session_id)" not in body


def test_restart_callback_atomically_claims_owned_session():
    body = _function_body("restart_session_handler", "cancel_session_handler")
    assert "claim_owned_quiz_session_restart(session_id, user_id)" in body
    assert "cancel_quiz_session(session_id)" not in body
    assert "get_quiz_session(session_id)" not in body


def test_cancel_callback_requires_owned_session_and_clears_only_matching_memory():
    body = _function_body("cancel_session_handler", "show_battle_menu")
    assert "cancel_owned_quiz_session(session_id, user_id)" in body
    assert 'active.get("session_id") == session_id' in body
    assert "cancel_quiz_session(session_id)" not in body


def test_review_errors_rejects_cross_user_target():
    body = _function_body("review_errors_handler", "_restore_session_to_memory")
    assert "if target_id != user_id:" in body
    assert "Нет доступа к данным другого пользователя" in body
    assert "except (TypeError, ValueError):" in body
