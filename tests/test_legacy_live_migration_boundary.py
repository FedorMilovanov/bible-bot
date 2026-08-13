from pathlib import Path


CONTROLLER = (
    Path(__file__).resolve().parents[1] / "telegram_controller.py"
).read_text(encoding="utf-8")


def async_function(name: str) -> str:
    marker = f"async def {name}"
    start = CONTROLLER.index(marker)
    next_async = CONTROLLER.find("\nasync def ", start + len(marker))
    return CONTROLLER[start:] if next_async == -1 else CONTROLLER[start:next_async]


def _assert_before(source: str, first: str, later: str) -> None:
    if later in source:
        assert first in source
        assert source.index(first) < source.index(later)


def test_attempt_bound_live_answer_is_the_only_production_path():
    normal_send = async_function("send_question")
    challenge_send = async_function("send_challenge_question")
    render = async_function("_send_current_question")
    answer = async_function("_handle_inline_answer")
    timeout = async_function("_handle_question_timeout")
    shutdown = async_function("_save_all_sessions")

    for marker in (
        "build_live_answer_callback(",
        "apply_live_answer_once(",
        "apply_live_timeout_once(",
        "mark_live_question_sent(",
    ):
        assert marker in CONTROLLER

    assert "_send_current_question(" in normal_send
    assert "_send_current_question(" in challenge_send
    assert "build_live_answer_callback(" in render
    assert 'callback_data=f"qa_{' not in render
    assert 'callback_data=f"cha_{' not in render

    assert "apply_live_answer_once(" in answer
    assert "apply_live_timeout_once(" in timeout

    for blind_write in (
        "advance_quiz_session(",
        "set_question_sent_at(",
        "update_quiz_session(",
    ):
        assert blind_write not in CONTROLLER

    for mutation in (
        'data["correct_answers"] +=',
        'data["current_question"] +=',
        'data["answered_questions"].append(',
    ):
        assert mutation not in answer
        assert mutation not in timeout

    # Durable answer/timeout acceptance must happen before non-idempotent UI,
    # timer cancellation or analytics side effects.
    for later in (
        "_cancel_runtime_timer(",
        "_animate_answer_buttons(",
        "record_question_stat(",
    ):
        _assert_before(answer, "apply_live_answer_once(", later)
    _assert_before(timeout, "apply_live_timeout_once(", "record_question_stat(")

    if "record_question_stat(" in answer:
        assert "if outcome.applied:" in answer
        assert answer.index("if outcome.applied:") < answer.index("record_question_stat(")
    if "record_question_stat(" in timeout:
        assert "if outcome.applied:" in timeout
        assert timeout.index("if outcome.applied:") < timeout.index("record_question_stat(")

    # Shutdown may clean process-local timers, never roll stale RAM into Mongo.
    assert "update_quiz_session(" not in shutdown

    assert 'pattern=r"^qa_' not in CONTROLLER
    assert 'pattern=r"^cha_' not in CONTROLLER
    assert 'pattern=r"^qa:"' in CONTROLLER
    assert 'pattern=r"^cha:"' in CONTROLLER
