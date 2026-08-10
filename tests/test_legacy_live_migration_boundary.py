from pathlib import Path


BOT = (Path(__file__).resolve().parents[1] / "bot.py").read_text(encoding="utf-8")


def async_function(name: str) -> str:
    marker = f"async def {name}"
    start = BOT.index(marker)
    next_async = BOT.find("\nasync def ", start + len(marker))
    return BOT[start:] if next_async == -1 else BOT[start:next_async]


def _assert_before(source: str, first: str, later: str) -> None:
    if later in source:
        assert source.index(first) < source.index(later)


def test_attempt_bound_live_answer_migration_is_all_or_nothing():
    live_markers = (
        "build_live_answer_callback(",
        "apply_live_answer_once(",
        "apply_live_timeout_once(",
        "mark_live_question_sent(",
    )
    if not any(marker in BOT for marker in live_markers):
        # The legacy controller is intentionally still unmigrated. Once the
        # first live marker lands, all invariants below become mandatory.
        return

    normal_send = async_function("send_question")
    challenge_send = async_function("send_challenge_question")
    answer = async_function("_handle_inline_answer")
    timeout = async_function("_handle_question_timeout")
    shutdown = async_function("_save_all_sessions")

    assert "build_live_answer_callback(" in BOT
    assert 'callback_data=f"qa_{' not in normal_send
    assert 'callback_data=f"cha_{' not in challenge_send

    assert "apply_live_answer_once(" in answer
    assert "advance_quiz_session(" not in answer
    assert "apply_live_timeout_once(" in timeout
    assert "advance_quiz_session(" not in timeout

    # Mongo is the progress authority. The controller must not re-implement
    # answer/index/counter mutation before or after the durable transition.
    for mutation in (
        'data["correct_answers"] +=',
        'data["current_question"] +=',
        'data["answered_questions"].append(',
    ):
        assert mutation not in answer
        assert mutation not in timeout

    # A failed durable transition must not have already cancelled timers,
    # animated an answer, or written non-idempotent analytics. These side
    # effects happen only after apply_live_* has accepted/replayed the answer.
    for later in (
        "timer_task.cancel()",
        "_cancel_countdown(",
        "_animate_answer_buttons(",
        "record_question_stat(",
    ):
        _assert_before(answer, "apply_live_answer_once(", later)
    _assert_before(timeout, "apply_live_timeout_once(", "_cancel_countdown(")

    # Exact lost-response replay returns applied=False. Legacy analytics are
    # increment-only, so if they remain in the controller they must be gated on
    # a newly applied transition rather than replayed again.
    if "record_question_stat(" in answer:
        assert "outcome = apply_live_answer_once(" in answer
        assert "if outcome.applied:" in answer
        assert answer.index("if outcome.applied:") < answer.index("record_question_stat(")

    # Once per-answer Mongo CAS is authoritative, stale RAM must never write
    # progress counters back during shutdown and roll durable state backwards.
    assert "update_quiz_session(" not in shutdown

    assert 'pattern=r"^qa_' not in BOT
    assert 'pattern=r"^cha_' not in BOT
    assert 'pattern=r"^qa:' in BOT
    assert 'pattern=r"^cha:' in BOT
