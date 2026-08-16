import asyncio
import inspect
import threading

import telegram_challenge_controller as challenge
import telegram_quiz_runtime_controller as quiz


def test_status_lookup_does_not_block_event_loop(monkeypatch):
    release = threading.Event()
    backup_fired = threading.Event()

    def blocking_lookup(user_id):
        assert user_id == 314
        release.wait(timeout=1.0)
        return None

    def emergency_release():
        backup_fired.set()
        release.set()

    monkeypatch.setattr(quiz, "get_active_quiz_session_strict", blocking_lookup)

    async def scenario():
        timer = threading.Timer(0.4, emergency_release)
        timer.start()
        try:
            lookup = asyncio.create_task(quiz._status_session(314))
            await asyncio.sleep(0.02)
            assert not backup_fired.is_set()
            assert not lookup.done()
            release.set()
            assert await asyncio.wait_for(lookup, timeout=0.5) == (None, "none")
        finally:
            release.set()
            timer.cancel()

    asyncio.run(scenario())


def test_quiz_latency_sensitive_persistence_uses_thread_boundary():
    functions = {
        "show_results": ("finalize_live_persisted_attempt",),
        "show_challenge_results": ("finalize_live_persisted_attempt",),
        "_launch_attempt": ("launch_quiz_attempt", "get_active_quiz_session_strict"),
        "_send_current_question": ("mark_live_question_sent",),
        "_handle_inline_answer": ("apply_live_answer_once", "record_question_stat"),
        "_handle_question_timeout": ("apply_live_timeout_once", "record_question_stat"),
        "resume_session_handler": ("resolve_session_action",),
        "cancel_session_handler": ("resolve_session_action", "cancel_owned_incomplete_quiz_attempt"),
        "_cancel_current": ("cancel_current_incomplete_session",),
        "_status_session": ("get_active_quiz_session_strict",),
        "remind_unfinished_tests_job": ("get_stale_sessions",),
    }
    for function_name, persistence_calls in functions.items():
        source = inspect.getsource(getattr(quiz, function_name))
        assert "_run_blocking_io(" in source, function_name
        for persistence_call in persistence_calls:
            assert persistence_call in source, (function_name, persistence_call)


def test_blocking_boundary_has_no_process_local_mutex_or_retry_loop():
    source = inspect.getsource(quiz._run_blocking_io)
    assert "asyncio.to_thread" in source
    assert "Lock(" not in source
    assert "while " not in source
    assert "sleep(" not in source


def test_challenge_restart_uses_same_thread_boundary():
    source = inspect.getsource(challenge.restart_session_handler)
    assert source.count("quiz._run_blocking_io(") >= 2
    assert "resolve_session_action" in source
    assert "restart_owned_quiz_attempt" in source
