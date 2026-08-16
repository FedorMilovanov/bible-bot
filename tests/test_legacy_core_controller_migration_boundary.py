from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CORE = (ROOT / "telegram_quiz_controller.py").read_text(encoding="utf-8")
COMPAT = (ROOT / "telegram_controller.py").read_text(encoding="utf-8")


ANSWER_MARKERS = (
    "build_live_answer_callback(",
    "apply_live_answer_once(",
    "apply_live_timeout_once(",
    "mark_live_question_sent(",
)
LIFECYCLE_MARKERS = (
    "launch_quiz_attempt(",
    "restart_owned_quiz_attempt(",
    "cancel_current_incomplete_session(",
    "resolve_session_action(",
    "session_action_payloads(",
)


def test_production_core_migrates_answer_and_lifecycle_as_one_boundary():
    for marker in ANSWER_MARKERS:
        assert marker in CORE
    for marker in LIFECYCLE_MARKERS:
        assert marker in CORE

    # The production core must have exactly one state authority. Historical
    # blind/global writers cannot coexist with attempt-bound answer/lifecycle CAS.
    for historical in (
        "advance_quiz_session(",
        "set_question_sent_at(",
        "update_quiz_session(",
        "create_quiz_session(",
        "cancel_active_quiz_session(",
        "cancel_quiz_session(",
    ):
        assert historical not in CORE


def test_bot_free_core_and_compat_name_do_not_delegate_state_back_to_legacy():
    assert "import bot" not in CORE
    assert "from bot" not in CORE
    assert "legacy." not in CORE
    assert "import bot" not in COMPAT
    assert "from bot" not in COMPAT
    assert "legacy." not in COMPAT

    forbidden_delegations = (
        "legacy.quiz_inline_answer",
        "legacy.challenge_inline_answer",
        "legacy.send_question",
        "legacy.send_challenge_question",
        "legacy.resume_session_handler",
        "legacy.restart_session_handler",
        "legacy.cancel_session_handler",
        "legacy.cancel_quiz_handler",
        "legacy.show_results",
        "legacy.show_challenge_results",
    )
    for marker in forbidden_delegations:
        assert marker not in CORE


def test_compat_module_resolves_to_one_mutable_core_module_object():
    assert "import telegram_quiz_controller as _core" in COMPAT
    assert "sys.modules[__name__] = _core" in COMPAT
