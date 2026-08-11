from pathlib import Path


BOT = (Path(__file__).resolve().parents[1] / "bot.py").read_text(encoding="utf-8")


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


def test_answer_authority_and_lifecycle_migrate_as_one_controller_boundary():
    if not any(marker in BOT for marker in (*ANSWER_MARKERS, *LIFECYCLE_MARKERS)):
        # The controller is still fully historical. The first marker from either
        # side activates this cross-boundary contract: in-place restart and
        # attempt-bound answer authority must never be deployed separately.
        return

    for marker in ANSWER_MARKERS:
        assert marker in BOT
    for marker in LIFECYCLE_MARKERS:
        assert marker in BOT

    # Lifecycle-first is unsafe because atomic restart preserves the Mongo
    # container id: an old blind answer/timer writer that knows only session_id
    # could mutate the replacement attempt. Answer-first is unsafe because the
    # historical global cancel/create paths can still erase completed evidence.
    for historical in (
        "advance_quiz_session(",
        "set_question_sent_at(",
        "update_quiz_session(",
        "create_quiz_session(",
        "cancel_active_quiz_session(",
        "cancel_quiz_session(",
    ):
        assert historical not in BOT
