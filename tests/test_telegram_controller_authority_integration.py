from __future__ import annotations

import os

os.environ.setdefault("ADMIN_USER_ID", "1")
os.environ.setdefault("DISABLE_WEB_SERVER", "true")

import achievement_catalog
import course_catalog
import question_identity
import quiz_answer_history
import telegram_answer_animation
import telegram_conversation_states as conversation_states
import telegram_production as production
import telegram_quiz_result_menu
import telegram_quiz_runtime_state as quiz_runtime
import telegram_report_state as report_state


_STRUCTURAL_LEVEL_FIELDS = ("pool_key", "points_per_q", "num_questions")


def test_controller_legacy_metadata_resolves_to_catalog_and_canonical_states():
    canonical = course_catalog.legacy_level_config()
    assert production.legacy.LEVEL_CONFIG
    assert set(production.legacy.LEVEL_CONFIG).issubset(canonical)

    for key, current in production.legacy.LEVEL_CONFIG.items():
        expected = canonical[key]
        for field in _STRUCTURAL_LEVEL_FIELDS:
            assert current[field] == expected[field]
        assert isinstance(current["name"], str) and current["name"].strip()

    # The compatibility bridge intentionally preserves deployed copy while the
    # routing/scoring/count fields come from the catalog.
    assert production.legacy.LEVEL_CONFIG["level_linguistics_ch1"]["name"].endswith("(ч.1)")

    assert production.quiz.CHOOSING_LEVEL == conversation_states.CHOOSING_LEVEL
    assert production.quiz.ANSWERING == conversation_states.ANSWERING
    assert production.quiz.BATTLE_ANSWERING == conversation_states.BATTLE_ANSWERING
    assert production.quiz.REPORT_TEXT == report_state.REPORT_TEXT
    assert production.quiz.REPORT_PHOTO == report_state.REPORT_PHOTO
    assert production.quiz.REPORT_CONFIRM == report_state.REPORT_CONFIRM


def test_controller_transitional_runtime_and_pure_helpers_have_canonical_identity():
    assert production.quiz.user_data is quiz_runtime.get_user_data()
    assert production.legacy.user_data is quiz_runtime.get_user_data()
    assert production.legacy.user_locks is quiz_runtime.get_user_locks()
    assert production.legacy._create_session_data is quiz_runtime.create_session_data
    assert production.legacy._reset_bad_input is quiz_runtime.reset_bad_input
    assert production.legacy._inc_bad_input is quiz_runtime.increment_bad_input

    assert production.legacy.get_qid is question_identity.get_qid
    assert production.legacy.stable_question_id is question_identity.stable_question_id
    assert production.legacy._is_wrong is quiz_answer_history.is_wrong
    assert production.legacy._correct_text is quiz_answer_history.correct_text
    assert production.legacy.build_progress_bar is quiz_answer_history.build_progress_bar
    assert production.legacy.ACHIEVEMENTS is achievement_catalog.ACHIEVEMENTS


def test_controller_transitional_presentation_calls_have_canonical_identity():
    assert (
        production.legacy.send_final_results_menu
        is telegram_quiz_result_menu.send_final_results_menu
    )
    assert (
        production.legacy._animate_answer_buttons
        is telegram_answer_animation.animate_answer_buttons
    )
