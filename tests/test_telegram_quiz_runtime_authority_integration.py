from __future__ import annotations

import os

os.environ.setdefault("ADMIN_USER_ID", "1")
os.environ.setdefault("DISABLE_WEB_SERVER", "true")

import achievement_catalog
import question_identity
import quiz_answer_history
import telegram_answer_animation
import telegram_conversation_states as conversation_states
import telegram_production as production
import telegram_quiz_result_menu
import telegram_quiz_runtime_controller as quiz
import telegram_quiz_runtime_state as quiz_runtime


def test_production_uses_canonical_quiz_runtime_module_directly():
    assert production.quiz is quiz
    assert production.quiz.ANSWERING == conversation_states.ANSWERING
    assert production.quiz.user_data is quiz_runtime.get_user_data()
    assert not hasattr(production, "legacy")


def test_quiz_runtime_helpers_have_direct_canonical_identity():
    assert quiz.create_session_data is quiz_runtime.create_session_data
    assert quiz.get_qid is question_identity.get_qid
    assert quiz.is_wrong is quiz_answer_history.is_wrong
    assert quiz.build_progress_bar is quiz_answer_history.build_progress_bar
    assert quiz.ACHIEVEMENTS is achievement_catalog.ACHIEVEMENTS
    assert quiz.animate_answer_buttons is telegram_answer_animation.animate_answer_buttons
    assert quiz.send_final_results_menu is telegram_quiz_result_menu.send_final_results_menu


def test_quiz_runtime_source_has_no_transitional_authority_spelling():
    from pathlib import Path

    source = Path(quiz.__file__).read_text(encoding="utf-8")
    assert "import bot" not in source
    assert "from bot" not in source
    assert "legacy." not in source
    assert "import telegram_controller" not in source
    assert "from telegram_controller" not in source
