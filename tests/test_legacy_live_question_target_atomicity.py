from copy import deepcopy

import pytest

import legacy_live_question as live_question


def test_invalid_memory_only_question_does_not_allocate_callback_scope():
    data = {
        "session_id": None,
        "current_question": "broken",
        "questions": [{"question": "Q"}],
    }
    before = deepcopy(data)

    with pytest.raises(
        live_question.LegacyLiveQuestionStateInvalid,
        match="current_question is invalid",
    ):
        live_question.capture_live_question_target(data)

    assert data == before
    assert "callback_scope_id" not in data


def test_unrenderable_memory_only_question_does_not_allocate_callback_scope():
    data = {
        "session_id": None,
        "current_question": 1,
        "questions": [{"question": "Q"}],
    }
    before = deepcopy(data)

    with pytest.raises(
        live_question.LegacyLiveQuestionStateInvalid,
        match="not renderable",
    ):
        live_question.capture_live_question_target(data)

    assert data == before
    assert "callback_scope_id" not in data
