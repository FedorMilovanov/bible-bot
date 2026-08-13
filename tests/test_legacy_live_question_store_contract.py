from copy import deepcopy

import pytest

import legacy_live_question as question


def _data():
    return {
        "session_id": "container-1",
        "attempt_id": "attempt-1",
        "questions": [{"id": "q1"}],
        "current_question": 0,
    }


def _target(data):
    return question.capture_live_question_target(data)


@pytest.mark.parametrize(
    "store_result,match",
    [
        ({"applied": "yes", "sent_at": 10.0, "session": {}}, "invalid state"),
        (
            {
                "applied": True,
                "sent_at": 10.0,
                "session": {
                    "_id": "other-container",
                    "attempt_id": "attempt-1",
                    "current_index": 0,
                },
            },
            "invalid session",
        ),
        (
            {
                "applied": True,
                "sent_at": 10.0,
                "session": {
                    "_id": "container-1",
                    "attempt_id": "attempt-other",
                    "current_index": 0,
                },
            },
            "another attempt",
        ),
        (
            {
                "applied": True,
                "sent_at": 10.0,
                "session": {
                    "_id": "container-1",
                    "attempt_id": "attempt-1",
                    "current_index": 1,
                },
            },
            "another question",
        ),
    ],
)
def test_invalid_store_response_never_installs_ram_timer(monkeypatch, store_result, match):
    data = _data()
    target = _target(data)
    before = deepcopy(data)
    monkeypatch.setattr(question, "mark_question_sent_once", lambda *_args, **_kwargs: store_result)

    with pytest.raises(question.LegacyLiveQuestionStateInvalid, match=match):
        question.mark_live_question_sent(42, data, target, sent_at=10.0)

    assert data == before
    assert "question_sent_at" not in data
