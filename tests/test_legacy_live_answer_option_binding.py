from copy import deepcopy

import pytest

import legacy_live_answer as live


def _question():
    return {
        "question": "Who?",
        "options": ["Peter", "Paul", "John"],
        "correct": 1,
    }


def _data():
    return {
        "session_id": "container-1",
        "attempt_id": "attempt-1",
        "questions": [_question()],
        "current_question": 0,
        "current_options": ["Peter", "Paul", "John"],
        "current_correct_text": "Paul",
        "question_sent_at": 100.0,
        "answered_questions": [],
        "correct_answers": 0,
        "current_streak": 0,
        "max_streak": 0,
        "fastest_answer": None,
    }


def test_old_callback_is_rejected_if_same_question_slot_now_contains_other_option(monkeypatch):
    data = _data()
    payload = live.build_live_answer_callback("qa", data, 0, 0)
    before_reshuffle = deepcopy(data)

    # Same durable attempt and question index, but a later render changed the
    # display order. The old button's slot 0 represented Peter; it must never be
    # reinterpreted as Paul merely because RAM now has a different shuffle.
    data["current_options"] = ["Paul", "Peter", "John"]
    called = False

    def record(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("stale option mapping must fail before Mongo")

    monkeypatch.setattr(live, "record_owned_quiz_answer", record)

    with pytest.raises(live.LegacyLiveAnswerStale, match="mapping changed"):
        live.apply_live_answer_once(42, data, payload, "qa", now=105.0)

    assert called is False
    assert data["current_question"] == before_reshuffle["current_question"]
    assert data["correct_answers"] == before_reshuffle["correct_answers"]
    assert data["answered_questions"] == before_reshuffle["answered_questions"]


def test_callback_remains_valid_if_rerender_keeps_same_option_in_same_slot(monkeypatch):
    data = _data()
    payload = live.build_live_answer_callback("qa", data, 0, 1)
    question = data["questions"][0]

    def record(_session_id, _user_id, **kwargs):
        assert kwargs["user_answer"] == "Paul"
        return {
            "applied": True,
            "session": {
                "_id": "container-1",
                "attempt_id": "attempt-1",
                "current_index": 1,
                "correct_count": 1,
                "answered_questions": [
                    {
                        "index": 0,
                        "qid": live.legacy_question_id(question),
                        "user_answer": "Paul",
                        "is_correct": True,
                        "question_obj": question,
                        "latency_seconds": 5.0,
                    }
                ],
            },
        }

    monkeypatch.setattr(live, "record_owned_quiz_answer", record)

    outcome = live.apply_live_answer_once(42, data, payload, "qa", now=105.0)

    assert outcome.applied is True
    assert outcome.user_answer == "Paul"
    assert outcome.is_correct is True
    assert data["current_question"] == 1
