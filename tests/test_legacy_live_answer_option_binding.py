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


def test_old_callback_keeps_original_answer_identity_after_same_question_reshuffle(monkeypatch):
    data = _data()
    question = data["questions"][0]
    payload = live.build_live_answer_callback("qa", data, 0, 0)

    # The old rendered slot 0 meant Peter. A later render of the same durable
    # attempt/question changed slot 0 to Paul. Callback semantics must remain
    # Peter so an already-committed lost-response transition can replay exactly.
    data["current_options"] = ["Paul", "Peter", "John"]

    def record(_session_id, _user_id, **kwargs):
        assert kwargs["user_answer"] == "Peter"
        assert kwargs["is_correct"] is False
        return {
            "applied": False,
            "session": {
                "_id": "container-1",
                "attempt_id": "attempt-1",
                "current_index": 1,
                "correct_count": 0,
                "answered_questions": [
                    {
                        "index": 0,
                        "qid": live.legacy_question_id(question),
                        "user_answer": "Peter",
                        "is_correct": False,
                        "question_obj": question,
                        "latency_seconds": 5.0,
                    }
                ],
            },
        }

    monkeypatch.setattr(live, "record_owned_quiz_answer", record)

    outcome = live.apply_live_answer_once(42, data, payload, "qa", now=105.0)

    assert outcome.applied is False
    assert outcome.user_answer == "Peter"
    assert outcome.is_correct is False
    assert data["current_question"] == 1
    assert data["answered_questions"][0]["user_answer"] == "Peter"


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


def test_callback_builder_rejects_non_permutation_current_options():
    data = _data()
    data["current_options"] = ["Peter", "Paul", "Eve"]

    with pytest.raises(live.LegacyLiveStateInvalid, match="not a permutation"):
        live.build_live_answer_callback("qa", data, 0, 0)
