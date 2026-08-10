from copy import deepcopy

import pytest

import legacy_live_answer as live


def _question():
    return {
        "question": "Who?",
        "options": ["Peter", "Paul"],
        "correct": 1,
    }


def _data():
    return {
        "session_id": "container-1",
        "attempt_id": "attempt-1",
        "questions": [_question()],
        "current_question": 0,
        "current_options": ["Peter", "Paul"],
        "current_correct_text": "Paul",
        "question_sent_at": 100.0,
        "answered_questions": [],
        "correct_answers": 0,
        "current_streak": 0,
        "max_streak": 0,
        "fastest_answer": None,
    }


def _record(*, answer="Paul", correct=True, qid=None):
    question = _question()
    return {
        "index": 0,
        "qid": qid or live.legacy_question_id(question),
        "user_answer": answer,
        "is_correct": correct,
        "question_obj": question,
        "latency_seconds": 5.0,
    }


def _session(*, attempt_id="attempt-1", record=None, correct_count=1):
    return {
        "_id": "container-1",
        "attempt_id": attempt_id,
        "current_index": 1,
        "correct_count": correct_count,
        "answered_questions": [record or _record()],
    }


@pytest.mark.parametrize(
    "store_result, message",
    [
        (
            {"applied": "yes", "session": _session()},
            "invalid applied state",
        ),
        (
            {"applied": True, "session": _session(attempt_id="attempt-other")},
            "another quiz attempt",
        ),
        (
            {
                "applied": True,
                "session": _session(record=_record(qid="wrong-qid")),
            },
            "conflicting transition record",
        ),
    ],
)
def test_answer_store_postcondition_fails_before_ram_sync(
    monkeypatch,
    store_result,
    message,
):
    data = _data()
    before = deepcopy(data)
    payload = live.build_live_answer_callback("qa", data, 0, 1)
    before = deepcopy(data)

    monkeypatch.setattr(
        live,
        "record_owned_quiz_answer",
        lambda *_args, **_kwargs: store_result,
    )

    with pytest.raises(live.LegacyLiveStateInvalid, match=message):
        live.apply_live_answer_once(42, data, payload, "qa", now=105.0)

    assert data == before


def test_timeout_store_conflicting_tail_fails_before_ram_sync(monkeypatch):
    data = _data()
    before = deepcopy(data)
    monkeypatch.setattr(
        live,
        "record_owned_quiz_answer",
        lambda *_args, **_kwargs: {
            "applied": True,
            "session": _session(
                record=_record(answer="Paul", correct=True),
                correct_count=1,
            ),
        },
    )

    with pytest.raises(live.LegacyLiveStateInvalid, match="conflicting transition record"):
        live.apply_live_timeout_once(
            42,
            data,
            0,
            expected_attempt_id="attempt-1",
            now=130.0,
        )

    assert data == before
