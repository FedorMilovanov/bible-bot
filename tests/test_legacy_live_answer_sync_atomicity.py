from copy import deepcopy

import pytest

import legacy_live_answer as live


def test_invalid_durable_snapshot_does_not_partially_rebind_ram_attempt():
    data = {
        "session_id": "container-1",
        "attempt_id": "attempt-current",
        "current_question": 0,
        "correct_answers": 0,
        "answered_questions": [],
        "current_streak": 0,
        "max_streak": 0,
        "fastest_answer": None,
    }
    before = deepcopy(data)
    invalid_session = {
        "_id": "container-1",
        "attempt_id": "attempt-other",
        "current_index": 1,
        "correct_count": 0,
        "answered_questions": [
            {
                "index": 0,
                "qid": "q1",
                "user_answer": "A",
                "is_correct": True,
                "question_obj": {"question": "Q", "options": ["A"], "correct": 0},
                "latency_seconds": 1.0,
            }
        ],
    }

    with pytest.raises(live.LegacyLiveStateInvalid, match="correct_count contradicts"):
        live._sync_ram_from_session(data, invalid_session)

    assert data == before
