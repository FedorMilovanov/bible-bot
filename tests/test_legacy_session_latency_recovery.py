from datetime import datetime

from config import SPEED_MODE_TIMEOUT
from legacy_session_recovery import persisted_fastest_answer, recovery_fields


def _session(answered_questions):
    return {
        "_id": "session-latency",
        "user_id": "42",
        "status": "in_progress",
        "mode": "level",
        "questions_data": [{"id": f"q{i}"} for i in range(len(answered_questions) or 1)],
        "current_index": len(answered_questions),
        "correct_count": sum(
            1 for item in answered_questions if isinstance(item, dict) and item.get("is_correct") is True
        ),
        "answered_questions": answered_questions,
        "level_key": "easy",
        "level_name": "Easy",
        "chat_id": 42,
        "start_time": datetime(2026, 8, 10, 12, 0, 0).timestamp(),
        "time_limit": SPEED_MODE_TIMEOUT,
    }


def _answer(index, *, latency_marker="missing"):
    item = {
        "index": index,
        "qid": f"q{index}",
        "user_answer": "A",
        "is_correct": True,
        "ts": f"2026-08-10T12:00:{index + 1:02d}",
    }
    if latency_marker != "missing":
        item["latency_seconds"] = latency_marker
    return item


def test_legacy_answers_without_latency_keep_fastest_unknown():
    session = _session([_answer(0), _answer(1)])

    assert persisted_fastest_answer(session) is None
    assert recovery_fields(session)["fastest_answer"] is None


def test_new_answer_records_recover_fastest_proven_latency():
    session = _session([
        _answer(0, latency_marker=4.2),
        _answer(1, latency_marker=2.7),
        _answer(2, latency_marker=3.1),
    ])

    assert persisted_fastest_answer(session) == 2.7
    assert recovery_fields(session)["fastest_answer"] == 2.7


def test_mixed_legacy_and_new_records_can_prove_a_fast_answer():
    session = _session([
        _answer(0),
        _answer(1, latency_marker=2.9),
    ])

    assert persisted_fastest_answer(session) == 2.9


def test_explicit_none_latency_is_unknown_not_zero():
    session = _session([_answer(0, latency_marker=None)])

    assert persisted_fastest_answer(session) is None


def test_present_malformed_latency_invalidates_speed_achievement_evidence():
    for bad in (True, "fast", -1, float("inf"), float("nan")):
        session = _session([
            _answer(0, latency_marker=2.5),
            _answer(1, latency_marker=bad),
        ])
        assert persisted_fastest_answer(session) is None


def test_malformed_latency_does_not_change_recovered_quiz_mode():
    session = _session([_answer(0, latency_marker="broken")])
    fields = recovery_fields(session)

    assert fields["quiz_mode"] == "speed"
    assert fields["score_multiplier"] == 2.0
    assert fields["fastest_answer"] is None
