import pytest

import legacy_live_question as question
from legacy_live_answer import LegacyLiveAnswerStale


def _data(*, attempt_id="attempt-1", index=0, session_id="container-1"):
    return {
        "session_id": session_id,
        "attempt_id": attempt_id,
        "questions": [{"id": "q1"}, {"id": "q2"}],
        "current_question": index,
    }


def test_capture_target_is_attempt_and_index_bound():
    target = question.capture_live_question_target(_data())
    assert target.attempt_id == "attempt-1"
    assert target.question_index == 0


def test_persisted_send_uses_timer_store_and_syncs_canonical_timestamp(monkeypatch):
    data = _data()
    target = question.capture_live_question_target(data)
    captured = {}

    def mark(session_id, user_id, **kwargs):
        captured.update(session_id=session_id, user_id=user_id, **kwargs)
        return {
            "applied": False,
            "sent_at": 99.5,
            "session": {
                "_id": "container-1",
                "attempt_id": "attempt-1",
                "current_index": 0,
            },
        }

    monkeypatch.setattr(question, "mark_question_sent_once", mark)
    result = question.mark_live_question_sent(42, data, target, sent_at=100.0)
    assert result == 99.5
    assert data["question_sent_at"] == 99.5
    assert captured["expected_attempt_id"] == "attempt-1"
    assert captured["expected_index"] == 0


def test_restart_before_marker_makes_old_target_stale_without_store(monkeypatch):
    data = _data(attempt_id="attempt-old")
    target = question.capture_live_question_target(data)
    data["attempt_id"] = "attempt-new"
    called = False

    def mark(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("stale target must not reach timer store")

    monkeypatch.setattr(question, "mark_question_sent_once", mark)
    with pytest.raises(LegacyLiveAnswerStale, match="no longer current"):
        question.mark_live_question_sent(42, data, target, sent_at=100.0)
    assert called is False
    assert "question_sent_at" not in data


def test_question_advance_before_marker_is_stale(monkeypatch):
    data = _data(index=0)
    target = question.capture_live_question_target(data)
    data["current_question"] = 1
    monkeypatch.setattr(question, "mark_question_sent_once", lambda *_args, **_kwargs: pytest.fail("old timer must not write"))
    with pytest.raises(LegacyLiveAnswerStale):
        question.mark_live_question_sent(42, data, target, sent_at=100.0)


def test_memory_only_review_uses_same_target_but_no_mongo(monkeypatch):
    data = _data(session_id=None)
    data.pop("attempt_id")
    target = question.capture_live_question_target(data)
    monkeypatch.setattr(question, "mark_question_sent_once", lambda *_args, **_kwargs: pytest.fail("memory-only review must not persist"))
    result = question.mark_live_question_sent(42, data, target, sent_at=12.5)
    assert result == 12.5
    assert data["question_sent_at"] == 12.5
    assert data["callback_scope_id"] == target.attempt_id


def test_invalid_current_question_or_sent_at_fails_before_store(monkeypatch):
    with pytest.raises(question.LegacyLiveQuestionStateInvalid, match="current_question"):
        question.capture_live_question_target(_data(index=-1))
    data = _data()
    target = question.capture_live_question_target(data)
    monkeypatch.setattr(question, "mark_question_sent_once", lambda *_args, **_kwargs: pytest.fail("invalid sent_at must fail first"))
    with pytest.raises(ValueError, match="sent_at"):
        question.mark_live_question_sent(42, data, target, sent_at=float("inf"))
