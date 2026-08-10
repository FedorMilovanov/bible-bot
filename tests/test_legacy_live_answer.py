from copy import deepcopy

import pytest

import legacy_live_answer as live
from session_integrity import QuizSessionStoreUnavailable


def _question():
    return {
        "question": "Who?",
        "options": ["Peter", "Paul"],
        "correct": 1,
    }


def _data(*, session_id="s1", attempt_id=None, current=0, questions=None):
    questions = questions or [_question()]
    data = {
        "session_id": session_id,
        "questions": questions,
        "current_question": current,
        "current_options": ["Peter", "Paul"],
        "current_correct_text": "Paul",
        "question_sent_at": 100.0,
        "answered_questions": [],
        "correct_answers": 0,
        "current_streak": 0,
        "max_streak": 0,
        "fastest_answer": None,
    }
    if attempt_id is not None:
        data["attempt_id"] = attempt_id
    return data


def _record(question, *, index=0, answer="Paul", correct=True, latency=5.0):
    return {
        "index": index,
        "qid": live.legacy_question_id(question),
        "user_answer": answer,
        "is_correct": correct,
        "question_obj": deepcopy(question),
        "latency_seconds": latency,
        "ts": "2026-08-10T12:00:05",
    }


def _durable(data, *, current, correct, records, attempt_id=None):
    return {
        "_id": data["session_id"],
        "attempt_id": attempt_id or data.get("attempt_id") or data["session_id"],
        "current_index": current,
        "correct_count": correct,
        "answered_questions": records,
    }


def test_durable_answer_cas_happens_before_ram_and_syncs_from_session(monkeypatch):
    data = _data(attempt_id="attempt-1")
    q = data["questions"][0]
    payload = live.build_live_answer_callback("qa", data, 0, 1)
    captured = {}

    def record(session_id, user_id, **kwargs):
        captured.update({"session_id": session_id, "user_id": user_id, **kwargs})
        return {
            "applied": True,
            "session": _durable(
                data,
                current=1,
                correct=1,
                records=[_record(q)],
            ),
        }

    monkeypatch.setattr(live, "record_owned_quiz_answer", record)

    outcome = live.apply_live_answer_once(42, data, payload, "qa", now=105.0)

    assert captured["session_id"] == "s1"
    assert captured["user_id"] == 42
    assert captured["expected_attempt_id"] == "attempt-1"
    assert captured["expected_index"] == 0
    assert captured["question_id"] == live.legacy_question_id(q)
    assert captured["user_answer"] == "Paul"
    assert captured["is_correct"] is True
    assert captured["latency_seconds"] == 5.0
    assert outcome.applied is True
    assert outcome.persisted is True
    assert outcome.correct_count == 1
    assert outcome.current_streak == 1
    assert data["attempt_id"] == "attempt-1"
    assert data["current_question"] == 1
    assert data["correct_answers"] == 1
    assert data["answered_questions"] == [{"question_obj": q, "user_answer": "Paul"}]
    assert data["fastest_answer"] == 5.0


def test_store_outage_does_not_advance_any_ram_counter(monkeypatch):
    data = _data(attempt_id="attempt-1")
    payload = live.build_live_answer_callback("qa", data, 0, 1)
    before = deepcopy(data)

    def unavailable(*args, **kwargs):
        raise QuizSessionStoreUnavailable("mongo down")

    monkeypatch.setattr(live, "record_owned_quiz_answer", unavailable)

    with pytest.raises(QuizSessionStoreUnavailable):
        live.apply_live_answer_once(42, data, payload, "qa", now=105.0)

    assert data == before


def test_wrong_attempt_token_is_rejected_before_store(monkeypatch):
    current = _data(session_id="container", attempt_id="attempt-new")
    old = _data(session_id="container", attempt_id="attempt-old")
    payload = live.build_live_answer_callback("qa", old, 0, 1)
    called = False

    def record(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("store must not be called")

    monkeypatch.setattr(live, "record_owned_quiz_answer", record)

    with pytest.raises(live.LegacyLiveAnswerStale, match="another attempt"):
        live.apply_live_answer_once(42, current, payload, "qa", now=105.0)

    assert called is False


def test_legacy_session_id_still_scopes_callback_without_attempt_field(monkeypatch):
    data = _data(session_id="legacy")
    payload = live.build_live_answer_callback("qa", data, 0, 1)
    q = data["questions"][0]
    captured = {}

    def record(_session_id, _user_id, **kwargs):
        captured.update(kwargs)
        return {
            "applied": True,
            "session": _durable(
                data,
                current=1,
                correct=1,
                records=[_record(q)],
                attempt_id="legacy",
            ),
        }

    monkeypatch.setattr(live, "record_owned_quiz_answer", record)

    live.apply_live_answer_once(42, data, payload, "qa", now=105.0)

    assert captured["expected_attempt_id"] == "legacy"
    assert data["attempt_id"] == "legacy"


def test_old_question_index_is_rejected_before_store(monkeypatch):
    questions = [_question(), _question()]
    data = _data(attempt_id="attempt-1", current=1, questions=questions)
    payload_data = _data(attempt_id="attempt-1", current=0, questions=questions)
    payload = live.build_live_answer_callback("qa", payload_data, 0, 1)
    called = False

    def record(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("store must not be called")

    monkeypatch.setattr(live, "record_owned_quiz_answer", record)

    with pytest.raises(live.LegacyLiveAnswerStale, match="another question"):
        live.apply_live_answer_once(42, data, payload, "qa", now=105.0)

    assert called is False


def test_duplicate_question_ids_remain_ordered_by_expected_index(monkeypatch):
    q = _question()
    questions = [deepcopy(q), deepcopy(q)]
    data = _data(attempt_id="attempt-1", current=1, questions=questions)
    data["answered_questions"] = [{"question_obj": q, "user_answer": "Paul"}]
    data["correct_answers"] = 1
    data["current_streak"] = 1
    data["max_streak"] = 1
    payload = live.build_live_answer_callback("qa", data, 1, 1)
    captured = {}

    def record(session_id, user_id, **kwargs):
        captured.update(kwargs)
        return {
            "applied": True,
            "session": _durable(
                data,
                current=2,
                correct=2,
                records=[_record(q, index=0), _record(q, index=1)],
            ),
        }

    monkeypatch.setattr(live, "record_owned_quiz_answer", record)

    outcome = live.apply_live_answer_once(42, data, payload, "qa", now=105.0)

    assert captured["expected_attempt_id"] == "attempt-1"
    assert captured["expected_index"] == 1
    assert captured["question_id"] == live.legacy_question_id(q)
    assert outcome.current_index == 2
    assert outcome.max_streak == 2


def test_exact_store_replay_rebuilds_ram_without_double_increment(monkeypatch):
    data = _data(attempt_id="attempt-1")
    q = data["questions"][0]
    payload = live.build_live_answer_callback("qa", data, 0, 1)

    monkeypatch.setattr(
        live,
        "record_owned_quiz_answer",
        lambda *args, **kwargs: {
            "applied": False,
            "session": _durable(
                data,
                current=1,
                correct=1,
                records=[_record(q)],
            ),
        },
    )

    outcome = live.apply_live_answer_once(42, data, payload, "qa", now=105.0)

    assert outcome.applied is False
    assert outcome.correct_count == 1
    assert data["correct_answers"] == 1
    assert len(data["answered_questions"]) == 1


def test_durable_counter_mismatch_fails_closed_after_store_response(monkeypatch):
    data = _data(attempt_id="attempt-1")
    q = data["questions"][0]
    payload = live.build_live_answer_callback("qa", data, 0, 1)

    monkeypatch.setattr(
        live,
        "record_owned_quiz_answer",
        lambda *args, **kwargs: {
            "applied": True,
            "session": _durable(
                data,
                current=1,
                correct=0,
                records=[_record(q, correct=True)],
            ),
        },
    )

    with pytest.raises(live.LegacyLiveStateInvalid, match="correct_count contradicts"):
        live.apply_live_answer_once(42, data, payload, "qa", now=105.0)


def test_timeout_uses_same_attempt_and_index_cas(monkeypatch):
    data = _data(attempt_id="attempt-1")
    q = data["questions"][0]
    captured = {}

    def record(session_id, user_id, **kwargs):
        captured.update(kwargs)
        return {
            "applied": True,
            "session": _durable(
                data,
                current=1,
                correct=0,
                records=[_record(q, answer="⏱ Время вышло", correct=False, latency=30.0)],
            ),
        }

    monkeypatch.setattr(live, "record_owned_quiz_answer", record)

    outcome = live.apply_live_timeout_once(
        42,
        data,
        0,
        expected_attempt_id="attempt-1",
        now=130.0,
    )

    assert captured["expected_attempt_id"] == "attempt-1"
    assert captured["expected_index"] == 0
    assert captured["user_answer"] == "⏱ Время вышло"
    assert captured["is_correct"] is False
    assert captured["latency_seconds"] == 30.0
    assert outcome.current_index == 1
    assert outcome.correct_count == 0
    assert outcome.current_streak == 0


def test_timeout_from_old_attempt_after_restart_is_rejected_before_store(monkeypatch):
    data = _data(session_id="container", attempt_id="attempt-new", current=0)
    called = False

    def record(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("store must not be called")

    monkeypatch.setattr(live, "record_owned_quiz_answer", record)

    with pytest.raises(live.LegacyLiveAnswerStale, match="another attempt"):
        live.apply_live_timeout_once(
            42,
            data,
            0,
            expected_attempt_id="attempt-old",
            now=130.0,
        )

    assert called is False
    assert data["current_question"] == 0
    assert data["answered_questions"] == []


def test_timeout_store_outage_leaves_ram_unchanged(monkeypatch):
    data = _data(attempt_id="attempt-1")
    before = deepcopy(data)

    monkeypatch.setattr(
        live,
        "record_owned_quiz_answer",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            QuizSessionStoreUnavailable("mongo down")
        ),
    )

    with pytest.raises(QuizSessionStoreUnavailable):
        live.apply_live_timeout_once(
            42,
            data,
            0,
            expected_attempt_id="attempt-1",
            now=130.0,
        )

    assert data == before


def test_retry_error_session_is_memory_only_but_still_session_scoped(monkeypatch):
    data = _data(session_id=None)
    payload = live.build_live_answer_callback("qa", data, 0, 1)
    scope = data["callback_scope_id"]

    def should_not_persist(*args, **kwargs):
        raise AssertionError("retry-errors must remain memory-only")

    monkeypatch.setattr(live, "record_owned_quiz_answer", should_not_persist)

    outcome = live.apply_live_answer_once(42, data, payload, "qa", now=105.0)

    assert outcome.persisted is False
    assert outcome.applied is True
    assert data["callback_scope_id"] == scope
    assert data["current_question"] == 1
    assert data["correct_answers"] == 1


def test_invalid_memory_only_index_fails_before_answer_list_mutation(monkeypatch):
    data = _data(session_id=None)
    data["current_question"] = "broken"
    before = deepcopy(data)

    def should_not_persist(*args, **kwargs):
        raise AssertionError("memory-only path must not touch Mongo")

    monkeypatch.setattr(live, "record_owned_quiz_answer", should_not_persist)

    with pytest.raises(live.LegacyLiveStateInvalid, match="current_question is invalid"):
        live._apply_memory_only(
            data,
            question=data["questions"][0],
            user_answer="Paul",
            is_correct=True,
            latency_seconds=5.0,
        )

    assert data == before
