import pytest

import legacy_session_launch as launch


def _question(qid):
    return {"id": qid, "question": f"Question {qid}"}


def _partial():
    q1 = _question("q1")
    return {
        "_id": "container-1",
        "attempt_id": "attempt-old",
        "user_id": "42",
        "status": "in_progress",
        "mode": "level",
        "level_key": "easy",
        "level_name": "Easy",
        "question_ids": ["q1", "q2"],
        "questions_data": [q1, _question("q2")],
        "current_index": 1,
        "correct_count": 1,
        "answered_questions": [
            {
                "index": 0,
                "qid": "q1",
                "user_answer": "A",
                "is_correct": True,
                "question_obj": q1,
                "ts": "1970-01-01T00:01:41",
            }
        ],
        "time_limit": None,
        "start_time": 100.0,
    }


def _complete():
    q1 = _question("q1")
    return {
        "_id": "container-1",
        "attempt_id": "attempt-old",
        "user_id": "42",
        "status": "in_progress",
        "mode": "level",
        "level_key": "easy",
        "level_name": "Easy",
        "question_ids": ["q1"],
        "questions_data": [q1],
        "current_index": 1,
        "correct_count": 1,
        "answered_questions": [
            {
                "index": 0,
                "qid": "q1",
                "user_answer": "A",
                "is_correct": True,
                "question_obj": q1,
                "ts": "1970-01-01T00:01:41",
            }
        ],
        "time_limit": None,
        "start_time": 100.0,
    }


def _launch(**overrides):
    kwargs = {
        "user_id": 42,
        "mode": "level",
        "question_ids": ["n1", "n2"],
        "questions_data": [_question("n1"), _question("n2")],
        "level_key": "medium",
        "level_name": "Medium",
        "time_limit": None,
        "chat_id": 100,
    }
    kwargs.update(overrides)
    return launch.launch_quiz_attempt(**kwargs)


def test_no_active_session_creates_durable_container(monkeypatch):
    monkeypatch.setattr(launch, "get_active_quiz_session_strict", lambda _uid: None)
    captured = {}

    def create(**kwargs):
        captured.update(kwargs)
        return {
            "_id": "container-new",
            "attempt_id": "container-new",
            "status": "in_progress",
        }

    monkeypatch.setattr(launch, "create_quiz_session_strict", create)

    result = _launch()

    assert result.session_id == "container-new"
    assert result.attempt_id == "container-new"
    assert captured["level_key"] == "medium"
    assert captured["question_ids"] == ["n1", "n2"]


def test_partial_active_attempt_blocks_generic_launch_without_mutation(monkeypatch):
    active = _partial()
    monkeypatch.setattr(launch, "get_active_quiz_session_strict", lambda _uid: active)
    monkeypatch.setattr(
        launch,
        "create_quiz_session_strict",
        lambda **_: pytest.fail("active attempt must not create another container"),
    )

    with pytest.raises(launch.LegacySessionLaunchActiveAttempt) as exc_info:
        _launch(mode="random20", level_key="random20", time_limit=20)

    exc = exc_info.value
    assert exc.session is active
    assert exc.session_id == "container-1"
    assert exc.attempt_id == "attempt-old"


def test_repeated_generic_launch_against_same_partial_attempt_stays_non_destructive(monkeypatch):
    active = _partial()
    monkeypatch.setattr(launch, "get_active_quiz_session_strict", lambda _uid: active)
    calls = 0

    def create(**_kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("generic retry must not create or replace")

    monkeypatch.setattr(launch, "create_quiz_session_strict", create)

    for _ in range(2):
        with pytest.raises(launch.LegacySessionLaunchActiveAttempt) as exc_info:
            _launch()
        assert exc_info.value.attempt_id == "attempt-old"
    assert calls == 0


def test_completed_active_result_blocks_new_launch_before_mutation(monkeypatch):
    monkeypatch.setattr(launch, "get_active_quiz_session_strict", lambda _uid: _complete())
    monkeypatch.setattr(
        launch,
        "create_quiz_session_strict",
        lambda **_: pytest.fail("completed evidence must not create another active container"),
    )

    with pytest.raises(launch.LegacySessionLaunchResultPending, match="finalized"):
        _launch()


def test_concurrent_create_race_fails_closed_instead_of_replacing_unknown_request(monkeypatch):
    monkeypatch.setattr(launch, "get_active_quiz_session_strict", lambda _uid: None)

    def create(**_kwargs):
        raise launch.QuizSessionAlreadyActive("other request won")

    monkeypatch.setattr(launch, "create_quiz_session_strict", create)

    with pytest.raises(launch.LegacySessionLaunchConflict, match="won"):
        _launch()


def test_lookup_and_create_outages_are_explicit(monkeypatch):
    def lookup_fail(_uid):
        raise launch.QuizSessionAccessUnavailable("mongo down")

    monkeypatch.setattr(launch, "get_active_quiz_session_strict", lookup_fail)
    with pytest.raises(launch.LegacySessionLaunchUnavailable, match="lookup"):
        _launch()

    monkeypatch.setattr(launch, "get_active_quiz_session_strict", lambda _uid: None)
    monkeypatch.setattr(
        launch,
        "create_quiz_session_strict",
        lambda **_: (_ for _ in ()).throw(
            launch.QuizSessionAccessUnavailable("mongo down")
        ),
    )
    with pytest.raises(launch.LegacySessionLaunchUnavailable, match="creation"):
        _launch()


def test_ambiguous_duplicate_active_lookup_is_launch_conflict(monkeypatch):
    monkeypatch.setattr(
        launch,
        "get_active_quiz_session_strict",
        lambda _uid: (_ for _ in ()).throw(
            launch.QuizSessionAccessSchemaInvalid("multiple active sessions")
        ),
    )
    monkeypatch.setattr(
        launch,
        "create_quiz_session_strict",
        lambda **_: pytest.fail("ambiguous active state must never create"),
    )

    with pytest.raises(launch.LegacySessionLaunchConflict, match="ambiguous"):
        _launch()


def test_corrupt_active_session_is_never_replaced(monkeypatch):
    bad = _partial()
    bad["correct_count"] = "1"
    monkeypatch.setattr(launch, "get_active_quiz_session_strict", lambda _uid: bad)
    monkeypatch.setattr(
        launch,
        "create_quiz_session_strict",
        lambda **_: pytest.fail("corrupt active session must not create another container"),
    )

    with pytest.raises(launch.LegacySessionLaunchConflict, match="contradictory"):
        _launch()


def test_invalid_new_spec_fails_in_strict_create_without_fallback(monkeypatch):
    monkeypatch.setattr(launch, "get_active_quiz_session_strict", lambda _uid: None)
    monkeypatch.setattr(
        launch,
        "create_quiz_session_strict",
        lambda **_: (_ for _ in ()).throw(ValueError("time_limit invalid")),
    )

    with pytest.raises(ValueError, match="time_limit"):
        _launch(time_limit=0)
