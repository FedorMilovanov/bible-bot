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
    monkeypatch.setattr(
        launch,
        "restart_owned_quiz_attempt",
        lambda *_args, **_kwargs: pytest.fail("no active session must not restart"),
    )

    result = _launch()

    assert result.session_id == "container-new"
    assert result.attempt_id == "container-new"
    assert result.created_new_container is True
    assert result.replaced_incomplete_attempt is False
    assert captured["level_key"] == "medium"
    assert captured["question_ids"] == ["n1", "n2"]


def test_partial_active_attempt_is_atomically_replaced_in_same_container(monkeypatch):
    active = _partial()
    monkeypatch.setattr(launch, "get_active_quiz_session_strict", lambda _uid: active)
    captured = {}

    def restart(session_id, user_id, **kwargs):
        captured.update(session_id=session_id, user_id=user_id, **kwargs)
        return {
            "applied": True,
            "session": {
                "_id": "container-1",
                "attempt_id": "attempt-new",
                "status": "in_progress",
            },
        }

    monkeypatch.setattr(launch, "restart_owned_quiz_attempt", restart)
    monkeypatch.setattr(
        launch,
        "create_quiz_session_strict",
        lambda **_: pytest.fail("partial active attempt must not create second container"),
    )

    result = _launch(mode="random20", level_key="random20", time_limit=20)

    assert result.session_id == "container-1"
    assert result.attempt_id == "attempt-new"
    assert result.created_new_container is False
    assert result.replaced_incomplete_attempt is True
    assert captured["expected_attempt_id"] == "attempt-old"
    assert captured["mode"] == "random20"
    assert captured["level_key"] == "random20"
    assert captured["time_limit"] == 20


def test_completed_active_result_blocks_new_launch_before_mutation(monkeypatch):
    monkeypatch.setattr(launch, "get_active_quiz_session_strict", lambda _uid: _complete())
    monkeypatch.setattr(
        launch,
        "restart_owned_quiz_attempt",
        lambda *_args, **_kwargs: pytest.fail("completed evidence must not be replaced"),
    )
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


def test_replace_race_is_explicit(monkeypatch):
    monkeypatch.setattr(launch, "get_active_quiz_session_strict", lambda _uid: _partial())
    monkeypatch.setattr(
        launch,
        "restart_owned_quiz_attempt",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            launch.QuizSessionLifecycleConflict("changed")
        ),
    )

    with pytest.raises(launch.LegacySessionLaunchConflict, match="changed"):
        _launch()


def test_corrupt_active_session_is_never_replaced(monkeypatch):
    bad = _partial()
    bad["correct_count"] = "1"
    monkeypatch.setattr(launch, "get_active_quiz_session_strict", lambda _uid: bad)
    monkeypatch.setattr(
        launch,
        "restart_owned_quiz_attempt",
        lambda *_args, **_kwargs: pytest.fail("corrupt active session must not be replaced"),
    )

    with pytest.raises(launch.LegacySessionLaunchConflict, match="contradictory"):
        _launch()


def test_invalid_new_spec_fails_in_strict_create_or_restart_without_fallback(monkeypatch):
    monkeypatch.setattr(launch, "get_active_quiz_session_strict", lambda _uid: None)
    monkeypatch.setattr(
        launch,
        "create_quiz_session_strict",
        lambda **_: (_ for _ in ()).throw(ValueError("time_limit invalid")),
    )

    with pytest.raises(ValueError, match="time_limit"):
        _launch(time_limit=0)
