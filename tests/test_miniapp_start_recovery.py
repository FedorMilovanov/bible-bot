from copy import deepcopy

import pytest
from pymongo.errors import PyMongoError

import database
import questions
import web_api.quiz as quiz


USER = {"id": 101, "username": "user", "first_name": "User"}


def question(index: int) -> dict:
    return {
        "id": f"q{index}",
        "question": f"Question {index}?",
        "options": ["A", "B"],
        "correct": 0,
        "explanation": "",
        "verse": "",
        "topic": "",
    }


def active_session(*, mode="relaxed", current_index=2) -> dict:
    prepared = [question(index) for index in range(10)]
    return {
        "_id": "session-existing",
        "user_id": "101",
        "status": "in_progress",
        "pool_key": "easy_p1",
        "stats_level_key": "easy_p1",
        "mode": mode,
        "is_challenge": False,
        "questions": prepared,
        "question_count": 10,
        "current_index": current_index,
        "correct_count": min(current_index, 1),
        "current_streak": 0,
        "max_streak": 1,
        "answered": [],
        "time_limit": None,
        "score_multiplier": 1.0,
        "started_at_dt": quiz._now(),
        "updated_at_dt": quiz._now(),
        "question_sent_at": None,
        "leaderboard_recorded": False,
    }


class FakeSessions:
    def __init__(self, *, active=None, find_error=None):
        self.active = deepcopy(active)
        self.find_error = find_error
        self.find_calls = []
        self.inserted = []

    def find_one(self, query):
        self.find_calls.append(deepcopy(query))
        if self.find_error is not None:
            raise self.find_error
        if query == {"user_id": "101", "status": "in_progress"}:
            return deepcopy(self.active)
        return None

    def insert_one(self, document):
        self.inserted.append(deepcopy(document))


def install_common(monkeypatch, sessions):
    monkeypatch.setattr(quiz, "miniapp_sessions", lambda: sessions)
    monkeypatch.setattr(questions, "get_pool_by_key", lambda _key: [question(i) for i in range(10)])


def test_same_start_request_resumes_existing_session_without_mutation(monkeypatch):
    existing = active_session()
    sessions = FakeSessions(active=existing)
    install_common(monkeypatch, sessions)

    def forbidden_profile_init(*_args, **_kwargs):
        raise AssertionError("profile initialization must not run on start replay")

    monkeypatch.setattr(database, "init_user_stats", forbidden_profile_init)
    monkeypatch.setattr(database, "get_user_stats", forbidden_profile_init)

    body, message, status = quiz.start_quiz(
        USER,
        {"pool_key": "easy_p1", "mode": "relaxed", "count": 10, "challenge": False},
    )

    assert status == 200
    assert message is None
    assert body["resumed"] is True
    assert body["session_id"] == "session-existing"
    assert body["index"] == 2
    assert body["question"]["id"] == "q2"
    assert sessions.inserted == []
    # No update_many method exists on the fake. A regression to destructive
    # abandon-before-create would therefore fail this test immediately.


def test_different_start_request_preserves_active_session_and_returns_conflict(monkeypatch):
    sessions = FakeSessions(active=active_session())
    install_common(monkeypatch, sessions)

    body, message, status = quiz.start_quiz(
        USER,
        {"pool_key": "easy_p1", "mode": "timed", "count": 10, "challenge": False},
    )

    assert body is None
    assert status == 409
    assert "another active quiz is in progress" in message
    assert sessions.inserted == []


def test_exact_complete_active_is_finalized_before_new_session(monkeypatch):
    sessions = FakeSessions(active=active_session(current_index=10))
    install_common(monkeypatch, sessions)
    finalized = []

    def fake_finalize(session, user):
        finalized.append((session["_id"], user["id"]))
        return {"points": 10, "daily_bonus": 0, "new_achievements": []}

    monkeypatch.setattr(quiz, "_finalize_quiz", fake_finalize)
    monkeypatch.setattr(database, "init_user_stats", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(database, "get_user_stats", lambda _uid: {"_id": "101"})

    body, message, status = quiz.start_quiz(
        USER,
        {"pool_key": "easy_p1", "mode": "relaxed", "count": 10, "challenge": False},
    )

    assert status == 200
    assert message is None
    assert finalized == [("session-existing", 101)]
    assert body["resumed"] is False
    assert body["session_id"] != "session-existing"
    assert len(sessions.inserted) == 1


def test_exact_complete_is_not_replaced_when_finalization_is_pending(monkeypatch):
    sessions = FakeSessions(active=active_session(current_index=10))
    install_common(monkeypatch, sessions)
    monkeypatch.setattr(quiz, "_finalize_quiz", lambda *_args, **_kwargs: None)

    body, message, status = quiz.start_quiz(
        USER,
        {"pool_key": "easy_p1", "mode": "relaxed", "count": 10, "challenge": False},
    )

    assert body is None
    assert status == 503
    assert "finalization is incomplete" in message
    assert sessions.inserted == []


def test_active_lookup_outage_never_falls_through_to_new_insert(monkeypatch):
    sessions = FakeSessions(find_error=PyMongoError("mongo unavailable"))
    install_common(monkeypatch, sessions)

    body, message, status = quiz.start_quiz(
        USER,
        {"pool_key": "easy_p1", "mode": "relaxed", "count": 10, "challenge": False},
    )

    assert body is None
    assert status == 503
    assert message == "database temporarily unavailable"
    assert sessions.inserted == []
