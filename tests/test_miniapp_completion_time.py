from copy import deepcopy
from datetime import datetime, timedelta

import web_api.quiz as quiz


USER = {"id": 101, "username": "user", "first_name": "User"}
QUESTION = {
    "id": "q1",
    "question": "Question?",
    "options": ["A", "B"],
    "correct": 0,
    "explanation": "",
    "verse": "",
    "topic": "",
}


def test_durable_completion_elapsed_ignores_late_retry_clock(monkeypatch):
    started = datetime(2026, 8, 11, 12, 0, 0)
    completed = started + timedelta(seconds=60)
    monkeypatch.setattr(quiz, "_now", lambda: started + timedelta(hours=3))

    elapsed = quiz._completion_elapsed_seconds({
        "started_at_dt": started,
        "completed_at_dt": completed,
        "completion_time_protocol": quiz.COMPLETION_TIME_PROTOCOL_DURABLE,
    })

    assert elapsed == 60.0


def test_legacy_completion_without_marker_keeps_backward_compatible_fallback(monkeypatch):
    started = datetime(2026, 8, 11, 12, 0, 0)
    retry_at = started + timedelta(seconds=90)
    monkeypatch.setattr(quiz, "_now", lambda: retry_at)

    assert quiz._completion_elapsed_seconds({"started_at_dt": started}) == 90.0


def test_legacy_completion_prefers_transitional_durable_timestamp(monkeypatch):
    started = datetime(2026, 8, 11, 12, 0, 0)
    completed = started + timedelta(seconds=25)
    monkeypatch.setattr(quiz, "_now", lambda: started + timedelta(hours=1))

    assert quiz._completion_elapsed_seconds({
        "started_at_dt": started,
        "completed_at_dt": completed,
    }) == 25.0


def test_durable_protocol_without_completion_timestamp_fails_closed():
    started = datetime(2026, 8, 11, 12, 0, 0)

    try:
        quiz._completion_elapsed_seconds({
            "started_at_dt": started,
            "completed_at_dt": None,
            "completion_time_protocol": quiz.COMPLETION_TIME_PROTOCOL_DURABLE,
        })
    except ValueError as exc:
        assert "completion time is missing" in str(exc)
    else:
        raise AssertionError("missing durable completion evidence must fail closed")


class FinalizeSessions:
    def __init__(self, claimed):
        self.claimed = deepcopy(claimed)
        self.finished = None

    def update_one(self, query, update):
        if update.get("$set", {}).get("status") == "finished":
            self.finished = deepcopy(self.claimed)
            self.finished.update(deepcopy(update["$set"]))
        return object()

    def find_one(self, query):
        if self.finished is not None:
            return deepcopy(self.finished)
        return deepcopy(self.claimed)


def test_challenge_finalizer_uses_durable_completion_time_not_retry_time(monkeypatch):
    started = datetime(2026, 8, 11, 12, 0, 0)
    completed = started + timedelta(seconds=42)
    claimed = {
        "_id": "challenge-result",
        "status": "finalizing",
        "leaderboard_recorded": True,
        "question_count": 20,
        "questions": [deepcopy(QUESTION)] * 20,
        "correct_count": 18,
        "started_at_dt": started,
        "completed_at_dt": completed,
        "completion_time_protocol": quiz.COMPLETION_TIME_PROTOCOL_DURABLE,
        "is_challenge": True,
        "stats_level_key": "random20",
    }
    sessions = FinalizeSessions(claimed)
    captured = []
    monkeypatch.setattr(quiz, "miniapp_sessions", lambda: sessions)
    monkeypatch.setattr(quiz, "_claim_or_resume_finalization", lambda _session, _sessions: deepcopy(claimed))
    monkeypatch.setattr(quiz, "_now", lambda: started + timedelta(hours=4))

    def apply_once(**kwargs):
        captured.append(kwargs["time_seconds"])
        return {"points": 18, "daily_bonus": 0, "new_achievements": []}

    monkeypatch.setattr(quiz, "apply_challenge_result_once", apply_once)

    result = quiz._finalize_quiz(deepcopy(claimed), USER)

    assert result["points"] == 18
    assert captured == [42.0]
    assert sessions.finished["status"] == "finished"


class AnswerSessions:
    def __init__(self, session):
        self.session = deepcopy(session)
        self.answer_update = None

    def find_one(self, query):
        return deepcopy(self.session)

    def find_one_and_update(self, query, update, return_document=None):
        self.answer_update = deepcopy(update)
        self.session["current_index"] += update["$inc"]["current_index"]
        self.session["correct_count"] += update["$inc"]["correct_count"]
        self.session.update(deepcopy(update["$set"]))
        self.session.setdefault("answered", []).append(deepcopy(update["$push"]["answered"]))
        return deepcopy(self.session)


def answer_session(*, question_count=1):
    questions = [deepcopy(QUESTION) for _ in range(question_count)]
    for index, item in enumerate(questions):
        item["id"] = f"q{index + 1}"
    return {
        "_id": "session-1",
        "user_id": "101",
        "status": "in_progress",
        "pool_key": "easy_p1",
        "mode": "relaxed",
        "is_challenge": False,
        "questions": questions,
        "question_count": question_count,
        "current_index": 0,
        "correct_count": 0,
        "current_streak": 0,
        "max_streak": 0,
        "answered": [],
        "time_limit": None,
        "score_multiplier": 1.0,
        "started_at_dt": datetime(2026, 8, 11, 12, 0, 0),
        "completed_at_dt": None,
        "completion_time_protocol": quiz.COMPLETION_TIME_PROTOCOL_DURABLE,
        "question_sent_at": 100.0,
        "leaderboard_recorded": False,
    }


def test_last_answer_atomically_persists_completion_timestamp(monkeypatch):
    session = answer_session(question_count=1)
    sessions = AnswerSessions(session)
    answer_time = datetime(2026, 8, 11, 12, 0, 30)
    monkeypatch.setattr(quiz, "miniapp_sessions", lambda: sessions)
    monkeypatch.setattr(quiz, "_now", lambda: answer_time)
    monkeypatch.setattr(quiz.time, "time", lambda: 101.0)
    monkeypatch.setattr(quiz, "_finalize_quiz", lambda _session, _user: {
        "points": 1,
        "daily_bonus": 0,
        "new_achievements": [],
    })

    body, message, status = quiz.answer_quiz(
        USER,
        {"session_id": "session-1", "question_id": "q1", "chosen": 0},
    )

    assert status == 200
    assert message is None
    assert body["finished"] is True
    assert sessions.answer_update["$set"]["completed_at_dt"] == answer_time
    assert (
        sessions.answer_update["$set"]["completion_time_protocol"]
        == quiz.COMPLETION_TIME_PROTOCOL_DURABLE
    )


def test_nonfinal_answer_does_not_set_completion_timestamp(monkeypatch):
    session = answer_session(question_count=2)
    sessions = AnswerSessions(session)
    monkeypatch.setattr(quiz, "miniapp_sessions", lambda: sessions)
    monkeypatch.setattr(quiz, "_now", lambda: datetime(2026, 8, 11, 12, 0, 30))
    monkeypatch.setattr(quiz.time, "time", lambda: 101.0)

    body, message, status = quiz.answer_quiz(
        USER,
        {"session_id": "session-1", "question_id": "q1", "chosen": 0},
    )

    assert status == 200
    assert message is None
    assert body["finished"] is False
    assert "completed_at_dt" not in sessions.answer_update["$set"]
