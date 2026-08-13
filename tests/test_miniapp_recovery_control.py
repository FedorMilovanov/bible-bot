from copy import deepcopy

import web_api.quiz as quiz


USER = {"id": 101, "username": "user", "first_name": "User"}


def question(index: int) -> dict:
    return {
        "id": f"q{index}",
        "question": f"Question {index}?",
        "options": ["A", "B"],
        "correct": 0,
        "explanation": f"Explanation {index}",
        "verse": "",
        "topic": "",
    }


def active_session(*, current_index: int = 2, status: str = "in_progress") -> dict:
    questions = [question(index) for index in range(10)]
    answered = [
        {
            "id": questions[index]["id"],
            "chosen": 0,
            "correct": 0,
            "ok": True,
            "timed_out": False,
        }
        for index in range(min(current_index, 10))
    ]
    return {
        "_id": "session-1",
        "user_id": "101",
        "status": status,
        "pool_key": "easy_p1",
        "stats_level_key": "easy_p1",
        "mode": "relaxed",
        "is_challenge": False,
        "questions": questions,
        "question_count": 10,
        "current_index": current_index,
        "correct_count": min(current_index, 10),
        "current_streak": min(current_index, 3),
        "max_streak": min(current_index, 3),
        "answered": answered,
        "time_limit": None,
        "score_multiplier": 1.0,
        "started_at_dt": quiz._now(),
        "completed_at_dt": quiz._now() if current_index == 10 else None,
        "updated_at_dt": quiz._now(),
        "question_sent_at": 1.0,
        "leaderboard_recorded": status in {"finalizing", "score_error", "finished"},
    }


class FakeSessions:
    def __init__(self, session=None, *, race_latest=None):
        self.session = deepcopy(session)
        self.race_latest = deepcopy(race_latest)
        self.updates = []

    def find_one(self, query):
        source = self.race_latest if self.race_latest is not None and self.updates else self.session
        if not source:
            return None
        if "_id" in query and str(source.get("_id")) != str(query["_id"]):
            return None
        if "user_id" in query and str(source.get("user_id")) != str(query["user_id"]):
            return None
        status = query.get("status")
        if isinstance(status, dict) and "$in" in status and source.get("status") not in status["$in"]:
            return None
        if isinstance(status, str) and source.get("status") != status:
            return None
        return deepcopy(source)

    def find_one_and_update(self, query, update, **_kwargs):
        self.updates.append((deepcopy(query), deepcopy(update)))
        if self.race_latest is not None:
            return None
        if not self.session:
            return None
        if self.session.get("status") != query.get("status"):
            return None
        if self.session.get("current_index") != query.get("current_index"):
            return None
        for key, value in update.get("$set", {}).items():
            self.session[key] = value
        return deepcopy(self.session)


def test_active_recovery_returns_owned_current_question_and_prior_review(monkeypatch):
    sessions = FakeSessions(active_session(current_index=2))
    monkeypatch.setattr(quiz, "miniapp_sessions", lambda: sessions)

    body, message, status = quiz.get_active_quiz(USER)

    assert status == 200
    assert message is None
    assert body["active"] is True
    assert body["session_id"] == "session-1"
    assert body["pool_key"] == "easy_p1"
    assert body["index"] == 2
    assert body["score"] == 2
    assert len(body["answers"]) == 2
    assert body["answers"][0]["correct"] == 0
    assert "correct" not in body["question"]
    assert "explanation" not in body["question"]


def test_active_recovery_does_not_expose_foreign_session(monkeypatch):
    foreign = active_session()
    foreign["user_id"] = "999"
    sessions = FakeSessions(foreign)
    monkeypatch.setattr(quiz, "miniapp_sessions", lambda: sessions)

    body, message, status = quiz.get_active_quiz(USER)

    assert status == 200
    assert message is None
    assert body == {"active": False}


def test_cancel_abandons_only_incomplete_owned_session(monkeypatch):
    sessions = FakeSessions(active_session(current_index=2))
    monkeypatch.setattr(quiz, "miniapp_sessions", lambda: sessions)

    body, message, status = quiz.cancel_quiz(USER, {"session_id": "session-1"})

    assert status == 200
    assert message is None
    assert body == {"cancelled": True, "already_cancelled": False}
    query, update = sessions.updates[0]
    assert query == {
        "_id": "session-1",
        "user_id": "101",
        "status": "in_progress",
        "current_index": 2,
    }
    assert update["$set"]["status"] == "abandoned"
    assert update["$set"]["question_sent_at"] is None


def test_cancel_is_idempotent_after_abandon(monkeypatch):
    abandoned = active_session(current_index=2, status="abandoned")
    sessions = FakeSessions(abandoned)
    monkeypatch.setattr(quiz, "miniapp_sessions", lambda: sessions)

    body, message, status = quiz.cancel_quiz(USER, {"session_id": "session-1"})

    assert status == 200
    assert message is None
    assert body == {"cancelled": True, "already_cancelled": True}
    assert sessions.updates == []


def test_completed_attempt_is_finalized_not_abandoned(monkeypatch):
    complete = active_session(current_index=10)
    sessions = FakeSessions(complete)
    finalized = []
    monkeypatch.setattr(quiz, "miniapp_sessions", lambda: sessions)
    monkeypatch.setattr(
        quiz,
        "_finalize_quiz",
        lambda session, user: finalized.append((session["_id"], user["id"])) or {
            "points": 10,
            "daily_bonus": 0,
            "new_achievements": [],
        },
    )

    body, message, status = quiz.cancel_quiz(USER, {"session_id": "session-1"})

    assert body is None
    assert status == 409
    assert "result is preserved" in message
    assert finalized == [("session-1", 101)]
    assert sessions.updates == []


def test_cancel_race_preserves_final_answer_that_committed_first(monkeypatch):
    before = active_session(current_index=9)
    after = active_session(current_index=10)
    sessions = FakeSessions(before, race_latest=after)
    finalized = []
    monkeypatch.setattr(quiz, "miniapp_sessions", lambda: sessions)
    monkeypatch.setattr(
        quiz,
        "_finalize_quiz",
        lambda session, user: finalized.append((session["current_index"], user["id"])) or {
            "points": 10,
            "daily_bonus": 0,
            "new_achievements": [],
        },
    )

    body, message, status = quiz.cancel_quiz(USER, {"session_id": "session-1"})

    assert body is None
    assert status == 409
    assert "result is preserved" in message
    assert finalized == [(10, 101)]
