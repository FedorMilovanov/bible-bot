import copy
import hashlib
import hmac
import json
import time
from urllib.parse import urlencode

import pytest

import database
import keep_alive
from web_api import auth as auth_module
from web_api import quiz as quiz_module


class FakeCollection:
    def __init__(self):
        self.docs = {}

    def create_index(self, *args, **kwargs):
        return kwargs.get("name", "idx")

    def insert_one(self, doc):
        self.docs[doc["_id"]] = copy.deepcopy(doc)
        return object()

    @staticmethod
    def _matches(doc, query):
        for key, expected in query.items():
            actual = doc.get(key)
            if isinstance(expected, dict) and set(expected) == {"$in"}:
                if actual not in expected["$in"]:
                    return False
                continue
            if actual != expected:
                return False
        return True

    def find_one(self, query):
        for doc in self.docs.values():
            if self._matches(doc, query):
                return copy.deepcopy(doc)
        return None

    def find_one_and_update(self, query, update, return_document=None):
        for key, doc in self.docs.items():
            if not self._matches(doc, query):
                continue
            for field, amount in update.get("$inc", {}).items():
                doc[field] = doc.get(field, 0) + amount
            for field, value in update.get("$set", {}).items():
                doc[field] = value
            for field, value in update.get("$push", {}).items():
                doc.setdefault(field, []).append(value)
            self.docs[key] = doc
            return copy.deepcopy(doc)
        return None

    def update_one(self, query, update, **kwargs):
        for key, doc in self.docs.items():
            if not self._matches(doc, query):
                continue
            for field, value in update.get("$set", {}).items():
                doc[field] = value
            for field, amount in update.get("$inc", {}).items():
                doc[field] = doc.get(field, 0) + amount
            self.docs[key] = doc
            return object()
        return object()

    def update_many(self, query, update, **kwargs):
        for key, doc in list(self.docs.items()):
            if not self._matches(doc, query):
                continue
            for field, value in update.get("$set", {}).items():
                doc[field] = value
            self.docs[key] = doc
        return object()


def signed_init_data(token, user_id=123, auth_date=None):
    auth_date = int(auth_date or time.time())
    data = {
        "auth_date": str(auth_date),
        "query_id": "test-query",
        "user": json.dumps(
            {"id": user_id, "first_name": "Test", "username": "tester"},
            separators=(",", ":"),
            ensure_ascii=False,
        ),
    }
    check = "\n".join(f"{key}={value}" for key, value in sorted(data.items()))
    secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
    data["hash"] = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
    return urlencode(data)


@pytest.fixture
def client(monkeypatch):
    token = "123456:TEST_TOKEN"
    monkeypatch.setenv("BOT_TOKEN", token)
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("TELEGRAM_INIT_DATA_MAX_AGE_SECONDS", "3600")
    monkeypatch.setattr(database, "init_user_stats", lambda *args, **kwargs: True)
    monkeypatch.setattr(database, "get_user_stats", lambda user_id: {"_id": str(user_id)})
    keep_alive.app.config.update(TESTING=True)
    return keep_alive.app.test_client(), token


def test_intro_compatibility_aliases_remain_without_retired_runtime():
    import intro
    import questions

    assert intro.intro_part1_questions is questions.intro_part1_questions
    assert intro.intro_part2_questions is questions.intro_part2_questions
    assert intro.intro_part3_questions is questions.intro_part3_questions
    assert intro.intro_part1_questions


def test_job_queue_dependency_is_available():
    from telegram.ext import Application

    application = Application.builder().token("123456:TEST_TOKEN").build()
    assert application.job_queue is not None


def test_init_data_signature_and_age(client):
    _, token = client
    fresh = signed_init_data(token)
    assert auth_module.verify_init_data(fresh)["id"] == 123

    stale = signed_init_data(token, auth_date=int(time.time()) - 7200)
    assert auth_module.verify_init_data(stale) is None

    tampered = fresh.replace("tester", "attacker")
    assert auth_module.verify_init_data(tampered) is None


def test_query_string_user_id_is_not_auth(client):
    http, _ = client
    response = http.get("/api/me?user_id=123")
    assert response.status_code == 401


def test_leaderboard_requires_telegram_auth(client):
    http, _ = client
    response = http.get("/api/leaderboard?cat=general")
    assert response.status_code == 401


def test_authenticated_questions_never_expose_answer_or_explanation(client):
    http, token = client
    response = http.get(
        "/api/questions/easy_p1",
        headers={"X-Telegram-Init-Data": signed_init_data(token)},
    )
    assert response.status_code == 200
    questions = response.get_json()
    assert questions
    assert all("correct" not in q for q in questions)
    assert all("explanation" not in q for q in questions)


def test_conflicting_new_quiz_preserves_previous_active_session(client, monkeypatch):
    http, token = client
    sessions = FakeCollection()
    monkeypatch.setattr(quiz_module, "miniapp_sessions", lambda: sessions)
    auth = {"X-Telegram-Init-Data": signed_init_data(token)}

    first = http.post(
        "/api/quiz/start",
        headers=auth,
        json={"pool_key": "easy_p1", "mode": "relaxed", "count": 10},
    )
    assert first.status_code == 200
    first_id = first.get_json()["session_id"]

    second = http.post(
        "/api/quiz/start",
        headers=auth,
        json={"pool_key": "medium_p1", "mode": "relaxed", "count": 10},
    )
    assert second.status_code == 409
    assert sessions.docs[first_id]["status"] == "in_progress"
    assert len(sessions.docs) == 1


def test_server_authoritative_quiz_and_idempotent_replay(client, monkeypatch):
    http, token = client
    sessions = FakeCollection()
    monkeypatch.setattr(quiz_module, "miniapp_sessions", lambda: sessions)
    monkeypatch.setattr(
        quiz_module,
        "_finalize_quiz",
        lambda session, user: {"points": 42, "daily_bonus": 0, "new_achievements": []},
    )

    auth = {"X-Telegram-Init-Data": signed_init_data(token)}
    started = http.post(
        "/api/quiz/start",
        headers=auth,
        json={"pool_key": "easy_p1", "mode": "relaxed", "count": 10, "challenge": False},
    )
    assert started.status_code == 200
    body = started.get_json()
    assert body["total"] == 10
    assert "correct" not in body["question"]
    assert "explanation" not in body["question"]

    session_id = body["session_id"]
    private_session = sessions.docs[session_id]
    first = private_session["questions"][0]
    correct = first["correct"]

    answer = http.post(
        "/api/quiz/answer",
        headers=auth,
        json={"session_id": session_id, "question_id": first["id"], "chosen": correct},
    )
    assert answer.status_code == 200
    result = answer.get_json()
    assert result["ok"] is True
    assert result["score"] == 1
    assert result["correct_index"] == correct

    replay = http.post(
        "/api/quiz/answer",
        headers=auth,
        json={"session_id": session_id, "question_id": first["id"], "chosen": correct},
    )
    assert replay.status_code == 200
    assert replay.get_json()["ok"] is True
    assert sessions.docs[session_id]["current_index"] == 1

    current = http.post(
        "/api/quiz/current",
        headers=auth,
        json={"session_id": session_id},
    )
    assert current.status_code == 200
    current_body = current.get_json()
    assert current_body["index"] == 1
    assert current_body["question"]["id"] != first["id"]
    assert "correct" not in current_body["question"]


def test_client_timeout_is_server_recorded_as_timeout(client, monkeypatch):
    http, token = client
    sessions = FakeCollection()
    monkeypatch.setattr(quiz_module, "miniapp_sessions", lambda: sessions)
    auth = {"X-Telegram-Init-Data": signed_init_data(token)}

    started = http.post(
        "/api/quiz/start",
        headers=auth,
        json={"pool_key": "easy_p1", "mode": "timed", "count": 10},
    ).get_json()
    session_id = started["session_id"]
    question_id = started["question"]["id"]

    response = http.post(
        "/api/quiz/answer",
        headers=auth,
        json={"session_id": session_id, "question_id": question_id, "chosen": -1},
    )
    assert response.status_code == 200
    result = response.get_json()
    assert result["ok"] is False
    assert result["timed_out"] is True


def test_normal_finalization_uses_receipt_once_and_naive_utc(monkeypatch):
    sessions = FakeCollection()
    monkeypatch.setattr(quiz_module, "miniapp_sessions", lambda: sessions)
    assert quiz_module._now().tzinfo is None

    calls = []

    def apply_once(**kwargs):
        calls.append(kwargs["result_id"])
        return {"points": 7, "daily_bonus": 0, "new_achievements": []}

    monkeypatch.setattr(quiz_module, "apply_regular_result_once", apply_once)

    session = {
        "_id": "finalize-test",
        "status": "in_progress",
        "leaderboard_recorded": False,
        "question_count": 10,
        "questions": [],
        "correct_count": 7,
        "started_at_dt": quiz_module._now(),
        "is_challenge": False,
        "stats_level_key": "easy_p1",
        "score_multiplier": 1.0,
        "max_streak": 3,
    }
    sessions.insert_one(session)
    user = {"id": 123, "username": "tester", "first_name": "Test"}

    result = quiz_module._finalize_quiz(copy.deepcopy(session), user)
    assert result["points"] == 7
    assert calls == ["finalize-test"]
    assert sessions.docs[session["_id"]]["status"] == "finished"

    replay = quiz_module._finalize_quiz(sessions.find_one({"_id": session["_id"]}), user)
    assert replay["points"] == 7
    assert calls == ["finalize-test"]


def test_invalid_mode_and_count_are_rejected(client, monkeypatch):
    http, token = client
    sessions = FakeCollection()
    monkeypatch.setattr(quiz_module, "miniapp_sessions", lambda: sessions)
    auth = {"X-Telegram-Init-Data": signed_init_data(token)}

    bad_mode = http.post(
        "/api/quiz/start",
        headers=auth,
        json={"pool_key": "easy_p1", "mode": "turbo", "count": 10},
    )
    assert bad_mode.status_code == 400

    bad_count = http.post(
        "/api/quiz/start",
        headers=auth,
        json={"pool_key": "easy_p1", "mode": "relaxed", "count": "many"},
    )
    assert bad_count.status_code == 400

    short_test = http.post(
        "/api/quiz/start",
        headers=auth,
        json={"pool_key": "easy_p1", "mode": "relaxed", "count": 1},
    )
    assert short_test.status_code == 400

    fake_challenge = http.post(
        "/api/quiz/start",
        headers=auth,
        json={"pool_key": "hard_p1", "mode": "speed", "count": 20, "challenge": True},
    )
    assert fake_challenge.status_code == 400
