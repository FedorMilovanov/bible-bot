import database

from web_api import quiz_start


class MemorySessions:
    def __init__(self):
        self.inserted = None

    def find_one(self, _query):
        return None

    def insert_one(self, document):
        self.inserted = document
        return None


def test_chapter4_starts_through_server_authoritative_quiz_path(monkeypatch):
    sessions = MemorySessions()
    monkeypatch.setattr(quiz_start.core, "miniapp_sessions", lambda: sessions)
    monkeypatch.setattr(database, "init_user_stats", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(database, "get_user_stats", lambda user_id: {"_id": str(user_id)})
    monkeypatch.setattr(quiz_start.random, "sample", lambda population, count: list(population)[:count])

    body, message, status = quiz_start.start_quiz(
        {"id": 4401, "username": "reader", "first_name": "Reader"},
        {"pool_key": "chapter4", "mode": "relaxed", "count": 10},
    )

    assert status == 200
    assert message is None
    assert body is not None
    assert body["active"] is True
    assert body["pool_key"] == "chapter4"
    assert body["challenge"] is False
    assert body["total"] == 10
    assert body["question"]["id"].startswith("ch4_")
    assert "correct" not in body["question"]
    assert "explanation" not in body["question"]

    assert sessions.inserted is not None
    assert sessions.inserted["pool_key"] == "chapter4"
    assert sessions.inserted["stats_level_key"] == "chapter4"
    assert sessions.inserted["is_challenge"] is False
    assert sessions.inserted["question_count"] == 10
    assert all("correct" in item for item in sessions.inserted["questions"])


def test_chapter4_cannot_enter_challenge_path(monkeypatch):
    body, message, status = quiz_start.start_quiz(
        {"id": 4402, "username": "reader", "first_name": "Reader"},
        {"pool_key": "chapter4", "mode": "relaxed", "count": 20, "challenge": True},
    )

    assert body is None
    assert status == 400
    assert message == "challenge requires random_all pool"
