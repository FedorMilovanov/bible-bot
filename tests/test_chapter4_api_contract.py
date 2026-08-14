import database

from questions.chapter4.authoring import CHAPTER4_STAGING_QUESTIONS
from web_api import quiz, quiz_start


class MemorySessions:
    def __init__(self):
        self.inserted = None

    def find_one(self, _query):
        return None

    def insert_one(self, document):
        self.inserted = document
        return None


def _assert_public_question_is_safe(question):
    assert set(question) == {"id", "question", "options"}
    forbidden = {
        "correct",
        "explanation",
        "review_record_id",
        "research_id",
        "research_claim_id",
        "research_effective_claim_digest",
        "research_authority_sha",
        "source_ids",
        "sources",
        "claim_inspection_edge_ids",
        "reviewer",
        "review_decision",
    }
    assert forbidden.isdisjoint(question)


def test_public_question_cannot_leak_correct_or_private_review_metadata(monkeypatch):
    monkeypatch.setattr(quiz.random, "shuffle", lambda values: None)
    card = CHAPTER4_STAGING_QUESTIONS[0]
    prepared = quiz.prepare_question(card)
    assert "review_record_id" not in prepared
    assert "research_claim_id" not in prepared
    public = quiz.public_question(prepared)
    _assert_public_question_is_safe(public)


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
    _assert_public_question_is_safe(body["question"])

    assert sessions.inserted is not None
    assert sessions.inserted["pool_key"] == "chapter4"
    assert sessions.inserted["stats_level_key"] == "chapter4"
    assert sessions.inserted["is_challenge"] is False
    assert sessions.inserted["question_count"] == 10
    assert all("correct" in item for item in sessions.inserted["questions"])
    assert all("review_record_id" not in item for item in sessions.inserted["questions"])
    assert all("research_claim_id" not in item for item in sessions.inserted["questions"])


def test_chapter4_cannot_enter_challenge_path(monkeypatch):
    body, message, status = quiz_start.start_quiz(
        {"id": 4402, "username": "reader", "first_name": "Reader"},
        {"pool_key": "chapter4", "mode": "relaxed", "count": 20, "challenge": True},
    )

    assert body is None
    assert status == 400
    assert message == "challenge requires random_all pool"
