from copy import deepcopy
from datetime import datetime

import database
from legacy_result_card_delivery import RESULT_CARD_DELIVERY_PROTOCOL
from legacy_session_close import finish_completed_owned_quiz_session


class SessionCollection:
    def __init__(self, doc):
        self.doc = deepcopy(doc)
        self.update_doc = None

    def find_one(self, query, projection=None):
        del projection
        for key, value in query.items():
            if self.doc.get(key) != value:
                return None
        return deepcopy(self.doc)

    def find_one_and_update(self, query, update, return_document=None):
        del return_document
        for key, value in query.items():
            if self.doc.get(key) != value:
                return None
        self.update_doc = deepcopy(update)
        for key, value in update.get("$set", {}).items():
            self.doc[key] = deepcopy(value)
        return deepcopy(self.doc)


def _completed_session(*, chat_id=777, status="in_progress"):
    return {
        "_id": "s1",
        "user_id": "42",
        "status": status,
        "mode": "level",
        "level_name": "1 Петра 1",
        "chat_id": chat_id,
        "is_retry": False,
        "current_index": 2,
        "correct_count": 1,
        "question_ids": ["q1", "q2"],
        "answered_questions": [
            {
                "index": 0,
                "qid": "q1",
                "user_answer": "A",
                "is_correct": True,
                "question_obj": {"id": "q1"},
            },
            {
                "index": 1,
                "qid": "q2",
                "user_answer": "B",
                "is_correct": False,
                "question_obj": {"id": "q2"},
            },
        ],
    }


def _install(monkeypatch, doc):
    collection = SessionCollection(doc)
    now = datetime(2026, 8, 15, 12, 0, 0)
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: now)
    return collection


def test_terminal_cas_stages_pending_result_card_marker(monkeypatch):
    collection = _install(monkeypatch, _completed_session())

    result = finish_completed_owned_quiz_session("s1", 42)

    marker = result["result_card_delivery"]
    assert marker["protocol"] == RESULT_CARD_DELIVERY_PROTOCOL
    assert marker["delivered"] is False
    assert marker["attempts"] == 0
    assert marker["chat_id"] == 777
    assert marker["score"] == 1
    assert marker["total"] == 2
    assert collection.update_doc["$set"]["result_card_delivery"] == marker


def test_missing_destination_preserves_legacy_terminal_transition(monkeypatch):
    collection = _install(monkeypatch, _completed_session(chat_id=None))

    result = finish_completed_owned_quiz_session("s1", 42)

    assert result["status"] == "finished"
    assert "result_card_delivery" not in result
    assert "result_card_delivery" not in collection.update_doc["$set"]


def test_malformed_legacy_destination_does_not_block_scoring_close(monkeypatch):
    collection = _install(monkeypatch, _completed_session(chat_id=False))

    result = finish_completed_owned_quiz_session("s1", 42)

    assert result["status"] == "finished"
    assert "result_card_delivery" not in result
    assert "result_card_delivery" not in collection.update_doc["$set"]


def test_historical_finished_session_is_not_backfilled(monkeypatch):
    collection = _install(monkeypatch, _completed_session(status="finished"))

    result = finish_completed_owned_quiz_session("s1", 42)

    assert result["status"] == "finished"
    assert "result_card_delivery" not in result
    assert collection.update_doc is None
