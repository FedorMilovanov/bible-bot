from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from pymongo.errors import AutoReconnect

import legacy_retry_source as retry_source


class _Collection:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error
        self.query = None
        self.sort = None

    def find_one(self, query, *, sort=None):
        self.query = query
        self.sort = sort
        if self.error is not None:
            raise self.error
        return self.result


def _question(qid: str, text: str) -> dict:
    return {
        "id": qid,
        "question": text,
        "options": ["A", "B"],
        "correct": "A",
    }


def _finished_session(*, correct_count=1) -> dict:
    first = _question("q1", "First?")
    second = _question("q2", "Second?")
    started = datetime(2026, 8, 11, 18, 0, 0, tzinfo=UTC).timestamp()
    return {
        "_id": "session-1",
        "session_id": "session-1",
        "attempt_id": "session-1",
        "user_id": "42",
        "chat_id": 777,
        "status": "finished",
        "mode": "level",
        "level_key": "easy",
        "level_name": "Easy",
        "time_limit": None,
        "question_ids": ["q1", "q2"],
        "questions_data": [first, second],
        "current_index": 2,
        "correct_count": correct_count,
        "start_time": started,
        "answered_questions": [
            {
                "qid": "q1",
                "index": 0,
                "is_correct": False,
                "user_answer": "B",
                "question_obj": first,
                "ts": "2026-08-11T18:00:05",
            },
            {
                "qid": "q2",
                "index": 1,
                "is_correct": True,
                "user_answer": "A",
                "question_obj": second,
                "ts": "2026-08-11T18:00:10",
            },
        ],
        "end_time": datetime(2026, 8, 11, 18, 0, 10, 900000),
    }


def _install_database(monkeypatch, collection):
    database = SimpleNamespace(
        quiz_sessions_collection=collection,
        _uid=lambda value: str(value),
    )
    monkeypatch.setattr(retry_source, "_database", lambda: database)


def test_message_cutoff_is_exclusive_next_utc_second():
    aware = datetime(2026, 8, 11, 20, 0, 10, 750000, tzinfo=UTC)
    naive = datetime(2026, 8, 11, 20, 0, 10, 750000)
    expected = datetime(2026, 8, 11, 20, 0, 11)

    assert retry_source._message_cutoff(aware) == expected
    assert retry_source._message_cutoff(naive) == expected


def test_lookup_binds_owner_chat_finished_state_and_message_time(monkeypatch):
    session = _finished_session()
    collection = _Collection(result=session)
    _install_database(monkeypatch, collection)
    message_date = datetime(2026, 8, 11, 18, 0, 10, tzinfo=UTC)

    source = retry_source.load_retry_source_for_result_message(
        user_id=42,
        chat_id=777,
        message_date=message_date,
    )

    assert collection.query == {
        "user_id": "42",
        "chat_id": 777,
        "status": "finished",
        "end_time": {"$lt": datetime(2026, 8, 11, 18, 0, 11)},
    }
    assert collection.sort == [("end_time", -1), ("_id", -1)]
    assert source is not None
    assert source.session_id == "session-1"
    assert source.level_name == "Easy"
    assert len(source.questions) == 1
    assert source.questions[0]["id"] == "q1"


def test_lookup_returns_none_when_no_finished_source_exists(monkeypatch):
    collection = _Collection(result=None)
    _install_database(monkeypatch, collection)

    source = retry_source.load_retry_source_for_result_message(
        user_id=42,
        chat_id=777,
        message_date=datetime(2026, 8, 11, 18, 0, 10, tzinfo=UTC),
    )

    assert source is None


def test_lookup_fails_closed_on_mongo_error(monkeypatch):
    collection = _Collection(error=AutoReconnect("mongo unavailable"))
    _install_database(monkeypatch, collection)

    with pytest.raises(retry_source.LegacyRetrySourceUnavailable, match="lookup failed"):
        retry_source.load_retry_source_for_result_message(
            user_id=42,
            chat_id=777,
            message_date=datetime(2026, 8, 11, 18, 0, 10, tzinfo=UTC),
        )


def test_lookup_rejects_inconsistent_completed_ledger(monkeypatch):
    collection = _Collection(result=_finished_session(correct_count=2))
    _install_database(monkeypatch, collection)

    with pytest.raises(retry_source.LegacyRetrySourceInvalid, match="exact completed"):
        retry_source.load_retry_source_for_result_message(
            user_id=42,
            chat_id=777,
            message_date=datetime(2026, 8, 11, 18, 0, 10, tzinfo=UTC),
        )


def test_lookup_rejects_invalid_chat_id_before_mongo(monkeypatch):
    collection = _Collection(result=_finished_session())
    _install_database(monkeypatch, collection)

    with pytest.raises(ValueError, match="chat_id"):
        retry_source.load_retry_source_for_result_message(
            user_id=42,
            chat_id=True,
            message_date=datetime(2026, 8, 11, 18, 0, 10, tzinfo=UTC),
        )
    assert collection.query is None
