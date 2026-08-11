from copy import deepcopy
from datetime import datetime

import pytest

import database


class FakeQuizSessions:
    def __init__(self):
        self.calls = []
        self.error = None

    def update_one(self, query, update):
        if self.error is not None:
            raise self.error
        self.calls.append((deepcopy(query), deepcopy(update)))


def test_generic_update_rejects_progress_fields_before_mutation_or_store(monkeypatch):
    collection = FakeQuizSessions()
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    fields = {"current_index": 7, "correct_count": 5}
    original = deepcopy(fields)

    with pytest.raises(ValueError, match="generic quiz session update"):
        database.update_quiz_session("s1", fields)

    assert fields == original
    assert collection.calls == []


@pytest.mark.parametrize(
    "field",
    [
        "answered_questions",
        "question_ids",
        "questions_data",
        "attempt_id",
        "level_key",
        "time_limit",
    ],
)
def test_generic_update_rejects_other_authoritative_session_fields(monkeypatch, field):
    collection = FakeQuizSessions()
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    with pytest.raises(ValueError, match="generic quiz session update"):
        database.update_quiz_session("s1", {field: [] if field.endswith("s") else "x"})

    assert collection.calls == []


def test_legacy_timer_update_remains_allowed_and_does_not_mutate_caller(monkeypatch):
    collection = FakeQuizSessions()
    now = datetime(2026, 8, 11, 10, 0, 0)
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: now)
    fields = {"question_sent_at": 123.5}

    database.update_quiz_session("s1", fields)

    assert fields == {"question_sent_at": 123.5}
    assert collection.calls == [
        (
            {"_id": "s1"},
            {
                "$set": {
                    "question_sent_at": 123.5,
                    "updated_at": now.isoformat(),
                    "updated_at_dt": now,
                }
            },
        )
    ]


def test_legacy_terminal_status_update_remains_allowed(monkeypatch):
    collection = FakeQuizSessions()
    now = datetime(2026, 8, 11, 10, 0, 0)
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)
    monkeypatch.setattr(database, "_now_utc", lambda: now)

    database.update_quiz_session(
        "s1",
        {"status": "finished", "end_time": now},
    )

    stored = collection.calls[0][1]["$set"]
    assert stored["status"] == "finished"
    assert stored["end_time"] == now
    assert stored["updated_at_dt"] == now


def test_generic_update_fails_closed_when_storage_is_unavailable(monkeypatch):
    monkeypatch.setattr(database, "quiz_sessions_collection", None)

    with pytest.raises(
        database.LegacyQuizSessionPersistenceUnavailable,
        match="storage is unavailable",
    ):
        database.update_quiz_session("s1", {"question_sent_at": 123.5})


def test_generic_update_surfaces_storage_write_failure(monkeypatch):
    collection = FakeQuizSessions()
    collection.error = RuntimeError("mongo down")
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    with pytest.raises(
        database.LegacyQuizSessionPersistenceUnavailable,
        match="update failed",
    ) as exc_info:
        database.update_quiz_session("s1", {"status": "finished"})

    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert collection.calls == []
