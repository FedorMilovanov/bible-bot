from copy import deepcopy

import pytest

import database
import legacy_session_lifecycle as lifecycle


class ProgressedReplacement:
    def __init__(self):
        self.doc = {
            "_id": "container-1",
            "user_id": "42",
            "status": "in_progress",
            "attempt_id": "attempt-new",
            "previous_attempt_id": "attempt-old",
            "mode": "level",
            "level_key": "easy",
            "level_name": "Easy",
            "question_ids": ["q1"],
            "questions_data": [{"id": "q1"}],
            "current_index": 1,
            "correct_count": 1,
            "answered_questions": [
                {
                    "index": 0,
                    "qid": "q1",
                    "user_answer": "A",
                    "is_correct": True,
                    "question_obj": {"id": "q1"},
                    "ts": "2026-08-10T12:00:01",
                }
            ],
            "time_limit": None,
        }
        self.update_called = False

    def find_one(self, query):
        if query.get("_id") != self.doc["_id"] or query.get("user_id") != "42":
            return None
        return deepcopy(self.doc)

    def find_one_and_update(self, *args, **kwargs):
        self.update_called = True
        raise AssertionError("stale restart replay must not mutate replacement")


def test_old_restart_callback_is_stale_once_replacement_has_progressed(monkeypatch):
    collection = ProgressedReplacement()
    monkeypatch.setattr(database, "quiz_sessions_collection", collection)

    with pytest.raises(lifecycle.QuizSessionLifecycleConflict, match="has progressed"):
        lifecycle.restart_owned_quiz_attempt(
            "container-1",
            42,
            expected_attempt_id="attempt-old",
            mode="level",
            question_ids=["n1"],
            questions_data=[{"id": "n1"}],
            level_key="easy",
            level_name="Easy",
        )

    assert collection.update_called is False
