from __future__ import annotations

from types import SimpleNamespace

import pytest

import legacy_attempt_finalize as attempt_finalize
import legacy_learning_result_store as learning_store


class MemoryUsers:
    def __init__(self):
        self.doc = {"_id": "7"}
        self.updates = []

    def find_one(self, query):
        if query.get("_id") != "7":
            return None
        return self.doc

    def update_one(self, query, update):
        self.updates.append((query, update))
        receipt_path = next(
            key
            for key in update["$set"]
            if key.startswith("legacy_learning_receipts.")
        )
        digest = receipt_path.split(".", 1)[1]
        receipts = self.doc.setdefault("legacy_learning_receipts", {})
        if digest in receipts:
            return SimpleNamespace(modified_count=0)
        receipts[digest] = update["$set"][receipt_path]
        for key, value in update["$inc"].items():
            self.doc[key] = self.doc.get(key, 0) + value
        for key, value in update["$max"].items():
            self.doc[key] = max(self.doc.get(key, value), value)
        return SimpleNamespace(modified_count=1)


def _apply(users, monkeypatch, **overrides):
    monkeypatch.setattr(learning_store.database, "collection", users)
    monkeypatch.setattr(learning_store.database, "_now_utc", lambda: "now")
    kwargs = {
        "result_id": "quiz:attempt-1",
        "user_id": 7,
        "username": "reader",
        "first_name": "Reader",
        "level_key": "chapter2",
        "score": 8,
        "total": 10,
    }
    kwargs.update(overrides)
    return learning_store.apply_learning_progress_once(**kwargs)


def test_learning_progress_receipt_is_idempotent_and_never_updates_ranking_totals(monkeypatch):
    users = MemoryUsers()

    first = _apply(users, monkeypatch)
    second = _apply(users, monkeypatch)

    assert first["applied"] is True
    assert second["applied"] is False
    assert users.doc["chapter2_attempts"] == 1
    assert users.doc["chapter2_correct"] == 8
    assert users.doc["chapter2_total"] == 10
    assert users.doc["chapter2_best_score"] == 8

    _query, update = users.updates[0]
    assert update["$inc"] == {
        "chapter2_attempts": 1,
        "chapter2_correct": 8,
        "chapter2_total": 10,
    }
    forbidden = {
        "total_points",
        "total_tests",
        "total_questions_answered",
        "total_correct_answers",
        "perfect_count",
        "daily_activity_streak",
        "challenge_streak_count",
    }
    assert forbidden.isdisjoint(update["$inc"])
    assert first["points"] == 0
    assert first["daily_bonus"] == 0
    assert first["new_achievements"] == []


def test_learning_progress_retry_fails_closed_if_receipt_payload_does_not_match(monkeypatch):
    users = MemoryUsers()
    _apply(users, monkeypatch)

    with pytest.raises(
        learning_store.LegacyLearningProgressUnavailable,
        match="does not match",
    ):
        _apply(users, monkeypatch, score=7)

    with pytest.raises(
        learning_store.LegacyLearningProgressUnavailable,
        match="does not match",
    ):
        _apply(users, monkeypatch, level_key="chapter3")

    assert users.doc["chapter2_attempts"] == 1
    assert "chapter3_attempts" not in users.doc


@pytest.mark.parametrize("level_key", ["chapter2", "chapter3"])
def test_attempt_finalizer_routes_learning_courses_to_progress_store_and_closes_session(
    monkeypatch,
    level_key,
):
    session = {
        "_id": "session-1",
        "attempt_id": "attempt-1",
        "mode": "level",
        "level_key": level_key,
        "correct_count": 9,
        "question_ids": [f"q{index}" for index in range(10)],
    }
    monkeypatch.setattr(
        attempt_finalize,
        "validate_completed_owned_quiz_session",
        lambda _session_id, _user_id: session,
    )
    captured = {}

    def fake_progress(**kwargs):
        captured.update(kwargs)
        return {"kind": "learning", "applied": True, "points": 0}

    monkeypatch.setattr(attempt_finalize, "apply_learning_progress_once", fake_progress)
    monkeypatch.setattr(
        attempt_finalize,
        "finish_completed_owned_quiz_session",
        lambda _session_id, _user_id: session,
    )

    def forbidden_scored_path(**_kwargs):
        raise AssertionError("learning course entered scored finalizer")

    monkeypatch.setattr(attempt_finalize, "_finalize_normal_result", forbidden_scored_path)

    result = attempt_finalize.finalize_normal_result(
        user_id=7,
        data={
            "session_id": "session-1",
            "attempt_id": "attempt-1",
            "level_key": level_key,
            "username": "reader",
            "first_name": "Reader",
            "is_retry": False,
        },
        score=9,
        total=10,
        time_seconds=33.0,
        achievement_rewards={},
    )

    assert captured["level_key"] == level_key
    assert captured["result_id"] == "quiz:attempt-1"
    assert captured["score"] == 9
    assert captured["total"] == 10
    assert result["scored"] is False
    assert result["learning"] is True
    assert result["earned_base"] == 0
    assert result["session_finished"] is True
    assert result["new_achievements"] == []
