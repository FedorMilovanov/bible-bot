from __future__ import annotations

from types import SimpleNamespace

import web_api.result_store as result_store


class FakeCollection:
    def __init__(self, document: dict):
        self.document = document
        self.updates: list[tuple[dict, dict, bool]] = []

    def find_one(self, query):
        if query.get("_id") != self.document.get("_id"):
            return None
        return self.document

    def update_one(self, query, update, upsert=False):
        self.updates.append((query, update, upsert))
        return SimpleNamespace(modified_count=1)


def test_learning_receipt_rejects_mismatched_retry_but_accepts_safe_legacy_receipt():
    current = {
        "kind": "learning",
        "level_key": "chapter2",
        "score": 8,
        "total": 10,
        "points": 0,
        "daily_bonus": 0,
        "new_achievements": [],
    }
    assert result_store._validated_learning_receipt(
        current,
        level_key="chapter2",
        score=8,
        total=10,
    ) == current
    assert result_store._validated_learning_receipt(
        current,
        level_key="chapter2",
        score=7,
        total=10,
    ) is None
    assert result_store._validated_learning_receipt(
        current,
        level_key="chapter3",
        score=8,
        total=10,
    ) is None

    legacy = {
        "kind": "learning",
        "level_key": "chapter2",
        "points": 0,
        "daily_bonus": 0,
        "new_achievements": [],
    }
    assert result_store._validated_learning_receipt(
        legacy,
        level_key="chapter2",
        score=8,
        total=10,
    ) == legacy


def test_learning_result_persists_score_total_without_competitive_side_effects(monkeypatch):
    collection = FakeCollection({"_id": "7"})
    monkeypatch.setattr(result_store, "_user_collection", lambda: collection)
    monkeypatch.setattr(result_store, "_prune_old_receipts", lambda _user_id: None)

    receipt = result_store._apply_learning_result_once(
        user_id=7,
        result_id="session-1",
        username="reader",
        first_name="Reader",
        level_key="chapter2",
        score=8,
        total=10,
    )

    assert receipt is not None
    assert receipt["kind"] == "learning"
    assert receipt["level_key"] == "chapter2"
    assert receipt["score"] == 8
    assert receipt["total"] == 10
    assert receipt["points"] == 0
    assert receipt["daily_bonus"] == 0
    assert receipt["new_achievements"] == []

    _query, update, _upsert = collection.updates[0]
    assert update["$inc"] == {
        "chapter2_attempts": 1,
        "chapter2_correct": 8,
        "chapter2_total": 10,
    }
    assert "total_points" not in update["$inc"]
    assert "total_tests" not in update["$inc"]
    assert "total_questions_answered" not in update["$inc"]
    assert "total_correct_answers" not in update["$inc"]


def test_existing_mismatched_learning_receipt_fails_closed_without_second_write(monkeypatch):
    collection = FakeCollection(
        {
            "_id": "7",
            "miniapp_result_receipts": {
                "session-1": {
                    "kind": "learning",
                    "level_key": "chapter2",
                    "score": 8,
                    "total": 10,
                    "points": 0,
                    "daily_bonus": 0,
                    "new_achievements": [],
                }
            },
        }
    )
    monkeypatch.setattr(result_store, "_user_collection", lambda: collection)

    receipt = result_store._apply_learning_result_once(
        user_id=7,
        result_id="session-1",
        username="reader",
        first_name="Reader",
        level_key="chapter2",
        score=7,
        total=10,
    )

    assert receipt is None
    assert collection.updates == []
