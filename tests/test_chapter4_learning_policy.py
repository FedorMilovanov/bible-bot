import sys
from types import SimpleNamespace

from questions.pool_policy import is_non_scoring_learning_pool
from web_api import result_store


def test_chapter4_uses_learning_only_policy():
    assert is_non_scoring_learning_pool("chapter4") is True
    assert is_non_scoring_learning_pool("easy_p1") is False
    assert is_non_scoring_learning_pool("random20") is False


def test_regular_result_router_sends_chapter4_to_learning_path(monkeypatch):
    calls = []

    def fake_learning(**kwargs):
        calls.append(kwargs)
        return {
            "points": 0,
            "daily_bonus": 0,
            "new_achievements": [],
            "kind": "learning",
            "level_key": kwargs["level_key"],
        }

    monkeypatch.setattr(result_store, "_apply_learning_result_once", fake_learning)
    receipt = result_store.apply_regular_result_once(
        user_id=7,
        result_id="ch4-learning-router",
        username="reader",
        first_name="Reader",
        level_key="chapter4",
        score=8,
        total=10,
        time_seconds=31.0,
        score_multiplier=2.0,
        is_perfect=False,
        max_streak=4,
    )

    assert receipt == {
        "points": 0,
        "daily_bonus": 0,
        "new_achievements": [],
        "kind": "learning",
        "level_key": "chapter4",
    }
    assert len(calls) == 1
    assert calls[0]["level_key"] == "chapter4"
    assert calls[0]["score"] == 8
    assert calls[0]["total"] == 10


def test_learning_persistence_updates_chapter4_progress_not_ranking_totals(monkeypatch):
    captured = {}

    class FakeUsers:
        def find_one(self, query):
            assert query == {"_id": "7"}
            return {
                "_id": "7",
                "chapter4_attempts": 2,
                "chapter4_correct": 11,
                "chapter4_total": 20,
                "chapter4_best_score": 7,
                "total_points": 500,
                "total_tests": 12,
                "perfect_count": 3,
            }

    def fake_persist(*, users, user_id, result_id, level_key, receipt, update_doc):
        captured.update(
            users=users,
            user_id=user_id,
            result_id=result_id,
            level_key=level_key,
            receipt=receipt,
            update_doc=update_doc,
        )
        return receipt

    monkeypatch.setattr(result_store, "_users_collection", lambda: FakeUsers())
    monkeypatch.setattr(result_store, "_persist_regular_receipt", fake_persist)
    monkeypatch.setitem(sys.modules, "database", SimpleNamespace())

    receipt = result_store._apply_learning_result_once(
        user_id=7,
        result_id="ch4-learning-persist",
        username="reader",
        first_name="Reader",
        level_key="chapter4",
        score=9,
        total=10,
        time_seconds=20.0,
        score_multiplier=2.0,
        is_perfect=True,
        max_streak=10,
    )

    assert receipt == {
        "points": 0,
        "daily_bonus": 0,
        "new_achievements": [],
        "kind": "learning",
        "level_key": "chapter4",
    }
    update_doc = captured["update_doc"]
    assert update_doc["$inc"] == {
        "chapter4_attempts": 1,
        "chapter4_correct": 9,
        "chapter4_total": 10,
    }
    assert update_doc["$max"] == {"chapter4_best_score": 9}
    assert "total_points" not in update_doc["$inc"]
    assert "total_tests" not in update_doc["$inc"]
    assert "perfect_count" not in update_doc["$inc"]
    assert captured["receipt"]["daily_bonus"] == 0
    assert captured["receipt"]["new_achievements"] == []
