import sys
from types import SimpleNamespace

from questions.pool_policy import is_non_scoring_learning_pool
from web_api import result_store


def test_chapter4_uses_learning_only_policy():
    assert is_non_scoring_learning_pool("chapter4") is True
    assert is_non_scoring_learning_pool("easy_p1") is False
    assert is_non_scoring_learning_pool("random20") is False


def test_regular_result_router_sends_chapter4_to_learning_path(monkeypatch):
    captured = {}

    def fake_learning(**kwargs):
        captured.update(kwargs)
        return {
            "points": 0,
            "daily_bonus": 0,
            "new_achievements": [],
            "kind": "learning",
            "level_key": kwargs["level_key"],
        }

    monkeypatch.setitem(sys.modules, "database", SimpleNamespace(collection=object()))
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

    assert captured["level_key"] == "chapter4"
    assert captured["score"] == 8
    assert captured["total"] == 10
    assert receipt == {
        "points": 0,
        "daily_bonus": 0,
        "new_achievements": [],
        "kind": "learning",
        "level_key": "chapter4",
    }


def test_learning_persistence_updates_chapter4_progress_not_ranking_totals(monkeypatch):
    captured = {}

    class FakeCollection:
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

    def fake_persist(user_id, result_id, update, receipt):
        captured["user_id"] = user_id
        captured["result_id"] = result_id
        captured["update"] = update
        captured["receipt"] = receipt
        return dict(receipt)

    monkeypatch.setattr(result_store, "_user_collection", lambda: FakeCollection())
    monkeypatch.setattr(result_store, "_persist_once", fake_persist)

    receipt = result_store._apply_learning_result_once(
        user_id=7,
        result_id="ch4-learning-persist",
        username="reader",
        first_name="Reader",
        level_key="chapter4",
        score=9,
        total=10,
    )

    assert receipt == {
        "points": 0,
        "daily_bonus": 0,
        "new_achievements": [],
        "kind": "learning",
        "level_key": "chapter4",
        "score": 9,
        "total": 10,
    }
    update = captured["update"]
    assert update["$inc"] == {
        "chapter4_attempts": 1,
        "chapter4_correct": 9,
        "chapter4_total": 10,
    }
    assert update["$max"] == {"chapter4_best_score": 9}
    assert "total_points" not in update["$inc"]
    assert "total_tests" not in update["$inc"]
    assert "perfect_count" not in update["$inc"]
    assert captured["receipt"]["daily_bonus"] == 0
    assert captured["receipt"]["new_achievements"] == []
    assert captured["receipt"]["score"] == 9
    assert captured["receipt"]["total"] == 10
