import sys
from types import SimpleNamespace

from questions.pool_policy import is_non_scoring_learning_pool
from web_api import result_store


def test_chapter5_uses_learning_only_policy():
    assert is_non_scoring_learning_pool("chapter2") is True
    assert is_non_scoring_learning_pool("chapter3") is True
    assert is_non_scoring_learning_pool("chapter5") is True
    assert is_non_scoring_learning_pool("easy_p1") is False
    assert is_non_scoring_learning_pool("random20") is False


def test_regular_result_router_sends_chapter5_to_learning_path(monkeypatch):
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
        result_id="chapter5-learning-result",
        username="learner",
        first_name="Learner",
        level_key="chapter5",
        score=8,
        total=10,
        time_seconds=33.0,
        score_multiplier=2.0,
        is_perfect=False,
        max_streak=8,
    )

    assert captured["level_key"] == "chapter5"
    assert captured["score"] == 8
    assert captured["total"] == 10
    assert receipt["kind"] == "learning"
    assert receipt["points"] == 0


def test_learning_persistence_updates_chapter5_progress_not_ranking_totals(monkeypatch):
    captured = {}

    class FakeCollection:
        def find_one(self, query):
            assert query == {"_id": "7"}
            return {"_id": "7", "total_points": 900, "total_tests": 12}

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
        result_id="chapter5-progress-only",
        username="learner",
        first_name="Learner",
        level_key="chapter5",
        score=9,
        total=10,
    )

    assert receipt["kind"] == "learning"
    assert receipt["points"] == 0
    assert receipt["daily_bonus"] == 0
    assert receipt["new_achievements"] == []

    inc = captured["update"]["$inc"]
    assert inc == {
        "chapter5_attempts": 1,
        "chapter5_correct": 9,
        "chapter5_total": 10,
    }
    assert "total_points" not in inc
    assert "total_tests" not in inc
    assert "perfect_count" not in inc
    assert captured["update"]["$max"] == {"chapter5_best_score": 9}
