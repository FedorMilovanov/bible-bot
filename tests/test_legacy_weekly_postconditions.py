from copy import deepcopy
from types import SimpleNamespace

import pytest

import database
import legacy_result_store as store


class WeeklyRaceStore:
    def __init__(self, doc, *, after_failed_update=None):
        self.doc = deepcopy(doc)
        self.after_failed_update = deepcopy(after_failed_update)
        self.update_calls = 0

    def find_one(self, query):
        if self.doc is None or self.doc.get("_id") != query["_id"]:
            return None
        return deepcopy(self.doc)

    def insert_one(self, doc):
        self.doc = deepcopy(doc)

    def update_one(self, query, update):
        self.update_calls += 1
        if self.after_failed_update is not None or self.doc is None:
            self.doc = deepcopy(self.after_failed_update)
            return SimpleNamespace(modified_count=0)

        candidate = update["$set"]
        current_score = int(self.doc.get("best_score", 0))
        current_time = float(self.doc.get("best_time", 999999))
        better = current_score < candidate["best_score"]
        tied_faster = (
            current_score == candidate["best_score"]
            and current_time > candidate["best_time"]
        )
        if not (better or tied_faster):
            return SimpleNamespace(modified_count=0)
        self.doc.update(deepcopy(candidate))
        return SimpleNamespace(modified_count=1)


def _doc(*, score=17, time_seconds=50):
    return {
        "_id": "2026-W33_random20_42",
        "week_id": "2026-W33",
        "mode": "random20",
        "user_id": "42",
        "best_score": score,
        "best_time": time_seconds,
    }


def _sync():
    return store.sync_weekly_best(
        user_id=42,
        username="u",
        first_name="User",
        mode="random20",
        score=18,
        time_seconds=40,
        week_id="2026-W33",
    )


def test_weekly_noop_is_allowed_when_concurrent_result_is_better(monkeypatch):
    weekly = WeeklyRaceStore(
        _doc(),
        after_failed_update=_doc(score=19, time_seconds=100),
    )
    monkeypatch.setattr(database, "weekly_lb_collection", weekly)

    assert _sync() is None
    assert weekly.doc["best_score"] == 19


def test_weekly_noop_is_allowed_when_concurrent_tie_is_faster(monkeypatch):
    weekly = WeeklyRaceStore(
        _doc(),
        after_failed_update=_doc(score=18, time_seconds=30),
    )
    monkeypatch.setattr(database, "weekly_lb_collection", weekly)

    assert _sync() is None
    assert weekly.doc["best_time"] == 30


def test_weekly_missing_document_after_update_keeps_finalization_retryable(monkeypatch):
    weekly = WeeklyRaceStore(_doc(), after_failed_update=None)
    weekly.doc = _doc()

    def disappear(_query, _update):
        weekly.update_calls += 1
        weekly.doc = None
        return SimpleNamespace(modified_count=0)

    weekly.update_one = disappear
    monkeypatch.setattr(database, "weekly_lb_collection", weekly)

    with pytest.raises(
        store.LegacyResultStoreUnavailable,
        match="weekly best update was not persisted",
    ):
        _sync()


def test_weekly_worse_document_after_noop_keeps_finalization_retryable(monkeypatch):
    weekly = WeeklyRaceStore(
        _doc(),
        after_failed_update=_doc(score=16, time_seconds=10),
    )
    monkeypatch.setattr(database, "weekly_lb_collection", weekly)

    with pytest.raises(
        store.LegacyResultStoreUnavailable,
        match="weekly best update was not persisted",
    ):
        _sync()
