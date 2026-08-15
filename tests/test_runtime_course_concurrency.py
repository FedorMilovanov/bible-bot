from __future__ import annotations

import copy
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from pymongo.errors import DuplicateKeyError

import database
import legacy_result_store
from web_api import result_store


_MISSING = object()


def _get_path(doc: dict, path: str):
    value = doc
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return _MISSING
        value = value[part]
    return value


def _set_path(doc: dict, path: str, value) -> None:
    target = doc
    parts = path.split(".")
    for part in parts[:-1]:
        target = target.setdefault(part, {})
    target[parts[-1]] = copy.deepcopy(value)


def _unset_path(doc: dict, path: str) -> None:
    target = doc
    parts = path.split(".")
    for part in parts[:-1]:
        if not isinstance(target, dict) or part not in target:
            return
        target = target[part]
    if isinstance(target, dict):
        target.pop(parts[-1], None)


def _matches(doc: dict, query: dict) -> bool:
    for field, expected in query.items():
        if field == "$or":
            if not any(_matches(doc, branch) for branch in expected):
                return False
            continue
        actual = _get_path(doc, field)
        if isinstance(expected, dict):
            if "$exists" in expected and (actual is not _MISSING) != bool(expected["$exists"]):
                return False
            if "$lt" in expected and not (actual is not _MISSING and actual < expected["$lt"]):
                return False
            if "$gt" in expected and not (actual is not _MISSING and actual > expected["$gt"]):
                return False
            if "$ne" in expected:
                forbidden = expected["$ne"]
                if actual is not _MISSING and (
                    actual == forbidden or (isinstance(actual, list) and forbidden in actual)
                ):
                    return False
            continue
        if actual is _MISSING or actual != expected:
            return False
    return True


def _apply_update(doc: dict, update: dict) -> None:
    for field, amount in update.get("$inc", {}).items():
        current = _get_path(doc, field)
        _set_path(doc, field, (0 if current is _MISSING else current) + amount)
    for field, value in update.get("$set", {}).items():
        _set_path(doc, field, value)
    for field, value in update.get("$max", {}).items():
        current = _get_path(doc, field)
        if current is _MISSING or value > current:
            _set_path(doc, field, value)
    for field in update.get("$unset", {}):
        _unset_path(doc, field)


class AtomicUsers:
    """Small atomic Mongo model with a barrier before the first competing writes."""

    def __init__(self, doc: dict, *, competing_writes: int = 0, acknowledged: bool = True):
        self.doc = copy.deepcopy(doc)
        self._lock = threading.RLock()
        self._write_count_lock = threading.Lock()
        self._writes_waiting = 0
        self._barrier = threading.Barrier(competing_writes) if competing_writes > 1 else None
        self.acknowledged = acknowledged

    def _before_write(self) -> None:
        if self._barrier is None:
            return
        should_wait = False
        with self._write_count_lock:
            if self._writes_waiting < self._barrier.parties:
                self._writes_waiting += 1
                should_wait = True
        if should_wait:
            self._barrier.wait(timeout=5)

    def find_one(self, query, projection=None, *args, **kwargs):
        with self._lock:
            if self.doc is None or not _matches(self.doc, query):
                return None
            if not projection:
                return copy.deepcopy(self.doc)
            projected = {"_id": self.doc["_id"]}
            for field, include in projection.items():
                if not include:
                    continue
                value = _get_path(self.doc, field)
                if value is not _MISSING:
                    _set_path(projected, field, value)
            return projected

    def update_one(self, query, update, **kwargs):
        self._before_write()
        with self._lock:
            if not self.acknowledged:
                return SimpleNamespace(acknowledged=False, modified_count=0)
            if self.doc is None or not _matches(self.doc, query):
                return SimpleNamespace(acknowledged=True, modified_count=0)
            _apply_update(self.doc, update)
            return SimpleNamespace(acknowledged=True, modified_count=1)

    def find_one_and_update(self, query, update, return_document=None):
        self._before_write()
        with self._lock:
            if self.doc is None or not _matches(self.doc, query):
                return None
            _apply_update(self.doc, update)
            return copy.deepcopy(self.doc)


class AtomicWeekly:
    def __init__(self, *, competing_first_reads: int = 0):
        self.docs: dict[str, dict] = {}
        self._lock = threading.RLock()
        self._read_count_lock = threading.Lock()
        self._reads_waiting = 0
        self._barrier = (
            threading.Barrier(competing_first_reads) if competing_first_reads > 1 else None
        )

    def find_one(self, query, *args, **kwargs):
        with self._lock:
            doc = copy.deepcopy(self.docs.get(query["_id"]))
        if self._barrier is not None:
            should_wait = False
            with self._read_count_lock:
                if self._reads_waiting < self._barrier.parties:
                    self._reads_waiting += 1
                    should_wait = True
            if should_wait:
                self._barrier.wait(timeout=5)
        return doc

    def insert_one(self, doc):
        with self._lock:
            if doc["_id"] in self.docs:
                raise DuplicateKeyError("synthetic weekly insert race")
            self.docs[doc["_id"]] = copy.deepcopy(doc)
        return SimpleNamespace(acknowledged=True)

    def update_one(self, query, update, **kwargs):
        with self._lock:
            doc = self.docs.get(query["_id"])
            if doc is None or not _matches(doc, query):
                return SimpleNamespace(acknowledged=True, modified_count=0)
            _apply_update(doc, update)
            return SimpleNamespace(acknowledged=True, modified_count=1)


def _base_user() -> dict:
    return {
        "_id": "42",
        "username": "old",
        "first_name": "Old",
        "total_tests": 0,
        "total_questions_answered": 0,
        "total_correct_answers": 0,
        "total_time_spent": 0,
        "total_points": 0,
        "easy_attempts": 0,
        "easy_correct": 0,
        "easy_total": 0,
        "easy_best_score": 0,
        "easy_p1_attempts": 0,
        "easy_p1_correct": 0,
        "easy_p1_total": 0,
        "easy_p1_best_score": 0,
        "chapter2_attempts": 0,
        "chapter2_correct": 0,
        "chapter2_total": 0,
        "chapter2_best_score": 0,
        "perfect_count": 0,
        "max_streak_ever": 0,
        "daily_activity_streak": 0,
        "daily_activity_last": "",
        "last_daily_bonus": "",
        "challenge_streak_count": 0,
        "challenge_streak_last_date": "",
        "achievements": {},
    }


def _mini_scored(result_id: str):
    return result_store.apply_regular_result_once(
        user_id=42,
        result_id=result_id,
        username="u",
        first_name="User",
        level_key="easy_p1",
        score=7,
        total=10,
        time_seconds=20,
        score_multiplier=1.0,
        is_perfect=False,
        max_streak=3,
    )


def _learning(result_id: str, *, score: int = 8, total: int = 10):
    return result_store.apply_regular_result_once(
        user_id=42,
        result_id=result_id,
        username="u",
        first_name="User",
        level_key="chapter2",
        score=score,
        total=total,
        time_seconds=20,
        score_multiplier=2.0,
        is_perfect=score == total,
        max_streak=4,
    )


def _legacy_scored(result_id: str):
    return legacy_result_store.apply_base_result_once(
        result_id=result_id,
        user_id=42,
        username="u",
        first_name="User",
        level_key="easy",
        score=8,
        total=10,
        time_seconds=12.5,
        max_streak=4,
    )


def _run_pair(first, second):
    with ThreadPoolExecutor(max_workers=2) as pool:
        a = pool.submit(first)
        b = pool.submit(second)
        return a.result(timeout=10), b.result(timeout=10)


def test_two_distinct_scored_miniapp_results_serialize_economics(monkeypatch):
    users = AtomicUsers(_base_user(), competing_writes=2)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "db", None, raising=False)

    first, second = _run_pair(lambda: _mini_scored("mini-a"), lambda: _mini_scored("mini-b"))

    assert first is not None and second is not None
    assert sorted([first["daily_bonus"], second["daily_bonus"]]) == [0, 5]
    assert users.doc["total_tests"] == 2
    assert users.doc["easy_p1_attempts"] == 2
    assert users.doc["total_points"] == 19  # 7 + 7 base, exactly one daily 5


def test_duplicate_same_scored_result_retry_is_exactly_once(monkeypatch):
    users = AtomicUsers(_base_user(), competing_writes=2)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "db", None, raising=False)

    first, second = _run_pair(lambda: _mini_scored("same-mini"), lambda: _mini_scored("same-mini"))

    assert first == second
    assert users.doc["total_tests"] == 1
    assert users.doc["easy_p1_attempts"] == 1
    assert users.doc["total_points"] == 12


def test_learning_same_attempt_retry_is_exactly_once(monkeypatch):
    users = AtomicUsers(_base_user(), competing_writes=2)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "db", None, raising=False)

    first, second = _run_pair(lambda: _learning("learn-same"), lambda: _learning("learn-same"))

    assert first == second
    assert first["kind"] == "learning"
    assert first["points"] == 0 and first["daily_bonus"] == 0
    assert first["new_achievements"] == []
    assert users.doc["chapter2_attempts"] == 1
    assert users.doc["total_tests"] == 0
    assert users.doc["total_points"] == 0


def test_mismatched_learning_replay_fails_closed(monkeypatch):
    users = AtomicUsers(_base_user())
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "db", None, raising=False)

    assert _learning("learn-id", score=8, total=10) is not None
    assert _learning("learn-id", score=7, total=10) is None
    assert _learning("learn-id", score=8, total=9) is None
    assert users.doc["chapter2_attempts"] == 1
    assert users.doc["chapter2_correct"] == 8
    assert users.doc["chapter2_total"] == 10


def test_two_legitimate_learning_attempts_accumulate_concurrently(monkeypatch):
    users = AtomicUsers(_base_user(), competing_writes=2)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "db", None, raising=False)

    first, second = _run_pair(
        lambda: _learning("learn-a", score=6),
        lambda: _learning("learn-b", score=9),
    )

    assert first is not None and second is not None
    assert users.doc["chapter2_attempts"] == 2
    assert users.doc["chapter2_correct"] == 15
    assert users.doc["chapter2_total"] == 20
    assert users.doc["chapter2_best_score"] == 9
    assert users.doc["total_tests"] == 0
    assert users.doc["total_points"] == 0


def test_miniapp_scoring_and_legacy_telegram_scoring_share_cas(monkeypatch):
    users = AtomicUsers(_base_user(), competing_writes=2)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "db", None, raising=False)

    mini, legacy = _run_pair(lambda: _mini_scored("mini-cross"), lambda: _legacy_scored("tg-cross"))

    assert mini is not None
    assert legacy["receipt"]["result"]["score"] == 8
    assert users.doc["total_tests"] == 2
    assert users.doc["easy_p1_attempts"] == 1
    assert users.doc["easy_attempts"] == 1
    assert users.doc["total_questions_answered"] == 20
    assert users.doc["total_correct_answers"] == 15


def test_chapter_learning_and_scored_quiz_do_not_interfere(monkeypatch):
    users = AtomicUsers(_base_user(), competing_writes=2)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "db", None, raising=False)

    learning, scored = _run_pair(lambda: _learning("learn-cross"), lambda: _mini_scored("score-cross"))

    assert learning is not None and scored is not None
    assert users.doc["chapter2_attempts"] == 1
    assert users.doc["easy_p1_attempts"] == 1
    assert users.doc["total_tests"] == 1
    assert users.doc["total_points"] == 12
    assert learning["points"] == 0 and learning["daily_bonus"] == 0


def test_weekly_first_insert_race_is_monotonic(monkeypatch):
    weekly = AtomicWeekly(competing_first_reads=2)
    monkeypatch.setattr(database, "weekly_lb_collection", weekly, raising=False)

    def project(score, seconds):
        return result_store._sync_weekly_challenge_result(
            user_id=42,
            username="u",
            first_name="User",
            mode="random20",
            score=score,
            time_seconds=seconds,
            week_id="2026-W33",
        )

    _run_pair(lambda: project(18, 80.0), lambda: project(20, 95.0))

    doc = weekly.docs["2026-W33_random20_42"]
    assert doc["best_score"] == 20
    assert doc["best_time"] == 95.0


def test_unacknowledged_miniapp_write_is_not_treated_as_durable(monkeypatch):
    users = AtomicUsers(_base_user(), acknowledged=False)
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "db", None, raising=False)

    assert _mini_scored("w0-result") is None
    assert users.doc["total_tests"] == 0
    assert users.doc["total_points"] == 0
    assert "miniapp_result_receipts" not in users.doc
