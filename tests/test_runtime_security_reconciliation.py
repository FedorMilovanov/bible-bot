import asyncio
import copy
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from types import SimpleNamespace

import pytest

import battle_integrity
import database
import keep_alive
import legacy_delivery_worker
import legacy_session_access
from web_api import result_store


_MISSING = object()


def _get_path(doc, path):
    value = doc
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return _MISSING
        value = value[part]
    return value


def _set_path(doc, path, value):
    target = doc
    parts = path.split(".")
    for part in parts[:-1]:
        target = target.setdefault(part, {})
    target[parts[-1]] = copy.deepcopy(value)


class BarrierUserCollection:
    """Thread-safe Mongo-shaped fake that forces two callers to read one snapshot."""

    def __init__(self, doc):
        self.doc = copy.deepcopy(doc)
        self._lock = threading.Lock()
        self._barrier = threading.Barrier(2)
        self._initial_reads = 0

    def _matches_locked(self, query):
        for field, expected in query.items():
            actual = _get_path(self.doc, field)
            if isinstance(expected, dict) and "$exists" in expected:
                if (actual is not _MISSING) != bool(expected["$exists"]):
                    return False
            elif actual is _MISSING or actual != expected:
                return False
        return True

    def find_one(self, query, *args, **kwargs):
        wait = False
        with self._lock:
            if query == {"_id": self.doc["_id"]} and self._initial_reads < 2:
                self._initial_reads += 1
                wait = True
            result = copy.deepcopy(self.doc) if self._matches_locked(query) else None
        if wait:
            self._barrier.wait(timeout=2)
        return result

    def update_one(self, query, update, **kwargs):
        with self._lock:
            if not self._matches_locked(query):
                return SimpleNamespace(modified_count=0, acknowledged=True)
            for field, amount in update.get("$inc", {}).items():
                current = _get_path(self.doc, field)
                _set_path(
                    self.doc,
                    field,
                    (0 if current is _MISSING else current) + amount,
                )
            for field, value in update.get("$set", {}).items():
                _set_path(self.doc, field, value)
            for field, value in update.get("$max", {}).items():
                current = _get_path(self.doc, field)
                if current is _MISSING or value > current:
                    _set_path(self.doc, field, value)
            return SimpleNamespace(modified_count=1, acknowledged=True)


class AtomicBattleCollection:
    def __init__(self, doc):
        self.doc = copy.deepcopy(doc)
        self._lock = threading.Lock()

    def _matches(self, query):
        for field, expected in query.items():
            actual = self.doc.get(field, _MISSING)
            if isinstance(expected, dict) and "$ne" in expected:
                if actual == expected["$ne"]:
                    return False
            elif actual is _MISSING or actual != expected:
                return False
        return True

    def find_one_and_update(self, query, update, **kwargs):
        with self._lock:
            if not self._matches(query):
                return None
            for field, value in update.get("$set", {}).items():
                self.doc[field] = copy.deepcopy(value)
            return copy.deepcopy(self.doc)


class UnacknowledgedInsertCollection:
    def insert_one(self, doc):
        return SimpleNamespace(acknowledged=False)


def _base_user(user_id=123):
    return {
        "_id": str(user_id),
        "username": "tester",
        "first_name": "Test",
        "total_points": 0,
        "total_tests": 0,
        "total_questions_answered": 0,
        "total_correct_answers": 0,
        "total_time_spent": 0,
        "easy_p1_attempts": 0,
        "easy_p1_correct": 0,
        "easy_p1_total": 0,
        "easy_p1_best_score": 0,
        "random20_attempts": 0,
        "random20_correct": 0,
        "random20_total": 0,
        "random20_best_score": 0,
        "perfect_count": 0,
        "max_streak_ever": 0,
        "daily_activity_streak": 0,
        "daily_activity_last": "",
        "last_daily_bonus": "",
        "challenge_streak_count": 0,
        "challenge_streak_last_date": "",
        "random20_last_bonus_date": "",
        "achievements": {},
    }


def _run_two(call):
    with ThreadPoolExecutor(max_workers=2) as pool:
        left = pool.submit(call, "result-a")
        right = pool.submit(call, "result-b")
        return left.result(timeout=3), right.result(timeout=3)


def test_concurrent_distinct_regular_results_pay_daily_bonus_once(monkeypatch):
    users = BarrierUserCollection(_base_user())
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "db", None, raising=False)

    def apply(result_id):
        return result_store.apply_regular_result_once(
            user_id=123,
            result_id=result_id,
            username="tester",
            first_name="Test",
            level_key="easy_p1",
            score=7,
            total=10,
            time_seconds=30,
            score_multiplier=1.0,
            is_perfect=False,
            max_streak=3,
        )

    first, second = _run_two(apply)

    assert first is not None and second is not None
    assert sorted([first["daily_bonus"], second["daily_bonus"]]) == [0, 5]
    assert users.doc["total_tests"] == 2
    assert users.doc["easy_p1_attempts"] == 2
    assert users.doc["total_points"] == 19  # 7 + 7 base, one 5-point daily bonus


def test_concurrent_distinct_challenge_results_pay_mode_bonus_once(monkeypatch):
    users = BarrierUserCollection(_base_user())
    monkeypatch.setattr(database, "collection", users)
    monkeypatch.setattr(database, "db", None, raising=False)
    monkeypatch.setattr(database, "weekly_lb_collection", None)

    def apply(result_id):
        return result_store.apply_challenge_result_once(
            user_id=123,
            result_id=result_id,
            username="tester",
            first_name="Test",
            mode="random20",
            score=20,
            total=20,
            time_seconds=90,
        )

    first, second = _run_two(apply)

    assert first is not None and second is not None
    assert sorted([first["daily_bonus"], second["daily_bonus"]]) == [0, 100]
    assert users.doc["total_tests"] == 2
    assert users.doc["random20_attempts"] == 2
    assert users.doc["total_points"] == 140  # 20 + 20 base, one 100-point bonus
    assert users.doc["achievements"]["perfect_20"] == result_store._today_utc()


def test_simultaneous_battle_join_has_exactly_one_winner(monkeypatch):
    battles = AtomicBattleCollection(
        {
            "_id": "battle-1",
            "creator_id": 1,
            "creator_name": "creator",
            "opponent_id": None,
            "opponent_name": None,
            "status": "waiting",
        }
    )
    monkeypatch.setattr(database, "battles_collection", battles)
    go = threading.Barrier(3)

    def join(user_id):
        go.wait(timeout=2)
        return battle_integrity.claim_battle_opponent(
            "battle-1", user_id, f"user-{user_id}"
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(join, 2)
        second = pool.submit(join, 3)
        go.wait(timeout=2)
        results = [first.result(timeout=2), second.result(timeout=2)]

    assert sum(result is not None for result in results) == 1
    assert battles.doc["opponent_id"] in {2, 3}
    assert battles.doc["status"] == "in_progress"


def test_strict_session_create_rejects_unacknowledged_insert(monkeypatch):
    monkeypatch.setattr(legacy_session_access, "ensure_active_session_unique_index", lambda: True)
    monkeypatch.setattr(legacy_session_access, "_collection", lambda: UnacknowledgedInsertCollection())
    monkeypatch.setattr(
        legacy_session_access,
        "_database",
        lambda: SimpleNamespace(
            _now_utc=lambda: datetime.utcnow(),
            _uid=lambda value: str(value),
        ),
    )
    monkeypatch.setattr(
        legacy_session_access,
        "validated_session_spec",
        lambda **kwargs: {
            "mode": kwargs["mode"],
            "question_ids": kwargs["question_ids"],
            "questions_data": kwargs["questions_data"],
            "level_key": kwargs.get("level_key"),
            "level_name": kwargs.get("level_name"),
            "time_limit": kwargs.get("time_limit"),
            "chat_id": kwargs.get("chat_id"),
            "is_retry": kwargs.get("is_retry", False),
        },
    )

    with pytest.raises(legacy_session_access.QuizSessionAccessUnavailable):
        legacy_session_access.create_quiz_session_strict(
            user_id=123,
            mode="normal",
            question_ids=["q1"],
            questions_data=[{"id": "q1"}],
        )


def test_legacy_session_create_rejects_unacknowledged_insert(monkeypatch):
    monkeypatch.setattr(database, "quiz_sessions_collection", UnacknowledgedInsertCollection())

    with pytest.raises(database.LegacyQuizSessionPersistenceUnavailable):
        database.create_quiz_session(
            user_id=123,
            mode="normal",
            question_ids=["q1"],
            questions_data=[{"id": "q1"}],
        )


def test_waitress_default_envelope_remains_one_megabyte(monkeypatch):
    captured = {}

    def serve(app, **kwargs):
        captured.update(kwargs)

    monkeypatch.setitem(sys.modules, "waitress", SimpleNamespace(serve=serve))
    monkeypatch.delenv("MAX_REQUEST_BODY_BYTES", raising=False)
    monkeypatch.delenv("MAX_REQUEST_HEADER_BYTES", raising=False)
    monkeypatch.setenv("PORT", "8080")
    keep_alive.run()

    assert captured["max_request_body_size"] == 1024 * 1024
    assert captured["max_request_header_size"] == 64 * 1024


def test_delivery_claim_does_not_block_asyncio_loop(monkeypatch):
    def slow_claim(*args, **kwargs):
        time.sleep(0.12)
        return None

    monkeypatch.setattr(legacy_delivery_worker, "claim_battle_result_delivery", slow_claim)

    async def scenario():
        loop = asyncio.get_running_loop()
        started = loop.time()
        delivery = asyncio.create_task(
            legacy_delivery_worker.deliver_battle_recipient_once(
                "battle-1",
                123,
                lambda battle, role: asyncio.sleep(0),
            )
        )
        await asyncio.sleep(0.02)
        ticker_delay = loop.time() - started
        await delivery
        return ticker_delay

    assert asyncio.run(scenario()) < 0.08
