import copy
from types import SimpleNamespace

import database
from web_api import quiz as quiz_module
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


class FakeUserCollection:
    def __init__(self, doc):
        self.doc = copy.deepcopy(doc)

    def find_one(self, query):
        if self._matches(query):
            return copy.deepcopy(self.doc)
        return None

    def _matches(self, query):
        for field, expected in query.items():
            actual = _get_path(self.doc, field)
            if isinstance(expected, dict) and "$exists" in expected:
                if (actual is not _MISSING) != bool(expected["$exists"]):
                    return False
            elif actual is _MISSING or actual != expected:
                return False
        return True

    def update_one(self, query, update, **kwargs):
        if not self._matches(query):
            return SimpleNamespace(modified_count=0)

        for field, amount in update.get("$inc", {}).items():
            current = _get_path(self.doc, field)
            _set_path(self.doc, field, (0 if current is _MISSING else current) + amount)
        for field, value in update.get("$set", {}).items():
            _set_path(self.doc, field, value)
        for field, value in update.get("$max", {}).items():
            current = _get_path(self.doc, field)
            if current is _MISSING or value > current:
                _set_path(self.doc, field, value)
        return SimpleNamespace(modified_count=1)


class FakeSessionCollection:
    def __init__(self, doc):
        self.doc = copy.deepcopy(doc)

    def find_one(self, query):
        if all(self.doc.get(key) == value for key, value in query.items()):
            return copy.deepcopy(self.doc)
        return None

    def update_one(self, query, update, **kwargs):
        if not all(self.doc.get(key) == value for key, value in query.items()):
            return SimpleNamespace(modified_count=0)
        for field, value in update.get("$set", {}).items():
            self.doc[field] = copy.deepcopy(value)
        return SimpleNamespace(modified_count=1)


def base_user(user_id=123):
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
        "achievements": {},
    }


def test_regular_result_receipt_prevents_duplicate_aggregate(monkeypatch):
    users = FakeUserCollection(base_user())
    monkeypatch.setattr(database, "collection", users)

    first = result_store.apply_regular_result_once(
        user_id=123,
        result_id="session-regular",
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
    second = result_store.apply_regular_result_once(
        user_id=123,
        result_id="session-regular",
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

    assert first == second
    assert first["points"] == 12  # 7 base + first-test daily bonus 5
    assert first["daily_bonus"] == 5
    assert users.doc["total_tests"] == 1
    assert users.doc["easy_p1_attempts"] == 1
    assert users.doc["total_points"] == 12
    assert users.doc["miniapp_result_receipts"]["session-regular"]["points"] == 12


def test_challenge_receipt_prevents_duplicate_bonus_and_achievement(monkeypatch):
    users = FakeUserCollection(base_user())
    monkeypatch.setattr(database, "collection", users)

    first = result_store.apply_challenge_result_once(
        user_id=123,
        result_id="session-challenge",
        username="tester",
        first_name="Test",
        mode="random20",
        score=20,
        total=20,
        time_seconds=90,
    )
    second = result_store.apply_challenge_result_once(
        user_id=123,
        result_id="session-challenge",
        username="tester",
        first_name="Test",
        mode="random20",
        score=20,
        total=20,
        time_seconds=90,
    )

    assert first == second
    assert first["points"] == 120  # 20 base + Perfect-20 daily Challenge bonus 100
    assert first["daily_bonus"] == 100
    assert "⭐ Perfect 20 — разблокировано!" in first["new_achievements"]
    assert users.doc["total_tests"] == 1
    assert users.doc["random20_attempts"] == 1
    assert users.doc["total_points"] == 120
    assert "perfect_20" in users.doc["achievements"]


def test_finalizer_recovers_after_crash_between_user_update_and_session_finish(monkeypatch):
    users = FakeUserCollection(base_user())
    monkeypatch.setattr(database, "collection", users)

    session = {
        "_id": "session-crash",
        "status": "finalizing",
        "leaderboard_recorded": True,
        "question_count": 10,
        "questions": [],
        "correct_count": 7,
        "started_at_dt": quiz_module._now(),
        "is_challenge": False,
        "stats_level_key": "easy_p1",
        "score_multiplier": 1.0,
        "max_streak": 3,
    }
    sessions = FakeSessionCollection(session)
    monkeypatch.setattr(quiz_module, "miniapp_sessions", lambda: sessions)

    applied_before_crash = result_store.apply_regular_result_once(
        user_id=123,
        result_id="session-crash",
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
    assert applied_before_crash["points"] == 12
    assert users.doc["total_tests"] == 1

    recovered = quiz_module._finalize_quiz(
        sessions.find_one({"_id": "session-crash"}),
        {"id": 123, "username": "tester", "first_name": "Test"},
    )

    assert recovered["points"] == 12
    assert recovered["daily_bonus"] == 5
    assert sessions.doc["status"] == "finished"
    assert users.doc["total_tests"] == 1
    assert users.doc["easy_p1_attempts"] == 1
    assert users.doc["total_points"] == 12
