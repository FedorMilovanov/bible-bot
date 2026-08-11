from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from pymongo.errors import PyMongoError

import legacy_battle_progress as progress


Q1 = {"question": "Кто отрёкся трижды?", "options": ["Пётр", "Павел"], "correct": 0}
Q2 = {"question": "Кто был мытарем?", "options": ["Матфей", "Иоанн"], "correct": 0}


class FakeCollection:
    def __init__(self, *, finds=None, updates=None, find_error=None):
        self.finds = list(finds or [])
        self.updates = list(updates or [])
        self.find_error = find_error
        self.find_calls = []
        self.update_calls = []

    def find_one(self, query):
        self.find_calls.append(deepcopy(query))
        if self.find_error is not None:
            raise self.find_error
        if not self.finds:
            raise AssertionError("unexpected find_one call")
        value = self.finds.pop(0)
        return deepcopy(value)

    def find_one_and_update(self, query, update, return_document=None):
        self.update_calls.append((deepcopy(query), deepcopy(update), return_document))
        if not self.updates:
            raise AssertionError("unexpected find_one_and_update call")
        value = self.updates.pop(0)
        return deepcopy(value)


def battle(*, role="creator", live_progress=None):
    doc = {
        "_id": "battle-1",
        "creator_id": 101,
        "opponent_id": 202,
        "status": "in_progress",
        "final_claimed": False,
        "questions": [deepcopy(Q1), deepcopy(Q2)],
    }
    if live_progress is not None:
        doc["live_progress"] = {role: deepcopy(live_progress)}
    return doc


def answer_row(index, question, answer, latency, answered_at):
    correct = answer == question["options"][question["correct"]]
    points = 0 if not correct else 10 + round((7.0 - latency) / 7.0 * 7)
    return {
        "index": index,
        "qid": progress.battle_question_id(question),
        "user_answer": answer,
        "is_correct": correct,
        "points": points,
        "latency_seconds": latency,
        "answered_at": answered_at,
    }


def progress_state(*, started_at, answers=None, sent_at=None):
    answers = list(answers or [])
    return {
        "current_index": len(answers),
        "correct_count": sum(1 for row in answers if row["is_correct"]),
        "points": sum(row["points"] for row in answers),
        "answers": deepcopy(answers),
        "started_at": started_at,
        "question_sent_at": sent_at,
    }


def install(monkeypatch, collection, now):
    monkeypatch.setattr(progress, "_battle_collection", lambda: collection)
    monkeypatch.setattr(progress, "_database", lambda: SimpleNamespace(_now_utc=lambda: now))


def test_initialize_progress_once_and_bind_participant(monkeypatch):
    now = datetime(2026, 8, 11, 12, 0, 0)
    initial = progress_state(started_at=now)
    updated = battle(live_progress=initial)
    collection = FakeCollection(finds=[battle()], updates=[updated])
    install(monkeypatch, collection, now)

    result = progress.ensure_battle_progress("battle-1", 101, "creator")

    assert result["applied"] is True
    assert result["progress"] == initial
    query, update, _ = collection.update_calls[0]
    assert query["creator_id"] == 101
    assert query["live_progress.creator"] == {"$exists": False}
    assert update["$set"]["live_progress.creator"] == initial


def test_repeated_start_returns_existing_progress_without_reset(monkeypatch):
    started = datetime(2026, 8, 11, 12, 0, 0)
    first = answer_row(0, Q1, "Пётр", 1.0, started + timedelta(seconds=1))
    state = progress_state(started_at=started, answers=[first], sent_at=started + timedelta(seconds=2))
    collection = FakeCollection(finds=[battle(live_progress=state)])
    install(monkeypatch, collection, started + timedelta(seconds=3))

    result = progress.ensure_battle_progress("battle-1", 101, "creator")

    assert result["applied"] is False
    assert result["progress"]["current_index"] == 1
    assert collection.update_calls == []


def test_question_timer_is_first_write_and_replay_stable(monkeypatch):
    started = datetime(2026, 8, 11, 12, 0, 0)
    sent = started + timedelta(seconds=2)
    empty = progress_state(started_at=started)
    marked = progress_state(started_at=started, sent_at=sent)
    collection = FakeCollection(
        finds=[battle(live_progress=empty)],
        updates=[battle(live_progress=marked)],
    )
    install(monkeypatch, collection, sent)

    fresh = progress.mark_battle_question_sent("battle-1", 101, "creator", expected_index=0)

    assert fresh["applied"] is True
    assert fresh["sent_at"] == sent
    query, _, _ = collection.update_calls[0]
    assert query["live_progress.creator.current_index"] == 0
    assert query["live_progress.creator.answers"] == []
    assert query["questions.0"] == Q1

    replay_collection = FakeCollection(finds=[battle(live_progress=marked)])
    install(monkeypatch, replay_collection, sent + timedelta(seconds=10))
    replay = progress.mark_battle_question_sent("battle-1", 101, "creator", expected_index=0)

    assert replay["applied"] is False
    assert replay["sent_at"] == sent
    assert replay_collection.update_calls == []


def test_fresh_answer_uses_durable_timer_and_atomic_progress_cas(monkeypatch):
    started = datetime(2026, 8, 11, 12, 0, 0)
    sent = started + timedelta(seconds=2)
    now = sent + timedelta(seconds=1)
    before = progress_state(started_at=started, sent_at=sent)
    row = answer_row(0, Q1, "Пётр", 1.0, now)
    after = progress_state(started_at=started, answers=[row])
    collection = FakeCollection(
        finds=[battle(live_progress=before)],
        updates=[battle(live_progress=after)],
    )
    install(monkeypatch, collection, now)

    result = progress.record_battle_answer_once(
        "battle-1", 101, "creator", expected_index=0, user_answer="Пётр"
    )

    assert result["applied"] is True
    assert result["answer"]["latency_seconds"] == 1.0
    assert result["answer"]["points"] == 16
    query, update, _ = collection.update_calls[0]
    assert query["creator_id"] == 101
    assert query["live_progress.creator.current_index"] == 0
    assert query["live_progress.creator.question_sent_at"] == sent
    assert update["$inc"]["live_progress.creator.current_index"] == 1
    assert update["$inc"]["live_progress.creator.points"] == 16
    assert update["$set"]["live_progress.creator.question_sent_at"] is None


def test_lost_response_replay_keeps_first_speed_bonus(monkeypatch):
    started = datetime(2026, 8, 11, 12, 0, 0)
    answered_at = started + timedelta(seconds=1)
    first = answer_row(0, Q1, "Пётр", 1.0, answered_at)
    state = progress_state(started_at=started, answers=[first], sent_at=answered_at + timedelta(seconds=1))
    collection = FakeCollection(finds=[battle(live_progress=state)])
    install(monkeypatch, collection, started + timedelta(seconds=30))

    result = progress.record_battle_answer_once(
        "battle-1", 101, "creator", expected_index=0, user_answer="Пётр"
    )

    assert result["applied"] is False
    assert result["answer"]["latency_seconds"] == 1.0
    assert result["answer"]["points"] == 16
    assert collection.update_calls == []


def test_conflicting_replay_is_rejected_before_write(monkeypatch):
    started = datetime(2026, 8, 11, 12, 0, 0)
    first = answer_row(0, Q1, "Пётр", 1.0, started + timedelta(seconds=1))
    state = progress_state(started_at=started, answers=[first], sent_at=started + timedelta(seconds=2))
    collection = FakeCollection(finds=[battle(live_progress=state)])
    install(monkeypatch, collection, started + timedelta(seconds=3))

    with pytest.raises(progress.LegacyBattleProgressConflict, match="another battle answer"):
        progress.record_battle_answer_once(
            "battle-1", 101, "creator", expected_index=0, user_answer="Павел"
        )
    assert collection.update_calls == []


def test_completed_result_is_derived_only_from_durable_ledger(monkeypatch):
    started = datetime(2026, 8, 11, 12, 0, 0)
    first = answer_row(0, Q1, "Пётр", 1.0, started + timedelta(seconds=1))
    second = answer_row(1, Q2, "Иоанн", 2.0, started + timedelta(seconds=5))
    state = progress_state(started_at=started, answers=[first, second])
    collection = FakeCollection(finds=[battle(live_progress=state)])
    install(monkeypatch, collection, started + timedelta(seconds=99))

    result = progress.completed_battle_result_inputs("battle-1", 101, "creator")

    assert result["score"] == 1
    assert result["total"] == 2
    assert result["points"] == 16
    assert result["time_seconds"] == 5.0
    assert result["completed_at"] == started + timedelta(seconds=5)


def test_contradictory_counter_fails_closed(monkeypatch):
    started = datetime(2026, 8, 11, 12, 0, 0)
    first = answer_row(0, Q1, "Пётр", 1.0, started + timedelta(seconds=1))
    state = progress_state(started_at=started, answers=[first])
    state["correct_count"] = 0
    collection = FakeCollection(finds=[battle(live_progress=state)])
    install(monkeypatch, collection, started + timedelta(seconds=2))

    with pytest.raises(progress.LegacyBattleProgressInvalid, match="correct count"):
        progress.ensure_battle_progress("battle-1", 101, "creator")


def test_store_outage_is_explicit(monkeypatch):
    collection = FakeCollection(find_error=PyMongoError("mongo unavailable"))
    install(monkeypatch, collection, datetime(2026, 8, 11, 12, 0, 0))

    with pytest.raises(progress.LegacyBattleProgressUnavailable, match="initialization failed"):
        progress.ensure_battle_progress("battle-1", 101, "creator")
