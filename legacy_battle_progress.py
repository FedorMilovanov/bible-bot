"""Crash-safe, Telegram-independent per-question progress for legacy PvP battles."""
from __future__ import annotations

import hashlib
import math
from datetime import datetime

from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

from legacy_battle_protocols import BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE


class LegacyBattleProgressUnavailable(RuntimeError):
    """Raised when durable PvP progress cannot reach MongoDB."""


class LegacyBattleProgressConflict(RuntimeError):
    """Raised when a PvP transition is stale, unauthorized, or already conflicts."""


class LegacyBattleProgressInvalid(RuntimeError):
    """Raised when persisted PvP progress is structurally contradictory."""


def _database():
    import database

    return database


def _battle_collection():
    collection = getattr(_database(), "battles_collection", None)
    if collection is None:
        raise LegacyBattleProgressUnavailable("battle collection is unavailable")
    return collection


def _required_battle_id(value) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("battle_id is required")
    return value.strip()


def _required_user_id(value) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("user_id must be a positive integer")
    return value


def _required_role(value) -> str:
    if value not in {"creator", "opponent"}:
        raise ValueError("battle role must be creator or opponent")
    return value


def _required_index(value) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("question_index must be a non-negative integer")
    return value


def _participant_field(role: str) -> str:
    return f"{role}_id"


def _progress_path(role: str) -> str:
    return f"live_progress.{role}"


def battle_question_id(question: dict) -> str:
    """Mirror the stable quiz question identity over text + ordered options."""
    if not isinstance(question, dict):
        raise ValueError("battle question must be a dict")
    text = question.get("question")
    options = question.get("options")
    if not isinstance(text, str) or not text:
        raise ValueError("battle question text is required")
    if (
        not isinstance(options, list)
        or len(options) < 2
        or any(not isinstance(option, str) or not option for option in options)
        or len(set(options)) != len(options)
    ):
        raise ValueError("battle question options must be unique non-empty strings")
    raw_correct = question.get("correct")
    if (
        isinstance(raw_correct, bool)
        or not isinstance(raw_correct, int)
        or raw_correct < 0
        or raw_correct >= len(options)
    ):
        raise ValueError("battle correct option index is invalid")
    return hashlib.sha256((text + "".join(options)).encode()).hexdigest()[:12]


def _validated_questions(battle: dict) -> list[dict]:
    questions = battle.get("questions")
    if not isinstance(questions, list) or not questions:
        raise LegacyBattleProgressInvalid("battle questions are missing")
    for question in questions:
        try:
            battle_question_id(question)
        except ValueError as exc:
            raise LegacyBattleProgressInvalid("battle question snapshot is invalid") from exc
    return questions


def _expected_points(question: dict, user_answer: str, latency_seconds: float) -> tuple[bool, int]:
    options = question["options"]
    correct_text = options[question["correct"]]
    is_correct = user_answer == correct_text
    if not is_correct:
        return False, 0
    speed_bonus = round((7.0 - latency_seconds) / 7.0 * 7)
    return True, 10 + speed_bonus


def _validated_latency(value) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise LegacyBattleProgressInvalid("battle answer latency is invalid")
    latency = float(value)
    if not math.isfinite(latency) or latency < 0 or latency > 7.0:
        raise LegacyBattleProgressInvalid("battle answer latency is invalid")
    return latency


def _validate_answer_row(row: dict, question: dict, expected_index: int) -> tuple[bool, int, datetime]:
    if not isinstance(row, dict):
        raise LegacyBattleProgressInvalid("battle answer row is invalid")
    if row.get("index") != expected_index:
        raise LegacyBattleProgressInvalid("battle answer index is invalid")
    qid = battle_question_id(question)
    if row.get("qid") != qid:
        raise LegacyBattleProgressInvalid("battle answer question id is invalid")
    user_answer = row.get("user_answer")
    if not isinstance(user_answer, str) or user_answer not in question["options"]:
        raise LegacyBattleProgressInvalid("battle answer option is invalid")
    latency = _validated_latency(row.get("latency_seconds"))
    expected_correct, expected_points = _expected_points(question, user_answer, latency)
    if row.get("is_correct") is not expected_correct:
        raise LegacyBattleProgressInvalid("battle answer correctness is invalid")
    points = row.get("points")
    if isinstance(points, bool) or not isinstance(points, int) or points != expected_points:
        raise LegacyBattleProgressInvalid("battle answer points are invalid")
    answered_at = row.get("answered_at")
    if not isinstance(answered_at, datetime):
        raise LegacyBattleProgressInvalid("battle answer timestamp is invalid")
    return expected_correct, expected_points, answered_at


def _validated_progress(battle: dict, role: str) -> dict:
    questions = _validated_questions(battle)
    live_progress = battle.get("live_progress")
    if not isinstance(live_progress, dict):
        raise LegacyBattleProgressInvalid("battle live progress is missing")
    progress = live_progress.get(role)
    if not isinstance(progress, dict):
        raise LegacyBattleProgressInvalid("participant live progress is missing")

    current_index = progress.get("current_index")
    correct_count = progress.get("correct_count")
    points = progress.get("points")
    answers = progress.get("answers")
    started_at = progress.get("started_at")
    question_sent_at = progress.get("question_sent_at")

    if (
        isinstance(current_index, bool)
        or not isinstance(current_index, int)
        or current_index < 0
        or current_index > len(questions)
    ):
        raise LegacyBattleProgressInvalid("battle progress index is invalid")
    if isinstance(correct_count, bool) or not isinstance(correct_count, int) or correct_count < 0:
        raise LegacyBattleProgressInvalid("battle correct count is invalid")
    if isinstance(points, bool) or not isinstance(points, int) or points < 0:
        raise LegacyBattleProgressInvalid("battle points total is invalid")
    if not isinstance(answers, list) or len(answers) != current_index:
        raise LegacyBattleProgressInvalid("battle answer ledger length is invalid")
    if not isinstance(started_at, datetime):
        raise LegacyBattleProgressInvalid("battle progress start timestamp is invalid")
    if question_sent_at is not None and not isinstance(question_sent_at, datetime):
        raise LegacyBattleProgressInvalid("battle question timer marker is invalid")
    if current_index == len(questions) and question_sent_at is not None:
        raise LegacyBattleProgressInvalid("completed battle progress still has an active timer")

    counted_correct = 0
    counted_points = 0
    previous_at = started_at
    for index, row in enumerate(answers):
        row_correct, row_points, answered_at = _validate_answer_row(row, questions[index], index)
        if answered_at < previous_at:
            raise LegacyBattleProgressInvalid("battle answer timestamps are not monotonic")
        previous_at = answered_at
        counted_correct += 1 if row_correct else 0
        counted_points += row_points
    if correct_count != counted_correct:
        raise LegacyBattleProgressInvalid("battle correct count does not match the ledger")
    if points != counted_points:
        raise LegacyBattleProgressInvalid("battle points do not match the ledger")
    if question_sent_at is not None and question_sent_at < previous_at:
        raise LegacyBattleProgressInvalid("battle question timer predates durable progress")
    return progress


def _owned_battle_filter(battle_id: str, user_id: int, role: str) -> dict:
    return {
        "_id": battle_id,
        _participant_field(role): user_id,
        "question_progress_protocol": BATTLE_QUESTION_PROGRESS_PROTOCOL_DURABLE,
        "status": {"$in": ["waiting", "in_progress"]},
        "final_claimed": {"$ne": True},
    }


def _load_owned_battle(collection, battle_id: str, user_id: int, role: str) -> dict:
    battle = collection.find_one(_owned_battle_filter(battle_id, user_id, role))
    if battle is None:
        raise LegacyBattleProgressConflict("battle is missing, finalized, or not owned")
    return battle


def ensure_battle_progress(battle_id: str, user_id: int, role: str) -> dict:
    """Initialize one participant progress once; later starts return the durable winner."""
    battle_id = _required_battle_id(battle_id)
    user_id = _required_user_id(user_id)
    role = _required_role(role)
    collection = _battle_collection()
    database = _database()
    owner_filter = _owned_battle_filter(battle_id, user_id, role)
    path = _progress_path(role)
    try:
        battle = _load_owned_battle(collection, battle_id, user_id, role)
        _validated_questions(battle)
        live_progress = battle.get("live_progress")
        if isinstance(live_progress, dict) and role in live_progress:
            _validated_progress(battle, role)
            return {"applied": False, "battle": battle, "progress": live_progress[role]}
        if live_progress is not None and not isinstance(live_progress, dict):
            raise LegacyBattleProgressInvalid("battle live progress container is invalid")

        now = database._now_utc()
        initial = {
            "current_index": 0,
            "correct_count": 0,
            "points": 0,
            "answers": [],
            "started_at": now,
            "question_sent_at": None,
        }
        updated = collection.find_one_and_update(
            {**owner_filter, path: {"$exists": False}},
            {"$set": {path: initial}},
            return_document=ReturnDocument.AFTER,
        )
        if updated is not None:
            _validated_progress(updated, role)
            return {"applied": True, "battle": updated, "progress": updated["live_progress"][role]}

        existing = _load_owned_battle(collection, battle_id, user_id, role)
        _validated_progress(existing, role)
        return {"applied": False, "battle": existing, "progress": existing["live_progress"][role]}
    except (LegacyBattleProgressConflict, LegacyBattleProgressInvalid):
        raise
    except PyMongoError as exc:
        raise LegacyBattleProgressUnavailable("battle progress initialization failed") from exc


def mark_battle_question_sent(
    battle_id: str,
    user_id: int,
    role: str,
    *,
    expected_index: int,
) -> dict:
    """Persist the first timer marker for one exact participant question."""
    battle_id = _required_battle_id(battle_id)
    user_id = _required_user_id(user_id)
    role = _required_role(role)
    expected_index = _required_index(expected_index)
    collection = _battle_collection()
    database = _database()
    owner_filter = _owned_battle_filter(battle_id, user_id, role)
    path = _progress_path(role)
    try:
        battle = _load_owned_battle(collection, battle_id, user_id, role)
        questions = _validated_questions(battle)
        progress = _validated_progress(battle, role)
        if expected_index >= len(questions) or progress["current_index"] != expected_index:
            raise LegacyBattleProgressConflict("battle timer targets another question")
        existing_sent_at = progress.get("question_sent_at")
        if existing_sent_at is not None:
            return {"applied": False, "battle": battle, "sent_at": existing_sent_at}

        now = database._now_utc()
        updated = collection.find_one_and_update(
            {
                **owner_filter,
                f"{path}.current_index": expected_index,
                f"{path}.answers": progress["answers"],
                f"{path}.question_sent_at": None,
                f"questions.{expected_index}": questions[expected_index],
            },
            {"$set": {f"{path}.question_sent_at": now}},
            return_document=ReturnDocument.AFTER,
        )
        if updated is not None:
            validated = _validated_progress(updated, role)
            return {"applied": True, "battle": updated, "sent_at": validated["question_sent_at"]}

        existing = _load_owned_battle(collection, battle_id, user_id, role)
        validated = _validated_progress(existing, role)
        if validated["current_index"] != expected_index or validated.get("question_sent_at") is None:
            raise LegacyBattleProgressConflict("battle timer transition lost a state race")
        return {"applied": False, "battle": existing, "sent_at": validated["question_sent_at"]}
    except (LegacyBattleProgressConflict, LegacyBattleProgressInvalid):
        raise
    except PyMongoError as exc:
        raise LegacyBattleProgressUnavailable("battle timer write failed") from exc


def _semantic_replay(
    progress: dict,
    question: dict,
    expected_index: int,
    user_answer: str,
) -> dict:
    if progress["current_index"] != expected_index + 1:
        raise LegacyBattleProgressConflict("battle answer is not the immediate durable replay")
    stored = progress["answers"][expected_index]
    expected_correct = user_answer == question["options"][question["correct"]]
    if (
        stored.get("qid") != battle_question_id(question)
        or stored.get("user_answer") != user_answer
        or stored.get("is_correct") is not expected_correct
    ):
        raise LegacyBattleProgressConflict("another battle answer occupies this question")
    return stored


def record_battle_answer_once(
    battle_id: str,
    user_id: int,
    role: str,
    *,
    expected_index: int,
    user_answer: str,
) -> dict:
    """Persist one semantic PvP answer once and return the authoritative snapshot."""
    battle_id = _required_battle_id(battle_id)
    user_id = _required_user_id(user_id)
    role = _required_role(role)
    expected_index = _required_index(expected_index)
    if not isinstance(user_answer, str) or not user_answer:
        raise ValueError("user_answer is required")

    collection = _battle_collection()
    database = _database()
    owner_filter = _owned_battle_filter(battle_id, user_id, role)
    path = _progress_path(role)
    try:
        battle = _load_owned_battle(collection, battle_id, user_id, role)
        questions = _validated_questions(battle)
        if expected_index >= len(questions):
            raise LegacyBattleProgressConflict("battle answer targets another question")
        progress = _validated_progress(battle, role)
        question = questions[expected_index]
        if user_answer not in question["options"]:
            raise LegacyBattleProgressConflict("battle answer option is stale")
        if progress["current_index"] == expected_index + 1:
            stored = _semantic_replay(progress, question, expected_index, user_answer)
            return {
                "applied": False,
                "battle": battle,
                "progress": progress,
                "answer": stored,
            }
        if progress["current_index"] != expected_index:
            raise LegacyBattleProgressConflict("battle answer targets another question")

        sent_at = progress.get("question_sent_at")
        if not isinstance(sent_at, datetime):
            raise LegacyBattleProgressInvalid("battle answer has no durable timer marker")
        now = database._now_utc()
        elapsed = (now - sent_at).total_seconds()
        if not math.isfinite(elapsed) or elapsed < 0:
            raise LegacyBattleProgressInvalid("battle timer chronology is invalid")
        latency = min(elapsed, 7.0)
        is_correct, points = _expected_points(question, user_answer, latency)
        answer = {
            "index": expected_index,
            "qid": battle_question_id(question),
            "user_answer": user_answer,
            "is_correct": is_correct,
            "points": points,
            "latency_seconds": latency,
            "answered_at": now,
        }
        updated = collection.find_one_and_update(
            {
                **owner_filter,
                f"{path}.current_index": expected_index,
                f"{path}.answers": progress["answers"],
                f"{path}.question_sent_at": sent_at,
                f"questions.{expected_index}": question,
            },
            {
                "$inc": {
                    f"{path}.current_index": 1,
                    f"{path}.correct_count": 1 if is_correct else 0,
                    f"{path}.points": points,
                },
                "$push": {f"{path}.answers": answer},
                "$set": {f"{path}.question_sent_at": None},
            },
            return_document=ReturnDocument.AFTER,
        )
        if updated is not None:
            validated = _validated_progress(updated, role)
            return {
                "applied": True,
                "battle": updated,
                "progress": validated,
                "answer": answer,
            }

        existing = _load_owned_battle(collection, battle_id, user_id, role)
        validated = _validated_progress(existing, role)
        stored = _semantic_replay(validated, question, expected_index, user_answer)
        return {
            "applied": False,
            "battle": existing,
            "progress": validated,
            "answer": stored,
        }
    except (LegacyBattleProgressConflict, LegacyBattleProgressInvalid):
        raise
    except PyMongoError as exc:
        raise LegacyBattleProgressUnavailable("battle answer write failed") from exc


def completed_battle_result_inputs(battle_id: str, user_id: int, role: str) -> dict:
    """Derive final PvP scoring inputs exclusively from the durable answer ledger."""
    battle_id = _required_battle_id(battle_id)
    user_id = _required_user_id(user_id)
    role = _required_role(role)
    collection = _battle_collection()
    try:
        battle = _load_owned_battle(collection, battle_id, user_id, role)
        questions = _validated_questions(battle)
        progress = _validated_progress(battle, role)
        if progress["current_index"] != len(questions):
            raise LegacyBattleProgressConflict("battle progress is not complete")
        if not progress["answers"]:
            raise LegacyBattleProgressInvalid("completed battle has no answer evidence")
        completed_at = progress["answers"][-1]["answered_at"]
        duration = (completed_at - progress["started_at"]).total_seconds()
        if not math.isfinite(duration) or duration < 0:
            raise LegacyBattleProgressInvalid("battle completion chronology is invalid")
        return {
            "battle": battle,
            "role": role,
            "score": progress["correct_count"],
            "total": len(questions),
            "time_seconds": duration,
            "points": progress["points"],
            "completed_at": completed_at,
        }
    except (LegacyBattleProgressConflict, LegacyBattleProgressInvalid):
        raise
    except PyMongoError as exc:
        raise LegacyBattleProgressUnavailable("battle completion lookup failed") from exc
