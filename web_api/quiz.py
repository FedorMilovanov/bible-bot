"""Server-authoritative Mini App quiz sessions and scoring."""
from __future__ import annotations

import hashlib
import logging
import random
import time
import uuid
from datetime import datetime

from pymongo.errors import DuplicateKeyError, PyMongoError

from .db_hardening import OPEN_STATUSES, ensure_miniapp_indexes
from .result_store import apply_challenge_result_once, apply_regular_result_once

logger = logging.getLogger(__name__)

MODE_CONFIG = {
    "relaxed": {"time_limit": None, "multiplier": 1.0},
    "timed": {"time_limit": 30, "multiplier": 1.5},
    "speed": {"time_limit": 15, "multiplier": 2.0},
}
TIMEOUT_NETWORK_GRACE_SECONDS = 1.0
COMPLETION_TIME_PROTOCOL_DURABLE = "answer_completed_at_v1"


def _now() -> datetime:
    """UTC timestamp matching the repository's existing naive-UTC Mongo model."""
    return datetime.utcnow()


def question_id(question: dict) -> str:
    explicit = str(question.get("id") or "").strip()
    if explicit:
        return explicit
    material = str(question.get("question", "")) + "\x1f" + "\x1f".join(question.get("options", []))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def prepare_question(question: dict) -> dict:
    options = list(question.get("options") or [])
    correct = int(question.get("correct", -1))
    if not options or correct < 0 or correct >= len(options):
        raise ValueError("question has invalid options/correct index")

    indexed = list(enumerate(options))
    random.shuffle(indexed)
    shuffled_options = [text for _, text in indexed]
    shuffled_correct = next(i for i, (original_idx, _) in enumerate(indexed) if original_idx == correct)
    return {
        "id": question_id(question),
        "question": str(question.get("question", "")),
        "options": shuffled_options,
        "correct": shuffled_correct,
        "explanation": str(question.get("explanation", "")),
        "verse": str(question.get("verse", "")),
        "topic": str(question.get("topic", "")),
    }


def public_question(question: dict) -> dict:
    return {"id": question["id"], "question": question["question"], "options": list(question["options"])}


def stats_level_key(pool_key: str, *, is_challenge: bool, mode: str) -> str:
    if is_challenge:
        return "hardcore20" if mode == "speed" else "random20"
    return pool_key


def miniapp_sessions():
    try:
        import database

        db = getattr(database, "db", None)
        if db is None:
            return None
        ensure_miniapp_indexes()
        return db["miniapp_sessions"]
    except Exception:
        logger.exception("Mini App session collection unavailable")
        return None


def get_miniapp_history(user_id: int, limit: int = 10) -> list[dict]:
    sessions = miniapp_sessions()
    if sessions is None:
        return []
    try:
        cursor = (
            sessions.find(
                {"user_id": str(user_id), "status": "finished"},
                {
                    "stats_level_key": 1,
                    "mode": 1,
                    "correct_count": 1,
                    "question_count": 1,
                    "finished_at_dt": 1,
                },
            )
            .sort("finished_at_dt", -1)
            .limit(max(1, min(int(limit), 50)))
        )
        return [
            {
                "source": "miniapp",
                "level_name": item.get("stats_level_key", "Mini App"),
                "level_key": item.get("stats_level_key", ""),
                "correct_count": int(item.get("correct_count", 0)),
                "total_questions": int(item.get("question_count", 0)),
                "end_time": item.get("finished_at_dt"),
                "mode": item.get("mode", ""),
            }
            for item in cursor
        ]
    except Exception:
        logger.exception("failed to load Mini App history")
        return []


def _current_question_payload(session: dict) -> dict | None:
    questions = session.get("questions") or []
    index = int(session.get("current_index", 0))
    if index < 0 or index >= len(questions):
        return None
    time_limit = session.get("time_limit")
    sent_at = session.get("question_sent_at")
    remaining = None
    if time_limit and sent_at:
        remaining = max(0.0, float(time_limit) - (time.time() - float(sent_at)))
    return {
        "index": index,
        "total": len(questions),
        "time_limit": time_limit,
        "remaining_seconds": remaining,
        "question": public_question(questions[index]),
    }


def _matching_active_start_payload(
    session: dict,
    *,
    pool_key: str,
    mode: str,
    is_challenge: bool,
    count: int,
) -> dict | None:
    """Return a resumable current payload only for the exact requested quiz spec."""
    if (
        session.get("status") != "in_progress"
        or session.get("pool_key") != pool_key
        or session.get("mode") != mode
        or session.get("is_challenge") is not is_challenge
        or session.get("question_count") != count
    ):
        return None
    current = _current_question_payload(session)
    if current is None:
        return None
    return {
        "session_id": str(session["_id"]),
        "pool_key": pool_key,
        "mode": mode,
        "challenge": is_challenge,
        "resumed": True,
        **current,
    }


def start_quiz(user: dict, payload: dict) -> tuple[dict | None, str | None, int]:
    pool_key = str(payload.get("pool_key", "")).strip()
    mode = str(payload.get("mode", "relaxed")).strip()
    if mode not in MODE_CONFIG:
        return None, "invalid quiz mode", 400

    is_challenge = bool(payload.get("challenge"))
    if is_challenge:
        if pool_key != "random_all":
            return None, "challenge requires random_all pool", 400
        if mode not in {"relaxed", "speed"}:
            return None, "invalid challenge mode", 400
        expected_count = 20
    else:
        if pool_key == "random_all":
            return None, "random_all is reserved for Challenge 20", 400
        expected_count = 10

    if "count" in payload:
        try:
            requested_count = int(payload["count"])
        except (TypeError, ValueError):
            return None, "invalid question count", 400
        if requested_count != expected_count:
            return None, f"question count must be {expected_count}", 400
    count = expected_count

    try:
        from questions import get_pool_by_key

        pool = get_pool_by_key(pool_key)
    except KeyError:
        return None, "unknown question pool", 404
    except Exception:
        logger.exception("failed to load question pool")
        return None, "question pool unavailable", 503
    if len(pool) < count:
        return None, f"question pool contains fewer than {count} questions", 409

    sessions = miniapp_sessions()
    if sessions is None:
        return None, "database unavailable", 503

    user_id = str(user["id"])
    try:
        open_session = sessions.find_one(
            {"user_id": user_id, "status": {"$in": list(OPEN_STATUSES)}}
        )
    except PyMongoError:
        logger.exception("failed to resolve open Mini App session")
        return None, "database temporarily unavailable", 503
    except Exception:
        logger.exception("unexpected open Mini App session lookup failure")
        return None, "could not resolve open quiz session", 500

    if open_session:
        if open_session.get("status") == "in_progress":
            resumed = _matching_active_start_payload(
                open_session,
                pool_key=pool_key,
                mode=mode,
                is_challenge=is_challenge,
                count=count,
            )
            if resumed is not None:
                return resumed, None, 200

        open_total = int(
            open_session.get("question_count")
            or len(open_session.get("questions") or [])
        )
        open_index = int(open_session.get("current_index", 0))
        if open_total <= 0 or open_index != open_total:
            if open_session.get("status") == "in_progress":
                return None, "another active quiz is in progress; finish it before starting another", 409
            return None, "unfinished quiz result state is inconsistent", 409

        finalized = _finalize_quiz(open_session, user)
        if finalized is None:
            return None, "previous quiz result finalization is incomplete", 503

    try:
        from database import get_user_stats, init_user_stats

        init_user_stats(
            int(user["id"]),
            user.get("username", ""),
            user.get("first_name", ""),
        )
        if get_user_stats(int(user["id"])) is None:
            return None, "user profile unavailable", 503
    except Exception:
        logger.exception("failed to initialise user profile")
        return None, "user profile unavailable", 503

    selected = random.sample(pool, count)
    try:
        questions = [prepare_question(question) for question in selected]
    except (TypeError, ValueError):
        logger.exception("invalid question data in pool %s", pool_key)
        return None, "question data is invalid", 500

    cfg = dict(MODE_CONFIG[mode])
    if is_challenge and mode == "speed":
        cfg["time_limit"] = 10

    now = _now()
    session_id = str(uuid.uuid4())
    document = {
        "_id": session_id,
        "user_id": user_id,
        "username": user.get("username", ""),
        "first_name": user.get("first_name", ""),
        "status": "in_progress",
        "pool_key": pool_key,
        "stats_level_key": stats_level_key(pool_key, is_challenge=is_challenge, mode=mode),
        "mode": mode,
        "is_challenge": is_challenge,
        "questions": questions,
        "question_count": len(questions),
        "current_index": 0,
        "correct_count": 0,
        "current_streak": 0,
        "max_streak": 0,
        "answered": [],
        "time_limit": cfg["time_limit"],
        "score_multiplier": cfg["multiplier"],
        "started_at_dt": now,
        "completed_at_dt": None,
        "completion_time_protocol": COMPLETION_TIME_PROTOCOL_DURABLE,
        "updated_at_dt": now,
        "question_sent_at": time.time(),
        "leaderboard_recorded": False,
    }
    try:
        sessions.insert_one(document)
    except DuplicateKeyError:
        # A concurrent open session won after the pre-read. Never abandon it and
        # never invent a second attempt; the caller can repeat the start request.
        logger.info("open Mini App session already exists for user %s", user["id"])
        return None, "another unfinished quiz already exists; retry start", 409
    except PyMongoError:
        logger.exception("database unavailable while creating Mini App quiz session")
        return None, "database temporarily unavailable", 503
    except Exception:
        logger.exception("unexpected failure while creating Mini App quiz session")
        return None, "could not create quiz session", 500

    current = _current_question_payload(document)
    return {
        "session_id": session_id,
        "pool_key": pool_key,
        "mode": mode,
        "challenge": is_challenge,
        "resumed": False,
        **current,
    }, None, 200


def get_current_question(user: dict, payload: dict) -> tuple[dict | None, str | None, int]:
    session_id = str(payload.get("session_id", "")).strip()
    if not session_id:
        return None, "session_id is required", 400

    sessions = miniapp_sessions()
    if sessions is None:
        return None, "database unavailable", 503

    user_id = str(user["id"])
    session = sessions.find_one({"_id": session_id, "user_id": user_id})
    if not session or session.get("status") != "in_progress":
        return None, "quiz session not found or already finished", 409

    questions = session.get("questions") or []
    index = int(session.get("current_index", 0))
    if index < 0 or index >= len(questions):
        return None, "quiz session is inconsistent", 409

    if session.get("question_sent_at") is None:
        now_ts = time.time()
        sessions.update_one(
            {
                "_id": session_id,
                "user_id": user_id,
                "status": "in_progress",
                "current_index": index,
                "question_sent_at": None,
            },
            {"$set": {"question_sent_at": now_ts, "updated_at_dt": _now()}},
        )
        session = sessions.find_one({"_id": session_id, "user_id": user_id}) or session

    current = _current_question_payload(session)
    if current is None:
        return None, "quiz session is inconsistent", 409
    return {"session_id": session_id, **current}, None, 200


def _stored_result(session: dict) -> dict:
    return {
        "points": int(session.get("awarded_points", 0)),
        "daily_bonus": int(session.get("daily_bonus", 0)),
        "new_achievements": list(session.get("new_achievements") or []),
    }


def _completion_elapsed_seconds(session: dict) -> float:
    """Return stable quiz duration, using durable completion time for new sessions."""
    started_at = session.get("started_at_dt")
    if not isinstance(started_at, datetime):
        raise ValueError("Mini App session start time is invalid")

    protocol = session.get("completion_time_protocol")
    completed_at = session.get("completed_at_dt")
    if protocol == COMPLETION_TIME_PROTOCOL_DURABLE:
        if not isinstance(completed_at, datetime):
            raise ValueError("durable Mini App completion time is missing")
    elif protocol is None:
        # Backward compatibility for already-running sessions created before the
        # completion-time protocol existed. Prefer a durable timestamp if a
        # transitional final answer wrote one; otherwise retain legacy behavior.
        if completed_at is None:
            completed_at = _now()
        elif not isinstance(completed_at, datetime):
            raise ValueError("legacy Mini App completion time is invalid")
    else:
        raise ValueError("unsupported Mini App completion time protocol")

    elapsed = (completed_at - started_at).total_seconds()
    if elapsed < 0:
        raise ValueError("Mini App completion time predates session start")
    return elapsed


def _claim_or_resume_finalization(session: dict, sessions) -> dict | None:
    """Claim a fresh finalization or resume one interrupted after the claim."""
    if session.get("status") == "finished":
        return session

    if session.get("leaderboard_recorded") and session.get("status") in {"finalizing", "score_error"}:
        try:
            sessions.update_one(
                {"_id": session["_id"], "leaderboard_recorded": True},
                {"$set": {"status": "finalizing", "updated_at_dt": _now()}},
            )
        except Exception:
            logger.exception("failed to resume Mini App finalization")
            return None
        return sessions.find_one({"_id": session["_id"]}) or session

    try:
        from pymongo import ReturnDocument

        claimed = sessions.find_one_and_update(
            {
                "_id": session["_id"],
                "status": "in_progress",
                "leaderboard_recorded": False,
            },
            {
                "$set": {
                    "leaderboard_recorded": True,
                    "status": "finalizing",
                    "updated_at_dt": _now(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
    except Exception:
        logger.exception("failed to claim Mini App finalization")
        return None

    if claimed:
        return claimed

    latest = sessions.find_one({"_id": session["_id"]})
    if latest and latest.get("status") == "finished":
        return latest
    if latest and latest.get("leaderboard_recorded") and latest.get("status") in {"finalizing", "score_error"}:
        return latest
    return None


def _finalize_quiz(session: dict, user: dict) -> dict | None:
    sessions = miniapp_sessions()
    if sessions is None:
        return None

    claimed = _claim_or_resume_finalization(session, sessions)
    if not claimed:
        return None
    if claimed.get("status") == "finished":
        return _stored_result(claimed)

    total = int(claimed.get("question_count") or len(claimed.get("questions", [])))
    score = int(claimed.get("correct_count", 0))
    uid = int(user["id"])
    username = user.get("username", "")
    first_name = user.get("first_name", "")
    result_id = str(claimed["_id"])

    try:
        elapsed = _completion_elapsed_seconds(claimed)
        if claimed.get("is_challenge"):
            challenge_mode = claimed["stats_level_key"]
            receipt = apply_challenge_result_once(
                user_id=uid,
                result_id=result_id,
                username=username,
                first_name=first_name,
                mode=challenge_mode,
                score=score,
                total=total,
                time_seconds=elapsed,
            )
            if receipt is None:
                raise RuntimeError("challenge result receipt was not persisted")
        else:
            receipt = apply_regular_result_once(
                user_id=uid,
                result_id=result_id,
                username=username,
                first_name=first_name,
                level_key=claimed["stats_level_key"],
                score=score,
                total=total,
                time_seconds=elapsed,
                score_multiplier=float(claimed.get("score_multiplier", 1.0)),
                is_perfect=score == total,
                max_streak=int(claimed.get("max_streak", 0)),
            )
            if receipt is None:
                raise RuntimeError("regular result receipt was not persisted")
    except Exception as exc:
        logger.exception("failed to persist Mini App result %s", result_id)
        try:
            sessions.update_one(
                {"_id": result_id},
                {
                    "$set": {
                        "status": "score_error",
                        "score_error": type(exc).__name__,
                        "updated_at_dt": _now(),
                    }
                },
            )
        except Exception:
            logger.exception("failed to mark Mini App score error")
        return None

    result = {
        "points": int(receipt.get("points", 0)),
        "daily_bonus": int(receipt.get("daily_bonus", 0)),
        "new_achievements": list(receipt.get("new_achievements") or []),
    }
    finished_at = _now()
    try:
        sessions.update_one(
            {"_id": result_id, "leaderboard_recorded": True},
            {
                "$set": {
                    "status": "finished",
                    "finished_at_dt": finished_at,
                    "updated_at_dt": finished_at,
                    "awarded_points": result["points"],
                    "daily_bonus": result["daily_bonus"],
                    "new_achievements": result["new_achievements"],
                }
            },
        )
        stored = sessions.find_one({"_id": result_id})
    except Exception:
        logger.exception("failed to mark Mini App result finished")
        return None

    if stored and stored.get("status") == "finished":
        return _stored_result(stored)
    return None


def _replay_answer_response(session: dict, requested_question_id: str, user: dict) -> dict | None:
    answered = next((item for item in session.get("answered", []) if item.get("id") == requested_question_id), None)
    if not answered:
        return None

    questions = session.get("questions") or []
    question = next((item for item in questions if item.get("id") == requested_question_id), None)
    if not question:
        return None

    total = int(session.get("question_count") or len(questions))
    completed = int(session.get("current_index", 0)) >= total
    result = _stored_result(session) if session.get("status") == "finished" else {
        "points": 0,
        "daily_bonus": 0,
        "new_achievements": [],
    }
    if completed and session.get("status") in {"in_progress", "finalizing", "score_error"}:
        finalized = _finalize_quiz(session, user)
        if finalized is None:
            return None
        result = finalized
        session = miniapp_sessions().find_one({"_id": session["_id"]}) or session

    return {
        "ok": bool(answered.get("ok")),
        "timed_out": bool(answered.get("timed_out")),
        "correct_index": int(answered.get("correct", question["correct"])),
        "explanation": question.get("explanation", ""),
        "verse": question.get("verse", ""),
        "topic": question.get("topic", ""),
        "finished": completed,
        "score": int(session.get("correct_count", 0)),
        "total": total,
        "max_streak": int(session.get("max_streak", 0)),
        **result,
    }


def answer_quiz(user: dict, payload: dict) -> tuple[dict | None, str | None, int]:
    session_id = str(payload.get("session_id", "")).strip()
    requested_question_id = str(payload.get("question_id", "")).strip()
    try:
        chosen = int(payload.get("chosen", -1))
    except (TypeError, ValueError):
        return None, "invalid answer", 400
    if not session_id:
        return None, "session_id is required", 400
    if not requested_question_id:
        return None, "question_id is required", 400

    sessions = miniapp_sessions()
    if sessions is None:
        return None, "database unavailable", 503

    user_id = str(user["id"])
    session = sessions.find_one({"_id": session_id, "user_id": user_id})
    if not session:
        return None, "quiz session not found", 409

    replay = _replay_answer_response(session, requested_question_id, user)
    if replay is not None:
        return replay, None, 200

    session = sessions.find_one({"_id": session_id, "user_id": user_id}) or session
    if session.get("status") in {"finalizing", "score_error"}:
        return None, "result finalization is incomplete; retry the last answer", 503
    if session.get("status") != "in_progress":
        return None, "quiz session is not active", 409

    index = int(session.get("current_index", 0))
    questions = session.get("questions") or []
    if index < 0 or index >= len(questions):
        return None, "quiz session is inconsistent", 409

    question = questions[index]
    if requested_question_id != question.get("id"):
        return None, "question already processed or out of order", 409
    option_count = len(question.get("options") or [])
    if chosen < -1 or chosen >= option_count:
        return None, "answer index out of range", 400

    now_ts = time.time()
    sent_at = session.get("question_sent_at")
    time_limit = session.get("time_limit")
    if time_limit and not sent_at:
        return None, "question has not been presented", 409
    elapsed_question = max(0.0, now_ts - float(sent_at or now_ts))
    timed_out = bool(
        time_limit
        and (chosen == -1 or elapsed_question > float(time_limit) + TIMEOUT_NETWORK_GRACE_SECONDS)
    )
    correct_index = int(question["correct"])
    ok = not timed_out and chosen == correct_index
    current_streak = int(session.get("current_streak", 0)) + 1 if ok else 0
    max_streak = max(int(session.get("max_streak", 0)), current_streak)
    total = int(session.get("question_count") or len(questions))
    answer_time = _now()
    set_fields = {
        "current_streak": current_streak,
        "max_streak": max_streak,
        "updated_at_dt": answer_time,
        "question_sent_at": None,
    }
    if index + 1 >= total:
        set_fields["completed_at_dt"] = answer_time
        set_fields["completion_time_protocol"] = COMPLETION_TIME_PROTOCOL_DURABLE

    try:
        from pymongo import ReturnDocument

        updated = sessions.find_one_and_update(
            {
                "_id": session_id,
                "user_id": user_id,
                "status": "in_progress",
                "current_index": index,
            },
            {
                "$inc": {"current_index": 1, "correct_count": 1 if ok else 0},
                "$set": set_fields,
                "$push": {
                    "answered": {
                        "id": question["id"],
                        "chosen": chosen,
                        "correct": correct_index,
                        "ok": ok,
                        "timed_out": timed_out,
                        "elapsed_seconds": round(elapsed_question, 3),
                    }
                },
            },
            return_document=ReturnDocument.AFTER,
        )
    except Exception:
        logger.exception("failed to advance Mini App session")
        return None, "could not save answer", 503

    if not updated:
        latest = sessions.find_one({"_id": session_id, "user_id": user_id})
        replay = _replay_answer_response(latest or {}, requested_question_id, user)
        if replay is not None:
            return replay, None, 200
        if latest and latest.get("status") in {"finalizing", "score_error"}:
            return None, "result finalization is incomplete; retry the last answer", 503
        return None, "answer could not be committed", 409

    try:
        from database import record_question_stat

        record_question_stat(question["id"], session["pool_key"], ok, elapsed_question)
    except Exception:
        logger.exception("failed to record question stat")

    total = int(updated.get("question_count") or len(questions))
    finished = int(updated.get("current_index", 0)) >= total
    result = {"points": 0, "daily_bonus": 0, "new_achievements": []}
    if finished:
        finalized = _finalize_quiz(updated, user)
        if finalized is None:
            return None, "result persistence failed; retry the last answer", 503
        result = finalized

    return {
        "ok": ok,
        "timed_out": timed_out,
        "correct_index": correct_index,
        "explanation": question.get("explanation", ""),
        "verse": question.get("verse", ""),
        "topic": question.get("topic", ""),
        "finished": finished,
        "score": int(updated.get("correct_count", 0)),
        "total": total,
        "max_streak": max_streak,
        **result,
    }, None, 200
