import os
import time
import logging
import functools
import uuid
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient, ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)

MONGO_URL = os.environ.get("MONGO_URL")
DB_NAME = os.environ.get("MONGO_DB_NAME", "bible_bot")

cluster = None
collection = None
quiz_sessions_collection = None
battles_collection = None
reports_collection = None
weekly_lb_collection = None

if MONGO_URL:
    try:
        cluster = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
        db = cluster[DB_NAME]
        collection = db["users"]
        quiz_sessions_collection = db["quiz_sessions"]
        battles_collection = db["battles"]
        reports_collection = db["reports"]
        weekly_lb_collection = db["weekly_leaderboard"]
    except Exception as e:
        logger.error("Mongo init error: %s", e)
else:
    logger.warning("MONGO_URL is not set; database features are disabled")


ALL_LEVEL_KEYS = {
    "easy", "easy_p1", "easy_p2", "easy_p3",
    "medium", "medium_p1", "medium_p2", "medium_p3", "medium_p4", "medium_p5",
    "hard", "hard_p1", "hard_p2",
    "nero", "geography",
    "linguistics_ch1", "linguistics_ch1_2", "linguistics_ch1_3",
    "intro1", "intro2", "intro3",
}

POINTS_PER_QUESTION = {
    "easy": 1, "easy_p1": 1, "easy_p2": 1, "easy_p3": 1,
    "medium": 2, "medium_p1": 2, "medium_p2": 2, "medium_p3": 2,
    "medium_p4": 2, "medium_p5": 2,
    "hard": 3, "hard_p1": 3, "hard_p2": 3,
    "nero": 2, "geography": 2,
    "linguistics_ch1": 2, "linguistics_ch1_2": 2, "linguistics_ch1_3": 2,
    "intro1": 2, "intro2": 2, "intro3": 2,
}


def _uid(user_id):
    return str(user_id)


def _now_utc():
    return datetime.now(timezone.utc)


def _safe_level_key(level_key: str) -> str:
    if level_key in ALL_LEVEL_KEYS:
        return level_key
    logger.warning("Unknown level_key=%r; defaulting to 'easy'", level_key)
    return "easy"


def _normalize_score_total(score, total):
    try:
        score = int(score)
        total = int(total)
    except (TypeError, ValueError):
        return 0, 0
    total = max(0, total)
    score = max(0, min(score, total))
    return score, total


def mongo_retry(max_retries=2, delay=0.3):
    """Простой retry-декоратор для операций MongoDB."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < max_retries:
                        time.sleep(delay * (attempt + 1))
            logger.error("%s failed after %d attempts: %s",
                         func.__name__, max_retries + 1, last_error)
            return None
        return wrapper
    return decorator


def check_db_connection() -> bool:
    """Проверяет доступность MongoDB."""
    if collection is None:
        return False
    try:
        cluster.admin.command("ping")
        return True
    except Exception:
        return False


# ═══════════════════════════════════════════════
# TTL INDEXES
# ═══════════════════════════════════════════════

def _ensure_indexes():
    """Создаёт все необходимые индексы при старте."""

    if quiz_sessions_collection is not None:
        try:
            quiz_sessions_collection.create_index(
                [("updated_at_dt", ASCENDING)],
                expireAfterSeconds=21600,
                name="ttl_updated_at",
                background=True,
            )
            quiz_sessions_collection.create_index(
                [("user_id", ASCENDING), ("status", ASCENDING)],
                name="idx_user_status",
                background=True,
            )
        except Exception as e:
            logger.warning("quiz_sessions index: %s", e)

    if battles_collection is not None:
        try:
            battles_collection.create_index(
                [("created_at_dt", ASCENDING)],
                expireAfterSeconds=2592000,
                name="ttl_battles_created_at",
                background=True,
            )
            battles_collection.create_index(
                [("status", ASCENDING), ("created_at_dt", DESCENDING)],
                background=True,
            )
        except Exception as e:
            logger.warning("battles index: %s", e)

    if collection is not None:
        try:
            collection.create_index(
                [("last_activity", DESCENDING)],
                background=True,
            )
            collection.create_index(
                [("total_points", DESCENDING)],
                name="idx_total_points",
                background=True,
            )
            collection.create_index(
                [("created_at", ASCENDING)],
                name="idx_created_at",
                background=True,
            )
        except Exception as e:
            logger.warning("leaderboard index: %s", e)

    if reports_collection is not None:
        try:
            reports_collection.create_index(
                [("created_at_dt", ASCENDING)],
                expireAfterSeconds=7776000,
                name="ttl_reports_created_at",
                background=True,
            )
        except Exception as e:
            logger.warning("reports index: %s", e)

    if weekly_lb_collection is not None:
        try:
            weekly_lb_collection.create_index(
                [("updated_at_dt", ASCENDING)],
                expireAfterSeconds=5184000,
                name="ttl_weekly_lb_updated_at",
                background=True,
            )
            weekly_lb_collection.create_index(
                [("week_id", ASCENDING), ("mode", ASCENDING),
                 ("best_score", DESCENDING), ("best_time", ASCENDING)],
                name="idx_weekly_lb_lookup",
                background=True,
            )
        except Exception as e:
            logger.warning("weekly_lb index: %s", e)


_ensure_indexes()


# ═══════════════════════════════════════════════
# QUIZ SESSIONS — CRUD
# ═══════════════════════════════════════════════

def create_quiz_session(user_id: int, mode: str, question_ids: list,
                        questions_data: list,
                        level_key: str = None, level_name: str = None,
                        time_limit: int = None,
                        chat_id: int = None) -> str | None:
    """Persist a recoverable quiz session, or return None when persistence failed.

    Legacy handlers already treat a falsy session id as an in-memory-only quiz:
    answering can continue, but recovery/advance writes are skipped instead of
    pretending that a UUID exists in MongoDB.
    """
    if quiz_sessions_collection is None:
        return None
    session_id = str(uuid.uuid4())
    now = _now_utc()
    doc = {
        "_id": session_id,
        "user_id": _uid(user_id),
        "session_id": session_id,
        "status": "in_progress",
        "mode": mode,
        "level_key": level_key,
        "level_name": level_name,
        "question_ids": question_ids,
        "questions_data": questions_data,
        "current_index": 0,
        "correct_count": 0,
        "answered_questions": [],
        "time_limit": time_limit,
        "question_sent_at": None,
        "chat_id": chat_id,
        "start_time": time.time(),
        "started_at": now.isoformat(),
        "created_at": now,
        "updated_at": now.isoformat(),
        "updated_at_dt": now,
    }
    try:
        quiz_sessions_collection.insert_one(doc)
    except Exception as e:
        logger.error("create_quiz_session error: %s", e)
        return None
    return session_id


def get_active_quiz_session(user_id: int):
    if quiz_sessions_collection is None:
        return None
    try:
        return quiz_sessions_collection.find_one(
            {"user_id": _uid(user_id), "status": "in_progress"}
        )
    except Exception:
        return None


def get_quiz_session(session_id: str):
    if quiz_sessions_collection is None:
        return None
    try:
        return quiz_sessions_collection.find_one({"_id": session_id})
    except Exception:
        return None


def update_quiz_session(session_id: str, fields: dict):
    if quiz_sessions_collection is None:
        return
    now = _now_utc()
    fields["updated_at"] = now.isoformat()
    fields["updated_at_dt"] = now
    try:
        quiz_sessions_collection.update_one(
            {"_id": session_id},
            {"$set": fields}
        )
    except Exception as e:
        logger.error("update_quiz_session error: %s", e)


def advance_quiz_session(session_id: str, qid: str, user_answer: str,
                         is_correct: bool, question_obj: dict):
    if quiz_sessions_collection is None:
        return None
    now = _now_utc()
    answer_record = {
        "qid": qid,
        "user_answer": user_answer,
        "is_correct": is_correct,
        "question_obj": question_obj,
        "ts": now.isoformat(),
    }
    try:
        quiz_sessions_collection.update_one(
            {"_id": session_id},
            {
                "$inc": {
                    "current_index": 1,
                    "correct_count": 1 if is_correct else 0,
                },
                "$push": {"answered_questions": answer_record},
                "$set": {
                    "updated_at": now.isoformat(),
                    "updated_at_dt": now,
                },
            }
        )
        return quiz_sessions_collection.find_one({"_id": session_id})
    except Exception as e:
        logger.error("advance_quiz_session error: %s", e)
        return None


def set_question_sent_at(session_id: str, ts: float = None):
    update_quiz_session(session_id, {"question_sent_at": ts or time.time()})


def finish_quiz_session(session_id: str):
    now = _now_utc()
    update_quiz_session(session_id, {
        "status": "finished",
        "finished_at": now.isoformat(),
    })


def cancel_quiz_session(session_id: str):
    now = _now_utc()
    update_quiz_session(session_id, {
        "status": "cancelled",
        "cancelled_at": now.isoformat(),
    })


def cancel_active_quiz_session(user_id: int):
    if quiz_sessions_collection is None:
        return
    now = _now_utc()
    try:
        quiz_sessions_collection.update_many(
            {"user_id": _uid(user_id), "status": "in_progress"},
            {"$set": {
                "status": "cancelled",
                "cancelled_at": now.isoformat(),
                "updated_at": now.isoformat(),
                "updated_at_dt": now,
            }}
        )
    except Exception as e:
        logger.error("cancel_active_quiz_session error: %s", e)


def get_stale_sessions(max_age_hours: int = 2):
    if quiz_sessions_collection is None:
        return []
    threshold = _now_utc() - timedelta(hours=max_age_hours)
    try:
        return list(quiz_sessions_collection.find(
            {
                "status": "in_progress",
                "updated_at_dt": {"$lt": threshold},
            }
        ).limit(100))
    except Exception:
        return []


# ═══════════════════════════════════════════════
# USERS / STATS
# ═══════════════════════════════════════════════

# (rest of file unchanged below this section)
