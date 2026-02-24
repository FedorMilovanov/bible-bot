# database.py — Рефакторинг v3
# MongoDB: сессии, битвы, лидерборд, репорты, достижения

import os
import time
import uuid
import logging
import functools
from datetime import datetime, date, timedelta
from pymongo import MongoClient, ASCENDING, DESCENDING

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════
# ПОДКЛЮЧЕНИЕ
# ═══════════════════════════════════════════════

MONGO_URL = os.getenv("MONGO_URL")

if MONGO_URL:
    try:
        cluster = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
        db = cluster["bible_bot_db"]
        collection = db["leaderboard"]
        battles_collection = db["battles"]
        questions_stats_collection = db["questions_stats"]
        quiz_sessions_collection = db["quiz_sessions"]
        reports_collection = db["reports"]
        weekly_lb_collection = db["weekly_leaderboard"]
        logger.info("✅ MongoDB подключена")
    except Exception as e:
        logger.error("Ошибка подключения к БД: %s", e)
        collection = battles_collection = questions_stats_collection = None
        quiz_sessions_collection = reports_collection = weekly_lb_collection = None
else:
    logger.warning("⚠️ MONGO_URL не задана")
    collection = battles_collection = questions_stats_collection = None
    quiz_sessions_collection = reports_collection = weekly_lb_collection = None


# ═══════════════════════════════════════════════
# КОНСТАНТЫ
# ═══════════════════════════════════════════════

ALL_LEVEL_KEYS = [
    "easy", "easy_p1", "easy_p2",
    "medium", "medium_p1", "medium_p2",
    "hard", "hard_p1", "hard_p2",
    "practical_ch1", "practical_p1", "practical_p2",
    "linguistics_ch1", "linguistics_ch1_2", "linguistics_ch1_3",
    "nero", "geography",
    "intro1", "intro2", "intro3",
    "random20", "hardcore20",
]

_ALL_LEVEL_KEYS_SET = frozenset(ALL_LEVEL_KEYS)

POINTS_PER_QUESTION = {
    "easy": 1, "easy_p1": 1, "easy_p2": 1,
    "medium": 2, "medium_p1": 2, "medium_p2": 2,
    "hard": 3, "hard_p1": 3, "hard_p2": 3,
    "practical_ch1": 2, "practical_p1": 2, "practical_p2": 2,
    "linguistics_ch1": 3, "linguistics_ch1_2": 3, "linguistics_ch1_3": 3,
    "intro1": 2, "intro2": 2, "intro3": 2,
    "nero": 2, "geography": 2,
    "random20": 1, "hardcore20": 2,
}

REPORT_COOLDOWN_SECONDS = 60


# ═══════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════

def _uid(user_id) -> str:
    """Приводит user_id к строке для _id в MongoDB."""
    return str(user_id)


def _now_utc() -> datetime:
    return datetime.utcnow()


def _today_utc() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def _safe_level_key(key: str) -> str:
    """Проверяет что ключ уровня допустимый (защита от injection)."""
    if key in _ALL_LEVEL_KEYS_SET:
        return key
    logger.warning("Invalid level_key: %s — defaulting to 'easy'", key)
    return "easy"


def _validate_score(score: int, total: int) -> tuple:
    """Не позволяет записать невозможные результаты."""
    total = max(1, min(int(total), 100))
    score = max(0, min(int(score), total))
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
                        chat_id: int = None) -> str:
    if quiz_sessions_collection is None:
        return ""
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
        "end_time": now,
    })


def cancel_quiz_session(session_id: str):
    update_quiz_session(session_id, {"status": "cancelled"})


def cancel_active_quiz_session(user_id: int):
    session = get_active_quiz_session(user_id)
    if session:
        cancel_quiz_session(session["_id"])


def is_question_timed_out(session: dict) -> bool:
    time_limit = session.get("time_limit")
    if not time_limit:
        return False
    sent_at = session.get("question_sent_at")
    if not sent_at:
        return False
    return (time.time() - sent_at) >= time_limit


def get_stale_sessions(max_age_hours: int = 2, limit: int = 50) -> list:
    """Незавершённые сессии старше N часов (для напоминаний)."""
    if quiz_sessions_collection is None:
        return []
    cutoff = _now_utc() - timedelta(hours=max_age_hours)
    try:
        return list(
            quiz_sessions_collection.find({
                "status": "in_progress",
                "updated_at_dt": {"$lt": cutoff},
            }).limit(limit)
        )
    except Exception:
        return []


def get_user_history(user_id: int, limit: int = 10) -> list:
    """Последние N завершённых сессий пользователя."""
    if quiz_sessions_collection is None:
        return []
    try:
        pipeline = [
            {"$match": {"user_id": _uid(user_id), "status": "finished"}},
            {"$sort": {"updated_at_dt": -1}},
            {"$limit": limit},
            {"$project": {
                "level_name": 1,
                "level_key": 1,
                "correct_count": 1,
                "total_questions": {
                    "$size": {"$ifNull": ["$questions_data", []]}
                },
                "updated_at_dt": 1,
                "end_time": 1,
                "mode": 1,
                "start_time": 1,
            }},
        ]
        return list(quiz_sessions_collection.aggregate(pipeline))
    except Exception as e:
        logger.error("get_user_history error: %s", e)
        return []


# ═══════════════════════════════════════════════
# BATTLES — MongoDB CRUD
# ═══════════════════════════════════════════════

def create_battle_doc(battle_id: str, creator_id: int, creator_name: str,
                      questions: list) -> dict:
    if battles_collection is None:
        return None
    now = _now_utc()
    doc = {
        "_id": battle_id,
        "creator_id": creator_id,
        "creator_name": creator_name,
        "questions": questions,
        "status": "waiting",
        "creator_score": 0,
        "creator_answers": [],
        "creator_time": 0,
        "creator_points": 0,
        "creator_finished": False,
        "opponent_id": None,
        "opponent_name": None,
        "opponent_score": 0,
        "opponent_answers": [],
        "opponent_time": 0,
        "opponent_points": 0,
        "opponent_finished": False,
        "created_at": now.isoformat(),
        "created_at_dt": now,
        "updated_at": now.isoformat(),
    }
    try:
        battles_collection.insert_one(doc)
        return doc
    except Exception as e:
        logger.error("create_battle_doc error: %s", e)
        return None


def get_battle(battle_id: str):
    if battles_collection is None:
        return None
    try:
        return battles_collection.find_one({"_id": battle_id})
    except Exception:
        return None


def update_battle(battle_id: str, fields: dict):
    if battles_collection is None:
        return
    fields["updated_at"] = _now_utc().isoformat()
    try:
        battles_collection.update_one({"_id": battle_id}, {"$set": fields})
    except Exception as e:
        logger.error("update_battle error: %s", e)


def get_waiting_battles(limit: int = 10) -> list:
    if battles_collection is None:
        return []
    cutoff = _now_utc() - timedelta(minutes=10)
    try:
        return list(
            battles_collection.find(
                {"status": "waiting", "created_at_dt": {"$gte": cutoff}}
            ).sort("created_at_dt", DESCENDING).limit(limit)
        )
    except Exception:
        return []


def delete_battle(battle_id: str):
    if battles_collection is None:
        return
    try:
        battles_collection.delete_one({"_id": battle_id})
    except Exception as e:
        logger.error("delete_battle error: %s", e)


def cleanup_stale_battles() -> int:
    if battles_collection is None:
        return 0
    cutoff = _now_utc() - timedelta(minutes=10)
    try:
        result = battles_collection.delete_many(
            {"status": "waiting", "created_at_dt": {"$lt": cutoff}}
        )
        return result.deleted_count
    except Exception as e:
        logger.error("cleanup_stale_battles error: %s", e)
        return 0


# ═══════════════════════════════════════════════
# ПОЛЬЗОВАТЕЛИ
# ═══════════════════════════════════════════════

def get_user_stats(user_id):
    if collection is None:
        return None
    try:
        return collection.find_one({"_id": _uid(user_id)})
    except Exception:
        return None


def init_user_stats(user_id, username, first_name):
    if collection is None:
        return False
    uid = _uid(user_id)
    entry = collection.find_one({"_id": uid})
    now = _now_utc()

    if not entry:
        new_entry = {
            "_id": uid,
            "username": username or "Без username",
            "first_name": first_name or "Пользователь",
            "first_play_date": _today_utc(),
            "created_at": now,
            "last_activity": now,
            "total_points": 0,
            "total_tests": 0,
            "total_questions_answered": 0,
            "total_correct_answers": 0,
            "total_time_spent": 0,
            "battles_played": 0,
            "battles_won": 0,
            "battles_lost": 0,
            "battles_draw": 0,
            "achievements": {},
            "challenge_streak_count": 0,
            "challenge_streak_last_date": "",
            "daily_streak": 0,
            "daily_streak_last": "",
        }
        for key in ALL_LEVEL_KEYS:
            new_entry[f"{key}_attempts"] = 0
            new_entry[f"{key}_correct"] = 0
            new_entry[f"{key}_total"] = 0
            new_entry[f"{key}_best_score"] = 0
        try:
            collection.insert_one(new_entry)
            return True
        except Exception as e:
            logger.error("init_user_stats error: %s", e)
            return False
    else:
        try:
            collection.update_one(
                {"_id": uid},
                {"$set": {
                    "last_activity": now,
                    "username": username or entry.get("username", ""),
                    "first_name": first_name or entry.get("first_name", ""),
                }}
            )
        except Exception:
            pass
        return False


def touch_user_activity(user_id: int):
    if collection is None:
        return
    try:
        collection.update_one(
            {"_id": _uid(user_id)},
            {"$set": {"last_activity": _now_utc()}}
        )
    except Exception:
        pass


def update_daily_streak(user_id: int) -> int:
    """Обновляет ежедневную серию входов. Возвращает текущий streak."""
    if collection is None:
        return 0
    uid = _uid(user_id)
    today = _today_utc()
    entry = collection.find_one(
        {"_id": uid},
        {"daily_streak": 1, "daily_streak_last": 1}
    )
    if not entry:
        return 0

    streak = entry.get("daily_streak", 0)
    last = entry.get("daily_streak_last", "")

    if last == today:
        return streak

    try:
        if last:
            last_dt = datetime.strptime(last, "%Y-%m-%d")
            today_dt = datetime.strptime(today, "%Y-%m-%d")
            delta = (today_dt - last_dt).days
            if delta == 1:
                streak += 1
            elif delta > 1:
                streak = 1
        else:
            streak = 1

        collection.update_one(
            {"_id": uid},
            {"$set": {
                "daily_streak": streak,
                "daily_streak_last": today,
            }}
        )
    except Exception as e:
        logger.error("update_daily_streak error: %s", e)

    return streak


# ═══════════════════════════════════════════════
# ADMIN
# ═══════════════════════════════════════════════

def get_admin_stats() -> dict:
    if collection is None:
        return {"db_healthy": False}
    now = _now_utc()
    day_ago = now - timedelta(hours=24)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    try:
        total_users = collection.count_documents({})
        new_today = collection.count_documents({"created_at": {"$gte": today_start}})
        online_24h = collection.count_documents({"last_activity": {"$gte": day_ago}})
    except Exception:
        total_users = new_today = online_24h = 0

    return {
        "total_users": total_users,
        "new_today": new_today,
        "online_24h": online_24h,
        "db_healthy": check_db_connection(),
    }


def get_detailed_admin_stats() -> dict:
    """Расширенная статистика для admin-панели."""
    stats = get_admin_stats()
    if collection is None:
        return stats
    try:
        pipeline = [
            {"$group": {
                "_id": None,
                "avg_points": {"$avg": "$total_points"},
                "avg_tests": {"$avg": "$total_tests"},
                "avg_accuracy": {"$avg": {
                    "$cond": [
                        {"$gt": ["$total_questions_answered", 0]},
                        {"$divide": [
                            "$total_correct_answers",
                            "$total_questions_answered"
                        ]},
                        0,
                    ]
                }},
                "total_tests_all": {"$sum": "$total_tests"},
                "total_battles_all": {"$sum": "$battles_played"},
            }}
        ]
        result = list(collection.aggregate(pipeline))
        if result:
            r = result[0]
            stats["avg_points"] = round(r.get("avg_points", 0), 1)
            stats["avg_tests"] = round(r.get("avg_tests", 0), 1)
            stats["avg_accuracy"] = round(
                r.get("avg_accuracy", 0) * 100, 1
            )
            stats["total_tests_all"] = r.get("total_tests_all", 0)
            stats["total_battles_all"] = r.get("total_battles_all", 0)

        if quiz_sessions_collection is not None:
            stats["active_sessions"] = (
                quiz_sessions_collection.count_documents(
                    {"status": "in_progress"}
                )
            )

        if battles_collection is not None:
            stats["waiting_battles"] = (
                battles_collection.count_documents({"status": "waiting"})
            )

        if reports_collection is not None:
            stats["unread_reports"] = (
                reports_collection.count_documents(
                    {"admin_delivered": False}
                )
            )
    except Exception as e:
        logger.error("get_detailed_admin_stats error: %s", e)

    return stats


def get_all_user_ids() -> list:
    if collection is None:
        return []
    try:
        return [int(doc["_id"]) for doc in collection.find({}, {"_id": 1})]
    except Exception:
        return []


# ═══════════════════════════════════════════════
# LEADERBOARD & STATS
# ═══════════════════════════════════════════════

def add_to_leaderboard(user_id, username, first_name,
                       level_key, score, total, time_seconds,
                       score_multiplier: float = 1.0):
    if collection is None:
        return
    uid = _uid(user_id)
    now = _now_utc()
    level_key = _safe_level_key(level_key)
    score, total = _validate_score(score, total)

    ppq = POINTS_PER_QUESTION.get(level_key, 1)

    set_fields = {
        "username": username or "",
        "first_name": first_name or "Пользователь",
        "last_activity": now,
    }

    inc_fields = {
        "total_tests": 1,
        "total_questions_answered": total,
        "total_correct_answers": score,
        "total_time_spent": time_seconds,
        f"{level_key}_attempts": 1,
        f"{level_key}_correct": score,
        f"{level_key}_total": total,
        "total_points": round(score * ppq * score_multiplier),
    }

    try:
        collection.update_one(
            {"_id": uid},
            {
                "$inc": inc_fields,
                "$set": set_fields,
                "$max": {f"{level_key}_best_score": score},
            },
            upsert=True,
        )
    except Exception as e:
        logger.error("add_to_leaderboard error: %s", e)


def update_battle_stats(user_id, result):
    if collection is None:
        return
    uid = _uid(user_id)
    inc = {"battles_played": 1}
    if result == "win":
        inc["battles_won"] = 1
        inc["total_points"] = 5
    elif result == "lose":
        inc["battles_lost"] = 1
    elif result == "draw":
        inc["battles_draw"] = 1
        inc["total_points"] = 2
    try:
        collection.update_one(
            {"_id": uid},
            {"$inc": inc, "$set": {"last_activity": _now_utc()}},
            upsert=True,
        )
    except Exception as e:
        logger.error("update_battle_stats error: %s", e)


def get_user_position(user_id):
    if collection is None:
        return None, None
    uid = _uid(user_id)
    try:
        entry = collection.find_one({"_id": uid})
    except Exception:
        return None, None
    if not entry:
        return None, None
    pts = entry.get("total_points", 0)
    try:
        position = collection.count_documents(
            {"total_points": {"$gt": pts}}
        ) + 1
    except Exception:
        position = 0
    return position, entry


def get_leaderboard_page(page=0, per_page=10):
    if collection is None:
        return []
    try:
        return list(
            collection.find()
            .sort("total_points", DESCENDING)
            .skip(page * per_page)
            .limit(per_page)
        )
    except Exception:
        return []


def get_total_users():
    if collection is None:
        return 0
    try:
        return collection.count_documents({})
    except Exception:
        return 0


def get_points_to_next_place(user_id):
    if collection is None:
        return None
    uid = _uid(user_id)
    try:
        entry = collection.find_one({"_id": uid})
    except Exception:
        return None
    if not entry:
        return None
    pts = entry.get("total_points", 0)
    try:
        above = collection.find_one(
            {"total_points": {"$gt": pts}},
            sort=[("total_points", ASCENDING)]
        )
    except Exception:
        return None
    if not above:
        return None
    return above.get("total_points", pts) - pts


def get_category_leaderboard(category_key, limit=10):
    if collection is None:
        return []
    category_key = _safe_level_key(category_key)
    try:
        return list(
            collection.find(
                {f"{category_key}_attempts": {"$gt": 0}}
            )
            .sort(f"{category_key}_correct", DESCENDING)
            .limit(limit)
        )
    except Exception:
        return []


def get_context_leaderboard(limit=10):
    """Лидерборд по историческому контексту (aggregation pipeline)."""
    if collection is None:
        return []
    ctx_keys = ["nero", "geography", "intro1", "intro2", "intro3"]
    try:
        pipeline = [
            {"$match": {"$or": [
                {f"{k}_correct": {"$gt": 0}} for k in ctx_keys
            ]}},
            {"$addFields": {
                "_context_correct": {"$add": [
                    {"$ifNull": [f"${k}_correct", 0]} for k in ctx_keys
                ]},
                "_context_total": {"$add": [
                    {"$ifNull": [f"${k}_total", 0]} for k in ctx_keys
                ]},
            }},
            {"$addFields": {
                "_context_acc": {
                    "$cond": [
                        {"$gt": ["$_context_total", 0]},
                        {"$round": [
                            {"$multiply": [
                                {"$divide": [
                                    "$_context_correct",
                                    "$_context_total"
                                ]},
                                100,
                            ]},
                            0,
                        ]},
                        0,
                    ]
                }
            }},
            {"$sort": {"_context_correct": -1}},
            {"$limit": limit},
        ]
        return list(collection.aggregate(pipeline))
    except Exception as e:
        logger.error("get_context_leaderboard error: %s", e)
        return []


# ═══════════════════════════════════════════════
# CHALLENGE STATS
# ═══════════════════════════════════════════════

def get_current_week_id():
    d = date.today()
    return f"{d.year}-W{d.isocalendar()[1]:02d}"


def is_bonus_eligible(user_id: int, mode: str) -> bool:
    if collection is None:
        return True
    entry = collection.find_one({"_id": _uid(user_id)})
    if not entry:
        return True
    last_bonus_key = f"{mode}_last_bonus_date"
    last_date = entry.get(last_bonus_key, "")
    return last_date != _today_utc()


def compute_bonus(score: int, mode: str, eligible: bool) -> int:
    if not eligible:
        return 0
    if mode == "random20":
        thresholds = [
            (20, 100), (19, 80), (18, 60),
            (17, 40), (16, 25), (15, 10),
        ]
    else:
        thresholds = [
            (20, 200), (19, 150), (18, 110),
            (17, 80), (16, 50), (15, 25),
        ]
    for min_score, bonus in thresholds:
        if score >= min_score:
            return bonus
    return 0


def update_challenge_stats(user_id, username, first_name, mode,
                           score, total, time_seconds, eligible):
    if collection is None:
        return 0, []
    uid = _uid(user_id)
    today = _today_utc()
    score, total = _validate_score(score, total)

    ppq = POINTS_PER_QUESTION.get(mode, 1)
    earned_base = score * ppq
    bonus = compute_bonus(score, mode, eligible)
    total_earned = earned_base + bonus

    entry = collection.find_one({"_id": uid}) or {}
    achievements = entry.get("achievements", {})
    new_achievements = []

    upd = {
        "username": username or "",
        "first_name": first_name or "Пользователь",
        "last_activity": _now_utc(),
    }
    inc = {
        "total_tests": 1,
        "total_questions_answered": total,
        "total_correct_answers": score,
        "total_time_spent": time_seconds,
        "total_points": total_earned,
        f"{mode}_attempts": 1,
        f"{mode}_correct": score,
        f"{mode}_total": total,
    }

    if eligible:
        upd[f"{mode}_last_bonus_date"] = today

    # ── Streak logic ───────────────────────────
    if score >= 18 and mode in ("random20", "hardcore20"):
        streak_count = entry.get("challenge_streak_count", 0)
        streak_last = entry.get("challenge_streak_last_date", "")

        if streak_last == "":
            streak_count = 1
        else:
            try:
                last_dt = datetime.strptime(streak_last, "%Y-%m-%d")
                today_dt = datetime.strptime(today, "%Y-%m-%d")
                delta = (today_dt - last_dt).days
                if delta == 1:
                    streak_count += 1
                elif delta == 0:
                    pass  # уже засчитано сегодня
                else:
                    streak_count = 1
            except Exception:
                streak_count = 1

        upd["challenge_streak_count"] = streak_count
        upd["challenge_streak_last_date"] = today

        if streak_count >= 3 and "streak_3" not in achievements:
            achievements["streak_3"] = today
            new_achievements.append(
                "🔥 3-дневная серия 18+ — разблокировано!"
            )
    else:
        # Сбрасываем streak только если это ПЕРВАЯ попытка за день
        # и она провальная (если уже была успешная — не трогаем)
        streak_last = entry.get("challenge_streak_last_date", "")
        if streak_last != today:
            upd["challenge_streak_count"] = 0
            upd["challenge_streak_last_date"] = today

    # ── Perfect 20 ─────────────────────────────
    if score == 20 and "perfect_20" not in achievements:
        achievements["perfect_20"] = today
        new_achievements.append("⭐ Perfect 20 — разблокировано!")

    if new_achievements:
        upd["achievements"] = achievements

    try:
        collection.update_one(
            {"_id": uid},
            {
                "$inc": inc,
                "$set": upd,
                "$max": {f"{mode}_best_score": score},
            },
            upsert=True,
        )
    except Exception as e:
        logger.error("update_challenge_stats error: %s", e)

    return total_earned, new_achievements


def update_weekly_leaderboard(user_id, username, first_name,
                              mode, score, time_seconds):
    if weekly_lb_collection is None:
        return
    week_id = get_current_week_id()
    doc_id = f"{week_id}_{mode}_{user_id}"
    now = _now_utc()

    try:
        existing = weekly_lb_collection.find_one({"_id": doc_id})
        if (not existing
                or score > existing.get("best_score", 0)
                or (score == existing.get("best_score", 0)
                    and time_seconds < existing.get("best_time", 9999))):
            weekly_lb_collection.update_one(
                {"_id": doc_id},
                {"$set": {
                    "week_id": week_id,
                    "mode": mode,
                    "user_id": _uid(user_id),
                    "username": username or "",
                    "first_name": first_name or "Пользователь",
                    "best_score": score,
                    "best_time": time_seconds,
                    "updated_at": now.isoformat(),
                    "updated_at_dt": now,
                }},
                upsert=True,
            )
    except Exception as e:
        logger.error("update_weekly_leaderboard error: %s", e)


def get_weekly_leaderboard(mode, limit=10):
    if weekly_lb_collection is None:
        return []
    week_id = get_current_week_id()
    try:
        return list(
            weekly_lb_collection.find(
                {"week_id": week_id, "mode": mode}
            )
            .sort([("best_score", DESCENDING), ("best_time", ASCENDING)])
            .limit(limit)
        )
    except Exception:
        return []


def get_user_achievements(user_id):
    if collection is None:
        return {}, 0, ""
    entry = collection.find_one({"_id": _uid(user_id)})
    if not entry:
        return {}, 0, ""
    return (
        entry.get("achievements", {}),
        entry.get("challenge_streak_count", 0),
        entry.get("challenge_streak_last_date", ""),
    )


# ═══════════════════════════════════════════════
# UTILS
# ═══════════════════════════════════════════════

def format_time(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}с"
    m, s = divmod(seconds, 60)
    return f"{m}м {s}с"


def calculate_days_playing(first_date_str: str) -> int:
    try:
        first = datetime.strptime(first_date_str, "%Y-%m-%d")
        return max(1, (datetime.utcnow() - first).days + 1)
    except Exception:
        return 1


def calculate_accuracy(correct: int, total: int) -> int:
    if total == 0:
        return 0
    return round(correct / total * 100)


def record_question_stat(q_id: str, category: str,
                         is_correct: bool, elapsed: float):
    if questions_stats_collection is None:
        return
    try:
        questions_stats_collection.update_one(
            {"_id": q_id},
            {
                "$inc": {
                    "total_attempts": 1,
                    "correct_attempts": 1 if is_correct else 0,
                    "total_time": elapsed,
                },
                "$set": {"category": category},
            },
            upsert=True,
        )
    except Exception:
        pass


def get_question_stats(q_id: str = None):
    """Если q_id задан — возвращает стат одного вопроса.
    Иначе — список всех вопросов (для admin)."""
    if questions_stats_collection is None:
        return None if q_id else []
    if q_id:
        try:
            return questions_stats_collection.find_one({"_id": q_id})
        except Exception:
            return None
    try:
        return list(
            questions_stats_collection.find()
            .sort("total_attempts", DESCENDING)
        )
    except Exception:
        return []


def get_hardest_questions(limit: int = 10) -> list:
    """Топ самых сложных вопросов (наименьший % правильных)."""
    if questions_stats_collection is None:
        return []
    try:
        pipeline = [
            {"$match": {"total_attempts": {"$gte": 5}}},
            {"$addFields": {
                "accuracy": {
                    "$multiply": [
                        {"$divide": [
                            "$correct_attempts",
                            "$total_attempts"
                        ]},
                        100,
                    ]
                }
            }},
            {"$sort": {"accuracy": 1}},
            {"$limit": limit},
        ]
        return list(questions_stats_collection.aggregate(pipeline))
    except Exception as e:
        logger.error("get_hardest_questions error: %s", e)
        return []


# ═══════════════════════════════════════════════
# REPORTS
# ═══════════════════════════════════════════════

def can_submit_report(user_id: int) -> bool:
    """Проверяет кулдаун через MongoDB (переживает рестарт)."""
    if collection is None:
        return True
    try:
        entry = collection.find_one(
            {"_id": _uid(user_id)}, {"last_report_at": 1}
        )
        if not entry or "last_report_at" not in entry:
            return True
        last = entry["last_report_at"]
        if isinstance(last, datetime):
            elapsed = (_now_utc() - last).total_seconds()
        else:
            elapsed = REPORT_COOLDOWN_SECONDS + 1
        return elapsed >= REPORT_COOLDOWN_SECONDS
    except Exception:
        return True


def seconds_until_next_report(user_id: int) -> int:
    if collection is None:
        return 0
    try:
        entry = collection.find_one(
            {"_id": _uid(user_id)}, {"last_report_at": 1}
        )
        if not entry or "last_report_at" not in entry:
            return 0
        last = entry["last_report_at"]
        if isinstance(last, datetime):
            elapsed = (_now_utc() - last).total_seconds()
            return max(0, int(REPORT_COOLDOWN_SECONDS - elapsed))
        return 0
    except Exception:
        return 0


def insert_report(user_id: int, username: str, first_name: str,
                  report_type: str, text: str, context: dict = None) -> str:
    report_id = str(uuid.uuid4())
    now = _now_utc()
    text = (text or "")[:2000]

    doc = {
        "_id": report_id,
        "report_id": report_id,
        "type": report_type,
        "user_id": _uid(user_id),
        "username": username or "",
        "first_name": first_name or "",
        "text": text,
        "created_at": now.isoformat(),
        "created_at_dt": now,
        "context": context or {},
        "admin_delivered": False,
    }

    if reports_collection is not None:
        try:
            reports_collection.insert_one(doc)
        except Exception as e:
            logger.error("insert_report error: %s", e)

    # Обновляем кулдаун в коллекции users (переживает рестарт)
    if collection is not None:
        try:
            collection.update_one(
                {"_id": _uid(user_id)},
                {"$set": {"last_report_at": now}}
            )
        except Exception:
            pass

    return report_id


def mark_report_delivered(report_id: str):
    if reports_collection is None:
        return
    try:
        reports_collection.update_one(
            {"_id": report_id},
            {"$set": {"admin_delivered": True}}
        )
    except Exception as e:
        logger.error("mark_report_delivered error: %s", e)
