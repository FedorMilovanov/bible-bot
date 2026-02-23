# Работа с MongoDB — Рефакторинг v2

import os
import time
import uuid
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING, DESCENDING

# --- ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ ---
MONGO_URL = os.getenv('MONGO_URL')

if MONGO_URL:
    try:
        cluster = MongoClient(MONGO_URL)
        db = cluster["bible_bot_db"]
        collection = db["leaderboard"]
        battles_collection = db["battles"]
        questions_stats_collection = db["questions_stats"]
        quiz_sessions_collection = db["quiz_sessions"]
        reports_collection = db["reports"]
        weekly_lb_collection = db["weekly_leaderboard"]
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        collection = battles_collection = questions_stats_collection = None
        quiz_sessions_collection = reports_collection = weekly_lb_collection = None
else:
    print("⚠️ ВНИМАНИЕ: Не задана переменная MONGO_URL.")
    collection = battles_collection = questions_stats_collection = None
    quiz_sessions_collection = reports_collection = weekly_lb_collection = None


# ═══════════════════════════════════════════════
# TTL INDEXES
# ═══════════════════════════════════════════════

def _ensure_indexes():
    """Создаёт все необходимые индексы при старте."""
    # TTL для quiz_sessions — 6 часов по updated_at_dt
    if quiz_sessions_collection is not None:
        try:
            quiz_sessions_collection.create_index(
                [("updated_at_dt", ASCENDING)],
                expireAfterSeconds=21600,
                name="ttl_updated_at",
                background=True,
            )
        except Exception as e:
            print(f"quiz_sessions TTL index warning: {e}")

    # TTL для battles — 30 дней по created_at_dt (задание 1.1)
    if battles_collection is not None:
        try:
            battles_collection.create_index(
                [("created_at_dt", ASCENDING)],
                expireAfterSeconds=2592000,  # 30 дней
                name="ttl_battles_created_at",
                background=True,
            )
            # Индекс для быстрого поиска ожидающих битв
            battles_collection.create_index(
                [("status", ASCENDING), ("created_at_dt", DESCENDING)],
                background=True,
            )
        except Exception as e:
            print(f"battles TTL index warning: {e}")

    # Индекс для users — last_activity (для GC)
    if collection is not None:
        try:
            collection.create_index(
                [("last_activity", DESCENDING)],
                background=True,
            )
        except Exception as e:
            print(f"leaderboard index warning: {e}")

_ensure_indexes()


# ═══════════════════════════════════════════════
# QUIZ SESSIONS — CRUD
# ═══════════════════════════════════════════════

def create_quiz_session(user_id: int, mode: str, question_ids: list,
                        questions_data: list,
                        level_key: str = None, level_name: str = None,
                        time_limit: int = None) -> str:
    if quiz_sessions_collection is None:
        return ""
    session_id = str(uuid.uuid4())
    now = datetime.utcnow()
    doc = {
        "_id": session_id,
        "user_id": str(user_id),
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
        "start_time": time.time(),
        "started_at": now.isoformat(),
        "created_at": now,           # ISODate для TTL
        "updated_at": now.isoformat(),
        "updated_at_dt": now,
    }
    try:
        quiz_sessions_collection.insert_one(doc)
    except Exception as e:
        print(f"create_quiz_session error: {e}")
    return session_id


def get_active_quiz_session(user_id: int):
    if quiz_sessions_collection is None:
        return None
    try:
        return quiz_sessions_collection.find_one(
            {"user_id": str(user_id), "status": "in_progress"}
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
    now = datetime.utcnow()
    fields["updated_at"] = now.isoformat()
    fields["updated_at_dt"] = now
    try:
        quiz_sessions_collection.update_one(
            {"_id": session_id},
            {"$set": fields}
        )
    except Exception as e:
        print(f"update_quiz_session error: {e}")


def advance_quiz_session(session_id: str, qid: str, user_answer: str,
                         is_correct: bool, question_obj: dict):
    if quiz_sessions_collection is None:
        return None
    now = datetime.utcnow()
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
        print(f"advance_quiz_session error: {e}")
        return None


def set_question_sent_at(session_id: str, ts: float = None):
    update_quiz_session(session_id, {"question_sent_at": ts or time.time()})


def finish_quiz_session(session_id: str):
    update_quiz_session(session_id, {"status": "finished"})


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


# ═══════════════════════════════════════════════
# BATTLES — MongoDB-backed CRUD  (задание 1.2)
# ═══════════════════════════════════════════════

def create_battle_doc(battle_id: str, creator_id: int, creator_name: str,
                      questions: list) -> dict:
    """Создаёт документ битвы в MongoDB. Возвращает doc или None."""
    if battles_collection is None:
        return None
    now = datetime.utcnow()
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
        "created_at_dt": now,   # ISODate для TTL
        "updated_at": now.isoformat(),
    }
    try:
        battles_collection.insert_one(doc)
        return doc
    except Exception as e:
        print(f"create_battle_doc error: {e}")
        return None


def get_battle(battle_id: str) -> dict | None:
    if battles_collection is None:
        return None
    try:
        return battles_collection.find_one({"_id": battle_id})
    except Exception:
        return None


def update_battle(battle_id: str, fields: dict):
    if battles_collection is None:
        return
    fields["updated_at"] = datetime.utcnow().isoformat()
    try:
        battles_collection.update_one({"_id": battle_id}, {"$set": fields})
    except Exception as e:
        print(f"update_battle error: {e}")


def get_waiting_battles(limit: int = 10) -> list:
    """Возвращает битвы со статусом 'waiting', отсортированные по дате."""
    if battles_collection is None:
        return []
    cutoff = datetime.utcnow() - timedelta(minutes=10)
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
        print(f"delete_battle error: {e}")


def cleanup_stale_battles():
    """Удаляет битвы старше 10 минут (вызывается из JobQueue)."""
    if battles_collection is None:
        return 0
    cutoff = datetime.utcnow() - timedelta(minutes=10)
    try:
        result = battles_collection.delete_many(
            {"status": "waiting", "created_at_dt": {"$lt": cutoff}}
        )
        return result.deleted_count
    except Exception as e:
        print(f"cleanup_stale_battles error: {e}")
        return 0


# ═══════════════════════════════════════════════
# ОСНОВНЫЕ ФУНКЦИИ БД — ПОЛЬЗОВАТЕЛИ
# ═══════════════════════════════════════════════

HISTORICAL_CATEGORIES = {"nero", "geography"}


def get_user_stats(user_id):
    if collection is None:
        return None
    user_id_str = str(user_id)
    entry = collection.find_one({"_id": user_id_str})
    if not entry:
        return None
    return entry


def init_user_stats(user_id, username, first_name):
    if collection is None:
        return
    user_id_str = str(user_id)
    entry = collection.find_one({"_id": user_id_str})
    if not entry:
        now = datetime.utcnow()
        new_entry = {
            "_id": user_id_str,
            "username": username or "Без username",
            "first_name": first_name or "Пользователь",
            "first_play_date": datetime.now().strftime("%Y-%m-%d"),
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
        }
        # Инициализируем поля для каждой категории
        for key in ["easy", "medium", "hard", "nero", "geography",
                    "practical_ch1", "linguistics_ch1", "linguistics_ch1_2",
                    "linguistics_ch1_3", "intro1", "intro2", "intro3",
                    "random20", "hardcore20"]:
            new_entry[f"{key}_attempts"] = 0
            new_entry[f"{key}_correct"] = 0
            new_entry[f"{key}_total"] = 0
            new_entry[f"{key}_best_score"] = 0
        try:
            collection.insert_one(new_entry)
            return True
        except Exception as e:
            print(f"init_user_stats error: {e}")
    else:
        # Обновляем last_activity при каждом обращении
        try:
            collection.update_one(
                {"_id": user_id_str},
                {"$set": {
                    "last_activity": datetime.utcnow(),
                    "username": username or entry.get("username", ""),
                    "first_name": first_name or entry.get("first_name", ""),
                }}
            )
        except Exception:
            pass
    return False


def touch_user_activity(user_id: int):
    """Обновляет поле last_activity (для GC). Вызывается при каждом действии."""
    if collection is None:
        return
    try:
        collection.update_one(
            {"_id": str(user_id)},
            {"$set": {"last_activity": datetime.utcnow()}}
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════
# ADMIN STATS (задание 4.3)
# ═══════════════════════════════════════════════

def get_admin_stats() -> dict:
    """Возвращает статистику для /admin команды."""
    if collection is None:
        return {}
    now = datetime.utcnow()
    day_ago = now - timedelta(hours=24)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    total_users = collection.count_documents({})
    new_today = collection.count_documents({"created_at": {"$gte": today_start}})
    online_24h = collection.count_documents({"last_activity": {"$gte": day_ago}})

    return {
        "total_users": total_users,
        "new_today": new_today,
        "online_24h": online_24h,
    }


def get_all_user_ids() -> list:
    """Возвращает список всех user_id для рассылки."""
    if collection is None:
        return []
    try:
        return [int(doc["_id"]) for doc in collection.find({}, {"_id": 1})]
    except Exception:
        return []


# ═══════════════════════════════════════════════
# LEADERBOARD & STATS
# ═══════════════════════════════════════════════

def add_to_leaderboard(user_id, username, first_name, level_key, score, total, time_seconds):
    if collection is None:
        return
    user_id_str = str(user_id)
    now = datetime.utcnow()

    update_fields = {
        "username": username or "",
        "first_name": first_name or "Пользователь",
        "last_activity": now,
    }

    if level_key not in HISTORICAL_CATEGORIES:
        POINTS_MAP = {
            "easy": 1, "medium": 2, "hard": 3,
            "practical_ch1": 2, "linguistics_ch1": 3,
            "linguistics_ch1_2": 3, "linguistics_ch1_3": 3,
            "intro1": 2, "intro2": 2, "intro3": 2,
        }
        pts = score * POINTS_MAP.get(level_key, 1)
        update_fields["total_points"] = pts  # will use $inc below

    try:
        inc_fields = {
            "total_tests": 1,
            "total_questions_answered": total,
            "total_correct_answers": score,
            "total_time_spent": time_seconds,
            f"{level_key}_attempts": 1,
            f"{level_key}_correct": score,
            f"{level_key}_total": total,
        }
        if level_key not in HISTORICAL_CATEGORIES:
            POINTS_MAP = {
                "easy": 1, "medium": 2, "hard": 3,
                "practical_ch1": 2, "linguistics_ch1": 3,
                "linguistics_ch1_2": 3, "linguistics_ch1_3": 3,
                "intro1": 2, "intro2": 2, "intro3": 2,
            }
            inc_fields["total_points"] = score * POINTS_MAP.get(level_key, 1)

        collection.update_one(
            {"_id": user_id_str},
            {
                "$inc": inc_fields,
                "$set": update_fields,
                "$max": {f"{level_key}_best_score": score},
            },
            upsert=True
        )
    except Exception as e:
        print(f"add_to_leaderboard error: {e}")


def update_battle_stats(user_id, result):
    if collection is None:
        return
    user_id_str = str(user_id)
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
            {"_id": user_id_str},
            {"$inc": inc, "$set": {"last_activity": datetime.utcnow()}},
            upsert=True
        )
    except Exception as e:
        print(f"update_battle_stats error: {e}")


def get_user_position(user_id):
    if collection is None:
        return None, None
    user_id_str = str(user_id)
    entry = collection.find_one({"_id": user_id_str})
    if not entry:
        return None, None
    pts = entry.get("total_points", 0)
    position = collection.count_documents({"total_points": {"$gt": pts}}) + 1
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
    user_id_str = str(user_id)
    entry = collection.find_one({"_id": user_id_str})
    if not entry:
        return None
    pts = entry.get("total_points", 0)
    above = collection.find_one(
        {"total_points": {"$gt": pts}},
        sort=[("total_points", ASCENDING)]
    )
    if not above:
        return None
    return above.get("total_points", pts) - pts


def get_category_leaderboard(category_key, limit=10):
    if collection is None:
        return []
    try:
        return list(
            collection.find({f"{category_key}_attempts": {"$gt": 0}})
            .sort(f"{category_key}_correct", DESCENDING)
            .limit(limit)
        )
    except Exception:
        return []


def get_context_leaderboard(limit=10):
    if collection is None:
        return []
    try:
        users = list(collection.find(
            {"$or": [
                {"nero_correct": {"$gt": 0}},
                {"geography_correct": {"$gt": 0}},
                {"intro1_correct": {"$gt": 0}},
                {"intro2_correct": {"$gt": 0}},
                {"intro3_correct": {"$gt": 0}},
            ]}
        ))
        for u in users:
            correct = sum(u.get(f"{k}_correct", 0)
                         for k in ["nero", "geography", "intro1", "intro2", "intro3"])
            total = sum(u.get(f"{k}_total", 0)
                       for k in ["nero", "geography", "intro1", "intro2", "intro3"])
            u["_context_correct"] = correct
            u["_context_acc"] = round(correct / total * 100) if total else 0
        users.sort(key=lambda x: x["_context_correct"], reverse=True)
        return users[:limit]
    except Exception:
        return []


# ═══════════════════════════════════════════════
# CHALLENGE STATS
# ═══════════════════════════════════════════════

def get_current_week_id():
    from datetime import date
    d = date.today()
    return f"{d.year}-W{d.isocalendar()[1]:02d}"


def is_bonus_eligible(user_id: int, mode: str) -> bool:
    if collection is None:
        return True
    entry = collection.find_one({"_id": str(user_id)})
    if not entry:
        return True
    last_bonus_key = f"{mode}_last_bonus_date"
    last_date = entry.get(last_bonus_key, "")
    today = datetime.now().strftime("%Y-%m-%d")
    return last_date != today


def compute_bonus(score: int, mode: str, eligible: bool) -> int:
    if not eligible:
        return 0
    if mode == "random20":
        thresholds = [(20, 100), (19, 80), (18, 60), (17, 40), (16, 25), (15, 10)]
    else:
        thresholds = [(20, 200), (19, 150), (18, 110), (17, 80), (16, 50), (15, 25)]
    for min_score, bonus in thresholds:
        if score >= min_score:
            return bonus
    return 0


def update_challenge_stats(user_id, username, first_name, mode, score, total,
                            time_seconds, eligible):
    if collection is None:
        return 0, []
    user_id_str = str(user_id)
    today = datetime.now().strftime("%Y-%m-%d")
    ppq = 1 if mode == "random20" else 2
    earned_base = score * ppq
    bonus = compute_bonus(score, mode, eligible)
    total_earned = earned_base + bonus

    entry = collection.find_one({"_id": user_id_str}) or {}
    achievements = entry.get("achievements", {})
    new_achievements = []

    upd = {
        "username": username or "",
        "first_name": first_name or "Пользователь",
        "last_activity": datetime.utcnow(),
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

    # Streak logic
    if score >= 18 and mode in ("random20", "hardcore20"):
        streak_count = entry.get("challenge_streak_count", 0)
        streak_last = entry.get("challenge_streak_last_date", "")
        if streak_last == "":
            streak_count = 1
        else:
            try:
                last_dt = datetime.strptime(streak_last, "%Y-%m-%d")
                delta = (datetime.strptime(today, "%Y-%m-%d") - last_dt).days
                if delta == 1:
                    streak_count += 1
                elif delta == 0:
                    pass
                else:
                    streak_count = 1
            except Exception:
                streak_count = 1
        upd["challenge_streak_count"] = streak_count
        upd["challenge_streak_last_date"] = today

        if streak_count >= 3 and "streak_3" not in achievements:
            achievements["streak_3"] = today
            new_achievements.append("🔥 3-дневная серия 18+ — разблокировано!")
    else:
        upd["challenge_streak_count"] = 0
        upd["challenge_streak_last_date"] = today

    if score == 20 and "perfect_20" not in achievements:
        achievements["perfect_20"] = today
        new_achievements.append("⭐ Perfect 20 — разблокировано!")

    if new_achievements:
        upd["achievements"] = achievements

    try:
        collection.update_one(
            {"_id": user_id_str},
            {"$inc": inc, "$set": upd, "$max": {f"{mode}_best_score": score}},
            upsert=True
        )
    except Exception as e:
        print(f"update_challenge_stats error: {e}")

    return total_earned, new_achievements


def update_weekly_leaderboard(user_id, username, first_name, mode, score, time_seconds):
    if weekly_lb_collection is None:
        return
    week_id = get_current_week_id()
    doc_id = f"{week_id}_{mode}_{user_id}"
    existing = weekly_lb_collection.find_one({"_id": doc_id})
    if not existing or score > existing.get("best_score", 0) or \
       (score == existing.get("best_score", 0) and time_seconds < existing.get("best_time", 9999)):
        weekly_lb_collection.update_one(
            {"_id": doc_id},
            {"$set": {
                "week_id": week_id,
                "mode": mode,
                "user_id": str(user_id),
                "username": username or "",
                "first_name": first_name or "Пользователь",
                "best_score": score,
                "best_time": time_seconds,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            }},
            upsert=True
        )


def get_weekly_leaderboard(mode, limit=10):
    if weekly_lb_collection is None:
        return []
    week_id = get_current_week_id()
    try:
        return list(
            weekly_lb_collection.find({"week_id": week_id, "mode": mode})
            .sort([("best_score", DESCENDING), ("best_time", ASCENDING)])
            .limit(limit)
        )
    except Exception:
        return []


def get_user_achievements(user_id):
    if collection is None:
        return {}, 0, ""
    entry = collection.find_one({"_id": str(user_id)})
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
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}с"
    m, s = divmod(seconds, 60)
    return f"{m}м {s}с"


def calculate_days_playing(first_date_str: str) -> int:
    try:
        first = datetime.strptime(first_date_str, "%Y-%m-%d")
        return max(1, (datetime.now() - first).days + 1)
    except Exception:
        return 1


def calculate_accuracy(correct: int, total: int) -> int:
    if total == 0:
        return 0
    return round(correct / total * 100)


def record_question_stat(q_id: str, category: str, is_correct: bool, elapsed: float):
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
            upsert=True
        )
    except Exception:
        pass


def get_question_stats(q_id: str):
    if questions_stats_collection is None:
        return None
    return questions_stats_collection.find_one({"_id": q_id})


# ═══════════════════════════════════════════════
# REPORTS
# ═══════════════════════════════════════════════

_report_last_sent: dict = {}
REPORT_COOLDOWN_SECONDS = 60


def can_submit_report(user_id: int) -> bool:
    last = _report_last_sent.get(user_id, 0)
    return (time.time() - last) >= REPORT_COOLDOWN_SECONDS


def seconds_until_next_report(user_id: int) -> int:
    last = _report_last_sent.get(user_id, 0)
    remaining = REPORT_COOLDOWN_SECONDS - (time.time() - last)
    return max(0, int(remaining))


def insert_report(user_id: int, username: str, first_name: str,
                  report_type: str, text: str, context: dict = None) -> str:
    report_id = str(uuid.uuid4())
    now = datetime.utcnow()
    doc = {
        "_id": report_id,
        "report_id": report_id,
        "type": report_type,
        "user_id": str(user_id),
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
            print(f"insert_report error: {e}")
    _report_last_sent[user_id] = time.time()
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
        print(f"mark_report_delivered error: {e}")
