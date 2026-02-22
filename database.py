# Работа с MongoDB

import os
from datetime import datetime
from pymongo import MongoClient

# --- ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ ---
MONGO_URL = os.getenv('MONGO_URL')

if MONGO_URL:
    try:
        cluster = MongoClient(MONGO_URL)
        db = cluster["bible_bot_db"]
        collection = db["leaderboard"]
        battles_collection = db["battles"]
        questions_stats_collection = db["questions_stats"]
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        collection = None
        battles_collection = None
        questions_stats_collection = None
else:
    print("⚠️ ВНИМАНИЕ: Не задана переменная MONGO_URL.")
    collection = None
    battles_collection = None
    questions_stats_collection = None

# --- ФУНКЦИИ БАЗЫ ДАННЫХ ---

# Категории "Исторический контекст" — не влияют на общий рейтинг
HISTORICAL_CATEGORIES = {"nero", "geography", "intro1", "intro2", "intro3"}


def get_user_stats(user_id):
    """Получить или создать статистику пользователя"""
    if collection is None:
        return None
    
    user_id_str = str(user_id)
    entry = collection.find_one({"_id": user_id_str})
    
    if not entry:
        return None
    return entry

def init_user_stats(user_id, username, first_name):
    """Инициализировать статистику нового пользователя"""
    if collection is None:
        return
    
    user_id_str = str(user_id)
    entry = collection.find_one({"_id": user_id_str})
    
    if not entry:
        new_entry = {
            "_id": user_id_str,
            "username": username or "Без username",
            "first_name": first_name or "Пользователь",
            "first_play_date": datetime.now().strftime("%Y-%m-%d"),
            "total_points": 0,
            "total_tests": 0,
            "total_questions_answered": 0,
            "total_correct_answers": 0,
            "total_time_spent": 0,
            # Статистика по уровням
            "easy_attempts": 0, "easy_correct": 0, "easy_total": 0, "easy_best_score": 0,
            "medium_attempts": 0, "medium_correct": 0, "medium_total": 0, "medium_best_score": 0,
            "hard_attempts": 0, "hard_correct": 0, "hard_total": 0, "hard_best_score": 0,
            "nero_attempts": 0, "nero_correct": 0, "nero_total": 0, "nero_best_score": 0,
            "geography_attempts": 0, "geography_correct": 0, "geography_total": 0, "geography_best_score": 0,
            "practical_ch1_attempts": 0, "practical_ch1_correct": 0, "practical_ch1_total": 0, "practical_ch1_best_score": 0,
            "linguistics_ch1_attempts": 0, "linguistics_ch1_correct": 0, "linguistics_ch1_total": 0, "linguistics_ch1_best_score": 0,
            "linguistics_ch1_2_attempts": 0, "linguistics_ch1_2_correct": 0, "linguistics_ch1_2_total": 0, "linguistics_ch1_2_best_score": 0,
            "linguistics_ch1_3_attempts": 0, "linguistics_ch1_3_correct": 0, "linguistics_ch1_3_total": 0, "linguistics_ch1_3_best_score": 0,
            "intro1_attempts": 0, "intro1_correct": 0, "intro1_total": 0, "intro1_best_score": 0,
            "intro2_attempts": 0, "intro2_correct": 0, "intro2_total": 0, "intro2_best_score": 0,
            "intro3_attempts": 0, "intro3_correct": 0, "intro3_total": 0, "intro3_best_score": 0,
            # Статистика битв
            "battles_played": 0,
            "battles_won": 0,
            "battles_lost": 0,
            "battles_draw": 0,
            "last_date": datetime.now().strftime("%Y-%m-%d %H:%M")
        }
        collection.insert_one(new_entry)

def add_to_leaderboard(user_id, username, first_name, level_key, score, total, time_seconds):
    if collection is None:
        return

    points_per_question = {"easy": 1, "medium": 2, "hard": 3, "nero": 2, "geography": 2, "practical_ch1": 2, "linguistics_ch1": 3, "linguistics_ch1_2": 3, "linguistics_ch1_3": 3, "intro1": 2, "intro2": 2, "intro3": 2}
    earned_points = score * points_per_question.get(level_key, 1)
    # Исторические категории — не начисляем очки в общий рейтинг
    is_historical = level_key in HISTORICAL_CATEGORIES
    if is_historical:
        earned_points = 0
    user_id_str = str(user_id)
    
    try:
        entry = collection.find_one({"_id": user_id_str})
        
        if entry:
            update_data = {
                "total_points": entry.get("total_points", 0) + earned_points,
                "total_tests": entry.get("total_tests", 0) + 1,
                "total_questions_answered": entry.get("total_questions_answered", 0) + total,
                "total_correct_answers": entry.get("total_correct_answers", 0) + score,
                "total_time_spent": entry.get("total_time_spent", 0) + time_seconds,
                f"{level_key}_attempts": entry.get(f"{level_key}_attempts", 0) + 1,
                f"{level_key}_correct": entry.get(f"{level_key}_correct", 0) + score,
                f"{level_key}_total": entry.get(f"{level_key}_total", 0) + total,
                f"{level_key}_best_score": max(entry.get(f"{level_key}_best_score", 0), score),
                "last_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "first_name": first_name,
                "username": username
            }
            collection.update_one({"_id": user_id_str}, {"$set": update_data})
        else:
            init_user_stats(user_id, username, first_name)
            # Повторяем обновление
            add_to_leaderboard(user_id, username, first_name, level_key, score, total, time_seconds)
    except Exception as e:
        print(f"Ошибка записи: {e}")

def update_battle_stats(user_id, result):
    """Обновить статистику битв: result = 'win', 'lose', 'draw'"""
    if collection is None:
        return
    
    user_id_str = str(user_id)
    entry = collection.find_one({"_id": user_id_str})
    
    if entry:
        update_data = {
            "battles_played": entry.get("battles_played", 0) + 1,
        }
        if result == "win":
            update_data["battles_won"] = entry.get("battles_won", 0) + 1
            update_data["total_points"] = entry.get("total_points", 0) + 5  # Бонус за победу
        elif result == "lose":
            update_data["battles_lost"] = entry.get("battles_lost", 0) + 1
        else:
            update_data["battles_draw"] = entry.get("battles_draw", 0) + 1
            update_data["total_points"] = entry.get("total_points", 0) + 2  # Бонус за ничью
        
        collection.update_one({"_id": user_id_str}, {"$set": update_data})

def get_user_position(user_id):
    if collection is None:
        return None, None
    user_id_str = str(user_id)
    try:
        entry = collection.find_one({"_id": user_id_str})
        if not entry:
            return None, None
        my_points = entry.get("total_points", 0)
        count_better = collection.count_documents({"total_points": {"$gt": my_points}})
        return count_better + 1, entry
    except Exception:
        return None, None

def get_leaderboard_page(page_number):
    if collection is None:
        return []
    try:
        skip_amount = page_number * 10
        return list(collection.find().sort("total_points", -1).skip(skip_amount).limit(10))
    except Exception:
        return []

def get_total_users():
    if collection is None:
        return 0
    try:
        return collection.count_documents({})
    except Exception:
        return 0

def format_time(seconds):
    if seconds == float('inf') or seconds == 0:
        return "—"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes > 0:
        return f"{minutes}м {secs}с"
    return f"{secs}с"

def calculate_days_playing(first_play_date):
    """Вычислить сколько дней играет"""
    try:
        first_date = datetime.strptime(first_play_date, "%Y-%m-%d")
        delta = datetime.now() - first_date
        return delta.days + 1
    except:
        return 1

def calculate_accuracy(correct, total):
    """Вычислить процент правильных ответов"""
    if total == 0:
        return 0
    return round((correct / total) * 100)


def record_question_stat(question_id: str, level_key: str, is_correct: bool, time_seconds: float):
    """
    Записывает результат ответа на конкретный вопрос.
    question_id — уникальный идентификатор вопроса (например, хэш текста или поле 'id').
    """
    if questions_stats_collection is None:
        return
    try:
        doc = questions_stats_collection.find_one({"_id": question_id})
        if doc:
            total      = doc.get("total_answers", 0) + 1
            correct    = doc.get("correct_answers", 0) + (1 if is_correct else 0)
            prev_avg   = doc.get("avg_time_seconds", time_seconds)
            # Скользящее среднее времени
            avg_time   = round(prev_avg + (time_seconds - prev_avg) / total, 2)
            # Считаем какие дистракторы выбирали (если неверно — в wrong_choices)
            questions_stats_collection.update_one(
                {"_id": question_id},
                {"$set": {
                    "total_answers":   total,
                    "correct_answers": correct,
                    "accuracy_pct":    round(correct / total * 100),
                    "avg_time_seconds": avg_time,
                    "level_key":       level_key,
                    "last_updated":    datetime.now().strftime("%Y-%m-%d %H:%M"),
                }}
            )
        else:
            questions_stats_collection.insert_one({
                "_id":             question_id,
                "level_key":       level_key,
                "total_answers":   1,
                "correct_answers": 1 if is_correct else 0,
                "accuracy_pct":    100 if is_correct else 0,
                "avg_time_seconds": round(time_seconds, 2),
                "last_updated":    datetime.now().strftime("%Y-%m-%d %H:%M"),
            })
    except Exception as e:
        print(f"Ошибка записи статистики вопроса: {e}")


def get_question_stats(level_key: str = None, limit: int = 20):
    """
    Возвращает статистику вопросов.
    Если level_key задан — фильтрует по категории.
    Сортирует по accuracy_pct (сначала сложные).
    """
    if questions_stats_collection is None:
        return []
    try:
        query  = {"level_key": level_key} if level_key else {}
        return list(
            questions_stats_collection.find(query)
            .sort("accuracy_pct", 1)  # сначала самые сложные
            .limit(limit)
        )
    except Exception:
        return []

def get_points_to_next_place(user_id):
    """Сколько очков до следующего места в рейтинге."""
    if collection is None:
        return None
    try:
        user_id_str = str(user_id)
        entry = collection.find_one({"_id": user_id_str})
        if not entry:
            return None
        my_points = entry.get("total_points", 0)
        next_player = collection.find_one(
            {"total_points": {"$gt": my_points}},
            sort=[("total_points", 1)]
        )
        if not next_player:
            return None
        return next_player.get("total_points", 0) - my_points
    except Exception:
        return None

def get_category_leaderboard(category_key, limit=10):
    """Топ игроков по конкретной категории."""
    if collection is None:
        return []
    try:
        sort_field = f"{category_key}_correct"
        return list(
            collection.find(
                {f"{category_key}_attempts": {"$gt": 0}}
            )
            .sort(sort_field, -1)
            .limit(limit)
        )
    except Exception:
        return []


# ═══════════════════════════════════════════════
# RANDOM CHALLENGE — БАЗА ДАННЫХ
# ═══════════════════════════════════════════════

# Коллекция еженедельного лидерборда
if MONGO_URL:
    try:
        weekly_lb_collection = db["weekly_leaderboard"]
    except Exception:
        weekly_lb_collection = None
else:
    weekly_lb_collection = None


def get_current_week_id():
    """Возвращает строку вида 2026-08 (ISO год-неделя)."""
    now = datetime.now()
    return f"{now.isocalendar()[0]}-{now.isocalendar()[1]:02d}"


def get_today():
    return datetime.now().strftime("%Y-%m-%d")


def is_bonus_eligible(user_id, mode):
    """
    Проверяет, может ли пользователь получить бонус сегодня.
    mode: 'random20' | 'hardcore20'
    """
    if collection is None:
        return True
    field = f"{mode}_bonus_last_date"
    entry = collection.find_one({"_id": str(user_id)})
    if not entry:
        return True
    return entry.get(field) != get_today()


def mark_bonus_used(user_id, mode):
    """Записывает что бонус сегодня уже использован."""
    if collection is None:
        return
    field = f"{mode}_bonus_last_date"
    collection.update_one(
        {"_id": str(user_id)},
        {"$set": {field: get_today()}}
    )


def compute_bonus(score, mode, eligible):
    """Возвращает супер-бонус по таблице. 0 если не eligible."""
    if not eligible:
        return 0
    if mode == "random20":
        table = {20: 100, 19: 80, 18: 60, 17: 40, 16: 25, 15: 10}
    else:  # hardcore20
        table = {20: 200, 19: 150, 18: 110, 17: 80, 16: 50, 15: 25}
    return table.get(score, 0)


def update_challenge_stats(user_id, username, first_name, mode, score, total,
                            time_seconds, eligible):
    """
    Записывает результат Random Challenge в leaderboard.
    Начисляет обычные очки + бонус (если eligible).
    Обновляет streak и достижения.
    """
    if collection is None:
        return 0, 0

    points_per_q = 1 if mode == "random20" else 2
    earned = score * points_per_q
    bonus  = compute_bonus(score, mode, eligible)
    total_earned = earned + bonus

    user_id_str = str(user_id)
    today = get_today()

    entry = collection.find_one({"_id": user_id_str})
    if not entry:
        from database import init_user_stats
        init_user_stats(user_id, username, first_name)
        entry = collection.find_one({"_id": user_id_str})

    upd = {
        "total_points":              entry.get("total_points", 0) + total_earned,
        "total_tests":               entry.get("total_tests", 0) + 1,
        "total_questions_answered":  entry.get("total_questions_answered", 0) + total,
        "total_correct_answers":     entry.get("total_correct_answers", 0) + score,
        "total_time_spent":          entry.get("total_time_spent", 0) + time_seconds,
        f"{mode}_attempts":          entry.get(f"{mode}_attempts", 0) + 1,
        f"{mode}_correct":           entry.get(f"{mode}_correct", 0) + score,
        f"{mode}_total":             entry.get(f"{mode}_total", 0) + total,
        f"{mode}_best_score":        max(entry.get(f"{mode}_best_score", 0), score),
        "last_date":                 datetime.now().strftime("%Y-%m-%d %H:%M"),
        "first_name":                first_name,
        "username":                  username or "",
    }

    if eligible:
        mark_bonus_used(user_id, mode)

    # Streak (только на eligible попытке)
    achievements = entry.get("achievements", {})
    new_achievements = []

    if eligible:
        streak_count = entry.get("challenge_streak_count", 0)
        streak_last  = entry.get("challenge_streak_last_date", "")

        if score >= 18:
            # Проверяем непрерывность streak
            if streak_last == "":
                streak_count = 1
            else:
                try:
                    last_dt = datetime.strptime(streak_last, "%Y-%m-%d")
                    delta   = (datetime.strptime(today, "%Y-%m-%d") - last_dt).days
                    if delta == 1:
                        streak_count += 1
                    elif delta == 0:
                        pass  # уже засчитан сегодня
                    else:
                        streak_count = 1  # сброс
                except Exception:
                    streak_count = 1
            upd["challenge_streak_count"]     = streak_count
            upd["challenge_streak_last_date"] = today

            # Достижение 3-day streak
            if streak_count >= 3 and "streak_3" not in achievements:
                achievements["streak_3"] = today
                new_achievements.append("🔥 3-дневная серия 18+ — разблокировано!")
        else:
            # Сбрасываем streak при score < 18
            upd["challenge_streak_count"]     = 0
            upd["challenge_streak_last_date"] = today

        # Достижение Perfect 20
        if score == 20 and "perfect_20" not in achievements:
            achievements["perfect_20"] = today
            new_achievements.append("⭐ Perfect 20 — разблокировано!")

        if new_achievements:
            upd["achievements"] = achievements

    collection.update_one({"_id": user_id_str}, {"$set": upd})
    return total_earned, new_achievements


def update_weekly_leaderboard(user_id, username, first_name, mode, score, time_seconds):
    """Обновляет еженедельный лидерборд (только на bonus_eligible попытке)."""
    if weekly_lb_collection is None:
        return
    week_id = get_current_week_id()
    doc_id  = f"{week_id}_{mode}_{user_id}"
    existing = weekly_lb_collection.find_one({"_id": doc_id})

    if not existing or score > existing.get("best_score", 0) or \
       (score == existing.get("best_score", 0) and time_seconds < existing.get("best_time", 9999)):
        weekly_lb_collection.update_one(
            {"_id": doc_id},
            {"$set": {
                "week_id":    week_id,
                "mode":       mode,
                "user_id":    str(user_id),
                "username":   username or "",
                "first_name": first_name or "Пользователь",
                "best_score": score,
                "best_time":  time_seconds,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            }},
            upsert=True
        )


def get_weekly_leaderboard(mode, limit=10):
    """Возвращает топ еженедельного лидерборда по режиму."""
    if weekly_lb_collection is None:
        return []
    week_id = get_current_week_id()
    try:
        return list(
            weekly_lb_collection.find({"week_id": week_id, "mode": mode})
            .sort([("best_score", -1), ("best_time", 1)])
            .limit(limit)
        )
    except Exception:
        return []


def get_user_achievements(user_id):
    """Возвращает достижения и streak пользователя."""
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


def get_context_leaderboard(limit=10):
    """
    Объединённый рейтинг 'Знатоки контекста':
    суммирует верные ответы по intro1+intro2+intro3+nero+geography.
    """
    if collection is None:
        return []
    try:
        users = list(collection.find(
            {"$or": [
                {"intro1_attempts": {"$gt": 0}},
                {"nero_attempts":   {"$gt": 0}},
                {"geography_attempts": {"$gt": 0}},
            ]}
        ))
        for u in users:
            correct = (
                u.get("intro1_correct", 0) +
                u.get("intro2_correct", 0) +
                u.get("intro3_correct", 0) +
                u.get("nero_correct",   0) +
                u.get("geography_correct", 0)
            )
            total = (
                u.get("intro1_total", 0) +
                u.get("intro2_total", 0) +
                u.get("intro3_total", 0) +
                u.get("nero_total",   0) +
                u.get("geography_total", 0)
            )
            u["_context_correct"] = correct
            u["_context_acc"]     = round(correct / total * 100) if total else 0

        users.sort(key=lambda x: x["_context_correct"], reverse=True)
        return users[:limit]
    except Exception:
        return []


def get_context_leaderboard(limit=10):
    """
    Рейтинг 'Знатоки контекста' — суммирует верные ответы
    по nero + geography + intro1 + intro2 + intro3.
    """
    if collection is None:
        return []
    try:
        users = list(collection.find(
            {"$or": [
                {"nero_correct":      {"$gt": 0}},
                {"geography_correct": {"$gt": 0}},
                {"intro1_correct":    {"$gt": 0}},
                {"intro2_correct":    {"$gt": 0}},
                {"intro3_correct":    {"$gt": 0}},
            ]}
        ))
        for u in users:
            u["_context_correct"] = (
                u.get("nero_correct", 0) +
                u.get("geography_correct", 0) +
                u.get("intro1_correct", 0) +
                u.get("intro2_correct", 0) +
                u.get("intro3_correct", 0)
            )
            u["_context_total"] = (
                u.get("nero_total", 0) +
                u.get("geography_total", 0) +
                u.get("intro1_total", 0) +
                u.get("intro2_total", 0) +
                u.get("intro3_total", 0)
            )
            # Для совместимости с show_category_leaderboard
            u[f"context_correct"] = u["_context_correct"]
            u[f"context_total"]   = u["_context_total"]
            u[f"context_best_score"] = 0
            u[f"context_attempts"]   = 1
        users.sort(key=lambda x: x["_context_correct"], reverse=True)
        return users[:limit]
    except Exception:
        return []
