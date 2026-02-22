"""
Скрипт миграции базы данных bible_bot_db.
Добавляет недостающие поля всем пользователям, не затрагивая существующие данные.
Запускать ОДИН РАЗ на сервере: python3 migrate_db.py
"""

import os
import math
from pymongo import MongoClient
from datetime import datetime

MONGO_URL = os.getenv("MONGO_URL")
if not MONGO_URL:
    raise ValueError("Не задана переменная окружения MONGO_URL")

cluster = MongoClient(MONGO_URL)
db      = cluster["bible_bot_db"]
collection = db["leaderboard"]

# ──────────────────────────────────────────────────────────────
# Дефолтные значения для всех полей актуальной схемы
# ──────────────────────────────────────────────────────────────
DEFAULTS = {
    # Общая статистика
    "total_tests":               0,
    "total_questions_answered":  0,
    "total_correct_answers":     0,
    "total_time_spent":          0,
    "first_play_date":           "2026-01-01",   # условная дата для старых юзеров

    # Лёгкий уровень
    "easy_correct":              0,
    "easy_total":                0,

    # Средний уровень
    "medium_correct":            0,
    "medium_total":              0,

    # Сложный уровень
    "hard_correct":              0,
    "hard_total":                0,

    # Нерон
    "nero_attempts":             0,
    "nero_correct":              0,
    "nero_total":                0,
    "nero_best_score":           0,

    # География
    "geography_attempts":        0,
    "geography_correct":         0,
    "geography_total":           0,
    "geography_best_score":      0,

    # Применение
    "practical_ch1_attempts":    0,
    "practical_ch1_correct":     0,
    "practical_ch1_total":       0,
    "practical_ch1_best_score":  0,

    # Лингвистика ч.1
    "linguistics_ch1_attempts":    0,
    "linguistics_ch1_correct":     0,
    "linguistics_ch1_total":       0,
    "linguistics_ch1_best_score":  0,

    # Лингвистика ч.2
    "linguistics_ch1_2_attempts":    0,
    "linguistics_ch1_2_correct":     0,
    "linguistics_ch1_2_total":       0,
    "linguistics_ch1_2_best_score":  0,

    # Лингвистика ч.3
    "linguistics_ch1_3_attempts":    0,
    "linguistics_ch1_3_correct":     0,
    "linguistics_ch1_3_total":       0,
    "linguistics_ch1_3_best_score":  0,

    # Введение ч.1
    "intro1_attempts":           0,
    "intro1_correct":            0,
    "intro1_total":              0,
    "intro1_best_score":         0,

    # Введение ч.2
    "intro2_attempts":           0,
    "intro2_correct":            0,
    "intro2_total":              0,
    "intro2_best_score":         0,

    # Введение ч.3
    "intro3_attempts":           0,
    "intro3_correct":            0,
    "intro3_total":              0,
    "intro3_best_score":         0,

    # Битвы
    "battles_played":            0,
    "battles_won":               0,
    "battles_lost":              0,
    "battles_draw":              0,
}

# Поля с best_time — заменяем Infinity на 0
BEST_TIME_FIELDS = [
    "easy_best_time",
    "medium_best_time",
    "hard_best_time",
]

# ──────────────────────────────────────────────────────────────
# Миграция
# ──────────────────────────────────────────────────────────────
users  = list(collection.find())
total  = len(users)
fixed  = 0
errors = 0

print(f"🔍 Найдено пользователей: {total}")
print("─" * 50)

for user in users:
    uid        = user["_id"]
    set_fields = {}   # поля для $set (добавить/исправить)

    # 1. Добавляем недостающие поля (только если их нет)
    for field, default in DEFAULTS.items():
        if field not in user:
            set_fields[field] = default

    # 2. Исправляем Infinity в best_time полях
    for tf in BEST_TIME_FIELDS:
        val = user.get(tf)
        if val is not None and (val == float("inf") or (isinstance(val, float) and math.isinf(val))):
            set_fields[tf] = 0

    # 3. Пересчитываем total_tests из attempts если поле отсутствовало
    #    (только для старых юзеров где total_tests не было)
    if "total_tests" not in user:
        computed_tests = (
            user.get("easy_attempts", 0) +
            user.get("medium_attempts", 0) +
            user.get("hard_attempts", 0)
        )
        if computed_tests > 0:
            set_fields["total_tests"] = computed_tests

    # Применяем обновление если есть что менять
    if set_fields:
        try:
            collection.update_one({"_id": uid}, {"$set": set_fields})
            name = user.get("first_name", uid)
            print(f"✅ {name} ({uid}): добавлено полей — {len(set_fields)}")
            if any(tf in set_fields for tf in BEST_TIME_FIELDS):
                print(f"   ⚠️  Исправлен Infinity в best_time")
            fixed += 1
        except Exception as e:
            print(f"❌ Ошибка для {uid}: {e}")
            errors += 1
    else:
        name = user.get("first_name", uid)
        print(f"⬜ {name} ({uid}): уже актуальная схема")

print("─" * 50)
print(f"✅ Обновлено: {fixed} | ⬜ Без изменений: {total - fixed - errors} | ❌ Ошибок: {errors}")
print("🎉 Миграция завершена!")
