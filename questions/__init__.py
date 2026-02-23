# questions/__init__.py
"""
Пакет вопросов — 1 Петра.
Экспортирует пулы, валидирует формат, предоставляет lookup по ключу.
"""

import logging

from .chapter1 import (
    easy_questions, easy_questions_v17_25,
    medium_questions, medium_questions_v17_25,
    hard_questions, hard_questions_v17_25,
    nero_questions, geography_questions,
    practical_ch1_questions, practical_v17_25_questions,
    linguistics_ch1_questions, linguistics_ch1_questions_2,
    linguistics_v17_25_questions,
    all_chapter1_questions,
)
from .intro import (
    intro_part1_questions,
    intro_part2_questions,
    intro_part3_questions,
)

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════
# РЕЕСТР ПУЛОВ — единый источник правды
# ═══════════════════════════════════════════════

# Кешированные объединённые пулы (создаются один раз при импорте)
_pool_easy_all       = easy_questions + easy_questions_v17_25
_pool_medium_all     = medium_questions + medium_questions_v17_25
_pool_hard_all       = hard_questions + hard_questions_v17_25
_pool_practical_all  = practical_ch1_questions + practical_v17_25_questions
_pool_linguistics_all = (
    linguistics_ch1_questions
    + linguistics_ch1_questions_2
    + linguistics_v17_25_questions
)

POOL_REGISTRY: dict[str, list] = {
    # ── Лёгкий ──────────────────────────────────
    "easy":              _pool_easy_all,
    "easy_p1":           easy_questions,
    "easy_p2":           easy_questions_v17_25,
    # ── Средний ─────────────────────────────────
    "medium":            _pool_medium_all,
    "medium_p1":         medium_questions,
    "medium_p2":         medium_questions_v17_25,
    # ── Сложный ─────────────────────────────────
    "hard":              _pool_hard_all,
    "hard_p1":           hard_questions,
    "hard_p2":           hard_questions_v17_25,
    # ── Применение ──────────────────────────────
    "practical_ch1":     _pool_practical_all,
    "practical_p1":      practical_ch1_questions,
    "practical_p2":      practical_v17_25_questions,
    # ── Лингвистика ─────────────────────────────
    "linguistics_ch1":   linguistics_ch1_questions,
    "linguistics_ch1_2": linguistics_ch1_questions_2,
    "linguistics_ch1_3": linguistics_v17_25_questions,
    # ── Исторический контекст ───────────────────
    "nero":              nero_questions,
    "geography":         geography_questions,
    "intro1":            intro_part1_questions,
    "intro2":            intro_part2_questions,
    "intro3":            intro_part3_questions,
}

# Все вопросы из главы 1 (для битв)
BATTLE_POOL = all_chapter1_questions

# Все вопросы для challenge (собирается из частей)
CHALLENGE_POOLS = {
    "easy":        _pool_easy_all,
    "medium":      _pool_medium_all,
    "hard":        _pool_hard_all,
    "practical":   _pool_practical_all,
    "linguistics": _pool_linguistics_all,
}


# ═══════════════════════════════════════════════
# LOOKUP
# ═══════════════════════════════════════════════

def get_pool_by_key(key: str) -> list:
    """Возвращает пул вопросов по ключу уровня.
    Если ключ не найден — пустой список."""
    pool = POOL_REGISTRY.get(key)
    if pool is None:
        logger.warning("Unknown pool key: %s", key)
        return []
    return pool


def get_pool_size(key: str) -> int:
    """Размер пула по ключу."""
    return len(POOL_REGISTRY.get(key, []))


def get_all_pool_stats() -> dict[str, int]:
    """Словарь {key: количество_вопросов} — для admin/дебага."""
    return {key: len(pool) for key, pool in POOL_REGISTRY.items()}


def get_total_question_count() -> int:
    """Общее количество уникальных вопросов (по тексту)."""
    seen = set()
    for pool in POOL_REGISTRY.values():
        for q in pool:
            seen.add(q.get("question", ""))
    return len(seen)


# ═══════════════════════════════════════════════
# ВАЛИДАЦИЯ
# ═══════════════════════════════════════════════

_REQUIRED_FIELDS = {"question", "options", "correct", "explanation"}
_OPTIONAL_FIELDS = {"verse", "topic", "pdf_ref", "options_explanations", "id"}


def validate_question(q: dict, pool_name: str, index: int) -> list[str]:
    """Проверяет один вопрос. Возвращает список ошибок (пустой = ОК)."""
    errors = []

    # Обязательные поля
    for field in _REQUIRED_FIELDS:
        if field not in q:
            errors.append(
                f"[{pool_name}][{index}] missing '{field}'"
            )

    if "options" in q:
        opts = q["options"]
        if not isinstance(opts, list) or len(opts) < 2:
            errors.append(
                f"[{pool_name}][{index}] 'options' must be list with ≥2 items"
            )
        elif "correct" in q:
            correct_idx = q["correct"]
            if not isinstance(correct_idx, int) or correct_idx < 0 or correct_idx >= len(opts):
                errors.append(
                    f"[{pool_name}][{index}] 'correct'={correct_idx} "
                    f"out of range (0..{len(opts)-1})"
                )

        # Проверка на дубликаты вариантов
        if isinstance(opts, list) and len(opts) != len(set(opts)):
            errors.append(
                f"[{pool_name}][{index}] duplicate options found"
            )

        # options_explanations должен совпадать по длине
        if "options_explanations" in q:
            oe = q["options_explanations"]
            if isinstance(opts, list) and isinstance(oe, list) and len(oe) != len(opts):
                errors.append(
                    f"[{pool_name}][{index}] options_explanations length "
                    f"({len(oe)}) != options length ({len(opts)})"
                )

    if "question" in q:
        text = q["question"]
        if not isinstance(text, str) or len(text.strip()) < 10:
            errors.append(
                f"[{pool_name}][{index}] question text too short"
            )

    return errors


def validate_all_pools() -> tuple[int, list[str]]:
    """Валидирует ВСЕ пулы. Возвращает (total_checked, errors)."""
    all_errors = []
    total = 0

    for pool_name, pool in POOL_REGISTRY.items():
        if not pool:
            all_errors.append(f"[{pool_name}] EMPTY pool!")
            continue
        for i, q in enumerate(pool):
            total += 1
            errs = validate_question(q, pool_name, i)
            all_errors.extend(errs)

    return total, all_errors


def _run_startup_validation():
    """Запускается при импорте — логирует проблемы."""
    total, errors = validate_all_pools()
    stats = get_all_pool_stats()
    total_unique = get_total_question_count()

    logger.info(
        "📋 Questions loaded: %d total, %d unique, %d pools",
        total, total_unique, len(POOL_REGISTRY),
    )

    for key, count in stats.items():
        if count == 0:
            logger.warning("⚠️  Pool '%s' is EMPTY", key)

    if errors:
        logger.error(
            "❌ %d validation errors in questions:", len(errors)
        )
        for err in errors[:20]:  # первые 20, чтобы не заспамить
            logger.error("   %s", err)
        if len(errors) > 20:
            logger.error("   ... and %d more", len(errors) - 20)
    else:
        logger.info("✅ All %d questions passed validation", total)


_run_startup_validation()


# ═══════════════════════════════════════════════
# EXPORTS
# ═══════════════════════════════════════════════

__all__ = [
    # Пулы вопросов
    "easy_questions", "easy_questions_v17_25",
    "medium_questions", "medium_questions_v17_25",
    "hard_questions", "hard_questions_v17_25",
    "nero_questions", "geography_questions",
    "practical_ch1_questions", "practical_v17_25_questions",
    "linguistics_ch1_questions", "linguistics_ch1_questions_2",
    "linguistics_v17_25_questions",
    "all_chapter1_questions",
    "intro_part1_questions",
    "intro_part2_questions",
    "intro_part3_questions",
    # Реестр и lookup
    "POOL_REGISTRY",
    "BATTLE_POOL",
    "CHALLENGE_POOLS",
    "get_pool_by_key",
    "get_pool_size",
    "get_all_pool_stats",
    "get_total_question_count",
    # Валидация
    "validate_all_pools",
    "validate_question",
]
