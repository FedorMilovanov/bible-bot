from .chapter1 import (
    easy_questions, easy_questions_v17_25,
    medium_questions, medium_questions_v17_25,
    hard_questions, hard_questions_v17_25,
    nero_questions, geography_questions,
    practical_ch1_questions, practical_v17_25_questions,
    linguistics_ch1_questions, linguistics_ch1_questions_2, linguistics_v17_25_questions,
    all_chapter1_questions,
)
from .intro import (
    intro_part1_questions,
    intro_part2_questions,
    intro_part3_questions,
)

# ── Комбинированные пулы (собираются один раз при импорте) ──────────────
_POOLS: dict[str, list] = {
    # Лёгкий
    "easy":             easy_questions + easy_questions_v17_25,
    "easy_p1":          easy_questions,
    "easy_p2":          easy_questions_v17_25,
    # Средний
    "medium":           medium_questions + medium_questions_v17_25,
    "medium_p1":        medium_questions,
    "medium_p2":        medium_questions_v17_25,
    # Сложный
    "hard":             hard_questions + hard_questions_v17_25,
    "hard_p1":          hard_questions,
    "hard_p2":          hard_questions_v17_25,
    # Применение
    "practical_ch1":    practical_ch1_questions + practical_v17_25_questions,
    "practical_p1":     practical_ch1_questions,
    "practical_p2":     practical_v17_25_questions,
    # Лингвистика
    "linguistics_ch1":  linguistics_ch1_questions,
    "linguistics_ch1_2":linguistics_ch1_questions_2,
    "linguistics_ch1_3":linguistics_v17_25_questions,
    # Исторический контекст
    "nero":             nero_questions,
    "geography":        geography_questions,
    # Введение
    "intro1":           intro_part1_questions,
    "intro2":           intro_part2_questions,
    "intro3":           intro_part3_questions,
}


def get_pool_by_key(key: str) -> list:
    """Возвращает пул вопросов по строковому ключу.

    Raises KeyError, если ключ не найден — чтобы опечатки не остались
    незамеченными.
    """
    try:
        return _POOLS[key]
    except KeyError:
        raise KeyError(f"Неизвестный pool_key: {key!r}. Доступные: {list(_POOLS)}")


__all__ = [
    # Отдельные списки (нужны в тестах / других модулях)
    "easy_questions", "easy_questions_v17_25",
    "medium_questions", "medium_questions_v17_25",
    "hard_questions", "hard_questions_v17_25",
    "nero_questions", "geography_questions",
    "practical_ch1_questions", "practical_v17_25_questions",
    "linguistics_ch1_questions", "linguistics_ch1_questions_2", "linguistics_v17_25_questions",
    "all_chapter1_questions",
    "intro_part1_questions",
    "intro_part2_questions",
    "intro_part3_questions",
    # Утилиты
    "get_pool_by_key",
]
