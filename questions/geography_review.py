"""Source-reviewed geography corrections."""
from __future__ import annotations
from copy import deepcopy

GEOGRAPHY_OVERRIDES = {
    "geo_03": {
        "question": "Какие пять географических/провинциальных названий перечислены в адресе 1 Пет. 1:1?",
        "options": [
            "Иудея, Самария, Галилея, Сирия, Киликия",
            "Египет, Сирия, Аравия, Крит, Кипр",
            "Македония, Ахаия, Фракия, Иллирик, Италия",
            "Понт, Галатия, Каппадокия, Асия, Вифиния",
        ],
        "correct": 3,
        "explanation": "1 Пет. 1:1 перечисляет Понт, Галатию, Каппадокию, Асию и Вифинию. Формула «пять названий» не предполагает одинаковый административный статус всех пяти.",
        "claim_type": "text", "confidence": "high", "position": "neutral",
        "competitive": False, "sources": ["sblgnt"],
    },
    "geo_07": {
        "question": "Что означает слово «диаспора» в базовом историческом смысле?",
        "options": ["Рассеяние / общины вне родной земли", "Одна римская провинция", "Храмовая должность", "Военный союз"],
        "correct": 0,
        "explanation": "διασπορά означает «рассеяние». Его применение к адресатам 1 Петра — отдельный экзегетический вопрос.",
        "claim_type": "history", "confidence": "high", "position": "neutral",
        "competitive": False, "sources": ["sblgnt", "morphgnt_1peter"],
    },
    "geo_08": {
        "question": "Сколько географических/провинциальных названий перечислено в адресе 1 Пет. 1:1?",
        "options": ["Три", "Четыре", "Пять", "Семь"],
        "correct": 2,
        "explanation": "Пять: Понт, Галатия, Каппадокия, Асия и Вифиния.",
        "claim_type": "text", "confidence": "high", "position": "neutral",
        "competitive": False, "sources": ["sblgnt"],
    },
}

def apply_geography_overrides(pool: list[dict]) -> list[dict]:
    result = []
    for question in pool:
        item = deepcopy(question)
        override = GEOGRAPHY_OVERRIDES.get(str(item.get("id") or "").strip())
        if override:
            item.update(deepcopy(override))
        result.append(item)
    return result
