"""OT and Second Temple background for 1 Peter 3:18-22."""


def _i(qid, q, answer, wrong, exp, verse, sources, confidence, relationship):
    return {"id": qid, "question": q, "options": [answer, *wrong], "correct": 0,
            "explanation": exp, "verse": verse, "topic": "интертекст",
            "claim_type": "interpretation", "confidence": confidence, "position": "neutral",
            "competitive": False, "sources": sources, "relationship": relationship}


INTERTEXT_3_18_22 = [
    _i("ch3_int_001", "Какую связь с Быт. 6-8 безопасно утверждать для 3:20?",
       "Ной, ковчег и вода — явный narrative background, но не оформленная дословная цитата",
       ["3:20 дословно цитирует Быт. 6:1-4", "Связи с Бытием нет", "Пётр прямо цитирует только 1 Еноха"],
       "3:20 прямо называет Ноя и ковчег; Marcar контролирует более широкий flood context.",
       "1 Пет. 3:20; Быт. 6-8", ["sblgnt", "lxx_genesis_6", "marcar_noah_2017"], "high", "explicit_narrative_reference_not_quotation"),
    _i("ch3_int_002", "Как использовать Быт. 6:1-4 в споре о духах?",
       "Как фон fallen-spirit reading вместе с Watchers tradition, не как морфологическое определение πνεύμασιν",
       ["Как словарное определение πνεύμασιν", "Как прямую цитату 3:19", "Как доказательство единственного reading"],
       "Pierce/Grindheim показывают значимость Genesis-6/Watchers tradition; связь остаётся интертекстуальным аргументом.",
       "1 Пет. 3:19-20; Быт. 6:1-4", ["sblgnt", "lxx_genesis_6", "pierce_spirits_2011", "grindheim_spirits_2024"], "contested", "probable_background_for_one_major_reading"),
    _i("ch3_int_003", "Почему 1 Енох 10-14 релевантен fallen-spirit reading?",
       "Там Watchers связываются/удерживаются до суда, а Енох передаёт им судебное сообщение",
       ["1 Пётр называет Еноха автором 3:19", "1 Енох входит в SBLGNT", "1 Енох дословно содержит 1 Пет. 3:19"],
       "1 Енох 10:4-14 описывает заключение; 12:4-14:6 — сообщение Watchers. Это probable background, не явная цитата.",
       "1 Пет. 3:19-20; 1 Енох 10:4-14; 12:4-14:6", ["enoch_10_14_charles", "pierce_spirits_2011", "grindheim_spirits_2024"], "contested", "probable_second_temple_background"),
    _i("ch3_int_004", "Что добавляет Marcar к Noah background 1 Пет. 3-4?",
       "Более широкий Urzeit/Endzeit flood-pattern, а не только вопрос идентичности духов",
       ["Ной не относится к 3:20", "πνεύμασιν морфологически означает людей", "Исследуется только Средневековье"],
       "Marcar показывает риторико-эсхатологическую роль Noah/flood tradition рядом с более узким Enochic спором.",
       "1 Пет. 3:18-4:6", ["sblgnt", "lxx_genesis_6", "marcar_noah_2017"], "medium", "thematic_background"),
    _i("ch3_int_005", "Как связать потоп и крещение в 3:20-21 без перегрузки?",
       "Сам текст вводит ἀντίτυπον и βάπτισμα; точный typological referent и efficacy обсуждаются отдельно",
       ["Бытие прямо называет потоп крещением", "ἀντίτυπον доказывает одну систему", "Типологической связи нет"],
       "Типологический move задан 3:20-21, но его scope и систематика contested.",
       "1 Пет. 3:20-21", ["sblgnt", "lxx_genesis_6", "marcar_noah_2017", "jts_crawford_1p3_21"], "contested", "explicit_typological_move_with_contested_scope"),
]

__all__ = ["INTERTEXT_3_18_22"]
