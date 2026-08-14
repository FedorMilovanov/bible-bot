"""OT and Second Temple background for 1 Peter 3:18-22."""

CORRECT_POSITION = {
    "ch3_ot_001": 2,
    "ch3_ot_002": 0,
    "ch3_ot_003": 3,
    "ch3_ot_004": 1,
    "ch3_ot_005": 2,
}


def _pack(qid, answer, wrong):
    correct = CORRECT_POSITION[qid]
    options = list(wrong)
    options.insert(correct, answer)
    return options, correct


def _i(qid, q, answer, wrong, exp, verse, sources, confidence, relationship):
    options, correct = _pack(qid, answer, wrong)
    return {"id": qid, "question": q, "options": options, "correct": correct,
            "explanation": exp, "verse": verse, "topic": "интертекст",
            "claim_type": "interpretation", "confidence": confidence, "position": "neutral",
            "competitive": False, "sources": sources, "relationship": relationship}


INTERTEXT_3_18_22 = [
    _i("ch3_ot_001", "Как точнее классифицировать связь 1 Пет. 3:20 с Быт. 6-8?",
       "Это явная ссылка на повествование о Ное, ковчеге и воде, но не оформленная цитата конкретного предложения LXX",
       [
           "Это формальная цитата Быт. 6:1-4, где Пётр воспроизводит конкретную LXX-формулировку",
           "Это только аллюзия на Быт. 6:1-4 о «сына́х Божиих»; Ной, ковчег и вода не задают самостоятельной narrative связи",
           "Это типологическое упоминание Ноя, которое можно объяснить без обращения к повествованию Быт. 6-8 как текстовому фону",
       ],
       "3:20 прямо называет Ноя и ковчег и упоминает воду. LXX Genesis provides primary background; publisher abstract Marcar supports the broader Noah/flood framing, not a claim of formal quotation.",
       "1 Пет. 3:20; Быт. 6-8", ["sblgnt", "lxx_genesis_6", "marcar_noah_2017"], "high", "explicit_narrative_reference_not_quotation"),
    _i("ch3_ot_002", "Как Быт. 6:1-4 допустимо использовать в споре о «духах в темнице»?",
       "Как часть фона fallen-spirit/Watchers reading вместе с Second Temple tradition, но не как лексическое определение πνεύμασιν",
       [
           "Быт. 6 сам по себе грамматически идентифицирует πνεύμασιν как падших ангелов независимо от синтаксиса 1 Петра",
           "Быт. 6 релевантен только для датировки «дней Ноя» и не должен участвовать в обсуждении идентичности духов",
           "Связка Быт. 6 + Watchers tradition делает Christ-through-Noah reading филологически невозможным ещё до анализа 1 Пет. 3:19-20",
       ],
       "Genesis 6 is inspected primary background. Publisher synopsis Pierce establishes only that Watchers/1 Enoch and related early-Jewish punishment traditions are a scholarly background line for this passage; it does not make that line morphology or the only viable reading.",
       "1 Пет. 3:19-20; Быт. 6:1-4", ["sblgnt", "lxx_genesis_6", "pierce_spirits_2011"], "contested", "probable_background_for_one_major_reading"),
    _i("ch3_ot_003", "Как корректнее описать значение 1 Енох 10-14 для чтения 1 Пет. 3:19-20?",
       "Как конкретного свидетеля shared/probable Watchers background внутри более широкой Second Temple tradition; прямую литературную зависимость 1 Петра это не доказывает",
       [
           "Как доказанную прямую литературную зависимость: 1 Пётр использовал именно 1 Енох 10-14 как идентифицируемый письменный источник",
           "Как нерелевантный поздний материал: для интерпретации следует ограничиться только Быт. 6 и исключить Watchers tradition",
           "Как только общий Second Temple фон без специфической ценности мотивов заключения и суда в 1 Енох; конкретные параллели учитывать не следует",
       ],
       "Bounded Charles translation gives the inspected confinement/judgment/message motifs but is not a critical edition. Pierce's publisher synopsis confirms Watchers/1 Enoch as a scholarly background line, while the inspected Storms section applies a Genesis-6 fallen-spirit reading. None of these establishes direct literary dependence.",
       "1 Пет. 3:19-20; 1 Енох 10:4-14; 12:4-14:6", ["enoch_10_14_charles", "pierce_spirits_2011", "tgc_storms_1p3_18_22"], "contested", "probable_second_temple_background"),
    _i("ch3_ot_004", "Что в исследовании Marcar важно для Noah/flood background 1 Пет. 3-4?",
       "Более широкий Urzeit/Endzeit flood-pattern в аргументе 1 Петра, а не только решение вопроса о том, кто такие духи",
       [
           "Главным образом доказательство того, что πνεύμασιν в 3:19 обозначает fallen spirits",
           "Сведение Noah tradition почти исключительно к baptismal typology 3:21 без более широкого эсхатологического контекста 3-4 глав",
           "Аргумент в пользу того, что 3:19 описывает только проповедь Христа через Ноя во время строительства ковчега",
       ],
       "The inspected Cambridge abstract states Marcar's broader Urzeit/Endzeit Noah/flood framing across 1 Peter 3-4. The card stays inside that abstract-level claim and does not attribute uninspected article details.",
       "1 Пет. 3:18-4:6", ["sblgnt", "lxx_genesis_6", "marcar_noah_2017"], "medium", "thematic_background"),
    _i("ch3_ot_005", "Как описать типологическую связь потопа и крещения в 3:20-21 без систематического overclaim?",
       "Сам текст делает typological move через ἀντίτυπον и βάπτισμα; точный scope референта и baptismal efficacy требуют отдельной экзегезы",
       [
           "ἀντίτυπον делает именно воду единственным референтом типологии и тем самым уже решает вопрос baptismal efficacy",
           "Типология относится только к ковчегу как образу церкви, поэтому вода/потоп не участвуют в связи с βάπτισμα",
           "Поскольку βάπτισμα названо ἀντίτυπον, детали Noah/flood narrative больше не ограничивают смысл 3:21",
       ],
       "The typological move is direct text. The inspected UBS 3:21 section and Storms passage commentary show that translation/exegetical questions remain after recognizing it; neither morphology nor ἀντίτυπον alone supplies a complete baptismal system.",
       "1 Пет. 3:20-21", ["sblgnt", "lxx_genesis_6", "ubs_handbook_1p3_21", "tgc_storms_1p3_18_22"], "contested", "explicit_typological_move_with_contested_scope"),
]

__all__ = ["INTERTEXT_3_18_22"]
