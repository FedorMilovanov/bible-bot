"""Theology layer for 1 Peter 3:18-22. Project claims are explicit."""


def _h(qid, q, answer, wrong, exp, verse, sources, position="neutral", confidence="high"):
    return {"id": qid, "question": q, "options": [answer, *wrong], "correct": 0,
            "explanation": exp, "verse": verse, "topic": "богословие",
            "claim_type": "interpretation", "confidence": confidence, "position": position,
            "competitive": False, "sources": sources}


THEOLOGY_3_18_22 = [
    _h("ch3_theol_001", "Как project-евангельское чтение синтезирует περὶ ἁμαρτιῶν, δίκαιος ὑπὲρ ἀδίκων и προσαγάγῃ в 3:18?",
       "Как заместительное и примиряющее страдание Христа за неправедных, чтобы привести их к Богу",
       ["ὑπέρ один сам доказывает полную теорию искупления", "Смерть не связана с грехами", "Только нравственный пример без цели привести к Богу"],
       "Это synthesis всего предложения, не lexical claim об одном ὑπέρ; MacArthur плюс Davids/Schreiner дают project-side quorum.",
       "1 Пет. 3:18", ["sblgnt", "gty_1p3_18", "davids_1peter_nicnt", "schreiner_1peter_nac"], "project", "medium"),
    _h("ch3_theol_002", "Какой resurrection-context остаётся даже при споре о πνεύματι?",
       "Пассаж идёт от смерти/оживотворения к ἀνάστασις в 3:21 и небесному возвышению в 3:22",
       ["Воскресение не упомянуто", "πνεύματι один доказывает все детали воскресшего тела", "3:22 ставит Христа под ангелами"],
       "3:21 прямо называет воскресение Иисуса Христа, 3:22 — небо, правую руку Бога и подчинённые силы.",
       "1 Пет. 3:18-22", ["sblgnt", "davids_1peter_nicnt", "elliott_1peter_ayb", "schreiner_1peter_nac"]),
    _h("ch3_theol_003", "Как project-евангельское чтение формулирует βάπτισμα ... σῴζει?",
       "Не ослаблять σῴζει, но отрицать автоматизм внешней воды и читать стих с good-conscience ἐπερώτημα и воскресением Христа",
       ["Переписать как «крещение не спасает»", "Вода действует механически независимо от веры", "Удалить спорное ἐπερώτημα"],
       "Это project reading, не parsing fact. MacArthur/Davids/Schreiner дают евангельскую сторону; Crawford/Elliott удерживают сложность спора.",
       "1 Пет. 3:21", ["sblgnt", "gty_1p3_20_22", "davids_1peter_nicnt", "schreiner_1peter_nac", "jts_crawford_1p3_21", "elliott_1peter_ayb"], "project", "contested"),
    _h("ch3_theol_004", "Как 3:22 завершает triumph argument?",
       "Небесной сессией Христа и подчинением Ему ангелов, властей и сил",
       ["Христос уступает власть ангелам", "Пётр возвращается к ковчегу", "Текст не утверждает власть над силами"],
       "Финал 3:22 делает vindication/triumph явной темой, не требуя сначала решить идентичность духов 3:19.",
       "1 Пет. 3:22", ["sblgnt", "grindheim_spirits_2024", "gty_1p3_20_22", "davids_1peter_nicnt"]),
    _h("ch3_theol_005", "Почему triumph theme не обещает отсутствие страдания?",
       "Контекст допускает страдание за добро; Христов путь соединяет реальное страдание с последующим оправданием/победой",
       ["Христос не страдал", "3:22 отменяет 3:17", "Победа означает только политический успех"],
       "Project application следует траектории suffering-to-vindication, а не theology of immediate success.",
       "1 Пет. 3:17-22", ["sblgnt", "gty_1p3_18", "gty_1p3_20_22", "schreiner_1peter_nac"], "project", "medium"),
]

__all__ = ["THEOLOGY_3_18_22"]
