"""Theology layer for 1 Peter 3:18-22. Project claims are explicit."""

CORRECT_POSITION = {
    "ch3_theol_001": 0,
    "ch3_theol_002": 3,
    "ch3_theol_003": 1,
    "ch3_theol_004": 2,
    "ch3_theol_005": 0,
}


def _pack(qid, answer, wrong):
    correct = CORRECT_POSITION[qid]
    options = list(wrong)
    options.insert(correct, answer)
    return options, correct


def _h(qid, q, answer, wrong, exp, verse, sources, position="neutral", confidence="high"):
    options, correct = _pack(qid, answer, wrong)
    return {"id": qid, "question": q, "options": options, "correct": correct,
            "explanation": exp, "verse": verse, "topic": "богословие",
            "claim_type": "interpretation", "confidence": confidence, "position": position,
            "competitive": False, "sources": sources}


THEOLOGY_3_18_22 = [
    _h("ch3_theol_001", "Как project-евангельское чтение синтезирует περὶ ἁμαρτιῶν, δίκαιος ὑπὲρ ἀδίκων и προσαγάγῃ в 3:18?",
       "Как заместительное и примиряющее страдание Христа за неправедных, чтобы привести их к Богу",
       [
           "Как прежде всего нравственный пример праведного страдальца; заместительную функцию δίκαιος ὑπὲρ ἀδίκων проект не утверждает",
           "Как культовую жертву περὶ ἁμαρτιῶν, но без вывода о заместительном отношении Христа к ἀδίκων",
           "Как утверждение доступа к Богу через προσαγάγῃ, при котором механизм связи страдания Христа с грешниками оставляется полностью неопределённым",
       ],
       "Это project synthesis всего предложения, а не lexical claim об одном ὑπέρ. Relevant sections MacArthur и Storms реально inspected и дают два независимых evangelical passage witnesses.",
       "1 Пет. 3:18", ["sblgnt", "gty_1p3_18", "tgc_storms_1p3_18_22"], "project", "medium"),
    _h("ch3_theol_002", "Какой resurrection/exaltation context остаётся общим даже при споре о πνεύματι в 3:18?",
       "Пассаж движется от страдания/оживотворения к явной ἀνάστασις в 3:21 и небесному возвышению с подчинением сил в 3:22",
       [
           "Контекст воскресения зависит от того, будет ли πνεύματι сначала доказано как Святой Дух; без этого 3:21 нельзя использовать для 3:18-22",
           "3:21 говорит о воскресении, но 3:22 относится к отдельной теме ангельских сил и не выполняет функции vindication/exaltation",
           "Небесная сессия 3:22 сама по себе определяет πνεύματι в 3:18 как воскресшее человеческое состояние и закрывает синтаксический спор",
       ],
       "3:21 прямо называет воскресение Иисуса Христа, 3:22 — небо, правую руку Бога и подчинённые силы. Inspected GTY/Storms passages reinforce this contextual trajectory without making πνεύματι morphology do more than it can.",
       "1 Пет. 3:18-22", ["sblgnt", "gty_1p3_20_22", "tgc_storms_1p3_18_22"]),
    _h("ch3_theol_003", "Как project-евангельский guardrail читает βάπτισμα ... σῴζει, не решая HOLD-BAPTISM-SYSTEMATICS?",
       "Не ослаблять σῴζει, не сводить спасение к автоматизму внешней воды и читать всю конструкцию с good-conscience ἐπερώτημα и воскресением Христа",
       [
           "Принять sacramental-instrumental reading как уже доказанную системой грамматики и снять дальнейший систематический спор",
           "Перефразировать σῴζει как чисто символическое свидетельство веры, чтобы заранее исключить любую инструментальную роль крещения",
           "Сначала выбрать единственный перевод ἐπερώτημα как «обет» или «просьба» и затем вывести из этой глоссы полную baptismal systematics",
       ],
       "Это project guardrail, не parsing fact и не owner-level denominational formula. Relevant sections GTY и Storms дают два inspected evangelical witnesses; inspected UBS section и bounded Crawford abstract сохраняют lexical/translation dispute.",
       "1 Пет. 3:21", ["sblgnt", "gty_1p3_20_22", "tgc_storms_1p3_18_22", "ubs_handbook_1p3_21", "jts_crawford_1p3_21"], "project", "contested"),
    _h("ch3_theol_004", "Как 3:22 завершает triumph/vindication argument, не используя его как shortcut для 3:19?",
       "Небесной сессией Христа и подчинением Ему ангелов, властей и сил; этот финал утверждает Его победу, но сам не идентифицирует духов 3:19",
       [
           "Повторением πορευθεὶς, которое делает 3:22 хронологическим ключом и доказывает, что 3:19 описывает только post-ascension proclamation",
           "Прямым отождествлением ἀγγέλων, ἐξουσιῶν и δυνάμεων из 3:22 с πνεύμασιν ἐν φυλακῇ из 3:19, тем самым закрывая спор об адресатах",
           "Переходом к общей ангелологии, не связанной с vindication страдающего Христа и поэтому не выполняющей завершающей риторической функции",
       ],
       "Финал 3:22 делает vindication/triumph явной темой, не требуя сначала решить идентичность духов 3:19. GTY и Storms relevant sections были inspected на этом passage-level claim.",
       "1 Пет. 3:22", ["sblgnt", "gty_1p3_20_22", "tgc_storms_1p3_18_22"]),
    _h("ch3_theol_005", "Почему triumph theme 3:18-22 не следует превращать в обещание отсутствия страдания?",
       "Контекст допускает страдание за добро; Христов путь соединяет реальное страдание с последующим оправданием/победой",
       [
           "Noah/flood pattern позволяет ожидать для верных прежде всего временного избавления от страдания ещё до эсхатологической vindication",
           "Поскольку vindication Христа уникальна, она не даёт читателям никакого образца надежды и не связана с их страданием за добро",
           "Подчинение небесных сил в 3:22 сдвигает акцент к немедленной социальной победе верующих над противниками как главной форме triumph",
       ],
       "Project application следует траектории suffering-to-vindication, а не theology of immediate success. MacArthur и Storms passage sections дают два реально inspected evangelical witnesses.",
       "1 Пет. 3:17-22", ["sblgnt", "gty_1p3_18", "gty_1p3_20_22", "tgc_storms_1p3_18_22"], "project", "medium"),
]

__all__ = ["THEOLOGY_3_18_22"]
