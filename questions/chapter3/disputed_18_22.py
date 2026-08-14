"""Contested interpretation map for 1 Peter 3:18-22; never competitive."""

CORRECT_POSITION = {
    "ch3_disp_001": 3,
    "ch3_disp_002": 1,
    "ch3_disp_003": 0,
    "ch3_disp_004": 2,
    "ch3_disp_005": 3,
    "ch3_disp_006": 1,
}


def _pack(qid, answer, wrong):
    correct = CORRECT_POSITION[qid]
    options = list(wrong)
    options.insert(correct, answer)
    return options, correct


def _d(qid, question, answer, wrong, explanation, verse, sources, readings):
    options, correct = _pack(qid, answer, wrong)
    return {"id": qid, "question": question, "options": options, "correct": correct,
            "explanation": explanation, "verse": verse, "topic": "спорное место",
            "claim_type": "interpretation", "confidence": "contested", "position": "neutral",
            "competitive": False, "sources": sources, "readings": readings}


SPIRITS = [
    "sblgnt",
    "morphgnt_1peter",
    "gty_1p3_18_20",
    "tgc_storms_1p3_18_22",
    "grudem_noah_1p3_19",
    "lei_descensus_2025",
]
BAPTISM = [
    "sblgnt",
    "jts_crawford_1p3_21",
    "ubs_handbook_1p3_21",
    "gty_1p3_20_22",
    "tgc_storms_1p3_18_22",
]

DISPUTED_3_18_22 = [
    _d(
        "ch3_disp_001",
        "Почему πνεύμασιν в 3:19 не позволяет одной морфологией выбрать идентичность «духов в темнице»?",
        "MorphGNT даёт πνεῦμα, dat. neut. pl.; fallen-spirit, Christ-through-Noah и descensus/human-dead readings требуют дополнительных синтаксических, интертекстуальных и reception arguments",
        [
            "Поскольку πνεύματα в ряде NT-контекстов обозначает сверхъестественных существ, форма dat. neut. pl. делает fallen-spirit reading грамматически обязательным",
            "Связка ἀπειθήσασίν с днями Ноя сама грамматически идентифицирует духов как человеческих современников Ноя и тем самым доказывает Christ-through-Noah reading",
            "Ранняя descensus reception позволяет считать human-dead reading самим лексическим значением πνεύμασιν в этом стихе",
        ],
        "Inspected passage sections у MacArthur и Storms представляют fallen-spirit/victory readings; inspected sections авторского PDF Grudem — Christ-through-Noah. Publisher synopsis Lei подтверждает историческую значимость descensus reception и её переоценку, но не превращает human-dead reading в morphology fact.",
        "1 Пет. 3:19-20",
        SPIRITS,
        ["fallen_spirits_watchers", "christ_through_noah", "human_dead_descensus_reception"],
    ),
    _d(
        "ch3_disp_002",
        "Что можно заключить о времени и содержании ἐκήρυξεν в 3:19 без превращения tense-form в экзегезу?",
        "Серьёзные чтения помещают провозглашение после смерти/воскресения Христа, во дни Ноя через Ноя или в descensus; ἐκήρυξεν само по себе не решает время, адресатов и содержание",
        [
            "Аорист ἐκήρυξεν вместе с πορευθεὶς требует последовательного post-resurrection journey и поэтому доказывает victory proclamation fallen spirits",
            "Обычное употребление κηρύσσω как публичного провозглашения требует понимать содержание как предложение спасения человеческим умершим",
            "Временная клауза о днях Ноя делает проповедь Христа через Ноя во время строительства ковчега единственной допустимой хронологией",
        ],
        "MacArthur/Storms и Grudem дают реально inspected passage-level конкурирующие хронологии; Lei используется только в пределах publisher synopsis для descensus-reception family. Tense-form ἐκήρυξεν сам спор не закрывает.",
        "1 Пет. 3:19-20",
        SPIRITS,
        ["post_resurrection_victory", "pre_flood_christ_through_noah", "descensus_human_dead_reception"],
    ),
    _d(
        "ch3_disp_003",
        "Что показывает lexical-history review ἐπερώτημα в 3:21?",
        "Parsing noun nom. neut. sg. надёжен; appeal/request, pledge/stipulation и confession-related baptismal-response analyses нужно различать, а не превращать в parsing fact",
        [
            "Юридические употребления ἐπερώτημα делают pledge/stipulation единственным лексическим значением слова в 3:21",
            "Фраза εἰς θεόν и «добрая совесть» синтаксически требуют appeal/request и тем самым исключают pledge reading",
            "Ранняя связь крещения с ὁμολογία означает, что ἐπερώτημα само по себе лексически значит «исповедание»",
        ],
        "Inspected LSJ entry и passage-level UBS handbook сохраняют несколько lexical/translation possibilities. Publisher abstract Crawford сообщает его contractual/pledge и confession-related argument; full article не считается inspected, поэтому это не lexical certainty.",
        "1 Пет. 3:21",
        ["sblgnt", "morphgnt_1peter", "lsj_eperotema", "ubs_handbook_1p3_21", "jts_crawford_1p3_21"],
        ["appeal_request", "pledge_stipulation", "confession_response_related"],
    ),
    _d(
        "ch3_disp_004",
        "Как представить спор вокруг βάπτισμα ... νῦν σῴζει в 3:21 без lexical fiat?",
        "Сохранить «крещение спасает», отрицание простого внешнего омовения, good-conscience ἐπερώτημα и воскресение; затем различать sacramental и evangelical/faith-confessional модели",
        [
            "Связка βάπτισμα ... σῴζει сама по себе устанавливает механическую эффективность воды независимо от последующих квалификаций",
            "Оборот οὐ σαρκὸς ἀπόθεσις ῥύπου сам по себе доказывает чисто символическое крещение и исключает любую инструментальную роль",
            "Фраза δι᾽ ἀναστάσεως Ἰησοῦ Χριστοῦ переносит спасительную функцию полностью с βάπτισμα на воскресение, поэтому σῴζει нужно понимать нереференциально",
        ],
        "SBLGNT задаёт сильную формулировку и её квалификации. Inspected UBS section сохраняет competing translation lines; Crawford abstract подтверждает реальную спорность и сообщает contractual proposal; inspected GTY/Storms sections дают evangelical readings. Карточка сохраняет systematic families, а не объявляет одну из них грамматически доказанной.",
        "1 Пет. 3:21",
        BAPTISM,
        ["sacramental_efficacy", "faith_appeal_or_pledge_instrumentality", "sign_confession_resurrection_relation"],
    ),
    _d(
        "ch3_disp_005",
        "Почему θανατωθεὶς μὲν σαρκί / ζῳοποιηθεὶς δὲ πνεύματι требует экзегезы после parsing?",
        "Причастия параллельны, но функция дативов спорна: sphere/mode contrast и Holy-Spirit-agency readings нельзя выбрать одной морфологией",
        [
            "Пара дативов обязана обозначать две онтологические части Христа — тело и бестелесный человеческий дух — поэтому human-spirit reading следует прямо из падежа",
            "Пассивное ζῳοποιηθεὶς требует выраженного агента, поэтому πνεύματι грамматически обязательно означает Святого Духа как agent",
            "Связка μέν ... δέ задаёт прежде всего временную последовательность двух событий и тем самым снимает спор о функции σαρκί / πνεύματι",
        ],
        "MorphGNT фиксирует оба причастия как APPNSM и оба существительных как дативы. Inspected MacArthur и Storms sections демонстрируют, что переход от parsing к смыслу πνεύματι действительно является интерпретационным шагом.",
        "1 Пет. 3:18",
        ["sblgnt", "morphgnt_1peter", "gty_1p3_18_20", "tgc_storms_1p3_18_22"],
        ["sphere_or_mode_contrast", "holy_spirit_agency"],
    ),
    _d(
        "ch3_disp_006",
        "Как обращаться с ἐν ᾧ в начале 3:19?",
        "Зафиксировать ᾧ как relative pronoun dat. neut. sg., а референт и хронологию аргументировать отдельно",
        [
            "Совпадение рода/числа/падежа с πνεύματι делает πνεύματι обязательным antecedent и тем самым уже решает способ и время провозглашения",
            "ἐν ᾧ следует читать как чисто временное «когда», поэтому вопрос antecedent и связи с предыдущей клаузой грамматически исчезает",
            "Следующая клауза о днях Ноя ретроспективно задаёт значение ἐν ᾧ как «во дни Ноя», так что связь с πνεύματι учитывать не нужно",
        ],
        "Форма RR ----DSN- надёжна. Разные связи с предыдущей клаузой представлены в inspected Grudem/MacArthur/Storms material; morphology сама antecedent/chronology не выбирает.",
        "1 Пет. 3:19",
        ["sblgnt", "morphgnt_1peter", "grudem_noah_1p3_19", "gty_1p3_18_20", "tgc_storms_1p3_18_22"],
        ["post_resurrection_connection", "spirit_mediated_noah_connection"],
    ),
]

__all__ = ["DISPUTED_3_18_22"]
