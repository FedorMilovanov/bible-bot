"""Contested interpretation map for 1 Peter 3:18-22; never competitive."""


def _d(qid, question, answer, wrong, explanation, verse, sources, readings):
    return {"id": qid, "question": question, "options": [answer, *wrong], "correct": 0,
            "explanation": explanation, "verse": verse, "topic": "спорное место",
            "claim_type": "interpretation", "confidence": "contested", "position": "neutral",
            "competitive": False, "sources": sources, "readings": readings}


SPIRITS = ["sblgnt", "morphgnt_1peter", "gty_1p3_18_20", "grudem_noah_1p3_19", "pierce_spirits_2011", "grindheim_spirits_2024", "lei_descensus_2025", "horrell_williams_icc_v2"]
BAPTISM = ["sblgnt", "jts_crawford_1p3_21", "westfall_baptism_1999", "elliott_1peter_ayb", "davids_1peter_nicnt", "schreiner_1peter_nac", "gty_1p3_20_22", "horrell_williams_icc_v2"]

DISPUTED_3_18_22 = [
    _d(
        "ch3_disp_001",
        "Почему πνεύμασιν в 3:19 не доказывает одну идентификацию «духов в темнице»?",
        "MorphGNT даёт лишь πνεῦμα, dat. neut. pl.; fallen-spirit, Christ-through-Noah и human-dead/descensus чтения требуют экзегезы",
        ["Род автоматически доказывает ангелов", "MorphGNT помечает форму как людей Ноя", "В тексте нет πνεύμασιν"],
        "MacArthur/Pierce/Grindheim, Grudem и Lei представляют разные серьёзные чтения; morphology не выбирает между ними.",
        "1 Пет. 3:19-20",
        SPIRITS,
        ["fallen_spirits_watchers", "christ_through_noah", "human_dead_descensus"],
    ),
    _d(
        "ch3_disp_002",
        "Что честно сказать о времени и содержании ἐκήρυξεν в 3:19?",
        "Серьёзные чтения помещают провозглашение после Христовой смерти/воскресения, во дни Ноя через Ноя или в descensus; форма ἐκήρυξεν сама спор не решает",
        ["Аорист означает только посмертную евангелизацию", "κηρύσσω означает только приговор ангелам", "Форма доказывает время строительства ковчега"],
        "Адресаты, время и содержание выводятся из контекста, не из tense-form.",
        "1 Пет. 3:19-20",
        SPIRITS,
        ["post_resurrection_victory", "pre_flood_christ_through_noah", "descensus_human_dead"],
    ),
    _d(
        "ch3_disp_003",
        "Что показывает lexical-history review ἐπερώτημα в 3:21?",
        "Parsing noun nom. neut. sg. надёжен; appeal/request, pledge/stipulation и confession/response — конкурирующие лексико-исторические решения",
        ["Именительный падеж доказывает «обет»", "Средний род доказывает «просьба»", "У слова нет внебиблейской истории"],
        "LSJ знает question и legal stipulatio; Crawford связывает раннюю baptismal reception с ὁμολογία/pledge. Один translation choice не является parsing fact.",
        "1 Пет. 3:21",
        ["sblgnt", "morphgnt_1peter", "lsj_eperotema", "jts_crawford_1p3_21", "elliott_1peter_ayb", "horrell_williams_icc_v2"],
        ["appeal_request", "pledge_stipulation", "confession_response_related"],
    ),
    _d(
        "ch3_disp_004",
        "Как представить спор вокруг βάπτισμα ... νῦν σῴζει в 3:21?",
        "Сохранить «крещение спасает», отрицание внешнего омовения, good-conscience ἐπερώτημα и воскресение; затем различать sacramental и evangelical/faith-confessional модели",
        ["σῴζει доказывает механическую эффективность воды", "βάπτισμα здесь не означает крещение", "οὐ ... ἀλλὰ отменяет σῴζει"],
        "σῴζει читается вместе с квалификациями; Crawford/Westfall и комментарии подтверждают реальный систематический спор.",
        "1 Пет. 3:21",
        BAPTISM,
        ["sacramental_efficacy", "faith_appeal_or_pledge_instrumentality", "sign_confession_resurrection_relation"],
    ),
    _d(
        "ch3_disp_005",
        "Почему θανατωθεὶς μὲν σαρκί / ζῳοποιηθεὶς δὲ πνεύματι требует экзегезы после parsing?",
        "Причастия параллельны, но функция дативов спорна: sphere/mode contrast и Holy-Spirit-agency readings нельзя выбрать одной морфологией",
        ["πνεύματι — глагол", "μέν ... δέ требует двух субъектов", "σαρκί и πνεύματι стоят в разных падежах"],
        "MorphGNT фиксирует оба причастия как APPNSM и оба существительных как дативы; синтаксико-богословский вывод требует комментариев.",
        "1 Пет. 3:18",
        ["sblgnt", "morphgnt_1peter", "davids_1peter_nicnt", "schreiner_1peter_nac", "elliott_1peter_ayb", "horrell_williams_icc_v2"],
        ["sphere_or_mode_contrast", "holy_spirit_agency"],
    ),
    _d(
        "ch3_disp_006",
        "Как обращаться с ἐν ᾧ в начале 3:19?",
        "Зафиксировать ᾧ как relative pronoun dat. neut. sg., а референт и хронологию аргументировать отдельно",
        ["Дательный автоматически означает дни Ноя", "ᾧ прямо означает Ноя", "Фразу можно игнорировать как вставку"],
        "Форма RR ----DSN- надёжна; связь с πνεύματι/предыдущей клаузой остаётся синтаксико-экзегетическим вопросом.",
        "1 Пет. 3:19",
        ["sblgnt", "morphgnt_1peter", "grudem_noah_1p3_19", "pierce_spirits_2011", "horrell_williams_icc_v2"],
        ["post_resurrection_connection", "spirit_mediated_noah_connection"],
    ),
]

__all__ = ["DISPUTED_3_18_22"]
