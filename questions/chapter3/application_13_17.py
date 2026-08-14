"""Pastoral application downstream from text, Greek, and intertext."""

APPLICATION_3_13_17 = [
    {
        "id": "ch3_app_301",
        "question": "Какое применение прямо следует из ἕτοιμοι ἀεί πρὸς ἀπολογίαν?",
        "options": ["Развивать устойчивую готовность внятно объяснить христианскую надежду", "Искать спор с каждым", "Говорить только заученными формулами", "Избегать вопросов о вере"],
        "correct": 0,
        "explanation": "«Всегда готовы» поддерживает устойчивую подготовленность к ответу, но не превращает каждую встречу в обязательный дебат.",
        "verse": "1 Пет. 3:15", "topic": "Application: readiness", "claim_type": "application", "confidence": "high", "position": "pastoral", "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter", "jobes_becnt_1peter_2022"],
    },
    {
        "id": "ch3_app_302",
        "question": "Как использовать 1 Пет. 3:15 в современном курсе апологетики без эпистемического скачка?",
        "options": ["Учить обязанности дать основание надежды, а методы сравнивать отдельно", "Объявить одну школу переводом ἀπολογία", "Игнорировать надежду", "Считать кротость необязательной"],
        "correct": 0,
        "explanation": "ἀπολογία поддерживает идею ответа/защиты; выбор classical, evidential, cumulative-case, presuppositional или иной стратегии требует дополнительных аргументов, а не одной леммы.",
        "verse": "1 Пет. 3:15", "topic": "Application: apologetics methodology", "claim_type": "application", "confidence": "high", "position": "pastoral", "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter", "bdag_3", "jobes_becnt_1peter_2022"],
    },
    {
        "id": "ch3_app_303",
        "question": "Какой стиль ответа соответствует 1 Пет. 3:15-16?",
        "options": ["Внятный ответ с кротостью/почтением и доброй совестью", "Унижение ради победы", "Манипуляция страхом", "Разрыв аргументов и поведения"],
        "correct": 0,
        "explanation": "Пётр связывает λόγος о надежде с πραΰτης, φόβος и доброй совестью; применение оценивает и содержание, и манеру, и честность отвечающего.",
        "verse": "1 Пет. 3:15-16", "topic": "Application: gentleness and conscience", "claim_type": "application", "confidence": "high", "position": "pastoral", "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter", "jobes_becnt_1peter_2022"],
    },
    {
        "id": "ch3_app_304",
        "question": "Какое применение 1 Пет. 3:17 соответствует контрасту стиха?",
        "options": ["Если страдание приходит, следить, чтобы причиной была верность добру, а не собственное злодеяние", "Провоцировать страдание", "Считать последствия своего зла мученичеством", "Избегать добра"],
        "correct": 0,
        "explanation": "Пётр не романтизирует боль: он различает страдание при делании добра и при делании зла. Применение — не искать страдание, но хранить верность добру, если она оказывается дорогостоящей.",
        "verse": "1 Пет. 3:17", "topic": "Application: suffering for good", "claim_type": "application", "confidence": "high", "position": "pastoral", "competitive": False,
        "sources": ["sblgnt", "jobes_becnt_1peter_2022", "achtemeier_hermeneia_1peter"],
    },
]

__all__ = ["APPLICATION_3_13_17"]
