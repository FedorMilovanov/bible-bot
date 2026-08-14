"""Theology and explicitly disputed observations for 1 Peter 3:13-17."""

DISPUTED_3_13_17 = [
    {
        "id": "ch3_disp_301",
        "question": "Что честно сказать о синтаксисе κύριον δὲ τὸν Χριστόν?",
        "options": ["Обсуждаются «Христа как Господа» и «Господа — Христа»; морфология спор не закрывает", "Морфология доказывает одну догматическую систему", "κύριον в другом предложении", "Χριστόν — dative"],
        "correct": 0,
        "explanation": "κύριον и τὸν Χριστόν — accusative masculine singular, но точная синтаксическая метка обсуждается. Христологическую значимость следует аргументировать интертекстом Ис. 8, а не выдавать morphology code за богословский вывод.",
        "verse": "1 Пет. 3:15", "topic": "Disputed syntax", "claim_type": "interpretation", "confidence": "contested", "position": "neutral", "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter", "jobes_becnt_1peter_2022", "achtemeier_hermeneia_1peter"],
    },
]

THEOLOGY_3_13_17 = [
    {
        "id": "ch3_theol_301",
        "question": "В чём христологическая значимость связи 1 Пет. 3:15 с Ис. 8:13?",
        "options": ["Пётр применяет исайевскую формулу освящения Господа к Христу как Господу; это интертекстуальный христологический вывод, не морфология", "Она доказывается окончанием -ον", "Исаия не используется", "Христос назван ангелом"],
        "correct": 0,
        "explanation": "Переход от LXX κύριον αὐτὸν ἁγιάσατε к κύριον δὲ τὸν Χριστὸν ἁγιάσατε — сильное применение Lord-language Ис. 8 к Христу. Курс принимает этот вывод как богословско-интертекстуальный и не смешивает его с parsing.",
        "verse": "1 Пет. 3:15; Ис. 8:13 LXX", "topic": "Theology: Christ and Isaiah 8", "claim_type": "theology", "confidence": "medium", "position": "project", "competitive": False,
        "sources": ["sblgnt", "septuagint_bible", "scriptura_vanrensburg_moyise_1p3", "verbum_moyise_2005_1p3", "jobes_becnt_1peter_2022", "achtemeier_hermeneia_1peter"],
    },
    {
        "id": "ch3_theol_302",
        "question": "Что 1 Пет. 3:15 утверждает об ἀπολογία и чего не утверждает?",
        "options": ["Требует готовности дать спрашивающему защиту/ответ о надежде, но не выбирает одну современную apologetics methodology", "Предписывает только classical apologetics", "Предписывает только presuppositionalism", "Запрещает разумный ответ"],
        "correct": 0,
        "explanation": "Лексический факт: ἀπολογία — защита/ответ. Синтаксически адресат — каждый спрашивающий, содержание связано с надеждой. Обязанность объяснять надежду — богословское следствие; выбор позднейшей школы требует дополнительных аргументов.",
        "verse": "1 Пет. 3:15", "topic": "Theology: scope of apologia", "claim_type": "theology", "confidence": "high", "position": "neutral", "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter", "bdag_3", "jobes_becnt_1peter_2022"],
    },
    {
        "id": "ch3_theol_303",
        "question": "Почему «защиту» нельзя отделить от характера отвечающего?",
        "options": ["Ответ соседствует с πραΰτης / φόβος и доброй совестью", "Нужно выиграть любой спор", "Совесть не нужна", "Кротость запрещена"],
        "correct": 0,
        "explanation": "Пётр ставит рядом содержание ответа, его манеру и συνείδησιν ... ἀγαθήν. Аргументативная компетентность и этическая целостность поэтому не должны разводиться.",
        "verse": "1 Пет. 3:15-16", "topic": "Theology: witness and character", "claim_type": "theology", "confidence": "high", "position": "neutral", "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter", "jobes_becnt_1peter_2022"],
    },
    {
        "id": "ch3_theol_304",
        "question": "Как осторожно сформулировать связь страдания и Божьей воли в 3:17?",
        "options": ["Пётр условно допускает страдание при делании добра и считает его лучше страдания за зло; стих не делает зло гонителя морально добрым", "Всякое зло гонителя становится добром", "Праведный не страдает", "Нужно искать страдание"],
        "correct": 0,
        "explanation": "εἰ θέλοι τὸ θέλημα τοῦ θεοῦ содержит optative θέλοι и условную рамку. Текст поддерживает верность добру под страданием, но не даёт основания стирать моральную ответственность причиняющего зло.",
        "verse": "1 Пет. 3:17", "topic": "Theology: suffering and God's will", "claim_type": "theology", "confidence": "medium", "position": "neutral", "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter", "jobes_becnt_1peter_2022", "achtemeier_hermeneia_1peter"],
    },
]

__all__ = ["DISPUTED_3_13_17", "THEOLOGY_3_13_17"]
