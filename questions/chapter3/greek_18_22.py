"""MorphGNT-backed Greek questions for 1 Peter 3:18-22."""

BASE = ["sblgnt", "morphgnt_1peter"]

CORRECT_POSITION = {
    "ch3_gr_001": 1,
    "ch3_gr_002": 3,
    "ch3_gr_003": 2,
    "ch3_gr_004": 0,
    "ch3_gr_005": 2,
    "ch3_gr_006": 1,
    "ch3_gr_007": 3,
    "ch3_gr_008": 0,
    "ch3_gr_009": 1,
    "ch3_gr_010": 2,
    "ch3_gr_011": 3,
    "ch3_gr_012": 0,
    "ch3_gr_013": 2,
    "ch3_gr_014": 1,
    "ch3_gr_015": 0,
}


def _pack(qid, answer, wrong):
    correct = CORRECT_POSITION[qid]
    options = list(wrong)
    options.insert(correct, answer)
    return options, correct


def _g(qid, form, verse, answer, wrong, parse, lemma, note=""):
    options, correct = _pack(qid, answer, wrong)
    return {"id": qid, "question": f"Как MorphGNT разбирает {form} в {verse}?",
            "options": options, "correct": correct,
            "explanation": f"{form} = {parse} от {lemma}. {note}".strip(), "verse": verse,
            "topic": "греческая морфология", "claim_type": "greek", "confidence": "high",
            "position": "neutral", "competitive": False, "sources": list(BASE),
            "morphgnt": {"form": form, "parse": parse, "lemma": lemma}}


GREEK_3_18_22 = [
    _g("ch3_gr_001", "ἔπαθεν", "1 Пет. 3:18", "Aor. act. ind., 3 sg.", ["Aor. act. subj.", "Pres. act. ind.", "Aor. pass. ind."], "3AAI-S--", "πάσχω", "Parsing не выбирает модель искупления."),
    _g("ch3_gr_002", "προσαγάγῃ", "1 Пет. 3:18", "Aor. act. subj., 3 sg.", ["Aor. act. ind.", "Pres. act. subj.", "Aor. pass. subj."], "3AAS-S--", "προσάγω", "Стоит после ἵνα; богословский смысл цели — отдельный шаг."),
    _g("ch3_gr_003", "ἐκήρυξεν", "1 Пет. 3:19", "Aor. act. ind., 3 sg.", ["Aor. act. subj.", "Perf. act. ind.", "Aor. pass. ind."], "3AAI-S--", "κηρύσσω", "Форма не определяет адресатов, время или содержание провозглашения."),
    _g("ch3_gr_004", "διεσώθησαν", "1 Пет. 3:20", "Aor. pass. ind., 3 pl.", ["Aor. act. ind.", "Pres. pass. ind.", "Aor. pass. subj."], "3API-P--", "διασῴζω", "Роль воды требует контекста."),
    _g("ch3_gr_005", "ἀντίτυπον", "1 Пет. 3:21", "Adj., nom. neut. sg.", ["Noun, nom. neut. sg.", "Adj., acc. neut. sg.", "Adverb"], "A- ----NSN-", "ἀντίτυπος", "Морфология не доказывает полный typological referent."),
    _g("ch3_gr_006", "ἐπερώτημα", "1 Пет. 3:21", "Noun, nom. neut. sg.", ["Noun, gen. neut. sg.", "Adj., nom. neut. sg.", "Infinitive"], "N- ----NSN-", "ἐπερώτημα", "Appeal/pledge/confession — lexical-history dispute, не parsing fact."),
    _g("ch3_gr_007", "ἅπαξ", "1 Пет. 3:18", "Adverb", ["Adjective", "Preposition", "Noun"], "D- --------", "ἅπαξ", "Единократность в контексте — экзегетический вывод."),
    _g("ch3_gr_008", "θανατωθεὶς", "1 Пет. 3:18", "Aor. pass. ptc., nom. masc. sg.", ["Pres. act. ptc.", "Infinitive", "Aor. act. ind."], "-APPNSM-", "θανατόω", "Параллельно ζῳοποιηθεὶς; функция σαρκί/πνεύματι спорна."),
    _g("ch3_gr_009", "ζῳοποιηθεὶς", "1 Пет. 3:18", "Aor. pass. ptc., nom. masc. sg.", ["Pres. act. ptc.", "Infinitive", "Perf. pass. ind."], "-APPNSM-", "ζῳοποιέω", "Морфология не выбирает reading πνεύματι."),
    _g("ch3_gr_010", "ᾧ", "1 Пет. 3:19", "Relative pronoun, dat. neut. sg.", ["Personal pronoun", "Demonstrative", "Conjunction"], "RR ----DSN-", "ὅς", "Референт ἐν ᾧ требует синтаксической экзегезы."),
    _g("ch3_gr_011", "πνεύμασιν", "1 Пет. 3:19", "Noun, dat. neut. pl.", ["Noun, dat. masc. pl.", "Noun, gen. neut. pl.", "Adjective, dat. neut. pl."], "N- ----DPN-", "πνεῦμα", "Форма не идентифицирует духов; semantic referent требует экзегезы."),
    _g("ch3_gr_012", "ἀπειθήσασίν", "1 Пет. 3:20", "Aor. act. ptc., dat. masc. pl.", ["Pres. act. ptc.", "Aor. pass. ind.", "Infinitive"], "-AAPDPM-", "ἀπειθέω", "Контекст связывает непослушание с днями Ноя."),
    _g("ch3_gr_013", "σῴζει", "1 Пет. 3:21", "Pres. act. ind., 3 sg.", ["Aor. act. ind.", "Pres. pass. subj.", "Perf. mid. inf."], "3PAI-S--", "σῴζω", "Модель baptismal efficacy требует всего 3:21."),
    _g("ch3_gr_014", "ὑποταγέντων", "1 Пет. 3:22", "Aor. pass. ptc., gen. masc. pl.", ["Pres. act. ptc.", "Aor. pass. ind.", "Perf. mid. inf."], "-APPGPM-", "ὑποτάσσω", "Связано с ἀγγέλων, ἐξουσιῶν, δυνάμεων."),
    _g("ch3_gr_015", "πορευθεὶς", "1 Пет. 3:19", "Aor. pass. ptc., nom. masc. sg.", ["Aor. act. ind., 3 sg.", "Pres. mid. ptc.", "Aor. pass. ptc., gen. masc. pl."], "-APPNSM-", "πορεύομαι", "Форма сама по себе не датирует journey и не определяет место назначения."),
]

__all__ = ["GREEK_3_18_22"]
