"""Canonical content-truth layer for the legacy question bank.

The historical data files remain useful as a large authoring corpus, but production must
consume questions through this module.  Corrections, epistemic status, provenance and
ranking eligibility therefore live at one explicit boundary instead of being scattered
through Telegram/Mini App handlers.
"""
from __future__ import annotations

from copy import deepcopy


SOURCE_CATALOG = {
    "sblgnt": {
        "title": "SBL Greek New Testament",
        "url": "https://www.sblgnt.com/",
        "kind": "primary_text",
    },
    "morphgnt_1peter": {
        "title": "MorphGNT / SBLGNT morphology: 1 Peter",
        "url": "https://github.com/morphgnt/sblgnt/blob/master/81-1Pe-morphgnt.txt",
        "kind": "primary_text_morphology",
    },
    "oxford_1peter_contested": {
        "title": "Duane F. Watson, '1 Peter: Contested Issues', Oxford Handbook (2024)",
        "url": "https://academic.oup.com/edited-volume/57514/chapter-abstract/467936100",
        "kind": "scholarship",
    },
    "oxford_ephesus": {
        "title": "Ephesus, Oxford Classical Dictionary",
        "url": "https://academic.oup.com/edited-volume/61673/chapter-abstract/548937531",
        "kind": "reference",
    },
    "tacitus_annals_15_44": {
        "title": "Tacitus, Annals 15.44",
        "url": "https://www.perseus.tufts.edu/hopper/text?doc=Tac.+Ann.+15.44",
        "kind": "primary_source",
    },
    "pliny_10_96_97": {
        "title": "Pliny the Younger / Trajan, Letters 10.96-97",
        "url": "https://www.attalus.org/pliny/ep10b.html",
        "kind": "primary_source",
    },
    "cambridge_agrammatoi": {
        "title": "Thomas J. Kraus, Acts 4:13, New Testament Studies 45 (1999)",
        "url": "https://www.cambridge.org/core/journals/new-testament-studies/article/abs/uneducated-ignorant-or-even-illiterate-aspects-and-background-for-an-understanding-of-apammatoi-and-iitai-in-acts-413/33DA1E01344A050F7FE78C3F9C16F96C",
        "kind": "scholarship",
    },
    "cambridge_polycarp": {
        "title": "Cambridge Companion to the Apostolic Fathers: Polycarp",
        "url": "https://www.cambridge.org/core/books/abs/cambridge-companion-to-the-apostolic-fathers/polycarps-epistle-to-the-philippians-and-the-martyrdom-of-polycarp/5FC2910B0A8CACFD7CF167A573B752BB",
        "kind": "scholarship",
    },
    "richards_silvanus": {
        "title": "E. Randolph Richards, 'Silvanus Was Not Peter's Secretary', JETS 43.3 (2000)",
        "url": "https://etsjets.org/jets-volume/jets43/",
        "kind": "scholarship",
    },
}


# Questions whose present pedagogical value is real but whose claim is not suitable
# as a neutral ranking fact.  Rewritten items may eventually leave this set after a
# dedicated source review; until then they remain available in normal learning modes.
RANKING_QUARANTINE_IDS = frozenset(
    {
        "easy_02",
        "easy_03",
        "med_01",
        "med_03",
        "med_15",
        "hard_03",
        "hard_11",
        "hard_13",
    }
)


# Items that teach the project's traditional Petrine reconstruction rather than a
# universally settled result.  Their user-visible text is marked accordingly.
PROJECT_POSITION_IDS = frozenset(
    {
        "easy_02",
        "easy_03",
        "med_01",
        "med_15",
        "hard_11",
        "intro3_16",
    }
)


QUESTION_OVERRIDES: dict[str, dict] = {
    "easy_01": {
        "question": "Кто назван отправителем послания в 1 Пет. 1:1?",
        "options": ["Апостол Павел", "Пётр, апостол Иисуса Христа", "Апостол Иоанн", "Иуда, брат Иакова"],
        "correct": 1,
        "explanation": "1 Пет. 1:1 прямо представляет отправителя как «Пётр, апостол Иисуса Христа». Это утверждение текста; историческое авторство как научный вопрос рассматривается отдельно во введении.",
        "claim_type": "text",
        "confidence": "high",
        "position": "neutral",
        "competitive": True,
        "sources": ["sblgnt"],
    },
    "easy_02": {
        "question": "Какую раннюю датировку 1 Петра принимает традиционная позиция этого курса?",
        "options": ["45–50 гг.", "50–55 гг.", "Начало–середина 60-х гг.", "80–90 гг."],
        "correct": 2,
        "explanation": "Курс принимает раннюю датировку при Петровом авторстве. Это не нейтральный консенсус: современная литература обсуждает как Петрово авторство/дату в 60-х, так и более позднюю псевдонимную датировку.",
        "claim_type": "interpretation",
        "confidence": "contested",
        "position": "project",
        "competitive": False,
        "sources": ["oxford_1peter_contested"],
    },
    "easy_03": {
        "question": "Как традиционная позиция этого курса понимает «Вавилон» в 1 Пет. 5:13?",
        "options": ["Буквальный Вавилон в Месопотамии", "Антиохию", "Рим", "Александрию"],
        "correct": 2,
        "explanation": "Курс следует распространённому прочтению «Вавилона» как символического обозначения Рима. Сам стих прямо не расшифровывает географическое название, поэтому это реконструкция, а не буквальное утверждение текста.",
        "claim_type": "interpretation",
        "confidence": "contested",
        "position": "project",
        "competitive": False,
        "sources": ["oxford_1peter_contested", "sblgnt"],
    },
    "easy_04": {
        "question": "Кто назван в 1 Пет. 5:12 в связи с написанием/передачей письма?",
        "options": ["Тимофей", "Марк", "Лука", "Силуан (Сильван)"],
        "correct": 3,
        "explanation": "1 Пет. 5:12 называет Силуана: «через Силуана… написал вам». Был ли он курьером, секретарём или совмещал роли, обсуждается; сам стих не позволяет превращать секретарскую гипотезу в установленный факт.",
        "claim_type": "text",
        "confidence": "high",
        "position": "neutral",
        "competitive": True,
        "sources": ["sblgnt", "richards_silvanus"],
    },
    "easy_11": {
        "question": "Кого 1 Пет. 5:12 называет посредником, «через» которого было написано/передано письмо?",
        "options": ["Марк", "Тимофей", "Силуан", "Варнава"],
        "correct": 2,
        "explanation": "В 1 Пет. 5:12 назван Силуан. Формула διὰ Σιλουανοῦ допускает обсуждение его точной роли, поэтому вопрос проверяет имя, а не спорную реконструкцию обязанностей.",
        "claim_type": "text",
        "confidence": "high",
        "position": "neutral",
        "competitive": True,
        "sources": ["sblgnt", "richards_silvanus"],
    },
    "med_03": {
        "question": "Какое утверждение о πρόγνωσις («предузнание») в 1 Пет. 1:2 наиболее аккуратно?",
        "options": [
            "Слово само по себе доказывает одну конкретную систему предопределения",
            "Слово обозначает Божье предузнание/предварительный замысел; точная богословская модель его связи с избранием интерпретируется по-разному",
            "Слово означает только человеческое воспоминание",
            "Слово означает случайность",
        ],
        "correct": 1,
        "explanation": "πρόγνωσις связано с предварительным знанием/замыслом Бога. Переход от лексического значения к одной полной модели избрания — уже экзегетический и богословский шаг.",
        "claim_type": "interpretation",
        "confidence": "contested",
        "position": "neutral",
        "competitive": False,
        "sources": ["sblgnt", "oxford_1peter_contested"],
    },
    "med_13": {
        "question": "Каков базовый смысл слова διασποράς в 1 Пет. 1:1?",
        "options": ["Одна конкретная провинция", "Рассеяние / диаспора", "Храмовое служение", "Военный гарнизон"],
        "correct": 1,
        "explanation": "διασπορά означает «рассеяние, диаспора». Вопрос о том, используется ли здесь традиционный иудейский термин буквально или метафорически применительно к христианским адресатам, является отдельной экзегетической дискуссией.",
        "claim_type": "greek",
        "confidence": "high",
        "position": "neutral",
        "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter"],
    },
    "geo_04": {
        "question": "Какой из этих городов был одним из крупнейших и важнейшим административно-экономическим центром римской провинции Асия?",
        "options": ["Эфес", "Антиохия Сирийская", "Александрия Египетская", "Коринф"],
        "correct": 0,
        "explanation": "Эфес был ведущим экономическим и административным центром римской Асии и знаменитым центром культа Артемиды. Старый вариант был сломан: объяснение называло Эфес, но Эфеса не было среди ответов.",
        "claim_type": "history",
        "confidence": "high",
        "position": "neutral",
        "competitive": False,
        "sources": ["oxford_ephesus"],
    },
    "ling1_03": {
        "question": "Как MorphGNT разбирает ἀναγεννήσας в 1 Пет. 1:3?",
        "options": [
            "Аористное активное причастие, именительный падеж, мужской род, ед. число",
            "Настоящее активное причастие, винительный падеж",
            "Перфектный пассивный инфинитив",
            "Будущее среднее причастие",
        ],
        "correct": 0,
        "explanation": "ἀναγεννήσας размечено как аористное активное причастие (AAP, nom. masc. sg.). Сам аорист задаёт perfective viewpoint; из одной формы не следует школьное правило «аорист = завершённый результат».",
        "claim_type": "greek",
        "confidence": "high",
        "position": "neutral",
        "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter"],
    },
    "ling2_12": {
        "question": "В 1 Пет. 1:6 после ἐν ᾧ ἀγαλλιᾶσθε стоят слова ὀλίγον ἄρτι. Какой смысл они вносят?",
        "options": [
            "Гарантируют, что каждое испытание объективно закончится через несколько дней",
            "Ограничивают нынешний период скорби по отношению к более широкому горизонту спасения, не задавая точной длительности",
            "Говорят только о недавно обращённых",
            "Описывают географическое расстояние",
        ],
        "correct": 1,
        "explanation": "Греческий текст имеет ἐν ᾧ ἀγαλλιᾶσθε, ὀλίγον ἄρτι…; старый вопрос ошибочно склеивал «ἐν ὀλίγον ἄρτι». ὀλίγον ἄρτι передаёт ограниченность нынешнего периода, но не задаёт календарный срок.",
        "claim_type": "greek",
        "confidence": "high",
        "position": "neutral",
        "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter"],
    },
    "ling2_15": {
        "question": "Какую функцию имеют дательные φθαρτοῖς, ἀργυρίῳ ἢ χρυσίῳ в 1 Пет. 1:18?",
        "options": [
            "Локативную: «внутри серебра и золота»",
            "Инструментальную/средства: «не тленными вещами — серебром или золотом — вы были искуплены»",
            "Только временную",
            "Это формы именительного падежа",
        ],
        "correct": 1,
        "explanation": "В тексте нет конструкции ἐν φθαρτοῖς. SBLGNT имеет οὐ φθαρτοῖς, ἀργυρίῳ ἢ χρυσίῳ, ἐλυτρώθητε: дательные обозначают средство/цену, с которой сопоставляется искупление кровью Христа.",
        "claim_type": "greek",
        "confidence": "high",
        "position": "neutral",
        "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter"],
    },
    "ling3_03": {
        "question": "Как лучше различить ἀμώμου καὶ ἀσπίλου в 1 Пет. 1:19?",
        "options": [
            "Слова полностью тождественны и не имеют различимых оттенков",
            "ἀμώμου — «без порока»; ἀσπίλου — «незапятнанного/без пятна»",
            "ἀμώμου — «молодого»; ἀσπίλου — «белого»",
            "Оба слова описывают только одежду",
        ],
        "correct": 1,
        "explanation": "ἀμώμου передаёт «без порока», ἀσπίλου — «незапятнанного». Старые автоматические глоссы в этом вопросе были повреждены и дублировались.",
        "claim_type": "greek",
        "confidence": "high",
        "position": "neutral",
        "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter"],
    },
    "ling3_04": {
        "question": "Как MorphGNT разбирает προεγνωσμένου в 1 Пет. 1:20?",
        "options": [
            "Перфектное пассивное причастие, родительный падеж, мужской род, ед. число",
            "Аористный активный императив",
            "Настоящий средний инфинитив",
            "Существительное в дательном падеже",
        ],
        "correct": 0,
        "explanation": "προεγνωσμένου размечено как perfect passive participle (gen. masc. sg.) от προγινώσκω. Контекст добавляет πρὸ καταβολῆς κόσμου — «прежде основания мира»; более детальная богословская модель значения требует отдельной экзегезы.",
        "claim_type": "greek",
        "confidence": "high",
        "position": "neutral",
        "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter"],
    },
    "ling3_06": {
        "question": "Какая глагольная форма стоит в 1 Пет. 1:22 в призыве «любите друг друга» — ἀγαπήσατε?",
        "options": [
            "Презентный императив 2-го лица мн. числа",
            "Аористный активный императив 2-го лица мн. числа",
            "Перфектный пассивный индикатив",
            "Будущий активный инфинитив",
        ],
        "correct": 1,
        "explanation": "SBLGNT/MorphGNT имеет ἀγαπήσατε — аористный активный императив 2-го лица мн. числа. Старый вопрос ошибочно подставлял ἀγαπᾶτε и называл форму презентной.",
        "claim_type": "greek",
        "confidence": "high",
        "position": "neutral",
        "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter"],
    },
    "ling3_08": {
        "question": "Как корректнее описать связь ζῶντος… καὶ μένοντος с λόγου θεοῦ в 1 Пет. 1:23?",
        "options": [
            "Грамматика допускает обсуждение связи причастий со «словом» и с «Богом»; контекст 1:23–25 важен для решения",
            "Формы могут относиться только к читателям",
            "Формы относятся только к ангелам",
            "Греческий текст вообще не содержит этих причастий",
        ],
        "correct": 0,
        "explanation": "λόγου, ζῶντος, θεοῦ и μένοντος стоят в родительном мужского рода ед. числа, поэтому одной морфологии недостаточно, чтобы объявить спор закрытым. Многие переводы связывают «живое и пребывающее» со словом, но вопрос требует синтаксического и контекстуального решения.",
        "claim_type": "greek",
        "confidence": "contested",
        "position": "neutral",
        "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter"],
    },
    "ling3_09": {
        "question": "Как MorphGNT разбирает ἐξηράνθη в цитате 1 Пет. 1:24?",
        "options": [
            "Аористный пассивный индикатив, 3-е лицо ед. числа",
            "Настоящий активный императив",
            "Будущий средний индикатив",
            "Перфектный активный инфинитив",
        ],
        "correct": 0,
        "explanation": "ἐξηράνθη размечено как aorist passive indicative, 3 sg. Это морфологический факт; аорист сам по себе не кодирует школьную формулу «неизбежный итог».",
        "claim_type": "greek",
        "confidence": "high",
        "position": "neutral",
        "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter"],
    },
    "ling3_12": {
        "question": "Как лучше передать ἀνυπόκριτον в 1 Пет. 1:22?",
        "options": ["Воинственный", "Искренний / нелицемерный", "Скрытый", "Торжественный"],
        "correct": 1,
        "explanation": "В контексте ἀνυπόκριτον характеризует братолюбие как искреннее, нелицемерное. Театральный образ может служить иллюстрацией истории слова, но не должен подменять его контекстуальное значение.",
        "claim_type": "greek",
        "confidence": "high",
        "position": "neutral",
        "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter"],
    },
    "hard17_14": {
        "question": "Какое различие между φιλαδελφία и ἀγάπη в 1 Пет. 1:22 можно утверждать без лишней догрузки?",
        "options": [
            "Это полностью одинаковые формы одного слова",
            "φιλαδελφία обозначает братскую любовь; ἀγάπη — более общее слово для любви, а «жертвенная» — возможная богословская характеристика, не словарный эквивалент",
            "φιλαδελφία относится только к кровным родственникам",
            "ἀγάπη всегда означает исключительно романтическую любовь",
        ],
        "correct": 1,
        "explanation": "φιλαδελφία — братская любовь/любовь к братьям; ἀγάπη — любовь в более общем смысле. Называть ἀγάπη просто словарным «жертвенная любовь» слишком узко.",
        "claim_type": "greek",
        "confidence": "high",
        "position": "neutral",
        "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter"],
    },
    "intro1_03": {
        "question": "Как корректнее описать свидетельство Послания Поликарпа к Филиппийцам о раннем использовании 1 Петра?",
        "options": [
            "Поликарп прямо сообщает точную дату 112 г. и номер рукописи 1 Петра",
            "В письме Поликарпа есть близкие параллели с 1 Петром; датировка самого письма и степень прямой литературной зависимости обсуждаются",
            "Поликарп прямо отвергает 1 Петра",
            "Письмо Поликарпа относится только к IV веку",
        ],
        "correct": 1,
        "explanation": "Поликарп — важное раннее свидетельство рецепции 1 Петра, но фиксировать его письмо ровно 112–114 гг. и объявлять конкретную модель зависимости бесспорной слишком сильно. Современные обзоры допускают более широкий диапазон датировки.",
        "claim_type": "history",
        "confidence": "medium",
        "position": "neutral",
        "competitive": False,
        "sources": ["cambridge_polycarp"],
    },
    "intro1_05": {
        "question": "Что можно безопасно заключить из отсутствия 1 Петра в сохранившейся части Мураториева фрагмента?",
        "options": [
            "Это окончательно доказывает неподлинность 1 Петра",
            "Само отсутствие не является решающим аргументом: начало фрагмента повреждено/утрачено, а состав ранних канонических списков требует отдельного анализа",
            "Фрагмент вообще не перечисляет книги",
            "Фрагмент был написан Петром",
        ],
        "correct": 1,
        "explanation": "Повреждённость начала документа не позволяет просто превратить молчание сохранившегося текста в доказательство за или против авторства 1 Петра. Старое объяснение слишком уверенно называло утрату единственной причиной отсутствия.",
        "claim_type": "history",
        "confidence": "medium",
        "position": "neutral",
        "competitive": False,
        "sources": ["oxford_1peter_contested"],
    },
    "intro1_07": {
        "question": "Как осторожнее понимать ἀγράμματοι καὶ ἰδιῶται в Деян. 4:13 применительно к Петру и Иоанну?",
        "options": [
            "Фраза однозначно доказывает полную неграмотность и неспособность читать",
            "Она характеризует их как людей без учёного/специалистского статуса; точная степень грамотности не выводится из одной этой фразы",
            "Она означает, что они не знали арамейского",
            "Она означает, что они были римскими чиновниками",
        ],
        "correct": 1,
        "explanation": "Исследование документальных папирусов показывает, что ἀγράμματος/ἰδιώτης нельзя без остатка свести ни к «без раввинского образования», ни к современному уничижительному ярлыку. Фраза сама по себе не решает вопрос литературной грамотности Петра.",
        "claim_type": "history",
        "confidence": "medium",
        "position": "neutral",
        "competitive": False,
        "sources": ["cambridge_agrammatoi"],
    },
    "intro1_12": {
        "question": "Что переписка Плиния и Траяна (Письма 10.96–97) действительно показывает о преследовании христиан?",
        "options": [
            "Траян приказал активно разыскивать всех христиан по империи",
            "Плиний не знал установленной процедуры, а Траян запретил разыскивать христиан и отказался задавать единое жёсткое правило; это свидетельствует против простой модели систематической общеимперской охоты",
            "Переписка доказывает точную дату написания 1 Петра",
            "Плиний сообщает, что никаких христиан в Вифинии не было",
        ],
        "correct": 1,
        "explanation": "Плиний признаётся в неуверенности относительно процедуры; Траян отвечает, что общего правила задать нельзя и христиан не следует разыскивать. Это сильное свидетельство административной ситуации около 112 г., но не математическое доказательство всех предшествующих десятилетий.",
        "claim_type": "history",
        "confidence": "high",
        "position": "neutral",
        "competitive": False,
        "sources": ["pliny_10_96_97"],
    },
    "intro2_12": {
        "question": "Как корректнее описать сходство 1 Петра с Павловой традицией?",
        "options": [
            "Сходство автоматически доказывает прямое копирование конкретного письма Павла",
            "Возможны общая раннехристианская традиция и/или литературные контакты; само тематическое сходство не доказывает единственную модель зависимости",
            "У Петра и Павла вообще нет общих тем",
            "Сходство доказывает, что Павел написал 1 Петра",
        ],
        "correct": 1,
        "explanation": "Тематическое и словесное сходство требует анализа конкретных параллелей. Формула «наиболее вероятно только общая традиция» была слишком категоричной для вопроса, который остаётся частью дискуссии об авторстве и источниках.",
        "claim_type": "interpretation",
        "confidence": "contested",
        "position": "neutral",
        "competitive": False,
        "sources": ["oxford_1peter_contested"],
    },
    "intro2_13": {
        "question": "Что Гал. 2:11–14 позволяет утверждать об инциденте Петра и Павла в Антиохии?",
        "options": [
            "Павел прямо описывает публичный конфликт из-за поведения Петра под давлением группы, но текст сам по себе не даёт полной реконструкции всех последующих богословских отношений",
            "Павел говорит, что эпизод был выдуман",
            "Текст доказывает пожизненный разрыв между Петром и Павлом",
            "Текст доказывает, что Пётр позже стал автором всех Павловых писем",
        ],
        "correct": 0,
        "explanation": "Гал. 2 описывает реальный конфликт и обвинение в непоследовательности/лицемерии. Делать из этого либо вечный богословский разрыв, либо доказательство полного согласия — шаг за пределы самого эпизода.",
        "claim_type": "text",
        "confidence": "high",
        "position": "neutral",
        "competitive": False,
        "sources": ["sblgnt"],
    },
    "intro2_14": {
        "question": "Какую дату принимает курс, если исходить из Петрова авторства и римской реконструкции места написания?",
        "options": ["80–90 гг.", "Начало–середина 60-х гг.", "Около 120 г.", "II век до н. э."],
        "correct": 1,
        "explanation": "Это условный вывод внутри традиционной реконструкции. Современная наука обсуждает как Петрово авторство и дату в 60-х, так и более поздние варианты; вопрос не выдаёт одну реконструкцию за общий консенсус.",
        "claim_type": "interpretation",
        "confidence": "contested",
        "position": "project",
        "competitive": False,
        "sources": ["oxford_1peter_contested"],
    },
    "intro2_15": {
        "question": "Как следует оценивать отсутствие прямого упоминания Нероновых казней в 1 Петра при обсуждении датировки?",
        "options": [
            "Как абсолютное доказательство даты до 64 г.",
            "Как аргумент от молчания: он может учитываться вместе с другими данными, но сам по себе не устанавливает дату",
            "Как доказательство, что Нерона не существовало",
            "Как доказательство даты после Траяна",
        ],
        "correct": 1,
        "explanation": "Молчание о конкретном событии может иметь вес внутри совокупной исторической реконструкции, но утверждение «Пётр почти наверняка упомянул бы» является психологическим предположением, а не прямым текстовым фактом.",
        "claim_type": "interpretation",
        "confidence": "contested",
        "position": "neutral",
        "competitive": False,
        "sources": ["oxford_1peter_contested", "tacitus_annals_15_44"],
    },
    "intro3_01": {
        "question": "Какое утверждение о παρεπίδημοι («временно проживающие / пришельцы») в 1 Пет. 1:1 наиболее аккуратно?",
        "options": [
            "Слово само по себе доказывает, что каждый адресат юридически не имел гражданства",
            "Слово обозначает временно проживающих/чужеземцев; его богословское применение к идентичности христиан определяется контекстом письма",
            "Слово означает только священников",
            "Слово является названием одной римской провинции",
        ],
        "correct": 1,
        "explanation": "Лексически παρεπίδημος — временный житель/пришелец. Идея небесной родины может быть богословским развитием темы, но её нельзя выдавать за полный словарный эквивалент самого слова.",
        "claim_type": "greek",
        "confidence": "high",
        "position": "neutral",
        "competitive": False,
        "sources": ["sblgnt", "morphgnt_1peter"],
    },
    "intro3_12": {
        "question": "Как корректнее описать порядок Понт → Галатия → Каппадокия → Асия → Вифиния в 1 Пет. 1:1?",
        "options": [
            "Это доказанный путевой дневник Силуана",
            "Это последовательность областей в адресе письма; исследователи предлагали видеть в ней возможный маршрут циркуляции, но текст не сообщает маршрут курьера прямо",
            "Это алфавитный порядок греческих названий",
            "Это порядок по площади провинций",
        ],
        "correct": 1,
        "explanation": "Сам текст даёт последовательность географических названий. Превращать её в установленный маршрут Силуана — историческая реконструкция, которую следует маркировать как вероятностную.",
        "claim_type": "history",
        "confidence": "medium",
        "position": "neutral",
        "competitive": False,
        "sources": ["sblgnt", "oxford_1peter_contested"],
    },
    "intro3_16": {
        "question": "Какой вывод об авторстве принимает этот курс, учитывая существование современной дискуссии?",
        "options": [
            "Курс принимает Петрово авторство, одновременно признавая, что современная наука продолжает обсуждать Петрову и псевдонимную модели",
            "Современная наука единогласно доказала псевдонимность",
            "Современная наука единогласно доказала Петрово авторство",
            "Вопрос авторства никогда не обсуждался",
        ],
        "correct": 0,
        "explanation": "Позиция проекта — традиционное Петрово авторство. Но авторство, дата и место написания относятся к реально contested issues современной науки, поэтому позиция курса должна быть названа позицией курса, а не универсальным verdict.",
        "claim_type": "interpretation",
        "confidence": "contested",
        "position": "project",
        "competitive": False,
        "sources": ["oxford_1peter_contested"],
    },
}


def _infer_claim_type(pool_key: str, item: dict) -> str:
    topic = str(item.get("topic") or "").casefold()
    if pool_key.startswith("practical"):
        return "application"
    if pool_key.startswith("linguistics") or "гречес" in topic or "перевод" in topic:
        return "greek"
    if pool_key.startswith("intro") or any(
        marker in topic for marker in ("автор", "датиров", "псевд", "структур")
    ):
        return "interpretation"
    if pool_key in {"nero", "geography"} or any(
        marker in topic for marker in ("истор", "географ", "гонен")
    ):
        return "history"
    return "text"


def _default_sources(claim_type: str, item: dict) -> list[str]:
    if claim_type == "greek":
        return ["sblgnt", "morphgnt_1peter"]
    if claim_type == "history":
        return []
    if claim_type == "interpretation":
        return ["oxford_1peter_contested"]
    if item.get("verse"):
        return ["sblgnt"]
    return []


def curate_question(question: dict, *, pool_key: str) -> dict:
    """Return one canonical copy with corrections and epistemic metadata."""
    item = deepcopy(question)
    qid = str(item.get("id") or "").strip()
    override = QUESTION_OVERRIDES.get(qid)
    if override:
        item.update(deepcopy(override))

    claim_type = str(item.get("claim_type") or _infer_claim_type(pool_key, item))
    item["claim_type"] = claim_type

    if "confidence" not in item:
        if claim_type in {"application", "interpretation"}:
            item["confidence"] = "contested"
        elif claim_type == "history":
            item["confidence"] = "medium"
        else:
            item["confidence"] = "high"

    if "position" not in item:
        item["position"] = "project" if pool_key in {"intro1", "intro2"} else "neutral"

    if "competitive" not in item:
        item["competitive"] = (
            pool_key.startswith(("easy", "medium", "hard"))
            and qid not in RANKING_QUARANTINE_IDS
            and item["confidence"] == "high"
            and claim_type == "text"
        )

    if "sources" not in item:
        item["sources"] = _default_sources(claim_type, item)

    if item["position"] == "project" and pool_key in {"intro1", "intro2"}:
        text = str(item.get("question") or "")
        if not text.startswith("[Позиция курса]"):
            item["question"] = f"[Позиция курса] {text}"

    return item


def curate_pool(pool: list[dict], *, pool_key: str) -> list[dict]:
    """Canonicalize a raw legacy pool without mutating the authoring source."""
    return [curate_question(question, pool_key=pool_key) for question in pool]


__all__ = [
    "SOURCE_CATALOG",
    "RANKING_QUARANTINE_IDS",
    "PROJECT_POSITION_IDS",
    "QUESTION_OVERRIDES",
    "curate_question",
    "curate_pool",
]
