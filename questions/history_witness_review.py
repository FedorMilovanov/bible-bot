"""Second witnesses for the three context cards that rested on one source.

`geo_06`, `nero_12` and `nero_13` carried a real primary citation, but a
history claim needs a second, independent witness before it can stand as a
neutral fact (``docs/CONTENT_SOURCE_POLICY.md``: history = primary source when
available plus a scholarly or reference control). Each card keeps its original
witness and gets one more, and the explanation says what the second witness
supports instead of only naming a book.

The added identities are cited at book level on purpose. A book-level locus is
verifiable in any edition; a precise paragraph number would claim more precision
than this environment can check against the source text.
"""

from __future__ import annotations

HISTORY_WITNESS_SOURCE_CATALOG = {
    "tacitus_annals_13": {
        "title": "Tacitus, Annals 13 (Seneca and Burrus directing the young Nero)",
        "url": "https://penelope.uchicago.edu/Thayer/E/Roman/Texts/Tacitus/home.html",
        "kind": "primary_source",
    },
    "cassius_dio_roman_history_62": {
        "title": "Cassius Dio, Roman History 62 (death of Seneca, via the Xiphilinus epitome)",
        "url": "https://penelope.uchicago.edu/Thayer/E/Roman/Texts/Cassius_Dio/home.html",
        "kind": "primary_source",
    },
    "josephus_jewish_war_6": {
        "title": "Josephus, Jewish War 6 (Jerusalem, the temple and the Passover crowds)",
        "url": "https://penelope.uchicago.edu/Thayer/E/Roman/Texts/Josephus/home.html",
        "kind": "primary_source",
    },
}

HISTORY_WITNESS_OVERRIDES = {
    "geo_06": {
        "explanation": (
            "Иерусалим был религиозным центром иудаизма: там находился храм, куда "
            "совершались паломничества. Положение города фиксирует Pleiades Gazetteer, "
            "а значение храма и пасхальных паломничеств описывает Иосиф Флавий "
            "(«Иудейская война», кн. 6). Для 1 Петра это важно потому, что его читатели "
            "жили вдали от храма и синагог Иерусалима."
        ),
        "sources": ["pleiades_gazetteer", "josephus_jewish_war_6"],
    },
    "nero_12": {
        "explanation": (
            "Сенека, философ и сенатор, был наставником Нерона в начале правления: "
            "Светоний прямо называет его наставником будущего императора (Nero 7), а "
            "Тацит описывает, как Сенека и Бурр направляли молодого правителя в первые "
            "годы (Ann. 13). Позже его влияние ослабло, что отразилось и в его судьбе."
        ),
        "sources": ["suetonius_nero_7", "tacitus_annals_13"],
    },
    "nero_13": {
        "explanation": (
            "После раскрытия заговора Пизона (65 г.) Сенеку обвинили в соучастии и "
            "принудили к самоубийству; Тацит передаёт его последние часы как философскую "
            "сцену (Ann. 15.60–64), и та же развязка известна по Кассию Диону (Roman "
            "History 62, в эпитоме Ксифилина). Для 1 Петра это фон политической "
            "атмосферы Рима 60-х годов."
        ),
        "sources": ["tacitus_annals_15_60_64", "cassius_dio_roman_history_62"],
    },
}

__all__ = ["HISTORY_WITNESS_OVERRIDES", "HISTORY_WITNESS_SOURCE_CATALOG"]
