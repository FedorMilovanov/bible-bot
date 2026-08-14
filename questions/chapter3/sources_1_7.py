"""Lane-local reviewed sources for the 1 Peter 3:1-7 evidence slice.

This catalog is intentionally not wired into the shared source registry.  Agent A's
foundation tests resolve every card against this module only.
"""

SOURCE_CATALOG = {
    "sblgnt": {
        "title": "SBL Greek New Testament (1 Peter 3:1-7)",
        "kind": "primary_text_greek",
        "url": "https://www.sblgnt.com/",
    },
    "morphgnt_1peter": {
        "title": "MorphGNT / SBLGNT morphology: 1 Peter",
        "kind": "morphology_dataset",
        "url": "https://github.com/morphgnt/sblgnt/blob/master/81-1Pe-morphgnt.txt",
    },
    "net_1p3_1_7": {
        "title": "NET Bible, 1 Peter 3:1-7, translation notes",
        "kind": "translation_grammar_notes",
        "url": "https://classic.net.bible.org/passage.php?passage=1Pe+3%3A1-7",
    },
    "lxx_gen_18": {
        "title": "Septuagint Genesis 18 (esp. 18:12)",
        "kind": "primary_text_lxx",
        "url": "https://www.septuagint.bible/-/genesis-18",
    },
    "lxx_prov_3_25": {
        "title": "Septuagint Proverbs 3:25",
        "kind": "primary_text_lxx",
        "url": "https://www.studylight.org/interlinear-study-bible/greek/proverbs/3-25.html",
    },
    "abbott_smith_ptoesis": {
        "title": "G. Abbott-Smith, Manual Greek Lexicon of the New Testament, πτόησις",
        "kind": "lexicon",
        "url": "https://www.studylight.org/lexicons/eng/greek/4423.html",
    },
    "plutarch_conjugal_precepts": {
        "title": "Plutarch, Advice to Bride and Groom (Conjugalia Praecepta)",
        "kind": "primary_ancient_social_history",
        "url": "https://www.perseus.tufts.edu/hopper/text?doc=Perseus%3Atext%3A2008.01.0181",
    },
    "aristotle_politics_1": {
        "title": "Aristotle, Politics Book 1 (household relations)",
        "kind": "primary_ancient_social_history",
        "url": "https://www.perseus.tufts.edu/hopper/text?doc=Perseus:text:1999.01.0058:book=1",
    },
    "musonius_marriage": {
        "title": "Musonius Rufus, Lecture XIII A, What Is the Chief End of Marriage?",
        "kind": "primary_ancient_social_history",
        "url": "https://sites.google.com/site/thestoiclife/the_teachers/musonius-rufus/lectures/13-0",
    },
    "treggiari_roman_marriage": {
        "title": "Susan Treggiari, Roman Marriage: Iusti Coniuges (OUP, 1991)",
        "kind": "modern_social_history",
        "url": "https://academic.oup.com/book/47295",
    },
    "balch_wives_submissive": {
        "title": "David L. Balch, Let Wives Be Submissive: The Domestic Code in 1 Peter (1981)",
        "kind": "modern_social_history_exegesis",
        "url": "https://books.google.com/books/about/Let_Wives_be_Submissive.html?id=yzJhQgAACAAJ",
    },
    "horrell_mixed_marriage_2016": {
        "title": "David G. Horrell, Ethnicisation, Marriage and Early Christian Identity (NTS 62.3, 2016)",
        "kind": "peer_reviewed_social_history_exegesis",
        "url": "https://doi.org/10.1017/S0028688516000084",
    },
    "horrell_williams_icc_2023": {
        "title": "David G. Horrell and Travis B. Williams, 1 Peter: ICC, Vol. 2, Chapters 3-5 (2023)",
        "kind": "major_critical_commentary",
        "url": "https://www.bloomsbury.com/us/1-peter-9780567710611/",
    },
    "van_rensburg_sarah_2004": {
        "title": "Fika J. van Rensburg, Sarah's submissiveness to Abraham: A socio-historic interpretation of 1 Peter 3:5-6 (HTS, 2004)",
        "kind": "peer_reviewed_exegesis",
        "url": "https://doi.org/10.4102/hts.v60i1/2.499",
    },
    "davids_1peter_1990": {
        "title": "Peter H. Davids, The First Epistle of Peter (NICNT, 1990)",
        "kind": "major_evangelical_commentary",
        "url": "https://www.eerdmans.com/9780802825162/the-first-epistle-of-peter/",
    },
    "schreiner_1peter_2003": {
        "title": "Thomas R. Schreiner, 1, 2 Peter, Jude (NAC 37, 2003)",
        "kind": "major_conservative_commentary",
        "url": "https://www.bhpublishinggroup.com/product/1-2-peter-jude-2/",
    },
    "macarthur_1p3_1_7": {
        "title": "John MacArthur, How to Win Your Unbelieving Spouse (1 Peter 3:1-7)",
        "kind": "conservative_exposition_position",
        "url": "https://www.gty.org/resources/study-guides/chapters/60-31/how-to-win-your-unbelieving-spouse",
    },
    "macarthur_husbands": {
        "title": "John MacArthur, Husbands, Love Your Wives (with 1 Peter 3:7 exposition)",
        "kind": "conservative_exposition_position",
        "url": "https://www.gty.org/library/sermons-library/80-383/husbands-love-your-wives",
    },
    "piper_fearless_submission": {
        "title": "John Piper, The Beautiful Faith of Fearless Submission (1 Peter 3:1-7)",
        "kind": "conservative_exposition_position",
        "url": "https://www.desiringgod.org/messages/the-beautiful-faith-of-fearless-submission",
    },
}

CONSERVATIVE_SOURCE_IDS = frozenset({
    "davids_1peter_1990",
    "schreiner_1peter_2003",
    "macarthur_1p3_1_7",
    "macarthur_husbands",
    "piper_fearless_submission",
})

PRIMARY_SOCIAL_HISTORY_IDS = frozenset({
    "plutarch_conjugal_precepts",
    "aristotle_politics_1",
    "musonius_marriage",
})

MODERN_SOCIAL_HISTORY_IDS = frozenset({
    "treggiari_roman_marriage",
    "balch_wives_submissive",
    "horrell_mixed_marriage_2016",
    "horrell_williams_icc_2023",
    "van_rensburg_sarah_2004",
})

__all__ = [
    "SOURCE_CATALOG",
    "CONSERVATIVE_SOURCE_IDS",
    "PRIMARY_SOCIAL_HISTORY_IDS",
    "MODERN_SOCIAL_HISTORY_IDS",
]
