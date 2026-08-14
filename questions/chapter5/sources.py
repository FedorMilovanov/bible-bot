"""Chapter 5 claim-scoped source catalog from Research Wave 3.

Source identity is not global claim authority. Inspection depth and claim limits
stay owned by this Chapter-5 lane; the root registry receives identities only.
"""


def _source(title, url, kind, scope, limit):
    return {
        "title": title,
        "url": url,
        "kind": kind,
        "inspection_scope": scope,
        "claim_limit": limit,
    }


SOURCE_CATALOG = {
    "sblgnt": _source(
        "SBL Greek New Testament", "https://www.sblgnt.com/", "primary_text",
        "relevant_1Peter5_text_inspected",
        "Supports the printed SBLGNT wording only; not manuscript distribution.",
    ),
    "morphgnt_1peter": _source(
        "MorphGNT / SBLGNT morphology: 1 Peter",
        "https://github.com/morphgnt/sblgnt/blob/master/81-1Pe-morphgnt.txt",
        "primary_text_morphology", "relevant_1Peter5_rows_inspected",
        "Supports form/lemma/parse observations; morphology is not exegesis or apparatus.",
    ),
    "w3_storms_1peter": _source(
        "Sam Storms, 1 Peter Commentary, TGC",
        "https://www.thegospelcoalition.org/commentary/1-peter/",
        "evangelical_passage_commentary", "relevant_4_5_sections_inspected",
        "Supports Storms's stated readings, not scholarly consensus.",
    ),
    "w3_lxx_prov3": _source(
        "Rahlfs-Hanhart LXX, Proverbs 3:34",
        "https://www.die-bibel.de/en/bible/LXX%2CVUL/PRO.3", "primary_lxx",
        "exact_verse_inspected", "Supports wording comparison, not authorial motive.",
    ),
    "w3_elliott_elders_2008": _source(
        "John H. Elliott, Elders as leaders in 1 Peter and the early Church (2008)",
        "https://hts.org.za/index.php/hts/article/view/44",
        "peer_reviewed_leadership_study", "publisher_abstract_and_license_inspected",
        "Supports elder/oversight/shepherd/flock cluster; not a complete polity.",
    ),
    "w3_breed_elder_2016": _source(
        "Gert Breed, The diakonia of the elder according to 1 Peter (2016)",
        "https://indieskriflig.org.za/index.php/skriflig/article/view/2102",
        "peer_reviewed_leadership_study", "publisher_abstract_and_license_inspected",
        "Supports Breed's diakonia framing; one study is not polity consensus.",
    ),
    "w3_richards_silvanus_2000": _source(
        "E. Randolph Richards, Silvanus Was Not Peter's Secretary (2000)",
        "https://etsjets.org/jets-volume/jets43/", "epistolary_authorship_study",
        "official_metadata_and_author_summary_inspected",
        "Supports Richards's carrier thesis; full secretary question remains interpretive.",
    ),
    "w3_net_1p5_12": _source(
        "NET Bible note on 1 Peter 5:12",
        "https://classic.net.bible.org/verse.php?book=1Pe&chapter=5&tab=commentaries&theme=wiki&verse=12",
        "translation_note", "exact_note_inspected",
        "Maps carrier/amanuensis options and prefers carrier; not consensus.",
    ),
    "w3_davarlogos_babylon_2022": _source(
        "Hugo A. Cotro, Historia de dos ciudades (2022)",
        "https://publicaciones.uap.edu.ar/index.php/davarlogos/article/view/1036",
        "peer_reviewed_babylon_interpretation_control", "relevant_section_inspected",
        "Supports a circularity warning and alternative framing; not a decisive location proof.",
    ),
    "w3_babylon_history_map": _source(
        "Roy Schroeder, The Babylon of 1 Peter 5:13 (1954)",
        "https://scholar.csl.edu/bdiv/924/", "history_of_interpretation",
        "institutional_abstract_and_license_inspected",
        "Maps Rome/Mesopotamia/Egypt theories; not a current scholarly arbiter.",
    ),
    "w3_byrley_adversary_2017": _source(
        "Christopher Byrley, Persecution and the 'Adversary' of 1 Peter 5:8 (2017)",
        "https://equip.sbts.edu/publications/journals/journal-of-theology/sbjt-213-fall-2017/persecution-adversary-1-peter-58/",
        "peer_reviewed_persecution_lion_control", "full_web_article_inspected",
        "Supports Byrley's survey/cosmic-conflict reading; one article is not consensus.",
    ),
    "w3_hallstrom_lion_2022": _source(
        "Tyler Hallstrom, Like A Lion (2022)", "https://www.galaxie.com/article/jets65-3-08",
        "peer_reviewed_lion_background_control", "publisher_abstract_and_opening_inspected",
        "Supports multi-background caution; subscription-only details are not claimed inspected.",
    ),
    "w3_strawbridge_kiss_2025": _source(
        "Jennifer Strawbridge, Spiders Are Bad Kissers (2025)",
        "https://ora.ox.ac.uk/objects/uuid%3A12366315-c83f-4dab-8d9c-459b9ecac105",
        "peer_reviewed_greeting_reception_control", "relevant_sections_and_rights_inspected",
        "Supports embodied greeting/reconciliation/kinship themes, not one universal modern form.",
    ),
    "w3_net_1p5_3": _source(
        "NET Bible translation notes on 1 Peter 5:2-3",
        "https://classic.net.bible.org/passage.php?passage=1Pe+5%3A2%2C3", "translation_note",
        "exact_notes_inspected", "Supports participial relation and contextual κλῆρος gloss.",
    ),
    "w3h_abbott_smith_enkomboomai": _source(
        "Abbott-Smith s.v. ἐγκομβόομαι", "https://www.studylight.org/lexicons/eng/greek/1463.html",
        "public_domain_lexicon_entry", "entry_inspected",
        "Supports clothing imagery; not an exact slave-apron reconstruction.",
    ),
    "w3h_mm_enkomboomai": _source(
        "Moulton-Milligan s.v. ἐγκομβόομαι", "https://www.studylight.org/lexicons/eng/greek/1463.html",
        "public_domain_documentary_lexicon_entry", "entry_excerpt_inspected",
        "Negative corpus observation does not prove no ancient example existed.",
    ),
    "w3h_abbott_smith_antidikos": _source(
        "Abbott-Smith s.v. ἀντίδικος", "https://www.studylight.org/lexicons/eng/greek/476.html",
        "public_domain_lexicon_entry", "entry_inspected",
        "Supports legal-opponent/adversary range; not a literal courtroom scene.",
    ),
    "w3h_mm_antidikos": _source(
        "Moulton-Milligan s.v. ἀντίδικος", "https://www.sermonindex.net/strongs/greek/g476/",
        "public_domain_documentary_lexicon_entry", "entry_excerpt_inspected",
        "Supports documentary legal usage and wider adversary use; not heavenly-court proof.",
    ),
    "w3h_abbott_smith_sthenoo": _source(
        "Abbott-Smith s.v. σθενόω", "https://www.studylight.org/lexicons/eng/greek/4599.html",
        "public_domain_lexicon_entry", "entry_inspected",
        "Supports 'strengthen'; cannot establish textual presence in every witness.",
    ),
    "w3h_abbott_smith_suneklektos": _source(
        "Abbott-Smith s.v. συνεκλεκτός", "https://www.studylight.org/lexicons/eng/greek/4899.html",
        "public_domain_lexicon_entry", "entry_inspected",
        "Supports co-elect lexical range; does not identify the historical referent.",
    ),
    "w3g_step_varapp_1p5": _source(
        "STEP Bible VarApp — 1 Peter 5", "https://www.stepbible.org/?q=version%3DVarApp%40reference%3D1Pet.5",
        "secondary_open_textual_apparatus", "full_web_section_inspected_1Peter5",
        "May map witnesses; secondary apparatus is not ECM and cannot supply ECM editorial reasoning.",
    ),
    "w3i_sinaiticus_1p4_5": _source(
        "Codex Sinaiticus official transcription — 1 Peter 4:1-5:13",
        "https://codexsinaiticus.org/en/manuscript.aspx?book=53&chapter=4&verse=15",
        "primary_manuscript_transcription", "relevant_continuous_transcription_lines_inspected",
        "Supports only this named witness; not full manuscript distribution or original-text decision.",
    ),
    "w3i_sinaiticus_1p5_13_14": _source(
        "Codex Sinaiticus official transcription — 1 Peter 5:13-14",
        "https://codexsinaiticus.org/en/manuscript.aspx?dir=next&folioNo=4&lid=en&quireNo=89&side=v",
        "primary_manuscript_transcription", "1Peter5_13_14_transcription_inspected",
        "Supports the named-witness wording only; not a critical-text decision.",
    ),
    "w3n_williams_horrell_icc_v2_2023": _source(
        "Travis B. Williams and David G. Horrell, 1 Peter ICC Vol. 2 (2023)",
        "https://www.bloomsbury.com/us/1-peter-9780567710604/",
        "critical_academic_commentary_ecm_cbgm_based",
        "exact_text_critical_notes_for_1Pet5_2_and_5_12_inspected_via_indexed_text",
        "Supports their ECM-based judgments; commentary is not direct dECM witness-table readback.",
    ),
    "w3n_stanojevic_ecm_2021": _source(
        "Jovan Stanojevic, Orthodox New Testament Textual Scholarship (2021)",
        "https://gorgiaspress.com/orthodox-new-testament-textual-scholarship",
        "academic_direct_comparison_with_ecm_catholic_epistles",
        "published_ecm_comparison_entries_for_1Pet5_2_and_5_12_inspected",
        "Supports the represented ECM-side readings; not the full ECM apparatus.",
    ),
    "w3n_intf_ecm_catholic_controls": _source(
        "INTF, Online Tools for the ECM — Catholic Letters / CBGM controls",
        "https://ntvmr.uni-muenster.de/de_DE/intfblog/-/blogs/1568191",
        "official_ecm_method_and_edition_authority", "official_method_pages_inspected",
        "Supports ECM/CBGM edition context; not a passage-specific apparatus substitute.",
    ),
}

__all__ = ["SOURCE_CATALOG"]
