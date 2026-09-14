"""Evidence and classification review for legacy context cards.

The legacy chapter-1 authoring corpus canonicalises through
``questions.content_truth.curate_question``. That boundary used to leave
``claim_type="history"`` cards with an empty ``sources`` list, which meant a small
number of production cards - including two ranking-eligible ones - shipped with no
evidence at all. This module closes that gap explicitly:

* ``LEGACY_SOURCE_CATALOG`` adds the primary/reference identities those cards
  actually rest on, following the URL patterns already used by the repository for
  Perseus and Suetonius.
* ``LEGACY_REVIEW_OVERRIDES`` binds the remaining previously unsourced cards to
  real evidence and repairs four cards whose ``claim_type`` was mis-inferred as
  ``history`` although they only summarise the wording of 1 Peter itself.

Wording changes for trivia and internal-note leaks stay in the pool-specific
review modules; this module never changes a claim, only its evidence and class.
"""

LEGACY_SOURCE_CATALOG = {
    "suetonius_nero_5": {
        "title": "Suetonius, Nero 5-6 (family and Agrippina)",
        "url": "https://www.perseus.tufts.edu/hopper/text?doc=Perseus%3Atext%3A1999.02.0132%3Alife%3Dnero%3Achapter%3D5",
        "kind": "primary_source",
    },
    "suetonius_nero_7": {
        "title": "Suetonius, Nero 7 (Seneca as tutor, early reign)",
        "url": "https://www.perseus.tufts.edu/hopper/text?doc=Perseus%3Atext%3A1999.02.0132%3Alife%3Dnero%3Achapter%3D7",
        "kind": "primary_source",
    },
    "suetonius_nero_8": {
        "title": "Suetonius, Nero 8 (accession in 54 CE)",
        "url": "https://www.perseus.tufts.edu/hopper/text?doc=Perseus%3Atext%3A1999.02.0132%3Alife%3Dnero%3Achapter%3D8",
        "kind": "primary_source",
    },
    "suetonius_nero_31": {
        "title": "Suetonius, Nero 31 (Domus Aurea)",
        "url": "https://www.perseus.tufts.edu/hopper/text?doc=Perseus%3Atext%3A1999.02.0132%3Alife%3Dnero%3Achapter%3D31",
        "kind": "primary_source",
    },
    "tacitus_annals_12_69": {
        "title": "Tacitus, Annals 12.69 (death of Claudius and accession of Nero)",
        "url": "https://www.perseus.tufts.edu/hopper/text?doc=Tac.+Ann.+12.69",
        "kind": "primary_source",
    },
    "tacitus_annals_15_38_41": {
        "title": "Tacitus, Annals 15.38-41 (the Great Fire of Rome, 64 CE)",
        "url": "https://www.perseus.tufts.edu/hopper/text?doc=Tac.+Ann.+15.38",
        "kind": "primary_source",
    },
    "tacitus_annals_15_60_64": {
        "title": "Tacitus, Annals 15.60-64 (death of Seneca)",
        "url": "https://www.perseus.tufts.edu/hopper/text?doc=Tac.+Ann.+15.60",
        "kind": "primary_source",
    },
    "pleiades_gazetteer": {
        "title": "Pleiades: A Gazetteer of Past Places",
        "url": "https://pleiades.stoa.org/",
        "kind": "reference",
    },
    "orbis_roman_network": {
        "title": "ORBIS: The Stanford Geospatial Network Model of the Roman World",
        "url": "https://orbis.stanford.edu/",
        "kind": "reference",
    },
}


LEGACY_REVIEW_OVERRIDES = {
    # Nero and geography wording is owned end-to-end by nero_review_extra.py and
    # geography_review.py; here only the remaining unsourced cards are bound to
    # evidence and repaired where the claim class was wrong.
    "easy_12": {
        "sources": ["suetonius_nero_8", "tacitus_annals_12_69"],
    },
    "med_02": {
        "sources": ["tacitus_annals_15_38_41", "tacitus_annals_15_44"],
    },
    # --- Cards that only summarise 1 Peter must not masquerade as history -----
    "med_es2_07": {
        "claim_type": "text",
        "sources": ["sblgnt"],
    },
    "hard_es2_08": {
        "claim_type": "interpretation",
        "sources": ["sblgnt", "oxford_1peter_contested"],
    },
    "hard_deep_13": {
        "claim_type": "interpretation",
        "sources": ["sblgnt", "oxford_1peter_contested"],
    },
    "hard_deep_18": {
        "claim_type": "text",
        "sources": ["sblgnt"],
    },
    # --- Source-quorum top-ups (docs/CONTENT_SOURCE_POLICY.md) ---------------
    # Every history/project claim needs a second, independent witness. The cards
    # below carried a single modern reference; each now also names the primary
    # text or a second serious witness the explanation already rests on.
    "easy_02": {
        "sources": ["oxford_1peter_contested", "schreiner_1peter_2003"],
    },
    "geo_05": {
        "verse": "1 Пет. 1:1",
    },
    "nero_05": {
        "verse": "Деян. 22:25-28",
    },
    "geo_10": {
        "claim_type": "text",
        "sources": ["sblgnt"],
    },
    "nero_04": {
        "sources": ["eusebius_church_history_2_25", "schreiner_1peter_2003"],
    },
    "intro1_01": {
        "sources": ["oxford_1peter_contested", "sblgnt"],
    },
    "intro1_03": {
        "sources": ["cambridge_polycarp", "oxford_1peter_contested"],
    },
    "intro1_04": {
        "sources": ["oxford_1peter_contested", "cambridge_polycarp"],
    },
    "intro1_06": {
        "sources": ["oxford_1peter_contested", "sblgnt"],
    },
    "intro1_07": {
        "sources": ["cambridge_agrammatoi", "sblgnt"],
    },
    "intro1_08": {
        "sources": ["oxford_1peter_contested", "septuagint_bible"],
    },
    "intro1_11": {
        "sources": ["oxford_1peter_contested", "pliny_10_96_97"],
    },
    "intro1_05": {"sources": ["oxford_1peter_contested", "cambridge_polycarp"]},
    "intro1_09": {"sources": ["oxford_1peter_contested", "schreiner_1peter_2003"]},
    "intro1_10": {"sources": ["oxford_1peter_contested", "sblgnt"]},
    "intro1_12": {"sources": ["pliny_10_96_97", "oxford_1peter_contested"]},
    "intro1_13": {"sources": ["oxford_1peter_contested", "schreiner_1peter_2003"]},
    "intro1_14": {"sources": ["oxford_1peter_contested", "achtemeier_hermeneia_1peter"]},
    "intro1_15": {"sources": ["oxford_1peter_contested", "schreiner_1peter_2003"]},
    "intro2_01": {"sources": ["oxford_1peter_contested", "schreiner_1peter_2003"]},
    "intro2_02": {"sources": ["oxford_1peter_contested", "schreiner_1peter_2003"]},
    "intro2_03": {"sources": ["oxford_1peter_contested", "schreiner_1peter_2003"]},
    "intro2_04": {"sources": ["oxford_1peter_contested", "schreiner_1peter_2003"]},
    "intro2_05": {
        "sources": ["oxford_1peter_contested", "tacitus_annals_15_44", "schreiner_1peter_2003"],
    },
    "intro2_06": {"sources": ["oxford_1peter_contested", "schreiner_1peter_2003"]},
    "intro2_07": {"sources": ["oxford_1peter_contested", "schreiner_1peter_2003"]},
    "intro2_08": {"sources": ["oxford_1peter_contested", "schreiner_1peter_2003"]},
    "intro2_14": {"sources": ["oxford_1peter_contested", "schreiner_1peter_2003"]},
    "intro2_12": {"sources": ["oxford_1peter_contested", "schreiner_1peter_2003"]},
    "intro3_03": {"sources": ["oxford_1peter_contested", "horrell_williams_icc_2023"]},
    "intro2_16": {"sources": ["oxford_1peter_contested", "schreiner_1peter_2003"]},
    "intro3_16": {"sources": ["oxford_1peter_contested", "schreiner_1peter_2003"]},
    "tms1_app_01": {"sources": ["sblgnt", "schreiner_1peter_2003"]},
    "tms1_app_02": {"sources": ["sblgnt", "schreiner_1peter_2003"]},
}

# Cards that keep their wording but must not stay ranking-eligible merely because
# their evidence was silently empty before this review.
LEGACY_UNSOURCED_IDS = frozenset(LEGACY_REVIEW_OVERRIDES)

__all__ = [
    "LEGACY_REVIEW_OVERRIDES",
    "LEGACY_SOURCE_CATALOG",
    "LEGACY_UNSOURCED_IDS",
]
