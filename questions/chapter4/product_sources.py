"""Identity-only Chapter 4 source catalog for root product admission.

Claim-level evidence lives in the immutable Research handoff/review registry.
This catalog never promotes inspection depth, evidence status or a cross-lane
"strongest" value into the canonical root registry.
"""

from __future__ import annotations

RESEARCH_AUTHORITY_SHA = "7e0140129a4aba59a09737701967c3820ff1af57"

_SOURCE_IDS = {
    "w3_lxx_prov10": "LXX Proverbs 10:12 research witness",
    "w3_lxx_prov11": "LXX Proverbs 11:31 research witness",
    "w3_forbes_4_1_6": "Forbes, 1 Peter 4:1-6 exegetical source",
    "w3_storms_1peter": "Storms, 1 Peter teaching/exegetical source",
    "w3_atkinson_en_ho": "Atkinson, ἐν ᾧ in 1 Peter study",
    "w3l_gty_1p4_2_6": "Grace to You / MacArthur, 1 Peter 4:2-6",
    "w3l_horrell_4_6_2003": "Horrell, 1 Peter 4:6 study (2003)",
    "w3_strawbridge_kiss_2025": "Strawbridge/Kiss historical hospitality control",
    "w3_breed_elder_2016": "Breed, elder/diakonia study (2016)",
    "w3h_abbott_smith_oikonomos": "Abbott-Smith lexicon: οἰκονόμος",
    "w3h_abbott_smith_logion": "Abbott-Smith lexicon: λόγιον",
    "w3h_abbott_smith_pyrosis": "Abbott-Smith lexicon: πύρωσις",
    "w3h_abbott_smith_ktistes": "Abbott-Smith lexicon: κτίστης",
    "w3_brown_allotriepiskopos_2006": "Jeannine K. Brown, ἀλλοτριεπίσκοπος (JBL 2006)",
    "w3_ubs_handbook_1peter": "UBS Handbook on 1 Peter",
    "w3_byrley_adversary_2017": "Byrley, adversary/persecution setting study (2017)",
    "w3g_kok_dewinter_4_16_2017": "Kok/De Winter, 1 Peter 4:16 textual study (2017)",
    "intf_catholic_changes": "INTF Catholic Letters critical-text changes control",
    "w3k_lxx_mal3_dbg": "LXX Malachi 3 primary-text research witness",
    "w3k_proctor_johnson_summary_1993": "Proctor/Johnson Malachi-1 Peter proposal control (1993)",
    "w3k_liebengood_standrews_abstract": "Liebengood St Andrews abstract / alternative intertext control",
    "w3n_williams_horrell_icc_v2_2023": "Williams & Horrell, ICC 1 Peter vol. 2 (2023)",
    "w3n_stanojevic_ecm_2021": "Jovan Stanojević, ECM comparison study (2021)",
    "w3n_intf_ecm_catholic_controls": "INTF ECM Catholic Letters / CBGM institutional controls",
    "w2_stenschke_2009": "Stenschke, audience/background study (2009)",
    "w3i_sinaiticus_1p4_5": "Codex Sinaiticus primary witness readback, 1 Peter 4-5",
}

SOURCE_CATALOG = {
    source_id: {
        "title": title,
        "url": "",
        "kind": "chapter4_research_handoff_identity",
        "source_identity_only": True,
        "research_authority_sha": RESEARCH_AUTHORITY_SHA,
        "claim_limit": (
            "Identity/provenance only. Claim-level source support and exact "
            "claim-inspection edge IDs remain in the immutable Chapter-4 v2 "
            "product review contract / pinned Research authority."
        ),
    }
    for source_id, title in _SOURCE_IDS.items()
}

__all__ = ["RESEARCH_AUTHORITY_SHA", "SOURCE_CATALOG"]
