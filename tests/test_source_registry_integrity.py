"""Cross-chapter source-registry integrity and canonical URL regressions."""

from __future__ import annotations

import questions
from questions.chapter3.product_sources import SOURCE_CATALOG as CHAPTER3_IDENTITIES
from questions.chapter4.product_sources import SOURCE_CATALOG as CHAPTER4_IDENTITIES
from questions.chapter5.product_sources import SOURCE_CATALOG as CHAPTER5_IDENTITIES


def test_known_passage_sources_resolve_to_the_passages_their_ids_claim():
    expected = {
        "gty_1p1_13": "https://www.gty.org/sermons/60-10/hope-holiness-and-honor",
        "gty_1p1_18_21": "https://www.gty.org/sermons/80-200/the-basics-of-redemption",
        "gty_1p1_7": "https://www.gty.org/sermons/60-7/the-joy-of-salvation-part-1",
        "gty_1p3_18": (
            "https://www.gty.org/resources/study-guides/chapters/60-36/"
            "the-triumph-of-christs-suffering-part-1"
        ),
        "josephus_jewish_war_6": "https://penelope.uchicago.edu/josephus/war-6.html",
        "gty_1p2_18_21": "https://www.gty.org/sermons/60-26/submission-in-the-workplace-part-1",
        "macarthur_husbands": "https://www.gty.org/sermons/80-383/husbands-love-your-wives",
        "w3i_sinaiticus_1p4_5": (
            "https://www.codexsinaiticus.org/en/manuscript.aspx?book=53&chapter=4&verse=15"
        ),
        "w3i_sinaiticus_1p5_13_14": (
            "https://www.codexsinaiticus.org/en/manuscript.aspx?book=53&chapter=5&verse=13"
        ),
    }
    for source_id, url in expected.items():
        assert questions.SOURCE_CATALOG[source_id]["url"] == url


def test_removed_mismatched_gty_identity_is_not_left_as_dead_authority():
    assert "gty_1p1_17_21" not in questions.SOURCE_CATALOG
    hard = next(card for card in questions.get_pool_by_key("tms_deep") if card["id"] == "tms1_hard_05")
    assert hard["sources"] == ["sblgnt", "gty_1p1_13", "gty_1p1_18_21"]


def test_later_identity_catalogs_may_fill_only_a_missing_bibliographic_url():
    # Chapter 4 intentionally admitted several Research sources as identity-only
    # stubs. Chapter 5 later supplied public identity URLs for the same works.
    # The root registry should expose those URLs without importing any later
    # inspection-depth or claim-support metadata.
    for source_id, later in CHAPTER5_IDENTITIES.items():
        root = questions.SOURCE_CATALOG[source_id]
        if (
            root.get("source_identity_only") is True
            and later.get("source_identity_only") is True
            and later.get("url")
            and source_id in CHAPTER4_IDENTITIES
        ):
            assert root["url"] == later["url"], source_id
            assert root.get("research_authority_sha") == CHAPTER4_IDENTITIES[source_id].get(
                "research_authority_sha"
            )
            for forbidden in (
                "inspection_scope",
                "evidence_status",
                "claim_inspection_edge_ids",
                "strongest_depth",
                "claim_depth",
            ):
                assert forbidden not in root, (source_id, forbidden)


def test_non_identity_existing_authority_is_never_replaced_by_product_identity():
    # The root SBLGNT authority is intentionally broader than Chapter 3's
    # identity-only file locus. The source-registry merge must not replace it.
    root = questions.SOURCE_CATALOG["sblgnt"]
    assert root.get("source_identity_only") is not True
    assert root["url"] != CHAPTER3_IDENTITIES["sblgnt"]["url"]


def test_source_registry_contains_no_known_legacy_or_tls_fragile_url_forms():
    urls = {
        str(metadata.get("url") or "")
        for metadata in questions.SOURCE_CATALOG.values()
        if metadata.get("url")
    }
    assert not any("shop.gty.org/library/bibleqnas-library" in url for url in urls)
    assert not any("gty.org/library/sermons-library" in url for url in urls)
    assert not any(url.startswith("https://codexsinaiticus.org/") for url in urls)


def test_identity_url_enrichment_never_changes_earlier_authority_fields():
    """Later chapter identities may fill a blank URL and nothing else."""
    for source_id, later in CHAPTER5_IDENTITIES.items():
        if source_id not in CHAPTER4_IDENTITIES:
            continue
        earlier = CHAPTER4_IDENTITIES[source_id]
        if (
            earlier.get("source_identity_only") is not True
            or later.get("source_identity_only") is not True
            or not later.get("url")
        ):
            continue
        expected = dict(earlier)
        expected["url"] = later["url"]
        assert questions.SOURCE_CATALOG[source_id] == expected, source_id


def test_one_card_never_counts_multiple_aliases_of_the_same_work_as_independent_sources():
    """Source-ID aliases are provenance handles, not independent witnesses."""
    same_work_families = (
        {"davids_1peter_1990", "davids_1peter_nicnt"},
        {"pliny_10_96_97", "pliny_trajan_10_96_97"},
        {"schreiner_nac_1peter", "schreiner_1peter_2003", "schreiner_1peter_nac"},
        {"richards_silvanus", "w3_richards_silvanus_2000"},
        {
            "horrell_williams_icc_2023",
            "horrell_williams_icc_v2",
            "w3n_williams_horrell_icc_v2_2023",
        },
        {
            "tgc_storms_1p3_18_22",
            "tgc_1p_commentary",
            "tgc_storms_1peter",
            "w3_storms_1peter",
        },
        {"ubs_handbook_1p2_12", "ubs_handbook_1p3_21", "w3_ubs_handbook_1peter"},
    )
    aggregate_pools = {
        "easy", "medium", "hard", "practical_ch1", "random_all", "competitive_all"
    }
    for pool, cards in questions.POOL_REGISTRY.items():
        if pool in aggregate_pools:
            continue
        for card in cards:
            actual = set(card.get("sources") or ())
            for family in same_work_families:
                duplicated = sorted(actual & family)
                assert len(duplicated) <= 1, (pool, card["id"], duplicated)
