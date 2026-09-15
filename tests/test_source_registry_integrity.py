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
