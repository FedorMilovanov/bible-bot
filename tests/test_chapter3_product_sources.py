import questions
from questions.chapter3 import CHAPTER3_SOURCE_CATALOGS
from questions.chapter3.product_sources import SOURCE_CATALOG as PRODUCT_IDENTITIES


def test_every_lane_source_has_a_product_canonical_identity_or_existing_root_authority():
    lane_source_ids = set().union(*(set(catalog) for catalog in CHAPTER3_SOURCE_CATALOGS.values()))
    assert lane_source_ids
    assert lane_source_ids <= set(questions.SOURCE_CATALOG)
    assert lane_source_ids <= set(PRODUCT_IDENTITIES) | set(questions.SOURCE_CATALOG)


def test_chapter3_identity_records_never_claim_lane_inspection_depth():
    forbidden_depth_keys = {
        "inspection_level",
        "inspection_status",
        "inspection_scope",
        "evidence_status",
        "access_state",
        "rights_state",
    }
    for source_id, metadata in PRODUCT_IDENTITIES.items():
        assert metadata["source_identity_only"] is True, source_id
        assert metadata["product_evidence_status"] == "identity_only_lane_scoped", source_id
        assert metadata["chapter3_lanes"], source_id
        assert forbidden_depth_keys.isdisjoint(metadata), source_id
        assert "never upgrades" in metadata["claim_limit"], source_id


def test_shared_existing_sources_are_not_overwritten_by_chapter3_identity_records():
    # These work IDs predate Chapter 3 in the canonical root registry. Product
    # admission must retain existing authority rather than replace it with a
    # Chapter-3 lane-derived identity record.
    for source_id in ("sblgnt", "morphgnt_1peter"):
        assert source_id in PRODUCT_IDENTITIES
        assert source_id in questions.SOURCE_CATALOG
        assert questions.SOURCE_CATALOG[source_id] != PRODUCT_IDENTITIES[source_id]
        assert questions.SOURCE_CATALOG[source_id].get("source_identity_only") is not True


def test_root_registry_contains_no_synthetic_strongest_lane_status():
    lane_only_ids = set(PRODUCT_IDENTITIES) - {"sblgnt", "morphgnt_1peter"}
    assert lane_only_ids
    for source_id in lane_only_ids:
        root = questions.SOURCE_CATALOG[source_id]
        if root.get("source_identity_only") is True:
            assert root["product_evidence_status"] == "identity_only_lane_scoped"
            assert "inspection_level" not in root
            assert "inspection_status" not in root
            assert "inspection_scope" not in root
            assert "evidence_status" not in root
