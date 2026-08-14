from questions import POOL_REGISTRY, SOURCE_CATALOG


def test_canonical_metadata_values_and_sources_are_valid():
    claims = {"text", "greek", "history", "interpretation", "application"}
    confidence = {"high", "medium", "contested"}
    positions = {"neutral", "project"}
    for key, pool in POOL_REGISTRY.items():
        if key in {"random_all", "competitive_all", "easy", "medium", "hard", "practical_ch1"}:
            continue
        for item in pool:
            assert item["claim_type"] in claims
            assert item["confidence"] in confidence
            assert item["position"] in positions
            assert isinstance(item["competitive"], bool)
            if key == "chapter4":
                # Chapter 4 v2 intentionally keeps claim-level source evidence out
                # of runtime/public cards. The opaque review record owns it.
                assert isinstance(item["review_record_id"], str)
                assert item["review_record_id"].startswith("ch4prv2_")
                assert "sources" not in item
                assert "source_ids" not in item
                assert "claim_inspection_edge_ids" not in item
                continue
            assert isinstance(item["sources"], list)
            assert set(item["sources"]) <= set(SOURCE_CATALOG)
