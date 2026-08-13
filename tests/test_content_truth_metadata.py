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
            assert isinstance(item["sources"], list)
            assert set(item["sources"]) <= set(SOURCE_CATALOG)
