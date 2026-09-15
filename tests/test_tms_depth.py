"""
TMS Depth tests — ensure no banality, theological depth, verification.
"""
from questions import POOL_REGISTRY


def test_no_joke_distractors_in_reviewed():
    """Chapter 2-5 must not have joke markers."""
    for key in ["chapter2", "chapter3", "chapter4", "chapter5"]:
        pool = POOL_REGISTRY.get(key, [])
        for q in pool:
            opts = q.get("options", [])
            for opt in opts:
                low = str(opt).lower()
                assert "случайные прозвища" not in low, f"{q['id']} has joke distractor"
                assert "без смысловой нагрузки" not in low, f"{q['id']} has joke distractor"
                assert "торжественная формула без смысла" not in low, f"{q['id']} has joke distractor"

def test_easy_has_theological_hook():
    """Easy should have at least 20 chars explanation (target 120)."""
    pool = POOL_REGISTRY.get("easy", [])
    for q in pool:
        exp = str(q.get("explanation") or "")
        # target 120, but enforce 20 to allow legacy es2 that will be upgraded via tms_deep
        assert len(exp) >= 20, f"{q['id']} explanation too short: {len(exp)}"

def test_tms_deep_pool_exists_and_has_sources():
    pool = POOL_REGISTRY.get("tms_deep")
    assert pool is not None, "tms_deep pool missing"
    assert len(pool) >= 10, f"tms_deep too small: {len(pool)}"
    for q in pool:
        assert isinstance(q.get("sources"), list) and len(q["sources"]) >= 1, f"{q['id']} missing sources"
        assert len(str(q.get("explanation") or "")) >= 100, f"{q['id']} explanation too short"

def test_chapter2_sources_resolve():
    from questions.source_registry import SOURCE_CATALOG
    pool = POOL_REGISTRY.get("chapter2", [])
    for q in pool:
        for src in q.get("sources", []):
            assert src in SOURCE_CATALOG, f"{q['id']} unknown source {src}"

def test_chapter3_no_competitive_without_high_text():
    """Only high text neutral can be competitive in chapter3 (guardrail)."""
    pool = POOL_REGISTRY.get("chapter3", [])
    for q in pool:
        if q.get("competitive"):
            assert q.get("claim_type") == "text", f"{q['id']} competitive but claim_type {q.get('claim_type')}"
            assert q.get("confidence") == "high", f"{q['id']} competitive but confidence {q.get('confidence')}"
            assert q.get("position") == "neutral", f"{q['id']} competitive but position {q.get('position')}"

def test_no_bad_correct_indices():
    for key, pool in POOL_REGISTRY.items():
        if key in {"random_all", "competitive_all"}:
            continue
        for q in pool:
            opts = q.get("options", [])
            correct = q.get("correct")
            assert isinstance(correct, int), f"{q['id']} correct not int"
            assert 0 <= correct < len(opts), f"{q['id']} bad correct {correct} for len {len(opts)}"

def test_geo_04_fixed():
    """Ensure geo_04 override fixed the old broken question."""
    pool = POOL_REGISTRY.get("geography", [])
    found = [q for q in pool if q["id"] == "geo_04"]
    assert len(found) == 1
    q = found[0]
    assert "Эфес" in q["options"], "geo_04 should contain Ephesus"
    assert q["options"][q["correct"]] == "Эфес"
