import json
from collections import Counter
from pathlib import Path
import re
import unicodedata

import questions
from questions.chapter3 import (
    CHAPTER3_LANE_POOLS,
    CHAPTER3_SOURCE_CATALOGS,
    CHAPTER3_STAGING_QUESTIONS,
)

MANIFEST_PATH = Path(__file__).resolve().parents[1] / "data" / "chapter3-integration-manifest.json"
MANIFEST = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

EXPECTED_COUNTS = {"3:1-7": 56, "3:8-12": 37, "3:13-17": 27, "3:18-22": 45}
CLAIM_TYPES = {"text", "greek", "history", "interpretation", "application"}
CONFIDENCES = {"high", "medium", "contested"}
POSITIONS = {"neutral", "project"}


def _normalize(value: str) -> str:
    value = unicodedata.normalize("NFKC", value).casefold()
    return re.sub(r"\s+", " ", value).strip()


def _ids(items):
    return {str(item.get("id") or "").strip() for item in items if str(item.get("id") or "").strip()}


def test_frozen_manifest_matches_agent_e_audited_heads():
    assert MANIFEST["base"] == "9eefbae4cf91d178e9f488e695df9264478197c0"
    assert MANIFEST["frozen_lanes"] == {
        "A_3_1_7": "8b194513420ae6dc5adf853b051539ca1f499ed0",
        "B_3_8_12": "b7cc829e31a0aefc851b12245d7933afcb6561e8",
        "C_3_13_17": "d64656dbcfb8c8c894a11d6cc5e764a189de9336",
        "D_3_18_22": "d4151176053aec4a6bce7685922cb90dfc5f2a77",
        "E_audit": "8c045204190edb8e175b25863c9d87b70d194bc1",
    }
    assert MANIFEST["agent_e_blocker_count"] == 0
    assert MANIFEST["production_wiring"] is False
    assert MANIFEST["ranking_authorized"] is False


def test_integrated_lane_counts_and_global_ids_are_exact():
    assert set(CHAPTER3_LANE_POOLS) == set(EXPECTED_COUNTS)
    for lane, expected in EXPECTED_COUNTS.items():
        assert len(CHAPTER3_LANE_POOLS[lane]) == expected
    assert len(CHAPTER3_STAGING_QUESTIONS) == sum(EXPECTED_COUNTS.values()) == 165

    ids = [item["id"] for item in CHAPTER3_STAGING_QUESTIONS]
    assert all(ids)
    assert len(ids) == len(set(ids)) == 165


def test_canonical_metadata_and_editorial_shape_survive_integration():
    for item in CHAPTER3_STAGING_QUESTIONS:
        assert item["claim_type"] in CLAIM_TYPES, item["id"]
        assert item["confidence"] in CONFIDENCES, item["id"]
        assert item["position"] in POSITIONS, item["id"]
        assert item["competitive"] is False, item["id"]
        assert item["sources"], item["id"]
        assert isinstance(item["explanation"], str) and item["explanation"].strip(), item["id"]

        options = item["options"]
        assert len(options) == 4, item["id"]
        normalized = [_normalize(option) for option in options]
        assert all(normalized), item["id"]
        assert len(normalized) == len(set(normalized)), item["id"]
        assert isinstance(item["correct"], int) and 0 <= item["correct"] < 4, item["id"]


def test_application_contested_and_project_claims_are_quarantined_from_ranking():
    for item in CHAPTER3_STAGING_QUESTIONS:
        if item["claim_type"] == "application" or item["confidence"] == "contested" or item["position"] == "project":
            assert item["competitive"] is False, item["id"]


def test_lane_source_resolution_is_namespaced_and_never_cross_upgrades_metadata():
    root_sources = set(questions.SOURCE_CATALOG)
    assert set(CHAPTER3_SOURCE_CATALOGS) == set(EXPECTED_COUNTS)

    for lane, items in CHAPTER3_LANE_POOLS.items():
        local_catalog = CHAPTER3_SOURCE_CATALOGS[lane]
        known = root_sources | set(local_catalog)
        for item in items:
            assert set(item["sources"]) <= known, (lane, item["id"], set(item["sources"]) - known)

    catalogs = list(CHAPTER3_SOURCE_CATALOGS.values())
    assert len({id(catalog) for catalog in catalogs}) == len(catalogs)
    duplicate_ids = set(catalogs[0])
    for catalog in catalogs[1:]:
        duplicate_ids &= set(catalog)
    assert {"sblgnt", "morphgnt_1peter"} <= duplicate_ids
    assert MANIFEST["source_resolution_policy"] == "LANE_NAMESPACED_NO_CROSS_LANE_METADATA_UPGRADE"


def test_root_production_registry_and_all_competitive_surfaces_exclude_chapter3():
    staging_ids = _ids(CHAPTER3_STAGING_QUESTIONS)

    assert "chapter3" not in questions.POOL_REGISTRY
    assert not any(key.startswith("ch3_") for key in questions.POOL_REGISTRY)

    for key, pool in questions.POOL_REGISTRY.items():
        assert staging_ids.isdisjoint(_ids(pool)), key

    assert staging_ids.isdisjoint(_ids(questions.COMPETITIVE_POOL))
    assert staging_ids.isdisjoint(_ids(questions.BATTLE_POOL))
    for key, pool in questions.CHALLENGE_POOLS.items():
        assert staging_ids.isdisjoint(_ids(pool)), key


def test_integrated_answer_positions_remain_balanced_across_lane_boundaries():
    positions = Counter(item["correct"] for item in CHAPTER3_STAGING_QUESTIONS)
    assert positions == Counter({0: 43, 1: 41, 2: 41, 3: 40})
    assert max(positions.values()) - min(positions.values()) <= 3


def test_owner_project_decisions_preserve_dispute_and_noncompetitive_boundary():
    spirits = MANIFEST["project_policy"]["1Pet3_19_20_spirits"]
    assert spirits["course_position"] == "fallen_spirits_watchers_victory_proclamation"
    assert spirits["position"] == "project"
    assert spirits["confidence"] == "contested"
    assert spirits["competitive"] is False
    assert {"christ_through_noah", "human_dead_descensus_reception"} <= set(spirits["alternatives_retained"])

    eperotema = MANIFEST["project_policy"]["1Pet3_21_eperotema"]
    assert eperotema["course_policy"] == "NO_FORCED_SINGLE_RUSSIAN_GLOSS"
    assert len(eperotema["live_readings"]) >= 3
    assert eperotema["competitive"] is False

    baptism = MANIFEST["project_policy"]["1Pet3_21_baptism"]
    assert baptism["position"] == "project"
    assert baptism["confidence"] == "contested"
    assert baptism["competitive"] is False
    assert baptism["denominational_mechanism_forced"] is False


def test_staging_does_not_claim_chapter_completion_or_publication():
    assert MANIFEST["status"] == "STAGING_INTEGRATED_NOT_PRODUCTION"
    assert MANIFEST["chapter_complete_claimed"] is False
    assert MANIFEST["competitive_cards"] == 0
    assert MANIFEST["substantive_nonblocking_holds"]
