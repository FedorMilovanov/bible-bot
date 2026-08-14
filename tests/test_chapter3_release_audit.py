import json
from pathlib import Path

import questions
from course_catalog import SURFACE_MINIAPP, public_catalog
from questions.chapter3.challenge_taxonomy import CHAPTER3_CHALLENGE_TAXONOMY
from questions.chapter3.product_sources import SOURCE_CATALOG as CHAPTER3_PRODUCT_SOURCE_IDENTITIES
from questions.chapter3.ranking_audit import (
    CHAPTER3_RANKING_HOLD_REASONS,
    CHAPTER3_RANKING_READY_IDS,
)
from questions.chapter3.ranking_authority import CHAPTER3_RANKING_AUTHORIZED_IDS
from questions.chapter3.reviewed import CHAPTER3_REVIEWED_QUESTIONS
from questions.pool_policy import is_non_scoring_learning_pool

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = json.loads((ROOT / "data" / "chapter3-release-audit.json").read_text(encoding="utf-8"))
MINIAPP_INDEX = (ROOT / "miniapp" / "index.html").read_text(encoding="utf-8")
MINIAPP_APP = (ROOT / "miniapp" / "app.js").read_text(encoding="utf-8")
MINIAPP_CATALOG = (ROOT / "miniapp" / "course_catalog.js").read_text(encoding="utf-8")


def _ids(items):
    return {str(item["id"]) for item in items}


def test_release_lineage_pins_every_green_checkpoint():
    assert MANIFEST["lineage"] == {
        "base": "9eefbae4cf91d178e9f488e695df9264478197c0",
        "integration": "571f1f3d0fcab83c0b087735adcaf61316090525",
        "reviewed": "fe0b9ea7ed0098c2a625bb6956df0a1368e182ef",
        "normal_learning": "b84ea8559fddc3bade5044b9124ddab8784a0cc5",
        "ranking_audit": "b695f9170673caaaff517803d82331386750ccb2",
        "ranking_authority": "05fbcc7f2052cdd106d406dc364bc466dac843fa",
        "battle_admission": "6f7fbc5de2e7fbfc7d54b6c844234636dbc03a49",
        "challenge_taxonomy": "62fb982da58a4a97739dd9b504e43cb5d34d7ec2",
        "challenge_admission": "d9d944d88401cccd90408d00d3ef7bf0977c5bba",
    }
    for stage, gates in MANIFEST["exact_green_gates"].items():
        assert len(gates) == 3, stage
        assert gates[0].startswith("CI#"), (stage, gates)
        assert gates[1].startswith("Security#"), (stage, gates)
        assert gates[2].startswith("CodeQL#"), (stage, gates)


def test_normal_learning_is_exact_reviewed_bank_and_non_scoring():
    reviewed_ids = _ids(CHAPTER3_REVIEWED_QUESTIONS)
    root_ids = _ids(questions.get_pool_by_key("chapter3"))
    assert len(reviewed_ids) == len(root_ids) == 165
    assert root_ids == reviewed_ids
    assert is_non_scoring_learning_pool("chapter3") is True
    assert reviewed_ids.isdisjoint(_ids(questions.get_pool_by_key("random_all")))
    assert MANIFEST["normal_learning"] == {
        "reviewed_cards": 165,
        "pool_key": "chapter3",
        "non_scoring": True,
        "random_all": False,
        "points": 0,
        "daily_bonus": 0,
        "achievements": False,
    }


def test_ranking_authority_is_exact_twelve_with_zero_audit_holds():
    reviewed_ids = _ids(CHAPTER3_REVIEWED_QUESTIONS)
    authorized = set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    unauthorized = reviewed_ids - authorized
    assert len(authorized) == 12
    assert len(unauthorized) == 153
    assert authorized == set(CHAPTER3_RANKING_READY_IDS)
    assert not CHAPTER3_RANKING_HOLD_REASONS
    assert reviewed_ids & _ids(questions.COMPETITIVE_POOL) == authorized
    assert reviewed_ids & _ids(questions.BATTLE_POOL) == authorized
    assert unauthorized.isdisjoint(_ids(questions.COMPETITIVE_POOL))
    assert unauthorized.isdisjoint(_ids(questions.BATTLE_POOL))


def test_challenge_consumes_exact_taxonomy_and_keeps_hard_and_fallback_closed():
    reviewed_ids = _ids(CHAPTER3_REVIEWED_QUESTIONS)
    authorized = set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    challenge_authorized = set()
    for level in ("easy", "medium", "hard"):
        expected = set(CHAPTER3_CHALLENGE_TAXONOMY[level])
        assert _ids(questions.CHAPTER3_CHALLENGE_POOLS[level]) == expected
        actual = reviewed_ids & _ids(questions.CHALLENGE_POOLS[level])
        assert actual == expected, level
        challenge_authorized |= actual
    assert challenge_authorized == authorized
    assert CHAPTER3_CHALLENGE_TAXONOMY["hard"] == ()
    assert questions.CHALLENGE_FALLBACK_POOL is questions.CHAPTER1_COMPETITIVE_POOL
    assert reviewed_ids.isdisjoint(_ids(questions.CHALLENGE_FALLBACK_POOL))
    assert MANIFEST["ranking"]["challenge_easy"] == 6
    assert MANIFEST["ranking"]["challenge_medium"] == 6
    assert MANIFEST["ranking"]["challenge_hard"] == 0
    assert MANIFEST["ranking"]["challenge_fallback_chapter3"] == 0


def test_epistemic_categories_never_enter_authorized_ranking_set():
    authorized = set(CHAPTER3_RANKING_AUTHORIZED_IDS)
    for item in CHAPTER3_REVIEWED_QUESTIONS:
        if (
            item["position"] == "project"
            or item["confidence"] == "contested"
            or item["claim_type"] in {"greek", "history", "application"}
        ):
            assert item["id"] not in authorized, item["id"]
            assert item["id"] not in _ids(questions.COMPETITIVE_POOL), item["id"]
        if item["position"] == "project":
            assert item["question"].startswith("[Позиция курса]"), item["id"]


def test_authorized_cards_keep_exact_source_minimum_and_lane_depth_contract():
    reviewed = {item["id"]: item for item in CHAPTER3_REVIEWED_QUESTIONS}
    from questions.chapter3.sources_1_7 import SOURCE_CATALOG as SOURCES_1_7

    assert SOURCES_1_7["sblgnt"]["evidence_status"] == "inspected_primary"
    assert SOURCES_1_7["net_1p3_1_7"]["evidence_status"] == "inspected_passage"
    for qid in CHAPTER3_RANKING_AUTHORIZED_IDS:
        assert set(reviewed[qid]["sources"]) == {"sblgnt", "net_1p3_1_7"}


def test_product_source_identities_do_not_encode_claim_depth():
    forbidden = {
        "inspection_level",
        "inspection_status",
        "inspection_scope",
        "evidence_status",
        "access_state",
        "rights_state",
    }
    for source_id, metadata in CHAPTER3_PRODUCT_SOURCE_IDENTITIES.items():
        assert metadata["source_identity_only"] is True, source_id
        assert metadata["product_evidence_status"] == "identity_only_lane_scoped", source_id
        assert forbidden.isdisjoint(metadata), source_id


def test_miniapp_exposes_reviewed_chapter3_learning_without_ranked_normal_mode():
    catalog = public_catalog(surface=SURFACE_MINIAPP)
    chapter3 = next(
        course
        for group in catalog["groups"]
        for course in group["courses"]
        if course["key"] == "chapter3"
    )
    assert chapter3["scoring_mode"] == "learning"
    assert chapter3["points_per_question"] == 0
    assert '<div id="courseMenu"' in MINIAPP_INDEX
    assert '<script src="course_catalog.js"></script>' in MINIAPP_INDEX
    assert "chapter3.js" not in MINIAPP_INDEX
    assert "buildCourseStartPayload" in MINIAPP_CATALOG
    assert "course_key: course.key" in MINIAPP_CATALOG
    assert "ranked:" not in MINIAPP_CATALOG
    assert "scoring_mode:" not in MINIAPP_CATALOG
    assert "api('/api/catalog')" in MINIAPP_APP


def test_release_audit_makes_no_new_gameplay_or_merge_claim():
    assert MANIFEST["status"] == "CHAPTER3_RELEASE_AUDITED_STACK_NOT_MERGED"
    assert MANIFEST["release_audit_changes_gameplay"] is False
    assert MANIFEST["merge_authorized"] is False
    assert MANIFEST["main_mutated"] is False
    assert MANIFEST["surface_notes"]["miniapp_chapter3"] == "ADMITTED"
    assert MANIFEST["surface_notes"]["api_pool_chapter3"] == "ADMITTED"
    assert MANIFEST["surface_notes"]["legacy_telegram_menu"] == "NOT_REWRITTEN_MONOLITHICALLY"
