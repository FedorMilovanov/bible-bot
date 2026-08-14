from questions import (
    BATTLE_POOL,
    CHALLENGE_FALLBACK_POOL,
    CHALLENGE_POOLS,
    COMPETITIVE_POOL,
    POOL_REGISTRY,
)
from questions.chapter4.authoring import CHAPTER4_STAGING_QUESTIONS, answer_position_counts
from questions.chapter4.research_handoff import CHAPTER4_EFFECTIVE_RESEARCH_RECORDS
from questions.chapter4.reviewed import CHAPTER4_REVIEWED_QUESTIONS
from questions.source_registry import SOURCE_CATALOG


REQUIRED_FIELDS = {
    "id",
    "research_id",
    "question",
    "options",
    "correct",
    "explanation",
    "verse",
    "topic",
    "domain",
    "claim_type",
    "confidence",
    "position",
    "competitive",
    "sources",
    "evidence_lane",
    "inspection_depth",
}


def _ids(items):
    return {str(item.get("id") or "") for item in items}


def test_chapter4_authored_bank_is_complete_unique_and_well_formed():
    assert len(CHAPTER4_STAGING_QUESTIONS) == 52
    assert len(_ids(CHAPTER4_STAGING_QUESTIONS)) == 52
    research_ids = [str(item.get("research_id") or "") for item in CHAPTER4_STAGING_QUESTIONS]
    assert len(research_ids) == len(set(research_ids)) == 52

    effective_ids = {str(record["id"]) for record in CHAPTER4_EFFECTIVE_RESEARCH_RECORDS}
    for item in CHAPTER4_STAGING_QUESTIONS:
        assert REQUIRED_FIELDS <= item.keys()
        assert item["research_id"] in effective_ids
        assert isinstance(item["question"], str) and item["question"].strip()
        assert isinstance(item["explanation"], str) and item["explanation"].strip()
        assert isinstance(item["options"], list) and len(item["options"]) == 4
        assert all(isinstance(option, str) and option.strip() for option in item["options"])
        assert len({option.strip().casefold() for option in item["options"]}) == 4
        assert isinstance(item["correct"], int) and 0 <= item["correct"] <= 3
        assert item["competitive"] is False
        assert item["sources"]
        assert str(item["evidence_lane"]).strip()
        assert str(item["inspection_depth"]).strip()


def test_chapter4_answer_positions_are_not_runtime_shuffle_dependent():
    counts = answer_position_counts()
    assert set(counts) == {0, 1, 2, 3}
    assert sum(counts.values()) == 52
    assert all(count > 0 for count in counts.values())


def test_every_authored_source_resolves_in_canonical_source_registry():
    missing = {
        source_id
        for item in CHAPTER4_STAGING_QUESTIONS
        for source_id in item["sources"]
        if source_id not in SOURCE_CATALOG
    }
    assert missing == set()


def test_reviewed_bank_isolated_from_staging_objects():
    staged = {item["id"]: item for item in CHAPTER4_STAGING_QUESTIONS}
    reviewed = {item["id"]: item for item in CHAPTER4_REVIEWED_QUESTIONS}
    assert staged.keys() == reviewed.keys()
    assert len(reviewed) == 52

    for qid in staged:
        assert staged[qid] is not reviewed[qid]
        assert staged[qid]["options"] is not reviewed[qid]["options"]
        assert staged[qid]["sources"] is not reviewed[qid]["sources"]
        assert reviewed[qid]["competitive"] is False
        assert reviewed[qid]["competitive_status"] == "not_yet_admitted"
        assert reviewed[qid]["review_status"] == "learning_ready"


def test_sensitive_verse_boundaries_are_explicit_and_fail_closed():
    reviewed_by_research = {item["research_id"]: item for item in CHAPTER4_REVIEWED_QUESTIONS}

    verse_4_6 = reviewed_by_research["w3q_013"]
    assert verse_4_6["position"] == "project"
    assert verse_4_6["confidence"] == "contested"
    assert verse_4_6["question"].startswith("[Позиция курса]")
    assert "Horrell" in verse_4_6["explanation"]

    verse_4_14_ecm = reviewed_by_research["w3q_031"]
    assert verse_4_14_ecm["competitive"] is False
    assert verse_4_14_ecm["domain"] == "textual_criticism"
    assert "manuscript unanimity" in verse_4_14_ecm["explanation"]

    verse_4_14_sinaiticus = reviewed_by_research["w3q_137"]
    assert verse_4_14_sinaiticus["competitive"] is False
    assert "w3i_sinaiticus_1p4_5" in verse_4_14_sinaiticus["sources"]
    assert "один свидетель" in verse_4_14_sinaiticus["explanation"]

    verse_4_16 = reviewed_by_research["w3q_121"]
    assert verse_4_16["competitive"] is False
    assert "SBLGNT" in verse_4_16["explanation"]
    assert "ECM/NA28" in verse_4_16["explanation"]


def test_chapter4_has_zero_paths_into_competitive_surfaces():
    chapter4_ids = _ids(POOL_REGISTRY["chapter4"])
    challenge_ids = set().union(*(_ids(pool) for pool in CHALLENGE_POOLS.values()))
    assert chapter4_ids
    assert chapter4_ids.isdisjoint(_ids(POOL_REGISTRY["random_all"]))
    assert chapter4_ids.isdisjoint(_ids(COMPETITIVE_POOL))
    assert chapter4_ids.isdisjoint(_ids(BATTLE_POOL))
    assert chapter4_ids.isdisjoint(challenge_ids)
    assert chapter4_ids.isdisjoint(_ids(CHALLENGE_FALLBACK_POOL))
