# ruff: noqa: RUF001
import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest

from questions import (
    BATTLE_POOL,
    CHALLENGE_FALLBACK_POOL,
    CHALLENGE_POOLS,
    COMPETITIVE_POOL,
    POOL_REGISTRY,
)
from questions.chapter5 import research_metadata_v2 as research_meta
from questions.chapter5 import review_contract_v2 as contract
from questions.chapter5.bank import CHAPTER5_STAGING_QUESTIONS
from questions.chapter5.reviewed import CHAPTER5_REVIEWED_QUESTIONS
from web_api.quiz import prepare_question, public_question


def _card(candidate_id: str) -> dict:
    return next(
        card for card in CHAPTER5_STAGING_QUESTIONS
        if card["research_candidate_id"] == candidate_id
    )


def _review(candidate_id: str) -> dict:
    card = _card(candidate_id)
    return contract.PRODUCT_REVIEW_RECORDS[card["id"]]


def _ids(items) -> set[str]:
    return {str(item["id"]) for item in items}


def _git_blob_sha(raw: bytes) -> str:
    header = f"blob {len(raw)}\0".encode()
    return hashlib.sha1(header + raw).hexdigest()


def test_exact_product_bank_blob_and_no_padding_machinery():
    bank_path = Path(__file__).resolve().parents[1] / "questions" / "chapter5" / "bank.py"
    raw = bank_path.read_bytes()
    assert _git_blob_sha(raw) == "b15a6200fb7e4fde3e0c9ce9298645f9d3ff47d9"
    source = raw.decode("utf-8")
    assert ".ljust(" not in source
    assert ".rjust(" not in source
    assert ".center(" not in source


def test_authority_digest_recomputes_from_exact_research_pins():
    payload = {
        "repository": contract.RESEARCH_REPOSITORY,
        "research_sha": contract.RESEARCH_AUTHORITY_SHA,
        "final_snapshot_blob": contract.RESEARCH_FINAL_SNAPSHOT_BLOB,
        "candidate_blobs": [
            [f"{start:03d}-{end:03d}", blob]
            for start, end, _path, blob in contract.RESEARCH_CANDIDATE_SHARDS
        ],
        "wave3n_override_blob": contract.WAVE3N_OVERRIDE_BLOB,
        "wave3n_quorum_blob": contract.WAVE3N_QUORUM_BLOB,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    assert hashlib.sha256(raw.encode()).hexdigest() == contract.RESEARCH_AUTHORITY_DIGEST


def test_all_72_cards_have_v2_trace_and_independent_research_metadata():
    research_meta.validate_all_research_metadata(CHAPTER5_STAGING_QUESTIONS)
    contract.validate_full_bank()
    assert len(CHAPTER5_STAGING_QUESTIONS) == 72
    assert len(CHAPTER5_REVIEWED_QUESTIONS) == 72
    assert len(contract.PRODUCT_REVIEW_RECORDS) == 72

    for card in CHAPTER5_STAGING_QUESTIONS:
        research_meta.validate_research_metadata(card)
        review = contract.PRODUCT_REVIEW_RECORDS[card["id"]]
        contract.validate_product_review(card, review)
        trace = contract.trace_product_card(card["id"])
        assert trace["PRODUCT_CARD"] == card["id"]
        assert trace["PRODUCT_REVIEW_RECORD"] == review["product_review_id"]
        assert trace["EFFECTIVE_RESEARCH_CLAIM"]["candidate_id"] == card["research_candidate_id"]
        assert trace["EFFECTIVE_RESEARCH_CLAIM"]["claim_digest"] == review["claim_digest"]
        assert trace["CLAIM_SOURCE_EDGES"]
        assert all(edge["owner_lane"] == contract.OWNING_LANE for edge in trace["CLAIM_SOURCE_EDGES"])
        assert review["research_authority_sha"] == contract.RESEARCH_AUTHORITY_SHA
        assert review["authority_digest"] == contract.RESEARCH_AUTHORITY_DIGEST
        assert review["safe_phrasing_review"] == "PASS_INDEPENDENT_CONTENT_READBACK"
        assert review["blacklist_review"] == "PASS_AUTHORITATIVE_SURFACE"
        assert review["ranking_disposition"] == contract.RANKING_DISPOSITION
        assert review["competitive"] is False


def test_post_green_source_minimum_findings_are_closed_in_card_and_review():
    expected_minimum = {
        "w3q_095": {"sblgnt", "morphgnt_1peter"},
        "w3q_127": {"sblgnt", "morphgnt_1peter", "w3g_step_varapp_1p5"},
    }
    for candidate_id, required in expected_minimum.items():
        card = _card(candidate_id)
        review = _review(candidate_id)
        assert required.issubset(set(card["sources"]))
        assert required.issubset(set(review["source_subset"]))
        edge_sources = {edge["source_id"] for edge in review["claim_source_edges"]}
        assert required.issubset(edge_sources)


def test_all_chapter5_authoring_strings_are_normalized():
    for card in CHAPTER5_STAGING_QUESTIONS:
        values = [
            card["id"], card["research_candidate_id"], card["question"], card["explanation"],
            *card["options"], *card["sources"],
        ]
        for value in values:
            assert isinstance(value, str)
            assert value
            assert value == value.strip()
        assert len({option.casefold() for option in card["options"]}) == 4


def test_answer_positions_are_authored_not_runtime_shuffled():
    assert [card["correct"] for card in CHAPTER5_STAGING_QUESTIONS] == [
        index % 4 for index in range(72)
    ]
    assert {
        position: sum(card["correct"] == position for card in CHAPTER5_STAGING_QUESTIONS)
        for position in range(4)
    } == {0: 18, 1: 18, 2: 18, 3: 18}


def test_all_32_research_prototypes_reconciled_and_rejected_templates_are_not_authority():
    audits = contract.PROTOTYPE_AUDIT_RECORDS
    assert len(audits) == 32
    assert set(audits) == set(contract.PROTOTYPE_TO_CANDIDATE)
    assert contract.REJECTED_PROTOTYPES == {"w3mcq_020", "w3mcq_027"}
    for prototype_id, record in audits.items():
        assert record["research_only"] is True
        assert record["publication_authority"] is False
        assert record["ranking_authority"] is False
        if prototype_id in contract.REJECTED_PROTOTYPES:
            assert record["prototype_disposition"] == "REJECTED_TEMPLATE_NOT_PUBLICATION_AUTHORITY"
            assert record["rewrite_family_disposition"] == "NEEDS_REWRITE_RESOLVED_BY_INDEPENDENT_PRODUCT_REWRITE"
            assert record["product_disposition"] == "INDEPENDENT_PRODUCT_REWRITE_ACCEPTED"

    for candidate_id in ("w3q_052", "w3q_066"):
        review = _review(candidate_id)
        assert review["prototype_publication_authority"] is False
        assert review["product_rewrite_disposition"] == "INDEPENDENT_PRODUCT_REWRITE_ACCEPTED"


def test_historical_holds_remain_visible_but_wave3n_closures_pin_effective_claims():
    assert contract.HISTORICAL_HOLD_IDS == {"w3q_050", "w3q_051", "w3q_075"}
    for candidate_id in contract.HISTORICAL_HOLD_IDS:
        locator = contract.claim_locator(candidate_id)
        assert locator["effective_override"]["blob_sha"] == contract.WAVE3N_OVERRIDE_BLOB
        assert locator["effective_override"]["json_pointer"].startswith("/overrides/")


def test_textual_unit_5_2_edges_are_independent():
    episkopountes = _review("w3q_050")
    kata_theon = _review("w3q_051")
    assert all(edge["textual_unit"] == "1Pet5:2:episkopountes" for edge in episkopountes["claim_source_edges"])
    assert all(edge["textual_unit"] == "1Pet5:2:kata-theon" for edge in kata_theon["claim_source_edges"])
    assert "w3n_stanojevic_ecm_2021" not in episkopountes["source_subset"]
    assert "w3n_stanojevic_ecm_2021" in kata_theon["source_subset"]


def test_chapter5_has_zero_random_battle_challenge_or_fallback_leakage():
    chapter5_ids = _ids(CHAPTER5_REVIEWED_QUESTIONS)
    assert POOL_REGISTRY["chapter5"] == list(CHAPTER5_REVIEWED_QUESTIONS)
    assert not chapter5_ids.intersection(_ids(POOL_REGISTRY["random_all"]))
    assert not chapter5_ids.intersection(_ids(COMPETITIVE_POOL))
    assert not chapter5_ids.intersection(_ids(BATTLE_POOL))
    assert not chapter5_ids.intersection(_ids(CHALLENGE_FALLBACK_POOL))
    for pool in CHALLENGE_POOLS.values():
        assert not chapter5_ids.intersection(_ids(pool))


def test_public_question_hides_answer_and_review_internals_before_answer_point():
    prepared = prepare_question(deepcopy(_card("w3q_050")))
    payload = public_question(prepared)
    assert set(payload) == {"id", "question", "options"}
    for forbidden in (
        "correct", "explanation", "sources", "research_candidate_id",
        "product_review_id", "claim_digest", "claim_source_edges",
        "claim_inspection_edge_ids", "authority_digest",
    ):
        assert forbidden not in payload
    assert all("ch5-edge-" not in str(value) for value in payload.values())


def test_mutation_rejects_stale_authority_digest():
    card = deepcopy(_card("w3q_046"))
    review = deepcopy(_review("w3q_046"))
    review["authority_digest"] = "0" * 64
    with pytest.raises(ValueError, match="authority digest"):
        contract.validate_product_review(card, review)


def test_mutation_rejects_wrong_claim_digest():
    card = deepcopy(_card("w3q_046"))
    review = deepcopy(_review("w3q_046"))
    review["claim_digest"] = "f" * 64
    with pytest.raises(ValueError, match="claim digest"):
        contract.validate_product_review(card, review)


def test_mutation_rejects_fake_edge():
    card = deepcopy(_card("w3q_046"))
    review = deepcopy(_review("w3q_046"))
    review["claim_inspection_edge_ids"] = ("ch5-edge-fake",)
    with pytest.raises(ValueError, match="inspection edge"):
        contract.validate_product_review(card, review)


def test_mutation_rejects_source_from_wrong_5_2_textual_unit():
    card = deepcopy(_card("w3q_050"))
    review = deepcopy(_review("w3q_050"))
    card["sources"].append("w3n_stanojevic_ecm_2021")
    review["source_subset"] = tuple(card["sources"])
    with pytest.raises(ValueError, match="different textual unit"):
        contract.validate_product_review(card, review)


def test_mutation_rejects_unqualified_direct_decm_wording():
    card = deepcopy(_card("w3q_050"))
    review = deepcopy(_review("w3q_050"))
    card["options"][card["correct"]] = "Проект напрямую прочитал полный dECM witness table и подтвердил чтение"
    with pytest.raises(ValueError, match="overclaims textual evidence"):
        contract.validate_product_review(card, review)


def test_mutation_rejects_manuscript_unanimity():
    card = deepcopy(_card("w3q_068"))
    review = deepcopy(_review("w3q_068"))
    card["options"][card["correct"]] = "Все известные рукописи имеют ровно эти четыре формы"
    with pytest.raises(ValueError, match="overclaims textual evidence"):
        contract.validate_product_review(card, review)


def test_mutation_rejects_project_to_neutral_even_if_review_is_changed_too():
    card = deepcopy(_card("w3q_054"))
    review = deepcopy(_review("w3q_054"))
    card["position"] = "neutral"
    review["claimed_position"] = "neutral"
    with pytest.raises(ValueError):
        research_meta.validate_research_metadata(card)


def test_mutation_rejects_claim_type_or_confidence_strengthening():
    card = deepcopy(_card("w3q_128"))
    card["claim_type"] = "text"
    with pytest.raises(ValueError, match="Research metadata drift"):
        research_meta.validate_research_metadata(card)

    card = deepcopy(_card("w3q_118"))
    card["confidence"] = "medium"
    with pytest.raises(ValueError, match="Research metadata drift"):
        research_meta.validate_research_metadata(card)


def test_mutation_rejects_competitive_true():
    card = deepcopy(_card("w3q_046"))
    review = deepcopy(_review("w3q_046"))
    card["competitive"] = True
    review["competitive"] = True
    with pytest.raises(ValueError, match="no competitive authority"):
        contract.validate_product_review(card, review)


def test_mutation_rejects_outer_whitespace():
    card = deepcopy(_card("w3q_046"))
    review = deepcopy(_review("w3q_046"))
    card["question"] += " "
    with pytest.raises(ValueError, match="normalized text"):
        contract.validate_product_review(card, review)


def test_mutation_rejects_duplicate_option_surfaces():
    card = deepcopy(_card("w3q_046"))
    review = deepcopy(_review("w3q_046"))
    card["options"][1] = card["options"][0]
    with pytest.raises(ValueError, match="four unique"):
        contract.validate_product_review(card, review)


def test_mutation_rejects_wrong_answer_position_metadata():
    card = deepcopy(_card("w3q_046"))
    review = deepcopy(_review("w3q_046"))
    card["correct"] = 1
    review["correct_position"] = 1
    with pytest.raises(ValueError, match="answer-position metadata"):
        contract.validate_product_review(card, review)
