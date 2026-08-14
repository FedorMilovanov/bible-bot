from questions.chapter5.authority import (
    EFFECTIVE_RESEARCH,
    HISTORICAL_HOLD_IDS,
    TEXTUAL_CONTROL,
    WAVE3N_CLOSED_IDS,
    resolve_effective,
)
from questions.chapter5.bank import CHAPTER5_STAGING_QUESTIONS
from questions.chapter5.reviewed import CHAPTER5_REVIEWED_QUESTIONS
from questions.chapter5.sources import SOURCE_CATALOG


def test_effective_authority_exactly_72_unique_records():
    assert len(EFFECTIVE_RESEARCH) == 72
    assert len({r["candidate_id"] for r in EFFECTIVE_RESEARCH}) == 72
    assert len(CHAPTER5_STAGING_QUESTIONS) == 72
    assert {q["research_candidate_id"] for q in CHAPTER5_STAGING_QUESTIONS} == {
        r["candidate_id"] for r in EFFECTIVE_RESEARCH
    }


def test_historical_holds_remain_visible_but_wave3n_controls_effective_status():
    assert HISTORICAL_HOLD_IDS == {"w3q_050", "w3q_051", "w3q_075"}
    assert WAVE3N_CLOSED_IDS == HISTORICAL_HOLD_IDS
    for candidate_id in HISTORICAL_HOLD_IDS:
        record = resolve_effective(candidate_id)
        assert record["historical_status"] == "HOLD"
        assert record["effective_status"] == "READY_NONCOMPETITIVE"


def test_5_2_textual_units_are_distinct():
    assert TEXTUAL_CONTROL["5:2A"]["candidate_id"] == "w3q_050"
    assert TEXTUAL_CONTROL["5:2A"]["unit"] == "ἐπισκοποῦντες"
    assert TEXTUAL_CONTROL["5:2B"]["candidate_id"] == "w3q_051"
    assert TEXTUAL_CONTROL["5:2B"]["unit"] == "κατὰ θεόν"
    assert TEXTUAL_CONTROL["5:2A"]["unit"] != TEXTUAL_CONTROL["5:2B"]["unit"]


def test_textual_routes_do_not_claim_direct_decm_readback_or_unanimity():
    assert "not direct dECM" in TEXTUAL_CONTROL["5:2A"]["route"]
    assert "not direct dECM" in TEXTUAL_CONTROL["5:12"]["route"]
    assert "no manuscript-unanimity" in TEXTUAL_CONTROL["5:10"]["route"]


def test_5_10_card_is_edition_bounded():
    card = next(q for q in CHAPTER5_REVIEWED_QUESTIONS if q["research_candidate_id"] == "w3q_068")
    text = " ".join([card["question"], card["explanation"], *card["options"]])
    assert "SBLGNT" in text
    assert "MorphGNT" in text
    assert "рукопис" in text.lower() or "manuscript" in text.lower()


def test_5_12_card_distinguishes_ecm_based_treatment():
    card = next(q for q in CHAPTER5_REVIEWED_QUESTIONS if q["research_candidate_id"] == "w3q_075")
    text = " ".join([card["question"], card["explanation"], *card["options"]])
    assert "στῆτε" in text and "ἑστήκατε" in text
    assert "direct dECM" in text


def test_every_card_source_id_is_lane_known():
    for card in CHAPTER5_REVIEWED_QUESTIONS:
        assert card["sources"]
        assert set(card["sources"]) <= set(SOURCE_CATALOG)
