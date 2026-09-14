"""Offline check of the bank's parse answers against the vendored MorphGNT rows.

``scripts/verify_parse_claims.py`` holds the checker; this file runs it and pins
what it verified. The point of the pin is coverage: the checker is deliberately
conservative (a multi-form card is reported as INFO and skipped, a card without a
corpus row for its form is reported as INFO), so every skip is listed here with a
reason and a sharpened checker cannot quietly verify fewer cards.

Two things are pinned beyond the counts:

* the parse facts the lessons teach, card by card - the keyed option must state
  exactly the tense/voice/mood/person/number/case/gender the corpus row carries,
  so a wording edit that flips a grammatical label fails here;
* the mutations the checker has to catch - a wrong feature, a wrong lemma, and a
  distractor that restates the corpus parse as a bare label.

Chapter-3 wording is localized in the reviewed source layers on this branch;
nothing here rewrites a card. Every claim below is read from the effective
reviewed pools as they are.
"""

from __future__ import annotations

import copy
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts" / "verify_parse_claims.py"

# Verified coverage. ``cards_with_parse_claim`` counts the cards whose keyed option
# states morphology for a form that resolves to a corpus row and whose anchor names
# a 1 Peter verse.
VERIFIED_CARDS = 108
CORPUS_ROWS = 1134

# Cards the checker reports as context instead of verifying, with the reason. A new
# entry may only be added with the same kind of reason: the card names no form, or
# it asks about several forms at once so no single row is the reference.
KNOWN_INFO: frozenset[tuple[str, str, str]] = frozenset(
    {
        # The stem names no Greek form; the claim is about a construction.
        ("parse.card_without_form", "chapter3", "ch3_gr_201"),
        ("parse.card_without_form", "chapter3", "ch3_ot_203"),
        ("parse.card_without_form", "chapter3", "ch3_theol_203"),
        ("parse.card_without_form", "chapter5", "ch5_w3q_069"),
        ("parse.card_without_form", "chapter5", "ch5_w3q_126"),
        ("parse.card_without_form", "chapter5", "ch5_w3q_142"),
        ("parse.card_without_form", "chapter3", "ch3_theol_201"),
        ("parse.card_without_form", "chapter5", "ch5_w3q_112"),
        ("parse.card_without_form", "hard_p1", "hard_deep_04"),
        ("parse.card_without_form", "hard_p2", "hard17_03"),
        # Several forms are parsed at once ("what do παυσάτω, ἐκκλινάτω ... share").
        ("parse.multi_form_card", "chapter3", "ch3_gr_111"),
        ("parse.multi_form_card", "chapter3", "ch3_ot_202"),
        ("parse.multi_form_card", "chapter3", "ch3_gr_302"),
        ("parse.multi_form_card", "chapter3", "ch3_gr_305"),
        ("parse.multi_form_card", "chapter3", "ch3_disp_005"),
        ("parse.multi_form_card", "chapter3", "ch3_gr_307"),
        ("parse.multi_form_card", "chapter3", "ch3_gr_308"),
        ("parse.multi_form_card", "chapter3", "ch3_app_301"),
        ("parse.multi_form_card", "chapter5", "ch5_w3q_084"),
        ("parse.multi_form_card", "chapter5", "ch5_w3q_085"),
        ("parse.multi_form_card", "linguistics_ch1_2", "ling2_04"),
        ("parse.multi_form_card", "linguistics_ch1_3", "ling3_14"),
    }
)

# (card id, the form, the features the keyed option states, the parse code the
# corpus has for that form in the card's verse). Features are CCAT letters.
PARSE_FACTS: tuple[tuple[str, str, dict[str, str], str], ...] = (
    ("ch2_gr_001", "ἐπιποθήσατε", {"tense": "A", "voice": "A", "mood": "D", "person": "2", "number": "P"}, "2AAD-P--"),
    ("ch2_gr_004", "αὐξηθῆτε", {"tense": "A", "voice": "P", "mood": "S", "person": "2", "number": "P"}, "2APS-P--"),
    ("ch2_gr_005", "οἰκοδομεῖσθε", {"tense": "P", "voice": "P", "mood": "I", "person": "2", "number": "P"}, "2PPI-P--"),
    ("ch2_gr_008", "ἠλεημένοι", {"tense": "X", "voice": "P", "mood": "P", "case": "N", "number": "P"}, "-XPPNPM-"),
    ("ch2_gr_010", "Ὑποτάγητε", {"tense": "A", "voice": "P", "mood": "D", "person": "2", "number": "P"}, "2APD-P--"),
    ("ch2_gr_015", "ἀνήνεγκεν", {"tense": "A", "voice": "A", "mood": "I", "person": "3", "number": "S"}, "3AAI-S--"),
    ("ch3_gr_103", "ὑποτασσόμεναι", {"tense": "P", "voice": "P", "mood": "P", "case": "N", "number": "P", "gender": "F"}, "-PPPNPF-"),
    ("ch3_gr_117", "συγκληρονόμοις", {"case": "D", "number": "P", "gender": "M"}, "----DPM-"),
    ("ch3_gr_204", "ἐκλήθητε", {"tense": "A", "voice": "P", "mood": "I", "person": "2", "number": "P"}, "2API-P--"),
    ("ch3_gr_205", "κληρονομήσητε", {"tense": "A", "voice": "A", "mood": "S", "person": "2", "number": "P"}, "2AAS-P--"),
    ("ch3_gr_301", "ἁγιάσατε", {"tense": "A", "voice": "A", "mood": "D", "number": "P"}, "2AAD-P--"),
    ("ch3_gr_309", "θέλοι", {"tense": "P", "voice": "A", "mood": "O", "person": "3", "number": "S"}, "3PAO-S--"),
    ("ch3_gr_002", "προσαγάγῃ", {"tense": "A", "voice": "A", "mood": "S", "number": "S"}, "3AAS-S--"),
    ("ch3_gr_003", "ἐκήρυξεν", {"tense": "A", "voice": "A", "mood": "I", "number": "S"}, "3AAI-S--"),
    ("ch3_gr_004", "διεσώθησαν", {"tense": "A", "voice": "P", "mood": "I", "number": "P"}, "3API-P--"),
    ("ch3_gr_005", "ἀντίτυπον", {"case": "N", "number": "S", "gender": "N"}, "----NSN-"),
    ("ch3_gr_006", "ἐπερώτημα", {"case": "N", "number": "S", "gender": "N"}, "----NSN-"),
    ("ch3_gr_008", "θανατωθείς", {"tense": "A", "voice": "P", "mood": "P", "case": "N", "number": "S", "gender": "M"}, "-APPNSM-"),
    ("ch3_gr_010", "ᾧ", {"case": "D", "number": "S", "gender": "N"}, "----DSN-"),
    ("ch3_gr_011", "πνεύμασιν", {"case": "D", "number": "P", "gender": "N"}, "----DPN-"),
    ("ch3_gr_012", "ἀπειθήσασίν", {"tense": "A", "voice": "A", "case": "D", "number": "P", "gender": "M"}, "-AAPDPM-"),
    ("ch3_gr_014", "ὑποταγέντων", {"tense": "A", "voice": "P", "case": "G", "number": "P", "gender": "M"}, "-APPGPM-"),
    ("ch3_gr_015", "πορευθείς", {"tense": "A", "voice": "P", "case": "N", "number": "S", "gender": "M"}, "-APPNSM-"),
    ("ch3_disp_001", "πνεύμασιν", {"case": "D", "number": "P", "gender": "N"}, "----DPN-"),
    ("ch4_gr_001", "πέπαυται", {"tense": "X", "voice": "M", "mood": "I", "person": "3", "number": "S"}, "3XMI-S--"),
    ("ch4_gr_002", "εὐηγγελίσθη", {"tense": "A", "voice": "P", "mood": "I", "person": "3", "number": "S"}, "3API-S--"),
    ("ch4_gr_004", "ὁπλίσασθε", {"tense": "A", "voice": "M", "mood": "D", "person": "2", "number": "P"}, "2AMD-P--"),
    ("ch4_gr_005", "ἤγγικεν", {"tense": "X", "voice": "A", "mood": "I", "person": "3", "number": "S"}, "3XAI-S--"),
    ("ch4_gr_006", "ἀναπαύεται", {"tense": "P", "voice": "M", "mood": "I", "person": "3", "number": "S"}, "3PMI-S--"),
)


def _checker():
    if "_parse_claims" in sys.modules:
        return sys.modules["_parse_claims"]
    spec = importlib.util.spec_from_file_location("_parse_claims", CHECKER)
    module = importlib.util.module_from_spec(spec)
    sys.modules["_parse_claims"] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _cards_by_id() -> dict[str, dict]:
    checker = _checker()
    return {
        card["id"]: card
        for pool, cards in checker.pool_cards().items()
        for card in cards
    }


def test_the_bank_has_no_parse_answer_that_contradicts_the_corpus():
    checker = _checker()
    findings = checker.audit()
    blocking = [finding for finding in findings if finding.severity == checker.FINDING_BLOCKING]
    assert not blocking, blocking


def test_every_skip_is_a_reviewed_one():
    checker = _checker()
    info = {
        (finding.check_id, finding.pool, finding.card_id)
        for finding in checker.audit()
        if finding.severity == checker.FINDING_INFO
    }
    assert info == set(KNOWN_INFO), sorted(info ^ set(KNOWN_INFO))


def test_verified_coverage_is_pinned():
    checker = _checker()
    stats = checker.coverage()
    assert stats["cards_with_parse_claim"] == VERIFIED_CARDS
    assert stats["corpus_rows"] == CORPUS_ROWS


def test_keyed_parse_answers_match_the_corpus_row():
    checker = _checker()
    corpus = checker.load_corpus()
    cards = _cards_by_id()
    for card_id, form, expected, code in PARSE_FACTS:
        card = cards[card_id]
        refs = checker.first_peter_refs(str(card.get("verse") or ""))
        rows = [
            row
            for ref in refs
            for row in corpus.get((ref, checker._normalize_greek(form)), ())
        ]
        assert rows, f"{card_id}: no corpus row for {form} at {card['verse']!r}"
        row = rows[0]
        assert row["parse"] == code, (card_id, row["parse"], code)
        keyed = str(card["options"][card["correct"]])
        claimed = checker._claimed_features(keyed)
        for name, letter in expected.items():
            assert claimed.get(name) == letter, (card_id, name, claimed.get(name), letter)
        assert checker._row_matches(row, claimed, checker._lemma_of(keyed)), (
            card_id,
            row,
            claimed,
        )
        # The card may not hedge: every feature it names is in the row above, and
        # nothing it names contradicts the row.
        assert not checker._specified_conflicts(row, claimed), (card_id, row, claimed)


def test_a_flipped_label_is_caught():
    checker = _checker()
    corpus = checker.load_corpus()
    card = copy.deepcopy(_cards_by_id()["ch2_gr_001"])
    flipped = str(card["options"][card["correct"]]).replace("активный", "пассивный")
    assert flipped != card["options"][card["correct"]]
    card["options"][card["correct"]] = flipped
    findings = checker.audit_card(card, "chapter2", corpus)
    assert [finding.check_id for finding in findings] == ["parse.claim_mismatch"], findings


def test_a_wrong_lemma_is_caught():
    checker = _checker()
    corpus = checker.load_corpus()
    card = copy.deepcopy(_cards_by_id()["ch4_gr_002"])
    card["options"][card["correct"]] = str(card["options"][card["correct"]]).replace(
        "от εὐαγγελίζω", "от παύω"
    )
    findings = checker.audit_card(card, "chapter4", corpus)
    assert [finding.check_id for finding in findings] == ["parse.lemma_mismatch"], findings


def test_a_label_distractor_cannot_restate_the_corpus_parse():
    """A second option that is the same label-shaped parse is reported.

    The bank's grammar cards are parse drills: the wrong labels must be
    distinguishable by the features they name. A slot that repeats the keyed label
    is therefore a defect, and the checker has to see it.
    """

    checker = _checker()
    corpus = checker.load_corpus()
    card = copy.deepcopy(_cards_by_id()["ch4_gr_005"])
    keyed = str(card["options"][card["correct"]])
    assert checker._label_shaped(keyed)
    card["options"][0] = keyed
    findings = checker.audit_card(card, "chapter4", corpus)
    assert [finding.check_id for finding in findings] == [
        "parse.distractor_matches_corpus"
    ], findings


def test_a_sentence_about_a_form_is_not_a_second_parse_label():
    """A prose overclaim may quote a correct parse without becoming a second key."""

    checker = _checker()
    corpus = checker.load_corpus()
    card = copy.deepcopy(_cards_by_id()["ch4_gr_005"])
    keyed = str(card["options"][card["correct"]])
    prose = (
        keyed
        + ", поэтому один этот морфологический разбор якобы обязательно решает "
          "всю интерпретацию стиха"
    )
    assert checker._claimed_features(prose) == checker._claimed_features(keyed)
    assert checker._label_shaped(keyed)
    assert not checker._label_shaped(prose)
    card["options"][0] = prose
    findings = checker.audit_card(card, "chapter4", corpus)
    assert not [
        finding
        for finding in findings
        if finding.check_id == "parse.distractor_matches_corpus"
    ], findings


def test_pronoun_subtypes_do_not_collapse_into_duplicate_parse_answers():
    """Case/number/gender agreement does not erase relative-vs-personal pronoun type."""

    checker = _checker()
    corpus = checker.load_corpus()
    card = _cards_by_id()["ch3_gr_010"]
    keyed = checker._claimed_features(str(card["options"][card["correct"]]))
    personal = checker._claimed_features(str(card["options"][0]))
    assert keyed.get("pronoun_kind") == "RR"
    assert personal.get("pronoun_kind") == "RP"
    findings = checker.audit_card(card, "chapter3", corpus)
    assert not [
        finding
        for finding in findings
        if finding.check_id == "parse.distractor_matches_corpus"
    ], findings
