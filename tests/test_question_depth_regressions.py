"""Regression ratchets for the depth/evidence pass over the editable pools.

``AGENTS.md`` §9 requires a content change to prove the regression it fixed
cannot come back. The depth pass lengthened explanations that only restated the
key, removed the option-length cue outside chapter 5, and gave the three
single-witness context cards a second source. These tests pin those invariants
against the production pools instead of against the audit's own counters, so a
later edit that reintroduces a one-line explanation, a length cue or a single
source fails here even before ``--check`` runs.

Chapter 5 is excluded on purpose. After the 2026-09 wiseness repin and the
learner-language micro-repin (current raw-bank blob
7a02ccdef4f1089e66a9df263bca683f2b1f0d51) its option-length
findings are zero; what remains is six Research-gated ``source_quorum`` majors
(second evidence edges can only arrive in a new vendored Research release) and
a handful of deliberate recall cards for entry-level learners. Asserting the
editable-pool invariants against it would only re-describe that boundary.
"""
from __future__ import annotations

import importlib.util
import statistics
import sys
from pathlib import Path

import questions
from questions.chapter4.learner_clarifications import (
    EXPLANATION_CLARIFICATIONS,
    PINNED_PHRASES,
)
from questions.history_witness_review import (
    HISTORY_WITNESS_OVERRIDES,
    HISTORY_WITNESS_SOURCE_CATALOG,
)
from questions.intro_balance_review import BALANCED_DISTRACTORS
from questions.option_balance_chapters import CHAPTER_BALANCED_OPTIONS
from questions.option_balance_review import BALANCED_OPTIONS
from questions.option_order_review import OPTION_ORDER_SLOT

ROOT = Path(__file__).resolve().parents[1]
AUDIT_SCRIPT = ROOT / "scripts" / "audit_question_quality.py"

# Aggregates repeat leaf cards; chapter 5 stays excluded by its Research-gated
# source boundary (see the module docstring).
AGGREGATE_POOLS = frozenset(
    {"random_all", "competitive_all", "easy", "medium", "hard", "practical_ch1"}
)
ACCEPTED_DEBT_POOL = "chapter5"

# The three context cards that rested on one primary witness before this pass.
SECOND_WITNESS_CARDS = {
    "geo_06": ("pleiades_gazetteer", "josephus_jewish_war_6"),
    "nero_12": ("suetonius_nero_7", "tacitus_annals_13"),
    "nero_13": ("tacitus_annals_15_60_64", "cassius_dio_roman_history_62"),
}


def _audit_module():
    """Load the audit engine so the tests and the gate share one threshold."""
    if "_depth_audit" in sys.modules:
        return sys.modules["_depth_audit"]
    spec = importlib.util.spec_from_file_location("_depth_audit", AUDIT_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules["_depth_audit"] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _editable_cards():
    """Yield ``(pool, card)`` for every production pool outside chapter 5."""
    for pool, cards in questions.POOL_REGISTRY.items():
        if pool in AGGREGATE_POOLS or pool == ACCEPTED_DEBT_POOL:
            continue
        for card in cards:
            yield pool, card


def _bank_by_id() -> dict[str, dict]:
    return {card["id"]: card for _, card in _editable_cards()}


def test_editable_pool_explanations_teach_the_card():
    """A short explanation is the defect this pass closed; it must not return."""
    minimum = _audit_module().EXPLANATION_MIN_CHARS
    short = {
        f"{pool}/{card['id']}": len(str(card.get("explanation") or ""))
        for pool, card in _editable_cards()
        if len(str(card.get("explanation") or "")) < minimum
    }
    assert not short, f"explanations under {minimum} chars: {short}"


def test_editable_pool_explanations_are_not_copied_between_cards():
    """Two cards in one pool must not share the same explanation text."""
    per_pool: dict[str, dict[str, list[str]]] = {}
    for pool, card in _editable_cards():
        text = str(card.get("explanation") or "").strip()
        per_pool.setdefault(pool, {}).setdefault(text, []).append(card["id"])
    duplicates = {
        f"{pool}/{text[:60]}": ids
        for pool, texts in per_pool.items()
        for text, ids in texts.items()
        if len(ids) > 1
    }
    assert not duplicates, f"copied explanations: {duplicates}"


def test_editable_pool_key_is_never_the_unique_longest_option():
    """The length cue stays closed: no answer is the longest by shape alone."""
    label_set_max = _audit_module().LABEL_SET_MAX_CHARS
    offenders: dict[str, str] = {}
    for pool, card in _editable_cards():
        options = [str(option) for option in card["options"]]
        lengths = [len(option) for option in options]
        if statistics.median(lengths) <= label_set_max:
            # Label sets (names, references, parse tags) cannot be length-balanced
            # without changing the fact being tested; the audit reports them as INFO.
            continue
        correct = lengths[card["correct"]]
        others = [length for index, length in enumerate(lengths) if index != card["correct"]]
        if correct > max(others):
            offenders[f"{pool}/{card['id']}"] = f"{correct} vs {max(others)}"
    assert not offenders, f"keyed option is uniquely longest: {offenders}"


def test_history_and_greek_cards_keep_the_two_source_quorum():
    """A history or Greek claim needs a second, independent witness."""
    catalog = questions.SOURCE_CATALOG
    missing: dict[str, list[str]] = {}
    for pool, card in _editable_cards():
        if card.get("review_record_id"):
            # Chapter-4 wording keeps its evidence behind the review record.
            continue
        if str(card.get("claim_type")) not in {"history", "greek"}:
            continue
        sources = [str(source) for source in card.get("sources") or ()]
        unresolved = sorted(set(sources) - set(catalog))
        if len(set(sources)) < 2 or unresolved:
            missing[f"{pool}/{card['id']}"] = [*sources, f"unresolved={unresolved}"]
    assert not missing, f"claims below the source quorum: {missing}"


def test_context_cards_keep_their_second_witness():
    """geo_06, nero_12 and nero_13 must not fall back to one source."""
    bank = _bank_by_id()
    catalog = questions.SOURCE_CATALOG
    for question_id, (original, added) in SECOND_WITNESS_CARDS.items():
        card = bank[question_id]
        sources = {str(source) for source in card["sources"]}
        assert {original, added} <= sources, f"{question_id} lost a witness: {sorted(sources)}"
        assert added in catalog, f"{question_id}: {added} is not a canonical source id"
        assert added in HISTORY_WITNESS_OVERRIDES[question_id]["sources"]
        entry = catalog[added]
        assert entry.get("title") and entry.get("kind"), f"{added} lacks catalogue metadata"


def test_history_witness_layer_adds_no_unknown_source():
    """Every identity the witness layer introduces resolves in the catalogue."""
    catalog = questions.SOURCE_CATALOG
    for source_id in HISTORY_WITNESS_SOURCE_CATALOG:
        assert source_id in catalog, f"{source_id} is not registered in SOURCE_CATALOG"


def test_option_balance_tables_never_name_the_keyed_option():
    """Balance tables only rewrite distractors; the reviewed key stays intact."""
    bank = _bank_by_id()
    for table_name, table in (
        ("option_balance_review", BALANCED_OPTIONS),
        ("option_balance_chapters", CHAPTER_BALANCED_OPTIONS),
        ("intro_balance_review", BALANCED_DISTRACTORS),
    ):
        assert table, f"{table_name} must not be empty"
        for question_id, raw in table.items():
            card = bank.get(question_id)
            assert card is not None, f"{table_name}: unknown card {question_id}"
            keyed = card["options"][card["correct"]]
            # The intro table stores the distractor set, the chapter-1/2-4 tables
            # store it per slot; both must land in the final option list.
            replacements = list(raw.values()) if isinstance(raw, dict) else list(raw)
            assert replacements, f"{table_name}/{question_id}: empty override"
            for text in replacements:
                assert text in card["options"], f"{table_name}/{question_id}: {text[:40]!r} missing"
                assert text != keyed, f"{table_name}/{question_id}: override names the key"


def test_option_order_slots_match_the_learner_surface():
    """The authoring rotation must keep each balanced card in its planned slot."""
    bank = _bank_by_id()
    assert len(OPTION_ORDER_SLOT) >= 250, "the rotation table must not silently shrink"
    for question_id, slot in OPTION_ORDER_SLOT.items():
        card = bank.get(question_id)
        assert card is not None, f"option_order_review: unknown card {question_id}"
        assert card["correct"] == slot, (
            f"option_order_review/{question_id}: keyed slot {card['correct']} != planned {slot}"
        )


def test_clarification_layers_only_rewrite_learner_wording():
    """Learner clarifications stay substantive and never echo the keyed option."""
    bank = _bank_by_id()
    minimum = _audit_module().EXPLANATION_MIN_CHARS
    for question_id, text in EXPLANATION_CLARIFICATIONS.items():
        card = bank.get(question_id)
        assert card is not None, f"learner_clarifications: unknown card {question_id}"
        assert len(text) >= minimum, f"{question_id}: clarification is too short"
        assert card["explanation"] == text, f"{question_id}: clarification was not applied"
        assert text != card["options"][card["correct"]]
    for question_id, phrases in PINNED_PHRASES.items():
        card = bank.get(question_id)
        assert card is not None, f"learner_clarifications: unknown pinned card {question_id}"
        blob = " ".join(
            [str(card["question"]), str(card["explanation"]), *map(str, card["options"])]
        )
        for phrase in phrases:
            assert phrase in blob, f"{question_id}: pinned phrase {phrase!r} disappeared"
