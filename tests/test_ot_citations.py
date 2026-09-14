"""Offline guard for the Old Testament / LXX citations of the bank.

The corpus those citations were verified against is not vendored: the upstream
Rahlfs dataset is CC BY-NC-SA 4.0 and derives from CCAT material with its own
access terms (see ``scripts/verify_lxx_evidence.py``). What is stored is the
record of the last corpus run - the references each card cites and a SHA-256 of
the Greek the card quotes - so any later edit to one of those cards makes the
offline check ask for a fresh corpus run instead of silently keeping a citation
nobody re-read.

These tests are the offline half. The corpus half is one command:

    python scripts/verify_lxx_evidence.py --corpus <lxx.zip> \\
        --versification <versification.csv>
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts" / "verify_lxx_evidence.py"
FINGERPRINTS = ROOT / "data" / "ot-citation-fingerprints.json"

# Findings that may appear in a corpus run without blocking it: a word quoted in
# an option that names no verse, and a dictionary form whose stem the cited
# verses carry. Anything else means the run found a misquotation.
INFORMATIONAL_CHECKS = {"lxx.word_in_option_not_in_verses", "lxx.lemma_reference"}

# Cards whose attribution was checked against Rahlfs, with the verses they rest
# on. These are the reference claims the questions' answers depend on.
REVIEWED_REFERENCES = {
    "ling3_02": ["2.6.6"],  # λυτρόω in the exodus redemption (Exod 6:6)
    "ch2_gr_016": ["23.53.5"],  # τῷ μώλωπι of 1 Pet. 2:24 and Isaiah 53:5
    "ch3_ot_101": ["1.18.12"],  # Sarah's words in Gen 18:12
    "ch3_ot_104": ["20.3.25"],  # the φοβ-/πτόησις link with Prov 3:25
    "ch2_ot_008": ["23.53.9"],  # Isaiah 53:9 behind 1 Pet. 2:22
    "ch4_ot_001": ["20.10.12"],  # Prov 10:12 behind 1 Pet. 4:8
    "ch4_ot_002": ["20.11.31"],  # Prov 11:31 behind 1 Pet. 4:18
}


def _checker():
    if "_lxx_evidence" in sys.modules:
        return sys.modules["_lxx_evidence"]
    sys.path.insert(0, str(ROOT))
    spec = importlib.util.spec_from_file_location("_lxx_evidence", CHECKER)
    module = importlib.util.module_from_spec(spec)
    sys.modules["_lxx_evidence"] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _record() -> dict:
    return json.loads(FINGERPRINTS.read_text(encoding="utf-8"))


def test_no_ot_card_changed_since_the_last_corpus_run():
    """A changed quotation fails here and asks for a fresh corpus run."""
    module = _checker()
    findings = module.check_fingerprints(module.Evidence.load(module.EVIDENCE_PATH))
    assert not findings, [f"{f.check_id}: {f.item_id} - {f.message}" for f in findings]


def test_fingerprint_record_covers_every_ot_anchored_card():
    """The record must describe exactly the cards that cite an OT text."""
    module = _checker()
    record = _record()
    current = module.fingerprints(module.Evidence.load(module.EVIDENCE_PATH))
    assert set(record["cards"]) == set(current), "re-run the corpus check to refresh the record"
    for card_id, entry in record["cards"].items():
        assert entry["refs"], f"{card_id} is recorded without a reference"
        assert len(entry["quoted_sha256"]) == 64


def test_recorded_corpus_run_is_reproducible_and_clean():
    """The saved run names its corpus and reported no blocking finding."""
    provenance = _record()["provenance"]
    assert len(provenance["corpus_sha256"]) == 64
    assert len(provenance["versification_sha256"]) == 64
    assert provenance["verses_checked"] >= 100
    assert provenance["cards"] == len(_record()["cards"])
    assert set(provenance["findings"]) <= INFORMATIONAL_CHECKS, provenance["findings"]
    assert "not vendored" in provenance["note"]


def test_reviewed_cards_cite_the_verses_they_were_verified_against():
    """The reference attribution of the reviewed cards stays put."""
    record = _record()["cards"]
    for card_id, references in REVIEWED_REFERENCES.items():
        assert card_id in record, f"{card_id} is not in the citation record"
        assert references[0] in record[card_id]["refs"], (
            f"{card_id} no longer cites {references[0]}: {record[card_id]['refs']}"
        )


def test_a_changed_quotation_is_detected(monkeypatch):
    """The offline guard itself fails when a quotation changes under it."""
    module = _checker()
    real = list(module._bank())
    doctored = [
        (pool, dict(card, explanation=f"{card.get('explanation') or ''} καὶ ἐγκώμιον"))
        if card["id"] == "ch3_ot_104"
        else (pool, card)
        for pool, card in real
    ]
    monkeypatch.setattr(module, "_bank", lambda: iter(doctored))
    findings = module.check_fingerprints(module.Evidence.load(module.EVIDENCE_PATH))
    assert any(f.check_id == "lxx.quotations_changed" and f.item_id == "ch3_ot_104" for f in findings), findings


def test_option_words_of_the_parsing_card_are_recorded_as_reviewed():
    """The one card that quotes single distractor words is a known, seen case."""
    provenance = _record()["provenance"]
    assert provenance["findings"].get("lxx.word_in_option_not_in_verses", 0) >= 0
    assert set(provenance["findings"]) <= INFORMATIONAL_CHECKS
