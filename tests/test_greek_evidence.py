"""Offline corpus check for the Greek claims of the question bank.

``scripts/verify_greek_evidence.py`` holds the checker; this file runs it against
the vendored MorphGNT/SBLGNT excerpt and pins the editorial corrections that came
out of it. The corrections were real: four stems and two explanations cited forms
1 Peter does not read (a genitive where the text has a dative, a nominative where
the text has a dative plural, ``κάλυμμα`` for ``ἐπικάλυμμα``, the feminine
``τιμία`` for the dative ``τιμίῳ``, a nominative title for the dative of 4:19).
Each one now has a test so it cannot come back.

Chapter 5 stays excluded by design: two of its cards quote readings that
SBLGNT does not print, because the text-critical point of the card is precisely
that reading versus the SBLGNT base (ἑστήκατε as a competing 5:12 reading,
ἐκκλησία as the Sinaiticus 5:13 reading). They survived the 2026-09 wiseness
release repin with the raw-bank blob 1dbe41b5d296ef9a2738f68f3fef807fde030dce.
The pinned list below is explicit, so a new Greek finding outside it fails this
suite.
"""
from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import questions

ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts" / "verify_greek_evidence.py"

# Chapter-5 cards that intentionally cite a reading SBLGNT does not print:
# w3q_075 presents the στῆτε/ἑστήκατε variant pair and w3q_144 the Sinaiticus
# ἐκκλησία reading as a named-witness fact, never as the SBLGNT base text.
ACCEPTED_CHAPTER5_GREEK_DEBT = {
    "ch5_w3q_075": "ἑστήκατε",
    "ch5_w3q_144": "ἐκκλησία",
}

# (card id, field, the SBLGNT reading the card must cite, the form it must not).
CORRECTED_CITATIONS = (
    ("med_13", "question", "διασπορᾶς", "διασποράς"),
    ("intro3_01", "question", "παρεπιδήμοις", "παρεπίδημοι"),
    ("ling1_15", "question", "χαρᾷ ἀνεκλαλήτῳ", "χαρᾶς"),
    ("easy17_es2_04", "explanation", "τιμίῳ", "τιμία"),
    ("ch2_hist_004", "question", "παροίκους καὶ παρεπιδήμους", "πάροικοι"),
    ("ch2_app_006", "explanation", "ἐπικάλυμμα", "κάλυμμα κακίας"),
    ("ch4_theol_001", "explanation", "πιστῷ κτίστῃ", "πιστὸς κτίστης"),
)


def _checker():
    if "_greek_evidence" in sys.modules:
        return sys.modules["_greek_evidence"]
    spec = importlib.util.spec_from_file_location("_greek_evidence", CHECKER)
    module = importlib.util.module_from_spec(spec)
    sys.modules["_greek_evidence"] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _bank_by_id() -> dict[str, dict]:
    return {
        card["id"]: card
        for pool, cards in questions.POOL_REGISTRY.items()
        if pool not in {"random_all", "competitive_all", "easy", "medium", "hard", "practical_ch1"}
        for card in cards
    }


def test_evidence_excerpt_records_its_provenance():
    """The vendored rows must say where they came from and stay self-consistent."""
    module = _checker()
    evidence = module.Evidence.load()
    provenance = evidence.provenance
    assert provenance["source_url"].startswith("https://github.com/morphgnt/")
    assert "DOI: 10.5281/zenodo.376200" in provenance["citation"]
    assert "SBLGNT EULA" in provenance["license"] and "CC BY-SA 3.0" in provenance["license"]
    assert len(provenance["upstream_file_sha256"]) == 64
    assert provenance["upstream_rows"] >= provenance["excerpt_rows"] == len(evidence.rows)
    for row in evidence.rows:
        assert row.ref and row.pos and row.parse and row.word and row.lemma


def test_every_metadata_claim_matches_the_corpus():
    """Form, parse and lemma of every parsing claim must be the corpus reading."""
    module = _checker()
    findings = module.check_metadata_claims(module.Evidence.load())
    assert not findings, [f"{f.pool}/{f.item_id}: {f.message}" for f in findings]


def test_quoted_stems_match_the_corpus_outside_the_pinned_chapter5_debt():
    """A stem may not quote a form that 1 Peter does not have."""
    module = _checker()
    findings = module.check_quoted_forms(module.Evidence.load())
    observed = {f.item_id: f.message.split()[0] for f in findings}
    assert observed == ACCEPTED_CHAPTER5_GREEK_DEBT, observed


def test_reviewed_forms_have_the_corpus_row_the_correction_rests_on():
    """Every corrected form must exist in the excerpt as a 1 Peter form."""
    module = _checker()
    evidence = module.Evidence.load()
    index = evidence.by_word()
    for form in module.REVIEWED_FORMS:
        assert index.get(module.normalize(form)), f"{form} has no corpus row in the excerpt"


def test_cli_forces_utf8_even_under_a_legacy_windows_code_page():
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "cp1252"
    result = subprocess.run(
        [sys.executable, str(CHECKER)],
        cwd=ROOT,
        env=env,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
    stdout = result.stdout.decode("utf-8")
    assert "ἑστήκατε" in stdout
    assert "ἐκκλησία" in stdout


def test_corrected_cards_cite_the_sblgnt_reading():
    """The seven corrections stay applied, and the wrong form cannot return."""
    bank = _bank_by_id()
    for question_id, field, corrected, wrong in CORRECTED_CITATIONS:
        card = bank[question_id]
        if field == "explanation":
            text = str(card["explanation"])
        else:
            text = str(card[field])
        assert corrected in text, f"{question_id}: expected {corrected!r} in the {field}"
        assert wrong not in text, f"{question_id}: the wrong form {wrong!r} is back in the {field}"


def test_corrected_options_cite_the_sblgnt_reading():
    """Option-level corrections (both applied at the learner boundary) stay too."""
    bank = _bank_by_id()
    ch3 = bank["ch3_gr_119"]["options"]
    assert any("εἰς τὸ" in option for option in ch3)
    assert not any("εἰς τό" in option for option in ch3)
    ch4 = bank["ch4_lex_001"]["options"]
    assert any("ἀρχιποίμενος" in option for option in ch4)
    assert not any("ἀρχιποίμενες" in option for option in ch4)
    # 2:16 is nominative (ὡς ἐλεύθεροι ... ἀλλ' ὡς θεοῦ δοῦλοι); the distractor used the
    # accusative ἐλευθέρους, a case the letter never puts there.
    ch2 = bank["ch2_gr_009"]["options"]
    assert any("ὡς ἐλεύθεροι" in option for option in ch2)
    assert not any("ἐλευθέρους" in option for option in ch2)
    # 3:19 has πνεύμασιν; the answer may not call the lemma the form.
    ch3 = bank["ch3_disp_001"]
    assert "πνεύμασιν (лемма πνεῦμα)" in str(ch3["options"][ch3["correct"]])


def test_metadata_claims_do_not_rest_on_pool_aggregates():
    """The checker walks leaf pools once; aggregates would double-count a card."""
    from scripts.verify_greek_evidence import _bank

    ids = [card["id"] for _pool, card in _bank()]
    assert len(ids) == len(set(ids)), "a card is audited twice through an aggregate pool"
