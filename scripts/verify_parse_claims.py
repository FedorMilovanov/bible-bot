"""Check the bank's parse answers against the vendored MorphGNT rows.

The course teaches morphology from 1 Peter, so a parse card is only as good as
the corpus row behind it: the keyed option of every parse card is a claim about
tense, voice, mood, person, number, case, gender, part of speech or lemma of one
Greek form in one verse. Nothing in the repository checked that claim against the
morphology the bank says it uses. This script does: it reads the vendored
MorphGNT excerpt (``data/morphgnt-1peter-evidence.json``, the same rows the Greek
citation check uses), finds the form each card keys, and compares the claim with
the row.

What is checked per parse card
------------------------------
* the form the card names is present at the verse the card anchors (otherwise the
  card parses a word the verse does not have);
* every feature the keyed option names - part of speech, tense, voice, mood,
  person, number, case, gender - matches the corpus parse code;
* when the option names a lemma ("... от παύω") it matches the row lemma;
* in a card whose stem lists several forms ("what do παυσάτω, ἐκκλινάτω ...
  have in common") every listed form must carry the claimed features;
* no distractor of a single-form card states exactly the corpus parse, which
  would leave the card with two correct answers.

Claims are partial by design: a card that only says "3-е лицо ед. ч." is checked
for person and number, and only for those. The label vocabulary is both the
Russian grammatical wording and the transliterated loanwords this bank mixes in
("субжунктив", "императив", "индикатив", "активный", "пассивный"). Greek is
compared accent-insensitively and with the corpus' editorial markers and
punctuation stripped; a card whose Greek cannot be resolved to a corpus row is
reported as ``parse.form_not_in_corpus`` and never silently skipped.

Run offline (default): ``python scripts/verify_parse_claims.py``
Machine-readable: ``python scripts/verify_parse_claims.py --json``
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

EVIDENCE_PATH = ROOT / "data" / "morphgnt-1peter-evidence.json"

GREEK_RUN = re.compile(r"[\u0370-\u03ff\u1f00-\u1fff]+")
ANCHOR = re.compile(r"(\d+)\s*:\s*(\d+)")
RANGE_END = re.compile(r"\s*[-\u2013]\s*(\d+)")
FIRST_PETER_MARKER = re.compile(r"1\s*Пет|Петра", re.IGNORECASE)
OTHER_BOOK_MARKER = re.compile(r"Притч|Ис\.|Исх|Мал|Зах|Пс\.|Быт|Ос\.|Гал\.", re.IGNORECASE)

# CCAT/MorphGNT parse-code vocabulary (see the MorphGNT README): the eight
# characters are person, tense, voice, mood, case, number, gender, degree.
# The label vocabulary is the Russian wording, the transliterated loanwords
# ("субжунктив"), the English names, and the short Latin tags the bank mixes into
# Russian sentences ("Aor. act. subj., 3 sg."). A tag is only recognised with its
# period, so "act." is a voice but "active" inside a Russian word is not a match.
TENSE = {
    "P": ("настоящ", "презенс", "презент", "present", "pres."),
    "I": ("имперфект", "imperfect", "impf."),
    "F": ("будущ", "футурум", "future", "fut."),
    "A": ("аорист", "aorist", "aor."),
    "X": ("перфект", "perfect", "perf."),
    "Y": ("плюперфект", "плюсквамперфект", "pluperfect", "pluperf."),
}
VOICE = {
    "A": ("действительн", "активн", "актив", "active", "act."),
    "M": ("средн", "медиальн", "медий", "middle", "mid."),
    "P": ("страдательн", "пассивн", "пассив", "passive", "pass."),
}
MOOD = {
    "I": ("изъявительн", "изъяв.", "индикатив", "indicative", "ind."),
    "D": ("повелительн", "повелит.", "императив", "imperative", "impv."),
    "S": ("сослагательн", "сослагат.", "сослаг.", "субжунктив", "субъюнктив", "конъюнктив", "subjunctive", "subj."),
    "O": ("оптатив", "желат.", "желательное", "optative", "opt."),
    "N": ("инфинитив", "infinitive", "inf."),
    "P": ("причастие", "причастн", "participle", "ptcp.", "ptc."),
}
PERSON = {
    "1": ("1-е лицо", "1-е л.", "первое лицо", "1 л.", "first person"),
    "2": ("2-е лицо", "2-е л.", "второе лицо", "2 л.", "second person"),
    "3": ("3-е лицо", "3-е л.", "третье лицо", "3 л.", "third person"),
}
NUMBER = {
    "S": ("ед. ч.", "ед. числ", "единственн", "singular", "sg."),
    "P": ("мн. ч.", "мн. числ", "множественн", "plural", "pl."),
}
CASE = {
    "N": ("именительн", "именит.", "nominative", "nom."),
    "G": ("родительн", "родит.", "genitive", "gen."),
    "D": ("дательн", "дат.", "dative", "dat."),
    "A": ("винительн", "винит.", "accusative", "acc."),
    "V": ("звательн", "зват.", "vocative", "voc."),
}
GENDER = {
    "M": ("мужск", "м. род", "masculine", "masc."),
    "F": ("женск", "ж. род", "feminine", "fem."),
    "N": ("средн", "ср. род", "neuter", "neut."),
    "N+": ("общий род",),
}
# Part of speech is compared against the corpus part-of-speech column, so a card
# that calls an adjective a participle cannot pass on number alone.
POS = {
    "V": (
        "глагол", "verb", "причастие", "причастн", "participle", "ptcp.", "ptc.",
        "инфинитив", "infinitive",
    ),
    "N": ("существительн", "noun"),
    "A": ("прилагательн", "adjective", "adj."),
    "D": ("наречие", "adverb", "adv."),
    "C": ("союз", "conjunction", "conj."),
    "P": ("предлог", "preposition", "prep."),
    "RA": ("артикль", "article", "art.", "определённый артикль"),
    "R": ("местоимен", "pronoun", "pron."),
}

# MorphGNT distinguishes pronoun subtypes in the POS column. A personal,
# demonstrative and relative pronoun can share case/number/gender without being
# interchangeable distractors, so the checker must preserve that distinction.
PRONOUN_KIND = {
    "RP": ("личн", "personal"),
    "RD": ("указательн", "demonstrative"),
    "RR": ("относительн", "relative"),
}

FEATURE_TABLES = (
    ("tense", TENSE),
    ("voice", VOICE),
    ("mood", MOOD),
    ("person", PERSON),
    ("number", NUMBER),
    ("case", CASE),
    ("gender", GENDER),
)

LEMMA_MARKER = re.compile(
    r"\b(?:от|лемма|lemma)\s*[:=]?\s*([\u0370-\u03ff\u1f00-\u1fff]{3,})",
    re.IGNORECASE,
)

# A distractor only competes with the key when the option *is* a parse label.
# "Adj., nom. neut. sg." is a second answer; a full sentence that happens to
# quote the same morphology ("форма dat. neut. pl. делает чтение о падших духах
# грамматически обязательным") is an interpretation claim and is judged by the
# interpretation, not by the parse code.
GRAMMAR_LABEL_EXTRA = (
    "врем", "залог", "наклонен", "падеж", "лиц", "числ", "род", "лемм",
    "от", "и", "или", "е", "л", "ч",
)
LABEL_TOKEN = re.compile(r"[^\W\d_]{2,}", re.UNICODE)

FINDING_BLOCKING = "blocking"
FINDING_INFO = "info"


@dataclass(frozen=True)
class Finding:
    check_id: str
    severity: str
    card_id: str
    pool: str
    message: str


def _normalize_greek(text: str) -> str:
    """Accent/marker/case-insensitive key without erasing iota-subscript identity."""

    decomposed = unicodedata.normalize("NFD", str(text))
    chars: list[str] = []
    for char in decomposed:
        if char == "\u0345":  # COMBINING GREEK YPOGEGRAMMENI: preserve as iota.
            chars.append("\u03b9")
        elif not unicodedata.combining(char):
            chars.append(char)
    folded = unicodedata.normalize("NFC", "".join(chars)).casefold()
    return "".join(char for char in folded if char.isalpha())


def load_corpus(path: Path = EVIDENCE_PATH) -> dict[tuple[str, str], list[dict]]:
    """(ref, normalized form) -> corpus rows."""

    payload = json.loads(path.read_text(encoding="utf-8"))
    index: dict[tuple[str, str], list[dict]] = {}
    for row in payload["rows"]:
        key = (str(row["ref"]), _normalize_greek(row["word"]))
        index.setdefault(key, []).append(row)
    return index


def first_peter_refs(verse: str) -> list[str]:
    """Refs of the form 210401 for the 1 Peter anchors a card names."""

    if not verse:
        return []
    refs: list[str] = []
    for segment in re.split(r"[;/]|,\s*", verse):
        if OTHER_BOOK_MARKER.search(segment) and not FIRST_PETER_MARKER.search(segment):
            continue
        match = ANCHOR.search(segment)
        if not match:
            continue
        chapter, verse_number = int(match.group(1)), int(match.group(2))
        end = RANGE_END.match(segment[match.end():])
        last = int(end.group(1)) if end else verse_number
        for number in range(verse_number, max(last, verse_number) + 1):
            if 1 <= chapter <= 5 and 1 <= number <= 25:
                refs.append(f"21{chapter:02d}{number:02d}")
    return refs


def _features_of_parse_code(code: str) -> dict[str, str]:
    code = str(code).ljust(8, "-")
    return {
        "person": code[0],
        "tense": code[1],
        "voice": code[2],
        "mood": code[3],
        "case": code[4],
        "number": code[5],
        "gender": code[6],
    }


def _marker_hit(marker: str, lowered: str) -> bool:
    """Markers match at a word start only: "посредничество" is not the voice."""

    return re.search(r"(?<![\w\u0370-\u03ff\u1f00-\u1fff])" + re.escape(marker), lowered) is not None


def _claimed_features(text: str) -> dict[str, str]:
    """Features and part of speech a Russian/English parse label names."""

    lowered = str(text).casefold()
    claimed: dict[str, str] = {}
    for name, table in FEATURE_TABLES:
        for letter, markers in table.items():
            if any(_marker_hit(marker, lowered) for marker in markers):
                claimed.setdefault(name, letter)
                break
    for letter, markers in POS.items():
        if any(_marker_hit(marker, lowered) for marker in markers):
            claimed["pos"] = letter
            break
    for code, markers in PRONOUN_KIND.items():
        if any(_marker_hit(marker, lowered) for marker in markers):
            claimed["pronoun_kind"] = code
            claimed["pos"] = "R"
            break
    # "средний" alone is a gender; the voice needs the word "залог" (or English).
    if "voice" in claimed and claimed["voice"] == "M" and not re.search(r"залог|middle voice", lowered):
        claimed.pop("voice")
    # ...and the reverse: "средний залог" is the middle voice, not a neuter noun.
    if claimed.get("gender") == "N" and re.search(r"средн\w*\s+залог|middle voice", lowered):
        claimed.pop("gender")
    # A participle is a verb form: if the label says participle, the corpus part of
    # speech is the verb column, not the adjective column.
    if claimed.get("mood") == "P":
        claimed["pos"] = "V"
    return claimed


def _has_parse_claim(text: str) -> bool:
    """True only for a grammatical claim, not for a lone interpretive word.

    "Перспектива будущей радости" carries a future-tense word but says nothing
    about a form; a parse claim names a mood, a voice, a case, a part of speech,
    or at least two morphological features.
    """

    claimed = _claimed_features(text)
    if not claimed:
        return False
    return bool({"mood", "voice", "case", "pos"} & set(claimed)) or len(claimed) >= 2


MORPHOLOGY_CONTEXT = re.compile(
    r"морфолог|разбор|падеж|\bрод\b|числ|наклон|залог|\bформ(?:а|ы|у|ой|е)\b",
    re.IGNORECASE,
)


def _card_has_parse_claim(stem: str, keyed: str) -> bool:
    """Recognize a safe single-feature claim only with an explicit Greek target."""

    if _has_parse_claim(keyed):
        return True
    return bool(
        _claimed_features(keyed)
        and GREEK_RUN.search(stem)
        and MORPHOLOGY_CONTEXT.search(f"{stem} {keyed}")
    )


def _explained_label_token(token: str) -> bool:
    """True for a token that is grammatical vocabulary rather than prose."""

    lowered = token.casefold().rstrip(".")
    if any(lowered.startswith(extra) for extra in GRAMMAR_LABEL_EXTRA):
        return True
    for _name, table in FEATURE_TABLES:
        for markers in table.values():
            if any(lowered.startswith(marker.rstrip(".")) for marker in markers):
                return True
    for markers in POS.values():
        if any(lowered.startswith(marker.rstrip(".")) for marker in markers):
            return True
    return False


def _label_shaped(text: str) -> bool:
    """True when the option is nothing but a parse label.

    Greek of the form under discussion and the "от <lemma>" tail are excluded, so
    "Перфект среднего залога, изъявительное наклонение, 3-е лицо ед. ч., от παύω"
    counts as a label while a sentence about what a form "does" does not.
    """

    without_greek = GREEK_RUN.sub(" ", str(text))
    for token in LABEL_TOKEN.findall(without_greek):
        if not _explained_label_token(token):
            return False
    return bool(_claimed_features(text))


def _lemma_of(text: str) -> str | None:
    match = LEMMA_MARKER.search(str(text))
    return _normalize_greek(match.group(1)) if match else None


def _card_texts(card: dict) -> tuple[str, str, str]:
    options = [str(option) for option in card.get("options", ())]
    correct = card.get("correct")
    keyed = options[correct] if isinstance(correct, int) and 0 <= correct < len(options) else ""
    return str(card.get("question", "")), keyed, str(card.get("explanation", ""))


def _has_morphology(row: dict) -> bool:
    """A row that states nothing (a preposition, a particle) is not a parse target."""

    return any(character != "-" for character in str(row.get("parse") or ""))


def _informative_forms(forms: list[str], rows_for) -> list[str]:
    """Drop all-dash forms when the stem also names a form that carries morphology."""

    informative = [form for form in forms if any(_has_morphology(row) for row in rows_for(form))]
    return informative or forms


def _candidate_forms(text: str, *, min_length: int = 3) -> list[str]:
    seen: list[str] = []
    for run in GREEK_RUN.findall(str(text)):
        normalized = _normalize_greek(run)
        if len(normalized) >= min_length and normalized not in seen:
            seen.append(normalized)
    return seen


def _pos_ok(
    row: dict,
    claimed_pos: str | None,
    claimed_pronoun_kind: str | None = None,
) -> bool:
    if claimed_pos is None:
        return True
    pos = str(row.get("pos") or "")
    if claimed_pronoun_kind is not None:
        return pos.startswith(claimed_pronoun_kind)
    if claimed_pos == "V":
        return pos.startswith("V")
    if claimed_pos == "R":
        return pos.startswith("R")
    return pos.startswith(claimed_pos)


def _row_matches(row: dict, claimed: dict[str, str], lemma: str | None) -> bool:
    if not _pos_ok(row, claimed.get("pos"), claimed.get("pronoun_kind")):
        return False
    code = _features_of_parse_code(row["parse"])
    for name, letter in claimed.items():
        if name in {"pos", "pronoun_kind"}:
            continue
        corpus_letter = code.get(name, "-")
        if corpus_letter in ("-", ""):
            continue
        if corpus_letter != letter:
            return False
    if lemma is not None and lemma != _normalize_greek(row["lemma"]):
        return False
    return True


def _specified_conflicts(row: dict, claimed: dict[str, str]) -> list[str]:
    code = _features_of_parse_code(row["parse"])
    conflicts = [
        f"{name}: card {claimed[name]!r} vs corpus {code[name]!r}"
        for name in claimed
        if name not in {"pos", "pronoun_kind"}
        and code.get(name) not in ("-", "")
        and code[name] != claimed[name]
    ]
    pronoun_kind = claimed.get("pronoun_kind")
    if pronoun_kind and not str(row.get("pos") or "").startswith(pronoun_kind):
        conflicts.append(
            f"pronoun_kind: card {pronoun_kind!r} vs corpus {str(row.get('pos') or '')!r}"
        )
    return conflicts


def audit_card(card: dict, pool: str, corpus: dict[tuple[str, str], list[dict]]) -> list[Finding]:
    stem, keyed, _explanation = _card_texts(card)
    card_id = str(card.get("id") or "<no-id>")
    if not _card_has_parse_claim(stem, keyed):
        return []
    claimed = _claimed_features(keyed)
    refs = first_peter_refs(str(card.get("verse") or ""))
    if not refs:
        return [
            Finding(
                "parse.card_without_anchor",
                FINDING_INFO,
                card_id,
                pool,
                "parse claim without a 1 Peter verse anchor; not checked",
            )
        ]
    claimed_lemma = _lemma_of(keyed)
    lemma_normalized = claimed_lemma or ""

    def rows_for(form: str) -> list[dict]:
        return [row for ref in refs for row in corpus.get((ref, form), ())]

    # The form under discussion is the Greek the stem quotes; the lemma behind the
    # "от ..." marker of an option is a lemma, not a form and not a target.
    stem_forms = [form for form in _candidate_forms(stem) if form != lemma_normalized]
    if not stem_forms:
        # A one-letter form (ᾧ in 3:19) is a legitimate subject; it is only
        # considered when the stem names nothing longer, so articles and
        # particles elsewhere in the text cannot become accidental targets. The
        # all-dash rows are dropped here, so a card about ᾧ is checked against ᾧ
        # and not against the ἐν that surrounds it.
        stem_forms = [form for form in _candidate_forms(stem, min_length=1) if form != lemma_normalized]
    stem_forms = _informative_forms(stem_forms, rows_for)
    keyed_forms = [form for form in _candidate_forms(keyed) if form != lemma_normalized]
    targets = [form for form in stem_forms if rows_for(form)] or [
        form for form in keyed_forms if rows_for(form)
    ]
    if not targets:
        unresolved = stem_forms or keyed_forms
        if unresolved:
            return [
                Finding(
                    "parse.form_not_in_corpus",
                    FINDING_BLOCKING,
                    card_id,
                    pool,
                    f"no corpus row for {unresolved!r} at {refs!r}",
                )
            ]
        return [
            Finding(
                "parse.card_without_form",
                FINDING_INFO,
                card_id,
                pool,
                "parse claim names no Greek form; not checked",
            )
        ]

    findings: list[Finding] = []
    single_form = len(targets) == 1
    every_form_claimed = bool(re.search(r"\bвсе\b|\bобе\b|every\b", keyed.casefold()))
    if not single_form and not every_form_claimed:
        return [
            Finding(
                "parse.multi_form_card",
                FINDING_INFO,
                card_id,
                pool,
                f"card makes per-form claims about {targets!r}; not machine-checked",
            )
        ]

    matched_forms: dict[str, dict | None] = {}
    for form in targets:
        form_rows = rows_for(form)
        key_row = next((row for row in form_rows if _row_matches(row, claimed, claimed_lemma)), None)
        matched_forms[form] = key_row
        if key_row is not None:
            continue
        best = form_rows[0]
        conflicts = _specified_conflicts(best, claimed)
        message = f"{best['word']} {best['parse']} ({best['lemma']}): " + (
            ", ".join(conflicts) if conflicts else "no claimed feature agrees with the row"
        )
        if claimed_lemma is not None and all(
            claimed_lemma != _normalize_greek(row["lemma"]) for row in form_rows
        ):
            message = (
                f"{best['word']} {best['parse']}: card lemma {claimed_lemma!r} is not the corpus lemma "
                f"{best['lemma']!r}"
            )
            findings.append(Finding("parse.lemma_mismatch", FINDING_BLOCKING, card_id, pool, message))
            continue
        findings.append(Finding("parse.claim_mismatch", FINDING_BLOCKING, card_id, pool, message))

    if not single_form:
        return findings

    key_row = matched_forms[targets[0]]
    if key_row is None:
        return findings
    for index, option in enumerate(card.get("options", ())):
        if index == card.get("correct"):
            continue
        distractor = _claimed_features(str(option))
        if len(distractor) < 2:
            continue
        code = _features_of_parse_code(key_row["parse"])
        specified = [name for name in distractor if name != "pos" and code.get(name) not in ("-", "")]
        # Only a parse-label-shaped distractor can be a second correct answer; a
        # sentence that merely contains the word "причастие" is an interpretation
        # claim, not a competing parse.
        if len(specified) + (1 if "pos" in distractor else 0) < 3:
            continue
        if not _label_shaped(option):
            continue
        distractor_lemma = _lemma_of(str(option))
        if distractor_lemma is not None and distractor_lemma != _normalize_greek(key_row["lemma"]):
            continue
        if _row_matches(key_row, distractor, distractor_lemma):
            findings.append(
                Finding(
                    "parse.distractor_matches_corpus",
                    FINDING_BLOCKING,
                    card_id,
                    pool,
                    f"option {index} states the corpus parse {key_row['parse']}",
                )
            )
    return findings


def pool_cards() -> dict[str, list[dict]]:
    """Learner-facing leaf pools only; the composite views repeat the same cards."""

    import questions

    from scripts.audit_question_quality import LEAF_POOLS

    pools: dict[str, list[dict]] = {}
    for key in LEAF_POOLS:
        try:
            pools[key] = [dict(card) for card in questions.get_pool_by_key(key)]
        except KeyError:
            continue
    return pools


def audit(pools: dict[str, list[dict]] | None = None) -> list[Finding]:
    corpus = load_corpus()
    pools = pools if pools is not None else pool_cards()
    findings: list[Finding] = []
    for pool, cards in pools.items():
        for card in cards:
            findings.extend(audit_card(card, pool, corpus))
    return findings


def coverage(pools: dict[str, list[dict]] | None = None) -> dict[str, int]:
    """Count candidates separately from cards the checker actually verifies."""

    corpus = load_corpus()
    pools = pools if pools is not None else pool_cards()
    candidates = 0
    verified = 0
    manual = 0
    blocked = 0
    for pool, cards in pools.items():
        for card in cards:
            _stem, keyed, _explanation = _card_texts(card)
            if not _card_has_parse_claim(_stem, keyed):
                continue
            if not first_peter_refs(str(card.get("verse") or "")):
                continue
            candidates += 1
            findings = audit_card(card, pool, corpus)
            if any(finding.severity == FINDING_BLOCKING for finding in findings):
                blocked += 1
            elif any(finding.severity == FINDING_INFO for finding in findings):
                manual += 1
            else:
                verified += 1
    return {
        # Backward-compatible candidate count; this was historically (and
        # misleadingly) described as the number already machine-verified.
        "cards_with_parse_claim": candidates,
        "parse_claim_cards": candidates,
        "machine_verified_cards": verified,
        "manual_review_cards": manual,
        "blocking_cards": blocked,
        "corpus_rows": sum(len(rows) for rows in corpus.values()),
        "corpus_form_keys": len(corpus),
    }


def main() -> int:
    # Windows shells can expose a legacy code page even when stdout is captured.
    # The findings contain polytonic Greek; force a deterministic UTF-8 CLI.
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="backslashreplace")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit findings and coverage as JSON")
    args = parser.parse_args()

    findings = audit()
    stats = coverage()
    blocking = [finding for finding in findings if finding.severity == FINDING_BLOCKING]
    if args.json:
        print(
            json.dumps(
                {"coverage": stats, "findings": [finding.__dict__ for finding in findings]},
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        for finding in findings:
            print(f"{finding.severity:8s} {finding.check_id:32s} {finding.pool}/{finding.card_id}: {finding.message}")
        print(
            f"checked {stats['cards_with_parse_claim']} parse card(s) against "
            f"{stats['corpus_rows']} vendored corpus rows; {len(blocking)} blocking finding(s)"
        )
    return 1 if blocking else 0


if __name__ == "__main__":
    raise SystemExit(main())
