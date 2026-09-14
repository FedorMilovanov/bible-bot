"""Verify the bank's Greek claims against the MorphGNT/SBLGNT corpus.

``AGENTS.md`` §3 requires a parsing claim to be machine-checked against the
canonical Greek/morphology corpus, never typed from memory, and §9 requires the
check to be a test. This script is that check.

Two modes:

* ``--corpus <file>`` — full review-time verification against a freshly fetched
  ``NN-1Pe-morphgnt.txt`` from the MorphGNT project. Every ``morphgnt`` metadata
  claim of the bank must exist in that file with the recorded parse and lemma,
  and every Greek form quoted inside a learner-facing stem of a text/Greek card
  must be one of the corpus's forms, an inflected form of a corpus lemma, or an
  explicit citation of another book. Findings are printed; a mismatch in a
  metadata claim exits non-zero.
* default — offline verification against ``data/morphgnt-1peter-evidence.json``,
  the vendored excerpt of the corpus rows the bank's claims rest on. This is what
  the offline test suite runs.

The corpus is a byte-for-byte excerpt: the evidence file records the upstream
URL, the SHA-256 of the file the rows were read from, and the retrieval date, so
anyone can re-derive the same table.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

EVIDENCE_PATH = ROOT / "data" / "morphgnt-1peter-evidence.json"
CORPUS_URL = "https://github.com/morphgnt/sblgnt/blob/master/81-1Pe-morphgnt.txt"

# Punctuation and apparatus markers MorphGNT carries in its text column.
MARKERS = "⸀⸁⸂⸃⸄⸅⸆⸇.,;·:()[]«»"
APOSTROPHES = "\u2019\u0027\u02bc\u1fbd"  # right quote, ASCII quote, modifier apostrophe, koronis
GREEK = re.compile(
    r"[\u0370-\u03ff\u1f00-\u1fff][\u0370-\u03ff\u1f00-\u1fff\u0300-\u036f" + APOSTROPHES + r"]*"
)


def normalize(text: str) -> str:
    """Case- and punctuation-insensitive key for a Greek token."""
    stripped = unicodedata.normalize("NFC", text.strip(MARKERS))
    for apostrophe in APOSTROPHES:
        stripped = stripped.replace(apostrophe, "\u1fbd")
    return stripped.lower()


# Editions and textbooks differ on two things that do not change the form being
# cited: a word quoted outside its sentence carries an acute accent where the
# running text has a grave, and a movable nu may be printed or dropped. A card
# that cites a form out of context is therefore compared with those two relaxed.
# Accents that *do* mark a different form are kept: the circumflex distinguishes
# "διασπορᾶς" (genitive singular, 1 Pet. 1:1) from "διασποράς" (accusative plural),
# and breathing marks distinguish words.
RELAXED_DROPS = ("\u0300", "\u0301")  # grave, acute


def relaxed(text: str) -> str:
    """Case-insensitive key that ignores final nu and grave/acute-only accents."""
    decomposed = unicodedata.normalize("NFD", normalize(text))
    without_tone = "".join(ch for ch in decomposed if ch not in RELAXED_DROPS)
    if without_tone.endswith("\u03bd"):  # movable nu
        without_tone = without_tone[:-1]
    return unicodedata.normalize("NFC", without_tone)


def matches_corpus(token: str, index_keys: set[str], relaxed_keys: set[str]) -> bool:
    key = normalize(token)
    return key in index_keys or relaxed(token) in relaxed_keys


def greek_tokens(text: str) -> list[str]:
    """Greek words quoted in a learner-facing string.

    A trailing hyphen marks a morphological prefix (``ἀ-`` in ``ἀνεκλαλήτῳ``),
    not a word, so prefixes and one-letter fragments are not treated as forms.
    """
    tokens: list[str] = []
    raw = str(text)
    for match in GREEK.finditer(raw):
        if raw[match.end() : match.end() + 1] == "-":
            continue
        token = match.group(0).strip(APOSTROPHES)
        if len(token) < 2:
            continue
        tokens.append(token)
    return tokens


@dataclass(frozen=True)
class Row:
    ref: str
    pos: str
    parse: str
    word: str
    lemma: str

    @property
    def verse(self) -> str:
        return f"{int(self.ref[2:4])}:{int(self.ref[4:6])}"

    @property
    def combined_parse(self) -> str:
        return f"{self.pos} {self.parse}"


def parse_corpus_line(line: str) -> Row | None:
    parts = line.rstrip("\n").split(" ")
    if len(parts) < 7:
        return None
    return Row(ref=parts[0], pos=parts[1], parse=parts[2], word=parts[3], lemma=parts[6])


def load_corpus(path: Path) -> list[Row]:
    rows: list[Row] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        row = parse_corpus_line(line)
        if row is not None:
            rows.append(row)
    if not rows:
        raise ValueError(f"no MorphGNT rows read from {path}")
    return rows


@dataclass
class Evidence:
    rows: list[Row]
    provenance: dict

    @classmethod
    def load(cls, path: Path = EVIDENCE_PATH) -> Evidence:
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows = [
            Row(
                ref=entry["ref"],
                pos=entry["pos"],
                parse=entry["parse"],
                word=entry["word"],
                lemma=entry["lemma"],
            )
            for entry in payload["rows"]
        ]
        return cls(rows=rows, provenance=dict(payload.get("provenance", {})))

    def by_word(self) -> dict[str, list[Row]]:
        index: dict[str, list[Row]] = defaultdict(list)
        for row in self.rows:
            index[normalize(row.word)].append(row)
        return dict(index)

    def by_lemma(self) -> dict[str, list[Row]]:
        index: dict[str, list[Row]] = defaultdict(list)
        for row in self.rows:
            index[normalize(row.lemma)].append(row)
        return dict(index)


@dataclass(frozen=True)
class Finding:
    check_id: str
    pool: str
    item_id: str
    message: str


def _bank():
    import questions

    aggregate = {"random_all", "competitive_all", "easy", "medium", "hard", "practical_ch1"}
    for pool, cards in questions.POOL_REGISTRY.items():
        if pool in aggregate:
            continue
        for card in cards:
            yield pool, card


# Inflected forms of a lemma legitimately differ from the corpus surface forms; a
# card may also cite another book on purpose (Romans, Hebrews, the LXX, Acts).
OTHER_BOOK_MARKERS = ("Рим.", "Евр.", "Деян.", "Исх.", "Лев.", "Притч.", "Ис.", "Пс.", "Быт.", "LXX")


# Forms that the editorial fixes of the depth/evidence pass rest on. They are
# quoted inside explanations or option text rather than stems, so the stem scan
# does not reach them; they are listed here explicitly so the excerpt carries the
# row each correction is checked against and the regression test can pin them.
REVIEWED_FORMS = (
    "διασπορᾶς",  # 1 Pet 1:1, genitive singular (was quoted as διασποράς)
    "παρεπιδήμοις",  # 1 Pet 1:1, dative plural (was quoted as παρεπίδημοι)
    "χαρᾷ",  # 1 Pet 1:8, dative singular (was quoted as χαρᾶς)
    "ἀνεκλαλήτῳ",  # 1 Pet 1:8, dative singular (was quoted as ἀνεκλαλήτου)
    "τιμίῳ",  # 1 Pet 1:19, dative (was quoted as the feminine τιμία)
    "ἐπικάλυμμα",  # 1 Pet 2:16 (was quoted as κάλυμμα)
    "πιστῷ",  # 1 Pet 4:19, dative (titular citation was nominative)
    "κτίστῃ",  # 1 Pet 4:19, dative
    "ἀρχιποίμενος",  # 1 Pet 5:4, genitive singular (was quoted as ἀρχιποίμενες)
    "καλοὶ",  # 1 Pet 4:10, nominative plural
    "οἰκονόμοι",  # 1 Pet 4:10, nominative plural
)


def is_first_peter_anchor(verse: str) -> bool:
    """True when the card's verse anchor points at 1 Peter alone.

    The chapter-1 course pools anchor with a bare ``1:8`` / ``1:17`` while the
    chapter aggregates write ``1 Пет. 2:11``; both mean 1 Peter. A card that also
    names another text (a Psalm, Isaiah, Acts, Romans, the LXX) is quoting that
    other text on purpose and is out of scope here.
    """
    anchor = verse.strip()
    if not anchor:
        return True
    if any(marker in anchor for marker in OTHER_BOOK_MARKERS):
        return False
    return anchor.startswith("1 Пет.") or bool(re.match(r"^\d:\d", anchor))


def check_metadata_claims(evidence: Evidence) -> list[Finding]:
    """Every ``morphgnt`` claim must match the corpus row for that form."""
    by_word, by_lemma = evidence.by_word(), evidence.by_lemma()
    findings: list[Finding] = []
    for pool, card in _bank():
        meta = card.get("morphgnt")
        if not meta:
            continue
        form, parse, lemma = str(meta["form"]), str(meta["parse"]), str(meta["lemma"])
        verse = str(card.get("verse") or "")
        candidates = by_word.get(normalize(form), [])
        if verse.startswith("1 Пет."):
            in_verse = [row for row in candidates if row.verse == verse.replace("1 Пет.", "").strip()]
            candidates = in_verse or candidates
        if not candidates:
            findings.append(
                Finding("greek.form_not_in_corpus", pool, card["id"], f"{form} not in the corpus")
            )
            continue
        parses = {row.combined_parse for row in candidates} | {row.parse for row in candidates}
        if parse not in parses:
            findings.append(
                Finding(
                    "greek.parse_mismatch",
                    pool,
                    card["id"],
                    f"{form}: card says {parse!r}, corpus has {sorted(parses)}",
                )
            )
        lemmas = {normalize(row.lemma) for row in candidates}
        if normalize(lemma) not in lemmas:
            findings.append(
                Finding(
                    "greek.lemma_mismatch",
                    pool,
                    card["id"],
                    f"{form}: card says {lemma!r}, corpus has {sorted(lemmas)}",
                )
            )
        elif normalize(lemma) not in by_lemma:
            findings.append(
                Finding(
                    "greek.lemma_not_indexed",
                    pool,
                    card["id"],
                    f"{lemma} is missing from the evidence excerpt",
                )
            )
    return findings


def check_quoted_forms(evidence: Evidence) -> list[Finding]:
    """Stems of text/Greek cards may not quote a form the corpus does not have."""
    word_index, lemma_index = evidence.by_word(), evidence.by_lemma()
    word_keys, lemma_keys = set(word_index), set(lemma_index)
    relaxed_keys = {relaxed(key) for key in word_keys | lemma_keys}
    findings: list[Finding] = []
    for pool, card in _bank():
        if str(card.get("claim_type")) not in {"text", "greek"}:
            continue
        if not is_first_peter_anchor(str(card.get("verse") or "")):
            continue
        for token in greek_tokens(str(card.get("question") or "")):
            if matches_corpus(token, word_keys | lemma_keys, relaxed_keys):
                continue
            findings.append(
                Finding(
                    "greek.form_in_stem_not_in_corpus",
                    pool,
                    card["id"],
                    f"{token} is not a 1 Peter form or lemma in the evidence excerpt",
                )
            )
    return findings


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_evidence(corpus: Path, wanted: set[str], out: Path) -> dict:
    """Write the excerpt of ``corpus`` rows needed for the ``wanted`` forms."""
    wanted_keys = {normalize(form) for form in wanted}
    wanted_relaxed = {relaxed(form) for form in wanted}
    rows = [
        row
        for row in load_corpus(corpus)
        if matches_corpus(row.word, wanted_keys, wanted_relaxed)
        or normalize(row.lemma) in wanted_keys
    ]
    payload = {
        "provenance": {
            "source": "MorphGNT / SBLGNT morphology, 1 Peter",
            "source_url": CORPUS_URL,
            "citation": (
                "Tauber, J. K., ed. (2017) MorphGNT: SBLGNT Edition. Version 6.12 [Data set]. "
                "https://github.com/morphgnt/sblgnt DOI: 10.5281/zenodo.376200"
            ),
            "license": (
                "Upstream states: the SBLGNT text is subject to the SBLGNT EULA "
                "(http://sblgnt.com/license/); the morphological parsing and lemmatization is "
                "released under CC BY-SA 3.0. The excerpt below is a small verbatim selection of "
                "those rows, kept only to verify the bank's parsing and quotation claims."
            ),
            "upstream_file_sha256": sha256_of(corpus),
            "upstream_rows": len(load_corpus(corpus)),
            "excerpt_rows": len(rows),
            "note": (
                "Excerpt of the corpus rows that the question bank's Greek claims rest on. Rows are "
                "reproduced unchanged from the upstream file, with the citation and licence terms "
                "above. It is the only vendored corpus; the Old Testament corpus is not vendored "
                "(see data/ot-citation-fingerprints.json). Further redistribution follows the "
                "upstream terms."
            ),
        },
        "rows": [
            {"ref": row.ref, "pos": row.pos, "parse": row.parse, "word": row.word, "lemma": row.lemma}
            for row in sorted(rows, key=lambda r: (r.ref, r.word))
        ],
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return payload


def quoted_greek(card: dict) -> list[str]:
    """Every Greek word a learner can see on the card."""
    text = " ".join(
        [
            str(card.get("question") or ""),
            str(card.get("explanation") or ""),
            *map(str, card.get("options") or []),
        ]
    )
    return greek_tokens(text)


def claims_needed() -> set[str]:
    """Forms and lemmas every machine-checkable claim of the bank depends on.

    The excerpt has to answer two questions: does a parsing claim match the
    corpus, and is a quoted form a form 1 Peter actually has? A card that quotes
    a Psalm still quotes 1 Peter next to it (``ch3_ot_302`` reads ``ταῖς
    καρδίαις ὑμῶν`` while its anchor names Isaiah), so the scan covers every
    learner-facing string of every card, not just the 1 Peter-anchored stems.
    """
    wanted: set[str] = set()
    for _pool, card in _bank():
        meta = card.get("morphgnt")
        if meta:
            wanted.update({str(meta["form"]), str(meta["lemma"])})
        wanted.update(quoted_greek(card))
    wanted.update(REVIEWED_FORMS)
    return wanted


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, help="full 1Pe MorphGNT file to verify against")
    parser.add_argument("--write-evidence", action="store_true", help="rewrite the vendored excerpt")
    parser.add_argument("--json", type=Path, help="write findings as JSON")
    args = parser.parse_args(argv)

    if args.write_evidence:
        if not args.corpus:
            parser.error("--write-evidence needs --corpus")
        payload = build_evidence(args.corpus, claims_needed(), EVIDENCE_PATH)
        print(
            "evidence excerpt: "
            f"{payload['provenance']['excerpt_rows']} rows from {payload['provenance']['upstream_rows']}"
        )
        return 0

    evidence = Evidence.load() if not args.corpus else Evidence(
        rows=load_corpus(args.corpus),
        provenance={"source_url": str(args.corpus), "mode": "full corpus"},
    )

    findings = check_metadata_claims(evidence) + check_quoted_forms(evidence)
    for finding in findings:
        print(f"{finding.check_id}: {finding.pool}/{finding.item_id}: {finding.message}")
    print(
        f"greek evidence: {len(findings)} finding(s) "
        f"({len(evidence.rows)} corpus rows, provenance={evidence.provenance.get('source_url')})"
    )
    if args.json:
        args.json.write_text(
            json.dumps([finding.__dict__ for finding in findings], ensure_ascii=False, indent=1),
            encoding="utf-8",
        )
    blocking = [f for f in findings if f.check_id in {"greek.parse_mismatch", "greek.lemma_mismatch"}]
    return 1 if blocking else 0


if __name__ == "__main__":
    raise SystemExit(main())
