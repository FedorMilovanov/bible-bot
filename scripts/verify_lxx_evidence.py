"""Verify the bank's Old Testament / LXX quotations against a Rahlfs corpus.

``AGENTS.md`` §3 requires a parsing or quotation claim to be machine-checked
against the canonical text, never typed from memory. ``verify_greek_evidence.py``
covers 1 Peter itself; this script covers the other half of the bank's Greek: the
cards that quote a Psalm, Isaiah, Proverbs, Genesis or Exodus while discussing
1 Peter's use of them.

The check reads every Old Testament reference off a card — the verse anchor plus
inline citations such as ``Притч. 3:25`` or ``Пс. 33:5`` — loads those verses from
a Rahlfs corpus, and reports any Greek word on the card that is in neither the
cited verses nor the 1 Peter corpus. That is how a quotation attributed to the
wrong verse, or a paraphrase passed off as a quotation, becomes visible.

Review commands (the corpus is not vendored, see "Licensing" below)::

    python scripts/verify_lxx_evidence.py --corpus lxx-text.zip \\
        --versification versification.csv --write-fingerprints
    python scripts/verify_lxx_evidence.py            # offline: fingerprints only

Without ``--corpus`` the script re-derives the citation fingerprint of every
OT-anchored card and compares it with ``data/ot-citation-fingerprints.json``, the
record of the last corpus run. A mismatch means the Greek of one of those cards
changed and the corpus check has to be re-run for it; the offline run is what CI
enforces.

Corpus sources (neither is vendored, and the zip is not committed):

* text: ``eliranwong/LXX-Rahlfs-1935``, ``12-Marvel.Bible/01-text_accented.csv.zip``
* versification: same repository, ``12-Marvel.Bible/00-versification_original.csv``

Licensing. That dataset is CC BY-NC-SA 4.0 and derives from CCAT material whose
terms require a user declaration, so the project stores the *references* it
verified and a fingerprint of its own card text, not the corpus. The vendored
1 Peter excerpt is a separate case: it comes from the MorphGNT/SBLGNT project
(see ``data/morphgnt-1peter-evidence.json`` for its provenance).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import unicodedata
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.verify_greek_evidence import (  # noqa: E402
    APOSTROPHES,
    EVIDENCE_PATH,
    Evidence,
    Finding,
    greek_tokens,
    normalize,
    relaxed,
    sha256_of,
)

FINGERPRINTS_PATH = ROOT / "data" / "ot-citation-fingerprints.json"

# Russian book abbreviations used by the bank, mapped to the Rahlfs book numbers
# of the upstream versification file.
BOOK_NUMBERS = {
    "Быт": 1, "Исх": 2, "Лев": 3, "Чис": 4, "Втор": 5, "Нав": 6, "Суд": 7, "Руф": 8,
    "1 Цар": 9, "2 Цар": 10, "3 Цар": 11, "4 Цар": 12, "1 Пар": 13, "2 Пар": 14,
    "Пс": 19, "Притч": 20, "Еккл": 21, "Песн": 22, "Ис": 23, "Иер": 24, "Плач": 25,
    "Иез": 26, "Дан": 27, "Ос": 28, "Иоил": 29, "Ам": 30, "Авд": 31, "Иона": 32,
    "Мих": 33, "Наум": 34, "Авв": 35, "Соф": 36, "Агг": 37, "Зах": 38, "Мал": 39,
}
# New-Testament and deuterocanonical abbreviations the bank also uses. They must
# be recognised so that an inherited book name is not carried over them.
OTHER_BOOKS = (
    "Пет", "Мф", "Мк", "Лк", "Ин", "Деян", "Рим", "1 Кор", "2 Кор", "Гал", "Еф",
    "Флп", "Кол", "1 Фес", "2 Фес", "1 Тим", "2 Тим", "Тит", "Флм", "Евр", "Иак",
    "1 Ин", "2 Ин", "3 Ин", "Иуд", "Откр", "Вар", "Сир", "Прем", "1 Мак", "2 Мак",
    "Тов", "Иудф", "Неем", "Езд", "1 Езд", "2 Езд", "Пар",
)
_BOOKS_PATTERN = "|".join(
    sorted(map(re.escape, (*BOOK_NUMBERS, *OTHER_BOOKS)), key=len, reverse=True)
)
BARE_REFERENCE = re.compile(r"(?<![\d:])(?P<chapter>\d+)\s*:\s*(?P<verse>\d+)(?:\s*[-–—]\s*(?P<endverse>\d+))?")
GREEK_ONLY = re.compile(r"^[\u0370-\u03ff\u1f00-\u1fff\u0300-\u036f" + APOSTROPHES + r"]+$")

# A dictionary lemma never equals an inflected corpus form. Such a citation is
# accepted when its stem (the first four letters) is that of a cited form.
LEMMA_STEM_LENGTH = 4


@dataclass(frozen=True)
class Reference:
    book: int
    chapter: int
    verse: int | None = None

    @property
    def key(self) -> str:
        return f"{self.book}.{self.chapter}" + (f".{self.verse}" if self.verse else "")


def iter_references(text: str) -> list[Reference]:
    """Every Old Testament reference of a card string, with book inheritance.

    ``Лев. 11:44-45; 19:2; 20:7`` names Leviticus once; the later bare ``19:2``
    inherits it. A reference to another book (``1 Пет. 3:10``) clears the
    inherited book so 1 Peter chapter numbers are never read as Rahlfs chapters.
    """
    references: list[Reference] = []
    for match in re.finditer(
        r"(?P<book>(?:" + _BOOKS_PATTERN + r"))\.?\s*(?P<chapter>\d+)"
        r"(?:\s*:\s*(?P<verse>\d+))?"
        r"(?:\s*[-–—]\s*(?P<end>\d+))?"
        r"(?P<rest>[^А-Яа-я]*)",
        text,
    ):
        name, chapter = match.group("book"), int(match.group("chapter"))
        verse = int(match.group("verse")) if match.group("verse") else None
        end = int(match.group("end")) if match.group("end") else None
        number = BOOK_NUMBERS.get(name)
        if number is None:
            continue
        if verse is not None:
            last = end if end and end >= verse else verse
            references.extend(Reference(number, chapter, v) for v in range(verse, last + 1))
        elif end and end >= chapter:
            references.extend(Reference(number, c) for c in range(chapter, end + 1))
        else:
            references.append(Reference(number, chapter))
        # Bare "19:2" continuations.
        if verse is not None:
            for tail in BARE_REFERENCE.finditer(match.group("rest") or ""):
                tail_chapter, tail_verse = int(tail.group("chapter")), int(tail.group("verse"))
                tail_end = int(tail.group("endverse")) if tail.group("endverse") else tail_verse
                references.extend(Reference(number, tail_chapter, v) for v in range(tail_verse, tail_end + 1))
    return references


def _leading_number(part: str) -> int | None:
    """Read ``33`` out of ``33``, ``29a``, ``‡2`` -- commentary numbering survives."""
    match = re.match(r"^‡?(\d+)", part)
    return int(match.group(1)) if match else None


def corpus_verses(text_path: Path, versification_path: Path, wanted: set[str]) -> dict[str, str]:
    """Return the wanted verses as ``{"19.33.5": "ἐξεζήτησα ..."}``."""
    words: dict[int, str] = {}
    if text_path.suffix == ".zip":
        with zipfile.ZipFile(text_path) as archive:
            member = next(name for name in archive.namelist() if name.endswith(".csv"))
            lines = archive.read(member).decode("utf-8", "replace").splitlines()
    else:
        lines = text_path.read_text(encoding="utf-8").splitlines()
    for line in lines:
        if not line or not line[0].isdigit():
            continue
        index, _, rest = line.partition("\t")
        if not index.isdigit():
            continue
        word = "".join(re.findall(r">([^<]*)</grk>", rest))
        if word:
            words[int(index)] = word
    if not words:
        raise ValueError(f"no LXX words read from {text_path}")

    starts: list[tuple[int, str]] = []
    for line in versification_path.read_text(encoding="utf-8").splitlines():
        parts = line.rstrip("\n").split("\t")
        if len(parts) >= 2 and parts[0].isdigit():
            starts.append((int(parts[0]), parts[1].lstrip("†")))
    starts.sort()
    if not starts:
        raise ValueError(f"no versification rows read from {versification_path}")

    verses: dict[str, str] = {}
    for position, (start, ref) in enumerate(starts):
        parts = ref.split(".")
        if len(parts) != 3:
            continue
        numbers = [_leading_number(part) for part in parts]
        if any(number is None for number in numbers):
            continue
        key = ".".join(str(number) for number in numbers)
        if not (key in wanted or f"{parts[0]}.{parts[1]}" in wanted):
            continue
        end = starts[position + 1][0] if position + 1 < len(starts) else max(words) + 1
        verses[key] = " ".join(words[index] for index in range(start, end) if index in words)
    return verses


def card_text(card: dict) -> str:
    return " ".join(
        [
            str(card.get("verse") or ""),
            str(card.get("question") or ""),
            str(card.get("explanation") or ""),
            *map(str, card.get("options") or []),
        ]
    )


def card_references(card: dict) -> list[Reference]:
    seen: dict[str, Reference] = {}
    for reference in iter_references(card_text(card)):
        seen.setdefault(reference.key, reference)
    return [seen[key] for key in sorted(seen)]


def _stem(token: str) -> str:
    decomposed = unicodedata.normalize("NFD", normalize(token))
    letters = "".join(ch for ch in decomposed if ch.isalpha())
    return unicodedata.normalize("NFC", letters[:LEMMA_STEM_LENGTH])


def check_ot_quotations(verses: dict[str, str], peter: Evidence) -> list[Finding]:
    """Greek quoted in a card's own voice must come from the verses it cites.

    Three levels, because a card is not one voice:

    * the stem and the explanation speak for the card, so a Greek word there has
      to be in the cited verses or in 1 Peter -- a blocking finding otherwise;
    * an option that names a reference and quotes Greek next to it is checked
      against *that* reference, which is how a misattributed quotation shows up;
    * an option that quotes a bare word (a distractor from the same semantic
      field, as in "which word stands in 1 Pet. 2:24?") is reported as a word in
      an option, for review, but cannot be blocking: nothing in the card says it
      is a quotation.

    Both option levels accept 1 Peter vocabulary, because a card that compares a
    Psalm with 1 Peter names the Psalm and then quotes Peter (``ch3_theol_301``
    writes ``τὸν Χριστόν`` next to ``Ис. 8``). That means the reverse case - 1
    Peter's own words presented under an Old Testament label - stays for the
    reviewer to adjudicate; what the check rules out is a word that belongs to
    neither text.
    """
    peter_words, peter_lemmas = peter.by_word(), peter.by_lemma()
    peter_keys = set(peter_words) | set(peter_lemmas)
    peter_relaxed = {relaxed(key) for key in peter_keys}
    forms_by_verse: dict[str, list[str]] = {key: greek_tokens(text) for key, text in verses.items()}
    vocabulary: dict[str, set[str]] = {}
    for key, forms in forms_by_verse.items():
        vocabulary[key] = {normalize(form) for form in forms} | {relaxed(form) for form in forms}
    stems: dict[str, set[str]] = {
        key: {_stem(form) for form in forms} for key, forms in forms_by_verse.items()
    }

    def known(token: str) -> bool:
        return normalize(token) in peter_keys or relaxed(token) in peter_relaxed

    def allowed_for(references: list[Reference]) -> tuple[set[str], set[str]]:
        keys: list[str] = []
        for reference in references:
            if reference.verse is not None:
                keys.append(reference.key)
            else:
                keys.extend(key for key in verses if key.startswith(f"{reference.book}.{reference.chapter}."))
        keys = [key for key in keys if key in vocabulary]
        if not keys:
            return set(), set()
        return (
            set().union(*(vocabulary[key] for key in keys)),
            set().union(*(stems[key] for key in keys)),
        )

    findings: list[Finding] = []
    for pool, card in _bank():
        references = card_references(card)
        if not references:
            continue
        cited, cited_stems = allowed_for(references)
        label = ", ".join(sorted({r.key for r in references})[:4])
        for token in greek_tokens(f"{card.get('question') or ''} {card.get('explanation') or ''}"):
            if not GREEK_ONLY.match(token) or known(token):
                continue
            key, eased = normalize(token), relaxed(token)
            if key in cited or eased in cited or _stem(token) in cited_stems:
                continue
            findings.append(
                Finding(
                    "lxx.form_not_in_cited_verses",
                    pool,
                    card["id"],
                    f"{token} is not in {label} or in 1 Peter",
                )
            )
        for option in card.get("options") or []:
            text = str(option)
            tokens = [token for token in greek_tokens(text) if GREEK_ONLY.match(token) and not known(token)]
            if not tokens:
                continue
            option_refs = [r for r in iter_references(text) if r.book in set(BOOK_NUMBERS.values())]
            if option_refs:
                option_allowed, option_stems = allowed_for(option_refs)
                for token in tokens:
                    key, eased = normalize(token), relaxed(token)
                    if key in option_allowed or eased in option_allowed or _stem(token) in option_stems:
                        continue
                    findings.append(
                        Finding(
                            "lxx.option_misattributed",
                            pool,
                            card["id"],
                            f"{token} is not in {', '.join(sorted({r.key for r in option_refs}))}",
                        )
                    )
                continue
            for token in tokens:
                key, eased = normalize(token), relaxed(token)
                if key in cited or eased in cited or _stem(token) in cited_stems:
                    continue
                findings.append(
                    Finding(
                        "lxx.word_in_option_not_in_verses",
                        pool,
                        card["id"],
                        f"option word {token} is not in {label} or in 1 Peter",
                    )
                )
    return findings


def fingerprints(peter: Evidence) -> dict[str, dict]:
    """Citation fingerprint of every OT-anchored card (our text, not the corpus)."""
    entries: dict[str, dict] = {}
    for _pool, card in _bank():
        references = card_references(card)
        if not references:
            continue
        quoted = sorted({normalize(token) for token in greek_tokens(card_text(card))})
        digest = hashlib.sha256("\n".join(quoted).encode("utf-8")).hexdigest()
        entries[str(card["id"])] = {"refs": [r.key for r in references], "quoted_sha256": digest}
    return entries


def build_fingerprint_file(
    peter_evidence: Evidence,
    verses: dict[str, str],
    out: Path,
    corpus_sha256: str = "",
    versification_sha256: str = "",
    findings: list[Finding] | None = None,
) -> dict:
    entries = fingerprints(peter_evidence)
    summary: dict[str, int] = defaultdict(int)
    for finding in findings or []:
        summary[finding.check_id] += 1
    payload = {
        "provenance": {
            "source": "LXX Rahlfs 1935 (eliranwong/LXX-Rahlfs-1935), review-time corpus",
            "source_url": "https://github.com/eliranwong/LXX-Rahlfs-1935",
            "corpus_sha256": corpus_sha256,
            "versification_sha256": versification_sha256,
            "upstream_rows": sum(len(greek_tokens(text)) for text in verses.values()),
            "verses_checked": len(verses),
            "cards": len(entries),
            "findings": dict(summary),
            "note": (
                "The corpus text is not vendored: upstream is CC BY-NC-SA 4.0 and derives from "
                "CCAT material with its own access terms. This file records the citations the "
                "corpus run verified and a SHA-256 of the Greek quoted on each card, so a later "
                "edit makes the offline check ask for a fresh corpus run."
            ),
        },
        "cards": entries,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return payload


def check_fingerprints(peter: Evidence, path: Path = FINGERPRINTS_PATH) -> list[Finding]:
    """Offline: has the Greek of an OT-anchored card changed since the last run?"""
    stored = json.loads(path.read_text(encoding="utf-8"))
    expected: dict[str, dict] = stored.get("cards", {})
    current = fingerprints(peter)
    findings: list[Finding] = []
    for card_id in sorted(set(current) | set(expected)):
        if card_id not in current:
            findings.append(
                Finding(
                    "lxx.card_vanished",
                    "ot_citations",
                    card_id,
                    "the card is gone; re-run the corpus check to refresh the record",
                )
            )
            continue
        if card_id not in expected:
            findings.append(
                Finding(
                    "lxx.card_not_reviewed",
                    "ot_citations",
                    card_id,
                    "new OT-anchored card; run the script with --corpus to verify its quotations",
                )
            )
            continue
        if current[card_id]["quoted_sha256"] != expected[card_id]["quoted_sha256"]:
            findings.append(
                Finding(
                    "lxx.quotations_changed",
                    "ot_citations",
                    card_id,
                    "the Greek of this card changed; re-run the script with --corpus",
                )
            )
        elif current[card_id]["refs"] != expected[card_id]["refs"]:
            findings.append(
                Finding(
                    "lxx.references_changed",
                    "ot_citations",
                    card_id,
                    f"citations now {current[card_id]['refs']}; re-run the script with --corpus",
                )
            )
    return findings


def _bank():
    from scripts.verify_greek_evidence import _bank as greek_bank

    return greek_bank()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--corpus", type=Path, help="LXX text file (.zip or .csv) to verify against")
    parser.add_argument("--versification", type=Path, help="word-index to verse map from the same repository")
    parser.add_argument("--write-fingerprints", action="store_true", help="record the verified citation states")
    parser.add_argument("--fingerprints", type=Path, default=FINGERPRINTS_PATH, help="file to compare with")
    parser.add_argument("--json", type=Path, help="write findings as JSON")
    args = parser.parse_args(argv)

    peter = Evidence.load(EVIDENCE_PATH)
    if not args.corpus:
        findings = check_fingerprints(peter, args.fingerprints)
        for finding in findings:
            print(f"{finding.check_id}: {finding.pool}/{finding.item_id}: {finding.message}")
        recorded = json.loads(args.fingerprints.read_text(encoding="utf-8")).get("provenance", {})
        print(
            f"lxx citations: {len(findings)} finding(s) (offline fingerprint mode, "
            f"{recorded.get('cards')} cards recorded over {recorded.get('verses_checked')} verses)"
        )
        if args.json:
            args.json.write_text(json.dumps([f.__dict__ for f in findings], ensure_ascii=False, indent=1), encoding="utf-8")
        return 1 if findings else 0

    if not args.versification:
        parser.error("--corpus needs --versification (see the module docstring for both URLs)")
    wanted = {reference.key for _pool, card in _bank() for reference in card_references(card)}
    verses = corpus_verses(args.corpus, args.versification, wanted)
    missing = sorted(key for key in wanted if key.count(".") == 2 and key not in verses)
    findings = check_ot_quotations(verses, peter)
    for finding in findings:
        print(f"{finding.check_id}: {finding.pool}/{finding.item_id}: {finding.message}")
    print(
        f"lxx evidence: {len(findings)} finding(s) "
        f"({len(verses)} verses of {len(wanted)} references, {len(missing)} not in the corpus)"
    )
    if missing:
        print("references absent from the corpus: " + ", ".join(missing[:10]))
    if args.json:
        args.json.write_text(json.dumps([f.__dict__ for f in findings], ensure_ascii=False, indent=1), encoding="utf-8")
    if args.write_fingerprints:
        payload = build_fingerprint_file(
            peter,
            verses,
            args.fingerprints,
            corpus_sha256=sha256_of(args.corpus),
            versification_sha256=sha256_of(args.versification),
            findings=findings,
        )
        print(f"fingerprints: {payload['provenance']['cards']} cards over {payload['provenance']['verses_checked']} verses")
    blocking = [f for f in findings if f.check_id == "lxx.form_not_in_cited_verses"]
    return 1 if blocking else 0


if __name__ == "__main__":
    raise SystemExit(main())
