#!/usr/bin/env python3
"""Question-bank pedagogical/editorial audit.

Structural and source checks already exist in tests/.  This script audits the
dimensions that are hard to assert with hard gates but are decisive for a
seminary-grade course that must stay readable for ordinary users:

  1. structural validity (4 options, valid correct index, unique ids);
  2. source-id resolution and source coverage;
  3. competitive-integrity rules (application/contested never competitive);
  4. content-truth taxonomy hygiene (application mislabelled as "contested",
     exegetical cards mistyped as "history", etc.);
  5. internal authoring-pipeline jargon leaking into user-facing text;
  6. plain English scholarly vocabulary in otherwise Russian cards;
  7. English grammar abbreviations in Greek-card options;
  8. thin / non-teaching explanations;
  9. answer-shape giveaways (correct option far longer than every distractor);
 10. MorphGNT parse-table consistency (internal) plus verification against
     the canonical upstream corpus vendored at vendor/morphgnt/81-1Pe-
     morphgnt.txt (AGENTS.md section 4); an alternative corpus path may be
     supplied via --morphgnt.

Usage:
    python3 scripts/audit_question_quality.py [--morphgnt path] [--json out]

Exit code is non-zero when any finding of severity "error" is present.
Warnings are reported but do not fail the run.
"""
from __future__ import annotations

import argparse
import collections
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import questions as Q
from questions.source_registry import SOURCE_CATALOG

LEARNING_POOLS = [
    "easy_p1", "easy_p2", "medium_p1", "medium_p2", "hard_p1", "hard_p2",
    "practical_p1", "practical_p2",
    "linguistics_ch1", "linguistics_ch1_2", "linguistics_ch1_3",
    "nero", "geography", "intro1", "intro2", "intro3",
    "chapter2", "chapter3", "chapter4", "chapter5",
]
REVIEWED_CHAPTERS = ("chapter2", "chapter3", "chapter4", "chapter5")

# ---------------------------------------------------------------------------
# Term catalogues
# ---------------------------------------------------------------------------

# Internal authoring/verification vocabulary that must never reach users.
PIPELINE_TERMS = [
    "Wave3n", "Wave 3n", "dECM", "readback", "witness table", "witness-table",
    "production-банк", "production банк", "production-status", "production-card",
    "production-status", "evidence route", "evidence routes", "evidence policy",
    "evidence boundary", "evidence lane", "evidence-first", "publication layer",
    "publication-safe", "project guardrail", "project-евангельский guardrail",
    "guardrail", "overclaim", "overclaims", "owner-level", "agent-level",
    "HOLD-", "HOLD ", "HOLD?", "HOLD.", "lexical fiat", "competitive pool",
    "flattening", "contractual proposal", "Research override",
    "research override", "release", "staging", "fail-closed", "admission",
    "inspected", "passage-level", "closure", "production-status",
    "witness-table", "secondary apparatus", "claim edge", "claim-edge",
    "text-base", "edition/text-base", "publication",
]

# English words/idioms in otherwise Russian cards that should be translated
# or glossed for ordinary readers.  Proper names, source abbreviations and the
# Greek surface forms themselves are allowed elsewhere.
ENGLISH_ISSUE_TERMS = [
    "baptismal efficacy", "baptismal-response", "baptismal systematics",
    "sacramental", "evangelical", "faith-confessional", "good-conscience",
    "vindication", "pastoral", "fallen-spirit", "fallen spirits", "Noah-like",
    "typological move", "descensus", "post-resurrection", "post-ascension",
    "preterist", "preterism", "futurism", "rapture", "cessationism",
    "Vice list", "vice list", "proverb reuse", "mixed-faith", "mixed-marriage",
    "household", "Greco-Roman", "prosperity-timetable", "secretary-theory",
    "polity", "denominational", "apologetics", "evidential", "textual criticism",
    "inspection", "Noah/flood", "Christ-through-Noah",
    "suffering-to-vindication", "triumph/vindication", "resurrection/exaltation",
    "Holy-Spirit-agency", "human-spirit", "appeal/request", "pledge/stipulation",
    "lexical-history", "flood-pattern", "Urzeit/Endzeit", "tense-form",
    "Named-witness", "witness-specific", "SBLGNT/base", "Meddling/interference",
    "Oracle/divine-utterance", "prophetic/imagery", "formal/sustained",
    "sphere/mode", "confession-related", "reading", "readings",
]

# English grammar abbreviations inside Greek-card options.
GRAMMAR_ABBR_RE = re.compile(
    r"\b(?:Pres|Perf|Aor|Aorist|ptcp|masc|fem|neut|Adj|inf|subj|ind|act|mid|pass|"
    r"nom|gen|dat|acc|sg|pl|imperative|infinitive|nominative|genitive|dative|"
    r"accusative|masculine|feminine|neuter|participle|subjunctive|indicative|"
    r"active|middle|passive|present|perfect|aorist|future|adjective|adverb|"
    r"noun|preposition|conjunction|pronoun|finite|optative|second|third|person)\b",
    re.IGNORECASE,
)

LATIN_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z/\-]{2,}")
# Tokens that are acceptable in a Russian product (sources, Greek-grammar tags,
# transliterated source names).
LATIN_ALLOWLIST = {
    "SBLGNT", "MorphGNT", "SBLGNT/MorphGNT", "ECM", "CBGM", "NA", "NA28",
    "UBS", "GTY", "TMSJ", "TMS", "LXX", "SBL", "NET", "ECM-based", "CBGM/ECM",
    "ECM/NA", "POS", "MT/common", "English", "STEP", "VarApp", "dECM", "Roma",
}
LATIN_PROPER = {
    "Noah", "Genesis", "Second", "Temple", "Watchers", "Williams", "Horrell",
    "Stanojevic", "Cotro", "Atkinson", "Richards", "Piper", "Cole", "Cambridge",
    "MacArthur", "Grudem", "Storms", "Crawford", "Charles", "Marcar", "Pierce",
    "Tacitus", "Suetonius", "Pliny", "Trajan", "Sinaiticus", "Codex", "Domus",
    "Aurea", "viae", "Silvanus", "Mark", "Peter", "Nero", "Claudius",
    "Agrippina", "Seneca", "Rome", "Babylon", "Antioch", "Ephesus", "Corinth",
    "Jerusalem", "Asia", "Pontus", "Galatia", "Cappadocia", "Bithynia", "Greek",
    "Hebrew", "James", "Lane", "Christensen", "Advice", "Bride", "Groom",
    "Isaianic", "Gentile",
}


def correct_index(q: dict):
    return q.get("correct_index", q.get("correct"))


def iter_cards():
    for pool_key in LEARNING_POOLS:
        for q in Q._POOLS[pool_key]:
            yield pool_key, q


class Findings:
    def __init__(self):
        self.errors = []
        self.warnings = []

    def err(self, pool, qid, code, detail):
        self.errors.append({"pool": pool, "id": qid, "code": code, "detail": detail})

    def warn(self, pool, qid, code, detail):
        self.warnings.append({"pool": pool, "id": qid, "code": code, "detail": detail})


def check_structure(f: Findings):
    seen = collections.defaultdict(list)
    for pool, q in iter_cards():
        qid = str(q.get("id") or "")
        seen[qid].append(pool)
        opts = q.get("options", [])
        ci = correct_index(q)
        if not isinstance(ci, int) or not 0 <= ci < len(opts):
            f.err(pool, qid, "bad_correct_index", str(ci))
        if len(opts) != 4:
            f.err(pool, qid, "option_count", len(opts))
        if len(set(opts)) != len(opts):
            f.err(pool, qid, "duplicate_options", "")
    for qid, pools in seen.items():
        if not qid or len(pools) > 1:
            f.err(",".join(pools), qid, "duplicate_or_missing_id", pools)


def check_sources(f: Findings):
    for pool, q in iter_cards():
        for sid in q.get("sources", []) or []:
            if sid not in SOURCE_CATALOG:
                f.err(pool, q.get("id"), "unknown_source", sid)
        # History claims are expected to carry evidence.  Chapter 4 strips
        # sources at its reviewed runtime boundary by design (they live in its
        # review records), so it is exempt at runtime.
        if (
            q.get("claim_type") == "history"
            and not q.get("sources")
            and pool != "chapter4"
        ):
            f.warn(pool, q.get("id"), "history_without_sources",
                   q.get("question", "")[:90])


def check_competitive(f: Findings):
    for pool, q in iter_cards():
        if q.get("competitive") is True and (
            q.get("claim_type") == "application"
            or q.get("confidence") == "contested"
            or q.get("position") == "project"
        ):
            f.err(pool, q.get("id"), "competitive_integrity",
                  f"{q.get('claim_type')}/{q.get('confidence')}/{q.get('position')}")


def check_taxonomy(f: Findings):
    # Legacy chapter-1 application pool tags pastoral application as
    # "contested"; that conflates pastoral application with genuinely disputed
    # interpretation (see AGENTS.md section 3).
    for pool in ("practical_p1", "practical_p2"):
        for q in Q._POOLS[pool]:
            if q.get("confidence") == "contested":
                f.warn(pool, q.get("id"), "application_tagged_contested",
                       "application should be medium/high + project, not contested")
    # Exegetical "what does the verse mean" cards mistyped as history.
    for pool in ("easy_p1", "easy_p2", "medium_p1", "medium_p2", "hard_p1", "hard_p2"):
        for q in Q._POOLS[pool]:
            if q.get("claim_type") == "history" and not q.get("sources"):
                verse_terms = ("что означает", "как ", "почему", "что подчёркивает",
                               "что наиболее точно", "тезис", "суммирует")
                stem = q.get("question", "").lower()
                if any(t in stem for t in verse_terms) and "император" not in stem:
                    f.warn(pool, q.get("id"), "exegesis_typed_history", stem[:90])


def _user_text(q: dict) -> str:
    return "\n".join([
        str(q.get("question", "")),
        *[str(o) for o in q.get("options", [])],
        str(q.get("explanation", "")),
    ])


def check_language(f: Findings):
    for pool, q in iter_cards():
        if pool not in REVIEWED_CHAPTERS:
            continue
        qid = q.get("id")
        text = _user_text(q)
        stem_only = str(q.get("question", "")) + "\n" + "\n".join(map(str, q.get("options", [])))

        for term in PIPELINE_TERMS:
            if term in text:
                # "release" appears in benign English-free Russian contexts?
                # require Latin token around it; term list is English already.
                f.err(pool, qid, "pipeline_jargon", term)
                break

        english_hits = [t for t in ENGLISH_ISSUE_TERMS if t in text]
        if english_hits:
            f.warn(pool, qid, "english_terms", "; ".join(sorted(set(english_hits))[:5]))

        # Grammar English only matters in what the user reads while answering.
        if q.get("claim_type") == "greek" and GRAMMAR_ABBR_RE.search(stem_only):
            # Russian-localized grammar cards exist; flag English abbreviations.
            tokens = set(LATIN_TOKEN_RE.findall(stem_only))
            leftover = sorted(
                t for t in tokens
                if t not in LATIN_ALLOWLIST and t not in LATIN_PROPER
                and GRAMMAR_ABBR_RE.fullmatch(t)
            )
            if leftover:
                f.warn(pool, qid, "english_grammar_abbr", ", ".join(leftover[:8]))


def check_explanations(f: Findings):
    meta_re = re.compile(
        r"evidence route|разные вещи|разные вопросы|тестируем|специально|"
        r"claim edge|claim-edge|inspection depth|метадан", re.IGNORECASE)
    for pool in REVIEWED_CHAPTERS:
        for q in Q._POOLS[pool]:
            expl = str(q.get("explanation", ""))
            cyrillic = re.compile("[" + chr(0x0400) + "-" + chr(0x04FF) + "]")
            if len(expl) < 55:
                f.warn(pool, q.get("id"), "thin_explanation", expl)
            elif meta_re.search(expl) and not cyrillic.search(expl[40:]):
                f.err(pool, q.get("id"), "non_teaching_explanation", expl)


def check_answer_shape(f: Findings):
    for pool, q in iter_cards():
        opts = q.get("options", [])
        ci = correct_index(q)
        if not isinstance(ci, int) or len(opts) != 4:
            continue
        lengths = [len(str(o)) for o in opts]
        distr = [lengths[i] for i in range(4) if i != ci]
        mean_d = sum(distr) / 3
        if lengths[ci] > 2.3 * mean_d and lengths[ci] > 60:
            f.warn(pool, q.get("id"), "length_giveaway",
                   f"correct={lengths[ci]} distractor_mean={mean_d:.0f}")


def check_morphgnt_tables(f: Findings, morphgnt_path: Path | None):
    """Verify hand-maintained MorphGNT tables.

    Without the corpus we only report that upstream verification was skipped;
    with 81-1Pe-morphgnt.txt supplied, every (surface, lemma, tag) is checked
    against canonical rows.
    """
    try:
        from questions.chapter3 import greek_1_7, greek_8_12
    except Exception as exc:  # pragma: no cover
        f.err("chapter3", "-", "morphgnt_import", str(exc))
        return
    authored = {}
    for row in greek_1_7.MORPHGNT_EVIDENCE_1P3.values():
        authored[row["surface"].strip("⸀⸂⸃ⸯ")] = (row["lemma"], row["tag"].replace(" ", ""))
    for surface, (lemma, tag) in greek_8_12.MORPHGNT_3_8_12.items():
        authored[surface] = (lemma, tag.replace(" ", ""))

    if morphgnt_path is None:
        f.warn("chapter3", "-", "morphgnt_upstream_skipped",
               f"{len(authored)} authored parse rows not checked against upstream "
               "(vendor the canonical file as vendor/morphgnt/81-1Pe-morphgnt.txt "
               "or pass --morphgnt 81-1Pe-morphgnt.txt)")
        return

    # Row layout (whitespace separated):
    #   ref  POS  parse  surface(with punctuation)  normalized  lemma
    # e.g.  210309 V- -PAPNPM- εὐλογοῦντες, εὐλογοῦντες εὐλογέω
    canonical = collections.defaultdict(set)
    for line in morphgnt_path.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) != 6 or not parts[0].isdigit():
            continue
        _, pos, parse, surface, normalized, lemma = parts
        full_tag = (pos + parse).strip()
        cleaned = re.sub(r"[⸀⸂⸃⸄⸅⸆⸇⸈⸉⸊⸌,.;··()]", "", surface)
        for key in {surface, normalized, cleaned}:
            canonical[key].add((lemma, full_tag))
    for surface, (lemma, tag) in authored.items():
        rows = canonical.get(surface)
        if not rows:
            f.err("chapter3", surface, "morphgnt_form_missing", "not found in corpus")
            continue
        if (lemma, tag) not in rows:
            f.err("chapter3", surface, "morphgnt_mismatch",
                  f"authored={(lemma, tag)} canonical={sorted(rows)}")


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MORPHGNT = REPO_ROOT / "vendor" / "morphgnt" / "81-1Pe-morphgnt.txt"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--morphgnt",
        type=Path,
        default=DEFAULT_MORPHGNT if DEFAULT_MORPHGNT.exists() else None,
        help="Canonical MorphGNT/SBLGNT 1 Peter corpus (defaults to the "
             "vendored copy under vendor/morphgnt/)",
    )
    ap.add_argument("--json", dest="json_out", type=Path, default=None)
    args = ap.parse_args()

    f = Findings()
    check_structure(f)
    check_sources(f)
    check_competitive(f)
    check_taxonomy(f)
    check_language(f)
    check_explanations(f)
    check_answer_shape(f)
    check_morphgnt_tables(f, args.morphgnt)

    by_code = collections.Counter(x["code"] for x in f.errors + f.warnings)
    print("=" * 78)
    print("QUESTION BANK QUALITY AUDIT")
    print("=" * 78)
    total = sum(len(Q._POOLS[p]) for p in LEARNING_POOLS)
    print(f"cards audited: {total}")
    print(f"errors: {len(f.errors)}   warnings: {len(f.warnings)}")
    print("-" * 78)
    for code, n in sorted(by_code.items(), key=lambda kv: (-kv[1], kv[0])):
        severity = "ERR " if any(e["code"] == code for e in f.errors) else "warn"
        print(f"  [{severity}] {code:38s} {n}")
    print("-" * 78)
    for item in f.errors:
        print(f"ERR  {item['pool']}/{item['id']}  {item['code']}: {item['detail']}")
    if args.json_out:
        args.json_out.write_text(
            json.dumps({"errors": f.errors, "warnings": f.warnings},
                       ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return 1 if f.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
