#!/usr/bin/env python3
"""Question-bank quality audit for the production 1 Peter course.

Learner-facing text in this file is Russian, so the CI lint step grants this
script the same per-file ``RUF001`` exemption the repository already uses for
other Russian-content modules.

The repository already enforces *structural* content truth (metadata values,
source IDs, ranking boundaries, lane digests). This tool audits the axis that
structure cannot see: whether a shipped card is a real knowledge question at the
declared level, whether its options can be beaten without knowing the text, and
whether the learner-facing Russian is still readable for a non-specialist.

Check families
--------------

``metadata.*``     structural content-truth contract (values, sources, quorum).
``disputed.*``     required disputed-passage handling and hedge discipline.
``wiseness.*``     test-wiseness leaks: option-length/shape/position, joke options.
``depth.*``        pedagogy: explanation depth, recall-only items, meta-stems.
``language.*``     learner-facing readability: internal pipeline vocabulary.
``content.*``      historical trivia that is not connected to the letter.
``levels.*``       availability of reviewed difficulty tiers per pool.

Severities
----------

``blocker``  A card that must not be shown to a learner as-is.
``major``    A validity or depth defect that materially damages the item.
``minor``    A defensible item with a quality weakness worth paying down.
``info``     Coverage/measurement data; never a gate by itself.

Budget
------

``data/question-quality-budget.json`` records the accepted count of every
non-blocker finding per pool. ``--check`` fails when a count grows. The budget is
a ratchet: it may only shrink, and ``--write-budget`` is the only way to refresh
it after an intentional, reviewed fix.
"""
from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from questions.level_policy import POOL_LEVELS, derived_level, ladder_summary  # noqa: E402
BUDGET_PATH = ROOT / "data" / "question-quality-budget.json"
DEFAULT_REPORT = ROOT / "docs" / "QUESTION_BANK_AUDIT.md"

BLOCKER = "blocker"
MAJOR = "major"
MINOR = "minor"
INFO = "info"
SEVERITY_ORDER = (BLOCKER, MAJOR, MINOR, INFO)

# Production leaf pools. Aggregate unions (``easy``, ``random_all``,
# ``competitive_all`` ...) are deliberately excluded so one card is audited once.
LEAF_POOLS: tuple[str, ...] = (
    "easy_p1",
    "easy_p2",
    "medium_p1",
    "medium_p2",
    "hard_p1",
    "hard_p2",
    "practical_p1",
    "practical_p2",
    "linguistics_ch1",
    "linguistics_ch1_2",
    "linguistics_ch1_3",
    "nero",
    "geography",
    "tms_deep",
    "intro1",
    "intro2",
    "intro3",
    "chapter2",
    "chapter3",
    "chapter4",
    "chapter5",
)

# Pools written for a general reader: heavy untranslated Greek in these pools is
# a usability problem, not a feature.
PLAIN_READER_POOLS = frozenset(
    {
        "easy_p1",
        "easy_p2",
        "medium_p1",
        "medium_p2",
        "hard_p1",
        "hard_p2",
        "practical_p1",
        "practical_p2",
        "nero",
        "geography",
    }
)

# Chapter 3 and 4 must keep explicit coverage of the disputed passages named in
# docs/CONTENT_SOURCE_POLICY.md before their courses can claim to teach them.
REQUIRED_DISPUTED_COVERAGE: dict[str, tuple[str, ...]] = {
    "chapter3": ("3:19", "3:20", "3:21"),
    "chapter4": ("4:6",),
}

# --- learner-facing readability ------------------------------------------------

# Internal research-pipeline vocabulary that must never reach a learner.
PIPELINE_MARKERS: dict[str, str] = {
    "inspected": r"\binspected\b",
    "hold-workflow": r"\bHOLD\b|HOLD-[A-Z-]+",
    "internal-english": r"\bguardrail\b|\blane\b|\bbounded\b|\bWave\w*\b|\bproduction-status\b|production-банк",
    "artifact-vocabulary": r"publisher synopsis|synopsis|secondary apparatus|relevant section|passage-level|owner-level|direct dECM|dECM",
    "tooling-vocabulary": r"review record|research claim|staging bank|release projection",
}

# A stem that asks how the course should *describe* a claim instead of what the
# text says is a meta-question: it teaches the authoring process, not 1 Peter.
PROJECT_LABEL = re.compile(
    r"позици\w*\s+курса|позиция\s+этого\s+курса|этот\s+курс|курс\s+принимает|"
    r"традиционн\w*\s+позици\w*|позицию\s+курса",
    re.IGNORECASE,
)

META_STEM_MARKERS = re.compile(
    r"как\s+(?:лучше\s+всего\s+)?(?:корректно\s+|безопасн\w*\s+|осторожн\w*\s+|историческ\w*\s+|"
    r"исследовательск\w*\s+|пастырск\w*\s+|аккуратн\w*\s+)?(?:формулир\w+|описать|описывать|обращаться|"
    r"суммировать|преподав\w+|представить|соотносится|подавать)",
    re.IGNORECASE,
)

ABSURD_DISTRACTOR_MARKERS = re.compile(
    r"вообще не|ни разу|только морские|полностью бессмысл|случайные прозвища|"
    r"не имеет смысла|абсолютно все|все три варианта равновероятны",
    re.IGNORECASE,
)

# Learner-facing text is Russian. The sigla of the witnesses the course teaches
# (LXX, MT, SBLGNT, ECM, NA28, CBGM), the corpora it names (MorphGNT), the source
# names a lesson is allowed to name (Latin spellings of the scholars and journals
# the catalog carries), the Latin name of a debated doctrine the course is teaching
# ("cessationism"), Roman numerals and the corpus' own eight-column parse tags are
# all part of a Russian grammar lesson. Anything else in Latin script is the
# authoring pipeline showing through, so it is counted per pool.
ALLOWED_LATIN_WORDS = frozenset(
    {
        # Primary texts, corpora and databases whose canonical name is Latin.
        "LXX", "MT", "SBLGNT", "ECM", "dECM", "NA", "NA28", "CBGM", "MorphGNT",
        "Pleiades", "ORBIS", "STEP", "Vaticanus", "Sinaiticus", "Alexandrinus",
        # Journals and reference works the source catalog cites.
        "TMSJ", "TGC", "UBS", "JETS", "NTS", "JBL", "IBR",
        # Bible translations: sigla, not prose.
        "ESV", "LSB", "NIV", "NASB", "KJV", "NKJV", "NRSV",
        # The Latin name of the debated doctrine a lesson is teaching.
        "cessationism",
        # Surnames of the scholars the course names, transliterated in the same
        # sentence. Keeping the Latin spelling lets a reader find the literature.
        "Achtemeier", "Atkinson", "Best", "Bigg", "Carson", "Cole", "Cross",
        "Davids", "Donelson", "Elliott", "Fee", "Goppelt", "Grudem", "Hengel",
        "Horrell", "Jobes", "Kelly", "MacArthur", "Marcar", "Michaels", "Moo",
        "Schreiner", "Selwyn", "Storms", "Williams",
    }
)
# A token is a run of Latin letters and digits that contains at least one letter,
# so a corpus tag ("2AAD-P--") is seen whole and a bare number is not a word.
LATIN_WORD = re.compile(r"(?=[A-Za-z0-9\-]*[A-Za-z])[A-Za-z0-9][A-Za-z0-9\-]{1,}")
PARSE_TAG = re.compile(r"^[0-9APMIDXFSON-]{8}$")
# A tag written with its leading dash ("-XPPNPM-") is one token in the text, so the
# tags are masked out before the Latin scan instead of being matched token-wise.
PARSE_TAG_IN_TEXT = re.compile(r"(?<![\w-])[0-9APMIDXFSON-]{8}(?![\w-])")
ROMAN_NUMERAL = re.compile(r"^[IVXLC]+$")
# A parenthetical that also carries a Russian gloss explains its Latin, so the
# gloss itself is not the pipeline showing through: "(лат. viae — дороги)".
PARENTHETICAL = re.compile(r"\(([^()]*)\)")
# Notes written for the reviewer, not for the learner: a wording pass left one in
# the text ("the old automatic glosses in this question were broken").
INTERNAL_NOTE_MARKERS = re.compile(
    r"старый вариант|прежн\w+ вариант|был сломан|были поврежден\w+|"
    r"автоматическ\w+ глосс\w*|повреждён\w+ и дублировал\w+|чернов\w+ вариант|"
    r"внутренн\w+ заметк",
    re.IGNORECASE,
)

# Absolute-certainty words are legitimate for direct text facts but unacceptable
# inside a card the course itself calls contested. The rule is sentence-scoped and
# negation-aware so that "это не доказывает" is not mistaken for an overclaim.
OVERCLAIM_PATTERNS = (
    r"\bоднозначно\b",
    r"\bбесспорно\b",
    r"\bнесомненно\b",
    r"доказанн\w+\s+факт",
    r"\bэто\s+доказывает\b",
    r"\bустановлено,?\s+что\b",
    r"\bисключительно\b",
    r"\bневозможно\b",
    r"\bнеизбежно\b",
)
OVERCLAIM_MARKERS = re.compile("|".join(OVERCLAIM_PATTERNS), re.IGNORECASE)
NEGATED_ASSERTION = re.compile(
    r"\bне\s+\w+|\bнельзя\b|\bбез\s+\w+|неверн\w+|ошибочн\w+|сомнительн\w+",
    re.IGNORECASE,
)

HEDGE_MARKERS = re.compile(
    r"\bобсужда\w+|\bспор\w+|\bдискус\w+|\bвероятн\w+|\bвозможн\w+|\bгипотез\w+|\bреконструкц\w+|\b"
    r"осторожн\w+|\bне\s+доказыва\w+|\bодна\s+морфологи\w+|\bинтерпретац\w+",
    re.IGNORECASE,
)

# "Trivia" here means a question whose content is an ancient-world fact that the
# letter neither states nor needs: it is answerable without ever opening 1 Peter.
TRIVIA_MARKERS = re.compile(
    r"как звали|как называется|какая сфера|чей (?:дворец|сын)|"
    r"в каком году (?:нерон|родил|умер)|сколько районов|какое море",
    re.IGNORECASE,
)

RECALL_MARKERS = re.compile(
    r"^(?:какие|сколько|кто|что)\b.*\b(?:перечисл\w+|назван\w+|призыв\w+|говорит)",
    re.IGNORECASE,
)

PARSING_MARKER = re.compile(
    r"\b(?:Aor|Perf|Pres|Impf|Fut|Nom|Gen|Dat|Acc|Voc|Ptcp|Part|Ind|Subj|Opt|Imper|Act|Pass|Mid)\b\.?"
    r"|\b[0-9][A-Z]{2}[A-Z]-[A-Z-]{3}\b|\b-[A-Z]{3}[A-Z-]{3}-\b",
)

GREEK_RUN = re.compile(r"[\u0370-\u03ff\u1f00-\u1fff]{4,}")
LETTER = re.compile(r"[\w\u0370-\u03ff\u1f00-\u1fff]+", re.UNICODE)

EXPLANATION_MIN_CHARS = 90
EXPLANATION_MAJOR_MIN_CHARS = 60
LENGTH_LEAK_RATIO = 1.35
LENGTH_LEAK_MIN_DELTA = 12
OPTION_SHAPE_MIN_SHARE = 0.45

# Option sets whose typical option is a short label ("Рим", "Иерусалим",
# "Aor. act. subj.", "Исх. 19:5-6", "Нелицемерная") cannot be length-balanced:
# the difference comes from the name of the referent or from the size of a
# grammatical tag, not from authoring emphasis, and lengthening a name would
# change the fact being tested. The median (not the maximum) decides, so one
# over-long distractor in an otherwise label-like set does not hide the pattern.
# Those cards are reported as INFO context instead of MAJOR/MINOR and stay in
# the priority list.
LABEL_SET_MAX_CHARS = 30
INDEX_SKEW_THRESHOLD = 0.5
INDEX_SKEW_MIN_POOL = 10


@dataclass(frozen=True)
class Finding:
    check_id: str
    severity: str
    pool: str
    item_id: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {
            "check_id": self.check_id,
            "severity": self.severity,
            "pool": self.pool,
            "item_id": self.item_id,
            "message": self.message,
        }


@dataclass
class PoolMetrics:
    pool: str
    cards: int = 0
    options: int = 0
    mean_option_chars: float = 0.0
    mean_explanation_chars: float = 0.0
    correct_longest_share: float = 0.0
    length_leak_share: float = 0.0
    index_skew: float = 0.0
    claim_types: dict[str, int] = field(default_factory=dict)
    confidence: dict[str, int] = field(default_factory=dict)
    competitive: int = 0
    verses: int = 0
    tiered: int = 0
    tiers: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "pool": self.pool,
            "cards": self.cards,
            "options": self.options,
            "mean_option_chars": round(self.mean_option_chars, 1),
            "mean_explanation_chars": round(self.mean_explanation_chars, 1),
            "correct_longest_share": round(self.correct_longest_share, 3),
            "length_leak_share": round(self.length_leak_share, 3),
            "index_skew": round(self.index_skew, 3),
            "claim_types": self.claim_types,
            "confidence": self.confidence,
            "competitive": self.competitive,
            "verses": self.verses,
            "tiered": self.tiered,
            "tiers": self.tiers,
        }


@dataclass
class AuditReport:
    findings: list[Finding]
    metrics: list[PoolMetrics]
    pools: dict[str, int]

    def counts(self) -> dict[str, dict[str, int]]:
        counts: dict[str, dict[str, int]] = defaultdict(dict)
        for finding in self.findings:
            counts[finding.check_id][finding.pool] = (
                counts[finding.check_id].get(finding.pool, 0) + 1
            )
        counts["__all__"] = {pool: size for pool, size in sorted(self.pools.items())}
        return {check: dict(sorted(pool.items())) for check, pool in sorted(counts.items())}

    def by_severity(self, severity: str) -> list[Finding]:
        return [finding for finding in self.findings if finding.severity == severity]

    def by_check(self, check_id: str) -> list[Finding]:
        return [finding for finding in self.findings if finding.check_id == check_id]

    def pool_findings(self, pool: str, severity: str | None = None) -> list[Finding]:
        return [
            finding
            for finding in self.findings
            if finding.pool == pool and (severity is None or finding.severity == severity)
        ]


def _pool_items() -> dict[str, list[dict]]:
    import questions

    pools: dict[str, list[dict]] = {}
    for key in LEAF_POOLS:
        pools[key] = list(questions.get_pool_by_key(key))
    return pools


def _source_catalog() -> dict[str, dict]:
    import questions

    return dict(questions.SOURCE_CATALOG)


def _normalize(text: str) -> str:
    return " ".join(match.group(0) for match in LETTER.finditer(text.casefold()))


def _tokens(text: str) -> set[str]:
    return {token for token in _normalize(text).split() if len(token) > 3}


def _latin_jargon(*texts: str) -> list[str]:
    """Latin-script words that a Russian lesson has not explained.

    The corpus' parse tags are masked out first (a tag written as "-XPPNPM-" is one
    token), then every parenthetical that also carries Cyrillic - a gloss such as
    "(лат. viae - дороги)" or "(NIV, ESV, LSB)". What is left must be a siglum, a
    database name, a scholar's Latin spelling or a Roman numeral.
    """

    blob = PARSE_TAG_IN_TEXT.sub(" ", " ".join(str(text) for text in texts))
    for span in PARENTHETICAL.findall(blob):
        if re.search(r"[\u0400-\u04ff]", span):
            blob = blob.replace(span, " ")
    return sorted(
        {
            word
            for word in LATIN_WORD.findall(blob)
            if word not in ALLOWED_LATIN_WORDS
            and not PARSE_TAG.match(word)
            and not ROMAN_NUMERAL.match(word)
        }
    )


def _visible(card: dict) -> tuple[str, str, str]:
    return str(card.get("question", "")), str(card.get("explanation", "")), " ".join(
        str(option) for option in card.get("options", ())
    )


def _stem_and_options(card: dict) -> str:
    question, _, options = _visible(card)
    return f"{question} {options}"


def _required_quorum(claim_type: str, position: str) -> int:
    if claim_type == "text":
        return 1
    if claim_type == "greek":
        return 2
    if claim_type == "history":
        return 2
    if position == "project":
        return 2
    return 1


def _audit_metadata(card: dict, pool: str, catalog: dict[str, dict]) -> list[Finding]:
    findings: list[Finding] = []
    item = str(card.get("id") or "<no-id>")

    for field_name in ("claim_type", "confidence", "position", "competitive"):
        if field_name not in card:
            findings.append(
                Finding("metadata.missing_field", BLOCKER, pool, item, f"missing {field_name}")
            )
    claim_type = str(card.get("claim_type") or "")
    if claim_type not in {"text", "greek", "history", "interpretation", "application"}:
        findings.append(
            Finding("metadata.invalid_value", BLOCKER, pool, item, f"claim_type={claim_type!r}")
        )
    confidence = str(card.get("confidence") or "")
    if confidence not in {"high", "medium", "contested"}:
        findings.append(
            Finding("metadata.invalid_value", BLOCKER, pool, item, f"confidence={confidence!r}")
        )
    position = str(card.get("position") or "")
    if position not in {"neutral", "project"}:
        findings.append(
            Finding("metadata.invalid_value", BLOCKER, pool, item, f"position={position!r}")
        )
    if not isinstance(card.get("competitive"), bool):
        findings.append(
            Finding("metadata.invalid_value", BLOCKER, pool, item, "competitive is not bool")
        )

    source_ids = card.get("sources")
    if card.get("review_record_id"):
        # Chapter 4 v2 intentionally keeps claim evidence behind the review record.
        return findings
    if not isinstance(source_ids, list) or not source_ids:
        findings.append(
            Finding("metadata.unresolved_source", BLOCKER, pool, item, "no source ids")
        )
        return findings
    unknown = sorted({str(source) for source in source_ids} - set(catalog))
    if unknown:
        findings.append(
            Finding("metadata.unresolved_source", BLOCKER, pool, item, f"unknown {unknown}")
        )
    quorum = _required_quorum(claim_type, position)
    if len({str(source) for source in source_ids}) < quorum:
        findings.append(
            Finding(
                "metadata.source_quorum",
                MAJOR,
                pool,
                item,
                f"{claim_type}/{position} has {len(source_ids)} source(s), policy quorum {quorum}",
            )
        )
    if claim_type == "greek":
        source_set = {str(source) for source in source_ids}
        blob = f"{card.get('question', '')} {card.get('explanation', '')} {' '.join(map(str, card.get('options', ())))}"
        if PARSING_MARKER.search(blob) and not any("morphgnt" in source for source in source_set):
            findings.append(
                Finding(
                    "metadata.source_quorum",
                    MAJOR,
                    pool,
                    item,
                    "parsing claim without MorphGNT/SBLGNT morphology control",
                )
            )
    if position == "project":
        # docs/CONTENT_SOURCE_POLICY.md requires the project position to be visible in
        # user-facing wording or explanation -- either as the canonical prefix or as an
        # explicit reference to the course position inside the text itself.
        visible = f"{card.get('question', '')} {card.get('explanation', '')}"
        labelled = str(card.get("question", "")).startswith("[Позиция курса]") or PROJECT_LABEL.search(visible)
        if not labelled:
            findings.append(
                Finding(
                    "metadata.project_position_unlabelled",
                    BLOCKER,
                    pool,
                    item,
                    "project card does not identify the course position",
                )
            )
    if position == "project" and card.get("competitive") is True:
        findings.append(
            Finding("metadata.competitive_violation", BLOCKER, pool, item, "project card is competitive")
        )
    if confidence == "contested" and card.get("competitive") is True:
        findings.append(
            Finding("metadata.competitive_violation", BLOCKER, pool, item, "contested card is competitive")
        )
    if claim_type in {"greek", "history", "application"} and card.get("competitive") is True:
        findings.append(
            Finding(
                "metadata.competitive_violation",
                BLOCKER,
                pool,
                item,
                f"{claim_type} card is competitive",
            )
        )
    return findings


def _audit_wiseness(card: dict, pool: str) -> list[Finding]:
    findings: list[Finding] = []
    item = str(card.get("id") or "<no-id>")
    options = [str(option) for option in card.get("options", ())]
    correct = card.get("correct")
    if len(options) < 2 or not isinstance(correct, int) or not 0 <= correct < len(options):
        findings.append(
            Finding("wiseness.invalid_option_set", BLOCKER, pool, item, "options/correct index invalid")
        )
        return findings

    lengths = [len(option) for option in options]
    others = [length for index, length in enumerate(lengths) if index != correct]
    correct_length = lengths[correct]
    ordered = sorted(lengths)
    middle = len(ordered) // 2
    median = (
        ordered[middle]
        if len(ordered) % 2
        else (ordered[middle - 1] + ordered[middle]) / 2
    )
    label_set = median <= LABEL_SET_MAX_CHARS
    if correct_length > max(others):
        findings.append(
            Finding(
                "wiseness.correct_longest",
                INFO if label_set else MAJOR,
                pool,
                item,
                f"correct option is uniquely longest ({correct_length} vs max {max(others)})"
                + (
                    "; label set - the difference is the length of a name, "
                    "reference or grammatical tag"
                    if label_set
                    else ""
                ),
            )
        )
    if (
        others
        and correct_length >= LENGTH_LEAK_RATIO * statistics.mean(others)
        and correct_length - max(others) >= LENGTH_LEAK_MIN_DELTA
    ):
        findings.append(
            Finding(
                "wiseness.length_leak",
                MAJOR,
                pool,
                item,
                f"length alone reveals the answer ({correct_length} vs {sorted(others)})",
            )
        )

    if max(lengths) and min(lengths) / max(lengths) < OPTION_SHAPE_MIN_SHARE:
        findings.append(
            Finding(
                "wiseness.option_shape_spread",
                INFO if label_set else MINOR,
                pool,
                item,
                f"options are not comparable in shape ({min(lengths)}..{max(lengths)})"
                + ("; label set - lexical length only" if label_set else ""),
            )
        )

    normalized = [_normalize(option) for option in options]
    if len(set(normalized)) != len(normalized):
        findings.append(
            Finding("wiseness.duplicate_options", BLOCKER, pool, item, "duplicate options")
        )

    for index, option in enumerate(options):
        if index != correct and ABSURD_DISTRACTOR_MARKERS.search(option):
            findings.append(
                Finding(
                    "wiseness.trivial_distractor",
                    MINOR,
                    pool,
                    item,
                    f"unfalsifiable/joke distractor: {option[:60]!r}",
                )
            )
    return findings


LETTER_REFERENCE = re.compile(
    r"1\s*Пет[а-я]*\.?\s*\d+\s*:\s*\d+|\b[1-5]\s*:\s*\d+",
    re.IGNORECASE,
)


def _needs_verse_anchor(card: dict) -> bool:
    """Return True when a card rests on 1 Peter and must name its verse.

    Cards that carry the text, its grammar, its interpretation or its
    application are anchored to a passage; that anchor is how a learner checks
    the claim. Pure history cards (the Roman setting, the letter's reception)
    rest on the source list that ``metadata.source_quorum`` verifies, so they
    need an external witness, not a verse, unless they themselves cite a passage
    of the letter.
    """
    if str(card.get("claim_type")) in {"text", "greek", "interpretation", "application"}:
        return True
    blob = " ".join(str(card.get(field) or "") for field in ("question", "explanation"))
    return bool(LETTER_REFERENCE.search(blob))


def _audit_depth(card: dict, pool: str) -> list[Finding]:
    findings: list[Finding] = []
    item = str(card.get("id") or "<no-id>")
    question, explanation, _ = _visible(card)
    correct = card.get("correct")
    options = [str(option) for option in card.get("options", ())]

    if len(explanation) < EXPLANATION_MAJOR_MIN_CHARS:
        findings.append(
            Finding(
                "depth.explanation_very_short",
                MAJOR,
                pool,
                item,
                f"explanation is {len(explanation)} chars (<{EXPLANATION_MAJOR_MIN_CHARS})",
            )
        )
    elif len(explanation) < EXPLANATION_MIN_CHARS:
        findings.append(
            Finding(
                "depth.explanation_short",
                MINOR,
                pool,
                item,
                f"explanation is {len(explanation)} chars (<{EXPLANATION_MIN_CHARS})",
            )
        )
    if isinstance(correct, int) and 0 <= correct < len(options):
        correct_tokens = _tokens(options[correct])
        explanation_tokens = _tokens(explanation)
        if correct_tokens and explanation_tokens:
            overlap = len(correct_tokens & explanation_tokens) / len(correct_tokens)
            if overlap >= 0.9 and not (explanation_tokens - correct_tokens):
                findings.append(
                    Finding(
                        "depth.explanation_restates",
                        MINOR,
                        pool,
                        item,
                        "explanation only restates the correct option",
                    )
                )

    if META_STEM_MARKERS.search(question):
        # Application cards are allowed to ask how a truth is to be presented; a
        # factual card that only asks how to *phrase* its own claim is a meta item.
        severity = MINOR if str(card.get("claim_type")) == "application" else MAJOR
        findings.append(
            Finding(
                "depth.meta_phrasing",
                severity,
                pool,
                item,
                f"stem asks how to phrase the claim: {question[:70]!r}",
            )
        )
    if str(card.get("claim_type")) == "text" and RECALL_MARKERS.search(question):
        findings.append(
            Finding("depth.recall_only", INFO, pool, item, "single-clause recall item")
        )
    if not str(card.get("verse") or "").strip() and _needs_verse_anchor(card):
        findings.append(
            Finding(
                "content.missing_verse",
                MINOR,
                pool,
                item,
                "the card rests on the letter but names no verse anchor",
            )
        )
    return findings


def _audit_language(card: dict, pool: str) -> list[Finding]:
    findings: list[Finding] = []
    item = str(card.get("id") or "<no-id>")
    question, explanation, options = _visible(card)
    stem_blob = f"{question} {options}"

    for name, pattern in PIPELINE_MARKERS.items():
        if re.search(pattern, stem_blob, re.IGNORECASE):
            findings.append(
                Finding(
                    "language.pipeline_vocabulary_in_stem",
                    BLOCKER,
                    pool,
                    item,
                    f"{name} visible to the learner",
                )
            )
            break
    for name, pattern in PIPELINE_MARKERS.items():
        if re.search(pattern, explanation, re.IGNORECASE):
            findings.append(
                Finding(
                    "language.pipeline_vocabulary_in_explanation",
                    MAJOR,
                    pool,
                    item,
                    f"{name} in explanation",
                )
            )
            break

    if pool in PLAIN_READER_POOLS:
        for option in options:
            greek = GREEK_RUN.search(option)
            if greek and not re.search(r"[\u0400-\u04ff]", option):
                findings.append(
                    Finding(
                        "language.unglossed_greek",
                        MINOR,
                        pool,
                        item,
                        f"Greek without Russian gloss: {greek.group(0)!r}",
                    )
                )
                break
    jargon = sorted(set(_latin_jargon(question, options, explanation)))
    if jargon:
        findings.append(
            Finding(
                "language.latin_jargon",
                INFO,
                pool,
                item,
                f"Latin-script words outside the allowed register: {jargon[:4]}",
            )
        )
    note = INTERNAL_NOTE_MARKERS.search(f"{question} {options} {explanation}")
    if note:
        findings.append(
            Finding(
                "language.internal_note",
                BLOCKER,
                pool,
                item,
                f"a note written for the reviewer reaches the learner: {note.group(0)!r}",
            )
        )
    anchored = bool(re.search(r"1\s*Пет|Петра", question))
    if TRIVIA_MARKERS.search(question) and not anchored:
        findings.append(
            Finding(
                "content.trivia",
                MINOR,
                pool,
                item,
                f"ancient-world trivia not anchored in the letter: {question[:70]!r}",
            )
        )
    return findings


QUOTED_SPAN = re.compile("«[^»]*»|\"[^\"]*\"")


def _unhedged_overclaim(blob: str) -> str | None:
    # A word quoted in order to name and reject it ("слово «невозможно» было бы
    # слишком сильным выводом") is a mention, not an assertion.
    blob = QUOTED_SPAN.sub(" ", blob)
    for sentence in re.split(r"(?<=[.!?;])\s+", blob):
        match = OVERCLAIM_MARKERS.search(sentence)
        if not match:
            continue
        if HEDGE_MARKERS.search(sentence):
            continue
        head = sentence[: match.start()]
        if NEGATED_ASSERTION.search(head):
            continue
        return match.group(0)
    return None


def _audit_disputed(card: dict, pool: str) -> list[Finding]:
    findings: list[Finding] = []
    item = str(card.get("id") or "<no-id>")
    _, explanation, _options = _visible(card)
    if str(card.get("confidence")) != "contested":
        return findings
    # Only the claim the card actually asserts is checked: its explanation and
    # the keyed answer. Wrong options may legitimately use absolute wording.
    options_list = [str(option) for option in card.get("options", ())]
    correct = card.get("correct")
    keyed = options_list[correct] if isinstance(correct, int) and 0 <= correct < len(options_list) else ""
    hit = _unhedged_overclaim(f"{explanation} {keyed}")
    if hit:
        findings.append(
            Finding(
                "disputed.overclaim_certainty",
                MAJOR,
                pool,
                item,
                f"contested card argues with unhedged certainty ({hit!r})",
            )
        )
    return findings


# Difficulty tiers. The bank has no reviewed ``level`` field yet, so the audit
# derives a three-tier mix from *already reviewed* metadata only: a direct text
# observation is a base item, interpretation/application/multi-source work is a
# core item, and work that requires Greek, history or a genuinely contested
# judgement is advanced. The derivation is reported, never used to gate a card.
TIER_ORDER = ("base", "core", "advanced")

# Chapter-1 pools are the only part of the bank whose name states a difficulty
# ladder; everything else groups cards by book, chapter or topic. The check reports
# how far the derived tier of such a pool matches the tier the name asserts - that
# is evidence about the ladder, and it is not a claim that any single card was
# individually reviewed for difficulty.
POOL_TIER_CLAIMS = {pool.rsplit("_", 1)[0]: level for pool, level in POOL_LEVELS.items()}


def _derived_difficulty(card: dict) -> str:
    """The proxy tier; the rule itself lives in ``questions/level_policy.py``."""
    return derived_level(card)


def _pool_metrics(
    pool: str,
    cards: list[dict],
) -> tuple[PoolMetrics, list[Finding]]:
    findings: list[Finding] = []
    metrics = PoolMetrics(pool=pool, cards=len(cards))
    option_lengths: list[int] = []
    explanation_lengths: list[int] = []
    correct_index = Counter()
    verses: set[str] = set()
    claim_types: Counter[str] = Counter()
    confidence: Counter[str] = Counter()
    longer = 0
    leaking = 0
    tiers: Counter[str] = Counter()

    for card in cards:
        options = [str(option) for option in card.get("options", ())]
        correct = card.get("correct")
        option_lengths.extend(len(option) for option in options)
        explanation_lengths.append(len(str(card.get("explanation", ""))))
        if isinstance(correct, int) and 0 <= correct < len(options):
            correct_index[correct] += 1
            if len(options) > 1:
                others = [len(o) for i, o in enumerate(options) if i != correct]
                if len(options[correct]) > max(others):
                    longer += 1
                if (
                    len(options[correct]) >= LENGTH_LEAK_RATIO * statistics.mean(others)
                    and len(options[correct]) - max(others) >= LENGTH_LEAK_MIN_DELTA
                ):
                    leaking += 1
        verse = str(card.get("verse") or "").strip()
        if verse:
            verses.add(verse)
        claim_types[str(card.get("claim_type"))] += 1
        confidence[str(card.get("confidence"))] += 1
        if card.get("competitive") is True:
            metrics.competitive += 1
        tier = _derived_difficulty(card)
        tiers[tier] += 1
        if card.get("level") or card.get("difficulty"):
            metrics.tiered += 1

    metrics.options = len(option_lengths)
    metrics.mean_option_chars = statistics.mean(option_lengths) if option_lengths else 0.0
    metrics.mean_explanation_chars = (
        statistics.mean(explanation_lengths) if explanation_lengths else 0.0
    )
    if cards:
        metrics.correct_longest_share = longer / len(cards)
        metrics.length_leak_share = leaking / len(cards)
    metrics.claim_types = dict(sorted(claim_types.items()))
    metrics.confidence = dict(sorted(confidence.items()))
    metrics.verses = len(verses)
    metrics.tiers = {tier: tiers.get(tier, 0) for tier in TIER_ORDER}
    if cards:
        metrics.index_skew = max(correct_index.values()) / len(cards) if correct_index else 0.0

    if cards:
        mix = f"{metrics.tiers['base']}/{metrics.tiers['core']}/{metrics.tiers['advanced']}"
        claimed = POOL_TIER_CLAIMS.get(pool.split("_")[0])
        if claimed:
            agree = metrics.tiers[claimed]
            message = (
                f"difficulty mix is derived from reviewed metadata (base/core/advanced = {mix}); "
                f"this pool's name asserts {claimed} and the derived tier agrees for {agree}/{len(cards)} "
                "cards, so the ladder is authored by pool, not per card"
            )
        else:
            message = (
                f"difficulty mix is derived from reviewed metadata (base/core/advanced = {mix}); "
                "no card carries a reviewed level field"
            )
        findings.append(Finding("levels.derived_tiers_only", INFO, pool, "-", message))
    if len(cards) >= INDEX_SKEW_MIN_POOL and metrics.index_skew >= INDEX_SKEW_THRESHOLD:
        findings.append(
            Finding(
                "wiseness.index_skew",
                MAJOR,
                pool,
                "-",
                f"correct answer lands in one slot in {metrics.index_skew:.0%} of cards "
                "(runtime shuffles, but the authoring order stays guessable)",
            )
        )
    return metrics, findings


def _audit_pool_coverage(pool: str, cards: list[dict]) -> list[Finding]:
    findings: list[Finding] = []
    required = REQUIRED_DISPUTED_COVERAGE.get(pool)
    if required:
        verses = " ".join(str(card.get("verse") or "") for card in cards)
        explanations = " ".join(str(card.get("explanation") or "") for card in cards)
        blob = f"{verses} {explanations}"
        for reference in required:
            if reference not in blob:
                findings.append(
                    Finding(
                        "disputed.passage_missing",
                        MAJOR,
                        pool,
                        "-",
                        f"no card covers disputed passage {reference}",
                    )
                )
    return findings


def audit() -> AuditReport:
    catalog = _source_catalog()
    pools = _pool_items()
    findings: list[Finding] = []
    metrics: list[PoolMetrics] = []
    sizes: dict[str, int] = {}

    for pool, cards in pools.items():
        sizes[pool] = len(cards)
        pool_metrics, pool_findings = _pool_metrics(pool, cards)
        metrics.append(pool_metrics)
        findings.extend(pool_findings)
        findings.extend(_audit_pool_coverage(pool, cards))
        for card in cards:
            findings.extend(_audit_metadata(card, pool, catalog))
            findings.extend(_audit_wiseness(card, pool))
            findings.extend(_audit_depth(card, pool))
            findings.extend(_audit_language(card, pool))
            findings.extend(_audit_disputed(card, pool))

    findings.sort(key=lambda finding: (SEVERITY_ORDER.index(finding.severity), finding.check_id, finding.pool, finding.item_id))
    return AuditReport(findings=findings, metrics=metrics, pools=sizes)


def _top_offenders(report: AuditReport, check_id: str, limit: int = 8) -> list[Finding]:
    return [finding for finding in report.findings if finding.check_id == check_id][:limit]


def _budget_counts(report: AuditReport) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for check_id, pools in report.counts().items():
        if check_id == "__all__":
            continue
        counts[check_id] = pools
    return counts


def citation_verification() -> list[str]:
    """Rendered status of the corpus-backed checks of the bank's own claims.

    The report measures the bank's own claims; this section answers the prior
    question - were they ever compared with the text? The offline halves of the
    checkers are cheap, so the report runs them and prints what they see.
    ``scripts/verify_greek_evidence.py`` covers 1 Peter against the vendored
    MorphGNT excerpt; ``scripts/verify_lxx_evidence.py`` covers the Old Testament
    quotations against the citation record of the last corpus run;
    ``scripts/verify_parse_claims.py`` compares the parse answer each grammar card
    keys with the morphology row of the form it quotes.
    """
    from scripts.verify_greek_evidence import (
        EVIDENCE_PATH,
        Evidence,
        check_metadata_claims,
        check_quoted_forms,
    )
    from scripts.verify_lxx_evidence import FINGERPRINTS_PATH, check_fingerprints
    from scripts.verify_parse_claims import audit as parse_claim_findings
    from scripts.verify_parse_claims import coverage as parse_claim_coverage

    lines: list[str] = []
    evidence = Evidence.load(EVIDENCE_PATH)
    greek_findings = check_metadata_claims(evidence) + check_quoted_forms(evidence)
    provenance = dict(evidence.provenance)
    lines.append(
        f"- 1 Пет: **{provenance.get('excerpt_rows', len(evidence.rows))}** строк MorphGNT/SBLGNT "
        f"(из {provenance.get('upstream_rows', '?')}), офлайн-проверка нашла **{len(greek_findings)}**."
    )
    lines.append(f"  - источник: {provenance.get('source_url', '?')}")
    for finding in greek_findings[:4]:
        lines.append(f"  - `{finding.check_id}` `{finding.pool}` / `{finding.item_id}`: {finding.message}")
    if len(greek_findings) > 4:
        lines.append(f"  - … ещё {len(greek_findings) - 4}")
    if FINGERPRINTS_PATH.is_file():
        record = json.loads(FINGERPRINTS_PATH.read_text(encoding="utf-8")).get("provenance", {})
        lxx_findings = check_fingerprints(evidence)
        informational = ", ".join(f"{key} × {value}" for key, value in sorted(record.get("findings", {}).items()))
        lines.append(
            f"- LXX/ВЗ: **{record.get('cards', 0)}** карточек с ветхозаветными ссылками, "
            f"**{record.get('verses_checked', 0)}** стихов Рафлса 1935; офлайн-проверка нашла "
            f"**{len(lxx_findings)}**."
        )
        lines.append(
            "  - прошлый прогон по корпусу: информационные находки "
            f"{informational or 'нет'}; блокирующих нет."
        )
        lines.append(
            "  - корпус LXX не вендорится (CC BY-NC-SA/CCAT): хранится запись ссылок и хеш "
            "цитат, `data/ot-citation-fingerprints.json`."
        )
    parse_rows = parse_claim_findings()
    parse_stats = parse_claim_coverage()
    parse_blocking = [finding for finding in parse_rows if finding.severity == "blocking"]
    parse_manual = [finding for finding in parse_rows if finding.severity == "info"]
    lines.append(
        f"- Разбор форм: **{parse_stats['cards_with_parse_claim']}** карточек, у которых ключевой "
        f"вариант утверждает разбор, сверены с **{parse_stats['corpus_rows']}** строками MorphGNT; "
        f"расхождений — **{len(parse_blocking)}**."
    )
    lines.append(
        "  - вне машинной сверки: **"
        f"{len(parse_manual)}** карточек (несколько форм в одном вопросе или форма не названа); "
        "`scripts/verify_parse_claims.py`, тесты `tests/test_parse_claims.py`."
    )
    return lines


def level_ladder_note(report: AuditReport) -> str:
    """One line about what the pool names assert versus what the tiers derive.

    The bank's difficulty ladder lives in the chapter-1 pool names (``easy_p1``,
    ``medium_p2``, ``hard_p1``); the derived tier is a proxy built from
    ``claim_type``/``confidence``. Printing the two side by side is the honest
    state of the "all levels" requirement: the ladder is authored by pool, and no
    card carries an individually reviewed level yet.
    """
    rows: list[str] = []
    for finding in report.findings:
        if finding.check_id != "levels.derived_tiers_only" or " asserts " not in finding.message:
            continue
        match = re.search(r"this pool's name asserts (\w+) and the derived tier agrees for (\d+)/(\d+)", finding.message)
        if match:
            rows.append(f"`{finding.pool}` {match.group(2)}/{match.group(3)} ({match.group(1)})")
    if not rows:
        return "Лестница уровней не задана ни одним пулом: уровень выводится из метаданных."
    summary = ladder_summary(_pool_items())
    authored = summary["authored"]
    return (
        f"Лестница глав 1 задана именами пулов: {', '.join(rows)}. Уровень пула несут "
        f"{summary['authored_cards']} карточек (base {authored['base']}, core {authored['core']}, "
        f"advanced {authored['advanced']}); остальные {summary['derived_cards']} получают производный "
        "уровень от `claim_type`/`confidence`, который в `medium_*`/`hard_*` совпадает с именем пула редко: "
        "там в основном `claim_type=text`, а схема относит его к `base`. Проверенного уровня у отдельной "
        "карточки вне главы 1 в банке нет."
    )


# 1 Peter is 105 verses: 25 + 25 + 22 + 19 + 14. A question base for a course on
# the letter can only teach what it asks about, so the report states which verses
# no card touches. The number is measured from the cards' ``verse`` anchors, ranges
# included; it is context, not a gate - a verse can be covered by a card that names
# it in the explanation only, and the anchors are the honest machine-readable half.
LETTER_CHAPTER_VERSES: tuple[int, ...] = (25, 25, 22, 19, 14)
VERSE_ANCHOR = re.compile(
    r"(?<!\d)(?P<chapter>\d+)\s*:\s*(?P<verse>\d+)"
    r"(?:\s*[-–—]\s*(?:(?P<end_chapter>\d+)\s*:\s*)?(?P<end>\d+))?"
)
# Anchors name the other text a card quotes (a Psalm, Isaiah, Proverbs), and those
# references must not be counted as coverage of 1 Peter. Segments are the parts of
# an anchor between the separators the bank uses.
ANCHOR_SEPARATOR = re.compile(r"[;/]|,\s*")
FIRST_PETER_MARKER = re.compile(r"1\s*Пет")
OTHER_BOOK_MARKER = re.compile(
    r"(?<![А-Яа-я])(?:"
    r"Пс|Ис|Притч|Быт|Исх|Лев|Чис|Втор|Нав|Суд|Руф|1 Цар|2 Цар|3 Цар|4 Цар|1 Пар|2 Пар|"
    r"Еккл|Песн|Иер|Плач|Иез|Дан|Ос|Иоил|Ам|Авд|Иона|Мих|Наум|Авв|Соф|Агг|Зах|Мал|"
    r"Неем|Езд|Сир|Прем|Тов|Иудф|1 Мак|2 Мак|Мф|Мк|Лк|Ин|Деян|Рим|1 Кор|2 Кор|Гал|Еф|"
    r"Флп|Кол|1 Фес|2 Фес|1 Тим|2 Тим|Тит|Флм|Евр|Иак|1 Ин|2 Ин|3 Ин|Иуд|Откр"
    r")\s*\."
)


def letter_coverage(pools: Mapping[str, list[dict]]) -> dict[str, object]:
    """Which verses of 1 Peter the bank's anchors cover, chapter by chapter."""
    covered: dict[int, set[int]] = {chapter: set() for chapter in range(1, len(LETTER_CHAPTER_VERSES) + 1)}
    out_of_range: list[str] = []
    for cards in pools.values():
        for card in cards:
            anchor = str(card.get("verse") or "")
            for segment in ANCHOR_SEPARATOR.split(anchor):
                if OTHER_BOOK_MARKER.search(segment) and not FIRST_PETER_MARKER.search(segment):
                    continue
                for match in VERSE_ANCHOR.finditer(segment):
                    chapter_number = int(match.group("chapter"))
                    first = int(match.group("verse"))
                    last_chapter = int(match.group("end_chapter") or chapter_number)
                    last = int(match.group("end") or first)
                    if chapter_number not in covered or first < 1:
                        continue
                    limit = LETTER_CHAPTER_VERSES[chapter_number - 1]
                    if last_chapter != chapter_number:
                        # A range across chapters, e.g. 1:23-2:2: count to the end of
                        # the first chapter, then from the start of the next one.
                        covered[chapter_number].update(range(first, limit + 1))
                        if last_chapter in covered and last <= LETTER_CHAPTER_VERSES[last_chapter - 1]:
                            covered[last_chapter].update(range(1, last + 1))
                        continue
                    if last < first or last > limit:
                        # A reference past the end of the chapter is a data error in the
                        # anchor, not coverage: record it and count only what exists.
                        out_of_range.append(f"{card.get('id')}: {match.group(0).strip()}")
                        last = limit
                        if first > limit:
                            continue
                    covered[chapter_number].update(range(first, last + 1))
    per_chapter = {chapter: len(verses) for chapter, verses in sorted(covered.items())}
    missing = {
        chapter: sorted(set(range(1, LETTER_CHAPTER_VERSES[chapter - 1] + 1)) - verses)
        for chapter, verses in sorted(covered.items())
        if len(verses) < LETTER_CHAPTER_VERSES[chapter - 1]
    }
    return {
        "per_chapter": per_chapter,
        "covered": sum(per_chapter.values()),
        "total": sum(LETTER_CHAPTER_VERSES),
        "missing": missing,
        "out_of_range": sorted(out_of_range),
    }


def render_markdown(report: AuditReport, *, budget: dict[str, Any] | None = None) -> str:
    severity_counts = Counter(finding.severity for finding in report.findings)
    lines: list[str] = []
    lines.append("# Аудит базы вопросов (1 Петра)")
    lines.append("")
    lines.append(
        "Отчёт сгенерирован `scripts/audit_question_quality.py`. "
        "Документ измеряет то, что не покрывают структурные тесты: глубину, верификацию, "
        "богословскую осторожность, читаемость и устойчивость к угадыванию."
    )
    lines.append("")
    lines.append("## Сводка")
    lines.append("")
    lines.append(f"- карточек в производственных пулах: **{sum(report.pools.values())}**")
    for severity in SEVERITY_ORDER:
        lines.append(f"- `{severity}`: **{severity_counts.get(severity, 0)}**")
    outside_counts = Counter(
        finding.severity for finding in report.findings if finding.pool != "chapter5"
    )
    lines.append(
        "- вне пятой главы (её банки заперты блоб-пинами и ждут выпускного repin): "
        + ", ".join(
            f"`{severity}` {outside_counts.get(severity, 0)}" for severity in SEVERITY_ORDER
        )
    )
    coverage = letter_coverage(_pool_items())
    lines.append(
        "- покрытие послания: **"
        f"{coverage['covered']}/{coverage['total']}** стихов ("
        + ", ".join(f"{chapter}: {count}/{LETTER_CHAPTER_VERSES[chapter - 1]}" for chapter, count in coverage["per_chapter"].items())
        + ")"
    )
    if coverage["missing"]:
        gaps = ", ".join(
            f"{chapter}:{verse}" for chapter, verses in coverage["missing"].items() for verse in verses
        )
        lines.append(f"- без вопросов: {gaps}")
    lines.append("")
    if budget is not None:
        over = compare_budget(report, budget)
        if over:
            lines.append("### Выход за бюджет")
            lines.append("")
            for check_id, pool, current, allowed in over:
                lines.append(f"- `{check_id}` / `{pool}`: {current} > {allowed}")
            lines.append("")
        else:
            lines.append("Бюджет качества соблюдён: ни один счётчик не вырос.")
            lines.append("")

    lines.append("## Метрики по пулам")
    lines.append("")
    lines.append(
        "| пул | карточек | ср. длина варианта | ср. длина объяснения | верный = самый длинный "
        "| сильная утечка | уклон позиции | якоря стихов | база/ядро/продвинутый |"
    )
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for metrics in report.metrics:
        lines.append(
            f"| `{metrics.pool}` | {metrics.cards} | {metrics.mean_option_chars:.0f} "
            f"| {metrics.mean_explanation_chars:.0f} | {metrics.correct_longest_share:.0%} "
            f"| {metrics.length_leak_share:.0%} | {metrics.index_skew:.0%} | {metrics.verses} "
            f"| {metrics.tiers.get('base', 0)}/{metrics.tiers.get('core', 0)}/"
            f"{metrics.tiers.get('advanced', 0)} |"
        )
    lines.append("")

    lines.append("## Проверки")
    lines.append("")
    checks = sorted({finding.check_id for finding in report.findings})
    for check_id in checks:
        bucket = [finding for finding in report.findings if finding.check_id == check_id]
        severity = bucket[0].severity
        pools = Counter(finding.pool for finding in bucket)
        lines.append(f"### `{check_id}` — {severity}, {len(bucket)}")
        lines.append("")
        lines.append("Пулы: " + ", ".join(f"`{pool}` ({count})" for pool, count in pools.most_common()))
        lines.append("")
        for finding in bucket[:5]:
            lines.append(f"- `{finding.pool}` / `{finding.item_id}`: {finding.message}")
        if len(bucket) > 5:
            lines.append(f"- … ещё {len(bucket) - 5}")
        lines.append("")

    blockers = report.by_severity(BLOCKER)
    lines.append("## Блокеры (`blocker`)")
    lines.append("")
    accepted = accepted_blocker_ids(budget)
    if not blockers:
        lines.append("Блокеров нет.")
    else:
        for finding in blockers[:40]:
            mark = " (принятый долг)" if finding.item_id in accepted else ""
            lines.append(
                f"- `{finding.check_id}` `{finding.pool}` / `{finding.item_id}`{mark}: {finding.message}"
            )
        if len(blockers) > 40:
            lines.append(f"- … ещё {len(blockers) - 40}")
    lines.append("")
    if accepted:
        lines.append("### Принятый долг (требует выпускного repin)")
        lines.append("")
        lines.append(
            "Эти блокеры перечислены в `data/question-quality-budget.json` → `accepted_blockers` "
            "с причиной. Список закреплён тестом `tests/test_question_bank_audit.py`, поэтому он "
            "не может вырасти без отдельного ревью."
        )
        lines.append("")
    lines.append("## Верификация цитат")
    lines.append("")
    lines.extend(citation_verification())
    lines.append("")
    lines.append("## Как читать отчёт")
    lines.append("")
    lines.append(
        "- `wiseness.*` — можно ли угадать ответ без знания текста (длина, форма, позиция вариантов)."
    )
    lines.append("- `depth.*` — учит ли карточка чему-то за пределами одного пересказа стиха.")
    lines.append(
        "- `language.*` — поймёт ли обычный читатель формулировку; внутренний язык конвейера "
        "исследований (`inspected`, `HOLD`, `Wave3n`, `production-status`) — блокер."
    )
    lines.append("- `disputed.*` — есть ли обязательное покрытие спорных мест и не выдаётся ли спор за факт.")
    lines.append("- `content.trivia` — факт о древнем мире, который не нужен для чтения послания.")
    lines.append(
        "- `levels.*` — распределение по трудности. "
        + level_ladder_note(report)
        + " Развести курсы по уровням можно будет после того, как этот уровень появится у карточки."
    )
    lines.append("")
    lines.append("## Что делать по приоритету")
    lines.append("")
    blocking_outside = [
        finding
        for finding in report.findings
        if finding.pool != "chapter5" and finding.severity in (BLOCKER, MAJOR, MINOR)
    ]
    chapter5_total = sum(1 for finding in report.findings if finding.pool == "chapter5")
    if blocking_outside:
        lines.append(
            "1. Вне пятой главы ещё остаются находки уровня `blocker`/`major`/`minor` — их закрывают "
            f"первыми ({len(blocking_outside)} шт.):"
        )
        for check_id, count in Counter(
            finding.check_id for finding in blocking_outside
        ).most_common():
            pools = sorted(
                {
                    finding.pool
                    for finding in blocking_outside
                    if finding.check_id == check_id
                }
            )
            lines.append(f"   - `{check_id}`: {count} — " + ", ".join(f"`{pool}`" for pool in pools))
    else:
        lines.append(
            "1. Вне пятой главы находок уровня `blocker`/`major`/`minor` нет: единственный оставшийся "
            f"долг — пятая глава ({len(blockers)} блокеров в `accepted_blockers`, всего {chapter5_total} "
            "находок). Её банки заперты блоб-пинами, поэтому содержимое меняет только выпускной repin; "
            "правка на месте обошла бы выпускное ревью (`docs/CHAPTER5_RELEASE_AUDIT.md`)."
        )
    level_pools = [
        finding for finding in report.findings if finding.check_id == "levels.derived_tiers_only"
    ]
    lines.append(
        "2. Добавить проверенное поле `level` (база/ядро/продвинутый) и развести курсы по уровням: "
        f"`levels.derived_tiers_only` сейчас отмечает {len(level_pools)} пулов, где трудность "
        "выводится из `claim_type`/`confidence`, а не из отдельного проверенного поля."
    )
    info_checks = Counter(
        finding.check_id for finding in report.by_severity(INFO)
    )
    jargon_pools = Counter(
        finding.pool for finding in report.by_check("language.latin_jargon")
    )
    lines.append(
        "3. INFO-находки вне пятой главы — контекст, а не дефекты, кроме одной семьи ("
        + ", ".join(f"`{check}` {count}" for check, count in sorted(info_checks.items()))
        + "). `language.latin_jargon` — это латиница в тексте для учащихся: "
        + ", ".join(f"`{pool}` {count}" for pool, count in sorted(jargon_pools.items()))
        + ". Третью главу локализует параллельная полоса (её строки вне этой ветки), "
        "пятая ждёт выпускного repin; в остальных пулах счётчик нулевой. Базовые "
        "recall-карточки для простых пользователей, метки/ссылки в вариантах, для которых "
        "выравнивание длины меняло бы сам проверяемый факт, и производные уровни — "
        "осознанный INFO-контекст."
    )
    lines.append(
        "4. Каждая новая карточка проходит `--check`: ratchet в `data/question-quality-budget.json` "
        "не даёт счётчикам вырасти, а `tests/test_question_depth_regressions.py` держит глубину "
        "объяснений, нейтральность длины вариантов и источник-quorum вне пятой главы."
    )
    lines.append("")
    return "\n".join(lines)


def accepted_blocker_ids(budget: dict[str, Any] | None) -> set[str]:
    """Blocker item ids recorded as accepted release-repin debt.

    Only ids explicitly listed with a reason in the budget file are accepted; the
    list is pinned by ``tests/test_question_bank_audit.py`` so it can only be
    changed by a reviewed test edit, never silently.
    """
    if not budget:
        return set()
    entries = budget.get("accepted_blockers", {})
    if not isinstance(entries, dict):
        return set()
    return {str(item_id) for item_id, reason in entries.items() if str(reason).strip()}


def compare_budget(report: AuditReport, budget: dict[str, Any]) -> list[tuple[str, str, int, int]]:
    """Return (check_id, pool, current, allowed) rows that exceed the budget."""
    allowed_counts: dict[str, dict[str, int]] = budget.get("counts", {})
    over: list[tuple[str, str, int, int]] = []
    for check_id, pools in _budget_counts(report).items():
        allowed_pools = allowed_counts.get(check_id, {})
        for pool, current in pools.items():
            allowed = allowed_pools.get(pool)
            if allowed is None:
                allowed = allowed_pools.get("*", 0)
            if current > allowed:
                over.append((check_id, pool, current, allowed))
    accepted = accepted_blocker_ids(budget)
    for finding in report.by_severity(BLOCKER):
        if finding.item_id in accepted:
            continue
        over.append((finding.check_id, finding.pool, 1, 0))
    return sorted(set(over))


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", type=Path, help="write the machine-readable report here")
    parser.add_argument("--report", type=Path, default=None, help="write the Markdown report here")
    parser.add_argument("--check", action="store_true", help="fail when the recorded budget is exceeded")
    parser.add_argument("--write-budget", action="store_true", help="rewrite the ratchet budget file")
    parser.add_argument("--list", type=int, default=0, help="print the first N findings per check")
    args = parser.parse_args(list(argv) if argv is not None else None)

    report = audit()
    budget = json.loads(BUDGET_PATH.read_text(encoding="utf-8")) if BUDGET_PATH.is_file() else None

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(
            json.dumps(
                {
                    "findings": [finding.as_dict() for finding in report.findings],
                    "metrics": [metrics.as_dict() for metrics in report.metrics],
                    "pools": report.pools,
                    "counts": report.counts(),
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(render_markdown(report, budget=budget), encoding="utf-8")

    if args.write_budget:
        BUDGET_PATH.parent.mkdir(parents=True, exist_ok=True)
        BUDGET_PATH.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "note": (
                        "Ratchet budget for scripts/audit_question_quality.py. Counts may only "
                        "shrink; refresh with --write-budget after a reviewed fix."
                    ),
                    "counts": _budget_counts(report),
                    "accepted_blockers": {
                        item_id: reason
                        for item_id, reason in (budget or {}).get("accepted_blockers", {}).items()
                        if item_id in accepted_blocker_ids(budget)
                    },
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    severity_counts = Counter(finding.severity for finding in report.findings)
    print(
        f"pools={len(report.pools)} cards={sum(report.pools.values())} "
        f"blocker={severity_counts.get(BLOCKER, 0)} major={severity_counts.get(MAJOR, 0)} "
        f"minor={severity_counts.get(MINOR, 0)} info={severity_counts.get(INFO, 0)}"
    )

    if args.list:
        for check_id in sorted({finding.check_id for finding in report.findings}):
            bucket = [finding for finding in report.findings if finding.check_id == check_id]
            print(f"\n== {check_id} ({len(bucket)}, {bucket[0].severity})")
            for finding in bucket[: args.list]:
                print(f"   {finding.pool:16s} {finding.item_id:16s} {finding.message}")

    if args.check:
        if not budget:
            print(f"budget file is missing: {BUDGET_PATH}", flush=True)
            return 2
        accepted = accepted_blocker_ids(budget)
        if accepted:
            print(f"accepted blocker debt: {len(accepted)} card(s) pending release repin", flush=True)
        over = compare_budget(report, budget)
        if over:
            print("question-quality budget exceeded:")
            for check_id, pool, current, allowed in over[:40]:
                print(f"  {check_id} / {pool}: {current} > {allowed}")
                offenders = [
                    finding
                    for finding in report.findings
                    if finding.check_id == check_id and finding.pool == pool
                ][:4]
                for finding in offenders:
                    print(f"    - {finding.item_id}: {finding.message}")
            if len(over) > 40:
                print(f"  … {len(over) - 40} more")
            return 1
        print("question-quality budget respected")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
