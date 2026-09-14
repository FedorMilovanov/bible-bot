"""Claim-class correction for the introduction/context courses.

The introduction courses ask about the letter's *origin*: authorship, dating,
canonical reception and the language situation of its author and readers. Those
claims rest on external witnesses — Polycarp, the Muratorian fragment, Pliny and
Trajan, Achtemeier, Hengel, the epigraphic evidence for Greek in Galilee — not on
a verse of 1 Peter.

They shipped typed as ``interpretation`` or ``greek``, which made the audit's
verse-anchor check misfire: a verse anchor is how a learner verifies a claim
about the text, while an origin claim is verified through the source list that
``metadata.source_quorum`` checks. Typing them as ``history`` also states the
evidence requirement correctly (two witnesses: the primary/reception source plus
a modern control).

Cards that really do rest on the letter keep their type and here name their
passage. Nothing else changes: no stem, option, key, explanation or source list.

The module is merged per field in
``questions/review_composition_2026_09.REVIEW_LAYERS``, so its two keys never
drop a reviewed field from an earlier layer.
"""

from __future__ import annotations

RECEPTION_HISTORY_IDS = (
    "intro1_03",
    "intro1_04",
    "intro1_05",
    "intro1_06",
    "intro1_08",
    "intro1_10",
    "intro1_11",
    "intro1_13",
    "intro1_14",
    "intro1_15",
    "intro2_01",
    "intro2_03",
    "intro2_04",
    "intro2_05",
    "intro2_06",
    "intro2_08",
    "intro2_09",
    "intro2_10",
    "intro2_11",
    "intro2_12",
    "intro2_15",
    "intro3_03",
    "intro3_16",
)

# Cards about the letter's own content keep ``interpretation`` and now name the
# passage a learner can check.
INTRO_VERSE_ANCHORS = {
    "intro3_09": "1 Пет. 4:17-19; 5:10",
    "intro3_11": "1 Пет. 1:1-5:14",
    "intro3_14": "1 Пет. 1:24-25",
    # Linguistics pool: the election/calling synthesis is anchored to the two
    # places where 1 Peter itself uses the vocabulary.
    "ling2_13": "1 Пет. 1:1-2; 2:9",
}

RECEPTION_REVIEW_OVERRIDES: dict[str, dict] = {
    question_id: {"claim_type": "history"} for question_id in RECEPTION_HISTORY_IDS
}
for _question_id, _verse in INTRO_VERSE_ANCHORS.items():
    RECEPTION_REVIEW_OVERRIDES.setdefault(_question_id, {})["verse"] = _verse

__all__ = [
    "INTRO_VERSE_ANCHORS",
    "RECEPTION_HISTORY_IDS",
    "RECEPTION_REVIEW_OVERRIDES",
]
