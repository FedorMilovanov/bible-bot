"""Staging-only aggregate for the audited 1 Peter Chapter 3 lanes.

This package aggregate is intentionally NOT registered in questions.POOL_REGISTRY.
Production admission, ranking, and publication are separate gates.
"""

from .application_1_7 import APPLICATION_3_1_7
from .application_8_12 import APPLICATION_3_8_12
from .application_13_17 import APPLICATION_3_13_17
from .application_18_22 import APPLICATION_3_18_22
from .disputed_18_22 import DISPUTED_3_18_22
from .greek_1_7 import GREEK_3_1_7
from .greek_8_12 import GREEK_3_8_12
from .greek_13_17 import GREEK_3_13_17
from .greek_18_22 import GREEK_3_18_22
from .history_1_7 import HISTORY_3_1_7
from .intertext_1_7 import INTERTEXT_3_1_7
from .intertext_8_12 import INTERTEXT_3_8_12
from .intertext_13_17 import INTERTEXT_3_13_17
from .intertext_18_22 import INTERTEXT_3_18_22
from .sources import SOURCE_CATALOG as SOURCES_3_18_22
from .sources_1_7 import SOURCE_CATALOG as SOURCES_3_1_7
from .sources_8_12 import SOURCE_CATALOG as SOURCES_3_8_12
from .sources_13_17 import SOURCE_CATALOG as SOURCES_3_13_17
from .text_1_7 import TEXT_3_1_7
from .text_8_12 import TEXT_3_8_12
from .text_13_17 import TEXT_3_13_17
from .text_18_22 import TEXT_3_18_22
from .theology_1_7 import DISPUTED_3_1_7, THEOLOGY_3_1_7
from .theology_8_12 import DISPUTED_3_8_12, THEOLOGY_3_8_12
from .theology_13_17 import DISPUTED_3_13_17, THEOLOGY_3_13_17
from .theology_18_22 import THEOLOGY_3_18_22

# Explicit domain map is the machine-readable coverage surface used by the
# reviewed/admission layer. Domain ordering deliberately preserves the audited
# per-lane card order from the staging aggregate.
CHAPTER3_DOMAIN_POOLS = {
    "3:1-7": {
        "text": TEXT_3_1_7,
        "greek": GREEK_3_1_7,
        "intertext": INTERTEXT_3_1_7,
        "history": HISTORY_3_1_7,
        "theology": THEOLOGY_3_1_7,
        "disputed": DISPUTED_3_1_7,
        "application": APPLICATION_3_1_7,
    },
    "3:8-12": {
        "text": TEXT_3_8_12,
        "greek": GREEK_3_8_12,
        "intertext": INTERTEXT_3_8_12,
        "theology": THEOLOGY_3_8_12,
        "disputed": DISPUTED_3_8_12,
        "application": APPLICATION_3_8_12,
    },
    "3:13-17": {
        "text": TEXT_3_13_17,
        "greek": GREEK_3_13_17,
        "intertext": INTERTEXT_3_13_17,
        "theology": THEOLOGY_3_13_17,
        "disputed": DISPUTED_3_13_17,
        "application": APPLICATION_3_13_17,
    },
    "3:18-22": {
        "text": TEXT_3_18_22,
        "greek": GREEK_3_18_22,
        "disputed": DISPUTED_3_18_22,
        "intertext": INTERTEXT_3_18_22,
        "theology": THEOLOGY_3_18_22,
        "application": APPLICATION_3_18_22,
    },
}

CHAPTER3_LANE_POOLS = {
    lane: [
        question
        for domain_questions in domains.values()
        for question in domain_questions
    ]
    for lane, domains in CHAPTER3_DOMAIN_POOLS.items()
}

CHAPTER3_SOURCE_CATALOGS = {
    "3:1-7": SOURCES_3_1_7,
    "3:8-12": SOURCES_3_8_12,
    "3:13-17": SOURCES_3_13_17,
    "3:18-22": SOURCES_3_18_22,
}

CHAPTER3_STAGING_QUESTIONS = [
    question
    for lane_questions in CHAPTER3_LANE_POOLS.values()
    for question in lane_questions
]

# Backward-compatible internal name only. Root production registry does not import it.
CHAPTER3_DRAFT_QUESTIONS = list(CHAPTER3_STAGING_QUESTIONS)

__all__ = [
    "CHAPTER3_DOMAIN_POOLS",
    "CHAPTER3_DRAFT_QUESTIONS",
    "CHAPTER3_LANE_POOLS",
    "CHAPTER3_SOURCE_CATALOGS",
    "CHAPTER3_STAGING_QUESTIONS",
]
