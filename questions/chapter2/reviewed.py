"""Reviewed authoring aggregate for 1 Peter chapter 2.

This is the promotion boundary between scattered review modules and a single
chapter-level bank.
"""

from copy import deepcopy

from .application_freedom_2_15_16 import APPLICATION_FREEDOM_2_15_16
from .application_growth import APPLICATION_GROWTH_2_1_3
from .application_identity import APPLICATION_IDENTITY_2_4_10
from .application_suffering import APPLICATION_SUFFERING_2_18_25
from .application_witness import APPLICATION_WITNESS_2_11_12
from .disputed_2_8 import DISPUTED_2_8
from .disputed_2_12 import DISPUTED_2_12
from .draft import CHAPTER2_DRAFT_QUESTIONS
from .history_bodily_suffering import HISTORY_BODILY_2_18_25
from .history_exiles_2_11 import HISTORY_EXILES_2_11
from .history_oiketai import HISTORY_OIKETAI_2_18
from .history_roman_2_13_14 import HISTORY_ROMAN_2_13_14
from .quality_overrides import apply_quality_overrides
from .theology_civil import THEOLOGY_CIVIL_2_13_17
from .theology_people_text import THEOLOGY_PEOPLE_TEXT


CHAPTER2_REVIEW_QUARANTINE_IDS = frozenset(
    {
        "ch2_hist_001",
        "ch2_hist_003",
        "ch2_hist_004",
        "ch2_theol_010",
    }
)

_SUPPLEMENTAL_REVIEWED = (
    APPLICATION_GROWTH_2_1_3
    + APPLICATION_IDENTITY_2_4_10
    + APPLICATION_WITNESS_2_11_12
    + APPLICATION_FREEDOM_2_15_16
    + APPLICATION_SUFFERING_2_18_25
    + HISTORY_OIKETAI_2_18
    + HISTORY_BODILY_2_18_25
    + HISTORY_EXILES_2_11
    + HISTORY_ROMAN_2_13_14
    + THEOLOGY_CIVIL_2_13_17
    + THEOLOGY_PEOPLE_TEXT
    + DISPUTED_2_8
    + DISPUTED_2_12
)

_REVIEWED_SOURCE_ITEMS = [
    item
    for item in CHAPTER2_DRAFT_QUESTIONS + _SUPPLEMENTAL_REVIEWED
    if item["id"] not in CHAPTER2_REVIEW_QUARANTINE_IDS
]

CHAPTER2_REVIEWED_QUESTIONS = [
    apply_quality_overrides(deepcopy(item))
    for item in _REVIEWED_SOURCE_ITEMS
]


__all__ = [
    "CHAPTER2_REVIEWED_QUESTIONS",
    "CHAPTER2_REVIEW_QUARANTINE_IDS",
]
