"""Authoring package for 1 Peter chapter 2.

The chapter remains outside production until the coverage roadmap is complete.
"""

from .draft import (
    CHAPTER2_DRAFT_QUESTIONS,
    GREEK_2_DRAFT,
    INTERTEXT_2_DRAFT,
    TEXT_2_DRAFT,
    THEOLOGY_2_DRAFT,
)
from .greek import GREEK_2_1_10
from .ot_exodus19 import OT_EXODUS19_2_9
from .ot_psalm34 import OT_PSALM34_2_3
from .ot_stone import OT_STONE_2_6_8
from .text import TEXT_2_1_10

INTERTEXT_2_1_10 = OT_PSALM34_2_3 + OT_STONE_2_6_8 + OT_EXODUS19_2_9

__all__ = [
    "CHAPTER2_DRAFT_QUESTIONS",
    "GREEK_2_1_10",
    "GREEK_2_DRAFT",
    "INTERTEXT_2_1_10",
    "INTERTEXT_2_DRAFT",
    "TEXT_2_1_10",
    "TEXT_2_DRAFT",
    "THEOLOGY_2_DRAFT",
]
