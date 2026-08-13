"""Authoring bank for 1 Peter chapter 2.

The chapter is not production-complete until 2:1-25 passes the coverage roadmap.
"""

from .greek import GREEK_2_1_10
from .ot_exodus19 import OT_EXODUS19_2_9
from .ot_psalm34 import OT_PSALM34_2_3
from .ot_stone import OT_STONE_2_6_8
from .text import TEXT_2_1_10

INTERTEXT_2_1_10 = OT_PSALM34_2_3 + OT_STONE_2_6_8 + OT_EXODUS19_2_9

CHAPTER2_DRAFT_QUESTIONS = TEXT_2_1_10 + GREEK_2_1_10 + INTERTEXT_2_1_10

__all__ = [
    "CHAPTER2_DRAFT_QUESTIONS",
    "GREEK_2_1_10",
    "INTERTEXT_2_1_10",
    "OT_EXODUS19_2_9",
    "OT_PSALM34_2_3",
    "OT_STONE_2_6_8",
    "TEXT_2_1_10",
]
