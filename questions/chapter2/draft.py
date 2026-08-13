"""Single authoring aggregate for the reviewed portions of 1 Peter chapter 2."""

from .greek import GREEK_2_1_10
from .greek_11_17 import GREEK_2_11_17
from .greek_18_25 import GREEK_2_18_25
from .ot_exodus19 import OT_EXODUS19_2_9
from .ot_isaiah53 import OT_ISAIAH53_2_21_25
from .ot_psalm34 import OT_PSALM34_2_3
from .ot_stone import OT_STONE_2_6_8
from .text import TEXT_2_1_10
from .text_11_12 import TEXT_2_11_12
from .text_13 import TEXT_2_13
from .text_16_17 import TEXT_2_16_17
from .text_18_20 import TEXT_2_18_20
from .text_21_25 import TEXT_2_21_25
from .theology_21_25 import THEOLOGY_2_21_25

TEXT_2_DRAFT = TEXT_2_1_10 + TEXT_2_11_12 + TEXT_2_13 + TEXT_2_16_17 + TEXT_2_18_20 + TEXT_2_21_25
GREEK_2_DRAFT = GREEK_2_1_10 + GREEK_2_11_17 + GREEK_2_18_25
INTERTEXT_2_DRAFT = OT_PSALM34_2_3 + OT_STONE_2_6_8 + OT_EXODUS19_2_9 + OT_ISAIAH53_2_21_25
THEOLOGY_2_DRAFT = THEOLOGY_2_21_25

CHAPTER2_DRAFT_QUESTIONS = TEXT_2_DRAFT + GREEK_2_DRAFT + INTERTEXT_2_DRAFT + THEOLOGY_2_DRAFT

__all__ = [
    "CHAPTER2_DRAFT_QUESTIONS",
    "GREEK_2_DRAFT",
    "INTERTEXT_2_DRAFT",
    "TEXT_2_DRAFT",
    "THEOLOGY_2_DRAFT",
]
