"""Authoring bank for 1 Peter chapter 2.

The chapter is not production-complete until 2:1-25 passes the coverage roadmap.
"""

from .greek import GREEK_2_1_10
from .text import TEXT_2_1_10

CHAPTER2_DRAFT_QUESTIONS = TEXT_2_1_10 + GREEK_2_1_10

__all__ = [
    "CHAPTER2_DRAFT_QUESTIONS",
    "GREEK_2_1_10",
    "TEXT_2_1_10",
]
