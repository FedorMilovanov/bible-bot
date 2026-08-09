"""Stable import path for legacy bot.py; canonical data lives in questions.intro."""

from questions.intro import intro_part1_questions, intro_part2_questions, intro_part3_questions

__all__ = ["intro_part1_questions", "intro_part2_questions", "intro_part3_questions"]
