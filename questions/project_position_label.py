"""Visible course-position labelling for the canonical chapter-1 pools.

``docs/CONTENT_SOURCE_POLICY.md`` and ``docs/TMS_QUALITY_ADDENDUM.md`` require a
card that carries the course's own position (``position == "project"``) to make
that position visible to the learner: either with the canonical prefix
``[Позиция курса]`` or by naming the course position in the wording itself.

Chapters 2-5 enforce this in their reviewed boundaries (Chapter 3 and 4 prepend
the prefix while building the aggregate). The chapter-1 canonical pools and the
``tms_deep`` pool had no such boundary, so five reviewed project cards shipped
without any visible mark (``easy_12``, ``med_01``, ``tms1_app_01``-``03``).

This pass is wording-only and one-directional: it prepends the reviewed prefix
to a project card whose stem does not carry it and does not already name the
course position (``курс``). Options, the keyed index, the explanation, verses,
sources and every other metadata value are untouched.
"""

from __future__ import annotations

VISIBLE_PREFIX = "[Позиция курса]"


def apply_project_position_label(question: dict) -> dict:
    """Make a project-position card visibly labelled, if it is not already."""
    if question.get("position") != "project":
        return question

    stem = str(question.get("question", ""))
    if stem.startswith(VISIBLE_PREFIX) or "курс" in stem.casefold():
        return question

    question["question"] = f"{VISIBLE_PREFIX} {stem}"
    return question
