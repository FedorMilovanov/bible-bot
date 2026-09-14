#!/usr/bin/env python3
"""Regenerate the immutable seal for the Chapter 4 localization release.

The Chapter 4 bank is a sealed reviewed release: user-facing text may only
change through a new content pass that issues fresh review-record IDs and
content digests.  After editing CARD_REVISIONS in
questions/chapter4/localization_pass.py, run:

    python3 scripts/build_chapter4_localization_seal.py

and paste the emitted block into REVIEW_RECORD_REVISIONS_3.  The tool starts
from the staged bank with the second adversarial pass already applied, so the
digests seal exactly the card content users will see after this third pass.
"""

from __future__ import annotations

import copy

from questions.chapter4.authoring import CHAPTER4_STAGING_QUESTIONS
from questions.chapter4.localization_pass import CARD_REVISIONS
from questions.chapter4.review_registry import product_card_content_digest
from questions.chapter4.second_pass_revisions import (
    apply_second_pass_card_revisions,
)


def main() -> None:
    cards = {card["id"]: copy.deepcopy(card) for card in CHAPTER4_STAGING_QUESTIONS}
    # Importing questions.chapter4 already applies the second pass in place on
    # the package-level list; deepcopy above captured that post-pass-2 state,
    # but reapply defensively so the tool is also correct for direct imports.
    apply_second_pass_card_revisions(list(cards.values()))

    for card_id, revision in CARD_REVISIONS.items():
        card = cards[card_id]
        if "question" in revision:
            card["question"] = revision["question"]
        if "options" in revision:
            card["options"] = list(revision["options"])
        if "explanation" in revision:
            card["explanation"] = revision["explanation"]
        digest = product_card_content_digest(card)
        record_id = f"ch4prv3_{card_id}_{digest[:12]}"
        print(f'    "{card_id}": ("{record_id}", "{digest}"),')


if __name__ == "__main__":
    main()
