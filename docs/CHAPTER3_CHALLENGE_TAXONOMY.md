# 1 Peter Chapter 3 — reviewed Challenge taxonomy

**Status:** `TAXONOMY REVIEWED / CHALLENGE NOT YET ADMITTED`

This layer is stacked on the exact-green Battle checkpoint:

`6f7fbc5de2e7fbfc7d54b6c844234636dbc03a49`

That parent already admits exactly twelve source-reviewed direct-text Chapter-3 cards to general competitive/Battle while keeping Challenge fully Chapter-1-only.

## Do not force a symmetric taxonomy

The current authority set is not a mixed bank of text, Greek, history and advanced exegesis. It is deliberately narrow: twelve direct-text cards from 1 Peter 3:1–7.

Forcing `4 easy / 4 medium / 4 hard` merely for visual symmetry would misdescribe the content and create a product fiction. No card in the current authority set requires the multi-source or disputed reasoning expected of the existing hard Challenge tier.

Therefore:

```text
EASY = 6
MEDIUM = 6
HARD = 0
```

`hard = 0` is a reviewed conclusion, not an unfinished placeholder.

## Easy bucket

- `ch3_text_101` — direct scope: own husbands;
- `ch3_text_102` — direct quantifier boundary: some husbands;
- `ch3_text_106` — direct divine valuation in 3:4;
- `ch3_text_108` — Sarah's two directly stated actions;
- `ch3_text_109` — the paired closing actions in 3:6;
- `ch3_text_111` — direct co-heir statement in 3:7.

These are local identification/scope tasks whose answer is stated in a single immediate clause or pair.

## Medium bucket

- `ch3_text_103` — connect the winning purpose with observed conduct across 3:1–2;
- `ch3_text_104` — retain a three-item external-adornment enumeration against plausible nearby distractors;
- `ch3_text_105` — track the explicit 3:3→3:4 outer/inner contrast;
- `ch3_text_107` — distinguish the class description of holy women from Sarah-specific and husband-directed neighboring statements;
- `ch3_text_110` — distinguish the paired husband obligations from adjacent wife-directed clauses;
- `ch3_text_112` — recognize the final purpose/result relation concerning hindered prayers.

These remain direct-text questions, but require local relation, contrast, list retention or neighboring-clause discrimination rather than one isolated phrase.

## Answer-position audit

Difficulty classification must not create a new answer-position tell.

The six-card easy bucket uses all four answer positions with counts differing by at most one. The six-card medium bucket does the same.

The taxonomy test therefore requires for each non-empty bucket:

```text
positions used = {0,1,2,3}
max(position_count) - min(position_count) <= 1
```

No option reordering or runtime shuffle is introduced just to satisfy the taxonomy. The already-audited card authoring remains intact.

## No Challenge mutation yet

This PR does not touch `CHALLENGE_POOLS` or `CHALLENGE_FALLBACK_POOL`.

The taxonomy is an explicit editorial authority that a later Challenge-admission PR may consume. Until then, deterministic Challenge regression tests continue to require zero Chapter-3 cards.

```text
TAXONOMY REVIEWED != CHALLENGE ADMITTED
HARD EMPTY != TAXONOMY INCOMPLETE
```

A later admission may add only the easy and medium authority IDs to their matching Challenge pools. Hard Challenge must remain Chapter-1-only until a separately reviewed hard Chapter-3 authority exists.
