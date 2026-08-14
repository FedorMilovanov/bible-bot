# 1 Peter Chapter 3 — Challenge admission by reviewed taxonomy

**Status:** `6 EASY + 6 MEDIUM ADMITTED / HARD CHAPTER-1-ONLY / FALLBACK CHAPTER-1-ONLY`

This layer consumes the exact-green reviewed taxonomy checkpoint:

`62fb982da58a4a97739dd9b504e43cb5d34d7ec2`

The taxonomy parent established:

- 12 ranking-authorized direct-text cards total;
- 6 reviewed easy IDs;
- 6 reviewed medium IDs;
- 0 hard IDs by design;
- every authorized ID classified exactly once;
- a rationale for every classification;
- balanced answer-position distributions inside each non-empty bucket;
- CI #1232, Security #1112 and CodeQL #914 successful.

## Admission path

Chapter 3 enters Challenge only through the reviewed taxonomy:

```text
ranking authority 12
-> reviewed Challenge taxonomy
-> CHAPTER3_CHALLENGE_POOLS
-> matching CHALLENGE_POOLS bucket
```

The resolver raises at import time if:

- any taxonomy ID is missing from the exact authorized competitive pool;
- an ID appears in more than one difficulty;
- taxonomy union differs from the explicit ranking authority.

No raw `competitive=True` metadata is sufficient for Challenge.

## Current Chapter-3 Challenge taxonomy

```text
EASY = ch3_text_101, 102, 106, 108, 109, 111
MEDIUM = ch3_text_103, 104, 105, 107, 110, 112
HARD = none
```

The hard bucket remains Chapter-1-only. No direct-text card is promoted to hard to make the distribution look symmetrical.

## Existing Challenge quotas stay unchanged

`random20` still asks for:

```text
6 easy + 6 medium + 8 hard
```

`hardcore20` still asks for:

```text
4 easy + 4 medium + 12 hard
```

Chapter-3 easy/medium cards participate only inside those corresponding category pools.

## Fallback remains Chapter-1-only

Even after explicit taxonomy admission:

```text
CHALLENGE_FALLBACK_POOL = CHAPTER1_COMPETITIVE_POOL
```

This is deliberate. If a category ever lacks enough questions, a shortage fallback must not become a second unreviewed Chapter-3 admission path.

Therefore Chapter 3 can appear in Challenge only by a taxonomy bucket, never by fallback.

## Unauthorized Chapter-3 containment

The full normal-learning course still contains 165 reviewed cards.

Only the 12 ranking-authorized text cards can appear in Challenge. The other 153 remain absent from:

- general competitive;
- Battle;
- Challenge.

Normal Chapter-3 learning still remains non-scoring. Challenge/Battle scoring applies only when a user explicitly enters those ranked modes.

## Required regression behavior

Deterministic Challenge tests must verify across many seeds:

1. every selection contains exactly 20 unique questions;
2. selected Chapter-3 IDs are a subset of the reviewed taxonomy only;
3. no unauthorized Chapter-3 ID is ever selected;
4. no Chapter-3 ID can enter through the fallback pool;
5. Random20 still contains exactly 6 easy / 6 medium / 8 hard by pool membership;
6. Hardcore20 still contains exactly 4 easy / 4 medium / 12 hard;
7. at least one deterministic seed actually selects Chapter-3 cards, proving the admission path is live rather than dead code;
8. hard selections contain zero Chapter-3 IDs.

## Product boundaries preserved

```text
CHALLENGE ADMISSION != NORMAL LEARNING SCORING
CHALLENGE TAXONOMY != GENERAL SOURCE AUTHORITY
EASY/MEDIUM ADMISSION != HARD ADMISSION
CATEGORY ADMISSION != FALLBACK ADMISSION
```

No battle scoring formula, persistence logic, daily bonus policy, Mini App normal-learning result policy, or legacy `random_all` pool is modified here.
