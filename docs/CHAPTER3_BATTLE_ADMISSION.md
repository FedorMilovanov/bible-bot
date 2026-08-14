# 1 Peter Chapter 3 — Battle admission with Challenge isolation

**Status:** `12 AUTHORIZED IDS IN GENERAL COMPETITIVE + BATTLE / CHALLENGE CLOSED`

This product layer is stacked on the exact-green explicit ranking authority checkpoint:

`05fbcc7f2052cdd106d406dc364bc466dac843fa`

Parent authority proves:

- exactly 12 pinned IDs;
- pinned IDs exactly equal the independent fail-closed audit READY set;
- zero audit HOLDs;
- every ID is direct text / high confidence / neutral / `competitive=True`;
- exact evidence minimum `sblgnt` + inspected NET 1 Peter 3:1–7 passage notes;
- CI #1229, Security #1109, CodeQL #911 all successful.

## Exact gameplay admission

Only these IDs enter the enlarged general competitive surface and legacy Battle pool:

```text
ch3_text_101 ... ch3_text_112
```

The other 153 reviewed Chapter-3 cards remain outside competitive and Battle pools.

Root composition is now explicit:

```text
COMPETITIVE_POOL = CHAPTER1_COMPETITIVE_POOL + CHAPTER3_AUTHORIZED_COMPETITIVE_POOL
BATTLE_POOL = COMPETITIVE_POOL
```

`CHAPTER3_AUTHORIZED_COMPETITIVE_POOL` resolves the explicit authority set against the reviewed Chapter-3 bank and raises at import time if an authorized ID is missing or fails the existing structural ranking policy.

## Normal learning remains separate

The full 165-card Chapter-3 reviewed course stays available through the normal `chapter3` learning pool and remains in `NON_SCORING_LEARNING_POOLS`.

Normal Chapter-3 quizzes therefore still have:

```text
points = 0
daily_bonus = 0
new_achievements = []
```

Battle outcome points are a separate PvP mechanic and apply only when the user actually enters Battle.

## Challenge is deliberately not admitted

Chapter 3 has no reviewed easy/medium/hard Challenge taxonomy yet. Therefore the twelve Battle-authorized cards are **not** added to any `CHALLENGE_POOLS` entry.

More importantly, the previous Challenge fallback used the general `COMPETITIVE_POOL`. Once Chapter 3 joins that pool, such a fallback could become a hidden admission path whenever a category pool is short.

This layer removes that ambiguity by pinning:

```text
CHALLENGE_FALLBACK_POOL = CHAPTER1_COMPETITIVE_POOL
```

Challenge selection now falls back only to the same Chapter-1 competitive authority that existed before Chapter-3 Battle admission.

Regression tests run 128 deterministic seeds for each of `random20` and `hardcore20`, requiring exactly 20 unique questions and zero Chapter-3 IDs every time.

```text
BATTLE AUTHORITY != CHALLENGE TAXONOMY
GENERAL COMPETITIVE EXPANSION != CHALLENGE FALLBACK EXPANSION
```

## Legacy controller compatibility

The legacy Telegram controller imports `BATTLE_POOL` from `questions`; no monolithic `bot.py` rewrite is required to change the source-reviewed Battle question surface. Battle result atomicity/idempotency remains owned by the existing `battle_integrity.py` layer.

This PR changes only question-pool composition and its control plane. It does not change battle scoring formulas, persistence, claims, result delivery, or user authorization.

## Required exact-head gate

Before this checkpoint is considered green:

- full pytest, including the evolved staging/review/audit/authority lifecycle tests;
- deterministic Challenge non-leakage tests;
- production container/import/smoke;
- Security Audit;
- CodeQL

must all pass on the same exact head.
