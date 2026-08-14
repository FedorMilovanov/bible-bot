# 1 Peter Chapter 3 — reviewed admission boundary

**Status:** `REVIEWED BANK COMPLETE / NOT PRODUCT-WIRED / NOT RANKING-AUTHORIZED`

This layer is stacked on the exact-green Chapter-3 integration checkpoint:

`571f1f3d0fcab83c0b087735adcaf61316090525`

The integration checkpoint already proved:

- 165 cards across 3:1–7, 3:8–12, 3:13–17, and 3:18–22;
- zero exact ID collisions;
- canonical metadata integrity;
- lane-local source-depth integrity;
- no Chapter-3 IDs in root production or ranking surfaces;
- CI #1217, Security Audit #1097, and CodeQL #899 all successful.

## Why a second boundary exists

Staging integration proves that the four independently authored lanes coexist safely. It is not, by itself, the product promotion surface.

`questions/chapter3/reviewed.py` creates the same kind of explicit chapter-level promotion boundary already used by Chapter 2:

```text
AUDITED LANE OBJECTS
-> STAGING AGGREGATE
-> REVIEWED DEEPCOPY / EDITORIAL POLICY
-> FUTURE NORMAL-LEARNING PRODUCT WIRING
```

The root product registry still does not import Chapter 3 in this PR.

## Review result

The reviewed bank contains all 165 staging IDs and has an explicit empty quarantine:

```text
STAGING = 165
QUARANTINE = 0
REVIEWED = 165
```

An empty quarantine is not inferred from lack of objections. It is explicit because every card arrived through a lane-level foundation suite, Agent E's cross-lane audit, and the exact-green integration gate. Future removals must be explicit IDs in `CHAPTER3_REVIEW_QUARANTINE_IDS`; silently dropping a card is not allowed.

The roadmap's earlier 100–130 estimate remains a planning range, not an editorial quota. No reviewed card is discarded merely to hit a target count.

## What changes at the reviewed boundary

Audited staging objects are never mutated. Each reviewed card is a deep copy.

Only epistemic presentation policy is enforced:

- project-position questions receive the visible prefix `[Позиция курса]` if the lane did not already include it;
- project questions are noncompetitive;
- contested questions are noncompetitive;
- Greek morphology cards are noncompetitive;
- history cards are noncompetitive;
- application cards are noncompetitive.

Objective neutral text/interpretation cards that already carry `competitive=True` remain **internal ranking candidates only**. Their IDs are exported as `CHAPTER3_RANKING_CANDIDATE_IDS`, but this set is not imported by root `ranking_policy.py`, `COMPETITIVE_POOL`, Battle, or Challenge.

```text
REVIEWED competitive=True != RANKING ADMISSION
```

## Source resolution

Chapter 3 cannot copy Chapter 2's flat source lookup blindly because the four lanes intentionally retain lane-local inspection metadata. The reviewed bank therefore records the originating lane for every ID.

A reviewed card resolves sources only against:

```text
root SOURCE_CATALOG + that reviewed card's own lane catalog
```

It never resolves against a synthetic strongest-record union of all lane catalogs.

This preserves the Agent E invariant:

```text
SAME WORK ID != SAME INSPECTION DEPTH
SOURCE CANONICALIZATION != SOURCE-DEPTH UPGRADE
```

## Coverage matrix

Coverage is now machine-addressable through `CHAPTER3_DOMAIN_POOLS`.

Required domains:

- 3:1–7 — text, Greek, intertext, history, theology, disputed, application;
- 3:8–12 — text, Greek, intertext, theology, disputed, application;
- 3:13–17 — text, Greek, intertext, theology, disputed, application;
- 3:18–22 — text, Greek, intertext, theology, disputed, application.

The reviewed regression test requires every required domain to be non-empty and preserves the audited lane/card order.

## Substantive disputes remain represented

Reviewed-bank completeness does not erase the nonblocking substantive HOLDs recorded by Agent E. In particular, the reviewed corpus continues to mark as interpretation/project/contested where appropriate:

- household and social reconstruction in 3:1–7;
- `φόβος`, `κατὰ γνῶσιν`, `ἀσθενεστέρῳ σκεύει`, `ὁμοίως`;
- `εἰς τοῦτο` and the broader role of Psalm 34 in 3:8–12;
- Isaiah 8 reuse and its Christological force in 3:13–17;
- flesh/spirit syntax, spirits/proclamation, `ἐπερώτημα`, baptismal systematics, and 1 Enoch dependence in 3:18–22.

For 3:19–20 the project course position remains fallen spirits / Watchers + victory proclamation, while serious alternatives remain visible. For 3:21 no single Russian gloss for `ἐπερώτημα` is forced and no denomination-specific baptismal mechanism is smuggled into a lexical card.

## Next gate

This reviewed bank may become eligible for **normal-learning-only** product wiring after fresh exact-head CI, Security Audit, and CodeQL on this branch.

Even after that future step:

```text
chapter3 normal learning != random_all
chapter3 normal learning != Challenge
chapter3 normal learning != Battle
chapter3 normal learning != rating/rewards
```

Ranking admission remains a separate card-by-card source-review decision.
