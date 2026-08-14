# 1 Peter Chapter 3 — ranking-readiness audit

**Status:** `AUDITED / FAIL-CLOSED / NO RANKING ADMISSION`

This layer is stacked on the exact-green Chapter-3 normal-learning product checkpoint:

`b84ea8559fddc3bade5044b9124ddab8784a0cc5`

Parent gates:

- CI #1227 — SUCCESS;
- Security Audit #1107 — SUCCESS;
- CodeQL Stacked PR #909 — SUCCESS.

Chapter 3 is already available as a reviewed, non-scoring Mini App/API learning course. This audit does not modify that product surface.

## Why ranking needs another gate

The reviewed layer deliberately preserves some `competitive=True` metadata on objective cards. That flag means only that a card may be considered for future ranking. It does not prove that the claim's evidence has the stricter depth needed for PvP/Challenge.

The repository's existing structural ranking policy requires:

```text
competitive = true
confidence = high
position = neutral
claim_type = text
sources = non-empty
```

Chapter 3 adds a source-depth gate on top of that structure.

## Source-depth rule

Every source cited by a candidate must satisfy one of two conditions:

1. it is present in that card's own lane-local source catalog with an explicitly claim-ready inspected status; or
2. it is the narrowly allowlisted root primary text `sblgnt` for a lane that intentionally uses canonical root text identity.

No commentary, lexicon, article, or historical source receives the root-only shortcut.

The audit fails closed on:

- publisher abstract only;
- bibliographic/catalog metadata only;
- table-of-contents only;
- edition metadata only;
- unknown inspection status;
- missing inspection status;
- a non-primary source present only in the root identity registry but not inspected in the card's own lane.

## Lane-specific status normalization

The four audited lanes use different metadata schemas, so the audit normalizes them explicitly rather than pretending one key fits all:

- 3:1–7 — `evidence_status`;
- 3:8–12 — `inspection_level`;
- 3:13–17 — `inspection_status`;
- 3:18–22 — `inspection_scope`.

Statuses are mapped to either claim-ready or limited. Unrecognized statuses are HOLD, not guessed.

This preserves the cross-lane invariant:

```text
SAME SOURCE ID != SAME INSPECTION DEPTH
PRODUCT IDENTITY != CLAIM EVIDENCE
STRONGER NEIGHBORING LANE != AUTOMATIC UPGRADE
```

## Audit outputs

`questions/chapter3/ranking_audit.py` exposes:

- `CHAPTER3_RANKING_AUDIT` — readiness + reasons for every reviewed competitive candidate;
- `CHAPTER3_RANKING_READY_IDS` — candidates that pass structural + source-depth rules;
- `CHAPTER3_RANKING_HOLD_REASONS` — fail-closed reasons for all remaining candidates.

These are audit outputs only. Root ranking policy does not import them in this PR.

## No admission in this layer

Regression tests require all Chapter-3 candidates — including any audit-READY IDs — to remain absent from:

- `COMPETITIVE_POOL`;
- `BATTLE_POOL`;
- every `CHALLENGE_POOLS` pool.

Normal learning remains exactly the same 165 reviewed cards.

```text
RANKING_READY != RANKING_ADMITTED
AUDIT_GREEN != GAMEPLAY_CHANGE
```

## Next decision

After fresh exact-head CI/Security/CodeQL, the READY/HOLD partition can be reviewed as a separate authority checkpoint. Only a later explicit admission PR may change root competitive composition, and it must add card-by-card admission IDs rather than silently importing all `competitive=True` Chapter-3 metadata.
