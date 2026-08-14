# 1 Peter Chapter 3 — cross-layer release audit

**Status:** `FULL STACK AUDITED / DRAFT STACK / NOT MERGED`

This audit is intentionally read-only with respect to gameplay. It sits on top of the exact-green Challenge-admission checkpoint:

`d9d944d88401cccd90408d00d3ef7bf0977c5bba`

Its job is to prove that the complete Chapter-3 lifecycle still agrees end-to-end after all staged admissions.

## Frozen green lineage

```text
base                9eefbae4cf91d178e9f488e695df9264478197c0
integration         571f1f3d0fcab83c0b087735adcaf61316090525
reviewed            fe0b9ea7ed0098c2a625bb6956df0a1368e182ef
normal learning     b84ea8559fddc3bade5044b9124ddab8784a0cc5
ranking audit       b695f9170673caaaff517803d82331386750ccb2
ranking authority   05fbcc7f2052cdd106d406dc364bc466dac843fa
Battle admission    6f7fbc5de2e7fbfc7d54b6c844234636dbc03a49
Challenge taxonomy  62fb982da58a4a97739dd9b504e43cb5d34d7ec2
Challenge admission d9d944d88401cccd90408d00d3ef7bf0977c5bba
```

Every stage above has its own exact-head CI, Security Audit and CodeQL success recorded in the machine manifest.

## End-to-end product state

### Normal learning

The normal Chapter-3 course is exactly the 165-card reviewed deep-copy bank.

```text
pool = chapter3
cards = 165
normal-learning points = 0
daily bonus = 0
normal-learning achievements = none
random_all inclusion = no
```

The Mini App exposes Chapter 3 through the server-authoritative API path and starts normal quizzes with `ranked=false`.

### Ranked authority

Exactly 12 cards are source-reviewed and ranking-authorized:

`ch3_text_101` through `ch3_text_112`.

They are all direct-text / high-confidence / neutral cards with exact evidence minimum:

- SBLGNT — `inspected_primary` in the owning 3:1–7 lane;
- NET 1 Peter 3:1–7 notes — `inspected_passage`.

The ranking audit has zero HOLDs for this candidate surface.

The other 153 reviewed Chapter-3 cards remain outside general competitive and Battle.

### Challenge

The same explicit 12-card authority is admitted only through the reviewed taxonomy:

```text
Easy   6
Medium 6
Hard   0
```

Hard remains Chapter-1-only because the current Chapter-3 authority is direct-text only. No card is promoted to hard for symmetry.

Challenge fallback remains `CHAPTER1_COMPETITIVE_POOL`, so Chapter 3 cannot enter through shortage fallback.

## Epistemic boundary

No ranked Chapter-3 card may be:

- `position=project`;
- `confidence=contested`;
- `claim_type=greek`;
- `claim_type=history`;
- `claim_type=application`.

Project-position questions remain visibly labeled `[Позиция курса]` in the reviewed bank.

This keeps the user's TMS/MacArthur project position explicit without relabeling disputed exegesis as neutral fact.

## Source boundary

The root product source registry contains conservative Chapter-3 source identities where needed, but those identity records carry no claim inspection depth.

Claim support continues to resolve against the reviewed card's own lane catalog.

```text
PRODUCT SOURCE IDENTITY != CLAIM DEPTH
SAME WORK ID != SAME INSPECTION DEPTH
CROSS-LANE STRONGEST RECORD != AUTHORITY
```

Existing shared root authority is never overwritten by Chapter-3 identity metadata.

## Legacy Telegram menu

The Chapter-3 course is available through the Mini App/API. The monolithic legacy `bot.py` menu was deliberately not replaced wholesale merely to add a button.

That remains an implementation-surface gap, not a content or ranking workaround. A future Telegram-menu exposure should be done through a modular menu/pool registry refactor rather than a whole-file replacement.

## Release-audit rule

This audit adds only manifest/documentation/tests. It does not change:

- question pools;
- Battle scoring;
- Challenge quotas;
- result persistence;
- normal-learning scoring policy;
- Mini App behavior;
- `main`;
- merge state.

The full stack is not considered release-audited until this final exact head also passes CI, Security Audit and CodeQL.

```text
ALL LAYERS GREEN != AUTOMATIC MERGE
RELEASE AUDIT != MAIN MUTATION
```
