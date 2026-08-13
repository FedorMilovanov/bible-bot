# Chapter 2 Coverage Matrix

Status values: `done`, `partial`, `pending`, `n/a`.

A row is not complete until its evidence domains have been intentionally reviewed. A domain does not require a standalone question when the same claim is already adequately covered by an adjacent reviewed module; do not manufacture questions merely for matrix symmetry.

| Passage | Text | Greek | OT/LXX | History/social | Theology | Disputed | Application | Source review | Production |
|---|---|---|---|---|---|---|---|---|---|
| 2:1-3 | done | done | done (Ps 33/34) | n/a | done (growth) | done (`logikon` lexical range reviewed and non-competitive) | done | done | reviewed only |
| 2:4-5 | done | done | done as background (temple/priesthood imagery reviewed; not mislabeled as a standalone explicit quotation) | n/a | done (spiritual house/priesthood) | n/a | done | done | reviewed only |
| 2:6-8 | done | done | done (Isa 28; Ps 117/118; Isa 8) | n/a | done contextually (Christ/stone logic + 2:8 review) | done (appointment/accountability mapped) | done via 2:4-10 identity application | done | reviewed only |
| 2:9-10 | done | done | done (Exod 19 + Hosea 1-2) | n/a | done at passage level (people-of-God synthesis) | n/a (whole-canon church/Israel system intentionally not forced from one passage) | done | done | reviewed only |
| 2:11-12 | done | done | n/a | done (social/spiritual exile distinction reviewed) | done contextually (identity and witness) | done (`day of visitation` mapped) | done | done | reviewed only |
| 2:13-17 | done | done | n/a | done (Roman administration reviewed with Pliny/Trajan kept as later comparison) | done (civil authority + canonical limit kept separate from direct text) | done at project-position level | done (2:15-16 freedom/good application) | done | reviewed only |
| 2:18-20 | done | done | n/a | done (ancient household slavery/dependence named directly) | done contextually (2:21-25 supplies the Christological rationale) | n/a (historical/ethical cautions handled in history and application layers) | done with 2:18-25 suffering application | done | reviewed only |
| 2:21-23 | done | done | done (Isa 53) | done (embodied suffering context reviewed) | done (Christ as example) | n/a | done | done | reviewed only |
| 2:24-25 | done | done | done (Isa 53) | n/a | done except quarantined wording in `ch2_theol_002` | n/a (broader extent-of-atonement system is outside the reviewed proposition) | done via 2:18-25 suffering application | done with quarantine | reviewed only |

## Current review architecture

Chapter 2 remains intentionally outside production.

- `questions/chapter2/draft.py` is the evidence-authoring aggregate.
- `questions/chapter2/reviewed.py` is the reviewed chapter bank and excludes `ch2_theol_002` through an explicit quarantine.
- `questions/chapter2/quality_overrides.py` applies editorial corrections at the reviewed boundary for competitive items whose raw authoring wording is intentionally preserved.
- direct-text 2:1-10 and 2:21-25 have already received a first distractor-quality rewrite in the raw modules;
- 2:11-20 receives its first competitive distractor-quality pass through the active reviewed quality layer;
- `docs/QUESTION_QUALITY_STANDARD.md` defines the project rule for plausible, parallel distractors and answer-shape leakage;
- `docs/CHAPTER2_SOURCE_AUDIT.md` records the manual source-quality and URL/source-class pass.

The reviewed bank is guarded for unique IDs, schema, source resolution, quarantine, and epistemic boundaries. The chapter remains absent from `POOL_REGISTRY`.

## Current quarantine

`ch2_theol_002` is excluded from `CHAPTER2_REVIEWED_QUESTIONS` because its explanation currently reaches farther into the systematic extent-of-atonement question than the proposition being tested requires.

Do not promote this item until the wording is narrowed successfully. Its exclusion does not create a chapter-coverage hole because the direct text, Isaiah 53 intertext, and other 2:21-25 theology cards already cover substitutionary sin-bearing.

## Remaining chapter-2 blockers

1. Finish the human wording/distractor-quality pass under `docs/QUESTION_QUALITY_STANDARD.md`, especially non-competitive theology/history/application cards and any remaining obvious answer-shape clues.
2. Run the final duplicate/near-duplicate wording review on the reviewed bank.
3. Either narrow `ch2_theol_002` successfully or keep it excluded from the production candidate.
4. Remove any no-longer-used authoring helper files when write safety permits; cleanup must not delay content correctness.
5. Build the canonical Chapter-2 production pool from the reviewed bank only after the editorial pass is complete.
6. Keep Chapter 2 out of `_COMPETITIVE_LEAF_KEYS` until a separate ranking-admission review is completed.
7. Expose the normal learning pool to Telegram/Mini App only after exact-head CI, Security Audit, CodeQL, production import, and web smoke checks succeed on the integration tree.

## Definition of complete chapter 2

Chapter 2 may enter production only after its reviewed bank passes source, epistemic, wording, and duplicate review; quarantined items remain excluded; and fresh exact-head CI, Security Audit, and CodeQL succeed on the actual integration result.
