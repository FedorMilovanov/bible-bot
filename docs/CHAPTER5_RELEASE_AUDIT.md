# Chapter 5 release audit — 1 Peter 5:1–14

## Authority

- bible-bot base: `e4dea87d7348ee940bc628f7f8d53379e05a5a3a`
- Research authority: `0142430af8ba80f28e0fd9cde669d32611a1d2af`
- Effective Chapter-5 research claims: **72**
- Research MCQ prototypes: **32** (editorial input, not production/ranking authority)

Resolution order is base candidate → later candidate overrides → source upgrades/quorum → Wave3n where applicable → MCQ prototype → editorial override. Historical HOLD artifacts remain auditable, but later effective authority controls current disposition.

## Admission summary

| Layer | Count |
|---|---:|
| Effective research | 72 |
| Authored staging | 72 |
| Reviewed | 72 |
| Quarantine | 0 |
| Normal-learning product | 72 |
| Ranking READY | 0 |
| Ranking HOLD | 72 |
| Battle | 0 |
| Challenge | 0 |

Chapter 5 is **learning-only / non-scoring**. No Chapter-5 card enters `random_all`, `COMPETITIVE_POOL`, `BATTLE_POOL`, or `CHALLENGE_POOLS`.

## Textual-critical boundaries

1. **5:2A — ἐπισκοποῦντες.** Separate textual unit. Wave3n uses Williams–Horrell's ECM-based textual treatment. This is not direct dECM witness-table readback.
2. **5:2B — κατὰ θεόν.** Separate textual unit from ἐπισκοποῦντες. Stanojević's published comparison represents the ECM-side reading; it is not the full dECM apparatus.
3. **5:10 — four restoration verbs.** The four-form set is stated only for the SBLGNT/MorphGNT text base. Secondary apparatus and Sinaiticus retain their own evidence limits; no manuscript-unanimity claim is made.
4. **5:12 — στῆτε / ἑστήκατε.** `στῆτε` is preferred through explicit published ECM-based treatment plus independent reasoning. No direct project dECM readback is claimed.
5. **5:13 — Babylon / ἐκκλησία.** The text statement “Babylon” is kept distinct from historical identification. Sinaiticus's explicit `ἐκκλησία` is a named-witness fact and is not inserted into the SBLGNT/base text.

## Coverage and answer positions

Correct-index distribution is exactly **0:18 / 1:18 / 2:18 / 3:18**. Release tests additionally inspect sizeable verse and claim-type buckets for collapsed answer-position bias. No runtime shuffle is used.

Cards cover direct text, Greek/morphology/lexical controls, syntax/interpretation, intertext, history/social context, leadership, theology, textual criticism, disputed readings, and visibly marked `[Позиция курса]` applications.

Claim-type counts: `text=29`, `interpretation=12`, `greek=13`, `application=13`, `history=5`.

## Source safety

The root source registry receives Chapter-5 **identity-only** records. Claim-level inspection depth remains owned by the Chapter-5 lane catalog. Existing shared source identities are never overwritten.

Safety invariants include:

- source found ≠ claim proved;
- morphology ≠ exegesis;
- lexicon range ≠ passage exegesis;
- one commentator ≠ consensus;
- secondary apparatus ≠ ECM;
- named manuscript ≠ original-text decision;
- ECM decision ≠ manuscript unanimity;
- ECM-based commentary ≠ direct dECM readback;
- zero research HOLDs ≠ ranking/gameplay authority.

## Ranking result

The independent audit deliberately returns **READY=0 / HOLD=72**. Research Wave 3 reported `COMPETITIVE_CANDIDATES=0`, and no later authority admits Chapter 5 to competitive gameplay. Finding zero candidates is a valid fail-closed outcome.

## Product contract

`POOL_REGISTRY["chapter5"]` exposes exactly the reviewed 72-card bank. `questions.pool_policy` marks it non-scoring, so the existing server-authoritative learning-result path persists `chapter5_attempts`, `chapter5_correct`, `chapter5_total`, and `chapter5_best_score` without ranking points/achievements.
