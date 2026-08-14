# Chapter 5 release audit v2 — 1 Peter 5:1–14

## Authority and immutable handoff

- bible-bot integration base: `e4dea87d7348ee940bc628f7f8d53379e05a5a3a`
- exact Research authority: `FedorMilovanov/Research@0142430af8ba80f28e0fd9cde669d32611a1d2af`
- Research authority digest: `51b935cd819a664ddc6d036c72f38d903c3a1d1c862e283a64b82987dfa7175b`
- reviewed product-bank Git blob: `91d51413a6a0a3f3ad7e6e308c2a6885426ed38f`
- effective Chapter-5 claims: **72**
- Chapter-5 Research prototypes reconciled: **32/32**

The v2 direction of authority is deliberately one-way:

`PRODUCT_CARD → PRODUCT_REVIEW_RECORD → EFFECTIVE_RESEARCH_CLAIM → CLAIM/SOURCE EDGE → OWNING LANE`.

The product repository does not reconstruct Research truth from the card. Each review record pins the exact Research SHA, authority digest, content-addressed candidate locator, per-claim digest, exact inspected edge IDs, source subset, safe-phrasing/blacklist review, claimed type/confidence/position, correct-position metadata and an explicit ranking disposition. The reviewed admission boundary additionally verifies an independent Research metadata map, the exact Git blob identity of `questions/chapter5/bank.py`, and the post-green second adversarial readback.

## First independent semantic readback — 72/72

The first v2 readback compared every Chapter-5 product card against its exact Research candidate and relevant effective Wave3g/Wave3n override. It was a content audit, not a schema-validity check. For all 72 cards the pass examined whether the question actually tests the Research claim, whether the keyed answer is no stronger than the evidence, whether distractors avoid teaching false certainty, false-consensus/manuscript-unanimity language, visible `[Позиция курса]` labelling, historical inference smuggled in from a toponym/morphology/textual form, source-minimum coverage, and exact Research claim type/confidence/position.

The pass found real pre-v2 drift and corrected it in the authoring bank rather than weakening the contract. Source subsets were repaired for `w3q_073`, `084`, `087`, `105`, `107`, `108`, `109`, `110`, `118`, `125`, `126`; confidence was corrected for `w3q_118`, `125`, `126`, `127`; `w3q_128` was corrected from `text` to the Research-authoritative `interpretation` type. After these corrections the claim-type distribution is **text=28 / interpretation=13 / greek=13 / application=13 / history=5**.

## First exact-head green anchor

Only after the corrected bank and the v2 admission contract were frozen did the first release gate complete on exact head `54a8d2d69209b4e900a7ed6e1134365cc9b9b4f8`:

- CI — **SUCCESS**
- Security Audit — **SUCCESS**
- CodeQL — **SUCCESS**
- full pytest, Mini App JavaScript checks/tests, production Docker build, production-controller import and built-container web smoke all completed successfully inside CI.

That exact green head is the immutable anchor recorded in every second-pass adversarial review record.

## Second independent adversarial readback — 72/72

A second semantic pass was performed only after the first exact-head green and against the same frozen product-bank blob `91d51413a6a0a3f3ad7e6e308c2a6885426ed38f`. It did not reuse the first pass as a proof. The adversarial matrix independently attacked:

- claim/stem mismatch;
- keyed answers that silently strengthen the evidence;
- distractors that teach false authority or fake consensus;
- universal manuscript/unanimity language;
- lost project-position labels;
- historical conclusions inferred merely from a toponym, morphology or text form;
- leakage between independent textual units;
- promotion of a named witness into an original-text verdict;
- promotion of ECM-based published evidence into a claim of direct project dECM readback.

Result: **72/72 PASS, finding count = 0**. The most sensitive textual cards were re-read explicitly, including `w3q_050`, `051`, `068`, `075`, `078`, `119`, `125–128`, `141–144`. No further semantic bank edit was required, so the reviewed product-bank Git blob remained unchanged. The second pass is materialized in `questions/chapter5/adversarial_review_v2.py` and is now a mandatory admission gate, not merely prose evidence.

## Prototype reconciliation — 32/32

Research MCQ prototypes remain research/editorial artifacts only. No prototype is publication authority and none is ranking authority. The complete machine ledger is `data/chapter5-prototype-audit-v2.json`.

The explicit rejected templates `w3mcq_020` and `w3mcq_027` are preserved in the audit trail as `REJECTED_TEMPLATE_NOT_PUBLICATION_AUTHORITY`. Their corresponding production claims/cards (`w3q_052` and `w3q_066`) are admitted only under an explicit `INDEPENDENT_PRODUCT_REWRITE_ACCEPTED` record. The rejected family disposition is `NEEDS_REWRITE_RESOLVED_BY_INDEPENDENT_PRODUCT_REWRITE`.

The other 30 prototypes are `RECONCILED_RESEARCH_REFERENCE_ONLY`: they may explain editorial lineage but never confer publication or competitive authority. Research-side editorial overrides remain audit-visible without being promoted to product authority.

## Textual-critical boundaries

1. **5:2A — `ἐπισκοποῦντες`.** This is an independent textual unit. The accepted route is the published ECM-based Williams–Horrell treatment plus INTF controls. It is **not** a direct project dECM witness-table readback. The Stanojević edge used for `κατὰ θεόν` is explicitly forbidden from being transferred to this unit.
2. **5:2B — `κατὰ θεόν`.** This is separate from `ἐπισκοποῦντες`. Williams–Horrell plus Stanojević's published ECM comparison support the effective closure; the product still does not claim direct full dECM readback.
3. **5:10 — four restoration verbs.** `καταρτίσει, στηρίξει, σθενώσει, θεμελιώσει` are stated as an **SBLGNT/MorphGNT edition/text-base observation**. Secondary apparatus and named witnesses show transmission complexity. No wording says or implies that all manuscripts have the same four forms/order.
4. **5:12 — `στῆτε / ἑστήκατε`.** `στῆτε` is supported through published ECM-based editorial treatment and independent reasoning. An editorial decision is not a direct census of every witness; no project direct-dECM claim is made.
5. **5:13 — Babylon / `ἐκκλησία`.** Textual `Βαβυλών` is not automatically the historical identification “Rome”. Sinaiticus's explicit `ἐκκλησία` is a named-witness fact and is not elevated to an original-text verdict or inserted into the SBLGNT base text.

Historical HOLD artifacts for `w3q_050`, `w3q_051` and `w3q_075` remain audit-visible. The effective Wave3n closures, not the historical HOLD state, control current learning-product disposition.

## Canonical data and answer positions

The old `ljust()` presentation padding has been removed entirely from the authoring source. Canonical stems, options, explanations, card IDs, claim IDs and source IDs must be non-empty and have no leading/trailing whitespace. Four option surfaces must also be unique after case-folding. Layout belongs to UI/CSS, not to canonical answer strings.

Correct positions are deliberately authored and remain exactly **0:18 / 1:18 / 2:18 / 3:18**. No Chapter-5 runtime shuffle is used to conceal answer-position imbalance.

## Product/gameplay/persistence boundary

All **72/72** cards are normal learning only and `competitive=False`. No Chapter-5 ranking authority is created by this work.

- `POOL_REGISTRY["chapter5"]` = reviewed Chapter-5 bank only.
- Chapter 5 contributes **zero IDs** to `random_all`, `COMPETITIVE_POOL`, `BATTLE_POOL`, every `CHALLENGE_POOLS` member and `CHALLENGE_FALLBACK_POOL`.
- result persistence uses the existing non-scoring learning path and updates only Chapter-5 progress counters (`chapter5_attempts`, `chapter5_correct`, `chapter5_total`, `chapter5_best_score`). It does not increment ranking totals, points, daily bonus or achievements.

## Public API boundary

Before an answer is accepted, Mini App `public_question()` exposes exactly `id`, `question`, and `options`. It does not expose `correct`, `explanation`, Research candidate metadata, product-review IDs, authority/claim digests, source subsets or claim/source edge IDs. Correct/explanation data may appear only in the already-answered recovery/review payload after the answer authority point.

## Negative and mutation coverage

The v2 suite rejects stale authority digest, wrong claim digest, fabricated inspection edge, a source borrowed from the wrong 5:2 textual unit, direct-dECM wording from ECM-based evidence, manuscript-unanimity wording, project→neutral relabelling, claim-type/confidence strengthening, `competitive=True`, Challenge/fallback leakage, canonical whitespace/padding, duplicated option surfaces, wrong answer-position metadata and pre-answer public-API leakage.

## Final release gate policy

The second pass is complete. The only remaining release condition is a fresh exact-head run after the second-pass records/docs were added: **CI + Security Audit + CodeQL must all succeed on the same post-pass2 PR head**, including full pytest, JS checks, production Docker build/import and built-container web smoke. The exact final SHA and run evidence are recorded in PR #29 after those checks so this tracked document does not mutate its own release head. PR #29 remains Draft and `main` remains untouched until Agent 5 performs the later integration decision.
