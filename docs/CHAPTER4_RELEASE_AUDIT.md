# Chapter 4 Release Audit v2 — 1 Peter 4:1–19

## Immutable authority

- `bible-bot` integration base: `e4dea87d7348ee940bc628f7f8d53379e05a5a3a`.
- Research repository: `FedorMilovanov/Research`.
- Research handoff-v2 exact SHA: `7e0140129a4aba59a09737701967c3820ff1af57` (Research PR #184).
- Research authority digest SHA-256: `1f444991ecc2f180abdbe0f459148ba8dbf0a5045b1d8888e462683c78366c7d`.
- Research handoff schema: **2**.

The product no longer trusts a `research_id` link by itself. Each production card resolves through an opaque `review_record_id` to a separate immutable v2 review record containing the exact Research SHA/digest, effective-claim digest, source identities and exact claim-inspection edge IDs. Runtime/public cards do not carry claim-level evidence metadata.

## Reconciliation counts

| Layer | Count |
| --- | ---: |
| Effective Chapter-4 Research claims | 72 |
| Research MCQ prototypes | 32 |
| Effective Research HOLD | 0 |
| Research competitive candidates | 0 |
| Product cards | 52 |
| Immutable v2 product review records | 52 |
| Reviewed cards | 52 |
| Review quarantine | 0 |
| `PRODUCT_CARD` dispositions | 52 |
| `RETAINED_RESEARCH_SUPPORT` dispositions | 20 |
| Ranking-ready | 0 |
| Battle admission | 0 |
| Challenge admission | 0 |

All 72 effective Research claims have an explicit product disposition. The 20 non-card claims are retained with concrete support/non-duplication reasons; none silently disappear.

## 52-card immutable review contract

Every card review record includes:

- product card ID and immutable product-review record ID;
- exact Research repository, SHA and authority digest;
- exact Research claim ID and effective-claim digest;
- handoff schema version;
- exact source IDs plus exact claim-inspection edge IDs;
- claimed position, confidence and claim type;
- explicit safe-phrasing and overclaim-blacklist review flags;
- explicit reviewer and decision;
- no `ranking_review_id` unless ranking is separately considered.

The validator additionally binds the review record to an exact product-card content digest. It rejects forged claim IDs, stale/wrong claim digests, source-without-edge, swapped edges, project→neutral promotion, confidence promotion, claim-type promotion and a competitive flag.

Reviewed cards are deep copies of staging objects; option lists are isolated. Project-position questions receive visible `[Позиция курса]` labelling at the reviewed boundary.

## Agent-E prototype crosswalk — 32/32

Agent-E v2 classification is reconciled for every Chapter-4 Research prototype:

- `SAFE_TEMPLATE`: **13**;
- `NEEDS_REWRITE`: **10**;
- `NONCOMPETITIVE_ONLY`: **6**;
- `REJECT_AS_PRODUCT_TEMPLATE / REFERENCE_DRIFT`: **3**.

The three reference-drift cases remain rejected as templates:

- `w3mcq_003` / `w3q_005`;
- `w3mcq_037` / `w3q_014`;
- `w3mcq_047` / `w3q_038`.

Their current cards are explicitly recorded as **independent product rewrites after rejected templates**. The product validator reads the exact pinned Research prototype material and fails if unsafe prototype stems or wrong-option wording are mechanically copied into the product.

## `w3q_123` ranking discrepancy

The discrepancy has a dedicated product ranking review: `ch4rankv2_w3q_123_no_admission`.

Decision: **`NO_RANKING_ADMISSION`**.

Reasons are authority-level, not numerical: Research v2 keeps the claim noncompetitive; it belongs to the genuine 4:16 textual-critical unit; the supporting treatment is ECM/CBGM scholarly exposition rather than a new Chapter-3-style product ranking authority; Chapter 4 textual criticism remains noncompetitive. Therefore `COMPETITIVE_POOL`, Battle and Challenge admission are all false.

## Required epistemic boundaries

- **4:6:** the selected reading remains visible `[Позиция курса]`; a separate neutral contested card remains. Morphology of `εὐηγγελίσθη` / `νεκροῖς` does not determine chronology, location or addressees.
- **Malachi 3:** a serious proposed prophetic/imagery background, not a formal or exclusive quotation; degree of dependence remains disputed.
- **4:14:** current ECM-based editorial preference is not manuscript unanimity.
- **Sinaiticus:** a named-witness reading is not an Ausgangstext decision.
- **4:16:** SBLGNT `ὀνόματι` is explicitly distinguished from ECM/NA28 `μέρει`; edition identities are not flattened.
- **Textual criticism:** remains noncompetitive.

## Source boundary

Every Research source ID used by the 72-claim handoff resolves against the canonical source identity registry. Chapter-4 additions are identity/provenance only. No global `strongest-depth`, inspection-depth or cross-lane evidence-promotion field is added to the root registry. Exact source/evidence edges remain private to review/handoff records.

## Learning-only / persistence boundary

`POOL_REGISTRY["chapter4"]` contains only the reviewed 52-card bank. Chapter 4 remains absent from:

- `random_all`;
- `COMPETITIVE_POOL`;
- `BATTLE_POOL`;
- every `CHALLENGE_POOLS` bucket;
- Challenge fallback.

A Chapter-4 result awards zero points, zero daily bonus and zero achievements and does not increment ranked totals or perfect counters. Persistence is limited to `chapter4_attempts`, `chapter4_correct`, `chapter4_total`, and `chapter4_best_score`.

Public quiz payloads expose only `id`, `question` and `options`; `correct`, explanation, review IDs, Research IDs/digests, source IDs/edges and reviewer metadata remain server-side/private.

## Adversarial negative tests

The suite actively tries to forge or promote the contract: forged Research claim ID, stale digest, swapped edge, source without edge, project→neutral, confidence raise, interpretation→text, competitive enablement, insertion into random/competitive/Battle/Challenge/fallback, and public correct/private-review metadata leakage. These paths must fail closed.

## First green and mandatory second content pass

The first complete green exact head was:

`2f9ae1cb03b5d6817efefe0988e7e619c2fd5afc`

- CI #1361 / run `31843872381`: success;
- Security Audit #1241 / run `31843872280`: success;
- CodeQL #335 / run `31843872423`: success;
- Chapter 4 Research Handoff v2 #27 / run `31843872269`: success.

Only after that first green, a second independent adversarial readback was conducted across **52/52 cards** using the exact-head product artifact (`artifact_id=9235206954`, SHA-256 `fa517a7fe7d67fe302923017c85d0f683e34947a0724f832c8141dbe01867ef6`).

The pass found one systemic presentation issue affecting **16 cards**: answer-length / option-shape cueing. It found no new epistemic-overclaim or wrong-key finding. The 16 cards were revised and received new immutable review-record IDs/content digests. Severe correct-answer length ratios (`>1.8` versus mean distractor length) fell from **13 to 0**; correct-is-longest concentration fell from **36/52 to 29/52**. The machine record is `data/chapter4-second-adversarial-pass-v2.json`; current open findings: **0**.

## Final exact-head gate and merge boundary

After the second-pass commits, the final branch head must independently pass CI, Security Audit, CodeQL and Chapter 4 Research Handoff v2. Those exact-head run IDs belong in PR #30 rather than being inferred from the first-green parent.

PR #30 remains **Draft**. This workstream does not merge `main`; Agent 5 owns final integration/merge. `GREEN_VALIDATOR != PUBLICATION_OR_RANKING_AUTHORITY` remains in force.
