# Chapter 4 Release Audit — 1 Peter 4:1–19

## Authority and lineage

- Audited `bible-bot` base: `e4dea87d7348ee940bc628f7f8d53379e05a5a3a`.
- Audited Research authority: `0142430af8ba80f28e0fd9cde669d32611a1d2af`.
- Authoring/review/ranking lifecycle commit: `ea809112653146874a47ea5b185ac3f646f76b97`.
- Normal-learning admission commit: `c21f26131b17b88ff16274687cf70deb8227be6c`.

The production authority is the reviewed Chapter 4 bank under `questions/`. Research PR prose is not treated as authority by itself; the effective handoff resolves the audited Research corpus and later override/quorum layers.

## Effective state

| Gate | Effective state |
| --- | ---: |
| Research claims | 72 |
| Research MCQ prototypes | 32 |
| Effective Research HOLD | 0 |
| Authored cards | 52 |
| Reviewed cards | 52 |
| Review quarantine | 0 |
| Ranking-ready cards | 0 |
| Battle admission | 0 |
| Challenge admission | 0 |

The final three zeros are deliberate fail-closed policy, not unfinished authoring. Chapter 4 has no Research competitive candidates, and absence of Research HOLD does not imply ranking/publication admission.

## Normal-learning contract

`POOL_REGISTRY["chapter4"]` is populated only from `CHAPTER4_REVIEWED_QUESTIONS`. Chapter 4 is a non-scoring learning pool and remains outside `random_all`, `COMPETITIVE_POOL`, Battle, Challenge pools, and Challenge fallback.

A completed Chapter 4 attempt may update only Chapter 4 learning progress (`chapter4_attempts`, `chapter4_correct`, `chapter4_total`, `chapter4_best_score`). It awards zero ranking points, zero daily bonus, and zero achievements and does not increment the normal ranked-test/perfect counters.

The server-authoritative quiz path resolves `chapter4` through the canonical pool registry. Correct answers remain inside the durable session; the public question payload does not expose `correct` or the explanation before answer processing. No Chapter 4 Mini App UI or `bot.py` surface is added in this workstream.

## Sensitive epistemic boundaries

- **1 Peter 4:6:** scholarly dispute remains explicit. The selected course reading is visibly labelled `[Позиция курса]`; morphology is not used to prove chronology, location, or the identity of the dead.
- **Malachi 3:** treated as a serious proposed background where relevant, not as a formal or exclusive quotation claim.
- **1 Peter 4:14:** the current ECM-based critical decision is distinguished from manuscript unanimity. The named Sinaiticus expansion is taught as a witness fact, not as automatic proof of Ausgangstext.
- **1 Peter 4:16:** SBLGNT `ὀνόματι` is distinguished from ECM/NA28 `μέρει`; the edition difference is named rather than flattened.
- **Greek morphology:** parsing supports grammatical observation but never substitutes for exegesis.

## Source-depth boundary

New root source-registry entries required by Chapter 4 provide identity/provenance only. They do not promote inspection depth. Claim-level evidence lane and actual inspection depth remain attached to the Chapter 4 authored/handoff record. Existing root source authority is not overwritten, and evidence depth is never upgraded across lanes by source identity alone.

## Exact-head validation gate

A Draft PR is the validation surface for the exact final branch head. Required gates are:

- `CI`: workflow validation/actionlint, dependency consistency, secret guard, Ruff, compileall, full pytest, Mini App JavaScript syntax and unit tests, Docker production build, production-controller import inside the built image, and web smoke inside the built image.
- `Security Audit`: `pip-audit`.
- `CodeQL`: Python and JavaScript/TypeScript with `security-extended` queries.

A green validator proves the tested head passed those gates. It does **not** create competitive/publication authority: `GREEN_VALIDATOR != PUBLICATION_APPROVAL`.

## Merge boundary

This workstream authorizes a **Draft PR only**. `main` is not mutated and merge is not authorized here. Any future competitive, Battle, Challenge, publication, or merge admission requires an explicit later authority decision.
