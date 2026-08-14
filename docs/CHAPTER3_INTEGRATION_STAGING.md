# 1 Peter Chapter 3 — staging integration

**Status:** `INTEGRATED FOR REVIEW / NOT PRODUCTION / NOT RANKING / NOT CHAPTER-COMPLETE`

This branch integrates the exact Agent A/B/C/D lane heads that Agent E audited together. It does not merge or rewrite the lane PRs. Audited lane files are reused byte-for-byte from their frozen commits; integration-specific work is limited to the chapter-local aggregate, manifest, and cross-lane regression contract.

## Frozen authority

- base: `9eefbae4cf91d178e9f488e695df9264478197c0`
- A / 3:1–7: `8b194513420ae6dc5adf853b051539ca1f499ed0`
- B / 3:8–12: `b7cc829e31a0aefc851b12245d7933afcb6561e8`
- C / 3:13–17: `d64656dbcfb8c8c894a11d6cc5e764a189de9336`
- D / 3:18–22: `d4151176053aec4a6bce7685922cb90dfc5f2a77`
- E audit: `8c045204190edb8e175b25863c9d87b70d194bc1`

Agent E's frozen audit records 165 cards, zero exact ID collisions, clean canonical metadata, source-depth PASS in all four lanes, and blocker count zero. Its verdict remains `PASS_WITH_HOLD` for every lane: staging readiness is not publication readiness.

## Integrated card corpus

```text
3:1–7   = 56
3:8–12  = 37
3:13–17 = 27
3:18–22 = 45
TOTAL   = 165
```

The audited per-card `competitive` flags are preserved byte-for-byte. Some objective A-lane cards are deliberately marked `competitive=True` as **future ranking candidates**; contested, project, and application cards remain `competitive=False`.

That metadata is not production admission. The package exposes `CHAPTER3_STAGING_QUESTIONS` only inside `questions.chapter3`. Root `questions/__init__.py`, `POOL_REGISTRY`, `COMPETITIVE_POOL`, Battle, Challenge, and `random_all` are intentionally unchanged, and cross-lane tests require every Chapter-3 ID — including every `competitive=True` candidate — to remain absent from those production/ranking surfaces.

Therefore:

```text
competitive=True ON A STAGING CARD != RANKING_ADMITTED
STAGING_INTEGRATED != PRODUCTION_ADMITTED
STAGING_INTEGRATED != RANKING_AUTHORIZED
PASS_WITH_HOLD != PUBLICATION_READY
```

## Source resolution

Lane source catalogs remain namespaced. The same source ID may occur in more than one lane with different inspection metadata schemas or depths. Integration must never merge those records by ID and accidentally upgrade a shallow inspection into a stronger one.

The cross-lane contract therefore resolves each card against:

```text
root canonical source IDs + that card's own lane catalog
```

and never against a union that chooses the strongest metadata record.

## Project decisions at the integration boundary

### 3:19–20 — spirits/proclamation

For future course authoring, the conservative project position is recorded as **fallen spirits / Watchers + victory proclamation**, supported in the lane by inspected MacArthur/GTY and Storms/TGC passage material. Grudem's Christ-through-Noah reading and the descensus/human-dead reception family remain explicitly represented.

Metadata boundary:

```text
position = project
confidence = contested
competitive = false
```

This is an owner/course position, not a claim that Greek morphology or neutral scholarship has closed the dispute.

### 3:21 — `ἐπερώτημα`

No one-word Russian gloss is forced at integration. Appeal/request, pledge/stipulation, and confession/response-related readings remain live. Future user-facing wording should teach the ambiguity rather than conceal it.

### 3:21 — baptism

The course may reject a reading that treats external washing itself as the whole saving mechanism, because the verse explicitly contrasts removal of dirt and links the clause to good conscience and Christ's resurrection. Integration does not force a denominationally precise sacramental mechanism and does not derive systematics from the noun or verb alone.

## Nonblocking substantive HOLDs

The unresolved exegetical/historical questions preserved by A/B/C/D and Agent E remain quarantined from ranking wherever they are contested/project/application. They include household/social reconstruction, `φόβος`, `ἀσθενεστέρῳ σκεύει`, `εἰς τοῦτο`, broader Psalm function, Isaiah/Christological force, flesh/spirit syntax, spirits/proclamation alternatives, `ἐπερώτημα`, baptismal systematics, and direct-dependence claims about 1 Enoch.

These are not integration blockers because the cards encode the uncertainty and are not admitted to production. They become blockers if a later production author presents one as an undisputed fact or tries to promote a contested/project/application claim into ranking.

## Next admission gate

1. Fresh CI, Security Audit, and CodeQL must pass on the exact integration head.
2. Run a product-side editorial curation pass over the 165-card staging bank; the roadmap's 100–130 planning range is not a quota, and staging volume is not a reason to publish redundant cards.
3. Build the Chapter-3 reviewed/admitted aggregate only after curation and source review, analogous to Chapter 2's reviewed boundary.
4. Preserve audited `competitive=True` only as candidate metadata until objective cards are explicitly source-reviewed and separately admitted to root ranking surfaces.
5. Only after the reviewed aggregate, coverage matrix, and fresh exact-head gates may Chapter 3 be considered for normal-learning production exposure.
