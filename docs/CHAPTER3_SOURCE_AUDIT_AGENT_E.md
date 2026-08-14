# 1 Peter 3 Source Audit — Agent E

**Role:** independent source/evidence and cross-lane auditor for 1 Peter 3:1–22.  
**Integration baseline:** `9eefbae4cf91d178e9f488e695df9264478197c0`.  
**Agent E branch:** `agent/ch3-e`.  
**Lane rule:** this audit does not edit A/B/C/D question branches, does not merge, and does not mutate the shared source registry or production wiring.

## Governing rule: FAIL CLOSED

- Stage closure is not publication authority.
- A located source is not a proved claim.
- A live URL is not an inspected source.
- Metadata is not passage-level exegesis.
- An abstract is not full-text evidence.
- A publisher/catalog page is not an inspected commentary section.
- One commentator is not consensus.
- Morphology is not exegesis.
- Historical plausibility is not a statement made by the biblical text.
- A green lane CI run is not integration or publication authorization.

The first Agent E pass remains the evidence-control baseline: SBLGNT/MorphGNT text and morphology, LXX/OT controls, ancient background, social-history limits, conservative disagreements, peer-reviewed controls, explicit `DO NOT CLAIM`, `CONFLICT MAP`, `SOURCE GAPS`, and HOLD boundaries. The machine matrix remains unchanged in this second pass because the findings below are cross-lane integration/quality findings rather than changes to those underlying claim nodes.

## First-pass guardrails still in force

1. 1 Pet 3:1 does not say every addressed wife had an unbelieving husband.
2. `ἀσθενεστέρῳ σκεύει` is not resolved by morphology alone.
3. 1 Pet 3:10–12 is a sustained quotation/adaptation of Ps 33:13–17 LXX (= Ps 34:12–16 common MT/English numbering), not mechanically verbatim copying.
4. Isa 8:12–13 and 1 Pet 3:14–15 have strong verbal reuse; exact terminology for quotation/allusion must not hide the distinction between textual observation and theological inference.
5. Inspected SBLGNT/MorphGNT at 3:18 reads `ἔπαθεν`; English “died” must not be retrojected into the Greek surface.
6. `σαρκί ... πνεύματι`, the identity of the spirits in prison, the timing/content of `ἐκήρυξεν`, and the exact sense of `ἐπερώτημα` remain exegetically disputed.
7. Crawford cannot be used from an abstract to prove an exclusive lexical meaning “pledge.”
8. Baptismal-regeneration conclusions cannot be proved or disproved by lexical fiat from `βάπτισμα` or `σῴζει`.
9. Grudem, MacArthur, Schreiner, and other conservative witnesses materially differ on 3:19–21; no manufactured conservative consensus is allowed.
10. Direct literary dependence on 1 Enoch and manuscript-distribution claims at 3:18 remain HOLD without stronger evidence.

# SECOND PASS — REAL PR HEAD AUDIT

## Audit snapshot at start

The following SHAs were fetched from the live PR objects at the start of this pass and are the authority for the findings below:

- `A_SHA=aecdcb6fbf15648e23aad51ce65560e75bff3986` — PR #17 `agent/ch3-a`
- `B_SHA=d321048fc59a865ba824d3ca68fe2605da0ea5c3` — PR #14 `agent/ch3-b`
- `C_SHA=820087cf00ecfa9a648329d5041ec8a85e2cba4f` — PR #13 `agent/ch3-c`
- `D_SHA=95b6c972ff381d8e10158c417f2433139fdfc323` — PR #16 `agent/ch3-d1`

No finding in this document silently applies to a later SHA. A final live head check is required at the end of Agent E work; any moved lane must be marked `STALE_AUDIT_REQUIRES_RERUN` rather than inherited into a PASS claim.

## METADATA VIOLATIONS

Canonical values required for all A–D question modules:

- `claim_type`: `text | greek | history | interpretation | application`
- `position`: `neutral | project`
- `confidence`: `high | medium | contested`

### A — PR #17

No illegal values found in the audited head. `text`, `greek`, `history`, `interpretation`, and `application` are used within the allowed set; `position` is `neutral` or `project`; confidence values are canonical.

### B — PR #14

No illegal values found in the audited head. The intertext module correctly maps its analytical cards to `text` or `interpretation` rather than inventing an `intertext` enum.

### C — PR #13 — BLOCKER

| PR | file | question ID | current value | required disposition |
|---|---|---|---|---|
| #13 | `questions/chapter3/intertext_13_17.py` | `ch3_ot_301` | `claim_type="intertext"` | `claim_type="interpretation"` |
| #13 | same | `ch3_ot_302` | `claim_type="intertext"` | `claim_type="text"` is the cleanest mapping for the direct textual comparison; `interpretation` is acceptable only if the card is rewritten as analysis |
| #13 | same | `ch3_ot_303` | `claim_type="intertext"` | `claim_type="interpretation"` |
| #13 | same | `ch3_ot_304` | `claim_type="intertext"` | `claim_type="interpretation"` |
| #13 | `questions/chapter3/theology_13_17.py` | `ch3_theol_301` | `claim_type="theology"` | `claim_type="interpretation"` |
| #13 | same | `ch3_theol_302` | `claim_type="theology"` | `claim_type="interpretation"` |
| #13 | same | `ch3_theol_303` | `claim_type="theology"` | `claim_type="interpretation"` |
| #13 | same | `ch3_theol_304` | `claim_type="theology"` | `claim_type="interpretation"` |
| #13 | `questions/chapter3/application_13_17.py` | `ch3_app_301` | `position="pastoral"` | `position="project"` |
| #13 | same | `ch3_app_302` | `position="pastoral"` | `position="project"` |
| #13 | same | `ch3_app_303` | `position="pastoral"` | `position="project"` |
| #13 | same | `ch3_app_304` | `position="pastoral"` | `position="project"` |

The lane test currently preserves two of the invalid enums: `tests/test_chapter3_13_17_foundation.py` asserts `claim_type == "intertext"` and separately asserts `claim_type == "theology"`. Those assertions must be updated together with the question metadata; a green test that enforces a noncanonical schema is not evidence of integration readiness.

No noncanonical `confidence` value was found in C.

### D — PR #16

No illegal values found in `claim_type`, `position`, or `confidence`. D's problem is ID taxonomy, source-depth, and distractor/answer-key leakage, not the canonical enum set.

## CROSS-LANE COLLISIONS

### Question IDs

No exact question-ID collision was found across the four audited heads. The current numeric ranges are structurally separated:

- D: low `001+`
- A: `101+`
- B: `201+`
- C: `301+`

A/B/C use the expected namespaces. D currently uses `ch3_int_001`–`ch3_int_005` in `intertext_18_22.py`; `ch3_int_*` is outside the required Chapter-3 taxonomy. Required disposition: rename these five IDs to `ch3_ot_001`–`ch3_ot_005`. That hypothetical rename creates **no collision** with A/B/C because their `ch3_ot_*` ranges begin at 101/201/301. `tests/test_chapter3_18_22_completion.py` currently references `ch3_int_003` and must be updated with the rename.

### Source-catalog collisions and aliases

Do not perform blind string deduplication. Canonicalize the logical work while retaining inspection state, passage scope, edition/format, and access URL where they differ.

**Same ID, divergent metadata:**

- `sblgnt` exists in A and B with the same logical source/URL but different titles and kinds (`primary_text_greek` vs `primary_text`). Safe canonical work ID: `sblgnt`; use one generic title/kind and keep passage scope separately.
- `morphgnt_1peter` exists in A and B with the same logical source/URL but different kinds (`morphology_dataset` vs `primary_text_morphology`). Safe canonical work ID: `morphgnt_1peter`, kind `morphology_dataset`. Prefer the Agent E pinned revision `aaed91e57c8e4a8dc9a2383e129ca5e75fe6393d` over mutable `master` for reproducible morphology evidence.

**Safe logical aliases, provided inspection state is not upgraded during aliasing:**

- Agent E `sblgnt_1p3` ↔ lane `sblgnt`.
- Agent E `morphgnt_1p3` ↔ lane `morphgnt_1peter`.
- A `plutarch_conjugal_precepts` ↔ Agent E `plutarch_conjugalia`.
- A `van_rensburg_sarah_2004` ↔ Agent E `janse2004`.
- B `christensen_ps34_1peter` ↔ Agent E `christensen2015`.
- B `greaux_ps34_1peter` ↔ Agent E `greaux2009`.
- C `verbum_moyise_2005_1p3` ↔ Agent E `moyise2005`.
- D `grudem_noah_1p3_19` ↔ Agent E `grudem1986`.
- D `jts_crawford_1p3_21` ↔ Agent E `crawford2016`.
- D `ubs_handbook_1p3_21` ↔ Agent E `arichea_nida1980`.
- D `pierce_spirits_2011` ↔ Agent E `pierce2011`.
- C `jobes_becnt_1peter_2022` ↔ Agent E `jobes2022`.
- A `davids_1peter_1990` ↔ D `davids_1peter_nicnt` ↔ Agent E `davids1990`.
- A `schreiner_1peter_2003` ↔ D `schreiner_1peter_nac` ↔ Agent E `schreiner2003`.
- A `horrell_williams_icc_2023` ↔ D `horrell_williams_icc_v2` (same ICC vol. 2 work; publisher URLs appear format/ISBN-specific).
- B `lxx_rahlfs_hanhart_ps33` ↔ Agent E `lxx_ps33` at the work/edition+passage level.
- D `lxx_genesis_6` ↔ Agent E `lxx_gen6_9` at the Rahlfs-Hanhart edition level, while retaining different passage scopes.
- C `achtemeier_hermeneia_1peter` ↔ Agent E `achtemeier1996`.
- A `balch_wives_submissive` ↔ Agent E `balch1981`.

**Do not auto-alias without edition-level verification:**

- A `lxx_gen_18` uses a different host from Agent E's Deutsche Bibelgesellschaft Rahlfs-Hanhart control; verify the actual edition before treating the text witness as identical.
- D `enoch_10_14_charles` and Agent E `1enoch_watchers` are the same ancient work-family but not automatically the same hosted translation/scope.
- D labels Elliott AYB as 2001 while Agent E's bibliographic control records the original Anchor Bible publication as 2000. This may be edition/reprint metadata rather than an error; do not silently overwrite either year until the exact edition represented by D's URL is verified.

**Verified collision corrections:**

- D has corrected Grudem to *Trinity Journal* NS 7.2 (Fall **1986**), 3–31. This agrees with Agent E's first-pass correction; do not revert to 1987 as the journal publication year.
- D's Crawford metadata, JTS 67.1 (2016), 23–37, agrees with Agent E. Preserve the inspection limitation: abstract/metadata is not the full argument.
- D's Westfall bibliography gives JSNTSup 186 (1999), 106–135. No conflicting duplicate was found in A/B/C; the problem is inspection depth, not a cross-lane page-range collision.

## CROSS-LANE THEOLOGICAL CONSISTENCY

No hidden contradiction in a **neutral direct fact** was found across the four audited heads. The following areas are substantively compatible when their evidence layers are respected:

- **Fear:** A correctly leaves the referent of `φόβος` in 3:2 disputed. C discusses fear/`φόβος` in the distinct 3:14–16 context and the Isaianic fear logic. These are different occurrences/contexts, not contradictory neutral claims.
- **Suffering for righteousness:** B explicitly refuses to turn 3:12 into immunity from suffering; C 3:14 directly allows blessed suffering for righteousness; D's suffering-to-vindication synthesis likewise does not promise an easy life.
- **Retaliation/non-retaliation:** B consistently teaches no evil-for-evil/insult-for-insult and blessing instead. No A/C/D neutral claim contradicts this.
- **God's will:** C keeps `εἰ θέλοι τὸ θέλημα τοῦ θεοῦ` conditional and explicitly refuses to turn the persecutor's evil into moral good. No competing lane asserts the opposite as neutral fact.
- **Baptism and spirits in prison:** D preserves multiple serious readings and does not present morphology as resolving them. No other lane silently contradicts those disputed boundaries.
- **Project vs neutral:** A/B/D generally separate project synthesis from neutral facts. C must repair its schema violations before this separation is mechanically reliable.

**Terminology HOLD — Isaiah 8:** C calls Isa 8:12–13 in 1 Pet 3:14–15 an “explicit quotation without an introductory formula”; Agent E's first pass uses “clear verbal reuse/adaptation.” These formulations need not represent a substantive contradiction, but the integration owner should normalize the relationship vocabulary to the repository's quotation/allusion taxonomy and preserve the distinction between observable verbal reuse and downstream Christological inference. Do not turn a label choice into false certainty.

## DISTRACTOR LEAKAGE

### System-level answer-position check

The integration baseline's `questions/__init__.py` randomizes **question selection/order** for competitive challenge pools, but no option-level shuffle is performed there. Repository code search in this audit found no option-shuffle implementation. Therefore a lane that stores every correct answer at index 0 creates a real answer-position leakage risk unless the integrator introduces and tests a separate option shuffler before publication.

### A — content leakage suspects

A does not have a lane-wide fixed correct index, but several cards are guessable from tone/category without knowing the passage:

- `ch3_hist_101` — the correct answer is the only nuanced/nonabsolute option; distractors are universal claims or a category error.
- `ch3_app_102` — “do not make intimidation a spiritual ideal” is morally obvious against grotesque options such as forbidding outside help; low discrimination from textual knowledge.
- `ch3_app_104` — the correct answer is the only noncoercive/nonpsychological caricature; three distractors are obvious strawmen.
- `ch3_app_108` — the evidence-first method is contrasted with three explicitly anti-evidence procedures; answerable as a methodology quiz without 1 Peter knowledge.
- `ch3_disp_103` — the correct Gen 18/Peter distinction is a long nuanced statement while distractors make elementary false claims (Roman-status proof, Abraham demanding the title, no verbal link).

Required action: A should improve these distractors while preserving the claim boundaries; Agent E does not rewrite them.

### B — BLOCKER: lane-wide index leakage

Every audited B item stores `correct=0`:

- `ch3_text_201`–`ch3_text_208`
- `ch3_gr_201`–`ch3_gr_212`
- `ch3_ot_201`–`ch3_ot_208`
- `ch3_theol_201`–`ch3_theol_204`
- `ch3_disp_201`
- `ch3_app_201`–`ch3_app_204`

That is 37/37 items. In addition, strong content cues occur in:

- `ch3_text_208` — long complete verse summary versus prosperity/grotesque alternatives.
- `ch3_ot_207` — long scholarly synthesis versus “mere decoration,” “no suffering,” or total legal-defense prohibition.
- `ch3_disp_201` — nuanced syntactic dispute versus absurd claims that MorphGNT settles it, the expression is meaningless, or `τοῦτο` is a proper name.
- `ch3_app_204` — only the correct answer is pastorally defensible; the others are harmful absolutes.

Required action: vary answer positions with reviewed distractors or implement/test an integration-level option shuffler that correctly remaps `correct`. Do not rely on question-order shuffling.

### C — BLOCKER: dominant index leakage plus weak distractors

All 22 non-text cards audited in C store the correct answer at index 0:

- `ch3_gr_301`–`ch3_gr_309`
- `ch3_ot_301`–`ch3_ot_304`
- `ch3_disp_301`
- `ch3_theol_301`–`ch3_theol_304`
- `ch3_app_301`–`ch3_app_304`

The five text cards have mixed indices, so the leak is not literally 27/27, but 24/27 lane items still have index 0. Strong content leakage examples:

- `ch3_ot_303` — “explicit quotation” is opposed by “accidental echo,” a Psalm 22 citation, and a medieval gloss.
- `ch3_theol_301` — the correct answer is a long qualified intertextual conclusion; distractors are cartoon errors such as “proved by -ον” or “Christ is called an angel.”
- `ch3_app_304` — the reasonable application is opposed by “provoke suffering,” “call consequences of your own evil martyrdom,” and “avoid good.”

Required action: repair answer-position distribution/option shuffling and improve the flagged distractors after the metadata/source blockers are fixed.

### D — BLOCKER: lane-wide index leakage

Every audited D item stores `correct=0`:

- `ch3_text_001`–`ch3_text_010`
- `ch3_gr_001`–`ch3_gr_015`
- `ch3_int_001`–`ch3_int_005` (also needs taxonomy rename to `ch3_ot_*`)
- `ch3_disp_001`–`ch3_disp_006`
- `ch3_theol_001`–`ch3_theol_005`
- `ch3_app_001`–`ch3_app_004`

That is 45/45 items. Strong content leakage examples:

- `ch3_int_003` — correct nuanced Watchers-background statement versus “1 Peter names Enoch,” “1 Enoch is in SBLGNT,” or “1 Enoch verbatim contains 1 Pet 3:19.”
- `ch3_disp_003` — lexical-history answer versus “nominative proves pledge,” “neuter proves appeal,” or “no extrabiblical history.”
- `ch3_theol_003` — long qualified baptismal synthesis versus three obvious extremes.
- `ch3_app_002` — evidence-first teaching versus “prove one identity from πνεύμασιν,” hide readings, or turn the dispute into ranking.

Required action is the same as B: vary reviewed answer positions or add a tested option-shuffle layer that remaps `correct`.

## SOURCE INSPECTION GAPS

The useful evidence-state ladder for integration is:

`URL_FOUND < METADATA_VERIFIED < ABSTRACT_INSPECTED < PARTIAL_TEXT_INSPECTED < FULL_RELEVANT_SECTION_INSPECTED`.

Aliases must never promote a source up this ladder.

### A — source-depth blockers/HOLDs

- **Treggiari, Roman Marriage:** A's catalog points to the OUP book page. That supports `URL_FOUND/METADATA_VERIFIED`, not the substantive variability claims used in `ch3_hist_101` and especially high-confidence `ch3_hist_105`. Inspect the relevant pages/sections or replace the citation with inspected evidence.
- **Balch, Let Wives Be Submissive:** A points to a Google Books catalog record yet uses the work in `ch3_hist_107` as an exact household-code strategy witness. Catalog metadata is not the argument. Inspect the relevant section or keep the card on HOLD.
- **Horrell/Williams ICC vol. 2:** A points to a publisher page while using the commentary in intertext/history/disputed cards as if exact passage positions were established. Publisher metadata cannot stand in for passage readback.
- **Davids NICNT:** A points to an Eerdmans publisher page but uses Davids for exact lexical/exegetical support in `ch3_gr_107`, `ch3_gr_113`, `ch3_disp_101`, `ch3_disp_106`, `ch3_app_105`, and `ch3_app_107`. Agent E's first-pass receipt for Davids was publisher-description level, not relevant-section inspection.
- **Schreiner NAC:** the B&H publisher page may identify the book but not prove a passage position. Where A uses Schreiner for a claim-specific application, either record the actually inspected passage/public teaching or treat it as bibliographic support only.

A's explicit `INSPECTED_CONSERVATIVE_SOURCE_IDS` for MacArthur/Piper is good practice; do not silently extend that inspected status to the catalog-only commentaries.

### B — nonblocking evidence HOLDs plus quality blocker

- **Gréaux 2009:** Agent E independently reached `ABSTRACT_INSPECTED`. The broad suffering/deliverance thesis may be reported as the author's thesis, but not as full-text consensus or detailed argument.
- **Christensen 2015:** Agent E independently reached `PARTIAL_TEXT_INSPECTED`, not full argument closure. `ch3_ot_207` and related synthesis should remain clearly interpretive.
- B's direct SBLGNT/MorphGNT and Ps 33 LXX comparison is materially stronger than these secondary function claims. Preserve that asymmetry.

No B card was found making an exclusive Crawford-like lexical leap; B's integration blocker is primarily distractor/index leakage, not a hidden 3:21-style overclaim.

### C — source-depth BLOCKERS

- **Jobes 2022:** C's `jobes_becnt_1peter_2022` URL is a Google Books catalog record. Agent E's independent state is `BIBLIOGRAPHIC_INSPECTED`. It is nevertheless used as exact support in `ch3_gr_305`, `ch3_disp_301`, `ch3_theol_301`–`304`, and `ch3_app_301`–`304`. This is not permissible. Inspect the relevant 3:13–17 pages or remove Jobes as claim-specific proof.
- **Achtemeier Hermeneia:** Agent E independently had `TABLE_OF_CONTENTS_INSPECTED`, not relevant-page readback. C uses it in `ch3_gr_309`, `ch3_disp_301`, `ch3_theol_301`, `ch3_theol_304`, and `ch3_app_304`. That attribution must be held until the relevant section is inspected.
- **BDAG:** C's URL is the University of Chicago publisher page for the lexicon, not the lexical entries. It cannot prove the exact `ἀπολογία`/`φόβος` senses used in `ch3_gr_304`, `ch3_gr_307`, or `ch3_app_302`. Add an inspected lexical-entry receipt or use a source whose entry text was actually inspected.
- **Rahlfs-Hanhart/Göttingen catalog:** an edition catalog verifies bibliographic identity, not the Isa 8 wording. Use the actual LXX text source for textual comparison; keep the catalog as edition metadata only.
- **Moyise 2005:** Agent E's first-pass state was abstract/metadata, so detailed historical-literary attributions should not exceed the verified case-study scope without full-text readback.

### D — source-depth BLOCKERS/HOLDs

- **Schreiner NAC quorum:** D's `schreiner_1peter_nac` entry is a B&H publisher page, yet `tests/test_chapter3_18_22_completion.py` requires that ID as the independent evangelical source for every project item. A publisher page cannot satisfy a claim-specific project quorum. D's notes say Schreiner's position was cross-checked in public teaching; the required fix is to add the actually inspected teaching/interview as a distinct source receipt or directly inspect the relevant NAC pages. Until then the project quorum is not closed.
- **Westfall 1999:** D stores an IxTheo bibliographic record but uses Westfall substantively in `ch3_theol_003`, `ch3_disp_004`, and `ch3_app_003`. Bibliography is not chapter inspection; inspect pp. 106–135 or downgrade/remove claim-specific use.
- **Horrell/Williams ICC vol. 2:** publisher metadata is used for exact disputed-passage positions in Greek/disputed/theology cards. Inspect the relevant 3:18–22 pages or keep those at HOLD.
- **Pierce 2011:** Mohr publisher description establishes the monograph's subject; it does not by itself prove every detailed fallen-spirit attribution. Use only the broad scoped thesis until relevant pages are inspected.
- **Marcar 2017:** D's URL is an abstract page. Treat the Urzeit/Endzeit/flood thesis as `ABSTRACT_INSPECTED`, not full-text closure.
- **Lei 2025:** publisher description supports reception-history relevance, not every exact exegetical conclusion.
- **Crawford 2016:** still `ABSTRACT_INSPECTED` in Agent E's independent record. D handles this better than an exclusive-pledge claim: it explicitly preserves appeal/pledge/confession families. Keep it that way; do not rank “pledge” without the full article.
- **Davids/Elliott:** D's own notes correctly say their exact 3:18–22 positions were not inspected and do not use them as claim-specific proof. Preserve that discipline.

## SAFE CANONICALIZATION PLAN

1. Canonicalize **logical works**, not evidence receipts. A merged source record must retain `inspection_level`, passage scope, edition/format, and limitations.
2. Use `sblgnt` as the work-level Greek text ID and `morphgnt_1peter` as the morphology ID; pin MorphGNT to commit `aaed91e57c8e4a8dc9a2383e129ca5e75fe6393d` for reproducibility.
3. Canonicalize Grudem's 3:19 article to Fall 1986, 3–31; do not reintroduce 1987 as the journal publication year.
4. Canonicalize Crawford's work-level bibliography to JTS 67.1 (2016), 23–37, but retain `ABSTRACT_INSPECTED` until full text is read.
5. Treat Davids, Jobes, Schreiner, Horrell/Williams, Achtemeier, Pierce, Westfall, etc. as work-level aliases only. Never let aliasing turn a publisher/catalog record into `FULL_RELEVANT_SECTION_INSPECTED`.
6. Keep LXX witnesses edition-aware. “LXX” is not a single URL/source identity; preserve Rahlfs-Hanhart and other edition/host distinctions until verified.
7. Keep 1 Enoch translation/edition identity explicit; do not merge different hosted translations merely because they cover the same ancient work.
8. Resolve Elliott 2000/2001 only after verifying which edition/reprint D's publisher URL represents.
9. No shared registry mutation belongs in Agent E's branch. The integrator should apply only the aliases needed by published claims after source-depth blockers are closed.

## INTEGRATION READINESS

### A — **BLOCK**

**Exact audited SHA:** `aecdcb6fbf15648e23aad51ce65560e75bff3986`.

**Blockers:** substantive historical/exegetical cards use Treggiari/Balch/Horrell-Williams/Davids at publisher/catalog depth beyond the stored independent evidence receipt. `ch3_hist_105` is especially problematic because it marks the generalized Roman-marriage variability conclusion high-confidence while its modern controls are not passage-inspected.

**Non-blocking HOLDs:** disputed `φόβος`, adornment, Sarah, `ἀσθενεστέρῳ σκεύει`, `κατὰ γνῶσιν`, and `ὁμοίως` are appropriately quarantined; several distractors should be strengthened.

**Required before merge:** inspect and record the relevant source sections (or remove/downgrade those citations/claims), then revise the flagged low-discrimination distractors. Canonical metadata and ID taxonomy themselves pass.

### B — **BLOCK**

**Exact audited SHA:** `d321048fc59a865ba824d3ca68fe2605da0ea5c3`.

**Blocker:** 37/37 questions store the correct option at index 0, and the baseline code only shuffles selected questions, not their options. This creates a deterministic answer-position leak unless integration adds a tested option shuffler or B varies the stored answer positions.

**Non-blocking HOLDs:** Gréaux is abstract-level and Christensen partial-text in Agent E's independent receipts; broad scholarly theses can remain labeled interpretation, but should not be promoted to consensus/full-text certainty.

**Required before merge:** eliminate/test the option-index leak and improve the strongest strawman distractors. Preserve B's otherwise clean canonical metadata, ID taxonomy, and strong primary Ps 33/LXX comparison.

### C — **BLOCK**

**Exact audited SHA:** `820087cf00ecfa9a648329d5041ec8a85e2cba4f`.

**Blockers:** noncanonical `claim_type="intertext"`, noncanonical `claim_type="theology"`, noncanonical `position="pastoral"`; tests explicitly enforce some of those invalid values; Jobes/Achtemeier/BDAG are used beyond inspected depth; 24/27 lane items place the correct answer at index 0 and 22/22 non-text cards do so.

**Non-blocking HOLDs:** exact quotation-vs-verbal-reuse terminology for Isa 8 should be normalized without flattening the genuine intertext; the `κύριον ... τὸν Χριστόν` syntactic labeling remains appropriately disputed.

**Required before merge:** repair canonical metadata and tests, close or remove claim-specific catalog/publisher source uses, then remove the answer-position/distractor leakage.

### D — **BLOCK**

**Exact audited SHA:** `95b6c972ff381d8e10158c417f2433139fdfc323`.

**Blockers:** `ch3_int_001`–`005` violate the required namespace and must become collision-free `ch3_ot_001`–`005`; D's project-quorum test treats a Schreiner publisher page as an inspected independent passage witness; Westfall/Horrell-Williams and other sources are used beyond their stored inspection depth; 45/45 lane items place the correct answer at index 0.

**Non-blocking HOLDs:** D appropriately preserves competing spirits/`ἐπερώτημα`/baptism/`σαρκί...πνεύματι` readings, Crawford is not inflated into exclusive pledge certainty, and Grudem's 1986 metadata correction is sound.

**Required before merge:** rename the intertext namespace and update tests; provide a real inspected Schreiner source receipt (or inspect the NAC passage) for the project quorum; inspect/downgrade the other claim-specific sources; remove/test answer-position leakage and strengthen the flagged distractors.

## Overall second-pass conclusion

The four lanes are **not ready for hypothetical four-way integration at the audited heads**. This is not a claim that their research is worthless: the primary-text work is often strong and the major disputed theological boundaries are mostly handled responsibly. The blocking failures are narrower and concrete — canonical schema in C, D ID taxonomy, source-inspection depth in A/C/D, and deterministic distractor/answer-position leakage in B/C/D.

Do **not** declare Chapter 3 complete from this audit. Do **not** merge on the basis of lane-local green CI alone. Close the blockers at new lane heads, then rerun Agent E against those exact SHAs.