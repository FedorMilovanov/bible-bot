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

### C — PR #13 — BLOCKER AT AUDITED SHA

| PR | file | question ID | current value at audited SHA | required disposition |
|---|---|---|---|---|
| #13 | `questions/chapter3/intertext_13_17.py` | `ch3_ot_301` | `claim_type="intertext"` | `claim_type="interpretation"` |
| #13 | same | `ch3_ot_302` | `claim_type="intertext"` | `claim_type="text"` is the cleanest mapping for the direct textual comparison; `interpretation` is acceptable only if rewritten as analysis |
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

At the audited SHA, `tests/test_chapter3_13_17_foundation.py` also asserted `claim_type == "intertext"` and `claim_type == "theology"`; those tests therefore preserved the schema violation rather than detecting it.

### D — PR #16

No illegal values found in `claim_type`, `position`, or `confidence` at the audited SHA. D's audited-head problems were ID taxonomy, source-depth, and distractor/answer-key leakage.

## CROSS-LANE COLLISIONS

### Question IDs

No exact question-ID collision was found across the four audited heads. The audited numeric ranges were structurally separated: D `001+`, A `101+`, B `201+`, C `301+`.

At the audited D SHA, `intertext_18_22.py` used `ch3_int_001`–`ch3_int_005`, outside the required Chapter-3 taxonomy. The collision-free required mapping was `ch3_ot_001`–`ch3_ot_005`; A/B/C `ch3_ot_*` ranges began at 101/201/301. The audited D test also referenced `ch3_int_003` and would have needed update with the rename.

### Source-catalog collisions and aliases

Do not perform blind string deduplication. Canonicalize the logical work while retaining inspection state, passage scope, edition/format, and access URL where they differ.

**Same ID, divergent metadata:**

- `sblgnt` in A and B represented the same logical source/URL but with different titles/kinds (`primary_text_greek` vs `primary_text`). Safe canonical work ID: `sblgnt`; keep passage scope separately.
- `morphgnt_1peter` in A and B represented the same logical source/URL but different kinds (`morphology_dataset` vs `primary_text_morphology`). Safe canonical work ID: `morphgnt_1peter`, kind `morphology_dataset`; prefer Agent E's pinned revision `aaed91e57c8e4a8dc9a2383e129ca5e75fe6393d` over mutable `master`.

**Safe logical aliases, provided inspection state is not upgraded:**

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
- D `lxx_genesis_6` ↔ Agent E `lxx_gen6_9` at the Rahlfs-Hanhart edition level, retaining different passage scopes.
- C `achtemeier_hermeneia_1peter` ↔ Agent E `achtemeier1996`.
- A `balch_wives_submissive` ↔ Agent E `balch1981`.

**Do not auto-alias without edition-level verification:**

- A `lxx_gen_18` used a different host from Agent E's Deutsche Bibelgesellschaft Rahlfs-Hanhart control; verify actual edition before treating the text witness as identical.
- D `enoch_10_14_charles` and Agent E `1enoch_watchers` are the same ancient work-family but not automatically the same hosted translation/scope.
- D labeled Elliott AYB as 2001 while Agent E's bibliographic control records original Anchor Bible publication as 2000. This may be edition/reprint metadata; do not silently overwrite either year without edition verification.

**Verified collision corrections at the audited D head:**

- Grudem was corrected to *Trinity Journal* NS 7.2 (Fall **1986**), 3–31, agreeing with Agent E's first-pass correction.
- Crawford JTS 67.1 (2016), 23–37 agreed with Agent E; abstract/metadata remained weaker than full article inspection.
- Westfall was catalogued as JSNTSup 186 (1999), 106–135; no conflicting duplicate existed in A/B/C at the audited snapshot.

## CROSS-LANE THEOLOGICAL CONSISTENCY

No hidden contradiction in a **neutral direct fact** was found across the four audited heads when evidence layers were respected:

- **Fear:** A left the referent of `φόβος` in 3:2 disputed. C discussed a distinct fear context in 3:14–16 and Isa 8. No neutral-fact conflict.
- **Suffering for righteousness:** B refused to turn 3:12 into immunity from suffering; C 3:14 allowed blessed suffering for righteousness; D's suffering-to-vindication synthesis did not promise an easy life.
- **Retaliation/non-retaliation:** B consistently taught no evil-for-evil/insult-for-insult and blessing instead; no other audited lane contradicted this as a neutral fact.
- **God's will:** C kept `εἰ θέλοι τὸ θέλημα τοῦ θεοῦ` conditional and did not make the persecutor's evil morally good.
- **Baptism and spirits in prison:** D preserved multiple serious readings and did not present morphology as resolving them.
- **Project vs neutral:** A/B/D generally separated project synthesis from neutral facts. C's audited-head schema violations prevented clean mechanical enforcement until fixed.

**Terminology HOLD — Isaiah 8:** C's audited head called Isa 8:12–13 in 1 Pet 3:14–15 an “explicit quotation without an introductory formula”; Agent E's first pass uses “clear verbal reuse/adaptation.” Those need not be substantive contradiction, but integration should normalize the repository quotation/allusion vocabulary while separating observable verbal reuse from Christological inference.

## DISTRACTOR LEAKAGE

### System-level answer-position check

The integration baseline's `questions/__init__.py` randomizes question selection/order for competitive challenge pools but does not shuffle options. Repository code search in this pass found no option-shuffle implementation. Therefore audited lanes storing every correct answer at index 0 created a real answer-position leakage risk unless a separate option shuffler is introduced and tested before publication.

### A — content leakage suspects at audited SHA

- `ch3_hist_101` — correct answer was the only nuanced/nonabsolute option; distractors were universal claims or category error.
- `ch3_app_102` — anti-intimidation answer was morally obvious against grotesque alternatives such as forbidding outside help.
- `ch3_app_104` — correct answer was the only noncoercive/nonpsychological caricature.
- `ch3_app_108` — evidence-first method contrasted with three explicitly anti-evidence procedures.
- `ch3_disp_103` — long nuanced Gen 18/Peter distinction versus elementary false claims.

### B — BLOCKER AT AUDITED SHA: lane-wide index leakage

Every audited B item stored `correct=0`: `ch3_text_201–208`, `ch3_gr_201–212`, `ch3_ot_201–208`, `ch3_theol_201–204`, `ch3_disp_201`, `ch3_app_201–204` — 37/37 items.

Strong content cues included `ch3_text_208`, `ch3_ot_207`, `ch3_disp_201`, and `ch3_app_204`, where long/nuanced correct answers were contrasted with implausible or morally grotesque distractors.

### C — BLOCKER AT AUDITED SHA: dominant index leakage

All 22 non-text cards stored the correct answer at index 0: `ch3_gr_301–309`, `ch3_ot_301–304`, `ch3_disp_301`, `ch3_theol_301–304`, `ch3_app_301–304`. Including text items, 24/27 lane cards had index 0. Strong content cues included `ch3_ot_303`, `ch3_theol_301`, and `ch3_app_304`.

### D — BLOCKER AT AUDITED SHA: lane-wide index leakage

Every audited D item stored `correct=0`: `ch3_text_001–010`, `ch3_gr_001–015`, `ch3_int_001–005`, `ch3_disp_001–006`, `ch3_theol_001–005`, `ch3_app_001–004` — 45/45 items. Strong content cues included `ch3_int_003`, `ch3_disp_003`, `ch3_theol_003`, and `ch3_app_002`.

## SOURCE INSPECTION GAPS

Evidence-state ladder used by Agent E:

`URL_FOUND < METADATA_VERIFIED < ABSTRACT_INSPECTED < PARTIAL_TEXT_INSPECTED < FULL_RELEVANT_SECTION_INSPECTED`.

Aliases must never promote a source up this ladder.

### A — audited-head source-depth blockers/HOLDs

- **Treggiari, Roman Marriage:** OUP book page supported metadata, not the substantive variability claims in `ch3_hist_101` / high-confidence `ch3_hist_105`.
- **Balch, Let Wives Be Submissive:** Google Books catalog record was used for the exact household-code model in `ch3_hist_107`; catalog metadata is not the argument.
- **Horrell/Williams ICC vol. 2:** publisher page was used across intertext/history/disputed cards as if exact passage positions were established.
- **Davids NICNT:** Eerdmans publisher page was used for exact lexical/exegetical support in `ch3_gr_107`, `ch3_gr_113`, `ch3_disp_101`, `ch3_disp_106`, `ch3_app_105`, `ch3_app_107`; Agent E's independent receipt was publisher-description level.
- **Schreiner NAC:** B&H publisher page could identify the book but not by itself prove a passage position.

A's explicit inspected-conservative set for MacArthur/Piper was good practice; audited-head problems came from extending claim-specific weight to other weaker receipts.

### B — audited-head HOLDs

- **Gréaux 2009:** Agent E independently reached `ABSTRACT_INSPECTED`; broad thesis only, not consensus/full-text closure.
- **Christensen 2015:** Agent E independently reached `PARTIAL_TEXT_INSPECTED`; function/unifying-role claims remained interpretation.
- B's direct SBLGNT/MorphGNT/Ps 33 LXX comparison was materially stronger than those secondary function claims.

### C — audited-head source-depth BLOCKERS

- **Jobes 2022:** C pointed to a Google Books catalog; Agent E state was `BIBLIOGRAPHIC_INSPECTED`, yet audited cards used it for exact support in `ch3_gr_305`, `ch3_disp_301`, `ch3_theol_301–304`, `ch3_app_301–304`.
- **Achtemeier Hermeneia:** Agent E had `TABLE_OF_CONTENTS_INSPECTED`, not passage readback, yet the audited lane used it claim-specifically.
- **BDAG:** University of Chicago publisher page was not a lexical-entry receipt, yet audited cards used it to support exact lexical claims.
- **Rahlfs-Hanhart/Göttingen catalog:** edition metadata is not the Isa 8 text itself; actual LXX text must carry the wording claim.
- **Moyise 2005:** Agent E first-pass state was abstract/metadata; detailed attribution could not exceed verified scope.

### D — audited-head source-depth BLOCKERS/HOLDs

- **Schreiner NAC quorum:** D's audited test required `schreiner_1peter_nac` as independent project evidence even though its stored URL was a B&H publisher page. D notes said a public teaching cross-check existed; that actually inspected source needed its own receipt or the NAC passage needed direct inspection.
- **Westfall 1999:** IxTheo bibliographic record was used substantively in theology/disputed/application cards; bibliography is not chapter inspection.
- **Horrell/Williams ICC vol. 2:** publisher metadata was used for exact disputed-passage positions.
- **Pierce 2011:** publisher synopsis established subject, not every detailed reading attributed to the monograph.
- **Marcar 2017:** stored URL was an abstract page; broad flood/Urzeit-Endzeit thesis only.
- **Lei 2025:** publisher description supported reception-history relevance, not every exact exegetical conclusion.
- **Crawford 2016:** remained `ABSTRACT_INSPECTED`; audited D correctly avoided exclusive pledge certainty, which should remain the guardrail.
- **Davids/Elliott:** audited D notes correctly treated them as bibliographic/general controls rather than exact disputed-position proof.

## SAFE CANONICALIZATION PLAN

1. Canonicalize logical works, not evidence receipts; merged records retain `inspection_level`, passage scope, edition/format, and limitations.
2. Use `sblgnt` as the work-level Greek text ID and `morphgnt_1peter` as morphology; pin MorphGNT to `aaed91e57c8e4a8dc9a2383e129ca5e75fe6393d`.
3. Canonicalize Grudem 3:19 article to Fall 1986, 3–31; do not reintroduce 1987 as journal publication year.
4. Canonicalize Crawford bibliography to JTS 67.1 (2016), 23–37, but preserve abstract-level limitation until full text is read.
5. Treat Davids, Jobes, Schreiner, Horrell/Williams, Achtemeier, Pierce, Westfall, etc. as work-level aliases only; aliasing never upgrades a publisher/catalog record to `FULL_RELEVANT_SECTION_INSPECTED`.
6. Keep LXX witnesses edition-aware; “LXX” is not one source identity.
7. Keep 1 Enoch translation/edition identity explicit.
8. Resolve Elliott 2000/2001 only after verifying exact edition/reprint.
9. No shared registry mutation belongs in Agent E's branch; integrator applies only aliases actually needed after source-depth closure.

## INTEGRATION READINESS

The statuses below are valid **only for the audited start snapshot SHAs**. They are historical audit findings, not verdicts on later heads.

### A — **BLOCK at audited SHA**

**Exact audited SHA:** `aecdcb6fbf15648e23aad51ce65560e75bff3986`.

**Blockers:** claim-specific historical/exegetical use of Treggiari/Balch/Horrell-Williams/Davids exceeded independent inspection depth. Several distractors had low discrimination.

**Non-blocking HOLDs:** disputed `φόβος`, adornment, Sarah, `ἀσθενεστέρῳ σκεύει`, `κατὰ γνῶσιν`, and `ὁμοίως` were appropriately quarantined.

**Required before merge:** inspect/record relevant source sections or remove/downgrade claim-specific use; revise flagged distractors. Canonical metadata/ID taxonomy passed at that SHA.

### B — **BLOCK at audited SHA**

**Exact audited SHA:** `d321048fc59a865ba824d3ca68fe2605da0ea5c3`.

**Blocker:** 37/37 questions stored correct option at index 0 while the baseline did not shuffle options.

**Non-blocking HOLDs:** Gréaux abstract-level and Christensen partial-text function claims remained interpretation, not consensus.

**Required before merge:** eliminate/test option-index leak and improve strongest strawman distractors.

### C — **BLOCK at audited SHA**

**Exact audited SHA:** `820087cf00ecfa9a648329d5041ec8a85e2cba4f`.

**Blockers:** noncanonical `claim_type="intertext"`, noncanonical `claim_type="theology"`, noncanonical `position="pastoral"`; tests enforced invalid values; Jobes/Achtemeier/BDAG used beyond inspected depth; dominant answer-position/distractor leakage.

**Required before merge:** repair metadata/tests, source receipts, and distractor/answer-position leakage.

### D — **BLOCK at audited SHA**

**Exact audited SHA:** `95b6c972ff381d8e10158c417f2433139fdfc323`.

**Blockers:** `ch3_int_001–005` noncanonical namespace; publisher-only Schreiner treated as project quorum; Westfall/Horrell-Williams inspection gaps; 45/45 items at correct index 0.

**Non-blocking HOLDs:** D preserved competing spirits/`ἐπερώτημα`/baptism/`σαρκί...πνεύματι` readings and did not inflate Crawford into exclusive pledge certainty.

**Required before merge:** taxonomy/test fix, real inspected Schreiner receipt, close/downgrade claim-specific source gaps, remove/test answer-position leakage.

## FINAL HEAD RECHECK — FAIL-CLOSED OVERRIDE

The required end-of-audit fetch found that **all four target PR heads moved after the start snapshot**:

- A / PR #17: audited `aecdcb6fbf15648e23aad51ce65560e75bff3986` → final observed head `511dbc6b152538d452d21f53d938d6a4e050a9e9`
- B / PR #14: audited `d321048fc59a865ba824d3ca68fe2605da0ea5c3` → final observed head `ad562314f2d746c1fa585ebb2ab8ca25ee806bbc`
- C / PR #13: audited `820087cf00ecfa9a648329d5041ec8a85e2cba4f` → final observed head `4eee93c6001c283f9382ca02b2e52c05acad969c`
- D / PR #16: audited `95b6c972ff381d8e10158c417f2433139fdfc323` → final observed head `129a956663b622b8d3c52ebf7333ed38c94a2d1e`

Therefore the current integration-readiness state is:

- `A: STALE_AUDIT_REQUIRES_RERUN`
- `B: STALE_AUDIT_REQUIRES_RERUN`
- `C: STALE_AUDIT_REQUIRES_RERUN`
- `D: STALE_AUDIT_REQUIRES_RERUN`

The newer PR descriptions visibly claim that some identified issues were addressed, especially C's metadata/source/distractor findings and D's `ch3_int_*` taxonomy/distractor findings. **Those claims are not treated here as audited evidence.** Agent E did not inspect the new diffs against the new heads in this pass, and no PASS/PASS_WITH_HOLD/BLOCK verdict is transferred to them.

## Overall second-pass conclusion

At the exact heads actually inspected, the four lanes had concrete blockers. At the required end recheck, every lane had moved. Consequently **no current A/B/C/D head is certified by this audit**. The correct fail-closed next state is a fresh Agent E rerun against the four new exact SHAs above.

Do **not** declare Chapter 3 complete. Do **not** merge based on stale audit findings or lane-local green CI alone.