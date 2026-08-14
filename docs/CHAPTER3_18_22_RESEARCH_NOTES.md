# 1 Peter 3:18-22 — research notes

Start verified: `agent/ch3-d1` originated from `9eefbae4cf91d178e9f488e695df9264478197c0`. No production/root wiring.

## Evidence boundaries

**TEXT** = SBLGNT wording only. **GREEK** = SBLGNT + MorphGNT; parsing cannot decide disputed referents/theology. **INTERPRETATION** = competing readings explicit, `competitive=False`. **PROJECT** = evangelical synthesis explicitly `position="project"`.

Pre-publication taxonomy: direct text `ch3_text_*`, Greek `ch3_gr_*`, disputed `ch3_disp_*`, OT/Second Temple `ch3_ot_*`, theology `ch3_theol_*`, application `ch3_app_*`. Stable foundation IDs `ch3_gr_001-006` and `ch3_disp_001-004` remain unchanged.

All 45 cards remain `competitive=False`.

## Answer-position hardening

Correct answers are no longer constructed as first-option answers. Each bank has explicit per-ID answer positions and inserts the correct option at that position.

Combined lane order `TEXT + GREEK + DISPUTED + OT + THEOLOGY + APPLICATION` is intentionally non-cyclic and has:

- position 0: 12 cards;
- position 1: 11 cards;
- position 2: 11 cards;
- position 3: 11 cards;
- no run of three equal correct positions.

Regression tests enforce the exact `[11, 11, 11, 12]` sorted distribution, all four positions, no three-equal run, and rejection of a simple shifted `0-1-2-3` cycle.

## MorphGNT control

Reviewed against `81-1Pe-morphgnt.txt`:
`ἅπαξ D- --------`; `ἔπαθεν 3AAI-S--`; `προσαγάγῃ 3AAS-S--`; `θανατωθεὶς -APPNSM-`; `ζῳοποιηθεὶς -APPNSM-`; `ᾧ RR ----DSN-`; `πνεύμασιν N- ----DPN-`; `πορευθεὶς -APPNSM-`; `ἐκήρυξεν 3AAI-S--`; `ἀπειθήσασίν -AAPDPM-`; `διεσώθησαν 3API-P--`; `ἀντίτυπον A- ----NSN-`; `σῴζει 3PAI-S--`; `ἐπερώτημα N- ----NSN-`; `ὑποταγέντων -APPGPM-`.

Greek cards now cite only SBLGNT + MorphGNT. Secondary commentaries are not used to make morphology claims.

## 3:18 textual precision

SBLGNT begins the Christ-clause with `Χριστὸς ἅπαξ περὶ ἁμαρτιῶν ἔπαθεν`. Here **`ἔπαθεν` = “suffered”**. An English rendering or exposition using “died” must not be retrojected into the Greek textual claim.

The later `θανατωθεὶς μὲν σαρκί / ζῳοποιηθεὶς δὲ πνεύματι` is a separate textual unit in the verse. No manuscript-distribution claim is made because no critical apparatus was inspected in this lane.

The project substitution/reconciliation synthesis is based on the clause complex (`περὶ ἁμαρτιῶν`, `δίκαιος ὑπὲρ ἀδίκων`, `ἵνα ... προσαγάγῃ τῷ θεῷ`), not on claiming that one preposition proves a complete atonement theory.

## 3:19-20 reading map

Retained families:

1. **fallen spirits / Watchers / victory proclamation**;
2. **Christ through Noah**;
3. **descensus / human-dead reception**.

Morphology of `πνεύμασιν`, the aorist `ἐκήρυξεν`, `ἐν ᾧ`, and `πορευθεὶς` does not choose among these families.

Evidence is now depth-bounded:

- MacArthur GTY 3:18-20: relevant sermon section inspected; supports his fallen-spirit/victory reading as a position, not morphology.
- Sam Storms/TGC 3:18-22: relevant passage section inspected; independently supplies an evangelical fallen-spirit/vindication reading.
- Grudem, *Trinity Journal* 7.2 (Fall 1986), 3-31: relevant sections of the author-hosted PDF inspected; supports his Christ-through-Noah reading and comparison with alternatives. This lane does not claim an end-to-end full-article audit.
- Lei: publisher synopsis only; may support the historical importance/re-evaluation of descensus reception, not the identity of `πνεύματα` as a lexical fact.

## Genesis 6 / 1 Enoch

LXX Genesis 6 was inspected as primary OT background. It is not a lexical definition of `πνεύμασιν`.

The Charles translation of 1 Enoch 10-14 remains a **bounded primary witness in translation**, not a critical textual edition. The inspected translation gives Watchers confinement/judgment/message motifs. It cannot prove direct literary dependence by 1 Peter.

Pierce is now explicitly `publisher_abstract_inspected`: the Mohr Siebeck synopsis supports only that his monograph studies 1 Peter 3:18-22 against Watchers/1 Enoch and related early-Jewish sin-and-punishment traditions. No detailed Pierce conclusion is attributed beyond the synopsis.

Marcar is `publisher_abstract_inspected`: the Cambridge abstract supports her broader Noah/flood Urzeit-Endzeit framing across 1 Peter 3-4. Detailed uninspected article arguments are not attributed.

Grindheim is likewise bounded at publisher/journal abstract level and is not currently required by a card.

## 3:20-21 and `ἐπερώτημα`

Keep the whole sequence: `δι’ ὕδατος`; `ἀντίτυπον ... βάπτισμα ... νῦν σῴζει`; `οὐ σαρκὸς ἀπόθεσις ῥύπου`; `ἀλλὰ συνειδήσεως ἀγαθῆς ἐπερώτημα εἰς θεόν`; `δι’ ἀναστάσεως Ἰησοῦ Χριστοῦ`.

`ἐπερώτημα` parsing as noun nom. neut. sg. is morphology. Translation/history remains disputed:

- LSJ entry: relevant lexical section inspected;
- Arichea/Nida UBS 1 Pet 3:21: relevant passage section inspected and preserves question/answer, appeal/request, contractual/pledge and relationship possibilities;
- Crawford, JTS 67.1 (2016), 23-37: **publisher abstract inspected, full article not inspected**. The abstract reports his contractual/pledge and early-baptismal/confession argument; it does not establish an exclusive lexical meaning.

Appeal/request, pledge/stipulation, and confession/response-related families therefore remain separate.

The baptismal-efficacy map retains major families without owner-level closure. Inspected GTY and Storms passage material supplies evangelical readings; UBS/Crawford provide bounded lexical/translation controls. Westfall is no longer used substantively because only title/page metadata was inspected.

## 3:22

The direct text states Christ's heavenly session and the subjection of angels, authorities, and powers. Inspected GTY and Storms passage sections support the triumph/vindication reading, but 3:22 is not used as a shortcut that identifies the imprisoned spirits of 3:19.

## Generic source-inspection contract

Every local `SOURCE_CATALOG` entry now has:

- `inspection_scope`;
- `claim_limit`.

Allowed states are: `primary_text_inspected`, `full_relevant_text_inspected`, `relevant_section_inspected`, `full_article_inspected`, `publisher_abstract_inspected`, `metadata_only`, `bibliographic_only`.

`metadata_only` and `bibliographic_only` are card-ineligible by regression test. `publisher_abstract_inspected` may remain on a card only for a thesis actually exposed by the abstract/synopsis and is bounded by `claim_limit`.

### Card-eligible inspected controls

- SBLGNT — `primary_text_inspected`;
- MorphGNT 1 Peter — `primary_text_inspected`;
- LXX Genesis 6 — `primary_text_inspected`;
- 1 Enoch 10-14 in Charles translation — `primary_text_inspected`, bounded translation witness;
- GTY Parts 1-3 — `relevant_section_inspected`;
- Sam Storms/TGC 1 Peter 3:18-22 — `relevant_section_inspected`;
- Grudem article — `relevant_section_inspected`;
- UBS Handbook 3:21 — `relevant_section_inspected`;
- LSJ `ἐπερώτημα` — `relevant_section_inspected`;
- Crawford — `publisher_abstract_inspected`;
- Pierce — `publisher_abstract_inspected`;
- Marcar — `publisher_abstract_inspected`;
- Lei — `publisher_abstract_inspected`;
- Grindheim — `publisher_abstract_inspected` but currently not needed by cards.

### Catalog controls removed from card evidence

- Schreiner NAC — `bibliographic_only`; product page is not passage inspection. Removed from all cards and from project quorum.
- Horrell/Williams ICC — `metadata_only`; publisher description/TOC is not commentary-page inspection. Removed from all cards.
- Westfall — `metadata_only`; title and pp. **106-135** verified, chapter argument not inspected. Removed from all cards; there is no metadata HOLD.
- Davids NICNT — `bibliographic_only`; removed/not used for exact positions.
- Elliott AYB — `bibliographic_only`; removed/not used for exact positions.

The independent evangelical project quorum is now **MacArthur/GTY + Sam Storms/TGC**, both marked `project_passage_witness=True` only because relevant 1 Peter 3:18-22 material was actually inspected. Tests require at least two such passage-level inspected witnesses on every `position="project"` item, including application cards.

## HOLD

1. `HOLD-PROJECT-SPIRITS`: integration owner decides whether to adopt fallen-spirit/Watchers + victory proclamation.
2. `HOLD-EPEROTEMA-TRANSLATION`: no course-wide Russian standard (`просьба` / `обет` / `исповедание/ответ`).
3. `HOLD-BAPTISM-SYSTEMATICS`: no denominationally precise regeneration formula without owner decision.
4. `HOLD-RANKING`: every lane item remains `competitive=False`.
5. `HOLD-PRODUCTION-WIRING`: no root registry/aggregate/production mutation.
