# 1 Peter 3:13-17 — Agent C evidence notes

Status: isolated foundation lane; **not production-reviewed and not ranking-eligible**.

## Canonical metadata contract

This lane uses only the repository enums from `AGENTS.md`:

- `claim_type`: `text`, `greek`, `history`, `interpretation`, `application`;
- `confidence`: `high`, `medium`, `contested`;
- `position`: `neutral`, `project`.

There is no `intertext`, `theology`, or `pastoral` enum value. Observable intertextual comparisons are `text`; classification, function, syntax, and theological inference are `interpretation`; course/pastoral use is `application` with `position=project`.

## Epistemic layers

Keep separate: lexical fact, morphology, syntax, intertextual observation, intertextual/theological inference, and modern application. In particular, `ἀπολογία` does **not** by itself establish classical, evidential, cumulative-case, presuppositional, or any other single modern apologetics methodology.

## SBLGNT / MorphGNT checkpoints

MorphGNT (`81-1Pe-morphgnt.txt`) gives:

- `ἁγιάσατε` — `V- 2AAD-P--`: aorist active imperative, 2nd plural.
- `κύριον` — `N- ----ASM-`; `τὸν Χριστόν` — article + noun, accusative masculine singular.
- `ἕτοιμοι` — `A- ----NPM-`; `ἀεί` — adverb.
- `ἀπολογίαν` — `N- ----ASF-`.
- `αἰτοῦντι` — `V- -PAPDSM-`.
- `λόγον` — `N- ----ASM-`; `ἐλπίδος` — `N- ----GSF-`.
- `πραΰτητος` — `N- ----GSF-`; `φόβου` — `N- ----GSM-`.
- `συνείδησιν` — `N- ----ASF-`; `ἔχοντες` — `V- -PAPNPM-`; `ἀγαθήν` — `A- ----ASF-`.
- `θέλοι` — `V- 3PAO-S--`: present active optative, 3rd singular.

Morphology establishes forms. It does not by itself settle predicative vs appositional labeling of `κύριον`, the classification of Isaiah reuse, lexical scope, or the Christological force of that reuse.

## Isaiah 8:12-13 and 1 Peter 3:14-15

### Observable layer

- Isa 8:12 LXX and 1 Pet 3:14 share the fear/trouble sequence (`φόβος` / `φοβηθῆτε`, `ταραχθῆτε`).
- LXX Isa 8:13: `κύριον αὐτὸν ἁγιάσατε`.
- 1 Pet 3:15: `κύριον δὲ τὸν Χριστὸν ἁγιάσατε ἐν ταῖς καρδίαις ὑμῶν`.

These comparisons are encoded as `claim_type=text`, because they are observations about the wording of the two texts.

### Interpretation layer

Van Rensburg and Moyise argue that the cumulative wording constitutes an explicit quotation / clear verbal reuse despite the absence of an introductory formula. That classification is encoded as `claim_type=interpretation`, `confidence=medium`, not morphology.

The bibliographic issue is **Scriptura vol. 80 (2002), pp. 275-286**. The journal platform also displays a later electronic-platform publication date in 2013; that platform date must not replace the issue year in the citation.

Moyise (2005) is a second peer-reviewed treatment of the same case, but it is **not author-independent**, because Moyise co-authored the 2002 study.

### Independent exegetical control and Christological inference

For the relevant 1 Pet 3:15 passage, G. W. Blenkin, *The First Epistle General of Peter*, Cambridge Greek Testament for Schools and Colleges (CUP, 1914), was independently inspected. Blenkin discusses the Isaiah wording, the predicate/appositional issue around `κύριον`, and the transfer of OT Lord-language to Christ.

Accordingly, `ch3_theol_301` remains a **medium-confidence project interpretation**: the textual replacement/adaptation is observable, while the exact Christological significance is an exegetical/theological inference. It is not promoted to `high` merely because the morphology is secure.

## Source-depth discipline

Source presence is not the same thing as inspected claim evidence.

- `bdag_3` remains `bibliographic_control_only`. No lane card cites it as substantive evidence because the relevant BDAG entries were not directly inspected in this pass.
- Jobes (2022) and Achtemeier (1996) remain `metadata_preview_only_not_claim_evidence` and are not used by lane cards as inspected claim evidence.
- The Goettingen Rahlfs-Hanhart record remains `edition_metadata_inspected`; it documents edition metadata but is not used as a card-level textual witness. The actual LXX wording is sourced through `septuagint_bible`.
- G. Abbott-Smith, *A Manual Greek Lexicon of the New Testament* (1922), entry `ἀπολογία`, was directly inspected in a public-domain digital transcription. The bounded scope is **only the headword `ἀπολογία`**: the entry characterizes a speech in defence and explicitly cites 1 Pet 3:15.
- No Abbott-Smith claim is made for uninspected `πραΰτης` or `φόβος` entries. Greek cards for those forms remain MorphGNT-backed morphology only.

Fail-closed rule for this lane: any local source marked bibliographic/catalog/metadata/preview-only or abstract-only is non-claim evidence unless a future explicit scoped mechanism is added and tested. Current cards use none of those statuses.

**HOLD:** do not promote the non-trivial Christological interpretation above `medium` on the basis of Jobes/Achtemeier metadata. A future promotion would require inspection of relevant independent modern exegetical pages or another equivalently strong independent control.

## ἀπολογία and apologetics

- Abbott-Smith's inspected `ἀπολογία` entry supports the bounded lexical sense of a defensive speech and cites 1 Pet 3:15.
- `παντὶ τῷ αἰτοῦντι` grammatically refers to every asker in the sentence; it does not itself specify a courtroom-only setting.
- `λόγον περὶ τῆς ἐν ὑμῖν ἐλπίδος` keeps the requested account focused on the believers' hope.
- `μετὰ πραΰτητος καὶ φόβου` plus `συνείδησιν ἔχοντες ἀγαθήν` place answer, manner, and integrity together in the paragraph.

Modern apologetic methods may appeal to the passage as a mandate for prepared witness. The inspected lexical entry plus immediate syntax do not choose one complete later methodology; cards use plausible alternatives such as courtroom-only, philosophical-proof-only, or testimony-only readings rather than absurd distractors.

## 3:17 — suffering and God's will

`εἰ θέλοι τὸ θέλημα τοῦ θεοῦ` contains optative `θέλοι` in a conditional frame. The immediate contrast says suffering while doing good is preferable to suffering while doing evil if that divine-will condition obtains.

Fail-closed boundary: do not infer that every evil act by a persecutor is therefore morally approved by God, and do not turn the verse into a command to seek suffering.

## Application confidence decisions

Application confidence was reviewed item by item rather than mechanically lowered:

- `ch3_app_301` — `medium`: modern practice is inferred from readiness / every asker.
- `ch3_app_302` — `medium`: course methodology is downstream from the inspected lexical entry plus syntax.
- `ch3_app_303` — `high`: the application nearly restates the explicit textual sequence of answer + gentleness/reverence + good conscience; it remains noncompetitive because it is still an application item.
- `ch3_app_304` — `medium`: the text supplies the good/evil contrast, while «do not seek suffering» is pastoral judgment downstream from it.

## Answer-position integrity

The 27-card lane now deliberately avoids answer-position leakage. Correct-option counts are **7 / 7 / 7 / 6** across positions 0-3 (order-independent); all four positions are used, the sequence is not a simple repeating cycle, and no position occurs more than twice consecutively. Only option order changed for this control; the intended correct proposition of each card did not change.

## Distractor and test controls

All lane cards use plausible confusions of the same domain: text vs nearby context, morphology alternatives, thematic echo vs quotation/reuse, Isaiah wording vs Petrine adaptation, morphology vs interpretation, courtroom-only vs broader defense, and human fear vs reverent manner.

Regression tests enforce canonical metadata enums, unique IDs, four unique options, valid `correct`, balanced answer-position counts, no runs longer than two identical correct positions, source resolution, generic exclusion of non-claim-evidence statuses, noncompetitive status, MorphGNT markers, observable-vs-interpretive Isaiah layering, item-specific application confidence, and a small banned set covering the previously identified absurd distractors.
