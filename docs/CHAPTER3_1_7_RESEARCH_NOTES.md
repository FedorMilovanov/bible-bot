# 1 Peter 3:1–7 — Agent A research notes

Status: **second-pass hardened lane slice only**. No Chapter 3 completion claim, no shared-registry integration, and no claim that unresolved exegetical HOLDs have been closed.

## Method

This lane applies the fail-closed discipline from `CONTENT_SOURCE_POLICY.md` and the Research control plane. Direct text, machine morphology, lexical/syntactic interpretation, social-history reconstruction, project interpretation, and pastoral application remain distinct layers.

- Greek, history reconstruction, project interpretation, disputed interpretation, and application remain `competitive=False` pending later ranking/review.
- A source URL or book listing does not count as inspected passage evidence.
- Source metadata in `questions/chapter3/sources_1_7.py` records the actual evidence status used in this lane.
- Bibliographic-only and publisher-TOC-only sources are retained for research control but are not used as claim evidence by question cards.
- An abstract-only source may support only the limited scope actually present in the abstract; it is not treated as full-text evidence.

## Second-pass editorial hardening

All 56 lane cards were reviewed again for question design and maintainability.

- Stable question IDs were preserved.
- Dense one-line helpers were replaced with reviewable helper functions and formatted card definitions.
- Distractors were rewritten toward the same semantic, morphological, historical, or pastoral category as the correct answer.
- Correct-answer position and length bias are guarded structurally rather than by brittle substring checks.
- The lane test normalizes Unicode/case/spacing for option uniqueness, rejects exact duplicate questions, and rejects extreme token-level near-duplicate questions.
- Canonical metadata is limited to the repository contract: `claim_type` in `text|greek|history|interpretation|application`, `confidence` in `high|medium|contested`, and `position` in `neutral|project`.

## Direct text and Greek boundaries

- 3:1 addresses wives toward **their own husbands**. `καὶ εἴ τινες` permits a case where **some** husbands disobey the word; it does not describe every marriage.
- 3:1–2 links the hoped-for “winning” of resistant husbands to observable `ἀναστροφή` and `ἄνευ λόγου`; this is not a doctrine of gospel-less salvation or a universal ban on speaking about faith.
- Both occurrences of `ὁμοίως` (3:1 and 3:7) are MorphGNT adverbs (`D- --------`); morphology does not decide the exact discourse force of the second occurrence.
- `ὑποτασσόμεναι` is present passive participle, nominative plural feminine (`V- -PPPNPF-`), not a finite imperative; its paraenetic force is contextual.
- `ἀναστροφῆς` is genitive singular of `ἀναστροφή`; the immediate context supports conduct/way of life, not a social-status reconstruction.
- `φόβῳ` is a dative of `φόβος`; morphology does not identify its referent.
- `κόσμος` is a noun whose adornment sense is controlled by the local list. The normative force of `οὐχ ... ἀλλʼ` remains interpretation.
- `πραέως` and `ἡσυχίου` are genitive singular neuter with `πνεύματος`; morphology does not encode a ban on truthful speech.
- `ἀσθενεστέρῳ` is comparative dative singular neuter with `σκεύει`; morphology does not encode the dimension of weakness.
- `συγκληρονόμοις` is adjective dative plural; morphology confirms the co-heir construction but not every theological implication drawn from it.
- `ἐγκόπτεσθαι` is present passive infinitive in `εἰς τὸ μὴ ...`.
- `κατὰ γνῶσιν` is grammatically “according to knowledge/with understanding”; the object/content of that knowledge is not expressed.

The lane test freezes all 23 selected MorphGNT rows for 3:1–7 and checks verse, surface, lemma, POS, and parse values.

## Sarah / LXX

**Genesis 18:12 LXX:** Sarah laughs within herself and calls Abraham `κύριός μου`. 1 Pet 3:6 names Sarah and says she was `κύριον αὐτὸν καλοῦσα`. Decision: a clear named narrative reference/verbal allusion, not a formal quotation. Genesis 18:12 does not contain `ὑπακούω` and is not a scene in which Abraham commands Sarah to use the title.

**Proverbs 3:25 LXX:** `οὐ φοβηθήσῃ πτόησιν` closely parallels 1 Pet 3:6 `μὴ φοβούμεναι μηδεμίαν πτόησιν`. Decision: strong LXX verbal background/allusion, not formal quotation; no citation formula appears and the wording is adapted.

## Social history

The lane rejects a single caricature called “Greco-Roman marriage.”

- Aristotle supplies a normative witness to structured household relations; it is not a census of Christian households.
- Plutarch includes real household and religious asymmetry, including advice that a wife share her husband's gods, while the same work also uses language of marital commonality and goodwill.
- Musonius Rufus can emphasize common life, companionship, and mutual devotion, demonstrating that ancient marriage discourse was not monolithic.
- Horrell 2016 was inspected at abstract level only. It supports the limited methodological point that mixed marriage and identity are relevant to discussion of 1 Peter 3 and that modern reconstructions require scrutiny; it is not used as full-text proof of detailed social claims.
- Treggiari and Balch are retained as bibliographic controls only in this lane and do not support question-card claims.

The text directly envisages at least some wives whose husbands disobey the word, but this lane does **not** reconstruct one legal class, property regime, abuse profile, or mixed-marriage status for all female addressees.

## Exegetical / project controls actually inspected

Passage-level controls used substantively in cards include SBLGNT, MorphGNT, LXX Genesis/Proverbs, NET notes, van Rensburg's full-text Sarah study, MacArthur's passage exposition, Piper's 1 Peter 3 material, Sam Storms/TGC on 1 Peter 3, Steven Cole on 1 Peter 3:1–6, and the Cambridge Bible commentary on 1 Peter 3.

The following remain research-catalog controls but **not passage-level inspected evidence in this lane**:

- Peter H. Davids, *The First Epistle of Peter* — bibliographic/product page only.
- Thomas R. Schreiner, *1, 2 Peter, Jude* — bibliographic/product page only.
- Horrell/Williams, *1 Peter*, ICC vol. 2 — publisher metadata/TOC only.
- Susan Treggiari, *Roman Marriage* — bibliographic page only.
- David Balch, *Let Wives Be Submissive* — bibliographic page only.

No question card relies on those bibliographic-only / TOC-only records as proof.

## Project synthesis

The course/project layer reads 3:1,5 as wife-to-own-husband submission while preserving the passage's own controls: the scope is own husbands rather than all women/all men; 3:6 joins doing good with freedom from `πτόησις`; 3:7 requires husbands to live with understanding and honor; the wife is a co-heir of the grace of life; and the husband's conduct is linked to hindered prayer.

These project conclusions remain explicitly `position="project"` and noncompetitive. Passage-level project cards require at least two inspected conservative/evangelical witnesses rather than merely two bibliographic entries.

## Explicit contested boundaries / HOLD

1. `φόβος` in 3:2: God-oriented fear vs husband/reverence/relational fear. Cole acknowledges both possibilities and prefers God-oriented fear; Cambridge relates the phrase to reverence toward the husband. The lane does not collapse the dispute.
2. External adornment: exact normative force of the `οὐχ ... ἀλλʼ` contrast remains open; the lane does not promote a strict-prohibition or comparative-priority reading to neutral lexical fact.
3. Sarah: the exact theological force of Peter's reuse of Gen 18:12 remains open even though the verbal/narrative connection is clear.
4. `πτόησις`: Prov 3:25 LXX is strong verbal background/allusion, not a formal quotation.
5. `κατὰ γνῶσιν`: content/object of the knowledge is unstated and requires interpretation.
6. `ἀσθενεστέρῳ σκεύει`: physical, social/situational, or combined readings remain possible; intellectual, moral, or spiritual inferiority is not encoded by the morphology.
7. `ὁμοίως` in 3:7: a corresponding responsibility is clear at discourse level, but the degree and kind of parallel with the preceding submission material remain disputed.
8. Historical status: no universal legal/social profile is reconstructed for every wife in the audience.

These are substantive HOLDs for later ranking/publication decisions. Second-pass hardening improves editorial quality and evidence control; it does not make these questions disappear.

## Exact-head verification

The second-pass content and editorial tests passed on `b6f36f2bee64fc56f7decd3c1b06d13225ac5dc4`, together with repository CI, Security Audit, and CodeQL Stacked PR. This records a verified stage closure for that SHA only; it is not a merge-readiness or publication-completeness claim.
