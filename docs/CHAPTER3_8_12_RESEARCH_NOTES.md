# 1 Peter 3:8–12 research notes — Agent B lane

## Scope and second-pass status

This note is bounded to the isolated Agent B lane for 1 Peter 3:8–12. The second pass hardens editorial quality, intertext terminology, source-inspection boundaries, and regression tests. It does **not** integrate a shared registry, chapter aggregate, runtime, UI, or publication path. All 37 lane questions remain `competitive=False`.

The evidence rule remains fail-closed: a URL or bibliographic match does not prove a claim, an abstract does not equal full-text inspection, and a broader scholarly proposal does not become a neutral textual fact merely because it is useful.

## Greek surface and morphology

Surface text is controlled by SBLGNT; morphology by MorphGNT's SBLGNT 1 Peter data.

- 3:8: `ὁμόφρονες`, `συμπαθεῖς`, `φιλάδελφοι`, `εὔσπλαγχνοι`, `ταπεινόφρονες` are tagged `A- ----NPM-`.
- 3:9: `ἀποδιδόντες` and `εὐλογοῦντες` are `V- -PAPNPM-`; `ἐκλήθητε` is `V- 2API-P--`; `κληρονομήσητε` is `V- 2AAS-P--`.
- 3:10–11: `παυσάτω`, `ἐκκλινάτω`, `ποιησάτω`, `ζητησάτω`, `διωξάτω` are `V- 3AAD-S--`.

Lexical controls are kept separate from morphology. `ὁμόφρων` supports concord/like-mindedness rather than identity on every opinion; `συμπαθής` shared feeling/sympathy; `φιλάδελφος` sibling love; `εὔσπλαγχνος` compassion/tender-heartedness; and `ταπεινόφρων` requires contextual control because older negative and biblical humble senses are both attested. Morphological identity never settles lexical identity.

## `εἰς τοῦτο ἐκλήθητε` — HOLD as contested syntax

Morphology is secure, but the attachment of `εἰς τοῦτο` is not reducible to the tag on `ἐκλήθητε`. The full Christensen article explicitly treats both a backward connection to the preceding conduct/blessing and a forward connection to the following `ἵνα` clause as readings with exegetical merit. Cambridge Greek Testament and Meyer likewise preserve the dispute. The lane therefore keeps `ch3_disp_201` at `confidence="contested"` rather than manufacturing certainty.

## Psalm 33 LXX / Psalm 34 MT-common-English terminology

The second pass adopts this formula consistently:

**1 Peter 3:10–12 = sustained quotation/adaptation of Psalm 33:13–17 LXX (Psalm 34:12–16 MT/common English numbering).**

It is deliberately **not** described as mechanically verbatim. Direct comparison preserves four observable adaptations/boundaries:

1. LXX 33:13 opens as a question (`τίς ἐστιν ἄνθρωπος...`) with `ἀγαπῶν`; 1 Pet 3:10 reshapes it as `ὁ γὰρ θέλων... ἀγαπᾶν...`.
2. LXX 33:14–15 uses second-person singular imperatives (`παῦσον`, `ἔκκλινον`, `ποίησον`, `ζήτησον`, `δίωξον`); 1 Peter uses third-person singular imperatives (`παυσάτω`, `ἐκκλινάτω`, `ποιησάτω`, `ζητησάτω`, `διωξάτω`).
3. LXX has `γλῶσσάν σου` and `χείλη σου`; 1 Pet 3:10 omits the possessive `σου` in both places.
4. 1 Pet 3:12 stops after the clause about the Lord's face against evildoers and does not reproduce the final LXX purpose clause `τοῦ ἐξολεθρεῦσαι ἐκ γῆς τὸ μνημόσυνον αὐτῶν`.

The omission in (4) is a textual observation. **No authorial motive for the omission is asserted by this lane.** Green and Christensen discuss possible motives; that discussion is not promoted to a closed question.

## Function of Psalm 34: textual fact vs scholarly interpretation

Two levels are kept distinct.

**Local text/discourse fact:** `γάρ` in `ὁ γὰρ θέλων` connects the quotation to the preceding exhortation as support/ground. `ch3_ot_208` is therefore a local `text` item with high confidence.

**Broader scholarly interpretation:** Green, Gréaux, and Christensen discuss the Psalm's ethical/paraenetic role and its relation to suffering and deliverance/vindication. Claims that extend beyond the immediate connective work of `γάρ` remain `interpretation`; the broader-function item `ch3_ot_207` is `confidence="medium"`. The lane does not turn a proposed all-letter structural role for Psalm 34 into a neutral fact.

## Source inspection level — second pass

- **Green, Tyndale Bulletin 41.2 (1990), 276–289:** the official 14-page PDF was inspected, including his direct treatment of 1 Pet 3:10–12 and the three moral themes of speech restraint, doing good/turning from evil, and seeking peace. Green can therefore support these local ethical-function claims. His stronger proposals about wider Psalm-33 influence remain scholarly interpretation rather than textual fact.
- **Gréaux, Review & Expositor 106.4 (2009), 603–613:** the publisher page confirms metadata but exposes only the abstract without subscription. Lane claims are limited to what that abstract explicitly states: the citation furthers exhortation, and the Psalm/letter share a suffering-and-deliverance horizon.
- **Christensen, JETS 58.2 (2015), 335–352:** the official ETS full PDF was inspected. It directly compares 1 Pet 3:10–12 with LXX Ps 33:13–17, discusses the opening adaptation, person changes and loss of possessive pronouns, treats both `εἰς τοῦτο` attachments, and develops a broader paraenetic/solidarity proposal while also warning against overextending a strict structural blueprint for the entire letter.

This inspection level is now encoded in `sources_8_12.py` so citation existence cannot silently masquerade as full-text closure.

## Editorial hardening

All text, Greek, intertext, theology/disputed, and application questions were re-read. Distractors based on unrelated themes or obvious nonsense were replaced with nearby textual, grammatical, lexical, intertextual, or application confusions. Examples include:

- virtue-list questions now substitute neighboring Christian virtues rather than unrelated social traits;
- Greek lexical questions now confuse adjacent 3:8 terms or plausible overextensions instead of physical/ritual categories;
- Psalm questions now contrast LXX/MT source confusions, person changes, omitted pronouns, and quotation boundaries rather than unrelated subject matter;
- theology/application options now test overreading, narrowing, retaliation, silence, or prosperity/immunity conclusions rather than absurd claims.

Regression tests enforce four unique non-empty normalized options, a valid answer index, metadata enums, source resolution, exact-question uniqueness, and a near-duplicate question threshold.

## HOLDs intentionally preserved

- `εἰς τοῦτο ἐκλήθητε`: exact best attachment remains contested.
- Ps 33:17 LXX final purpose-clause omission: the omission is certain; its authorial motive is not claimed.
- Gréaux: publisher abstract only; no claim beyond the abstract's stated scope is treated as full-text verified.
- Broader Psalm-34 influence across all of 1 Peter: scholarly interpretation, not neutral textual fact; the lane keeps the broader-function item at medium confidence.
- No additional thematic echo is promoted to explicit quotation without separate evidence.
