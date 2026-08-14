# 1 Peter 3 Source Audit — Agent E

**Scope:** independent source/evidence control plane for 1 Peter 3:1–22.  
**Baseline:** `9eefbae4cf91d178e9f488e695df9264478197c0`.  
**Lane rule:** this audit does not edit Chapter-3 question banks, shared registry/runtime, reviewed wiring, or CI.

## Governing rule: fail closed

This audit applies the repository and Research control-plane constraints literally:

- Stage closure is not publication authority.
- A located source is not a proved claim.
- A live URL is not an inspected source.
- An abstract is not full-text evidence.
- One commentator is not consensus.
- Morphology is not exegesis.
- Historical plausibility is not a statement made by the biblical text.

`questions/chapter3/sources_crosscutting.py` is the source catalog for this lane: every source records category, inspection level, exact scope, and limitation. `data/chapter3-evidence-matrix-agent-e.json` is the machine-readable claim matrix and carries claim status, source IDs, conflict map, DO NOT CLAIM guardrails, source gaps, and metadata corrections. The Python module loads that matrix so tests validate one evidence state rather than two duplicated copies.

## Source map

The sweep contains **36 classified sources**:

- **9 primary**: SBLGNT, pinned MorphGNT, Rahlfs-Hanhart Genesis/Psalms/Isaiah, 1 Enoch Watchers material, Plutarch, Musonius Rufus.
- **7 peer-reviewed journal controls**: Janse Van Rensburg; Rambiert-Kwaśniewska; Le Roux; Christensen (Psalm 34); Gréaux; Moyise; Crawford. Items marked abstract/partial-text are not allowed to prove detailed claims by themselves.
- **8 academic-control/reference works**: Johnson Hodge and Christensen (household-code debate) are conservatively parked here because this sweep did not independently close their peer-review status; plus Balch, Pierce, Arichea/Nida, Michaels, Achtemeier, Elliott.
- **12 conservative/exegetical witnesses**: six passage-specific MacArthur/GTY controls plus Grudem (1986), Schreiner, Jobes, Davids, Keener, and revised Grudem commentary.

The categories are intentionally not treated as equal kinds of evidence. A primary textual witness answers a different question from a social-history monograph or confessional commentary.

## High-confidence findings

### 3:1–7

MorphGNT confirms the major forms without turning them into social conclusions: `ὑποτασσόμεναι` (3:1, 5), Sarah's `ὑπήκουσεν` / `κύριον ... καλοῦσα` (3:6), and in 3:7 `ἀσθενεστέρῳ σκεύει`, `τιμήν`, and `συγκληρονόμοις`. Genesis 18:12 LXX supplies a clear textual/narrative anchor for Sarah calling Abraham “my lord.” The relationship is best labeled a **clear allusion/reference**, not a formal quotation with citation formula.

Ancient comparison must remain plural. Plutarch's *Conjugalia Praecepta* preserves asymmetrical/subordination assumptions together with obligations of respect/consideration; Musonius Rufus can idealize common life, companionship, mutual love, and reciprocal devotion. These sources make a one-line “the Greco-Roman marriage model was X” reconstruction unsafe.

The text envisages **at least some** wives whose husbands “disobey the word.” That supports a mixed-belief-marriage question, but not the universal claim that every wife in 3:1–6 had a pagan husband. Modern scholars also differ over whether Peter's household strategy is primarily accommodation/apologetic respectability, subversion, or a mixed strategy.

### 3:8–12 and Psalm 34

Direct comparison shows that 1 Pet 3:10–12 is a sustained quotation/adaptation of **Psalm 33:13–17 LXX** (= Psalm 34:12–16 in common English/MT numbering). Peter preserves the dense lexical sequence but adapts person/grammar and does not include the LXX clause about cutting off remembrance from the earth. “Verbatim quotation” is therefore too strong; “sustained quotation/adaptation” is safer.

The claims that Psalm 34 performs a particular *unifying* or *suffering-and-deliverance* function are plausible scholarly theses (Christensen; Gréaux) but are not elevated here to neutral fact because the full arguments were not all inspected.

### 3:13–17 and Isaiah 8

The LXX/SBLGNT comparison is strong: Isaiah 8:12–13 has `τὸν ... φόβον αὐτοῦ οὐ μὴ φοβηθῆτε οὐδὲ μὴ ταραχθῆτε` followed by `κύριον αὐτὸν ἁγιάσατε`; 1 Peter 3:14–15 reuses the fear/trouble language and writes `κύριον δὲ τὸν Χριστὸν ἁγιάσατε`. This is **clear verbal reuse/adaptation**, not merely a thematic background.

The textual move is Christologically significant because `τὸν Χριστόν` stands with `κύριον` in the Isaiah-shaped exhortation. A course may explain that significance, but it should distinguish the observable intertext from downstream systematic conclusions. `ἁγιάσατε` is an aorist active imperative 2nd plural; `ἀπολογίαν` is an accusative singular noun. Neither parsing result, by itself, defines a complete doctrine of apologetics.

### 3:18–22

A key correction against future false certainty: the inspected SBLGNT/MorphGNT at 3:18 reads **`ἔπαθεν`** (“suffered”), not a Greek verb meaning “died.” An English exposition may quote a translation with “died”; that wording must not be retrojected into the Greek source. A separate critical-apparatus check is still needed before making claims about the distribution/history of textual variants.

The `σαρκί ... πνεύματι` contrast is not resolved by morphology. Likewise 3:19 supplies `πνεύμασιν`, `πορευθεὶς`, and `ἐκήρυξεν`, but those forms do not identify the spirits, date the proclamation, or specify its content.

The Watchers tradition is real background evidence: 1 Enoch 10 links Noah-era judgment with rebellious heavenly beings bound in darkness awaiting judgment. Genesis 6 supplies the biblical Noah/“sons of God” setting. Chad Pierce's monograph confirms that modern discussion must reckon with multiple, conflated early Jewish sin/punishment traditions. **None of this proves direct literary dependence of 1 Peter on 1 Enoch.**

Conservative witnesses materially disagree. Grudem's 1986 article defends Christ preaching through Noah to humans. MacArthur and Schreiner prefer a victory/judgment proclamation to demonic spirits associated with the Noah/Genesis-6 complex; Schreiner publicly treats the issue with explicit humility. The course must present these as competing serious readings, not manufacture conservative consensus.

At 3:21 MorphGNT confirms `ἀντίτυπον`, `βάπτισμα`, `σῴζει`, and `ἐπερώτημα`; it cannot decide the sacramental theology. The UBS translator handbook documents question/answer, appeal/request, and contractual pledge options for `ἐπερώτημα`, plus ambiguity in the genitive `συνειδήσεως ἀγαθῆς`. Crawford's JTS abstract favors the pledge/contract line from early reception and papyrological comparison, but an abstract is not permission to declare that sense lexically certain.

## DO NOT CLAIM

1. Do not say every addressed wife had an unbelieving husband; 3:1 says “even if some.”
2. Do not say 3:1–7 commands a wife to remain in or endure domestic violence; that is neither a direct textual fact nor an acceptable pastoral shortcut.
3. Do not define `ἀσθενεστέρῳ σκεύει` as one settled kind of weakness by morphology alone.
4. Do not turn one Greco-Roman author or one household-code model into universal first-century social reality.
5. Do not call 3:10–12 mechanically verbatim Psalm 34; it is a sustained quotation/adaptation.
6. Do not downgrade Isaiah 8:12–13 to a vague thematic parallel; the verbal reuse is strong.
7. Do not say the SBLGNT Greek of 3:18 reads “died”; it reads `ἔπαθεν`.
8. Do not use morphology to settle `σαρκί ... πνεύματι`.
9. Do not say “spirits in prison” certainly means fallen angels, human dead, or Noah's contemporaries.
10. Do not make `ἐκήρυξεν` itself prove either evangelistic preaching or only condemnation/victory.
11. Do not claim direct dependence on 1 Enoch merely from parallel Watchers/Noah motifs.
12. Do not say `ἐπερώτημα` lexically means only “pledge” or only “appeal.”
13. Do not make “baptism now saves you” prove or disprove baptismal regeneration by lexical fiat.
14. Do not call one conservative expositor “the conservative view”; Grudem, MacArthur, Schreiner and others diverge.
15. Do not reconstruct empire-wide official persecution from chapter 3 alone; distinguish slander/hostility/suffering from a specific legal-persecution model.

## CONFLICT MAP

| Passage | Issue | Materially different serious options |
|---|---|---|
| 3:1–7 | Household-code strategy | accommodation/apologetic respectability; subversive/revolutionary subordination; mixed/nuanced models |
| 3:7 | `ἀσθενεστέρῳ σκεύει` | physical weakness; social vulnerability/status; combined/metaphorical accounts |
| 3:14–15 | Isaiah 8 reuse | clear verbal reuse is secure; the exact scale/formulation of the Christological conclusion requires further argument |
| 3:18 | `σαρκί ... πνεύματι` | spheres/aspects; human-spirit accounts; Holy-Spirit accounts; additional refinements |
| 3:19–20 | spirits / timing / proclamation | Grudem: preincarnate Christ through Noah to humans; MacArthur/Schreiner: post-resurrection victory to demonic spirits; descent/postmortem readings in wider tradition |
| 3:21 | `ἐπερώτημα` | question/answer; appeal/request; pledge/contract; subjective/objective genitive variants |
| 3:21 | baptism and salvation | sacramental/efficacious readings; non-regenerative symbolic/union readings; mediating accounts |

The conflict map is a publication guardrail, not an instruction to rank the views. Ranking requires a separate, source-complete argument.

## Verified metadata corrections

### Grudem 3:19–20

The existing Chapter-3 source title labels the Grudem item “(1987).” Bibliographic verification shows the journal publication as:

> Wayne A. Grudem, “Christ Preaching Through Noah: 1 Peter 3:19-20 in the Light of Dominant Themes in Jewish Literature,” *Trinity Journal* NS 7.2 (Fall **1986**): 3–31.

The unusual source of the mismatch is visible: Grudem's own résumé places the entry under a “1987” heading while the citation on that same résumé says “Trinity Journal 7:2 (Fall 1986) 3-31.” Trinity Journal/Galaxie metadata independently confirms Fall 1986. Agent E **did not edit** `questions/chapter3/sources.py`; the integrator should correct the display metadata later.

### Crawford 3:21

The existing `JTS 67.1 (2016), 23-37` metadata is substantively correct. It can be enriched to: *Journal of Theological Studies* 67.1 (April 2016): 23–37, DOI `10.1093/jts/flw085`; online publication 6 September 2016. Again, Agent E made no registry mutation.

## HOLD list

- **Direct literary dependence on 1 Enoch** — HOLD. Tradition/background is real; dependence is not proved by the inspected material.
- **A single resolved identity for the spirits in 3:19** — CONTESTED/HOLD for factual wording.
- **A single resolved time/content of Christ's proclamation** — CONTESTED/HOLD for factual wording.
- **One lexical meaning of `ἐπερώτημα`** — CONTESTED; exclusive “pledge”/“appeal” claims are rejected overclaims.
- **Baptismal regeneration proved/disproved by the word `σῴζει` or `βάπτισμα`** — HOLD as lexical argument.
- **Exact force of `σαρκί ... πνεύματι`** — CONTESTED.
- **Universal pagan-husband model for 3:1–6** — HOLD; the text says “some.”
- **Universal legal/social status reconstruction for wives in Asia Minor** — HOLD pending more location-specific primary legal evidence.
- **Empire-wide official persecution as the chapter's historical setting** — HOLD unless independently sourced passage-by-passage.
- **Manuscript-history claim for 3:18 `ἔπαθεν` vs alternatives** — HOLD until critical apparatus is inspected.

## SOURCE GAPS

1. Directly inspect the relevant 3:1–22 pages in Jobes (2nd ed., 2022), not just bibliographic metadata.
2. Directly inspect Davids NICNT and Michaels WBC on 3:18–22 before assigning their exact positions.
3. Read Crawford's full JTS article before using the papyri/early-reception argument to rank “pledge.”
4. Read Moyise's full article before attributing detailed historical/literary conclusions beyond the verified case-study scope.
5. Read the Balch/Elliott debate at full-text depth before ranking accommodation/subversion models.
6. Add a critical-apparatus witness (NA28/ECM where accessible) before manuscript-distribution claims at 3:18.
7. Add location-specific Roman/Asia-Minor legal primary evidence before universal legal-status claims about wives.
8. Add full-text peer-reviewed work on 3:19–20 if the project wants to make any quantitative/prevalence claim about modern scholarship.
9. Add patristic primary reception only if reception history becomes a course requirement; do not let reception history silently become grammatical proof.

## Integrator recommendations

Keep this lane as an audit/control plane. Do **not** import all source entries into the shared registry merely because they are listed here. Promote only the sources actually needed by a published question, preserve their inspection limitation, and require a claim-specific quorum. In particular:

- retain SBLGNT + pinned MorphGNT together for morphology;
- classify Sarah, Psalm 34, and Isaiah 8 relationships explicitly rather than under a generic “OT background” label;
- force `CONTESTED` metadata for 3:18–21 interpretive questions;
- keep social-history claims separate from direct-text claims;
- fix the Grudem journal year in shared metadata only during integration, not from this lane;
- do not rank the 3:19 or 3:21 views until the listed source gaps are actually closed.

This audit is **not publication authorization**. It is evidence-state and a set of stop-signs for future certainty claims.
