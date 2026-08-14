"""Independent, fail-closed source/evidence control plane for 1 Peter 3 (Agent E)."""

from __future__ import annotations

import json
from pathlib import Path

ALLOWED_STATUSES = {"VERIFIED", "PARTIAL", "CONTESTED", "HOLD", "REJECTED_OVERCLAIM"}
STRONG_INSPECTION = {"FULL_TEXT_INSPECTED", "PASSAGE_INSPECTED"}

def _s(category, title, url, passage, inspection_level, limitations, metadata_note=""):
    return {"category": category, "title": title, "url": url, "passage": passage,
            "scope": passage, "inspection_level": inspection_level, "limitations": limitations,
            "metadata_note": metadata_note}

SOURCES = {
    'sblgnt_1p3': _s('primary', 'SBL Greek New Testament: 1 Peter 3', 'https://github.com/LogosBible/SBLGNT', '1 Pet 3:1-22', 'PASSAGE_INSPECTED', 'SBLGNT wording is not a critical apparatus; variant-history claims require a critical edition/apparatus.', ''),
    'morphgnt_1p3': _s('primary', 'MorphGNT SBLGNT 81-1Pe-morphgnt.txt', 'https://github.com/morphgnt/sblgnt/blob/aaed91e57c8e4a8dc9a2383e129ca5e75fe6393d/81-1Pe-morphgnt.txt', '1 Pet 3:1-22', 'PASSAGE_INSPECTED', 'Morphology establishes form/parsing, not disputed semantics or theology.', 'Pinned repository commit aaed91e57c8e4a8dc9a2383e129ca5e75fe6393d.'),
    'lxx_gen18': _s('primary', 'Septuaginta (Rahlfs-Hanhart), Genesis 18', 'https://www.die-bibel.de/en/bible/LXX/GEN.18', 'Gen 18:12', 'PASSAGE_INSPECTED', 'Establishes a textual connection, not a complete theology of Sarah or marriage.', 'Deutsche Bibelgesellschaft online LXX: Rahlfs, 2nd revised ed. by Hanhart (2006).'),
    'lxx_ps33': _s('primary', 'Septuaginta (Rahlfs-Hanhart), Psalm 33', 'https://www.die-bibel.de/en/bible/LXX/PSA.33', 'Ps 33:13-17 LXX', 'PASSAGE_INSPECTED', 'Psalm numbering differs from MT/most English Bibles: LXX 33 = MT/English 34.', 'Deutsche Bibelgesellschaft online LXX: Rahlfs-Hanhart (2006).'),
    'lxx_isa8': _s('primary', 'Septuaginta (Rahlfs-Hanhart), Isaiah 8', 'https://www.die-bibel.de/en/bible/LXX/ISA.8', 'Isa 8:12-13 LXX', 'PASSAGE_INSPECTED', 'Direct verbal reuse does not by itself settle every Christological inference.', 'Deutsche Bibelgesellschaft online LXX: Rahlfs-Hanhart (2006).'),
    'lxx_gen6_9': _s('primary', 'Septuaginta (Rahlfs-Hanhart), Genesis 6-9', 'https://www.die-bibel.de/en/bible/LXX/GEN.6', 'Gen 6-9', 'PASSAGE_INSPECTED', "Does not identify 1 Pet 3:19's imprisoned spirits; that identification is interpretive.", 'Genesis 6-9 pages inspected; Rahlfs-Hanhart online edition.'),
    '1enoch_watchers': _s('primary', '1 Enoch, Book of the Watchers, ch. 10 (public-domain translation)', 'https://www.gutenberg.org/cache/epub/77815/pg77815-images.html', '1 En. 10', 'PASSAGE_INSPECTED', 'Translation/edition is old; proves the existence/content of a tradition, not direct literary dependence by 1 Peter.', ''),
    'plutarch_conjugalia': _s('primary', 'Plutarch, Conjugalia Praecepta (Advice to Bride and Groom)', 'https://www.perseus.tufts.edu/hopper/text?doc=Perseus%3Atext%3A2008.01.0181', 'Moralia 138a-146a', 'PASSAGE_INSPECTED', 'A literary moral witness, not a census of actual marriages or a universal Roman legal rule.', ''),
    'musonius_marriage': _s('primary', 'Musonius Rufus, Lecture XIII A, What Is the Chief End of Marriage?', 'https://sites.google.com/site/thestoiclife/the_teachers/musonius-rufus/lectures/13-0', 'Discourse XIII A', 'PASSAGE_INSPECTED', 'A philosophical ideal transmitted through later excerpts; not direct evidence for the precise recipients of 1 Peter.', ''),
    'hodge2010': _s('academic_control', 'Caroline E. Johnson Hodge, ‘Holy Wives’ in Roman Households: 1 Peter 3:1-6', 'https://digitalcommons.salve.edu/jift/vol4/iss1/1/', '1 Pet 3:1-6', 'ABSTRACT_INSPECTED', 'Abstract inspected, not full article; do not use alone for detailed reconstruction; peer-review status was not independently closed in this sweep.', 'Journal of Interdisciplinary Feminist Thought 4.1 (2010), Article 1.'),
    'janse2004': _s('peer_reviewed', "Fika Janse Van Rensburg, Sarah's submissiveness to Abraham", 'https://hts.org.za/index.php/hts/article/view/499', '1 Pet 3:5-6', 'ABSTRACT_INSPECTED', "Abstract inspected; implications for original reception remain the author's reconstruction.", 'HTS Teologiese Studies 60.1/2 (2004): 249-260; DOI 10.4102/hts.v60i1/2.499.'),
    'rambiert2018': _s('peer_reviewed', 'Anna Rambiert-Kwaśniewska, Mixed marriages and how to deal with them according to 1 Peter 3:1-7', 'https://czasopisma.uksw.edu.pl/index.php/ct/article/view/3014', '1 Pet 3:1-7', 'ABSTRACT_INSPECTED', 'Abstract inspected. Its violence/application argument is not a direct historical fact supplied by 1 Peter.', 'Collectanea Theologica 88.1 (2018): 43-71; DOI 10.21697/ct.2018.88.1.03.'),
    'leroux2019': _s('peer_reviewed', 'Elritia Le Roux, ἀπονέμοντες τιμήν: 1 Peter as subversive text', 'https://hts.org.za/index.php/hts/article/view/5430/14332', '1 Pet 3:1-7', 'FULL_TEXT_INSPECTED', "The 'revolutionary subordination' thesis is an interpretation, not consensus; article itself adopts a debated dating/social model.", 'HTS 75.4 (2019), a5430; DOI 10.4102/hts.v75i4.5430.'),
    'christensen2016': _s('academic_control', 'Sean M. Christensen, The Balch/Elliott Debate and the Hermeneutics of the Household Code', 'https://ixtheo.de/Record/1636388353', '1 Pet 2:11-3:12', 'BIBLIOGRAPHIC_INSPECTED', 'Bibliographic record inspected, not article body; use as a gap marker, not claim proof; peer-review status was not independently closed in this sweep.', 'Trinity Journal NS 37.2 (2016): 173-193.'),
    'christensen2015': _s('peer_reviewed', 'Sean M. Christensen, Solidarity in Suffering and Glory: The Unifying Role of Psalm 34 in 1 Peter 3:10-12', 'https://www.galaxie.com/article/jets58-2-08', '1 Pet 3:10-12', 'PARTIAL_TEXT_INSPECTED', "Full argument not inspected; do not promote 'unifying role' to neutral fact.", 'JETS 58.2 (June 2015): 335-352.'),
    'greaux2009': _s('peer_reviewed', 'Eric James Gréaux Sr., The Lord Delivers Us: An Examination of the Function of Psalm 34 in 1 Peter', 'https://journals.sagepub.com/doi/abs/10.1177/003463730910600407', '1 Pet 3:10-12', 'ABSTRACT_INSPECTED', "Abstract only; functional thesis remains the author's argument.", 'Review & Expositor 106.4 (2009): 603-613; DOI 10.1177/003463730910600407.'),
    'moyise2005': _s('peer_reviewed', 'Steve Moyise, Intertextuality and historical approaches to the use of Scripture in the New Testament', 'https://verbumetecclesia.org.za/index.php/ve/article/view/235', '1 Pet 3:14-15 / Isa 8:12-13', 'ABSTRACT_INSPECTED', 'Abstract/metadata inspected; full PDF still a source gap for detailed historical-literary conclusions.', 'Verbum et Ecclesia 26.2 (2005): 447-458; DOI 10.4102/ve.v26i2.235.'),
    'crawford2016': _s('peer_reviewed', 'Matthew R. Crawford, ‘Confessing God from a Good Conscience’: 1 Peter 3:21 and Early Christian Baptismal Theology', 'https://academic.oup.com/jts/article/67/1/23/2451894', '1 Pet 3:21', 'ABSTRACT_INSPECTED', "Abstract inspected, not paywalled full text; cannot make Crawford's favored pledge reading lexical certainty.", 'JTS 67.1 (Apr 2016): 23-37; DOI 10.1093/jts/flw085; online 6 Sep 2016.'),
    'balch1981': _s('academic_control', 'David L. Balch, Let Wives Be Submissive: The Domestic Code in 1 Peter', 'https://cart.sbl-site.org/books/060026P', '1 Pet 2:11-3:12', 'PUBLISHER_DESCRIPTION_INSPECTED', "Publisher description/review excerpt inspected, not full monograph; Balch's reconstruction is debated.", 'SBL Monograph Series 26; original 1981; 196 pp.; ISBN 9780891304296.'),
    'pierce2011': _s('academic_control', 'Chad T. Pierce, Spirits and the Proclamation of Christ', 'https://www.mohrsiebeck.com/en/book/spirits-and-the-proclamation-of-christ-9783161508585/', '1 Pet 3:18-22', 'PUBLISHER_DESCRIPTION_INSPECTED', 'Publisher synopsis inspected, not the full monograph; no direct-dependence claim is licensed by the synopsis.', 'WUNT II/305, Mohr Siebeck, 2011; 309 pp.; ISBN 9783161508585.'),
    'arichea_nida1980': _s('academic_control', 'Daniel C. Arichea and Eugene A. Nida, A Handbook on The First Letter from Peter', 'https://tips.translation.bible/tip_term/translation-commentary-on-1-peter-321/', '1 Pet 3:21', 'PASSAGE_INSPECTED', 'A translator handbook is not a current peer-reviewed lexical monograph; useful chiefly as documented ambiguity control.', 'UBS Handbook Series, 1980.'),
    'michaels1988': _s('academic_control', 'J. Ramsey Michaels, 1 Peter', 'https://books.google.com/books?id=ZUEbAQAAMAAJ', '1 Pet 3:1-22', 'BIBLIOGRAPHIC_INSPECTED', 'Bibliographic metadata only in this sweep; exact positions must be inspected before attribution.', 'Word Biblical Commentary 49, Word Books, 1988, 337 pp.'),
    'achtemeier1996': _s('academic_control', 'Paul J. Achtemeier, 1 Peter: A Commentary on First Peter', 'https://www.jstor.org/stable/j.ctvb9364z', '1 Pet 3:1-22', 'TABLE_OF_CONTENTS_INSPECTED', 'TOC/metadata inspected, not the relevant full pages; positions remain HOLD until passage readback.', 'Hermeneia, Fortress Press, 1996, xxxvi+423 pp.; DOI 10.2307/j.ctvb9364z.'),
    'elliott2000': _s('academic_control', 'John H. Elliott, 1 Peter: A New Translation with Introduction and Commentary', 'https://librarycatalog.austinseminary.edu/cgi-bin/koha/opac-detail.pl?biblionumber=99216', '1 Pet 3:1-22', 'BIBLIOGRAPHIC_INSPECTED', 'Bibliographic metadata only in this sweep; do not attribute detailed claims without passage inspection.', 'Anchor Bible 37B, Doubleday, 2000, xxiii+956 pp.; ISBN 0385413637.'),
    'macarthur_3_1_7': _s('conservative', 'John MacArthur, How to Win Your Unbelieving Spouse', 'https://www.gty.org/resources/study-guides/chapters/60-31/how-to-win-your-unbelieving-spouse', '1 Pet 3:1-7', 'FULL_TEXT_INSPECTED', 'A confessional expositor witness, not social-history consensus.', 'GTY 60-31, 18 Jun 1989.'),
    'macarthur_3_10_12': _s('conservative', 'John MacArthur, Living and Loving the Good Life, Part 3', 'https://www.gty.org/sermons/60-34/living-and-loving-the-good-life-part-3', '1 Pet 3:10-12', 'FULL_TEXT_INSPECTED', 'Not a substitute for primary LXX comparison or peer-reviewed intertext work.', 'GTY 60-34, 3 Sep 1989.'),
    'macarthur_3_13_17': _s('conservative', 'John MacArthur, Securities Against a Hostile World', 'https://www.gty.org/resources/study-guides/chapters/60-35/securities-against-a-hostile-world', '1 Pet 3:13-17', 'FULL_TEXT_INSPECTED', 'Historical rhetoric about modern hostility is application, not evidence for first-century conditions.', 'GTY 60-35, 24 Sep 1989.'),
    'macarthur_3_18': _s('conservative', "John MacArthur, The Triumph of Christ's Suffering, Part 1", 'https://www.gty.org/resources/study-guides/chapters/60-36/the-triumph-of-christs-suffering-part-1', '1 Pet 3:18', 'FULL_TEXT_INSPECTED', "Its English quotation 'died' must not be retrojected as the SBLGNT Greek reading, which is ἔπαθεν.", 'GTY 60-36, 8 Oct 1989.'),
    'macarthur_3_18_20': _s('conservative', "John MacArthur, The Triumph of Christ's Suffering, Part 2", 'https://www.gty.org/resources/study-guides/chapters/60-37/the-triumph-of-christs-suffering-part-2', '1 Pet 3:18-20', 'FULL_TEXT_INSPECTED', "One serious conservative position; directly conflicts with Grudem's Noah-preaching view.", 'GTY 60-37, 15 Oct 1989.'),
    'macarthur_3_20_22': _s('conservative', "John MacArthur, The Triumph of Christ's Suffering, Part 3", 'https://www.gty.org/resources/study-guides/chapters/60-38/the-triumph-of-christs-suffering-part-3', '1 Pet 3:20-22', 'FULL_TEXT_INSPECTED', "MacArthur's anti-baptismal-regeneration conclusion is theological exegesis, not the lexical meaning of βάπτισμα or σῴζει.", 'GTY 60-38, 22 Oct 1989.'),
    'grudem1986': _s('conservative', 'Wayne A. Grudem, Christ Preaching Through Noah: 1 Peter 3:19-20', 'https://www.galaxie.com/volume/2467', '1 Pet 3:19-20', 'PARTIAL_TEXT_INSPECTED', 'Materially disputed by MacArthur, Schreiner and much modern scholarship; full article was not re-read end-to-end in this sweep.', "Trinity Journal NS 7.2 (Fall 1986): 3-31. Existing Chapter-3 metadata saying '(1987)' should be corrected for journal publication year."),
    'schreiner2003': _s('conservative', 'Thomas R. Schreiner, 1, 2 Peter, Jude', 'https://books.google.com/books/about/1_2_Peter_Jude.html?id=xK1B7rdJmKoC', '1 Pet 3:1-22', 'BIBLIOGRAPHIC_PLUS_AUTHOR_POSITION', "Book passage itself was not fully inspected in this sweep; position cross-checked through Schreiner's public teaching/Logos interview.", 'New American Commentary 37, B&H, 2003, 512 pp.; ISBN 9780805401370.'),
    'jobes2022': _s('conservative', 'Karen H. Jobes, 1 Peter, 2nd ed.', 'https://books.google.com/books/about/1_Peter.html?id=881kEAAAQBAJ', '1 Pet 3:1-22', 'BIBLIOGRAPHIC_INSPECTED', 'Exact 3:1-22 positions not inspected in this sweep; do not infer agreement with other conservative witnesses.', 'BECNT, Baker Academic, 2022, 400 pp.; hardback ISBN 9781540965783.'),
    'davids1990': _s('conservative', 'Peter H. Davids, The First Epistle of Peter', 'https://www.eerdmans.com/9780802825162/the-first-epistle-of-peter/', '1 Pet 3:1-22', 'PUBLISHER_DESCRIPTION_INSPECTED', 'Publisher metadata/description inspected; exact disputed-passage pages still require readback before attribution.', 'NICNT, Eerdmans, 7 Aug 1990, 288 pp.; ISBN 9780802825162.'),
    'keener2014': _s('conservative', 'Craig S. Keener, The IVP Bible Background Commentary: New Testament, 2nd ed.', 'https://www.ivpress.com/the-ivp-bible-background-commentary-new-testament', '1 Pet 3:1-22', 'PUBLISHER_DESCRIPTION_INSPECTED', 'Publisher description inspected, not the chapter pages; background notes must not be promoted to specific historical claims until read.', 'IVP Academic, 3 Jan 2014, 816 pp.; ISBN 9780830824786.'),
    'grudem2024': _s('conservative', 'Wayne Grudem, 1 Peter: An Introduction and Commentary, revised ed.', 'https://www.ivpress.com/1-peter', '1 Pet 3:1-22', 'BIBLIOGRAPHIC_INSPECTED', 'Bibliographic control only; do not assume the revised commentary reproduces every 1986 argument unchanged.', 'Tyndale New Testament Commentaries 17, IVP Academic, 2024, 288 pp.'),
}
for _source_id, _source in SOURCES.items():
    _source["source_id"] = _source_id

_MATRIX_PATH = Path(__file__).resolve().parents[2] / "data" / "chapter3-evidence-matrix-agent-e.json"
with _MATRIX_PATH.open(encoding="utf-8") as _fh:
    _MATRIX = json.load(_fh)

CLAIMS = _MATRIX["claims"]
DO_NOT_CLAIM = _MATRIX["do_not_claim"]
CONFLICT_MAP = _MATRIX["conflict_map"]
SOURCE_GAPS = _MATRIX["source_gaps"]
METADATA_CORRECTIONS = _MATRIX["metadata_corrections"]

def source_breakdown():
    counts = {}
    for source in SOURCES.values():
        counts[source["category"]] = counts.get(source["category"], 0) + 1
    return counts

def hold_claim_ids():
    return [claim["claim_id"] for claim in CLAIMS if claim["status"] in {"HOLD", "CONTESTED"}]

__all__ = ["ALLOWED_STATUSES", "STRONG_INSPECTION", "SOURCES", "CLAIMS", "DO_NOT_CLAIM",
           "CONFLICT_MAP", "SOURCE_GAPS", "METADATA_CORRECTIONS", "source_breakdown", "hold_claim_ids"]
