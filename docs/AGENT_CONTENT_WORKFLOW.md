# Agent Content Workflow

This workflow is mandatory for agents adding or revising Bible-course content. Read `AGENTS.md` and `docs/CONTENT_SOURCE_POLICY.md` first.

## 1. Anchor before work

Record:

- exact `main` SHA;
- working branch;
- target chapter/pericope;
- current coverage-matrix status;
- existing canonical/raw source boundaries.

Do not begin from memory of a previous branch.

## 2. Define the pericope, not a question quota

Before writing questions, list the propositions that need coverage:

- direct text;
- Greek morphology/syntax;
- lexical issues;
- OT/LXX quotation/allusion;
- historical/social context;
- conservative theology;
- serious disputed interpretations;
- pastoral application.

Mark irrelevant domains `n/a`; do not manufacture trivia to fill every category.

## 3. Build the source sheet first

For every non-trivial claim identify the evidence class and source quorum.

Required order:

1. biblical/Greek primary text;
2. morphology for Greek parsing;
3. relevant primary historical evidence when available;
4. conservative project witness;
5. independent evangelical exegetical control;
6. broader scholarly control for disputed/history/Greek claims.

If sources disagree, stop pretending the claim is neutral. Create a disputed item or lower confidence.

## 4. Write source IDs before prose

Add collision-safe source metadata to the canonical source registry architecture before an item depends on it.

Do not paste commentary paragraphs into the repository. Store bibliographic metadata and paraphrase findings.

## 5. Author in domain modules

New chapters use small modules grouped by evidence domain/pericope. Do not recreate a 300 KB monolith.

Recommended naming:

- `text_*.py`
- `greek_*.py`
- `ot_*.py`
- `history_*.py`
- `theology_*.py`
- `disputed_*.py`
- `application_*.py`

Keep each module reviewable and source-coherent.

## 6. Metadata is part of the answer

Every item must declare:

- `claim_type`;
- `confidence`;
- `position`;
- `competitive`;
- `sources`.

Do not add metadata later as decoration. It controls how the item is presented and whether it may affect ranking.

## 7. Greek workflow

For every Greek item:

1. copy the actual surface form from SBLGNT;
2. verify lemma/parsing in MorphGNT;
3. distinguish morphology from semantic interpretation;
4. check non-trivial semantics/syntax with serious exegetical/lexical sources;
5. do not use tense/aspect slogans as theology;
6. keep the item non-competitive until explicit source review promotes it.

A hand-typed Greek form that has not been checked is a defect, not a draft convenience.

## 8. Disputed workflow

For a disputed passage:

1. create a neutral problem statement;
2. record at least two serious views;
3. identify arguments/evidence for each;
4. state the course position separately;
5. mark interpretation items `contested` and `competitive=false`;
6. allow ranking only for undisputed textual facts around the passage.

Do not write a multiple-choice question whose only purpose is to make the preferred commentator win.

## 9. History workflow

For historical/social claims:

- distinguish primary source from later tradition;
- avoid false precision;
- do not convert ancient institutions into modern equivalents;
- separate historical description from modern application;
- use a modern scholarly control to detect outdated popular claims.

## 10. Application workflow

Application questions are pastoral exercises, not objective ranking items.

They must:

- be grounded in the pericope;
- preserve the ancient context;
- avoid claiming one highly specific pastoral tactic is the only faithful response when the text does not require it;
- always set `competitive=false`.

## 11. Update the coverage matrix immediately

After each reviewed slice, update the chapter coverage file with what is actually in the branch.

Never mark `done` because a file was attempted, blocked, or discussed in chat. `done` means the reviewed file exists, is wired into the intended draft/canonical layer, and its source/test obligations are satisfied.

## 12. Draft is not production

Incomplete chapters remain outside `POOL_REGISTRY` and user menus.

Do not expose a partial chapter to make progress visible. First finish the chapter's definition of done, then promote it canonically in one reviewed integration step.

## 13. Test before expansion

At each meaningful slice obtain fresh CI. If lint/compile/tests fail, stop adding content and fix the exact failure first.

Content tests should verify structure and semantics, not brittle prose substrings where AST/data assertions are possible.

## 14. Review before promotion

Before a chapter moves from draft to production:

- audit every stable ID;
- resolve every source ID;
- re-check all Greek forms used as evidence;
- check OT citation classification;
- review historical assertions;
- review all `position=project` wording;
- prove application/contested items are outside ranking;
- compare coverage matrix with actual modules.

## 15. Exact-head admission

A chapter is not merge-ready until fresh checks on the actual PR merge result succeed for:

- workflow/lint/dependency/secret guards;
- Python compile;
- full pytest;
- Mini App JavaScript tests;
- Docker/production import/web smoke;
- Security Audit;
- CodeQL.

Do not cite an older green run after the tree changes.

## 16. Required agent handoff

Every handoff must state:

- exact base/head SHA;
- branch/PR;
- files added/changed;
- pericope coverage completed;
- sources added;
- disputed issues and chosen disposition;
- tests/run IDs;
- what is still pending;
- whether anything is production-exposed;
- `READY`, `DRAFT`, or `STOP`.

Never report a blocked or merely planned file as completed.