# AGENTS.md — bible-bot operating contract

This file is mandatory for every human or automated agent working in this repository.
Read it before changing code, questions, documentation, CI, deployment, or GitHub settings.

## 1. Repository authority and branch discipline

- `main` is the integration authority. Treat it as protected even if GitHub branch protection is temporarily absent.
- Never develop directly on `main`. Create a narrow branch from the exact current `main` SHA.
- Do not force-push shared branches. Do not rewrite published history unless the user explicitly orders a recovery operation and the exact before/after graph is proven safe.
- Use a PR for integration. A PR is not merge-ready until fresh checks for its exact head/merge result are green.
- Do not rerun an old successful or failed workflow as evidence for a new tree. New code requires new exact-head evidence.
- If `main` moves, re-evaluate the merge result. Do not assume a previously green head is still admission-ready.
- Keep changes bounded. Content, runtime, deployment, and control-plane mutations should be separate commits and, when materially independent, separate PRs.

## 2. Production architecture boundaries

- `telegram_production.py` is the only production Telegram composition root.
- MongoDB is authoritative for durable quiz/PvP/report state. Do not replace durable state with process-local dictionaries, browser storage, or hidden fallback state.
- `bot.py` is transitional legacy/presentation code. Do not add new production state authority there.
- `questions/` is the only production question authority.
- Raw authoring corpora such as `questions/chapter1.py` and `questions/intro.py` must not be imported directly by production handlers. Production consumes canonical pools exposed by `questions/__init__.py`.
- Do not bypass ownership, idempotency, CAS, unique-index, receipt, retry, or finalization contracts to make a test pass.
- Do not weaken an allowlist, security test, source-truth guard, or invariant merely because new code violates it. Fix the architecture first.

## 3. Content-truth model is mandatory

Every production question must carry or receive canonical metadata:

- `claim_type`: `text`, `greek`, `history`, `interpretation`, or `application`;
- `confidence`: `high`, `medium`, or `contested`;
- `position`: `neutral` or `project`;
- `competitive`: explicit eligibility for PvP/Challenge ranking;
- `sources`: source IDs resolvable through the canonical source catalog.

Rules:

- Direct biblical-text observations may be `high/neutral` when the wording is unambiguous.
- Greek morphology must be machine-checked against the canonical Greek/morphology corpus; never type a parsing claim from memory.
- Historical reconstruction is not the same thing as the biblical text.
- A conservative/project interpretation is allowed and encouraged where appropriate, but it must be labelled as the course position when serious interpreters disagree.
- `application` and genuinely `contested` items are never competitive.
- Do not make a disputed dating, authorship reconstruction, allusion, secretary hypothesis, persecution model, or systematic-theology synthesis look like a lexical fact.

See `docs/CONTENT_SOURCE_POLICY.md` for the full evidence policy.
For chapter-completion criteria see `docs/FIRST_PETER_2_5_ROADMAP.md` and the per-chapter coverage matrix.

## 4. Source hierarchy

Use sources according to the claim being made.

### Biblical text / Greek

Primary authority:

1. SBL Greek New Testament (SBLGNT) for Greek surface text.
2. MorphGNT/SBLGNT for morphology/parsing.
3. Serious lexica/grammars and exegetical commentaries for semantic/syntactic interpretation.

Never infer `tense = theological meaning` by a schoolroom shortcut. Aspect, syntax, discourse, lexical range, and context matter.

### Conservative theological/exegetical position

Preferred conservative/evangelical witnesses include TMS/John MacArthur, Thomas Schreiner, Karen Jobes, Wayne Grudem, Peter Davids, Craig Keener, and other academically serious commentators relevant to the passage.

TMS/MacArthur may define the project's conservative doctrinal position, but one expositor is not enough to establish a neutral lexical, historical, or scholarly-consensus claim.

### History

Prefer contemporary/near-contemporary primary evidence when available (for example Tacitus, Suetonius, Pliny/Trajan) plus a modern scholarly control.

Later church-historical testimony (for example Eusebius) must be described as later testimony/tradition when that is what it is. Do not silently convert it into contemporary evidence.

### Contested scholarship

For a genuinely disputed passage, record the major viable interpretations, name the project position if one is adopted, and keep the item non-competitive unless the question tests an undisputed fact about the text itself.

## 5. Source quorum before a new claim enters production

Minimum evidence:

- Pure text observation: canonical biblical text.
- Morphology: SBLGNT + MorphGNT.
- Greek semantic/syntactic claim: Greek text/morphology + at least one serious exegetical/lexical source; use an additional independent source for non-trivial claims.
- Historical claim: primary source when available + modern scholarly/reference control.
- Conservative doctrinal claim: at least two serious conservative/evangelical witnesses when practical; label it `position=project` if it goes beyond what the text alone establishes.
- Contested passage: at least two materially different serious interpretations must be understood before writing the question; the explanation must not hide the dispute.

If the evidence quorum is not met, do not invent certainty. Mark the item `medium`/`contested`, keep it non-competitive, or do not publish it yet.

## 6. Question-writing rules

Every question must:

- test one identifiable proposition;
- have one defensible best answer at the declared confidence level;
- use mutually distinguishable options without trick wording;
- have an explanation that agrees with the marked correct option;
- avoid an answer that appears only in the explanation but not in the options;
- identify a disputed interpretation as disputed;
- avoid fake precision (dates, routes, roles, motives) not supported by the evidence;
- preserve stable IDs once published;
- avoid exposing correct answers to the Mini App before the user answers.

For new chapters use domain-specific stable IDs, for example:

- `ch2_text_001`
- `ch2_gr_001`
- `ch2_ot_001`
- `ch2_hist_001`
- `ch2_theol_001`
- `ch2_disputed_001`
- `ch2_app_001`

Do not reuse chapter-1 IDs or encode a mutable answer/index in the ID.

## 7. Chapters 2–5 quality bar

Do not declare a chapter complete because a fixed number of questions exists.
A chapter is complete only when the verse/pericope coverage matrix is complete and all required domains have been reviewed.

Required domains where relevant:

- direct text/comprehension;
- Greek morphology and syntax;
- lexical semantics;
- Old Testament/LXX quotation and allusion;
- Greco-Roman/Jewish historical-social context;
- conservative theological exposition;
- major disputed interpretations;
- pastoral/application questions separated from factual ranking questions.

See `docs/FIRST_PETER_2_5_ROADMAP.md`.

## 8. Competitive integrity

PvP and Challenge are assessment surfaces, not places to enforce a commentator's preference.

Default `competitive=false` for:

- application;
- disputed interpretations;
- authorship/date reconstructions;
- proposed allusions that are not explicit quotations;
- pastoral judgement scenarios;
- complex Greek claims that have not passed dedicated source review.

Promotion to competitive must be explicit and source-reviewed. Never broaden the competitive pool merely because it is too small; improve the objective bank instead.

## 9. Tests required for content changes

A content PR must add/update tests that prove, as applicable:

- IDs are unique and stable;
- options/correct indices are valid;
- source IDs resolve;
- required metadata exists;
- project/contested questions cannot enter ranking accidentally;
- Greek forms/parsing match the source corpus for machine-checkable claims;
- explicit OT quotations point to the correct source passage;
- known historical/content regressions cannot return;
- production does not import raw question corpora directly.

Do not replace semantic tests with brittle substring checks when AST/data-structure validation is possible.

## 10. Runtime and Mini App rules

- Server remains authoritative for score, answer correctness, timer deadlines, ownership, and result persistence.
- Browser/UI may display state but must not become the source of truth.
- Reload/background/resume flows must recover from the server.
- New API writes require owner scoping, bounded payloads, retry semantics, and explicit failure behavior.
- Do not expose future questions, correct answers, hidden explanations, secrets, Mongo internals, or trusted score fields to the client.

## 11. Security and secrets

- Never print, request, commit, or paste real bot tokens, Mongo credentials, webhook secrets, or production `.env` values.
- Security findings are not waived by adding ignore rules unless the finding is proven false and the suppression is narrowly documented.
- New user-controlled values must not be logged unsafely.
- Mongo/network failures in safety-critical paths fail closed rather than silently falling back to unsafe state.

## 12. Definition of done for a PR

Before calling work complete:

1. Re-read this file and relevant docs.
2. Verify exact changed files and no accidental scope creep.
3. Run/obtain fresh CI for the exact PR merge result.
4. Required gates: lint/workflow validation, dependency/secret guards, Python compile, full pytest, Mini App JS syntax/unit tests, production Docker build/import/smoke, Security Audit, CodeQL.
5. Resolve relevant review/security threads.
6. Do not merge with a known content-truth ambiguity hidden as a fact.
7. After merge to `main`, wait for new push-to-main checks on the actual merge SHA.
8. Production deployment is a separate acceptance step; a merged/green `main` is not proof that Render is running that SHA.

## 13. Stop conditions

STOP and report instead of improvising when:

- source evidence materially conflicts and the question cannot be made unambiguous;
- an unrelated CI/security failure appears;
- the exact base/head changed unexpectedly;
- production data migration or destructive cleanup would be required but was not authorized;
- a connector/write operation produces encoding corruption, partial writes, or an unexpected file scope;
- a fix would require weakening an existing safety invariant.

The goal is not maximum question count. The goal is a durable, source-traceable, conservative but intellectually honest course on 1 Peter.