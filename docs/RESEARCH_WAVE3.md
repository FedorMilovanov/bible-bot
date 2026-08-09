# Research Wave 3 — supply chain and deploy observability

Date: 2026-08-09

Wave 2 hardened the Mini App runtime boundary. Wave 3 protects the code-to-deploy path and makes a live Render revision identifiable without exposing secrets.

## Applied decisions

1. **Move CI to current Node 24 actions, still pinned by immutable SHA.**
   - `actions/checkout` v7.0.1 -> `3d3c42e5aac5ba805825da76410c181273ba90b1`
   - `actions/setup-python` v7.0.0 -> `5fda3b95a4ea91299a34e894583c3862153e4b97`
2. **Use a standalone Python vulnerability gate.** GitHub Dependency Review was tested first, but this repository has Dependency Graph disabled, so that action could not obtain dependency data. Rather than keep a permanently red/non-functional check, the branch now runs PyPA `pip-audit==2.10.1` directly against `requirements.txt` on push and pull requests.
3. **Verify the installed dependency graph.** CI runs `pip check` after installation.
4. **Build and run the real production Docker image in CI.** The image is built, then started with the HTTP entrypoint and smoke-tested through `/live` and the Mini App root. Unit tests alone do not prove that the container still builds or serves traffic.
5. **Expose non-secret deployment identity.** `/meta` returns only service name, branch, 40-character Render Git revision and `APP_ENV`. It never returns tokens, database URLs or arbitrary environment variables.
6. **Make the runtime reproducible.** Python is pinned to `3.11.15` in `.python-version`, CI and Docker. The Docker base is pinned by exact tag and digest, and Dependabot tracks Docker base updates.
7. **Refresh tested runtime dependencies selectively.** Flask, PyMongo, dnspython, python-dotenv and Pillow were upgraded under regression tests, `pip check`, media-generation smoke tests and Docker build/runtime smoke. PTB remains a deliberate migration wave.
8. **Add static and credential guards to maintained code.** Ruff checks the new HTTP/scripts/test layer without formatting legacy `bot.py`; a tracked-tree secret guard blocks obvious credentials and private-key files without printing secret values.
9. **Add CodeQL advanced security scanning.** Python and JavaScript/TypeScript are analyzed with `security-extended` queries using CodeQL Action 4.37.3 pinned to immutable SHA `e4fba868fa4b1b91e1fdab776edc8cfbe6e9fb81`.

## Additional primary-source pass

1. https://github.com/actions/checkout/releases
2. https://github.com/actions/checkout/commit/3d3c42e5aac5ba805825da76410c181273ba90b1
3. https://github.com/actions/setup-python/releases
4. https://github.com/actions/setup-python/commit/5fda3b95a4ea91299a34e894583c3862153e4b97
5. https://docs.github.com/en/actions/reference/security/secure-use
6. https://docs.github.com/en/pull-requests/how-tos/review-pull-requests/reviewing-dependency-changes-in-a-pull-request
7. https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/manage-your-dependency-security/configure-dependency-review-action
8. https://github.com/actions/dependency-review-action/releases
9. https://docs.github.com/en/code-security/concepts/supply-chain-security/dependency-review
10. https://pypi.org/project/pip-audit/
11. https://github.com/pypa/pip-audit
12. https://github.com/pypa/advisory-database
13. https://docs.github.com/en/code-security/how-tos/find-and-fix-code-vulnerabilities/configure-code-scanning/configuring-advanced-setup-for-code-scanning
14. https://docs.github.com/en/code-security/reference/code-scanning/codeql/codeql-queries
15. https://github.com/github/codeql-action
16. https://github.com/github/codeql-action/blob/main/CHANGELOG.md
17. https://github.com/github/codeql-action/commit/e4fba868fa4b1b91e1fdab776edc8cfbe6e9fb81
18. https://render.com/docs/environment-variables
19. https://render.com/docs/deploys
20. https://render.com/docs/configure-environment-variables
21. https://render.com/docs/python-version
22. https://pypi.org/project/Flask/
23. https://pypi.org/project/pymongo/
24. https://pypi.org/project/Pillow/
25. https://pypi.org/project/dnspython/
26. https://pypi.org/project/python-dotenv/
27. https://pypi.org/project/waitress/
28. https://pypi.org/project/ruff/
29. https://docs.astral.sh/ruff/
30. https://docs.docker.com/build/building/best-practices/
31. https://hub.docker.com/_/python

Combined with `RESEARCH_WAVE2.md`, the current audit trail contains far more than 30 primary-source references.

## Current dependency snapshot

- `python-telegram-bot[job-queue]==20.7` — intentionally held for a dedicated stateful `ConversationHandler` migration wave.
- `Flask==3.1.3`
- `pymongo==4.17.0`
- `dnspython==2.8.0`
- `Pillow==12.3.0` — major upgrade covered by real PNG/GIF smoke tests.
- `python-dotenv==1.2.2`
- `waitress==3.0.2`

The branch now has Dependabot, `pip-audit`, `pip check`, Ruff, secret guarding, CodeQL, Docker build/runtime smoke and content/deployment contract tests so future upgrades can be isolated and validated instead of landing as one unreviewed mass update.

## Deferred on purpose

1. **PTB 20.7 -> current major.** Build a compatibility harness around the stateful bot runtime before changing Telegram framework semantics.
2. **Polling -> webhook.** Free Render can benefit from inbound webhook traffic, but Telegram transport migration must be its own wave because webhook and `getUpdates` cannot run simultaneously.
3. **Crash-safe scoring ledger/transaction.** Final result persistence spans multiple Mongo writes. A correct exactly-once design needs a ledger/staged transaction approach; a single ad-hoc idempotency flag would create new partial-failure states.
4. **MongoDB schema validation.** Audit/migrate existing production documents before enforcing validators.
