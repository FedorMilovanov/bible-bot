# Research Wave 3 — supply chain and deploy observability

Date: 2026-08-09

Wave 2 hardened the Mini App runtime boundary. Wave 3 protects the code-to-deploy path and makes a live Render revision identifiable without exposing secrets.

## Applied decisions

1. **Move CI to current Node 24 actions, still pinned by immutable SHA.**
   - `actions/checkout` v7.0.1 -> `3d3c42e5aac5ba805825da76410c181273ba90b1`
   - `actions/setup-python` v7.0.0 -> `5fda3b95a4ea91299a34e894583c3862153e4b97`
2. **Add dependency review on pull requests.** The current Node 24 `actions/dependency-review-action` v5.0.0 is pinned to `a1d282b36b6f3519aa1f3fc636f609c47dddb294` and fails on newly introduced moderate-or-higher vulnerabilities.
3. **Verify the installed dependency graph.** CI runs `pip check` after installation.
4. **Build the real production Docker image in CI.** Unit tests alone do not prove that the container/Dockerfile still builds.
5. **Expose non-secret deployment identity.** `/meta` returns only service name, branch, 40-character Render Git revision and `APP_ENV`. It never returns tokens, database URLs or arbitrary environment variables.
6. **Do not mass-upgrade runtime packages in the same hardening change.** Current direct pins include packages that have moved multiple releases (and, for Pillow/PTB, multiple majors). Dependabot plus dependency review should isolate those upgrades; PTB remains a dedicated compatibility wave.

## Additional primary-source pass

1. https://github.com/actions/checkout/releases
2. https://github.com/actions/checkout/commit/3d3c42e5aac5ba805825da76410c181273ba90b1
3. https://github.com/actions/setup-python/releases
4. https://github.com/actions/setup-python/commit/5fda3b95a4ea91299a34e894583c3862153e4b97
5. https://docs.github.com/en/actions/reference/security/secure-use
6. https://docs.github.com/en/pull-requests/how-tos/review-pull-requests/reviewing-dependency-changes-in-a-pull-request
7. https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/manage-your-dependency-security/configure-dependency-review-action
8. https://github.com/actions/dependency-review-action/releases
9. https://github.com/actions/dependency-review-action/commit/a1d282b36b6f3519aa1f3fc636f609c47dddb294
10. https://docs.github.com/en/code-security/concepts/supply-chain-security/dependency-review
11. https://docs.github.com/en/code-security/how-tos/find-and-fix-code-vulnerabilities/configure-code-scanning/configure-code-scanning
12. https://docs.github.com/en/code-security/how-tos/find-and-fix-code-vulnerabilities/configure-code-scanning/configuring-advanced-setup-for-code-scanning
13. https://render.com/docs/environment-variables
14. https://render.com/docs/deploys
15. https://render.com/docs/configure-environment-variables
16. https://pypi.org/project/Flask/
17. https://pypi.org/project/pymongo/
18. https://pypi.org/project/Pillow/

Combined with `RESEARCH_WAVE2.md`, the current audit trail contains well over 30 primary-source references.

## Dependency snapshot observations

At audit time:

- Flask pin: `3.0.3`; PyPI current line is newer (`3.1.x`).
- PyMongo pin: `4.7.2`; PyPI current line is newer (`4.17.x`).
- Pillow pin: `10.4.0`; PyPI current line is `12.x`, which is a major-version migration rather than a routine patch.
- `python-telegram-bot` stays at `20.7` until a dedicated stateful ConversationHandler compatibility wave.

These are maintenance signals, not a reason to upgrade everything simultaneously. The repository now has Dependabot and PR dependency review precisely so upgrades can be isolated and validated.

## Next wave candidates

1. Dedicated direct-dependency upgrade PRs, starting with lower-risk same-major packages.
2. PTB 20.7 -> current major compatibility harness before touching `bot.py` runtime semantics.
3. CodeQL/default code scanning (repository setting or dedicated workflow).
4. Mini App UX wave: closing confirmation only during an active quiz, Telegram Main/Back button lifecycle, viewport/theme events and browser smoke testing.
5. Existing MongoDB data audit before enabling schema validation.
