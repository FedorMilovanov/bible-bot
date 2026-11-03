# Security policy · Политика безопасности

## Supported scope

Security maintenance targets the current protected `main` and its active deployment configuration. This repository includes a Telegram bot, a Mini App, HTTP/webhook ingress, MongoDB persistence and production provider integrations. **Source CI passing does not establish that a particular Render deployment or Telegram configuration is secure.**

## Private vulnerability reporting

**Do not open a public issue or PR containing credentials, active exploit details, MongoDB documents with personal data, raw Telegram `initData`, or private provider endpoints.**

Use GitHub's **Security → Report a vulnerability** private channel if enabled for this repository. If it is unavailable, contact the maintainer privately through an existing trusted channel; do not post the sensitive payload to create a public contact route. Report the affected component and commit, impact, redacted steps to reproduce, and suggested boundary. Maintainer: [FedorMilovanov](https://github.com/FedorMilovanov).

## Sensitive trust boundaries

- `BOT_TOKEN`, `MONGO_URL`, webhook secrets, administrator IDs and provider credentials belong in protected environment settings, not commits, action logs, issue text or client bundles.
- Telegram webhook requests must be authenticated before their body is parsed; the transport lives in [web_api/telegram_transport.py](web_api/telegram_transport.py).
- Mini App auth relies on validated and fresh Telegram `initData`, not an untrusted query-string user ID.
- MongoDB is authoritative for durable quiz, report, delivery and PvP state; duplicate/replayed operations must not earn additional results.
- The client must not receive correct answers or trusted scoring fields before submission. Quizzes and content-confidence rules are controlled by server-side policy; see [AGENTS.md](AGENTS.md).

If a secret was exposed, **revoke/rotate it with the provider first**, verify new credentials, and then address reachable Git history as separately authorized. Deleting a token from the latest commit or force-rewriting history is **not** provider-side revocation.

## Safe verification and production boundary

- Review code and run the existing dependency, secret, lint, compile and regression checks; [.github/workflows/ci.yml](.github/workflows/ci.yml) is the repository's CI entry point.
- Review [docs/DEPLOYMENT_PREFLIGHTS.md](docs/DEPLOYMENT_PREFLIGHTS.md) before any production changes. Failed or unavailable MongoDB/provider checks do not grant permission to auto-repair or delete data.
- [docs/PRODUCTION_ACCEPTANCE.md](docs/PRODUCTION_ACCEPTANCE.md) distinguishes predeploy read-only checks, authorized deployment, exact-SHA verification and postdeploy read-only evidence.
- Do not change Telegram webhook, BotFather settings, production DB indexes or credentials as part of a documentation/security-reporting PR. Testing is limited to accounts, data and environments for which the operator has authorization.

This file does not change runtime behavior, grant permission for provider writes or assert that historical vulnerabilities have been eradicated.
