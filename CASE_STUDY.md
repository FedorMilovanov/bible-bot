# Case study: verifiable Bible learning across Telegram and a Mini App

## Problem

A study course on 1 Peter supports Telegram conversations, a browser-based Mini App and multiple interaction types: lessons, normal quizzes, Challenge and PvP. Repeated callbacks, browser reloads and service restarts must **not** duplicate points, bypass user ownership, expose answers or treat debated theological interpretations as objectively scoreable facts.

## Engineering approach

1. **Single production entry:** [production_entrypoint.py](production_entrypoint.py) establishes logging and delegates to [telegram_production.py](telegram_production.py), the only Telegram composition root. [render.yaml](render.yaml) defines the checks-gated Render deployment.
2. **Authenticated transport:** [web_api/telegram_transport.py](web_api/telegram_transport.py) verifies the webhook secret and bridges accepted updates into the Python Telegram Bot application; polling is an explicit transport-only rollback.
3. **Server-owned scoring and data:** [session_integrity.py](session_integrity.py), [battle_integrity.py](battle_integrity.py) and [report_integrity.py](report_integrity.py) implement durable session/result boundaries backed by MongoDB. The Mini App never becomes the scoring or identity authority.
4. **Claim-aware course content:** [questions/](questions/) is the question authority, while [COURSE_SURFACE_ARCHITECTURE.md](COURSE_SURFACE_ARCHITECTURE.md) separates product catalog metadata from question selection and point policy. Debated historical/exegetical/application claims are kept outside inappropriate competitive rankings under [AGENTS.md](AGENTS.md).
5. **Fail-closed deployment:** [docs/DEPLOYMENT_PREFLIGHTS.md](docs/DEPLOYMENT_PREFLIGHTS.md) and [docs/PRODUCTION_ACCEPTANCE.md](docs/PRODUCTION_ACCEPTANCE.md) separate CI, read-only Mongo/provider evidence and exact deployed revision verification.

## Reproducible code-level checks

From repository root in the supported development environment:

```sh
python -m compileall -q .
pytest -q
node --check miniapp/app.js
```

The maintained [.github/workflows/ci.yml](.github/workflows/ci.yml) adds dependency/secret checks, Docker and backend/JavaScript tests. Local code tests are **not** a substitute for source inspection, authorized production MongoDB checks, or exact-SHA provider-side acceptance.

## What can and cannot be claimed

**Verifiable from the repository:** concrete single-root architecture, server-side auth/scoring design, source-qualified content policies, repeat-safe state contracts and automated test/release infrastructure.

**Not proven from repository code alone:** an always-healthy live deployment, BotFather's external Main Mini App state, number of active learners, learning outcomes, revenue or the current contents of production MongoDB.

For human/AI contribution and third-party citation boundaries, see [OWNER.md](OWNER.md). The theological approach of a project is not an assertion that all competing scholarly views are nonexistent; the content model explicitly distinguishes project interpretation from neutral text evidence.
