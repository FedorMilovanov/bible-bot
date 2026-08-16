# Supply-chain policy

Production and CI inputs are treated as executable dependencies, not documentation.

## Required invariants

- Third-party GitHub Actions are pinned to full 40-character commit SHAs. Version comments are informational only.
- GitHub-hosted runners use an explicit Ubuntu release, never `ubuntu-latest`.
- `actions/checkout` runs without persisted credentials unless a workflow has a reviewed write requirement.
- Production Docker base images are pinned by `sha256` digest.
- CI service images that execute alongside the application are pinned by digest.
- Direct Python requirements use exact `==` pins.
- `constraints.txt` freezes the complete runtime/test version graph and must agree with direct requirement pins.
- CI and production installs prefer wheels only (`--only-binary=:all:`) so package installation cannot silently fall back to source builds.
- Dependency scanning remains an independent gate (`pip-audit`, CodeQL, secret guard).

`scripts/check_supply_chain_policy.py` enforces the machine-checkable subset of these invariants and is itself run near the start of CI.

## Updating dependencies

Dependabot remains the discovery mechanism, not the admission mechanism. Update PRs must preserve immutable action/image references and the constraints graph. A version bump is accepted only after the normal CI, Security Audit, CodeQL, production Docker import and Mongo-backed container E2E gates are green on the exact candidate SHA.

When a direct Python dependency changes, update `constraints.txt` in the same change if its resolved graph changes. Never weaken the policy gate to make a dependency update pass.
