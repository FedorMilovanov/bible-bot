# Render deployment controls

Production deployment admission and health are controlled by the live Render service and mirrored in `render.yaml`.

Current production contract:

- `autoDeployTrigger: checksPass` — protected-main revisions must complete the required GitHub checks before Render starts a production deploy.
- `healthCheckPath: /production/ready` — new instances must satisfy the aggregate database + Telegram readiness contract before taking traffic.
- `buildFilter.ignoredPaths` is intentionally limited to:
  - `.github/**`
  - `docs/**`
  - `tests/**`

The build filter is an optimization only. It must never substitute for `checksPass`.

For an existing Render service, do not assume that changing `render.yaml` has updated the live control plane. Verify the provider-side service settings after rollout. In particular, confirm that Included Paths is empty and the three entries above are present under Ignored Paths before relying on skip behavior.

Paths that can affect runtime, dependencies, application data/content, deployment configuration, Mini App assets, or release/runtime scripts must remain deploy-triggering.

Operational verification should distinguish three independent properties:

1. GitHub required checks are terminal green before Render starts a runtime-affecting deployment.
2. A change confined to an ignored path does not create a Render production deploy.
3. Runtime-affecting changes still deploy and pass the five production acceptance endpoints: `/live`, `/ready`, `/telegram/ready`, `/production/ready`, and exact-revision `/meta`.
