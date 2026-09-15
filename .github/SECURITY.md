# Security Policy

## Supported scope

Security fixes are accepted for the current default branch and the currently deployed production service. Historical branches, retired prototypes, and superseded releases are not supported as runnable software, but historical credential exposure still requires provider-side revocation or rotation.

Security-sensitive areas include:

- Telegram authentication, webhook ingress, and provider integration;
- Mini App authentication, authorization, session integrity, and API boundaries;
- MongoDB authorization, durable state, retention, and data-integrity controls;
- dependency and supply-chain security;
- GitHub Actions, deployment configuration, and production-readiness controls;
- accidental disclosure of credentials or other sensitive information.

## Reporting a vulnerability

Use GitHub **Private Vulnerability Reporting** for this repository from the **Security** tab. Keep the report private until remediation and disclosure are coordinated.

A useful report includes:

- the affected component or endpoint;
- a concise description of the security impact;
- reproducible steps or a minimal proof of concept;
- the affected revision or production surface, when known;
- any relevant logs with credentials, tokens, personal data, and unrelated sensitive values removed.

Do not open a public issue for an undisclosed vulnerability.

## Credentials and secrets

Do not paste bot tokens, database connection strings, passwords, API keys, session tokens, or other credentials into issues, pull requests, discussions, CI logs, screenshots, or chat transcripts.

If a credential appears to be exposed:

1. report the location and credential type privately without reproducing the secret value;
2. do not use the credential to access data or modify provider state;
3. treat provider-side revocation or rotation as the security boundary;
4. verify production with replacement credentials through the existing readiness and acceptance checks;
5. resolve any secret-scanning alert only after revocation or rotation is confirmed.

Rewriting Git history is not a substitute for revoking an exposed credential.

## Responsible testing

Keep testing narrowly scoped to systems and accounts you are authorized to test. Avoid destructive actions, privacy-impacting access, denial-of-service testing, bulk traffic, or actions that could modify production data unless they are explicitly authorized for the remediation.

The repository's read-only preflights and production-acceptance tooling should be preferred when they can establish the relevant security property without mutating provider or production state.
