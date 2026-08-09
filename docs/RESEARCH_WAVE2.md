# Research Wave 2 — production hardening audit

Date: 2026-08-09

This wave intentionally uses primary/authoritative documentation and applies only changes with a clear fit for the current single-process Telegram polling + Waitress + Flask + MongoDB architecture.

## Decisions taken

1. **Keep `python-telegram-bot` 20.7 for this wave.** The bot has a large stateful `ConversationHandler` surface. PTB 22 removes functionality deprecated in 20.x, so the major upgrade should be a dedicated compatibility wave rather than an incidental dependency bump.
2. **Keep update processing sequential.** PTB explicitly warns against concurrent update processing with stateful `ConversationHandler` usage.
3. **Bound HTTP resources at Flask and Waitress.** Mini App JSON bodies are tiny; the previous Waitress default allowed a 1 GB request body. This wave sets 64 KiB body/header limits and adds per-authenticated-user API rate limits.
4. **Keep Telegram `initData` as the identity source.** It is HMAC-validated server-side, freshness-checked, bounded in size, and cached once per request. `initDataUnsafe` remains display-only in the browser.
5. **Make one-active-quiz a database invariant too.** A unique partial MongoDB index applies only to `status: in_progress`, complementing the application-level abandon guard.
6. **Use Telegram theme and safe-area CSS variables.** The Mini App now adapts to Telegram light/dark themes, content safe areas, and reduced-motion preferences instead of assuming one fixed dark viewport.
7. **Harden CI supply chain.** GitHub Actions are pinned to full commit SHAs, workflow permissions are read-only, superseded CI runs are cancelled, and Dependabot monitors pip and GitHub Actions.
8. **Modernize Render Blueprint syntax.** Use `runtime: python` (the current field; `env` is discouraged), explicit request-limit env vars, and a bounded shutdown window.

## Primary-source pass (40+)

### Telegram / Mini Apps / Bot API

1. https://core.telegram.org/bots/webapps
2. https://core.telegram.org/bots/api
3. https://core.telegram.org/api/bots/webapps
4. https://core.telegram.org/api/web-events
5. https://core.telegram.org/method/bots.setBotMenuButton
6. https://core.telegram.org/api/bots/menu
7. https://core.telegram.org/bots/features
8. https://core.telegram.org/bots/faq

### python-telegram-bot

9. https://docs.python-telegram-bot.org/en/v20.7/
10. https://docs.python-telegram-bot.org/en/v20.7/telegram.ext.applicationbuilder.html
11. https://docs.python-telegram-bot.org/en/v20.7/telegram.ext.conversationhandler.html
12. https://docs.python-telegram-bot.org/en/v20.7/telegram.ext.jobqueue.html
13. https://docs.python-telegram-bot.org/en/v20.7/telegram.ext.aioratelimiter.html
14. https://docs.python-telegram-bot.org/en/v20.7/telegram.bot.html
15. https://docs.python-telegram-bot.org/en/v20.7/telegram.webappinfo.html
16. https://docs.python-telegram-bot.org/en/v20.7/telegram.menubuttonwebapp.html
17. https://docs.python-telegram-bot.org/en/stable/
18. https://docs.python-telegram-bot.org/en/stable/changelog.html
19. https://docs.python-telegram-bot.org/en/stable/telegram.ext.applicationbuilder.html
20. https://docs.python-telegram-bot.org/en/stable/telegram.ext.rate-limiting-tree.html

### MongoDB / PyMongo

21. https://www.mongodb.com/docs/languages/python/pymongo-driver/current/data-formats/dates-and-times/
22. https://www.mongodb.com/docs/manual/core/index-ttl/
23. https://www.mongodb.com/docs/manual/core/index-partial/
24. https://www.mongodb.com/docs/manual/core/index-unique/
25. https://www.mongodb.com/docs/manual/core/write-operations-atomicity/
26. https://www.mongodb.com/docs/languages/python/pymongo-driver/current/crud/configure/
27. https://www.mongodb.com/docs/languages/python/pymongo-driver/current/connect/connection-options/connection-pools/
28. https://www.mongodb.com/docs/languages/python/pymongo-driver/current/indexes/
29. https://www.mongodb.com/docs/languages/python/pymongo-driver/current/connect/connection-targets/
30. https://www.mongodb.com/docs/manual/core/schema-validation/

### Waitress / Flask

31. https://docs.pylonsproject.org/projects/waitress/en/latest/arguments.html
32. https://docs.pylonsproject.org/projects/waitress/en/latest/reverse-proxy.html
33. https://docs.pylonsproject.org/projects/waitress/en/latest/
34. https://flask.palletsprojects.com/en/stable/web-security/
35. https://flask.palletsprojects.com/en/stable/config/#MAX_CONTENT_LENGTH

### Render

36. https://render.com/docs/web-services
37. https://render.com/docs/health-checks
38. https://render.com/docs/blueprint-spec
39. https://render.com/docs/free
40. https://render.com/docs/faq

### GitHub Actions / dependency security

41. https://docs.github.com/en/actions/reference/security/secure-use
42. https://docs.github.com/en/code-security/concepts/supply-chain-security/about-the-dependabot-yml-file
43. https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/secure-your-dependencies/auto-update-actions
44. https://docs.github.com/en/code-security/reference/supply-chain-security/dependabot-options-reference
45. https://docs.github.com/en/code-security/tutorials/secure-your-dependencies/optimizing-pr-creation-version-updates

### OWASP

46. https://owasp.org/API-Security/editions/2023/en/0xa4-unrestricted-resource-consumption/
47. https://owasp.org/www-project-secure-headers/
48. https://cheatsheetseries.owasp.org/cheatsheets/REST_Security_Cheat_Sheet.html

## Deferred on purpose

- PTB 20.7 -> current major: dedicated migration wave with bot-runtime compatibility tests first.
- Webhook conversion: polling is currently coherent with the single-process architecture; switching transport is not automatically an improvement.
- Redis/shared rate limiting: unnecessary while Render is a single instance. Required before horizontal scaling.
- Strict CSP / trusted proxy configuration: should be introduced only after a browser/Telegram Web smoke test so the Mini App is not accidentally blocked inside Telegram containers.
- MongoDB collection schema validation: useful, but should be preceded by an existing-data compatibility audit/migration.
