# Research Wave 4 — data integrity, deploy contracts and Mini App UX

Date: 2026-08-09

Wave 4 focuses on silent drift: question data that looks valid but is omitted from a combined pool, deploy surfaces that can diverge over time, and Mini App behavior that is technically functional but unnecessarily hostile to mobile/accessibility users.

## Applied decisions

1. **Validate the canonical question catalog in CI.** Every leaf pool must have enough questions for a standard quiz; IDs are globally unique; question/options/correct-index schema is valid; answer options cannot be duplicated after trim/casefold.
2. **Make `random_all` actually mean all canonical leaf pools.** The new catalog test found the only integrity failure: 25 historical questions (`nero` + `geography`) were omitted even though `_build_random_all_pool()` described itself as the combined pool of all questions. The registry now includes those leaf pools and deduplicates by ID.
3. **Lock the deploy contract in tests.** At the time of this historical wave the root `bot.py` launcher was part of that contract; it has since been fully retired. Current deployment authority is `production_entrypoint.py`, enforced by the modern deploy and retirement regressions. The historical wave also locked `/live`, Render `runtime: python`, resource-limit variables and removal of obsolete `render.yaml.txt`.
4. **Allow user zoom.** The Mini App no longer sets `maximum-scale=1` or `user-scalable=no` in the viewport meta tag.
5. **Improve assistive semantics.** Dynamic feedback/toast/leaderboard regions use polite live-region semantics; decorative progress/emoji are hidden from the accessibility tree; static buttons use explicit `type="button"`.
6. **Scope Telegram closing confirmation to an active quiz.** A small `lifecycle.js` module enables close confirmation only when `screen-quiz` is active and disables it elsewhere, rather than forcing confirmation while merely browsing menus/statistics/results.
7. **Respect Telegram theme/safe areas and reduced motion.** CSS consumes official Telegram theme/safe-area variables and `prefers-reduced-motion`.
8. **Smoke-test result media.** PNG and GIF result generators are executed in tests, which enabled a safer Pillow major upgrade.
9. **Smoke-test the built container, not just its build.** CI starts the image with the HTTP entrypoint and checks both `/live` and Mini App HTML; the image itself also declares a `/live` healthcheck.

## Primary sources

1. https://core.telegram.org/bots/webapps
2. https://core.telegram.org/api/web-events
3. https://core.telegram.org/bots/api
4. https://www.w3.org/WAI/WCAG21/Understanding/resize-text.html
5. https://www.w3.org/WAI/ARIA/apg/practices/names-and-descriptions/
6. https://www.w3.org/WAI/WCAG22/Techniques/html/H91
7. https://www.w3.org/WAI/standards-guidelines/act/rules/b4f0c3/
8. https://developer.mozilla.org/en-US/docs/Web/Accessibility/ARIA/Roles/status_role
9. https://developer.mozilla.org/en-US/docs/Web/CSS/@media/prefers-reduced-motion
10. https://developer.mozilla.org/en-US/docs/Web/CSS/env
11. https://render.com/docs/blueprint-spec
12. https://render.com/docs/health-checks
13. https://render.com/docs/python-version
14. https://docs.docker.com/reference/dockerfile/#healthcheck
15. https://docs.docker.com/build/ci/github-actions/
16. https://www.mongodb.com/docs/manual/core/index-unique/
17. https://www.mongodb.com/docs/manual/core/index-partial/
18. https://www.mongodb.com/docs/manual/core/index-ttl/
19. https://www.mongodb.com/docs/manual/core/write-operations-atomicity/
20. https://pillow.readthedocs.io/en/stable/releasenotes/12.0.0.html