# 1 Peter Chapter 3 — explicit ranking authority

**Status:** `12 IDS AUTHORIZED / GAMEPLAY UNCHANGED`

This checkpoint is stacked on the exact-green ranking-readiness audit:

`b695f9170673caaaff517803d82331386750ccb2`

The audit already passed:

- CI #1228;
- Security Audit #1108;
- CodeQL Stacked PR #910.

## Why an explicit authority layer exists

A dynamic READY set is useful for testing, but production ranking should not silently import whatever a future audit implementation happens to return.

This layer therefore pins the exact intended IDs:

```text
ch3_text_101
ch3_text_102
ch3_text_103
ch3_text_104
ch3_text_105
ch3_text_106
ch3_text_107
ch3_text_108
ch3_text_109
ch3_text_110
ch3_text_111
ch3_text_112
```

The regression contract requires this explicit set to equal the dynamic `CHAPTER3_RANKING_READY_IDS` exactly. It also requires the audit HOLD map to be empty for the current candidate surface.

## Why these twelve

They are the Agent-A direct-text questions for 1 Peter 3:1–7. Each is:

```text
claim_type = text
confidence = high
position = neutral
competitive = true
```

Each cites exactly:

- `sblgnt` — lane status `inspected_primary`;
- `net_1p3_1_7` — lane status `inspected_passage`.

The cards ask direct textual distinctions rather than disputed social reconstruction, Greek parsing, application, or project theology.

Agent B marks all 3:8–12 cards noncompetitive; Agent C's 3:13–17 lane is noncompetitive; Agent D's 3:18–22 direct-text layer is also noncompetitive. Those lanes therefore do not enter this authority checkpoint merely because they are available in normal learning.

## What authority does not mean

This PR still makes no gameplay change.

The twelve IDs remain absent from:

- root `COMPETITIVE_POOL`;
- `BATTLE_POOL`;
- all `CHALLENGE_POOLS`.

```text
EXPLICIT RANKING AUTHORITY != GAMEPLAY ADMISSION
```

A later product PR must decide where these twelve belong. In particular, Battle and Challenge are not treated as interchangeable: Chapter-3 cards currently have no Challenge easy/medium/hard taxonomy, so Challenge must not receive them accidentally through a generic fallback.

## Next product decision

The safe next step, after fresh exact-head gates on this authority PR, is a narrowly scoped gameplay admission that can:

1. add these exact twelve IDs to the general competitive/Battle surface;
2. preserve Chapter-3 normal learning as non-scoring;
3. keep Challenge composition unchanged until an explicit difficulty taxonomy exists;
4. prevent Challenge fallback from silently pulling Chapter-3 cards from an enlarged general competitive pool.

That separation keeps source authority, ranking authority, Battle admission, and Challenge taxonomy as distinct decisions rather than one broad switch.
