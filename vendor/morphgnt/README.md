# Vendored MorphGNT / SBLGNT corpus (1 Peter)

## File

- `81-1Pe-morphgnt.txt` — machine-verified morphology for every token of
  1 Peter (SBLGNT), 1 664 rows covering 1 Pt 1:1–5:14 (105 verses).
- SHA-256 of this file:
  `747e96c0fc1692560b31062d4d98a6526653f2084ff6f30e0c62e97d24b2dc02`
- Vendored: 2026-09-14.

## Upstream

- Project: <https://github.com/morphgnt/sblgnt>
- Source file:
  <https://raw.githubusercontent.com/morphgnt/sblgnt/master/81-1Pe-morphgnt.txt>
- Columns in this vendored file (whitespace separated, the canonical
  MorphGNT six-column layout consumed by `scripts/audit_question_quality.py`):

  1. `BBCCVV` — book/chapter/verse locator (`21CCCCVV`, 21 = 1 Peter);
  2. part-of-speech tag (e.g. `N-`, `V-`, `A-`, `RA`, `RR`, `D-`);
  3. eight-position parsing code (e.g. `-PAPNPM-`, `3AAI-S--`);
  4. surface text with punctuation and editorial signs (⸀ ⸂ ⸃ …);
  5. normalized form;
  6. lemma.

- The current upstream master ships an additional duplicate
  accent-normalized column between columns 5 and 6 (seven tokens per
  line). It is dropped here to restore the documented six-column layout;
  no morphological content is altered.

## Why it is in the repository

`AGENTS.md` (section 4) requires Greek morphology to be machine-verified
against MorphGNT rather than asserted from commentaries. The audit
script cross-checks every hand-maintained parse table in
`questions/chapter3/greek_*.py` against this corpus:

```
python3 scripts/audit_question_quality.py            # uses the vendored file
python3 scripts/audit_question_quality.py --morphgnt <other-corpus-path>
```

The corpus is the authority for morphology only. It does not decide
syntax, semantics, referents, or theology; question cards must keep that
boundary explicit.

## License / attribution

- MorphGNT: © James K. Tauber, licensed CC BY-SA 4.0
  (<https://creativecommons.org/licenses/by-sa/4.0/>).
- SBLGNT text: © Society of Biblical Literature and Logos Bible Software;
  MorphGNT's use and redistribution terms apply to this derived data set.

Attribution is preserved here per the upstream licence; if the upstream
file changes, re-download, re-normalize to six columns, and update the
SHA-256 above.
