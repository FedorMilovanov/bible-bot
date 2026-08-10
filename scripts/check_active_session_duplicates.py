"""Read-only deployment preflight for duplicate active quiz sessions.

Run before enabling the strict one-active-session unique index. The command
never mutates MongoDB or chooses a "winner" among contradictory legacy rows.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from legacy_session_access import (  # noqa: E402
    QuizSessionAccessUnavailable,
    find_duplicate_active_session_users,
)


def main() -> int:
    try:
        duplicates = find_duplicate_active_session_users(limit=500)
    except QuizSessionAccessUnavailable as exc:
        print(
            json.dumps(
                {"ok": False, "error": "preflight_unavailable", "detail": str(exc)},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2

    if duplicates:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "duplicate_active_sessions",
                    "count": len(duplicates),
                    "users": duplicates,
                },
                ensure_ascii=False,
            )
        )
        return 1

    print(json.dumps({"ok": True, "duplicate_active_sessions": 0}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
