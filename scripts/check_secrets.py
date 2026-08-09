"""Fail CI when obvious credentials are committed to the tracked tree.

This is intentionally conservative and complements (not replaces) provider-side
secret scanning. Findings print only path/line/type, never the matched secret.
"""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

FORBIDDEN_FILENAMES = {
    ".env",
    "credentials.json",
    "service-account.json",
    "id_rsa",
    "id_ed25519",
}
FORBIDDEN_SUFFIXES = {".pem", ".p12", ".pfx"}

PATTERNS = {
    "Telegram bot token": re.compile(r"(?<!\d)\d{8,12}:[A-Za-z0-9_-]{30,}(?![A-Za-z0-9_-])"),
    "MongoDB URI with embedded credentials": re.compile(
        r"mongodb(?:\+srv)?://[^\s/:@]+:[^\s/@]+@",
        re.IGNORECASE,
    ),
}

# This file intentionally contains a non-secret Mongo placeholder.
CONTENT_ALLOWLIST = {".env.example"}


def tracked_files() -> list[Path]:
    output = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT)
    return [ROOT / item.decode("utf-8") for item in output.split(b"\0") if item]


def main() -> int:
    findings: list[str] = []

    for path in tracked_files():
        relative = path.relative_to(ROOT).as_posix()
        name = path.name.lower()
        if name in FORBIDDEN_FILENAMES or path.suffix.lower() in FORBIDDEN_SUFFIXES:
            findings.append(f"{relative}: forbidden credential-like tracked file")
            continue

        if relative in CONTENT_ALLOWLIST:
            continue

        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue

        for line_number, line in enumerate(text.splitlines(), start=1):
            for label, pattern in PATTERNS.items():
                if pattern.search(line):
                    findings.append(f"{relative}:{line_number}: {label}")

    if findings:
        print("Potential committed secrets detected (values intentionally hidden):")
        for finding in findings:
            print(f"- {finding}")
        return 1

    print("Tracked-tree secret guard: no obvious committed credentials found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
