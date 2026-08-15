"""Exact content identities for the Chapter-5 authoring bank and release projection."""
from __future__ import annotations

import hashlib
from pathlib import Path

AGENT3_RAW_BANK_GIT_BLOB_SHA = "b15a6200fb7e4fde3e0c9ce9298645f9d3ff47d9"
CANONICAL_RELEASE_BANK_GIT_BLOB_SHA = "364c76b853271148a5018a3edb342034685edc9b"
# Backward-compatible name now identifies the actual canonical runtime surface.
PRODUCT_BANK_GIT_BLOB_SHA = CANONICAL_RELEASE_BANK_GIT_BLOB_SHA


def _git_blob_sha(path: Path) -> str:
    raw = path.read_bytes()
    header = f"blob {len(raw)}\0".encode()
    return hashlib.sha1(header + raw, usedforsecurity=False).hexdigest()


def current_agent3_raw_bank_git_blob_sha() -> str:
    return _git_blob_sha(Path(__file__).with_name("bank_raw.py"))


def current_product_bank_git_blob_sha() -> str:
    return _git_blob_sha(Path(__file__).with_name("bank.py"))


def validate_product_bank_identity() -> None:
    actual_raw = current_agent3_raw_bank_git_blob_sha()
    if actual_raw != AGENT3_RAW_BANK_GIT_BLOB_SHA:
        raise ValueError(
            "Chapter-5 Agent3 raw bank changed during release integration: "
            f"actual={actual_raw}, expected={AGENT3_RAW_BANK_GIT_BLOB_SHA}"
        )
    actual_release = current_product_bank_git_blob_sha()
    if actual_release != CANONICAL_RELEASE_BANK_GIT_BLOB_SHA:
        raise ValueError(
            "Chapter-5 canonical release projection changed without an explicit release repin: "
            f"actual={actual_release}, expected={CANONICAL_RELEASE_BANK_GIT_BLOB_SHA}"
        )


__all__ = [
    "AGENT3_RAW_BANK_GIT_BLOB_SHA",
    "CANONICAL_RELEASE_BANK_GIT_BLOB_SHA",
    "PRODUCT_BANK_GIT_BLOB_SHA",
    "current_agent3_raw_bank_git_blob_sha",
    "current_product_bank_git_blob_sha",
    "validate_product_bank_identity",
]
