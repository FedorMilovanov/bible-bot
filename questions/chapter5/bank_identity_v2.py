"""Exact content identities for the Chapter-5 authoring bank and release projection."""
from __future__ import annotations

import hashlib
from pathlib import Path

AGENT3_RAW_BANK_GIT_BLOB_SHA = "7a02ccdef4f1089e66a9df263bca683f2b1f0d51"
CANONICAL_RELEASE_BANK_GIT_BLOB_SHA = "364c76b853271148a5018a3edb342034685edc9b"
# Backward-compatible name now identifies the actual canonical runtime surface.
PRODUCT_BANK_GIT_BLOB_SHA = CANONICAL_RELEASE_BANK_GIT_BLOB_SHA


def _git_blob_sha_bytes(raw: bytes) -> str:
    """Return the canonical Git blob id for a tracked Python/text file.

    Git may materialize text files with CRLF in a Windows working tree while the
    repository object remains LF-normalized. Release identities pin the repository
    object, not a platform-specific checkout representation, so normalize only the
    checkout newline transform before hashing.
    """

    canonical = raw.replace(b"\r\n", b"\n")
    header = f"blob {len(canonical)}\0".encode()
    return hashlib.sha1(header + canonical, usedforsecurity=False).hexdigest()


def _git_blob_sha(path: Path) -> str:
    return _git_blob_sha_bytes(path.read_bytes())


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
