"""Exact content identity for the reviewed Chapter-5 product bank."""
from __future__ import annotations

import hashlib
from pathlib import Path

PRODUCT_BANK_GIT_BLOB_SHA = "b15a6200fb7e4fde3e0c9ce9298645f9d3ff47d9"


def current_product_bank_git_blob_sha() -> str:
    raw = (Path(__file__).with_name("bank.py")).read_bytes()
    header = f"blob {len(raw)}\0".encode()
    return hashlib.sha1(header + raw, usedforsecurity=False).hexdigest()


def validate_product_bank_identity() -> None:
    actual = current_product_bank_git_blob_sha()
    if actual != PRODUCT_BANK_GIT_BLOB_SHA:
        raise ValueError(
            "Chapter-5 product bank changed without an explicit v2 review-contract repin: "
            f"actual={actual}, expected={PRODUCT_BANK_GIT_BLOB_SHA}"
        )


__all__ = [
    "PRODUCT_BANK_GIT_BLOB_SHA",
    "current_product_bank_git_blob_sha",
    "validate_product_bank_identity",
]
