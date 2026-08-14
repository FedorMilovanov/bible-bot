"""Exact content identity for the reviewed Chapter-5 product bank."""
from __future__ import annotations

import hashlib
from pathlib import Path

PRODUCT_BANK_GIT_BLOB_SHA = "91d51413a6a0a3f3ad7e6e308c2a6885426ed38f"


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
