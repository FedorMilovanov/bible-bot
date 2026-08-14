#!/usr/bin/env python3
"""Reconstruct and verify the pinned immutable Research handoff release.

This helper is release/CI-only. Production runtime never imports Research and never
performs cross-repository network access.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import lzma
import tarfile
from pathlib import Path

EXPECTED_RESEARCH_RELEASE_SHA = "8d6e5bc3f303d0a6a2d1a15969e042907f3387db"
EXPECTED_RESEARCH_AUTHORITY_SHA = "0142430af8ba80f28e0fd9cde669d32611a1d2af"
EXPECTED_AUTHORITY_DIGEST = "1f444991ecc2f180abdbe0f459148ba8dbf0a5045b1d8888e462683c78366c7d"
EXPECTED_SCHEMA_VERSION = 2


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def reconstruct(research_root: Path, output_dir: Path) -> dict:
    handoff_root = research_root / "1_PETER_BOT" / "product_handoff"
    current = _json(handoff_root / "CURRENT_RELEASE.json")
    if current.get("research_authority_sha") != EXPECTED_RESEARCH_AUTHORITY_SHA:
        raise SystemExit("Research CURRENT_RELEASE authority SHA drift")
    if current.get("authority_digest_sha256") != EXPECTED_AUTHORITY_DIGEST:
        raise SystemExit("Research CURRENT_RELEASE authority digest drift")
    if current.get("whole_authority_sha256") != EXPECTED_AUTHORITY_DIGEST:
        raise SystemExit("Research CURRENT_RELEASE whole-authority digest drift")
    if current.get("schema_version") != EXPECTED_SCHEMA_VERSION:
        raise SystemExit("Research CURRENT_RELEASE schema drift")
    if current.get("immutable_release") is not True:
        raise SystemExit("Research CURRENT_RELEASE is not immutable")

    release_dir = handoff_root / str(current["release_path"])
    index = _json(release_dir / "bundle-index.json")
    if index.get("research_authority_sha") != EXPECTED_RESEARCH_AUTHORITY_SHA:
        raise SystemExit("bundle-index authority SHA drift")
    if index.get("authority_digest_sha256") != EXPECTED_AUTHORITY_DIGEST:
        raise SystemExit("bundle-index authority digest drift")
    if index.get("schema_version") != EXPECTED_SCHEMA_VERSION:
        raise SystemExit("bundle-index schema drift")
    if index.get("immutable") is not True:
        raise SystemExit("bundle-index is not immutable")

    segment_specs = list(index.get("segments") or [])
    if len(segment_specs) != int(index.get("segment_count", -1)):
        raise SystemExit("segment count mismatch")
    chunks: list[str] = []
    for spec in segment_specs:
        name = str(spec["name"])
        raw = (release_dir / name).read_bytes()
        if _sha256(raw) != spec.get("sha256"):
            raise SystemExit(f"segment SHA mismatch: {name}")
        text = raw.decode("ascii")
        if text.endswith("\n"):
            text = text[:-1]
        if len(text) != int(spec.get("base64_chars", -1)):
            raise SystemExit(f"segment length mismatch: {name}")
        chunks.append(text)

    packed = base64.b64decode("".join(chunks), validate=True)
    if _sha256(packed) != index.get("decoded_bundle_sha256"):
        raise SystemExit("decoded bundle SHA mismatch")

    output_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(packed), mode="r:xz") as tf:
        members = tf.getmembers()
        expected_names = sorted(str(name) for name in index.get("members") or [])
        actual_names = sorted(member.name for member in members if member.isfile())
        if actual_names != expected_names:
            raise SystemExit("immutable bundle member set drift")
        for member in members:
            if not member.isfile() or member.name not in expected_names:
                continue
            if Path(member.name).name != member.name:
                raise SystemExit(f"unsafe bundle member path: {member.name}")
            extracted = tf.extractfile(member)
            if extracted is None:
                raise SystemExit(f"cannot extract bundle member: {member.name}")
            data = extracted.read()
            expected_sha = index["expanded_member_sha256"].get(member.name)
            if _sha256(data) != expected_sha:
                raise SystemExit(f"expanded member SHA mismatch: {member.name}")
            (output_dir / member.name).write_bytes(data)

    manifest = _json(output_dir / "release-manifest.json")
    if manifest.get("research_authority_sha") != EXPECTED_RESEARCH_AUTHORITY_SHA:
        raise SystemExit("release-manifest authority SHA drift")
    if manifest.get("authority_digest_sha256") != EXPECTED_AUTHORITY_DIGEST:
        raise SystemExit("release-manifest authority digest drift")
    counts = manifest.get("counts") or {}
    if counts.get("chapter4") != 72 or counts.get("chapter5") != 72:
        raise SystemExit("Research release chapter counts drift")
    if counts.get("current_holds") != 0 or counts.get("competitive_candidates") != 0:
        raise SystemExit("Research release readiness drift")
    return {
        "research_authority_sha": EXPECTED_RESEARCH_AUTHORITY_SHA,
        "authority_digest_sha256": EXPECTED_AUTHORITY_DIGEST,
        "schema_version": EXPECTED_SCHEMA_VERSION,
        "chapter4": counts.get("chapter4"),
        "chapter5": counts.get("chapter5"),
        "claim_source_inspection_edges": counts.get("claim_source_inspection_edges"),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--research-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    summary = reconstruct(args.research_root.resolve(), args.output_dir.resolve())
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
