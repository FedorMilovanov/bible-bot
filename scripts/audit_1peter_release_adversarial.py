#!/usr/bin/env python3
"""Second-pass adversarial audit for the unified 1 Peter Chapter 1-5 release.

The audit records every tracked-tree occurrence of the release-sensitive terms
requested by the release owner and then validates the actual runtime/AST
contracts. It intentionally avoids formatting-sensitive source assertions.
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FIRST_GREEN_SHA = "d9e0be4d64b974342d609e2c13dff4ef1b838e79"
SEARCH_TERMS = (
    "chapter4",
    "chapter5",
    "NON_SCORING",
    "COMPETITIVE_POOL",
    "BATTLE_POOL",
    "CHALLENGE",
    "POOL_REGISTRY",
    "POINTS_PER_QUESTION",
    "result_store",
    "source_registry",
)
RUNTIME_SUFFIXES = {".py", ".js", ".html"}
EXCLUDED_PREFIXES = (".git/", ".release-research/")


def _git(*args: str) -> str:
    return subprocess.check_output(
        ["git", *args], cwd=ROOT, text=True, encoding="utf-8", errors="replace"
    ).strip()


def _tracked_files() -> list[str]:
    return [line for line in _git("ls-files").splitlines() if line]


def _grep_term(term: str) -> list[dict]:
    proc = subprocess.run(
        ["git", "grep", "-n", "-I", "-F", "--", term],
        cwd=ROOT,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    if proc.returncode not in {0, 1}:
        raise RuntimeError(f"git grep failed for {term}: {proc.stderr}")
    records: list[dict] = []
    for raw in proc.stdout.splitlines():
        path, line, text = raw.split(":", 2)
        if path.startswith(EXCLUDED_PREFIXES):
            continue
        records.append({"path": path, "line": int(line), "text": text})
    return records


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def _add(findings: list[dict], rule: str, message: str, **details: object) -> None:
    findings.append({"rule": rule, "message": message, **details})


def _ids(items) -> set[str]:
    return {str(item.get("id") or "").strip() for item in items}


def _check_pool_policy_and_registry(findings: list[dict]) -> None:
    import questions
    from questions.pool_policy import get_pool_policy

    expected_counts = {"chapter2": 78, "chapter3": 165, "chapter4": 52, "chapter5": 72}
    for chapter, expected in expected_counts.items():
        if chapter not in questions.POOL_REGISTRY:
            _add(findings, "CANONICAL_POOL_REGISTRATION", f"{chapter} missing from POOL_REGISTRY")
            continue
        if len(questions.POOL_REGISTRY[chapter]) != expected:
            _add(
                findings,
                "CANONICAL_POOL_COUNT",
                f"{chapter} canonical pool count drift",
                expected=expected,
                actual=len(questions.POOL_REGISTRY[chapter]),
            )
        policy = get_pool_policy(chapter)
        if not (
            policy.scoring_mode == "learning"
            and policy.ranked is False
            and policy.points_per_question == 0
        ):
            _add(
                findings,
                "POOL_POLICY_OWNER",
                f"{chapter} is not learning-only zero-point policy",
            )

    learning_ids = {
        chapter: _ids(questions.POOL_REGISTRY[chapter])
        for chapter in expected_counts
        if chapter in questions.POOL_REGISTRY
    }
    random_ids = _ids(questions.POOL_REGISTRY["random_all"])
    for chapter, pool_ids in learning_ids.items():
        if random_ids.intersection(pool_ids):
            _add(findings, "RANDOM_ALL_LEAKAGE", f"{chapter} leaks into random_all")

    ch45 = learning_ids.get("chapter4", set()) | learning_ids.get("chapter5", set())
    for name, pool in (
        ("COMPETITIVE_POOL", questions.COMPETITIVE_POOL),
        ("BATTLE_POOL", questions.BATTLE_POOL),
        ("CHALLENGE_FALLBACK_POOL", questions.CHALLENGE_FALLBACK_POOL),
    ):
        overlap = sorted(ch45.intersection(_ids(pool)))
        if overlap:
            _add(findings, "CH45_GAMEPLAY_LEAKAGE", f"Chapter4/5 leak into {name}", ids=overlap)
    for key, pool in questions.CHALLENGE_POOLS.items():
        overlap = sorted(ch45.intersection(_ids(pool)))
        if overlap:
            _add(findings, "CH45_CHALLENGE_LEAKAGE", f"Chapter4/5 leak into challenge {key}", ids=overlap)

    if len(questions.CHAPTER3_AUTHORIZED_COMPETITIVE_POOL) != 12:
        _add(
            findings,
            "CH3_COMPETITIVE_COUNT",
            "Chapter 3 competitive authority is not exactly 12",
            actual=len(questions.CHAPTER3_AUTHORIZED_COMPETITIVE_POOL),
        )


def _check_catalog_registration_gate(findings: list[dict]) -> None:
    import course_catalog
    import questions
    from course_catalog import SURFACE_MINIAPP, SURFACE_TELEGRAM, list_courses

    telegram = {entry.key for entry in list_courses(surface=SURFACE_TELEGRAM)}
    miniapp = {entry.key for entry in list_courses(surface=SURFACE_MINIAPP)}
    for chapter in ("chapter2", "chapter3", "chapter4", "chapter5"):
        if chapter not in telegram or chapter not in miniapp:
            _add(findings, "SURFACE_CATALOG_MISMATCH", f"{chapter} missing from Telegram or Mini App catalog")

    original = course_catalog._pool_registry
    try:
        for chapter in ("chapter4", "chapter5"):
            entry = course_catalog.resolve_course(chapter)
            without = dict(questions.POOL_REGISTRY)
            without.pop(chapter, None)
            course_catalog._pool_registry = lambda registry=without: registry
            if course_catalog.course_available(entry):
                _add(
                    findings,
                    "COURSE_CATALOG_REGISTRATION_GATE",
                    f"{chapter} remains available after canonical pool removal",
                )
    finally:
        course_catalog._pool_registry = original


def _check_miniapp(findings: list[dict], tracked: list[str]) -> None:
    forbidden_assets = [
        path for path in tracked if Path(path).name.casefold() in {"chapter4.js", "chapter5.js"}
    ]
    if forbidden_assets:
        _add(
            findings,
            "MINIAPP_HARDCODED_CHAPTER_ASSET",
            "hard-coded chapter assets exist",
            paths=forbidden_assets,
        )
    expected = {"miniapp/app.js", "miniapp/course_catalog.js", "miniapp/index.html"}
    missing = sorted(expected - set(tracked))
    if missing:
        _add(findings, "MINIAPP_ASSET_PATH", "canonical Mini App assets missing", paths=missing)

    catalog_js = _read("miniapp/course_catalog.js")
    if "course_key: course.key" not in catalog_js or "scoring_mode" not in catalog_js:
        _add(findings, "MINIAPP_CATALOG_POLICY", "Mini App helper is not server-catalog driven")
    if re.search(r"chapter[45]", catalog_js, flags=re.IGNORECASE):
        _add(
            findings,
            "MINIAPP_CATALOG_HARDCODE",
            "Mini App canonical helper contains chapter4/5-specific logic",
        )

    index = _read("miniapp/index.html")
    ordered_assets = ["flow_guard.js", "course_catalog.js", "app.js", "lifecycle.js"]
    positions = [index.find(f'src="{asset}"') for asset in ordered_assets]
    if any(position < 0 for position in positions) or positions != sorted(positions):
        _add(
            findings,
            "MINIAPP_ASSET_PATH",
            "Mini App canonical script assets are missing or loaded out of expected order",
            positions=dict(zip(ordered_assets, positions, strict=True)),
        )


def _telegram_course_patterns() -> dict[str, set[str]]:
    tree = ast.parse(_read("telegram_production.py"))
    patterns: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Name) or func.id != "CallbackQueryHandler" or not node.args:
            continue
        callback = node.args[0]
        if not (
            isinstance(callback, ast.Attribute)
            and isinstance(callback.value, ast.Name)
            and callback.value.id == "courses"
        ):
            continue
        pattern = None
        for keyword in node.keywords:
            if keyword.arg == "pattern" and isinstance(keyword.value, ast.Constant):
                pattern = keyword.value.value
        if isinstance(pattern, str):
            patterns.setdefault(callback.attr, set()).add(pattern)
    return patterns


def _check_telegram(findings: list[dict]) -> None:
    patterns = _telegram_course_patterns()
    expected = {
        "course_menu_callback": {"^course_menu$", "^chapter_1_menu$", "^historical_menu$"},
        "show_group_callback": {"^course_group:"},
        "course_callback": {"^course:"},
        "course_mode_callback": {"^course_mode:"},
        "legacy_level_callback": {"^level_"},
        "legacy_confirm_level_callback": {"^confirm_level_"},
        "legacy_mode_callback": {"^(relaxed|timed|speed)_mode_"},
        "legacy_intro_start_callback": {"^intro_start_"},
    }
    for callback, required in expected.items():
        actual = patterns.get(callback, set())
        missing = sorted(required - actual)
        if missing:
            _add(
                findings,
                "TELEGRAM_CALLBACK_CATALOG",
                f"generic/stale-safe Telegram callback coverage missing for {callback}",
                expected=sorted(required),
                actual=sorted(actual),
                missing=missing,
            )

    surface = _read("telegram_course_surface.py")
    if re.search(r"(?:if|elif).*chapter[45]", surface, flags=re.IGNORECASE):
        _add(
            findings,
            "TELEGRAM_CHAPTER_HARDCODE",
            "Telegram course surface contains chapter4/5-specific conditional branching",
        )


def _check_source_registry(findings: list[dict]) -> None:
    import questions.source_registry as source_registry

    forbidden_depth = {
        "inspection_scope",
        "evidence_status",
        "claim_inspection_edge_ids",
        "strongest_depth",
        "claim_depth",
    }
    for source_id, metadata in source_registry.SOURCE_CATALOG.items():
        if metadata.get("source_identity_only") is True:
            leaked = sorted(forbidden_depth.intersection(metadata))
            if leaked:
                _add(
                    findings,
                    "SOURCE_IDENTITY_DEPTH_LAUNDERING",
                    f"identity-only source {source_id} carries claim-depth metadata",
                    fields=leaked,
                )


def _check_public_json_and_result_store(findings: list[dict]) -> None:
    import questions
    from web_api.quiz import prepare_question, public_question

    for chapter in ("chapter4", "chapter5"):
        prepared = prepare_question(dict(questions.POOL_REGISTRY[chapter][0]))
        payload = public_question(prepared)
        if set(payload) != {"id", "question", "options"}:
            _add(
                findings,
                "PUBLIC_JSON_LEAK",
                f"{chapter} public question contains private fields",
                keys=sorted(payload),
            )

    result_store = _read("web_api/result_store.py")
    required = ("is_non_scoring_learning_pool", "_apply_learning_result_once")
    missing = [marker for marker in required if marker not in result_store]
    if missing:
        _add(
            findings,
            "RESULT_STORE_LEARNING_BOUNDARY",
            "result_store lost centralized learning-result routing",
            missing=missing,
        )


def _string_constants(node: ast.AST) -> set[str]:
    return {
        child.value
        for child in ast.walk(node)
        if isinstance(child, ast.Constant) and isinstance(child.value, str)
    }


def _check_legacy_allowlists(findings: list[dict], tracked: list[str]) -> list[dict]:
    """Find literal multi-chapter allowlists that stop at Chapter 2/3."""
    suspects: list[dict] = []
    skip_prefixes = (
        "tests/",
        "docs/",
        "data/",
        "scripts/",
        "questions/chapter2/",
        "questions/chapter3/",
        "questions/chapter4/",
        "questions/chapter5/",
    )
    for path in tracked:
        if path.startswith(skip_prefixes) or Path(path).suffix != ".py":
            continue
        try:
            tree = ast.parse(_read(path))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Set, ast.Tuple, ast.List)):
                continue
            strings = _string_constants(node)
            if {"chapter2", "chapter3"}.issubset(strings) and not {"chapter4", "chapter5"}.intersection(strings):
                suspects.append(
                    {
                        "path": path,
                        "line": getattr(node, "lineno", None),
                        "values": sorted(strings),
                    }
                )
    for row in suspects:
        _add(
            findings,
            "LEGACY_CHAPTER_ALLOWLIST",
            "literal runtime chapter allowlist stops at chapter2/chapter3",
            **row,
        )
    return suspects


def _check_production_imports(findings: list[dict]) -> None:
    os.environ.setdefault("ADMIN_USER_ID", "1")
    os.environ.setdefault("DISABLE_WEB_SERVER", "1")
    try:
        for module_name in (
            "course_catalog",
            "questions",
            "telegram_course_surface",
            "telegram_production",
            "web_api",
            "web_api.quiz",
            "web_api.result_store",
        ):
            __import__(module_name)
    except Exception as exc:  # pragma: no cover - CI diagnostic boundary
        _add(
            findings,
            "PRODUCTION_IMPORT",
            f"full production import failed: {type(exc).__name__}: {exc}",
        )


def audit() -> dict:
    checkout_head = _git("rev-parse", "HEAD")
    release_head = os.getenv("RELEASE_HEAD_SHA", "").strip() or checkout_head
    tracked = _tracked_files()
    occurrences = {term: _grep_term(term) for term in SEARCH_TERMS}
    findings: list[dict] = []

    _check_pool_policy_and_registry(findings)
    _check_catalog_registration_gate(findings)
    _check_miniapp(findings, tracked)
    _check_telegram(findings)
    _check_source_registry(findings)
    _check_public_json_and_result_store(findings)
    suspects = _check_legacy_allowlists(findings, tracked)
    _check_production_imports(findings)

    return {
        "schema_version": 2,
        "audit": "1PETER_CH1_5_SECOND_ADVERSARIAL_RELEASE_AUDIT",
        "first_green_sha": FIRST_GREEN_SHA,
        "audited_release_head_sha": release_head,
        "checkout_tree_sha": checkout_head,
        "searched_terms": list(SEARCH_TERMS),
        "tracked_file_count": len(tracked),
        "occurrence_counts": {term: len(rows) for term, rows in occurrences.items()},
        "occurrences": occurrences,
        "legacy_allowlist_suspects": suspects,
        "finding_count": len(findings),
        "findings": findings,
        "checks": {
            "pool_policy_owner": True,
            "canonical_pool_registration": True,
            "course_catalog_registration_gate": True,
            "miniapp_generic_catalog_and_assets": True,
            "telegram_generic_and_stale_callbacks": True,
            "public_json_private_metadata_guard": True,
            "result_store_learning_boundary": True,
            "source_identity_no_depth_upgrade": True,
            "legacy_multi_chapter_allowlists": True,
            "production_imports": True,
        }
        if not findings
        else {},
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--fail-on-findings", action="store_true")
    args = parser.parse_args()
    report = audit()
    args.report.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "audited_release_head_sha": report["audited_release_head_sha"],
                "checkout_tree_sha": report["checkout_tree_sha"],
                "finding_count": report["finding_count"],
                "occurrence_counts": report["occurrence_counts"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    for finding in report["findings"]:
        location = ""
        if finding.get("path"):
            location = str(finding["path"])
            if finding.get("line"):
                location += f":{finding['line']}"
            location += " "
        print(
            f"::error title=Second adversarial {finding['rule']}::"
            f"{location}{finding['message']}"
        )
    if args.fail_on_findings and report["finding_count"]:
        raise SystemExit(
            f"second adversarial audit found {report['finding_count']} release findings"
        )


if __name__ == "__main__":
    main()
