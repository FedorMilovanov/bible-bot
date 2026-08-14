#!/usr/bin/env python3
"""Second-pass adversarial audit for the unified 1 Peter Chapter 1-5 release.

This audit is intentionally repository-wide.  It records every tracked-tree
occurrence of the release-sensitive terms requested by the release owner, then
runs fail-closed structural checks for policy ownership, legacy allowlists,
Mini App assets, Telegram callbacks, public JSON boundaries, source identity
separation, and production imports.
"""
from __future__ import annotations

import argparse
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


def _check_required_owner_files(findings: list[dict]) -> None:
    policy = _read("questions/pool_policy.py")
    for chapter in ("chapter2", "chapter3", "chapter4", "chapter5"):
        if f'"{chapter}"' not in policy:
            _add(findings, "POOL_POLICY_OWNER", f"{chapter} missing from PoolPolicy registry")
    if "learning" not in policy or "points_per_question=0" not in policy.replace(" ", ""):
        _add(findings, "POOL_POLICY_OWNER", "learning-only zero-point policy is not explicit")

    questions_init = _read("questions/__init__.py")
    for chapter in ("chapter4", "chapter5"):
        if f'POOL_REGISTRY["{chapter}"]' not in questions_init:
            _add(findings, "CANONICAL_POOL_REGISTRATION", f"{chapter} not registered in POOL_REGISTRY")
    if "CHAPTER3_AUTHORIZED_COMPETITIVE_POOL" not in questions_init:
        _add(findings, "COMPETITIVE_AUTHORITY", "Chapter 3 competitive authority symbol missing")

    catalog = _read("course_catalog.py")
    if "POOL_REGISTRY" not in catalog or "pool_available" not in catalog:
        _add(findings, "COURSE_CATALOG_AUTHORITY", "course catalog is not pool-registration driven")
    for forbidden in ('"chapter4":', '"chapter5":'):
        if forbidden in catalog:
            _add(findings, "COURSE_CATALOG_HARDCODE", f"found hard-coded course map entry {forbidden}")


def _check_miniapp(findings: list[dict], tracked: list[str]) -> None:
    forbidden_assets = [
        path for path in tracked
        if Path(path).name.casefold() in {"chapter4.js", "chapter5.js"}
    ]
    if forbidden_assets:
        _add(findings, "MINIAPP_HARDCODED_CHAPTER_ASSET", "hard-coded chapter assets exist", paths=forbidden_assets)
    expected = {"miniapp/app.js", "miniapp/course_catalog.js", "miniapp/index.html"}
    missing = sorted(expected - set(tracked))
    if missing:
        _add(findings, "MINIAPP_ASSET_PATH", "canonical Mini App assets missing", paths=missing)
    catalog_js = _read("miniapp/course_catalog.js")
    if "course_key: course.key" not in catalog_js or "scoring_mode" not in catalog_js:
        _add(findings, "MINIAPP_CATALOG_POLICY", "Mini App course helper is not server-catalog driven")
    if re.search(r"chapter[45]", catalog_js, flags=re.IGNORECASE):
        _add(findings, "MINIAPP_CATALOG_HARDCODE", "Mini App canonical helper contains chapter4/5-specific logic")


def _check_telegram(findings: list[dict]) -> None:
    surface = _read("telegram_course_surface.py")
    required = (
        "resolve_course(",
        "list_courses(",
        'CallbackQueryHandler(course_mode_callback, pattern=r"^course_mode:")',
        'CallbackQueryHandler(course_menu_callback, pattern=r"^course_menu:")',
    )
    for marker in required:
        if marker not in surface:
            _add(findings, "TELEGRAM_CALLBACK_CATALOG", f"missing generic Telegram catalog marker: {marker}")
    if re.search(r"chapter[45]", surface, flags=re.IGNORECASE):
        _add(findings, "TELEGRAM_CHAPTER_HARDCODE", "Telegram course surface contains chapter4/5-specific branching")


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


def _check_runtime_policy(findings: list[dict]) -> None:
    import questions
    from course_catalog import SURFACE_MINIAPP, SURFACE_TELEGRAM, list_courses
    from questions.pool_policy import get_pool_policy
    from web_api.quiz import prepare_question, public_question

    def ids(items: object) -> set[str]:
        return {str(item["id"]) for item in items}  # type: ignore[index]

    learning = {}
    for chapter in ("chapter2", "chapter3", "chapter4", "chapter5"):
        policy = get_pool_policy(chapter)
        if not (
            policy.scoring_mode == "learning"
            and policy.ranked is False
            and policy.points_per_question == 0
        ):
            _add(findings, "LEARNING_SCORING_POLICY", f"{chapter} is not strictly learning-only")
        learning[chapter] = ids(questions.POOL_REGISTRY[chapter])

    random_ids = ids(questions.POOL_REGISTRY["random_all"])
    for chapter, pool_ids in learning.items():
        if random_ids.intersection(pool_ids):
            _add(findings, "RANDOM_ALL_LEAKAGE", f"{chapter} leaks into random_all")

    ch45 = learning["chapter4"] | learning["chapter5"]
    for name, pool in (
        ("COMPETITIVE_POOL", questions.COMPETITIVE_POOL),
        ("BATTLE_POOL", questions.BATTLE_POOL),
        ("CHALLENGE_FALLBACK_POOL", questions.CHALLENGE_FALLBACK_POOL),
    ):
        overlap = sorted(ch45.intersection(ids(pool)))
        if overlap:
            _add(findings, "CH45_GAMEPLAY_LEAKAGE", f"Chapter4/5 leak into {name}", ids=overlap)
    for key, pool in questions.CHALLENGE_POOLS.items():
        overlap = sorted(ch45.intersection(ids(pool)))
        if overlap:
            _add(findings, "CH45_CHALLENGE_LEAKAGE", f"Chapter4/5 leak into challenge {key}", ids=overlap)

    if len(questions.chapter3_questions) != 165:
        _add(findings, "CH3_COUNT", "Chapter 3 learning count is not 165", actual=len(questions.chapter3_questions))
    if len(questions.CHAPTER3_AUTHORIZED_COMPETITIVE_POOL) != 12:
        _add(
            findings,
            "CH3_COMPETITIVE_COUNT",
            "Chapter 3 competitive authority is not exactly 12",
            actual=len(questions.CHAPTER3_AUTHORIZED_COMPETITIVE_POOL),
        )
    if len(questions.chapter4_questions) != 52:
        _add(findings, "CH4_COUNT", "Chapter 4 reviewed count is not 52", actual=len(questions.chapter4_questions))
    if len(questions.chapter5_questions) != 72:
        _add(findings, "CH5_COUNT", "Chapter 5 reviewed count is not 72", actual=len(questions.chapter5_questions))

    telegram = {entry.key for entry in list_courses(surface=SURFACE_TELEGRAM)}
    miniapp = {entry.key for entry in list_courses(surface=SURFACE_MINIAPP)}
    for chapter in ("chapter2", "chapter3", "chapter4", "chapter5"):
        if chapter not in telegram or chapter not in miniapp:
            _add(findings, "SURFACE_CATALOG_MISMATCH", f"{chapter} missing from Telegram or Mini App catalog")

    for chapter in ("chapter4", "chapter5"):
        prepared = prepare_question(dict(questions.POOL_REGISTRY[chapter][0]))
        payload = public_question(prepared)
        if set(payload) != {"id", "question", "options"}:
            _add(findings, "PUBLIC_JSON_LEAK", f"{chapter} public question contains private fields", keys=sorted(payload))


def _check_legacy_allowlists(findings: list[dict], tracked: list[str]) -> list[dict]:
    """Find suspicious maintained runtime allowlists that stopped at chapter2/3.

    We deliberately report only code files, excluding tests/docs/audit scripts and
    known chapter-specific authoring/review modules.  The full grep corpus is
    still emitted separately, so every occurrence remains inspectable.
    """
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
    allow_paths = {
        "questions/__init__.py",
        "questions/pool_policy.py",
        "course_catalog.py",
        "telegram_course_surface.py",
    }
    for path in tracked:
        if path.startswith(skip_prefixes) or path in allow_paths:
            continue
        if Path(path).suffix not in RUNTIME_SUFFIXES:
            continue
        text = _read(path)
        # Literal collections/conditions mentioning chapter2 or chapter3 but
        # not the newly registered chapter4/5 are suspicious at release time.
        if ("chapter2" in text or "chapter3" in text) and "chapter4" not in text and "chapter5" not in text:
            for lineno, line in enumerate(text.splitlines(), 1):
                if "chapter2" in line or "chapter3" in line:
                    if any(token in line for token in (" in {", " in (", "==", "!=", "[", "{")):
                        suspects.append({"path": path, "line": lineno, "text": line.strip()})
    # No automatic failure solely for historical/legitimate chapter-specific
    # references.  Flag only allowlist-shaped lines in runtime routing/scoring.
    critical = [
        row for row in suspects
        if any(token in row["path"] for token in ("web_api", "telegram", "bot.py", "legacy_", "course"))
        and any(token in row["text"] for token in (" in {", " in (", "==", "!=", "allowed", "ALLOWED"))
    ]
    for row in critical:
        _add(findings, "LEGACY_CHAPTER_ALLOWLIST", "possible pre-Ch4/5 runtime allowlist", **row)
    return suspects


def _check_production_imports(findings: list[dict]) -> None:
    os.environ.setdefault("ADMIN_USER_ID", "1")
    os.environ.setdefault("DISABLE_WEB_SERVER", "1")
    try:
        import course_catalog  # noqa: F401
        import questions  # noqa: F401
        import telegram_course_surface  # noqa: F401
        import telegram_production  # noqa: F401
        import web_api  # noqa: F401
        import web_api.quiz  # noqa: F401
        import web_api.result_store  # noqa: F401
    except Exception as exc:  # pragma: no cover - CI diagnostic boundary
        _add(findings, "PRODUCTION_IMPORT", f"full production import failed: {type(exc).__name__}: {exc}")


def audit() -> dict:
    head = _git("rev-parse", "HEAD")
    tracked = _tracked_files()
    occurrences = {term: _grep_term(term) for term in SEARCH_TERMS}
    findings: list[dict] = []

    _check_required_owner_files(findings)
    _check_miniapp(findings, tracked)
    _check_telegram(findings)
    _check_source_registry(findings)
    _check_runtime_policy(findings)
    suspects = _check_legacy_allowlists(findings, tracked)
    _check_production_imports(findings)

    return {
        "schema_version": 1,
        "audit": "1PETER_CH1_5_SECOND_ADVERSARIAL_RELEASE_AUDIT",
        "first_green_sha": FIRST_GREEN_SHA,
        "audited_head_sha": head,
        "searched_terms": list(SEARCH_TERMS),
        "tracked_file_count": len(tracked),
        "occurrence_counts": {term: len(rows) for term, rows in occurrences.items()},
        "occurrences": occurrences,
        "legacy_allowlist_suspects": suspects,
        "finding_count": len(findings),
        "findings": findings,
        "checks": {
            "pool_policy_owner": True,
            "course_catalog_registration_driven": True,
            "miniapp_generic_catalog": True,
            "telegram_generic_callbacks": True,
            "public_json_private_metadata_guard": True,
            "source_identity_no_depth_upgrade": True,
            "production_imports": True,
        } if not findings else {},
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
    print(json.dumps({
        "audited_head_sha": report["audited_head_sha"],
        "finding_count": report["finding_count"],
        "occurrence_counts": report["occurrence_counts"],
    }, ensure_ascii=False, sort_keys=True))
    if args.fail_on_findings and report["finding_count"]:
        raise SystemExit(f"second adversarial audit found {report['finding_count']} release findings")


if __name__ == "__main__":
    main()
