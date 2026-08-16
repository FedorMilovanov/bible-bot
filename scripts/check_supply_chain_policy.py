from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HEX40_RE = re.compile(r"[0-9a-f]{40}")
SHA256_RE = re.compile(r"sha256:[0-9a-f]{64}")
EXACT_PYTHON_RE = re.compile(r"['\"]?(\d+\.\d+\.\d+)['\"]?")


def _normalize_name(raw: str) -> str:
    return re.sub(r"[-_.]+", "-", raw.strip().lower())


def _parse_pinned_requirements(path: Path) -> dict[str, str]:
    pins: dict[str, str] = {}
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith(("-r ", "-c ")):
            continue
        if "==" not in line:
            raise ValueError(f"{path}:{lineno}: dependency must use exact == pin: {line}")
        name, version = line.split("==", 1)
        name = name.split("[", 1)[0]
        if not name or not version or any(token in version for token in (";", " ", "*")):
            raise ValueError(f"{path}:{lineno}: unsupported or non-exact dependency pin: {line}")
        pins[_normalize_name(name)] = version
    return pins


def collect_violations(root: Path = ROOT) -> list[str]:
    violations: list[str] = []
    workflows = sorted((root / ".github" / "workflows").glob("*.yml"))

    for workflow in workflows:
        lines = workflow.read_text(encoding="utf-8").splitlines()
        for lineno, raw in enumerate(lines, 1):
            stripped = raw.strip()
            if stripped.startswith("uses:") or stripped.startswith("- uses:"):
                target = stripped.split("uses:", 1)[1].strip().split()[0]
                if target.startswith("./"):
                    continue
                if "@" not in target:
                    violations.append(f"{workflow}:{lineno}: action ref has no @ revision: {target}")
                    continue
                ref = target.rsplit("@", 1)[1]
                if not HEX40_RE.fullmatch(ref):
                    violations.append(
                        f"{workflow}:{lineno}: action must be pinned to a full 40-char commit SHA: {target}"
                    )
            if stripped.startswith("runs-on:") and "-latest" in stripped:
                violations.append(f"{workflow}:{lineno}: mutable runner label is forbidden: {stripped}")
            if "python-version:" in stripped:
                value = stripped.split("python-version:", 1)[1].strip()
                if not EXACT_PYTHON_RE.fullmatch(value):
                    violations.append(
                        f"{workflow}:{lineno}: python-version must be exact X.Y.Z, got {value}"
                    )
            if "mongo:" in stripped and "docker run" in stripped and "@sha256:" not in stripped:
                violations.append(
                    f"{workflow}:{lineno}: Mongo service image must be digest-pinned: {stripped}"
                )

    dockerfile = root / "Dockerfile"
    first_from = next(
        (line.strip() for line in dockerfile.read_text(encoding="utf-8").splitlines() if line.strip().startswith("FROM ")),
        "",
    )
    if not first_from or not SHA256_RE.search(first_from):
        violations.append("Dockerfile: production base image must be pinned by sha256 digest")

    requirement_paths = [root / "requirements.txt", root / "requirements-dev.txt", root / "constraints.txt"]
    parsed: dict[Path, dict[str, str]] = {}
    for path in requirement_paths:
        try:
            parsed[path] = _parse_pinned_requirements(path)
        except (OSError, ValueError) as exc:
            violations.append(str(exc))

    constraints = parsed.get(root / "constraints.txt", {})
    for path in (root / "requirements.txt", root / "requirements-dev.txt"):
        for name, version in parsed.get(path, {}).items():
            locked = constraints.get(name)
            if locked != version:
                violations.append(
                    f"{path}: direct pin {name}=={version} must match constraints.txt ({locked!r})"
                )

    return violations


def main() -> int:
    violations = collect_violations()
    if violations:
        for violation in violations:
            print(f"SUPPLY-CHAIN POLICY: {violation}", file=sys.stderr)
        return 1
    print("supply-chain-policy-ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
