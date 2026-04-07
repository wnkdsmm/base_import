from __future__ import annotations

import argparse
import subprocess
from fnmatch import fnmatch
from pathlib import PurePosixPath

CACHE_SEGMENTS = {
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "htmlcov",
}
LOCAL_ENV_SEGMENTS = {
    ".venv",
    "venv",
    "env",
    "ENV",
}
GENERATED_PREFIXES = (
    "logs/",
    "results/",
    "data/results/",
    "data/uploads/",
)
GENERATED_SUFFIXES = (
    ".pyc",
    ".pyo",
    ".pyd",
    ".log",
    ".err.log",
    ".out.log",
)


def _normalize_path(path: str) -> str:
    normalized = str(path or "").strip().replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized.lstrip("/")


def is_generated_artifact_path(path: str) -> bool:
    normalized = _normalize_path(path)
    if not normalized:
        return False

    pure_path = PurePosixPath(normalized)
    parts = pure_path.parts
    name = pure_path.name

    if any(segment in CACHE_SEGMENTS for segment in parts):
        return True
    if any(segment in LOCAL_ENV_SEGMENTS for segment in parts):
        return True
    if any(segment.endswith(".egg-info") for segment in parts):
        return True
    if normalized == ".coverage" or name == ".coverage":
        return True
    if any(normalized.startswith(prefix) for prefix in GENERATED_PREFIXES):
        return True
    if any(normalized.endswith(suffix) for suffix in GENERATED_SUFFIXES):
        return True
    if fnmatch(name, "tmp_*.txt"):
        return True
    return False


def _run_git_lines(*args: str) -> list[str]:
    completed = subprocess.run(
        ["git", *args],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def collect_changed_paths() -> list[str]:
    paths = set(_run_git_lines("diff", "--name-only", "--diff-filter=ACMRTUXB"))
    paths.update(_run_git_lines("diff", "--name-only", "--cached", "--diff-filter=ACMRTUXB"))
    paths.update(_run_git_lines("ls-files", "--others", "--exclude-standard"))
    return sorted(paths)


def collect_tracked_paths() -> list[str]:
    return sorted(set(_run_git_lines("ls-files")))


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check the working tree for generated artifacts that should stay out of git.",
    )
    parser.add_argument(
        "--tracked",
        action="store_true",
        help="Also inspect already tracked files to audit historical generated artifacts in the repository.",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="Optional explicit paths to inspect instead of reading git state.",
    )
    return parser


def main() -> int:
    parser = _build_argument_parser()
    args = parser.parse_args()

    if args.paths:
        inspected_paths = sorted({_normalize_path(path) for path in args.paths if _normalize_path(path)})
        scope_label = "explicit paths"
    else:
        inspected = set(collect_changed_paths())
        if args.tracked:
            inspected.update(collect_tracked_paths())
            scope_label = "changed, untracked, and tracked repository paths"
        else:
            scope_label = "changed and untracked paths"
        inspected_paths = sorted(inspected)

    offenders = [path for path in inspected_paths if is_generated_artifact_path(path)]
    if offenders:
        print("Generated artifacts detected:")
        for path in offenders:
            print(f" - {path}")
        return 1

    if inspected_paths:
        print(f"No generated artifacts detected in {scope_label}.")
    else:
        print(f"No paths to inspect in {scope_label}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
