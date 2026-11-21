#!/usr/bin/env python3
"""Classify the tested PR tree for optional root and unified macOS coverage.

Execute the copy of this script from the trusted PR base, never the candidate
checkout. The caller owns event/repository validation; non-same-repo PRs must
run full coverage without consulting this helper. Only an exact "skip" is a
positive decision.
"""

import argparse
import re
import subprocess
import sys

# Exact top-level prose files without paths referenced by Go, CI or fixture
# readers. In particular TreeDB/docs/docs_lint_test.go walks repo/docs, and
# TreeDB/collections/typed_storage_naming_test.go scans docs, TreeDB and
# experiments. CI policy files, other README/specs and all code remain in scope.
_PROSE = frozenset({b"CHANGELOG.md", b"TODO.md", b"TENTATIVE_DELETIONS.md"})
# These specific Markdown files are not inputs of the Unified native suite.
# Root's docs-check remains in scope; do not generalize to Markdown globs.
_UNIFIED_PROSE = frozenset({
    b"TreeDB/docs/spec/backup-restore.md",
    b"TreeDB/docs/spec/no-wal-fast-audit.md",
    b"docs/benchmarks/ycsb_mongodb_treedb_current.md",
})
# Root parses TreeDB test headers in its pre-filter go list; the always-on
# native TreeDB Mac job runs the unfiltered listing with the same module and
# Go version, then owns the tests. Unified imports only their production code.
_TEST_SOURCE = re.compile(
    rb"(?:TreeDB|HashDB)/(?:[A-Za-z0-9_-]+/)*[A-Za-z0-9_]+_test\.go"
)
_NON_PACKAGE_TEST_PART = re.compile(rb"(?:testdata|vendor|docs|evidence|fixtures?)", re.I)
_SHA = re.compile(r"[0-9a-f]{40}")
_REGULAR_EDIT = re.compile(rb":100644 100644 [0-9a-f]{40} [0-9a-f]{40} M")


def _git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "--no-replace-objects", *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
        timeout=25,
    )


def classify(base: str, head: str, lane: str) -> tuple[str, str]:
    if lane not in {"root", "unified"}:
        return "full", "unknown macOS lane"
    if not _SHA.fullmatch(base) or not _SHA.fullmatch(head):
        return "full", "expected two immutable commit SHAs"
    try:
        for sha in (base, head):
            if _git("cat-file", "-t", sha).stdout != b"commit\n":
                return "full", "comparison input is not a commit"
        raw = _git(
            "diff-tree", "--no-commit-id", "-r", "--raw", "-z", "--no-renames",
            "--no-abbrev", "--ignore-submodules=none", base, head, "--",
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return "full", "git could not verify the comparison"
    items = raw.split(b"\0")
    if not raw or items[-1] != b"" or len(items) % 2 != 1:
        return "full", "empty or unrecognized tree comparison"
    parent_modules = set()
    for header, path in zip(items[0:-1:2], items[1:-1:2]):
        if not _REGULAR_EDIT.fullmatch(header):
            return "full", "change is not a regular edit"
        if path in _PROSE or (lane == "unified" and path in _UNIFIED_PROSE):
            continue
        if not _TEST_SOURCE.fullmatch(path):
            return "full", "change can affect this lane"
        parts = path.split(b"/")
        if lane == "root" and parts[0] != b"TreeDB":
            return "full", "Root owns these tests"
        if (
            any(
                part.startswith(b"_")
                or _NON_PACKAGE_TEST_PART.fullmatch(part)
                or b"fixture" in part.lower()
                for part in parts
            )
        ):
            return "full", "test source may be a consumed fixture or evidence"
        parent_modules.update(
            b"/".join(parts[:i]).decode("ascii") + "/go.mod"
            for i in range(1, len(parts))
        )
    try:
        if parent_modules and _git("ls-tree", "-r", "-z", "--name-only", base,
                                   "--", *sorted(parent_modules)).stdout:
            return "full", "test is not owned by the root Go module"
    except (OSError, subprocess.SubprocessError):
        return "full", "could not verify test module ownership"
    return "skip", "only audited prose or tests owned by another lane changed"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="")
    parser.add_argument("--head", default="")
    parser.add_argument("--lane", default="")
    args = parser.parse_args()
    verdict, reason = classify(args.base, args.head, args.lane)
    print(reason, file=sys.stderr)
    print(verdict)


if __name__ == "__main__":
    main()
