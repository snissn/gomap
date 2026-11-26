#!/usr/bin/env python3
"""Balanced 7-shard Windows TreeDB runner for Tier 2 full sweeps.

Replaces the legacy 17-shard Windows matrix (which included a 0.00s always-skip
caching-heavy-1 shard and 5 overhead-dominated <60s shards) while preserving
100% package and test coverage across all TreeDB/... packages.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import subprocess
import sys

SHARD_COUNT = 7
TREEDB_PREFIX = "github.com/snissn/gomap/TreeDB"

DEDICATED_PACKAGES: frozenset[str] = frozenset({
    "github.com/snissn/gomap/TreeDB",
    "github.com/snissn/gomap/TreeDB/caching",
    "github.com/snissn/gomap/TreeDB/collections",
    "github.com/snissn/gomap/TreeDB/db",
    "github.com/snissn/gomap/TreeDB/mongo_gateway",
    "github.com/snissn/gomap/TreeDB/nativewire",
})

# Keep the three heaviest Windows collections tests split across shards 1 and 2.
COLLECTIONS_PIN_SHARD: dict[str, int] = {
    "TestVectorPartitionSourceImportDependencyEncodingCoalescedV2": 0,
    "TestVectorPartitionSourceImportDependencyEncodingContinuousV2": 1,
    "TestVectorPartitionSourceImportDependencyEncodingReopenV2": 0,
}


def list_treedb_packages(repo: Path, env: dict[str, str]) -> list[str]:
    proc = subprocess.run(
        ["go", "list", "./..."],
        cwd=repo / "TreeDB",
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return sorted(line.strip() for line in proc.stdout.splitlines() if line.strip())


def list_package_tests(repo: Path, env: dict[str, str], rel_pkg: str) -> list[str]:
    proc = subprocess.run(
        ["go", "test", rel_pkg, "-list", "^(Test|Example|Fuzz).*"],
        cwd=repo / "TreeDB",
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return [
        line.strip()
        for line in proc.stdout.splitlines()
        if line.strip().startswith(("Test", "Example", "Fuzz"))
    ]


def partition_tests(tests: list[str], index: int, count: int, pins: dict[str, int] | None = None) -> list[str]:
    out: list[str] = []
    fallback = 0
    pins = pins or {}
    for name in tests:
        if name in pins:
            if pins[name] == index:
                out.append(name)
            continue
        if (fallback % count) == index:
            out.append(name)
        fallback += 1
    return out


def _run_cmd(cmd: list[str], *, cwd: Path, env: dict[str, str], jsonl_path: Path | None = None) -> int:
    print("+", " ".join(cmd), flush=True)
    if jsonl_path is None:
        return subprocess.run(cmd, cwd=cwd, env=env).returncode
    with jsonl_path.open("a", encoding="utf-8") as logf:
        proc = subprocess.Popen(cmd, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            logf.write(line)
        return proc.wait()


def _run_named_tests(rel_pkg: str, tests: list[str], *, cwd: Path, env: dict[str, str], jsonl_path: Path) -> int:
    if not tests:
        return 0
    chunk: list[str] = []
    chunk_len = 0
    for name in tests:
        if chunk and chunk_len + len(name) + 1 > 6000:
            regex = "^(" + "|".join(re.escape(x) for x in chunk) + ")$"
            rc = _run_cmd(
                ["go", "test", "-json", "-timeout", "30m", "-p", "1", rel_pkg, "-run", regex],
                cwd=cwd,
                env=env,
                jsonl_path=jsonl_path,
            )
            if rc != 0:
                return rc
            chunk = [name]
            chunk_len = len(name)
        else:
            chunk.append(name)
            chunk_len += len(name) + 1
    if chunk:
        regex = "^(" + "|".join(re.escape(x) for x in chunk) + ")$"
        return _run_cmd(
            ["go", "test", "-json", "-timeout", "30m", "-p", "1", rel_pkg, "-run", regex],
            cwd=cwd,
            env=env,
            jsonl_path=jsonl_path,
        )
    return 0


def run_shard(repo: Path, shard: int, jsonl_path: Path) -> int:
    if not 1 <= shard <= SHARD_COUNT:
        print(f"Invalid Windows shard {shard}; expected 1..{SHARD_COUNT}", file=sys.stderr)
        return 2
    treedb_dir = repo / "TreeDB"
    env = dict(os.environ, GOWORK="off", GOMEMLIMIT="4GiB", GOMAXPROCS="2")
    _run_cmd(["go", "clean", "-testcache"], cwd=treedb_dir, env=env)

    if shard == 1:
        return _run_cmd(
            ["go", "test", "-json", "-timeout", "30m", "-p", "1", "./nativewire"],
            cwd=treedb_dir,
            env=env,
            jsonl_path=jsonl_path,
        )
    if shard in (2, 3):
        idx = shard - 2
        tests = list_package_tests(repo, env, "./collections")
        selected = partition_tests(tests, idx, 2, COLLECTIONS_PIN_SHARD)
        return _run_named_tests("./collections", selected, cwd=treedb_dir, env=env, jsonl_path=jsonl_path)
    if shard in (4, 5):
        idx = shard - 4
        root_tests = partition_tests(list_package_tests(repo, env, "."), idx, 2)
        caching_tests = partition_tests(list_package_tests(repo, env, "./caching"), idx, 2)
        rc = _run_named_tests(".", root_tests, cwd=treedb_dir, env=env, jsonl_path=jsonl_path)
        if rc != 0:
            return rc
        return _run_named_tests("./caching", caching_tests, cwd=treedb_dir, env=env, jsonl_path=jsonl_path)
    if shard == 6:
        return _run_cmd(
            ["go", "test", "-json", "-timeout", "30m", "-p", "1", "./db", "./mongo_gateway"],
            cwd=treedb_dir,
            env=env,
            jsonl_path=jsonl_path,
        )
    # shard == 7: go vet ./... + all remaining TreeDB packages
    rc = _run_cmd(["go", "vet", "./..."], cwd=treedb_dir, env=env)
    if rc != 0:
        return rc
    all_pkgs = list_treedb_packages(repo, env)
    rest = [p for p in all_pkgs if p not in DEDICATED_PACKAGES]
    if not rest:
        return 0
    return _run_cmd(
        ["go", "test", "-json", "-timeout", "30m", "-p", "1", *rest],
        cwd=treedb_dir,
        env=env,
        jsonl_path=jsonl_path,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".")
    parser.add_argument("--shard", type=int, required=True)
    parser.add_argument("--jsonl", required=True)
    args = parser.parse_args(argv)
    return run_shard(Path(args.repo).resolve(), args.shard, Path(args.jsonl))


if __name__ == "__main__":
    raise SystemExit(main())
