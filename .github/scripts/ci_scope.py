#!/usr/bin/env python3
"""3-tier pre-alpha CI scope planner and shard runner for snissn/gomap.

Tiers:
  - docs_only  (Tier 0):   docs-check + TreeDB/docs + TestTypedStorage* (<1m)
  - draft      (Tier 0.5): preflight gofmt + affected go vet & compile (~1m)
  - surgical   (Tier 1):   preflight + vet + CGO_ENABLED=0 cross-compile +
                           affected Linux tests + touched-package -race +
                           conditional OS escalation (~3-8m)
  - main_fast  (main/MQ):  coalesced full Linux verification (~8-10m)

Full cross-OS, full 8-shard -race, and perf/suite gates run in Tier 2
(.github/workflows/ci-full-sweep.yml) on schedule, workflow_dispatch, or
PR opt-in ([ci full] title / ci:full label).
"""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from typing import Any

MODULE_PREFIX = "github.com/snissn/gomap"

# Measured Linux normal-test weights (seconds, from CI runs 45891315836, 45913782156, 45914534147)
NORMAL_WEIGHTS: dict[str, float] = {
    "github.com/snissn/gomap/TreeDB/nativewire": 525.0,
    "github.com/snissn/gomap/cmd/treedb_vector_partition_bench": 401.0,
    "github.com/snissn/gomap/TreeDB": 310.0,
    "github.com/snissn/gomap/TreeDB/collections": 180.0,
    "github.com/snissn/gomap/TreeDB/db": 125.0,
    "github.com/snissn/gomap/TreeDB/internal/raftplacement": 83.0,
    "github.com/snissn/gomap/TreeDB/caching": 80.0,
    "github.com/snissn/gomap/TreeDB/internal/raftcluster": 80.0,
    "github.com/snissn/gomap/cmd/unified_bench": 73.0,
    "github.com/snissn/gomap/TreeDB/mongo_gateway": 47.0,
    "github.com/snissn/gomap/TreeDB/mvcc": 34.0,
    "github.com/snissn/gomap/TreeDB/internal/commitlog": 25.0,
    "github.com/snissn/gomap/TreeDB/cmd/treedb_rag_benchmark": 20.0,
    "github.com/snissn/gomap/cmd/treedb_text_hybrid_scale": 15.0,
    "github.com/snissn/gomap/cmd/treedb-document-service": 14.0,
    "github.com/snissn/gomap/internal/ycsbevidence": 13.0,
    "github.com/snissn/gomap/TreeDB/documentservice": 10.0,
    "github.com/snissn/gomap/TreeDB/internal/rootpublication": 10.0,
    "github.com/snissn/gomap/internal/redisserver": 10.0,
}

# Measured Linux -race weights (seconds, post-#5866 measured on runs 45913798272 & 45914534147)
RACE_WEIGHTS: dict[str, float] = {
    "github.com/snissn/gomap/TreeDB/nativewire": 980.0,
    "github.com/snissn/gomap/TreeDB": 900.0,
    "github.com/snissn/gomap/TreeDB/collections": 750.0,
    "github.com/snissn/gomap/TreeDB/db": 210.0,
    "github.com/snissn/gomap/TreeDB/mongo_gateway": 185.0,
    "github.com/snissn/gomap/TreeDB/cmd/treedb_rag_benchmark": 160.0,
    "github.com/snissn/gomap/TreeDB/caching": 160.0,
    "github.com/snissn/gomap/cmd/unified_bench": 150.0,
    "github.com/snissn/gomap/TreeDB/internal/raftplacement": 119.0,
    "github.com/snissn/gomap/TreeDB/internal/rootpublication": 97.0,
    "github.com/snissn/gomap/TreeDB/documentservice": 86.0,
    "github.com/snissn/gomap/TreeDB/internal/raftcluster": 84.0,
    "github.com/snissn/gomap/TreeDB/mvcc": 81.0,
}

# Known slow tests pinned across distinct slices via LPT during package test splitting.
KNOWN_SLOW_TEST_WEIGHTS: dict[str, dict[str, float]] = {
    "github.com/snissn/gomap/TreeDB": {
        "TestCompactStorageExhaustiveCommandWALRandom4KOffline": 168.0,
        "TestProductionAuthorityExecutableCompositeOmissionMatrix": 82.0,
        "TestPowerLossCertificationAuthoritativeResourcesPublicReopen": 51.0,
        "TestReadSnapshotsSeeQueuedWritesBeforeCheckpoint_ProfileMatrix": 25.0,
        "TestCompactStorageClearsPublicRewriteSourceGCBehindActiveWriters": 18.0,
        "TestReopenVerify_SplitMergeDeleteRange_IteratorParity": 18.0,
        "TestCOWAutoCheckpointIdleActualActivity": 16.0,
        "TestProfileFast_SetSyncRemainsVisibleAndPersistsAfterCheckpoint": 15.0,
        "TestIndexVacuumM4CertificationMatrix": 15.0,
    },
    "github.com/snissn/gomap/TreeDB/collections": {
        "TestTypedColumnQ2DenseGroupCountDistinctAdaptiveRankRunEquivalence": 79.0,
        "TestTypedGraphPreparedFilterDispersedQuality": 41.0,
        "TestNoWALFastBoundaryDrainsAcknowledgedCollectionBuffers": 38.0,
        "TestNativeTextPublicCrashReplay": 31.0,
        "TestVectorPartitionSourceImportDependencyEncodingCoalescedV2": 24.0,
        "TestR1MutationCrashCuts5059": 22.0,
        "TestVectorPartitionSourceImportDependencyEncodingReopenV2": 18.0,
        "TestNativeOrdinaryProjectedGenerationPressure": 18.0,
        "TestVectorPartitionSourceImportDependencyEncodingContinuousV2": 18.0,
        "TestTypedGraphFilterNavigationReducesDispersedTraversal": 15.0,
        "TestTypedGraphLifecyclePublicMutationAndReopen": 13.0,
        "TestVectorPartitionLocalAuxiliaryNavigationKeepsNativeLayer0AndEntryReachable": 13.0,
        "TestTypedStorageLegacyNameAllowlistIsComplete": 11.0,
        "TestPreparedInsertCrashRecoveryCuts": 11.0,
    },
    "github.com/snissn/gomap/TreeDB/db": {
        "TestCollectionPublicationPathsConvergeOnCoherentSnapshotPublication": 65.0,
        "TestVacuumRaceMissingKey": 60.0,
        "TestCompactIndex_AllocatesNewPagesByAppending": 29.0,
        "TestIndexVacuumTriggerReportMatchesFragmentationSpan": 20.0,
        "TestLeafFillTarget_IsEnforced": 15.0,
        "TestVacuumIndexOnline_RebuildsPackedInternalTreeForLeafRefs": 15.0,
        "TestPublicationSourceGuardNotificationRejectsExposedState": 14.0,
    },
    "github.com/snissn/gomap/TreeDB/mongo_gateway": {
        "TestStandaloneNativeDictionaryCachePolicy": 60.0,
        "TestStandaloneServerGoRunHelp": 47.0,
        "TestMongoVectorCreateIndexesValidatesWholeRequest": 6.0,
        "TestStandaloneLargeIndexedBSON": 6.0,
    },
    "github.com/snissn/gomap/TreeDB/nativewire": {
        "TestVectorPartitionLiveSelectedLifecycleV1": 265.0,
        "TestMultiOwnerTCPAcceptedModelScaledCorrectnessV1": 228.0,
    },
}

# Dynamic sharding targets tuned for ~4-5 minute Linux wall-clock on wide PRs.
COMPILE_SHARD_MIN_PACKAGES = 45
TARGET_NORMAL_SHARD_SECONDS = 135.0
TARGET_MAIN_NORMAL_SHARD_SECONDS = 165.0
NORMAL_SPLIT_THRESHOLD_SECONDS = 155.0
MAX_SURGICAL_TEST_SHARDS = 8
MAX_MAIN_TEST_SHARDS = 12
MAX_NORMAL_SLICES_PER_PACKAGE = 4

TARGET_RACE_SHARD_SECONDS = 145.0
RACE_SPLIT_THRESHOLD_SECONDS = 175.0
MAX_RACE_SHARDS = 12
MAX_RACE_SLICES_PER_PACKAGE = 6

SPLIT_TEST_CHUNK_MAX_BYTES = 28000

# Heavy indirect downstream suites skipped in Tier 1 surgical normal tests unless
# directly touched or one of their subsystem prefixes was touched.
# Note: vet + cross-compile STILL check the full production reverse-import cone.
HEAVY_INDIRECT_SUITES: dict[str, tuple[str, ...]] = {
    "github.com/snissn/gomap/TreeDB/nativewire": (
        "TreeDB/nativewire/",
        "TreeDB/internal/nativewire/",
        "TreeDB/internal/raft",
        "TreeDB/internal/vectorpartition/",
        "TreeDB/internal/sourcepartition/",
        "TreeDB/vectorpartition/",
    ),
    "github.com/snissn/gomap/TreeDB/internal/raftcluster": (
        "TreeDB/internal/raft",
        "TreeDB/nativewire/",
    ),
    "github.com/snissn/gomap/TreeDB/internal/raftplacement": (
        "TreeDB/internal/raft",
        "TreeDB/nativewire/",
    ),
    "github.com/snissn/gomap/cmd/treedb_vector_partition_bench": (
        "cmd/treedb_vector_partition_bench/",
        "TreeDB/vectorpartition/",
        "TreeDB/internal/vectorpartition/",
        "TreeDB/internal/sourcepartition/",
    ),
    "github.com/snissn/gomap/TreeDB/cmd/treedb_rag_benchmark": (
        "TreeDB/cmd/treedb_rag_benchmark/",
    ),
    "github.com/snissn/gomap/cmd/unified_bench": (
        "cmd/unified_bench/",
        "internal/benchprof/",
        "kvstore/",
    ),
}

# Packages excluded from -race runs (pure doc lint / static scripts).
RACE_EXCLUDED_PACKAGES: frozenset[str] = frozenset({
    "github.com/snissn/gomap/TreeDB/docs",
})

# High-risk platform / OS / mmap / durability-IO paths that trigger targeted
# macOS + Windows escalation even on ordinary Ready PRs.
OS_TAG_FILE_RE = re.compile(
    r"_(linux|darwin|windows|unix|bsd|cgo|amd64|arm64|other|fallback|nop)(_[A-Za-z0-9]+)*(_test)?\.go$"
)
OS_BUILD_CONSTRAINT_TOKEN_RE = re.compile(
    r"\b(linux|darwin|windows|unix|freebsd|openbsd|netbsd|dragonfly|solaris|cgo|amd64|arm64)\b"
)
OS_ESCALATION_PREFIXES: tuple[str, ...] = (
    "TreeDB/internal/atomicfile/",
    "TreeDB/internal/stableio/",
    "TreeDB/internal/lockfile/",
    "TreeDB/internal/mappedresource/",
    "TreeDB/internal/rootpublication/",
    "TreeDB/internal/durabilitycut/",
    "TreeDB/internal/powerlosscert/",
    "TreeDB/internal/powerlossoracle/",
    "TreeDB/internal/powerlossreopen/",
    "TreeDB/internal/collectionwal/",
    "TreeDB/internal/commandwalapply/",
    "TreeDB/internal/commandwalbarrier/",
    "TreeDB/pager/",
    "TreeDB/internal/commitlog/",
    "TreeDB/internal/valuelog/",
    "HashDB/internal/lockfile/",
)
OS_ESCALATION_FILE_RE = re.compile(
    r"^(?:"
    r"TreeDB/db/(?:command_wal|durability|durable_|wal_|leaf_generation_manifest|leaf_manifest|ordinary_leaf_manifest|native_command_wal|native_publication|ordered_root_publish|recoverable_root_set|root_publication|system_root_publish|stable_resource|stable_pager|compact_storage|index_swap|physical_snapshot_cut)"
    r"|TreeDB/caching/(?:stable_leaf_resource|txn_journal|cow_checkpoint)"
    r").*\.go$"
)
OS_ESCALATION_EXACT: frozenset[str] = frozenset({
    "go.mod",
    "go.sum",
})

# File extensions that are never considered docs-only even inside docs/ dirs.
CODE_LIKE_SUFFIXES: tuple[str, ...] = (
    ".go",
    ".py",
    ".sh",
    ".bash",
    ".zsh",
    ".yml",
    ".yaml",
    ".c",
    ".h",
    ".cc",
    ".cpp",
    ".rs",
    ".js",
    ".ts",
    ".mjs",
    ".cjs",
    ".proto",
    ".sql",
    ".s",
    ".S",
)
CODE_LIKE_BASENAMES: frozenset[str] = frozenset({
    "Makefile",
    "go.mod",
    "go.sum",
    "go.work",
    "Dockerfile",
})

DOCS_ASSET_PREFIXES: tuple[str, ...] = (
    "docs/",
    "TreeDB/docs/",
    "HashDB/docs/",
)
DOCS_CHECK_TRIGGER_EXACT: frozenset[str] = frozenset({
    "README.md",
    "AGENTS.md",
    "TreeDB/README.md",
    "TreeDB/AGENTS.md",
    "HashDB/README.md",
    "HashDB/AGENTS.md",
    "scripts/docs_check.sh",
    "TreeDB/collections/typed_storage_naming_test.go",
})
PYTHON_CLIENT_TRIGGER_PREFIXES: tuple[str, ...] = (
    "clients/python/",
    "TreeDB/nativewire/",
    "cmd/treedb-native-server/",
)
WINDOWS_SUPPORTED_PREFIXES: tuple[str, ...] = (
    "github.com/snissn/gomap/TreeDB",
    "github.com/snissn/gomap/HashDB",
)


def is_docs_only_path(path: str) -> bool:
    """Return True only when a changed path is unambiguously non-code docs/report."""
    p = path.strip("/")
    if not p:
        return False
    base = os.path.basename(p)
    if base in CODE_LIKE_BASENAMES:
        return False
    lower = p.lower()
    if any(lower.endswith(ext) for ext in CODE_LIKE_SUFFIXES):
        return False
    if lower.endswith(".md"):
        return True
    if base in ("LICENSE", "LICENSE.txt", "CHANGELOG", "AUTHORS", "NOTICE"):
        return True
    if any(p.startswith(prefix) for prefix in DOCS_ASSET_PREFIXES):
        # Allow benchmark reports, plots, and JSON/CSV evidence inside docs/ trees,
        # but fail closed on spec schemas other than pure docs assets.
        if lower.endswith((".txt", ".png", ".svg", ".jpg", ".jpeg", ".gif", ".webp", ".csv", ".tsv", ".pdf")):
            return True
        if lower.endswith(".json") and (
            "/benchmarks/" in p or "/evidence/" in p or p.startswith(("docs/benchmarks/", "docs/evidence/"))
        ):
            return True
    return False


def file_has_os_build_constraint(repo: Path | None, path: str) -> bool:
    """Return True if a changed .go file carries an OS/arch/cgo build constraint."""
    if repo is None or not path.endswith(".go"):
        return False
    full = repo / path
    if not full.is_file():
        return False
    try:
        with full.open("r", encoding="utf-8", errors="ignore") as fh:
            for idx, line in enumerate(fh):
                if idx >= 40:
                    break
                stripped = line.strip()
                if stripped.startswith("package "):
                    break
                if stripped.startswith(("//go:build", "// +build")):
                    if OS_BUILD_CONSTRAINT_TOKEN_RE.search(stripped):
                        return True
    except OSError:
        return False
    return False


def is_os_escalation_path(path: str, repo: Path | None = None) -> bool:
    p = path.strip("/")
    if p in OS_ESCALATION_EXACT:
        return True
    if any(p.startswith(prefix) for prefix in OS_ESCALATION_PREFIXES):
        return True
    if OS_TAG_FILE_RE.search(p):
        return True
    if OS_ESCALATION_FILE_RE.search(p):
        return True
    if file_has_os_build_constraint(repo, p):
        return True
    return False


def is_docs_check_trigger(path: str) -> bool:
    p = path.strip("/")
    if p in DOCS_CHECK_TRIGGER_EXACT:
        return True
    if p.lower().endswith(".md"):
        return True
    if any(p.startswith(prefix) for prefix in DOCS_ASSET_PREFIXES):
        return True
    return False


def is_windows_supported_package(pkg: str) -> bool:
    return any(
        pkg == prefix or pkg.startswith(prefix + "/")
        for prefix in WINDOWS_SUPPORTED_PREFIXES
    )


def git_changed_files(repo: Path, base_sha: str, head_sha: str) -> list[str] | None:
    """Return changed file paths between base_sha and head_sha, or None on error."""
    if not base_sha or not head_sha:
        return None
    try:
        # Ensure base commit is available (PR merge checkout with fetch-depth=2 has it).
        check = subprocess.run(
            ["git", "cat-file", "-e", f"{base_sha}^{{commit}}"],
            cwd=repo,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
        )
        if check.returncode != 0:
            subprocess.run(
                ["git", "fetch", "--no-tags", "--depth=1", "origin", base_sha],
                cwd=repo,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=60,
            )
        # Prefer merge-base diff when ancestry exists; fall back to two-dot diff.
        proc = subprocess.run(
            ["git", "diff", "--name-only", f"{base_sha}...{head_sha}"],
            cwd=repo,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if proc.returncode != 0:
            proc = subprocess.run(
                ["git", "diff", "--name-only", base_sha, head_sha],
                cwd=repo,
                capture_output=True,
                text=True,
                timeout=30,
            )
        if proc.returncode != 0:
            return None
        return sorted({line.strip() for line in proc.stdout.splitlines() if line.strip()})
    except (OSError, subprocess.SubprocessError):
        return None


def discover_go_packages(repo: Path) -> tuple[dict[str, str], dict[str, list[str]]]:
    """Return (pkg_to_rel_dir, pkg_to_prod_imports) for the root Go module."""
    env = dict(os.environ, GOWORK="off")
    proc = subprocess.run(
        ["go", "list", "-f", "{{.ImportPath}}\t{{.Dir}}\t{{join .Imports \",\"}}", "./..."],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
        check=True,
        timeout=180,
    )
    repo_resolved = repo.resolve()
    pkg_to_dir: dict[str, str] = {}
    pkg_to_imports: dict[str, list[str]] = {}
    for line in proc.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        ip, abs_dir, imps = parts[0].strip(), parts[1].strip(), parts[2].strip()
        if not ip:
            continue
        try:
            rel = Path(abs_dir).resolve().relative_to(repo_resolved).as_posix()
        except ValueError:
            rel = abs_dir
        if rel == ".":
            rel = ""
        pkg_to_dir[ip] = rel
        pkg_to_imports[ip] = [
            d for d in (imps.split(",") if imps else [])
            if d == MODULE_PREFIX or d.startswith(MODULE_PREFIX + "/")
        ]
    return pkg_to_dir, pkg_to_imports


def build_reverse_prod_imports(pkg_to_imports: dict[str, list[str]]) -> dict[str, set[str]]:
    rdeps: dict[str, set[str]] = {}
    for pkg, imports in pkg_to_imports.items():
        for dep in imports:
            rdeps.setdefault(dep, set()).add(pkg)
    return rdeps


def transitive_closure(roots: set[str], adj: dict[str, set[str]]) -> set[str]:
    seen = set(roots)
    stack = list(roots)
    while stack:
        cur = stack.pop()
        for nxt in adj.get(cur, ()):
            if nxt not in seen:
                seen.add(nxt)
                stack.append(nxt)
    return seen


def pack_lpt(packages: list[str], weights: dict[str, float], shard_count: int) -> list[list[str]]:
    """Longest-processing-time-first deterministic bin packing."""
    if not packages or shard_count <= 0:
        return []
    count = min(shard_count, len(packages))
    bins: list[list[str]] = [[] for _ in range(count)]
    loads: list[float] = [0.0 for _ in range(count)]
    ordered = sorted(packages, key=lambda p: (-weights.get(p, 2.0), p))
    for pkg in ordered:
        idx = min(range(count), key=lambda i: (loads[i], i))
        bins[idx].append(pkg)
        loads[idx] += weights.get(pkg, 2.0)
    return [sorted(b) for b in bins if b]


def build_compile_shards(packages: list[str], *, include_empty_zero: bool = False) -> list[dict[str, Any]]:
    """Split large vet/cross-compile cones into 2 parallel shards when >= 45 packages."""
    if not packages:
        return [{"id": 0, "packages": []}] if include_empty_zero else []
    n_shards = 2 if len(packages) >= COMPILE_SHARD_MIN_PACKAGES else 1
    if n_shards == 1:
        return [{"id": 0, "packages": sorted(packages)}]
    bins = pack_lpt(packages, NORMAL_WEIGHTS, n_shards)
    return [{"id": i, "packages": b} for i, b in enumerate(bins)]


def build_weighted_shards(
    packages: list[str],
    weights: dict[str, float],
    *,
    target_shard_seconds: float,
    split_threshold: float,
    max_shards: int,
    max_slices_per_pkg: int,
    default_weight: float,
    slice_compile_overhead: float = 0.0,
    initial_shard0_load: float = 0.0,
) -> list[dict[str, Any]]:
    """Pack whole packages and split-package test slices via LPT toward target wall-clock."""
    if not packages:
        return []
    work_items: list[dict[str, Any]] = []
    max_single_slices = 1
    for pkg in sorted(packages):
        w = weights.get(pkg, default_weight)
        if w > split_threshold and max_shards > 1 and max_slices_per_pkg > 1:
            slices = min(
                max_slices_per_pkg,
                max_shards,
                max(2, math.ceil(w / target_shard_seconds)),
            )
            max_single_slices = max(max_single_slices, slices)
            slice_w = w / slices
            for idx in range(slices):
                work_items.append({
                    "kind": "slice",
                    "package": pkg,
                    "index": idx,
                    "count": slices,
                    "weight": slice_w,
                    "pack_weight": slice_w + slice_compile_overhead,
                })
        else:
            work_items.append({
                "kind": "package",
                "package": pkg,
                "index": 0,
                "count": 1,
                "weight": w,
                "pack_weight": w + (slice_compile_overhead * 0.5 if w >= 60.0 else 0.0),
            })

    total_pack_work = sum(item["pack_weight"] for item in work_items) + initial_shard0_load
    budget_per_shard = target_shard_seconds + slice_compile_overhead
    heavy_item_count = sum(
        1 for item in work_items if item["kind"] == "slice" or item["weight"] >= target_shard_seconds * 0.75
    )
    n_shards = max(max_single_slices, heavy_item_count, math.ceil(total_pack_work / budget_per_shard))
    n_shards = min(max_shards, max(len(work_items), 1 if initial_shard0_load > 0 else 0), max(1, n_shards))

    pkg_bins: list[list[str]] = [[] for _ in range(n_shards)]
    slice_bins: list[list[dict[str, Any]]] = [[] for _ in range(n_shards)]
    slice_pkgs_in_bin: list[set[str]] = [set() for _ in range(n_shards)]
    pack_loads: list[float] = [initial_shard0_load] + [0.0] * (n_shards - 1)
    run_loads: list[float] = [initial_shard0_load] + [0.0] * (n_shards - 1)

    ordered = sorted(
        work_items,
        key=lambda it: (-it["pack_weight"], 0 if it["kind"] == "slice" else 1, it["package"], it["index"]),
    )
    for item in ordered:
        if item["kind"] == "slice":
            # Prefer bins not already holding another slice of the same package.
            idx = min(
                range(n_shards),
                key=lambda i: (1 if item["package"] in slice_pkgs_in_bin[i] else 0, pack_loads[i], i),
            )
            slice_bins[idx].append({
                "package": item["package"],
                "index": item["index"],
                "count": item["count"],
            })
            slice_pkgs_in_bin[idx].add(item["package"])
        else:
            idx = min(range(n_shards), key=lambda i: (pack_loads[i], i))
            pkg_bins[idx].append(item["package"])
        pack_loads[idx] += item["pack_weight"]
        run_loads[idx] += item["weight"]

    shards: list[dict[str, Any]] = []
    for i in range(n_shards):
        if not pkg_bins[i] and not slice_bins[i] and not (i == 0 and initial_shard0_load > 0):
            continue
        shards.append({
            "id": len(shards),
            "packages": sorted(pkg_bins[i]),
            "slices": sorted(slice_bins[i], key=lambda s: (s["package"], s["index"])),
            "estimated_seconds": round(run_loads[i], 1),
        })
    return shards


def partition_split_tests(pkg: str, tests: list[str], split_index: int, split_count: int) -> list[str]:
    """Deterministically partition a package's tests across split_count slices via LPT."""
    if split_count <= 1:
        return sorted(set(tests))
    unique_tests = sorted(set(tests))
    slow_weights = KNOWN_SLOW_TEST_WEIGHTS.get(pkg, {})
    bins: list[list[str]] = [[] for _ in range(split_count)]
    loads: list[float] = [0.0 for _ in range(split_count)]
    ordered = sorted(unique_tests, key=lambda name: (-slow_weights.get(name, 1.0), name))
    for name in ordered:
        idx = min(range(split_count), key=lambda i: (loads[i], i))
        bins[idx].append(name)
        loads[idx] += slow_weights.get(name, 1.0)
    return sorted(bins[split_index])


def discover_local_replace_dirs(repo: Path) -> tuple[str, ...]:
    """Parse root go.mod for local filesystem replace directives (e.g. third_party/...)."""
    go_mod = repo / "go.mod"
    if not go_mod.is_file():
        return ()
    dirs: set[str] = set()
    in_replace = False
    for raw_line in go_mod.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("//", 1)[0].strip()
        if not line:
            continue
        if line.startswith("replace ("):
            in_replace = True
            continue
        if in_replace and line == ")":
            in_replace = False
            continue
        if line.startswith("replace ") or in_replace:
            body = line[len("replace "):].strip() if line.startswith("replace ") else line
            if "=>" not in body:
                continue
            target = body.split("=>", 1)[1].strip().split()[0].strip('"')
            if target.startswith(("./", "../", "/")):
                try:
                    rel = (repo / target).resolve().relative_to(repo.resolve()).as_posix().strip("/")
                except ValueError:
                    continue
                if rel and rel != ".":
                    dirs.add(rel + "/")
    return tuple(sorted(dirs))


def build_plan(
    *,
    event_name: str,
    changed_files: list[str] | None,
    is_draft: bool,
    pr_title: str,
    pr_labels: list[str],
    dispatch_mode: str,
    pkg_to_dir: dict[str, str],
    pkg_to_imports: dict[str, list[str]],
    local_replace_dirs: tuple[str, ...] = (),
    repo: Path | None = None,
    base_sha: str = "",
    head_sha: str = "",
) -> dict[str, Any]:
    all_packages = sorted(pkg_to_dir.keys())
    dir_to_pkg = {d: p for p, d in pkg_to_dir.items()}
    rdeps = build_reverse_prod_imports(pkg_to_imports)

    ci_full = (
        event_name == "pull_request"
        and (("[ci full]" in pr_title.lower()) or ("ci:full" in pr_labels))
    )
    files = changed_files if changed_files is not None else []
    diff_known = changed_files is not None and len(changed_files) > 0

    docs_only = (
        event_name == "pull_request"
        and not ci_full
        and diff_known
        and all(is_docs_only_path(f) for f in files)
    )
    touches_local_replace = bool(local_replace_dirs) and any(
        any(f.strip("/").startswith(prefix) for prefix in local_replace_dirs)
        for f in files
    )
    touches_mod = touches_local_replace or any(f in ("go.mod", "go.sum") for f in files)

    if event_name in ("push", "merge_group") or dispatch_mode == "main_fast":
        tier = "main_fast"
    elif event_name == "pull_request" and docs_only:
        tier = "docs_only"
    elif event_name == "pull_request" and is_draft and not ci_full:
        tier = "draft"
    elif not diff_known or touches_mod:
        # Fail closed when PR diff is unavailable, go.mod/go.sum changed, or a
        # local go.mod replace module (e.g. third_party/hashicorp_raft_*) changed.
        tier = "main_fast"
    else:
        tier = "surgical"

    # Map changed files to directly touched packages (and track which touched
    # packages actually contain OS/durability-escalating files).
    touched_packages_set: set[str] = set()
    os_escalated_packages_set: set[str] = set()
    has_unmapped_os_escalation = False
    for f in files:
        file_escalates_os = is_os_escalation_path(f, repo=repo)
        parent = Path(f).parent.as_posix()
        if parent == ".":
            parent = ""
        # Walk up to nearest enclosing Go package dir (captures testdata/ fixtures too).
        cur = parent
        mapped_pkg: str | None = None
        while True:
            if cur in dir_to_pkg:
                mapped_pkg = dir_to_pkg[cur]
                break
            if not cur:
                break
            nxt = Path(cur).parent.as_posix()
            cur = "" if nxt == "." else nxt
        if mapped_pkg is not None:
            touched_packages_set.add(mapped_pkg)
            if file_escalates_os:
                os_escalated_packages_set.add(mapped_pkg)
        elif file_escalates_os:
            has_unmapped_os_escalation = True

    touched_packages = sorted(touched_packages_set)
    prod_cone = transitive_closure(touched_packages_set, rdeps)

    # Determine vet / cross-compile / test / race / OS-escalated package sets.
    if tier == "docs_only":
        vet_packages: list[str] = []
        test_packages: list[str] = []
        race_packages: list[str] = []
        escalate_os = False
        os_macos_packages: list[str] = []
        os_windows_packages: list[str] = []
        run_docs_check = True
        run_ci_script_tests = any(f.startswith((".github/", "scripts/")) for f in files)
        run_python_client = False
    elif tier == "draft":
        vet_packages = all_packages if touches_mod else (touched_packages if touched_packages else [])
        test_packages = []
        race_packages = []
        escalate_os = False
        os_macos_packages = []
        os_windows_packages = []
        run_docs_check = any(is_docs_check_trigger(f) for f in files)
        run_ci_script_tests = any(f.startswith((".github/", "scripts/")) for f in files)
        run_python_client = False
    elif tier == "main_fast":
        vet_packages = all_packages
        test_packages = all_packages
        race_packages = []
        # If a Ready PR touched go.mod/go.sum, also escalate core smoke on macOS + Windows.
        escalate_os = event_name == "pull_request" and touches_mod
        os_macos_packages = (
            [
                p for p in (
                    "github.com/snissn/gomap/HashDB",
                    "github.com/snissn/gomap/TreeDB/db",
                    "github.com/snissn/gomap/TreeDB/caching",
                    "github.com/snissn/gomap/cmd/unified_bench",
                )
                if p in pkg_to_dir
            ]
            if escalate_os
            else []
        )
        os_windows_packages = [p for p in os_macos_packages if is_windows_supported_package(p)]
        run_docs_check = True
        run_ci_script_tests = True
        run_python_client = True
    else:  # surgical (Tier 1 Ready PR)
        vet_packages = sorted(prod_cone)
        # Exclude heavy indirect downstream suites unless their subsystem was touched.
        surgical_tests: list[str] = []
        for pkg in sorted(prod_cone):
            if pkg in touched_packages_set:
                surgical_tests.append(pkg)
                continue
            prefixes = HEAVY_INDIRECT_SUITES.get(pkg)
            if prefixes is not None:
                if any(f.startswith(prefixes) for f in files):
                    surgical_tests.append(pkg)
                continue
            surgical_tests.append(pkg)
        test_packages = surgical_tests
        race_packages = [p for p in touched_packages if p not in RACE_EXCLUDED_PACKAGES]
        escalate_os = (bool(os_escalated_packages_set) or has_unmapped_os_escalation) and bool(touched_packages)
        if escalate_os:
            os_macos_packages = (
                touched_packages
                if (has_unmapped_os_escalation or not os_escalated_packages_set)
                else sorted(os_escalated_packages_set)
            )
        else:
            os_macos_packages = []
        os_windows_packages = [p for p in os_macos_packages if is_windows_supported_package(p)]
        run_docs_check = any(is_docs_check_trigger(f) for f in files)
        run_ci_script_tests = any(f.startswith((".github/", "scripts/")) for f in files)
        run_python_client = any(f.startswith(PYTHON_CLIENT_TRIGGER_PREFIXES) for f in files)

    darwin_cross_packages = list(vet_packages)
    windows_cross_packages = [p for p in vet_packages if is_windows_supported_package(p)]

    if tier in ("surgical", "main_fast"):
        vet_shards = build_compile_shards(vet_packages, include_empty_zero=run_ci_script_tests)
        cross_darwin_shards = build_compile_shards(darwin_cross_packages)
        cross_windows_shards = build_compile_shards(windows_cross_packages)
    else:
        vet_shards = []
        cross_darwin_shards = []
        cross_windows_shards = []

    # Build normal test shards (split-aware LPT weighted toward ~4m wall-clock).
    total_normal_weight = sum(NORMAL_WEIGHTS.get(p, 1.5) for p in test_packages)
    initial_test_shard0_load = 120.0 if run_python_client else 0.0
    if not test_packages:
        test_shards: list[dict[str, Any]] = []
    elif tier == "main_fast":
        test_shards = build_weighted_shards(
            test_packages,
            NORMAL_WEIGHTS,
            target_shard_seconds=TARGET_MAIN_NORMAL_SHARD_SECONDS,
            split_threshold=NORMAL_SPLIT_THRESHOLD_SECONDS,
            max_shards=MAX_MAIN_TEST_SHARDS,
            max_slices_per_pkg=MAX_NORMAL_SLICES_PER_PACKAGE,
            default_weight=1.5,
            slice_compile_overhead=25.0,
            initial_shard0_load=initial_test_shard0_load,
        )
    else:
        test_shards = build_weighted_shards(
            test_packages,
            NORMAL_WEIGHTS,
            target_shard_seconds=TARGET_NORMAL_SHARD_SECONDS,
            split_threshold=NORMAL_SPLIT_THRESHOLD_SECONDS,
            max_shards=MAX_SURGICAL_TEST_SHARDS,
            max_slices_per_pkg=MAX_NORMAL_SLICES_PER_PACKAGE,
            default_weight=1.5,
            slice_compile_overhead=25.0,
            initial_shard0_load=initial_test_shard0_load,
        )

    # Build race shards (touched packages only; split-aware LPT toward ~4-5m wall-clock).
    total_race_weight = sum(RACE_WEIGHTS.get(p, 5.0) for p in race_packages)
    race_shards = build_weighted_shards(
        race_packages,
        RACE_WEIGHTS,
        target_shard_seconds=TARGET_RACE_SHARD_SECONDS,
        split_threshold=RACE_SPLIT_THRESHOLD_SECONDS,
        max_shards=MAX_RACE_SHARDS,
        max_slices_per_pkg=MAX_RACE_SLICES_PER_PACKAGE,
        default_weight=5.0,
        slice_compile_overhead=45.0,
    )

    referenced_pkgs = set(test_packages) | set(race_packages)
    pkg_dirs = {p: pkg_to_dir[p] for p in sorted(referenced_pkgs) if p in pkg_to_dir}

    return {
        "schema": 2,
        "event_name": event_name,
        "base_sha": base_sha,
        "head_sha": head_sha,
        "tier": tier,
        "ci_full": ci_full,
        "is_draft": is_draft,
        "docs_only": docs_only,
        "changed_file_count": len(files),
        "changed_files_sample": files[:50],
        "touched_packages": touched_packages,
        "vet_packages": vet_packages,
        "vet_shards": vet_shards,
        "darwin_cross_packages": darwin_cross_packages,
        "cross_darwin_shards": cross_darwin_shards,
        "windows_cross_packages": windows_cross_packages,
        "cross_windows_shards": cross_windows_shards,
        "test_packages": test_packages,
        "test_shards": test_shards,
        "race_packages": race_packages,
        "race_shards": race_shards,
        "pkg_dirs": pkg_dirs,
        "escalate_os": escalate_os,
        "os_macos_packages": os_macos_packages,
        "os_windows_packages": os_windows_packages,
        "run_docs_check": run_docs_check,
        "run_ci_script_tests": run_ci_script_tests,
        "run_python_client": run_python_client,
        "estimated_normal_seconds": round(total_normal_weight, 1),
        "estimated_race_seconds": round(total_race_weight, 1),
    }


def write_github_outputs(plan: dict[str, Any], path: Path) -> None:
    vet_ids = [s["id"] for s in plan["vet_shards"]]
    cross_darwin_ids = [s["id"] for s in plan["cross_darwin_shards"]]
    cross_windows_ids = [s["id"] for s in plan["cross_windows_shards"]]
    test_ids = [s["id"] for s in plan["test_shards"]]
    race_ids = [s["id"] for s in plan["race_shards"]]
    os_runners: list[str] = []
    if plan["escalate_os"]:
        if plan["os_macos_packages"]:
            os_runners.append("macos-latest")
        if plan["os_windows_packages"]:
            os_runners.append("windows-latest")

    run_vet = bool(vet_ids)
    run_cross_darwin = bool(cross_darwin_ids)
    run_cross_windows = bool(cross_windows_ids)
    run_test = bool(test_ids) or plan["run_python_client"]
    run_race = bool(race_ids)
    escalate_os = bool(os_runners)
    # GitHub Actions rejects empty matrix vectors even on skipped jobs; always
    # emit a safe non-empty fallback vector gated by the boolean run_* flag.
    effective_vet_ids = vet_ids if vet_ids else [0]
    effective_cross_darwin_ids = cross_darwin_ids if cross_darwin_ids else [0]
    effective_cross_windows_ids = cross_windows_ids if cross_windows_ids else [0]
    effective_test_ids = test_ids if test_ids else [0]
    effective_race_ids = race_ids if race_ids else [0]
    effective_os_runners = os_runners if os_runners else ["ubuntu-latest"]

    lines = [
        f"tier={plan['tier']}",
        f"ci_full={'true' if plan['ci_full'] else 'false'}",
        f"docs_only={'true' if plan['docs_only'] else 'false'}",
        f"run_docs_check={'true' if plan['run_docs_check'] else 'false'}",
        f"run_ci_script_tests={'true' if plan['run_ci_script_tests'] else 'false'}",
        f"run_python_client={'true' if plan['run_python_client'] else 'false'}",
        f"run_vet={'true' if run_vet else 'false'}",
        f"vet_shards_json={json.dumps(effective_vet_ids)}",
        f"run_cross_darwin={'true' if run_cross_darwin else 'false'}",
        f"cross_darwin_shards_json={json.dumps(effective_cross_darwin_ids)}",
        f"run_cross_windows={'true' if run_cross_windows else 'false'}",
        f"cross_windows_shards_json={json.dumps(effective_cross_windows_ids)}",
        f"run_test={'true' if run_test else 'false'}",
        f"test_shards_json={json.dumps(effective_test_ids)}",
        f"run_race={'true' if run_race else 'false'}",
        f"race_shards_json={json.dumps(effective_race_ids)}",
        f"escalate_os={'true' if escalate_os else 'false'}",
        f"os_runners_json={json.dumps(effective_os_runners)}",
    ]
    with path.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def write_step_summary(plan: dict[str, Any], path: Path) -> None:
    max_test_est = max((s.get("estimated_seconds", 0.0) for s in plan["test_shards"]), default=0.0)
    max_race_est = max((s.get("estimated_seconds", 0.0) for s in plan["race_shards"]), default=0.0)
    lines = [
        "### CI Scope Plan",
        "",
        f"- **Tier:** `{plan['tier']}` (`docs_only={plan['docs_only']}`, `draft={plan['is_draft']}`, `ci_full={plan['ci_full']}`)",
        f"- **Changed files:** {plan['changed_file_count']}",
        f"- **Directly touched Go packages:** {len(plan['touched_packages'])}",
        f"- **Vet / cross-compile cones:** vet={len(plan['vet_packages'])} ({len(plan['vet_shards'])} shard), darwin={len(plan['darwin_cross_packages'])} ({len(plan['cross_darwin_shards'])} shard), windows={len(plan['windows_cross_packages'])} ({len(plan['cross_windows_shards'])} shard)",
        f"- **Normal Linux test packages:** {len(plan['test_packages'])} across {len(plan['test_shards'])} shard(s) (total est. {plan['estimated_normal_seconds']}s, max shard est. {max_test_est}s)",
        f"- **Touched `-race` packages:** {len(plan['race_packages'])} across {len(plan['race_shards'])} shard(s) (total est. {plan.get('estimated_race_seconds', 0.0)}s, max shard est. {max_race_est}s)",
        f"- **OS escalation:** `{plan['escalate_os']}` (macOS: {len(plan['os_macos_packages'])} pkg, Windows: {len(plan['os_windows_packages'])} pkg)",
        f"- **Extra gates:** `docs_check={plan['run_docs_check']}`, `ci_script_tests={plan['run_ci_script_tests']}`, `python_client={plan['run_python_client']}`",
    ]
    if plan["touched_packages"]:
        short = [p.removeprefix(MODULE_PREFIX + "/") for p in plan["touched_packages"][:20]]
        lines.append(f"- **Touched sample:** `{'`, `'.join(short)}`")
    lines.append("")
    with path.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def _run_cmd(cmd: list[str], *, cwd: Path, env: dict[str, str], jsonl_path: Path | None = None) -> int:
    print("+", " ".join(cmd), flush=True)
    if jsonl_path is None:
        return subprocess.run(cmd, cwd=cwd, env=env).returncode
    with jsonl_path.open("a", encoding="utf-8") as logf:
        proc = subprocess.Popen(cmd, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            logf.write(line)
        return proc.wait()


def _resolve_package_dir(repo: Path, plan: dict[str, Any], pkg: str) -> Path:
    rel = (plan.get("pkg_dirs") or {}).get(pkg)
    if rel is not None:
        return repo / rel if rel else repo
    if pkg == MODULE_PREFIX:
        return repo
    if pkg.startswith(MODULE_PREFIX + "/"):
        return repo / pkg[len(MODULE_PREFIX) + 1:]
    return repo


def _run_package_slice(
    pkg: str,
    split_index: int,
    split_count: int,
    *,
    race: bool,
    timeout: str,
    repo: Path,
    plan: dict[str, Any],
    compile_env: dict[str, str],
    run_env: dict[str, str],
    jsonl_path: Path | None,
) -> int:
    """Compile a test binary once with full runner vCPUs and execute one weighted slice via test2json."""
    pkg_dir = _resolve_package_dir(repo, plan, pkg)
    with tempfile.TemporaryDirectory(prefix="ci-slice-") as td:
        bin_path = Path(td) / "pkg.test"
        compile_cmd = ["go", "test", "-vet=off", "-c", "-o", str(bin_path), pkg]
        if race:
            compile_cmd.insert(2, "-race")
        rc = _run_cmd(compile_cmd, cwd=repo, env=compile_env)
        if rc != 0:
            return rc
        if not bin_path.is_file():
            print(f"No test binary produced for {pkg}; skipping slice {split_index}/{split_count}.", flush=True)
            return 0

        list_cmd = [str(bin_path), "-test.list", "^(Test|Example|Fuzz).*"]
        print("+", " ".join(list_cmd), flush=True)
        listed = subprocess.run(list_cmd, cwd=pkg_dir, env=run_env, capture_output=True, text=True)
        if listed.returncode != 0:
            sys.stdout.write(listed.stdout)
            sys.stderr.write(listed.stderr)
            return listed.returncode
        tests = [
            line.strip()
            for line in listed.stdout.splitlines()
            if line.strip().startswith(("Test", "Example", "Fuzz"))
        ]
        selected = partition_split_tests(pkg, tests, split_index, split_count)
        if not selected:
            print(f"No tests assigned to {pkg} slice {split_index}/{split_count}", flush=True)
            return 0

        chunks: list[list[str]] = []
        chunk: list[str] = []
        chunk_len = 0
        for name in selected:
            escaped = re.escape(name)
            if chunk and chunk_len + len(escaped) + 1 > SPLIT_TEST_CHUNK_MAX_BYTES:
                chunks.append(chunk)
                chunk = [escaped]
                chunk_len = len(escaped)
            else:
                chunk.append(escaped)
                chunk_len += len(escaped) + 1
        if chunk:
            chunks.append(chunk)

        for idx, part in enumerate(chunks):
            regex = "^(" + "|".join(part) + ")$"
            print(
                f"+ go tool test2json -t -p {pkg} {bin_path.name} -test.v -test.timeout {timeout} "
                f"-test.run <{len(part)} tests chunk {idx + 1}/{len(chunks)}>",
                flush=True,
            )
            cmd = [
                "go", "tool", "test2json", "-t", "-p", pkg,
                str(bin_path), "-test.v", "-test.timeout", timeout, "-test.run", regex,
            ]
            if jsonl_path is None:
                rc = subprocess.run(cmd, cwd=pkg_dir, env=run_env).returncode
            else:
                with jsonl_path.open("a", encoding="utf-8") as logf:
                    proc = subprocess.Popen(
                        cmd, cwd=pkg_dir, env=run_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
                    )
                    assert proc.stdout is not None
                    for line in proc.stdout:
                        sys.stdout.write(line)
                        sys.stdout.flush()
                        logf.write(line)
                    rc = proc.wait()
            if rc != 0:
                return rc
        return 0


def cmd_plan(args: argparse.Namespace) -> int:
    repo = Path(args.repo).resolve()
    labels: list[str] = []
    if args.pr_labels_json:
        try:
            raw = json.loads(args.pr_labels_json)
            if isinstance(raw, list):
                labels = [str(x) for x in raw]
        except json.JSONDecodeError:
            labels = []

    changed = git_changed_files(repo, args.base_sha, args.head_sha) if args.event_name == "pull_request" else []
    pkg_to_dir, pkg_to_imports = discover_go_packages(repo)
    local_replace_dirs = discover_local_replace_dirs(repo)
    plan = build_plan(
        event_name=args.event_name,
        changed_files=changed,
        is_draft=(args.is_draft.lower() == "true"),
        pr_title=args.pr_title or "",
        pr_labels=labels,
        dispatch_mode=args.dispatch_mode or "",
        pkg_to_dir=pkg_to_dir,
        pkg_to_imports=pkg_to_imports,
        local_replace_dirs=local_replace_dirs,
        repo=repo,
        base_sha=args.base_sha or "",
        head_sha=args.head_sha or "",
    )
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")

    if args.github_output:
        write_github_outputs(plan, Path(args.github_output))
    if args.step_summary:
        write_step_summary(plan, Path(args.step_summary))
    print(f"Planned CI tier={plan['tier']} touched={len(plan['touched_packages'])} test={len(plan['test_packages'])} race={len(plan['race_packages'])} escalate_os={plan['escalate_os']}")
    return 0


def cmd_run_shard(args: argparse.Namespace) -> int:
    repo = Path(args.repo).resolve()
    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    # Runtime test/race shards cap GOMAXPROCS=2 and GOMEMLIMIT=4GiB to avoid RSS
    # spikes across concurrent memtables; compile-only steps use all runner vCPUs.
    compile_env = dict(os.environ, GOWORK="off", GOMEMLIMIT="6GiB")
    compile_env.pop("GOMAXPROCS", None)
    env = dict(os.environ, GOWORK="off", GOMEMLIMIT="4GiB", GOMAXPROCS="2")
    jsonl_path = Path(args.jsonl) if args.jsonl else None

    if args.kind == "draft":
        pkgs = plan.get("vet_packages", [])
        if not pkgs:
            print("Draft PR touches no Go packages; compile/vet skipped.")
            return 0
        rc = _run_cmd(["go", "vet", *pkgs], cwd=repo, env=compile_env)
        if rc != 0:
            return rc
        return _run_cmd(["go", "test", "-vet=off", "-ldflags=-s -w", "-exec=true", "-run=^$", *pkgs], cwd=repo, env=compile_env)

    if args.kind == "vet":
        shards = plan.get("vet_shards")
        if shards is not None:
            shard = next((s for s in shards if s["id"] == args.shard_index), None)
            pkgs = shard.get("packages", []) if shard else []
        else:
            pkgs = plan.get("vet_packages", [])
        if not pkgs:
            print(f"No Go packages assigned to vet shard {args.shard_index}.")
            return 0
        return _run_cmd(["go", "vet", *pkgs], cwd=repo, env=compile_env)

    if args.kind == "cross-darwin":
        shards = plan.get("cross_darwin_shards")
        if shards is not None:
            shard = next((s for s in shards if s["id"] == args.shard_index), None)
            pkgs = shard.get("packages", []) if shard else []
        else:
            pkgs = plan.get("darwin_cross_packages", [])
        if not pkgs:
            print(f"No Go packages assigned to darwin cross-compile shard {args.shard_index}.")
            return 0
        # Compile-only cross-OS check (-vet=off); Linux vet runs in the parallel vet job
        # and native darwin/windows vet runs in os-escalated and Tier 2 sweeps.
        xenv = dict(compile_env, CGO_ENABLED="0", GOOS="darwin", GOARCH="arm64")
        return _run_cmd(["go", "test", "-vet=off", "-ldflags=-s -w", "-exec=true", "-run=^$", *pkgs], cwd=repo, env=xenv)

    if args.kind == "cross-windows":
        shards = plan.get("cross_windows_shards")
        if shards is not None:
            shard = next((s for s in shards if s["id"] == args.shard_index), None)
            pkgs = shard.get("packages", []) if shard else []
        else:
            pkgs = plan.get("windows_cross_packages", [])
        if not pkgs:
            print(f"No Windows-supported Go packages assigned to shard {args.shard_index}.")
            return 0
        xenv = dict(compile_env, CGO_ENABLED="0", GOOS="windows", GOARCH="amd64")
        return _run_cmd(["go", "test", "-vet=off", "-ldflags=-s -w", "-exec=true", "-run=^$", *pkgs], cwd=repo, env=xenv)

    if args.kind == "test":
        shards = plan.get("test_shards", [])
        shard = next((s for s in shards if s["id"] == args.shard_index), None)
        if shard is None or (not shard.get("packages") and not shard.get("slices")):
            print(f"No Go test packages assigned to shard {args.shard_index}.")
            return 0
        if shard.get("packages"):
            # Pre-compile all packages in parallel using full runner vCPUs (skips redundant vet),
            # then execute sequentially (-p 1) under bounded GOMAXPROCS/GOMEMLIMIT.
            rc = _run_cmd(
                ["go", "test", "-vet=off", "-exec=true", "-run=^$", *shard["packages"]],
                cwd=repo,
                env=compile_env,
            )
            if rc != 0:
                return rc
            rc = _run_cmd(
                ["go", "test", "-vet=off", "-json", "-count=1", "-timeout", "25m", "-p", "1", *shard["packages"]],
                cwd=repo,
                env=env,
                jsonl_path=jsonl_path,
            )
            if rc != 0:
                return rc
        for sl in shard.get("slices", []):
            rc = _run_package_slice(
                sl["package"],
                sl["index"],
                sl["count"],
                race=False,
                timeout="20m",
                repo=repo,
                plan=plan,
                compile_env=compile_env,
                run_env=env,
                jsonl_path=jsonl_path,
            )
            if rc != 0:
                return rc
        return 0

    if args.kind == "race":
        shards = plan.get("race_shards", [])
        shard = next((s for s in shards if s["id"] == args.shard_index), None)
        if shard is None or (not shard.get("packages") and not shard.get("slices")):
            print(f"No race shard {args.shard_index}.")
            return 0
        if shard.get("packages"):
            rc = _run_cmd(
                ["go", "test", "-race", "-vet=off", "-exec=true", "-run=^$", *shard["packages"]],
                cwd=repo,
                env=compile_env,
            )
            if rc != 0:
                return rc
            rc = _run_cmd(
                ["go", "test", "-race", "-vet=off", "-json", "-count=1", "-p", "1", "-timeout", "15m", *shard["packages"]],
                cwd=repo,
                env=env,
                jsonl_path=jsonl_path,
            )
            if rc != 0:
                return rc
        for sl in shard.get("slices", []):
            rc = _run_package_slice(
                sl["package"],
                sl["index"],
                sl["count"],
                race=True,
                timeout="12m",
                repo=repo,
                plan=plan,
                compile_env=compile_env,
                run_env=env,
                jsonl_path=jsonl_path,
            )
            if rc != 0:
                return rc
        return 0

    if args.kind in ("os-macos", "os-windows"):
        key = "os_macos_packages" if args.kind == "os-macos" else "os_windows_packages"
        pkgs = plan.get(key, [])
        if not pkgs:
            print(f"No packages scheduled for {args.kind}.")
            return 0
        _run_cmd(["go", "clean", "-testcache"], cwd=repo, env=env)
        rc = _run_cmd(["go", "vet", *pkgs], cwd=repo, env=compile_env)
        if rc != 0:
            return rc
        return _run_cmd(
            ["go", "test", "-json", "-timeout", "35m", "-p", "1", *pkgs],
            cwd=repo,
            env=env,
            jsonl_path=jsonl_path,
        )

    print(f"Unknown shard kind: {args.kind}", file=sys.stderr)
    return 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_plan = sub.add_parser("plan")
    p_plan.add_argument("--repo", default=".")
    p_plan.add_argument("--event-name", required=True)
    p_plan.add_argument("--base-sha", default="")
    p_plan.add_argument("--head-sha", default="")
    p_plan.add_argument("--is-draft", default="false")
    p_plan.add_argument("--pr-title", default="")
    p_plan.add_argument("--pr-labels-json", default="[]")
    p_plan.add_argument("--dispatch-mode", default="")
    p_plan.add_argument("--output", required=True)
    p_plan.add_argument("--github-output", default="")
    p_plan.add_argument("--step-summary", default="")
    p_plan.set_defaults(func=cmd_plan)

    p_run = sub.add_parser("run-shard")
    p_run.add_argument("--repo", default=".")
    p_run.add_argument("--plan", required=True)
    p_run.add_argument(
        "--kind",
        required=True,
        choices=["draft", "vet", "cross-darwin", "cross-windows", "test", "race", "os-macos", "os-windows"],
    )
    p_run.add_argument("--shard-index", type=int, default=0)
    p_run.add_argument("--jsonl", default="")
    p_run.set_defaults(func=cmd_run_shard)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
