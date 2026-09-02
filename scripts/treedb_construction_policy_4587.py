#!/usr/bin/env python3
"""Fail-closed preflight and decision validator for issue #4587 C0."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
import math
from pathlib import Path
import re
import struct
import subprocess
import sys
from typing import Any
from urllib.parse import urlsplit

SCHEMA = "treedb-construction-policy-4587/v8"
RESULT_SCHEMA = "treedb-construction-policy-4587-results/v5"
AUTHORIZATION_SCHEMA = "treedb-construction-policy-4587-execution-authorization/v2"
MEASUREMENT_SCHEMA = "treedb-construction-policy-4587-measurements/v4"
ISOLATION_SCHEMA = "treedb-construction-policy-4587-isolation/v2"
WINNER_SELECTION_SCHEMA = "treedb-construction-policy-4587-winner-selection/v2"
SEARCH_ORIGIN_SCHEMA = "treedb-construction-policy-4587-search-origin/v3"
COORDINATES = [128, 192, 256, 300]
CONTROL = 300
HISTORY_PATH = "docs/benchmarks/treedb_construction_policy_history_2026-09-02.md"
SEARCH_HELPER_PATH = "scripts/treedb_vdbbench_search_existing_index.py"
PROTOCOL_PATHS = (
    "docs/benchmarks/treedb_construction_policy_c0_4587.json",
    HISTORY_PATH,
    "scripts/treedb_construction_policy_4587.py",
    "scripts/test_treedb_construction_policy_4587.py",
    SEARCH_HELPER_PATH,
    "scripts/treedb_vectordbbench_artifact.py",
    "scripts/treedb_vectordbbench_artifact_test.py",
)
DRAFT_PATHS = set(PROTOCOL_PATHS)
WORK_KEYS = {"planning", "reciprocal"}
OBSERVER_PHASE_KEYS = {
    "decisions", "accepted", "rejected", "direct_exact_fp32_rows", "direct_exact_fp32_calls",
    "indexed_exact_fp32_rows", "indexed_exact_fp32_calls", "approximate_score_rows",
    "approximate_score_calls", "exact_fp32_dimensions", "diversity_predicates",
    "diversity_candidates", "diversity_comparisons_requested",
    "diversity_comparisons_executed", "unique_row_pairs", "repeated_row_pairs",
    "row_pair_replacements", "active_wall_nanos", "saturated",
    "candidate_count_histogram", "selected_count_histogram",
    "diversity_early_exit_histogram", "reciprocal_group_histogram",
    "prune_survivor_histogram",
}
RESOURCE_KEYS = {
    "peak_rss_bytes", "cumulative_allocated_bytes", "persisted_bytes",
}
DETERMINISM_KEYS = {
    "graph_config_checksum", "persisted_data_ledger_checksum", "adapter_lifecycle_checksum",
}
MEASUREMENT_KEYS = {
    "schema_version", "source", "origin", "phase_seconds",
    "cpu_utilization_logical_cores", "determinism", "diagnostic_work_profile",
    "resources", "projected_10m_adjacency_reduction_fraction",
}
TREEDB_DATA_FILE_PATTERNS = tuple(map(re.compile, (
    r"(?:maindb|dictdb|templatedb)/(?:LOCK|format\.json|index\.db|vlog_health\.json|vlog_ref_counts\.meta)",
    r"(?:maindb|dictdb|templatedb)/wal/(?:commit|wal)-l[0-9]+-[0-9]{6}\.log",
    r"(?:maindb|dictdb|templatedb)/wal/command-wal-journal-owner\.lock",
    r"(?:maindb|dictdb|templatedb)/(?:value_vlog|leaf_vlog)/value-l[0-9]+-[0-9]{6}\.log(?:\.lenidx)?",
    r"(?:maindb|dictdb|templatedb)/(?:value_vlog|leaf_vlog)/manifest(?:\.durable\.[0-9]{16})?\.json",
    r"maindb/column_assets/[A-Za-z0-9_.-]+/column-assets/assets/segments/segment-[0-9]{6,10}\.tca",
)))


def is_treedb_data_file(relative: Path) -> bool:
    return any(pattern.fullmatch(relative.as_posix())
               for pattern in TREEDB_DATA_FILE_PATTERNS)
MEASUREMENT_ORIGIN_KEYS = {
    "run_id", "artifact_root", "execution_commit", "dataset_sha256", "scale",
    "role", "partition", "ef_construction", "lifecycle_sha256",
    "lifecycle_started_at", "lifecycle_completed_at",
}
ISOLATION_KEYS = {
    "schema_version", "artifact_root", "lock_path", "lock_acquired_at",
    "lock_held_through_evidence", "coverage_completed_at", "gomaxprocs",
    "competing_processes", "peak_swap_used_bytes", "samples",
}
ISOLATION_SAMPLE_KEYS = {"timestamp", "swap_used_bytes", "competing_processes"}
SEARCH_ISOLATION_SCHEMA = "treedb-construction-policy-4587-search-isolation/v3"
SEARCH_ISOLATION_KEYS = {
    "schema_version", "artifact_root", "lock_path", "lock_acquired_at",
    "coverage_completed_at", "gomaxprocs", "service_environment",
    "service_binary_sha256", "service_argv", "service_started_at",
    "service_completed_at", "service_exit_code", "samples",
}
SEARCH_ISOLATION_SAMPLE_KEYS = {"timestamp", "swap_used_bytes", "competing_processes"}
SEARCH_ORIGIN_KEYS = {
    "schema_version", "run_id", "artifact_root", "manifest_sha256",
    "execution_commit", "dataset_sha256", "scale", "role", "partition",
    "ef_construction", "lifecycle_sha256", "lifecycle_started_at",
    "lifecycle_completed_at", "kind", "route", "result_sha256",
    "response_sha256", "index_metadata_sha256", "command_ledger_path",
    "command_sequence", "command_record_sha256", "probe_started_at",
    "probe_completed_at", "service_binary_sha256", "service_argv", "isolation",
    "search_started_at", "search_completed_at",
}
RUN_KEYS = {
    "run_id", "scale", "role", "partition", "ef_construction",
    "execution_commit", "dataset", "configuration", "artifact",
    "isolation_evidence", "measurement_evidence", "search_evidence",
}
GO_GATES = {
    "minimum_adjacency_reduction_fraction": 0.30,
    "maximum_absolute_recall_loss": 0.002,
    "maximum_absolute_ndcg_loss": 0.002,
    "minimum_production_qps_ratio": 0.95,
    "maximum_concurrent_p99_ratio": 1.05,
    "maximum_peak_rss_ratio": 1.0,
    "maximum_unexplained_persisted_or_allocation_increase_bytes": 0,
    "minimum_projected_10m_adjacency_reduction_fraction": 0.30,
    "require_deterministic_identity": True,
    "require_all_route_and_reopen_guardrails": True,
}
PROJECTION_MODEL = {
    "source_scale": 1_000_000,
    "target_scale": 10_000_000,
    "complexity": "n_log_n",
    "frozen_10m_control_adjacency_seconds": 5774.242,
}
SEARCH_ORDER = [
    ("diagnostic", "exact"),
    ("production", "exact"),
    ("diagnostic", "scalar_u8_rerank"),
    ("production", "scalar_u8_rerank"),
]


def fail(message: str) -> None:
    raise ValueError(message)


def run(*argv: str, cwd: Path | None = None,
        env: dict[str, str] | None = None) -> str:
    result = subprocess.run(argv, cwd=cwd, env=env, check=True, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result.stdout.rstrip()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(value: Any) -> str:
    data = json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
    return hashlib.sha256(data.encode()).hexdigest()


def object_at(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{name} must be an object")
    return value


def exact(value: Any, expected: Any, name: str) -> None:
    if value != expected:
        fail(f"{name}={value!r}, want {expected!r}")


def exact_keys(value: dict[str, Any], expected: set[str], name: str) -> None:
    exact(set(value), expected, f"{name} keys")


def nonnegative_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        fail(f"{name} must be a finite non-negative number")
    return float(value)


def positive_number(value: Any, name: str) -> float:
    result = nonnegative_number(value, name)
    if result <= 0:
        fail(f"{name} must be a finite positive number")
    return result
def finite_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        fail(f"{name} must be a finite number")
    return float(value)


def projected_10m_reduction(adjacency_seconds: float, scale: int) -> float | None:
    if scale != PROJECTION_MODEL["source_scale"]:
        return None
    source = PROJECTION_MODEL["source_scale"]
    target = PROJECTION_MODEL["target_scale"]
    factor = target * math.log(target) / (source * math.log(source))
    projected_seconds = positive_number(adjacency_seconds, "projection source adjacency") * factor
    return 1 - projected_seconds / PROJECTION_MODEL["frozen_10m_control_adjacency_seconds"]



def unit_interval_number(value: Any, name: str) -> float:
    result = nonnegative_number(value, name)
    if result > 1:
        fail(f"{name} must be a finite number in [0, 1]")
    return result


def nonnegative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        fail(f"{name} must be a non-negative integer")
    return value


def positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        fail(f"{name} must be a positive integer")
    return value


def full_sha(value: Any, name: str, length: int = 40) -> str:
    if not isinstance(value, str) or re.fullmatch(rf"[0-9a-f]{{{length}}}", value) is None:
        fail(f"{name} must be a {length}-character lowercase hexadecimal digest")
    return value


def utc_timestamp(value: Any, name: str) -> datetime:
    if not isinstance(value, str) or re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})",
        value,
    ) is None:
        fail(f"{name} must be an RFC3339 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be an RFC3339 timestamp") from exc
    return parsed.astimezone(timezone.utc)


def validate_go_gates(contract: dict[str, Any]) -> dict[str, Any]:
    gates = object_at(contract["experiment"]["go_gates"], "experiment.go_gates")
    exact_keys(gates, set(GO_GATES), "GO gate policy")
    exact(gates, GO_GATES, "frozen GO gate policy")
    exact(contract["experiment"]["projection_model"], PROJECTION_MODEL, "10M projection model")
    return gates


def read_bound_json(root: Path, binding: Any, name: str) -> tuple[dict[str, Any], Path]:
    bound = object_at(binding, name)
    exact_keys(bound, {"path", "sha256"}, name)
    relative = Path(bound["path"])
    if relative.is_absolute() or ".." in relative.parts:
        fail(f"{name}.path must be artifact-root relative")
    path = (root / relative).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError:
        fail(f"{name}.path escapes artifact root")
    full_sha(bound["sha256"], f"{name}.sha256", 64)
    exact(sha256_file(path), bound["sha256"], f"{name} SHA-256")
    value = json.loads(path.read_text())
    return object_at(value, name), path


def lifecycle_timing(root: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    lifecycle = object_at(manifest.get("lifecycle"), "manifest.lifecycle")
    exact(lifecycle.get("schema_version"), "treedb-vectordbbench-lifecycle/v1",
          "manifest.lifecycle schema")
    relative_value = lifecycle.get("file")
    if not isinstance(relative_value, str):
        fail("manifest.lifecycle.file must be artifact-root relative")
    relative = Path(relative_value)
    if relative.is_absolute() or ".." in relative.parts:
        fail("manifest.lifecycle.file must be artifact-root relative")
    path = (root / relative).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError:
        fail("manifest.lifecycle.file escapes artifact root")
    expected_sha = full_sha(lifecycle.get("sha256"), "manifest.lifecycle.sha256", 64)
    exact(sha256_file(path), expected_sha, "lifecycle SHA-256")
    raw = path.read_text()
    if not raw or not raw.endswith("\n"):
        fail("lifecycle JSONL must be complete and newline terminated")
    events: list[dict[str, Any]] = []
    prior_sequence = -1
    prior_timestamp: datetime | None = None
    for position, line in enumerate(raw.splitlines()):
        event = object_at(json.loads(line), f"lifecycle event {position}")
        exact_keys(event, {"schema_version", "sequence", "stage", "timestamp", "state"},
                   f"lifecycle event {position}")
        exact(event["schema_version"], "treedb-vectordbbench-lifecycle-event/v1",
              f"lifecycle event {position} schema")
        sequence = event["sequence"]
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence != prior_sequence + 1:
            fail("lifecycle event sequence must be contiguous from zero")
        timestamp = utc_timestamp(event["timestamp"], f"lifecycle event {position}.timestamp")
        if prior_timestamp is not None and timestamp < prior_timestamp:
            fail("lifecycle event timestamps must be chronological")
        object_at(event["state"], f"lifecycle event {position}.state")
        prior_sequence = sequence
        prior_timestamp = timestamp
        events.append(event)
    starts = [event for event in events if event["stage"] == "startup"]
    completions = [event for event in events if event["stage"] == "teardown"]
    if len(starts) != 1 or len(completions) != 1:
        fail("lifecycle must contain exactly one startup and teardown event")
    started_at = utc_timestamp(starts[0]["timestamp"], "lifecycle startup timestamp")
    completed_at = utc_timestamp(completions[0]["timestamp"], "lifecycle teardown timestamp")
    if started_at >= completed_at:
        fail("lifecycle teardown must occur after startup")
    return {
        "sha256": expected_sha,
        "started_at": starts[0]["timestamp"],
        "completed_at": completions[0]["timestamp"],
        "started": started_at,
        "completed": completed_at,
    }

def validate_construction_evidence(
    root: Path, manifest: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, float]]:
    lifecycle = object_at(manifest.get("lifecycle"), "manifest.lifecycle")
    raw_artifacts = lifecycle.get("raw_artifacts")
    if not isinstance(raw_artifacts, list):
        fail("manifest.lifecycle.raw_artifacts must be a list")
    matches = [item for item in raw_artifacts
               if isinstance(item, dict) and item.get("path") == "adapter-lifecycle.jsonl"]
    if len(matches) != 1:
        fail("lifecycle must bind exactly one adapter-lifecycle.jsonl")
    binding = matches[0]
    exact_keys(binding, {"path", "sha256"}, "adapter lifecycle binding")
    path = root / "adapter-lifecycle.jsonl"
    exact(sha256_file(path), full_sha(binding["sha256"], "adapter lifecycle SHA-256", 64),
          "adapter lifecycle SHA-256")
    raw = path.read_text()
    if not raw or not raw.endswith("\n"):
        fail("adapter lifecycle sidecar must be complete and newline terminated")
    records = [object_at(json.loads(line), f"adapter lifecycle line {position}")
               for position, line in enumerate(raw.splitlines())]
    starts = [record for record in records if record.get("event") == "optimize_start"]
    ends = [record for record in records if record.get("event") == "optimize_end"]
    if len(starts) != 1 or len(ends) != 1:
        fail("adapter lifecycle must contain exactly one optimize start and end")
    start_ns = positive_int(starts[0].get("timestamp_ns"), "optimize start timestamp_ns")
    end_ns = positive_int(ends[0].get("timestamp_ns"), "optimize end timestamp_ns")
    if start_ns >= end_ns:
        fail("optimize end must follow optimize start")
    response = object_at(ends[0].get("response"), "raw optimize response")
    status = object_at(response.get("status"), "raw optimize response.status")
    column_graph = object_at(status.get("column_graph_build"), "raw optimize column_graph_build")
    adjacency_nanos = positive_int(
        column_graph.get("adjacency_build_nanos"), "raw optimize adjacency_build_nanos")
    decisions = object_at(column_graph.get("construction_decisions"), "raw construction decisions")
    exact_keys(decisions, WORK_KEYS, "raw construction decisions")
    for phase_name, phase_value in decisions.items():
        phase = object_at(phase_value, f"raw construction decisions.{phase_name}")
        exact_keys(phase, OBSERVER_PHASE_KEYS, f"raw construction decisions.{phase_name}")
        for key, value in phase.items():
            name = f"raw construction decisions.{phase_name}.{key}"
            if key == "saturated":
                exact(value, False, name)
            elif key.endswith("_histogram"):
                if not isinstance(value, list) or len(value) != 16:
                    fail(f"{name} must contain exactly 16 buckets")
                for position, bucket in enumerate(value):
                    nonnegative_int(bucket, f"{name}[{position}]")
            else:
                nonnegative_int(value, name)
    if sum(decisions[phase]["decisions"] for phase in WORK_KEYS) <= 0:
        fail("raw construction decisions must contain observed decisions")
    return decisions, {
        "adjacency": adjacency_nanos / 1_000_000_000,
        "optimize": (end_ns - start_ns) / 1_000_000_000,
    }


def verify_git_identity(root: Path, source: dict[str, Any], allow_draft: bool) -> dict[str, Any]:
    base = source["definition_base_commit"]
    head = run("git", "rev-parse", "HEAD", cwd=root)
    exact(run("git", "merge-base", head, base, cwd=root), base, "definition base ancestry")
    changed = set(run("git", "diff", "--name-only", base, head, cwd=root).splitlines())
    allowed_committed = {HISTORY_PATH} | DRAFT_PATHS
    if changed - allowed_committed:
        fail(f"unexpected committed paths since definition base: {sorted(changed - allowed_committed)}")
    dirty = run("git", "status", "--porcelain=v1", cwd=root).splitlines()
    dirty_paths = {line[3:] for line in dirty if len(line) >= 4}
    if dirty and (not allow_draft or dirty_paths - DRAFT_PATHS):
        fail(f"gomap source is dirty outside reviewed draft paths: {dirty}")
    for path, expected in source["runtime_subtrees"].items():
        exact(run("git", "rev-parse", f"HEAD:{path}", cwd=root), expected, f"runtime subtree {path}")
    for group in ("runtime_blobs", "harness_blobs"):
        for path, expected in source[group].items():
            exact(run("git", "hash-object", path, cwd=root), expected, f"{group} {path}")
    return {"head": head, "dirty": dirty, "draft_mode": allow_draft}


def verify_external_git(source: dict[str, Any]) -> dict[str, Any]:
    cfg = object_at(source["vectordbbench"], "source_identity.vectordbbench")
    root = Path(cfg["root"])
    exact(run("git", "rev-parse", "HEAD", cwd=root), cfg["commit"], "VectorDBBench commit")
    exact(run("git", "status", "--porcelain=v1", cwd=root), "", "VectorDBBench status")
    for path, expected in cfg["subtrees"].items():
        exact(run("git", "rev-parse", f"HEAD:{path}", cwd=root), expected, f"VectorDBBench subtree {path}")
    for path, expected in cfg["blobs"].items():
        exact(run("git", "hash-object", path, cwd=root), expected, f"VectorDBBench blob {path}")
    return {"commit": cfg["commit"], "clean": True}


def partition_ordinals(ids: list[int], seed: str) -> tuple[list[int], list[int]]:
    seed_bytes = seed.encode()
    ranked = sorted(range(len(ids)), key=lambda ordinal: (
        hashlib.sha256(seed_bytes + b"\0" + struct.pack(">q", ids[ordinal])).digest(),
        ids[ordinal], ordinal,
    ))
    return sorted(ranked[:500]), sorted(ranked[500:])


def verify_datasets(contract: dict[str, Any]) -> dict[str, Any]:
    try:
        import pyarrow as arrow
        import pyarrow.parquet as parquet
    except ImportError as exc:
        fail(f"pyarrow is required for query-partition verification: {exc}")
    datasets = object_at(contract["datasets"], "datasets")
    source = object_at(datasets["canonical_query_source"], "datasets.canonical_query_source")
    test_path, neighbors_path = Path(source["test_path"]), Path(source["neighbors_path"])
    exact(sha256_file(test_path), source["test_sha256"], "canonical test SHA-256")
    exact(sha256_file(neighbors_path), source["neighbors_sha256"], "canonical neighbors SHA-256")
    tests, neighbors = parquet.read_table(test_path), parquet.read_table(neighbors_path)
    ids = tests.column("id").to_pylist()
    exact(ids, list(range(1000)), "canonical query IDs")
    exact(neighbors.column("id").to_pylist(), ids, "ground-truth query IDs")
    if any(len(row.as_py()) != 768 for row in tests.column("emb")):
        fail("canonical queries must all have 768 dimensions")
    split = object_at(datasets["partition"], "datasets.partition")
    selection, holdout = partition_ordinals(ids, split["seed"])
    exact(canonical_sha256(selection), split["selection_ordinals_sha256"], "selection ordinal digest")
    exact(canonical_sha256(holdout), split["holdout_ordinals_sha256"], "holdout ordinal digest")
    exact(sorted(selection + holdout), list(range(1000)), "partition coverage")
    if set(selection) & set(holdout):
        fail("selection and holdout partitions overlap")
    for name, ordinals in (("screening", selection), ("decision", holdout)):
        cfg = object_at(datasets[name], f"datasets.{name}")
        directory = Path(cfg["directory"])
        for filename, expected in cfg["files"].items():
            exact(sha256_file(directory / filename), expected, f"{name} {filename} SHA-256")
        split_test = parquet.read_table(directory / "test.parquet")
        split_neighbors = parquet.read_table(directory / "neighbors.parquet")
        expected_ids = [ids[index] for index in ordinals]
        exact(split_test.column("id").to_pylist(), expected_ids, f"{name} test IDs")
        exact(split_neighbors.column("id").to_pylist(), expected_ids, f"{name} neighbors IDs")
        ordinal_array = arrow.array(ordinals)
        if not split_test.equals(tests.take(ordinal_array)):
            fail(f"{name} test rows do not match canonical partition ordinals")
        if not split_neighbors.equals(neighbors.take(ordinal_array)):
            fail(f"{name} neighbor rows do not match canonical partition ordinals")
    return {"canonical_queries": len(ids), "selection_rows": len(selection), "holdout_rows": len(holdout),
            "selection_digest": canonical_sha256(selection), "holdout_digest": canonical_sha256(holdout)}


def authorization_source_identity(contract: dict[str, Any]) -> dict[str, Any]:
    source = object_at(contract["source_identity"], "source_identity")
    vectordbbench = object_at(source["vectordbbench"], "source_identity.vectordbbench")
    return {
        "runtime_subtrees": source["runtime_subtrees"],
        "runtime_blobs": source["runtime_blobs"],
        "harness_blobs": source["harness_blobs"],
        "vectordbbench": {
            "commit": vectordbbench["commit"],
            "subtrees": vectordbbench["subtrees"],
            "blobs": vectordbbench["blobs"],
        },
        "runtime": source["runtime"],
    }


def validate_authorization(contract: dict[str, Any], path: Path, *,
                           require_clean_head: bool = True) -> dict[str, Any]:
    path = path.resolve()
    checksum = sha256_file(path)
    authorization = object_at(json.loads(path.read_text()), "execution authorization")
    exact_keys(authorization, {
        "schema_version", "authorization_kind", "artifact_root", "execution_commit",
        "contract_sha256", "protocol_files", "source_identity", "service_binary",
    }, "execution authorization")
    exact(authorization["schema_version"], AUTHORIZATION_SCHEMA, "authorization schema")
    exact(authorization["authorization_kind"], "COORDINATOR_REVIEW_PROVENANCE",
          "authorization kind")
    artifact_root = Path(contract["commands"]["artifact_root"]).resolve()
    exact(authorization["artifact_root"], str(artifact_root), "authorization artifact root")
    commit = full_sha(authorization["execution_commit"], "authorization execution commit")
    gomap_root = Path(contract["source_identity"]["gomap_root"])
    if require_clean_head:
        exact(run("git", "rev-parse", "HEAD", cwd=gomap_root), commit,
              "authorization execution commit")
        exact(run("git", "status", "--porcelain=v1", cwd=gomap_root), "",
              "authorization source cleanliness")
    exact(authorization["contract_sha256"], canonical_sha256(contract),
          "authorization contract SHA-256")
    protocol_files = object_at(authorization["protocol_files"], "authorization.protocol_files")
    exact_keys(protocol_files, set(PROTOCOL_PATHS), "authorization.protocol_files")
    for protocol_path in PROTOCOL_PATHS:
        expected = full_sha(protocol_files[protocol_path],
                            f"authorization.protocol_files.{protocol_path}", 64)
        exact(sha256_file(gomap_root / protocol_path), expected,
              f"authorized protocol file {protocol_path} SHA-256")
    exact(authorization["source_identity"], authorization_source_identity(contract),
          "authorization source identity")
    binary = object_at(authorization["service_binary"], "authorization.service_binary")
    exact_keys(binary, {
        "path", "sha256", "build_argv", "build_environment", "go_version",
    }, "authorization.service_binary")
    if not isinstance(binary.get("path"), str) or not binary["path"]:
        fail("authorization.service_binary.path must be a non-empty string")
    binary_path = Path(binary["path"])
    if not binary_path.is_absolute():
        fail("authorization.service_binary.path must be absolute")
    exact(binary_path.resolve(), Path(contract["commands"]["binary"]).resolve(),
          "authorization service binary path")
    exact(binary["build_argv"], contract["commands"]["build_argv"],
          "authorization service build argv")
    exact(binary["build_environment"], expected_build_environment(contract),
          "authorization service build environment")
    exact(binary["go_version"], f"go version {contract['source_identity']['runtime']['go']}",
          "authorization Go toolchain version")
    expected_binary_sha = full_sha(binary["sha256"], "authorization.service_binary.sha256", 64)
    exact(sha256_file(binary_path), expected_binary_sha, "authorized service binary SHA-256")
    return {
        "path": str(path),
        "sha256": checksum,
        "execution_commit": commit,
        "protocol_files": protocol_files,
        "service_binary_sha256": expected_binary_sha,
    }


def resolve_authorization_binding(binding: Any, contract: dict[str, Any]) -> tuple[Path, str]:
    bound = object_at(binding, "decision.authorization")
    exact_keys(bound, {"path", "sha256"}, "decision.authorization")
    expected = full_sha(bound["sha256"], "decision.authorization.sha256", 64)
    if not isinstance(bound.get("path"), str) or not bound["path"]:
        fail("decision.authorization.path must be a non-empty string")
    path = Path(bound["path"])
    if path.is_absolute():
        return path.resolve(), expected
    if ".." in path.parts:
        fail("relative decision.authorization.path must stay within artifact root")
    artifact_root = Path(contract["commands"]["artifact_root"]).resolve()
    path = (artifact_root / path).resolve()
    try:
        path.relative_to(artifact_root)
    except ValueError:
        fail("relative decision.authorization.path escapes artifact root")
    return path, expected

def expected_build_environment(contract: dict[str, Any]) -> dict[str, str]:
    runtime = object_at(contract["source_identity"]["runtime"], "source_identity.runtime")
    artifact_root = Path(contract["commands"]["artifact_root"]).resolve()
    return {
        "PATH": str(Path(runtime["go_executable"]).resolve().parent),
        "GOROOT": str(Path(runtime["go_root"]).resolve()),
        "GOPATH": str(Path(runtime["go_path"]).resolve()),
        "GOMODCACHE": str(Path(runtime["go_mod_cache"]).resolve()),
        "GOCACHE": str(artifact_root / "go-build-cache"),
        "TMPDIR": str(artifact_root / "tmp"),
        "GOENV": "off",
        "GOTOOLCHAIN": "local",
        "GOWORK": "off",
        "GOFLAGS": "",
        "GOEXPERIMENT": "",
        "GODEBUG": "",
        "GOFIPS140": "off",
        "GOTELEMETRY": "off",
        "GO111MODULE": "on",
        "GOPROXY": "off",
        "GOSUMDB": "off",
        "CGO_ENABLED": "0",
        "GOOS": "linux",
        "GOARCH": "amd64",
        "GOAMD64": "v1",
        "GOMAXPROCS": str(runtime["gomaxprocs"]),
    }


def generate_authorization(contract: dict[str, Any], path: Path, service_binary: Path,
                           reviewed_head: str) -> dict[str, Any]:
    validate_contract(contract, True)
    root = Path(contract["source_identity"]["gomap_root"])
    head = run("git", "rev-parse", "HEAD", cwd=root)
    exact(head, full_sha(reviewed_head, "reviewed head"), "reviewed execution head")
    exact(run("git", "status", "--porcelain=v1", cwd=root), "",
          "authorization source cleanliness")
    service_binary = service_binary.resolve()
    expected_binary = Path(contract["commands"]["binary"]).resolve()
    exact(service_binary.resolve(), expected_binary,
          "authorization service binary output path")
    service_binary = expected_binary
    service_binary.parent.mkdir(parents=True, exist_ok=True)
    build_environment = expected_build_environment(contract)
    for variable in ("GOCACHE", "TMPDIR"):
        Path(build_environment[variable]).mkdir(parents=True, exist_ok=True)
    go_version = run(
        contract["commands"]["build_argv"][0], "version",
        cwd=root, env=build_environment)
    exact(go_version, f"go version {contract['source_identity']['runtime']['go']}",
          "authorization Go toolchain version")
    run(contract["commands"]["build_argv"][0], "mod", "verify",
        cwd=root, env=build_environment)
    run(*contract["commands"]["build_argv"], cwd=root, env=build_environment)
    authorization = {
        "schema_version": AUTHORIZATION_SCHEMA,
        "authorization_kind": "COORDINATOR_REVIEW_PROVENANCE",
        "artifact_root": str(Path(contract["commands"]["artifact_root"]).resolve()),
        "execution_commit": head,
        "contract_sha256": canonical_sha256(contract),
        "protocol_files": {
            protocol_path: sha256_file(root / protocol_path) for protocol_path in PROTOCOL_PATHS
        },
        "source_identity": authorization_source_identity(contract),
        "service_binary": {
            "path": str(service_binary),
            "sha256": sha256_file(service_binary),
            "build_argv": contract["commands"]["build_argv"],
            "build_environment": build_environment,
            "go_version": go_version,
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(authorization, indent=2, sort_keys=True) + "\n")
    return validate_authorization(contract, path)


def validate_python_command_contract(contract: dict[str, Any]) -> None:
    source = object_at(contract.get("source_identity"), "source_identity")
    runtime = object_at(source.get("runtime"), "source_identity.runtime")
    python_executable = runtime.get("python_executable")
    if (not isinstance(python_executable, str)
            or not Path(python_executable).is_absolute()):
        fail("frozen Python interpreter path must be absolute")
    exact(sha256_file(Path(python_executable)), runtime.get("python_sha256"),
          "frozen Python interpreter checksum")
    commands = object_at(contract.get("commands"), "commands")
    search_commands = object_at(
        commands.get("existing_index_search"), "commands.existing_index_search")
    exact(search_commands.get("service_environment"),
          {"GOMAXPROCS": str(runtime.get("gomaxprocs"))},
          "existing-index service environment")
    go_executable = runtime.get("go_executable")
    if not isinstance(go_executable, str) or not Path(go_executable).is_absolute():
        fail("frozen Go executable path must be absolute")
    exact(Path(go_executable).resolve(),
          (Path(runtime.get("go_root", "")) / "bin/go").resolve(),
          "frozen Go executable root binding")
    exact(sha256_file(Path(go_executable)), runtime.get("go_sha256"),
          "frozen Go executable checksum")
    expected_build_argv = [
        go_executable, "build", "-trimpath", "-buildvcs=false",
        "-o", commands.get("binary"), "./cmd/treedb-document-service",
    ]
    exact(commands.get("build_argv"), expected_build_argv,
          "authorization service build argv")
    exact(commands.get("build_environment"), expected_build_environment(contract),
          "authorization service build environment")
    python_commands = {
        "authorization_generate_argv_template": commands.get(
            "authorization_generate_argv_template"),
        "authorized_preflight_argv": commands.get("authorized_preflight_argv"),
        "winner_selection_generate_argv_template": commands.get(
            "winner_selection_generate_argv_template"),
        "lifecycle_harness_argv_template": commands.get(
            "lifecycle_harness_argv_template"),
        "decision_validation_argv": commands.get("decision_validation_argv"),
        "existing_index_search.probe_argv_template": search_commands.get(
            "probe_argv_template"),
        "existing_index_search.vectordbbench_common_argv_template": search_commands.get(
            "vectordbbench_common_argv_template"),
    }
    for name, argv in python_commands.items():
        if not isinstance(argv, list) or not argv:
            fail(f"commands.{name} must be a non-empty argv array")
        exact(argv[0], python_executable, f"commands.{name} frozen Python launcher")
    lifecycle_argv = python_commands["lifecycle_harness_argv_template"]
    exact(lifecycle_argv[1], "scripts/treedb_vectordbbench_artifact.py",
          "lifecycle harness script binding")
    for flag, expected in (("--python", python_executable), ("--use-uv", "off")):
        positions = [position for position, token in enumerate(lifecycle_argv) if token == flag]
        if len(positions) != 1 or positions[0] + 1 >= len(lifecycle_argv):
            fail(f"lifecycle harness must contain exactly one {flag} value")
        exact(lifecycle_argv[positions[0] + 1], expected,
              f"lifecycle harness {flag} binding")
    expected_service_environment = {"GOMAXPROCS": str(runtime["gomaxprocs"])}
    exact(commands.get("lifecycle_service_environment"), expected_service_environment,
          "lifecycle service environment")
    exact(commands.get("lifecycle_vdbbench_environment_template"), {
        "GOMAXPROCS": str(runtime["gomaxprocs"]),
        "PYTHONPATH": os.pathsep.join((
            source["vectordbbench"]["root"],
            str(Path(source["gomap_root"]) / "clients/python/treedb_client/src"),
        )),
        "RESULTS_LOCAL_DIR": "{artifact_root}/vdbbench-results/{row}",
        "LOG_FILE": "{artifact_root}/vdbbench.log",
        "NUM_PER_BATCH": "500",
        "TREEDB_LIFECYCLE_SIDECAR": "{artifact_root}/adapter-lifecycle.jsonl",
        "TREEDB_LIFECYCLE_BOUNDARY_ACK":
            "{artifact_root}/lifecycle-boundary-diagnostics.json",
    }, "lifecycle VectorDBBench environment template")


def validate_contract(contract: dict[str, Any], allow_draft: bool,
                      authorization_path: Path | None = None) -> dict[str, Any]:
    exact(contract.get("schema_version"), SCHEMA, "schema_version")
    exact(contract.get("authority"), "FROZEN_AUTHORITATIVE", "authority")
    exact(contract.get("engineering_status"), "QUALIFIED", "engineering_status")
    exact(contract.get("execution_validity"), "REQUIRES_FINAL_EXTERNAL_AUTHORIZATION",
          "execution_validity")
    exact(contract.get("protocol_verdict"), "PROTOCOL_ACCEPTED", "protocol_verdict")
    exact(contract.get("stage"), "frozen", "stage")
    exact(contract.get("trial_started"), False, "trial_started")
    exact(contract.get("scope"), "C0_ONLY", "scope")
    source = object_at(contract.get("source_identity"), "source_identity")
    exact(source["definition_base_commit"], "05c7bd35b02196879dc4378248228927ed367517",
          "definition base commit")
    validate_python_command_contract(contract)
    graph = object_at(contract["experiment"]["graph"], "experiment.graph")
    exact(graph["ef_construction_coordinates"], COORDINATES, "C0 coordinates")
    exact(graph["control_ef_construction"], CONTROL, "C0 control")
    exact((graph["strategy"], graph["metric"], graph["dimensions"], graph["m"]),
          ("column_graph", "cosine", 768, 16), "graph contract")
    search = object_at(contract["experiment"]["search"], "experiment.search")
    exact((search["top_k"], search["ef_search"], search["configured_rerank_candidates"],
           search["effective_rerank_candidates"]), (100, 192, 400, 192), "search contract")
    exact(search["diagnostic_transport"], {"stats_mode": "full_diagnostics", "response_format": "full",
                                               "require_vector_index_guards": True}, "diagnostic transport")
    exact(search["production_transport"], {"stats_mode": "production", "response_format": "ids",
                                               "require_vector_index_guards": False}, "production transport")
    ordering = object_at(contract["experiment"]["ordering"], "experiment.ordering")
    exact(ordering["screening_run_order"], COORDINATES, "screening order")
    exact(ordering["decision_run_order"], [300, "selected_screening_winner"], "decision order")
    isolation_policy = object_at(
        contract["experiment"]["isolation_and_noise"], "experiment.isolation_and_noise")
    exact(isolation_policy["diagnostics_interval_seconds"], 5, "isolation sampling interval")
    exact(isolation_policy["sampling_gap_tolerance_seconds"], 1, "isolation sampling tolerance")
    exact(contract["experiment"]["projection_model"], PROJECTION_MODEL, "10M projection model")
    exact(set(contract["experiment"]["required_metrics_per_run"]["diagnostic_work_profile"]), WORK_KEYS,
          "required diagnostic work metrics")
    exact(set(contract["experiment"]["required_metrics_per_run"]["resources"]), RESOURCE_KEYS,
          "required resource metrics")
    required = contract["experiment"]["required_metrics_per_run"]
    exact(required["cpu_utilization"], "cpu_utilization_logical_cores", "required CPU utilization metric")
    exact(set(required["determinism"]), DETERMINISM_KEYS, "required deterministic identity metrics")
    validate_go_gates(contract)
    isolation_schema = object_at(contract["isolation_schema"], "isolation_schema")
    exact_keys(isolation_schema, {
        "schema_version", "exact_keys", "sample_exact_keys", "coverage",
    }, "isolation_schema")
    exact(isolation_schema["schema_version"], ISOLATION_SCHEMA, "isolation contract schema")
    exact(set(isolation_schema["exact_keys"]), ISOLATION_KEYS, "isolation contract keys")
    exact(set(isolation_schema["sample_exact_keys"]), ISOLATION_SAMPLE_KEYS,
          "isolation sample contract keys")
    measurement_schema = object_at(contract["measurement_schema"], "measurement_schema")
    exact_keys(measurement_schema, {
        "schema_version", "exact_keys", "source_exact_keys", "origin_exact_keys",
        "producer_binding", "observer_phase_exact_keys", "timing_source",
    }, "measurement_schema")
    exact(measurement_schema["schema_version"], MEASUREMENT_SCHEMA, "measurement contract schema")
    exact(set(measurement_schema["exact_keys"]), MEASUREMENT_KEYS, "measurement contract keys")
    exact(set(measurement_schema["source_exact_keys"]), {
        "schema_version", "adapter_lifecycle", "diagnostics", "isolation",
        "data_root", "data_files",
    }, "measurement source contract keys")
    exact(set(measurement_schema["origin_exact_keys"]), MEASUREMENT_ORIGIN_KEYS,
          "measurement origin contract keys")
    exact(set(measurement_schema["observer_phase_exact_keys"]), OBSERVER_PHASE_KEYS,
          "observer phase contract keys")
    result_schema = object_at(contract["result_schema"], "result_schema")
    exact(result_schema["schema_version"], RESULT_SCHEMA, "result contract schema")
    exact(result_schema["decision_exact_keys"], [
        "schema_version", "execution_commit", "contract_sha256", "authorization",
        "winner_selection", "verdict", "runs",
    ], "decision contract keys")
    exact(result_schema["winner_selection_binding"]["schema_version"], WINNER_SELECTION_SCHEMA,
          "winner selection contract schema")
    exact(result_schema["search_origin_schema_version"], SEARCH_ORIGIN_SCHEMA,
          "search origin contract schema")
    exact(result_schema["search_evidence_exact_keys"],
          ["kind", "route", "origin", "result", "response", "index_metadata"],
          "search evidence contract keys")
    exact(set(result_schema["search_origin_exact_keys"]), SEARCH_ORIGIN_KEYS,
          "search origin contract keys")
    search_isolation_schema = object_at(
        result_schema["search_isolation_schema"], "result_schema.search_isolation_schema")
    exact(search_isolation_schema["schema_version"], SEARCH_ISOLATION_SCHEMA,
          "search isolation contract schema")
    exact(set(search_isolation_schema["exact_keys"]), SEARCH_ISOLATION_KEYS,
          "search isolation contract keys")
    exact(set(search_isolation_schema["sample_exact_keys"]), SEARCH_ISOLATION_SAMPLE_KEYS,
          "search isolation sample contract keys")
    gomap = verify_git_identity(Path(source["gomap_root"]), source, allow_draft)
    external = verify_external_git(source)
    datasets = verify_datasets(contract)
    if allow_draft:
        authorization = None
        reason = "draft mode never authorizes execution"
    else:
        if authorization_path is None:
            authorization_path = Path(contract["commands"]["authorization_manifest"])
        authorization = validate_authorization(contract, authorization_path)
        reason = "exact reviewed protocol bytes and service binary are authorized by external coordinator manifest"
    return {"contract": "valid", "gomap": gomap, "vectordbbench": external, "datasets": datasets,
            "execution_authorized": authorization is not None, "authorization": authorization,
            "reason": reason}


def dataset_expected(contract: dict[str, Any], scale: int, partition: str) -> dict[str, Any]:
    if (scale, partition) == (250000, "selection"):
        name = "screening"
        digest = contract["datasets"]["partition"]["selection_ordinals_sha256"]
    elif (scale, partition) == (1000000, "holdout"):
        name = "decision"
        digest = contract["datasets"]["partition"]["holdout_ordinals_sha256"]
    else:
        fail(f"forbidden scale/partition pair {(scale, partition)!r}")
    dataset = contract["datasets"][name]
    return {"name": dataset["name"], "directory": dataset["directory"],
            "vectors": dataset["vectors"], "dimensions": dataset["dimensions"],
            "metric": dataset["metric"], "train_sha256": dataset["files"]["train.parquet"],
            "test_sha256": dataset["files"]["test.parquet"],
            "neighbors_sha256": dataset["files"]["neighbors.parquet"],
            "partition_ordinals_sha256": digest}


def parse_result(path: Path, expected_index: str, name: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    result = object_at(json.loads(path.read_text()), name)
    rows = result.get("results")
    if not isinstance(rows, list) or len(rows) != 1:
        fail(f"{name}.results must contain exactly one row")
    row = object_at(rows[0], f"{name}.results[0]")
    exact(row.get("label"), ":)", f"{name} success label")
    task = object_at(row.get("task_config"), f"{name}.task_config")
    exact(object_at(task.get("db_config"), f"{name}.db_config").get("index_name"), expected_index,
          f"{name} index_name")
    return result, task, object_at(row.get("metrics"), f"{name}.metrics")


def validate_index_metadata(metadata: dict[str, Any], run_row: dict[str, Any], index_name: str, name: str) -> int:
    config = run_row["configuration"]
    exact(metadata.get("name"), index_name, f"{name}.name")
    exact(metadata.get("dimension"), config["dimensions"], f"{name}.dimension")
    exact(metadata.get("metric"), config["metric"], f"{name}.metric")
    exact(metadata.get("vector_strategy"), "column_graph", f"{name}.vector_strategy")
    exact(metadata.get("vector_m"), config["m"], f"{name}.vector_m")
    exact(metadata.get("vector_ef_construction"), config["ef_construction"], f"{name}.vector_ef_construction")
    exact(metadata.get("vector_ef_search"), config["ef_search"], f"{name}.vector_ef_search")
    generation = positive_int(metadata.get("generation"), f"{name}.generation")
    quantized = metadata.get("quantized_indexes")
    if not isinstance(quantized, list) or not any(
        isinstance(item, dict) and item.get("name") == "embedding.scalar_u8.fast" for item in quantized
    ):
        fail(f"{name} is missing embedding.scalar_u8.fast")
    return generation
def read_probe_command_record(
    root: Path, origin: dict[str, Any], expected_sequence: int, name: str,
    expected_helper_path: Path, expected_helper_sha256: str,
    expected_python_executable: str, expected_python_sha256: str,
) -> dict[str, Any]:
    ledger_relative = Path(origin["command_ledger_path"])
    if ledger_relative.is_absolute() or ".." in ledger_relative.parts:
        fail(f"{name}.command_ledger_path must be artifact-root relative")
    ledger_path = (root / ledger_relative).resolve()
    try:
        ledger_path.relative_to(root)
    except ValueError:
        fail(f"{name}.command_ledger_path escapes artifact root")
    records = [object_at(json.loads(line), f"{name}.command ledger record")
               for line in ledger_path.read_text().splitlines()]
    if len(records) != 4:
        fail("probe command ledger must contain exactly four append-only evidence records")
    for sequence, record in enumerate(records):
        exact_keys(record, {
            "schema_version", "sequence", "argv", "helper_sha256",
            "python_executable", "python_sha256", "vdbbench_argv", "vdbbench_env",
            "kind", "started_at", "completed_at", "probe_started_at",
            "probe_completed_at", "exit_code", "query_sha256",
            "dataset_files_before_sha256", "dataset_files_after_sha256",
            "run_id", "route", "result_sha256", "response_sha256",
            "index_metadata_sha256",
        }, f"probe command ledger[{sequence}]")
        exact(record["schema_version"], "treedb-construction-policy-4587-probe-command/v5",
              f"probe command ledger[{sequence}].schema_version")
        exact(record["sequence"], sequence, f"probe command ledger[{sequence}].sequence")
    record = records[expected_sequence]
    exact(origin["command_sequence"], expected_sequence, f"{name}.command_sequence")
    exact(origin["command_record_sha256"], canonical_sha256(record), f"{name}.command record SHA-256")
    exact(record["exit_code"], 0, f"{name}.command exit code")
    argv = record["argv"]
    if not isinstance(argv, list) or not argv or not isinstance(argv[0], str):
        fail(f"{name}.command argv must execute the frozen existing-index helper")
    if Path(argv[0]).resolve() != expected_helper_path:
        fail(f"{name}.command argv must resolve to the authorized existing-index helper")
    exact(record["helper_sha256"], expected_helper_sha256,
          f"{name}.command authorized helper SHA-256")
    exact(Path(record["python_executable"]).resolve(),
          Path(expected_python_executable).resolve(),
          f"{name}.command frozen helper Python interpreter")
    exact(record["python_sha256"], expected_python_sha256,
          f"{name}.command frozen helper Python checksum")
    return record


def probe_argv_options(argv: Any, name: str) -> dict[str, str]:
    if not isinstance(argv, list) or not argv or not isinstance(argv[0], str):
        fail(f"{name}.argv must execute the frozen existing-index helper")
    tokens = argv[1:]
    if len(tokens) % 2:
        fail(f"{name}.argv must contain flag/value pairs")
    options: dict[str, str] = {}
    for flag, value in zip(tokens[::2], tokens[1::2], strict=True):
        if not isinstance(flag, str) or not flag.startswith("--") or not isinstance(value, str) or flag in options:
            fail(f"{name}.argv contains a malformed or duplicate option")
        options[flag] = value
    exact(set(options), {
        "--base-url", "--index-name", "--query-json", "--metadata-out",
        "--diagnostic-response-out", "--production-response-out",
        "--diagnostic-result", "--production-result", "--diagnostic-origin-out",
        "--production-origin-out", "--command-ledger", "--run-id", "--artifact-root",
        "--manifest-sha256", "--execution-commit", "--dataset-sha256", "--scale",
        "--role", "--partition", "--lifecycle-sha256", "--lifecycle-started-at",
        "--lifecycle-completed-at", "--ef-construction", "--expected-generation", "--route",
        "--dataset-name", "--dataset-dir", "--vectordbbench-dir", "--service-bin",
        "--service-binary-sha256", "--python-executable", "--python-sha256",
        "--search-isolation-out", "--exclusive-lock", "--diagnostics-interval",
        "--service-health-timeout",
    }, f"{name}.argv frozen options")
    return options
def expected_vdbbench_argv(
    run_row: dict[str, Any], index_name: str, kind: str, route: str, base_url: str,
) -> list[str]:
    config = run_row["configuration"]
    command = [
        "-m", "vectordb_bench.cli.vectordbbench",
        "treedbcolumngraphexact" if route == "exact" else "treedbscalaru8rerank",
        "--base-url", base_url, "--index-name", index_name,
        "--skip-drop-old", "--skip-load", "--search-serial", "--search-concurrent",
        "--m", str(config["m"]), "--ef-construction", str(config["ef_construction"]),
        "--ef-search", str(config["ef_search"]), "--case-type", "PerformanceCustomDataset",
        "--k", str(config["top_k"]), "--num-concurrency", "32", "--concurrency-duration", "30",
        "--custom-case-name",
        f"treedb-4587-{run_row['scale']}-{run_row['role']}-{kind}-{route}",
        "--custom-dataset-name", run_row["dataset"]["name"],
        "--custom-dataset-dir", run_row["dataset"]["directory"],
        "--custom-dataset-size", str(run_row["scale"]),
        "--custom-dataset-dim", str(config["dimensions"]),
        "--custom-dataset-file-count", "1", "--query-embedding-encoding", "f32_le",
    ]
    if kind == "diagnostic":
        command.extend(["--stats-mode", "full_diagnostics", "--response-format", "full"])
    else:
        command.extend([
            "--stats-mode", "production", "--response-format", "ids",
            "--skip-vector-index-guards",
        ])
    if route == "scalar_u8_rerank":
        command.extend([
            "--quantized-index-name", "embedding.scalar_u8.fast",
            "--quantized-rerank-candidates", str(config["configured_rerank_candidates"]),
        ])
    return command





def validate_search_isolation(
    root: Path,
    binding: Any,
    expected_service_argv: list[str],
    expected_binary_sha256: str,
    contract: dict[str, Any],
    name: str,
) -> tuple[datetime, datetime, datetime, datetime]:
    isolation, _ = read_bound_json(root, binding, name)
    exact_keys(isolation, SEARCH_ISOLATION_KEYS, name)
    exact(isolation["schema_version"], SEARCH_ISOLATION_SCHEMA, f"{name}.schema_version")
    exact(isolation["artifact_root"], str(root), f"{name}.artifact_root")
    policy = contract["experiment"]["isolation_and_noise"]
    exact(isolation["lock_path"], policy["lock_path"], f"{name}.lock_path")
    exact(isolation["gomaxprocs"], policy["gomaxprocs"], f"{name}.gomaxprocs")
    exact(isolation["service_binary_sha256"], expected_binary_sha256,
          f"{name}.service_binary_sha256")
    exact(isolation["service_argv"], expected_service_argv, f"{name}.service_argv")
    expected_environment = contract["commands"]["existing_index_search"]["service_environment"]
    exact(isolation["service_environment"], expected_environment,
          f"{name}.service_environment")
    samples = isolation["samples"]
    if not isinstance(samples, list) or len(samples) < 2:
        fail(f"{name}.samples must contain start and completion observations")
    sample_times = []
    for position, sample in enumerate(samples):
        sample = object_at(sample, f"{name}.samples[{position}]")
        exact_keys(sample, SEARCH_ISOLATION_SAMPLE_KEYS, f"{name}.samples[{position}]")
        sample_times.append(utc_timestamp(sample["timestamp"], f"{name}.samples[{position}].timestamp"))
        exact(nonnegative_number(sample["swap_used_bytes"], f"{name}.samples[{position}].swap"),
              0.0, f"{name}.samples[{position}].swap")
        exact(sample["competing_processes"], [], f"{name}.samples[{position}].competitors")
    maximum_gap = policy["diagnostics_interval_seconds"] + policy["sampling_gap_tolerance_seconds"]
    for previous, current in zip(sample_times, sample_times[1:]):
        gap = (current - previous).total_seconds()
        if gap <= 0 or gap > maximum_gap:
            fail("search isolation sampling gap exceeds frozen interval and tolerance")
    lock_acquired = utc_timestamp(isolation["lock_acquired_at"], f"{name}.lock_acquired_at")
    coverage_completed = utc_timestamp(
        isolation["coverage_completed_at"], f"{name}.coverage_completed_at")
    service_started = utc_timestamp(isolation["service_started_at"], f"{name}.service_started_at")
    service_completed = utc_timestamp(
        isolation["service_completed_at"], f"{name}.service_completed_at")
    exact(isolation["service_exit_code"], 0, f"{name}.service_exit_code")
    exact(coverage_completed, sample_times[-1], f"{name}.coverage completion")
    if (lock_acquired > sample_times[0] or service_started < sample_times[0]
            or service_completed < service_started or coverage_completed < service_completed):
        fail("search isolation does not cover the owned service envelope")
    return sample_times[0], coverage_completed, service_started, service_completed


def validate_search_evidence(
    run_row: dict[str, Any], root: Path, timing: dict[str, Any], expected_base_url: str,
    expected_vdbbench_dir: str, expected_gomap_root: str, contract: dict[str, Any],
    expected_binary_sha256: str, authorized_helper_sha256: str,
) -> tuple[dict[str, dict[str, float]], datetime]:
    rows = run_row.get("search_evidence")
    if not isinstance(rows, list) or len(rows) != len(SEARCH_ORDER):
        fail("run.search_evidence must contain exactly four ordered diagnostic/production rows")
    config = run_row["configuration"]
    production: dict[str, dict[str, float]] = {}
    identities: set[tuple[str, int, str]] = set()
    prior_timestamp = -math.inf
    prior_command_completed = timing["completed"]
    parsed_base_url = urlsplit(expected_base_url)
    expected_service_argv = [
        contract["commands"]["binary"],
        "-dir", str(root / "treedb-data"),
        "-addr", f"{parsed_base_url.hostname}:{parsed_base_url.port}",
        "-profile", "command_wal_durable",
    ]
    runtime = contract["source_identity"]["runtime"]
    python_executable = runtime["python_executable"]
    python_sha256 = full_sha(runtime["python_sha256"], "VectorDBBench Python SHA-256", 64)
    if sha256_file(Path(python_executable)) != python_sha256:
        fail("frozen VectorDBBench Python interpreter checksum mismatch")
    expected_helper_path = (
        Path(contract["source_identity"]["gomap_root"]) / SEARCH_HELPER_PATH
    ).resolve()
    expected_helper_sha256 = full_sha(
        authorized_helper_sha256, "authorized existing-index helper SHA-256", 64)
    exact(sha256_file(expected_helper_path), expected_helper_sha256,
          "authorized existing-index helper checksum")
    route_envelopes: dict[
        str, tuple[datetime, datetime, datetime, datetime, Any]
    ] = {}
    prior_envelope_completed = timing["completed"]
    for position, (entry, expected) in enumerate(zip(rows, SEARCH_ORDER, strict=True)):
        item = object_at(entry, f"search_evidence[{position}]")
        exact_keys(item, {"kind", "route", "origin", "result", "response", "index_metadata"},
                   f"search_evidence[{position}]")
        exact((item["kind"], item["route"]), expected, f"search_evidence[{position}] ordering")
        origin, _ = read_bound_json(root, item["origin"], f"search_evidence[{position}].origin")
        _, result_path = read_bound_json(root, item["result"], f"search_evidence[{position}].result")
        response, _ = read_bound_json(root, item["response"], f"search_evidence[{position}].response")
        metadata, _ = read_bound_json(
            root, item["index_metadata"], f"search_evidence[{position}].index_metadata")
        exact_keys(origin, SEARCH_ORIGIN_KEYS, f"search_evidence[{position}].origin")
        expected_origin = {
            "schema_version": SEARCH_ORIGIN_SCHEMA,
            "run_id": run_row["run_id"],
            "artifact_root": str(root),
            "manifest_sha256": run_row["artifact"]["manifest_sha256"],
            "execution_commit": run_row["execution_commit"],
            "dataset_sha256": canonical_sha256(run_row["dataset"]),
            "scale": run_row["scale"],
            "role": run_row["role"],
            "partition": run_row["partition"],
            "ef_construction": run_row["ef_construction"],
            "lifecycle_sha256": timing["sha256"],
            "lifecycle_started_at": timing["started_at"],
            "lifecycle_completed_at": timing["completed_at"],
            "kind": item["kind"],
            "route": item["route"],
            "result_sha256": item["result"]["sha256"],
            "response_sha256": item["response"]["sha256"],
            "index_metadata_sha256": item["index_metadata"]["sha256"],
        }
        for key, expected_value in expected_origin.items():
            exact(origin[key], expected_value,
                  f"search_evidence[{position}] originating run binding {key}")
        exact(origin["service_binary_sha256"], expected_binary_sha256,
              f"search_evidence[{position}] authorized service binary")
        exact(origin["service_argv"], expected_service_argv,
              f"search_evidence[{position}] service argv")
        envelope_started = utc_timestamp(
            origin["search_started_at"], f"search_evidence[{position}] search start")
        envelope_completed = utc_timestamp(
            origin["search_completed_at"], f"search_evidence[{position}] search completion")
        if item["route"] not in route_envelopes:
            observed_start, observed_completed, service_started, service_completed = (
                validate_search_isolation(
                    root, origin["isolation"], expected_service_argv, expected_binary_sha256,
                    contract, f"search_evidence[{position}].isolation"))
            exact((envelope_started, envelope_completed), (observed_start, observed_completed),
                  f"search_evidence[{position}] isolation timing binding")
            if envelope_started <= prior_envelope_completed:
                fail("exact/scalar search envelopes must be ordered after lifecycle completion")
            route_envelopes[item["route"]] = (
                envelope_started, envelope_completed, service_started, service_completed,
                origin["isolation"])
            prior_envelope_completed = envelope_completed
        else:
            exact(
                (envelope_started, envelope_completed, origin["isolation"]),
                (route_envelopes[item["route"]][0], route_envelopes[item["route"]][1],
                 route_envelopes[item["route"]][4]),
                f"search_evidence[{position}] route envelope binding")
            service_started, service_completed = route_envelopes[item["route"]][2:4]
        command = read_probe_command_record(
            root, origin, position, f"search_evidence[{position}].origin",
            expected_helper_path, expected_helper_sha256,
            python_executable, python_sha256)
        exact(command["run_id"], run_row["run_id"], f"search_evidence[{position}] command run")
        exact((command["kind"], command["route"]), (item["kind"], item["route"]),
              f"search_evidence[{position}] command identity")
        exact(command["result_sha256"], item["result"]["sha256"],
              f"search_evidence[{position}] command result checksum")
        exact(command["response_sha256"], item["response"]["sha256"],
              f"search_evidence[{position}] command response checksum")
        exact(command["index_metadata_sha256"], item["index_metadata"]["sha256"],
              f"search_evidence[{position}] command index metadata checksum")
        expected_dataset_files = {
            "test.parquet": run_row["dataset"]["test_sha256"],
            "neighbors.parquet": run_row["dataset"]["neighbors_sha256"],
        }
        exact(command["dataset_files_before_sha256"], expected_dataset_files,
              f"search_evidence[{position}] pre-command dataset checksums")
        exact(command["dataset_files_after_sha256"], expected_dataset_files,
              f"search_evidence[{position}] post-command dataset checksums")
        probe_started = utc_timestamp(
            command["probe_started_at"], f"search_evidence[{position}] probe start")
        probe_completed = utc_timestamp(
            command["probe_completed_at"], f"search_evidence[{position}] probe completion")
        command_started = utc_timestamp(
            command["started_at"], f"search_evidence[{position}] command start")
        command_completed = utc_timestamp(
            command["completed_at"], f"search_evidence[{position}] command completion")
        exact(origin["probe_started_at"], command["probe_started_at"],
              f"search_evidence[{position}] probe start binding")
        exact(origin["probe_completed_at"], command["probe_completed_at"],
              f"search_evidence[{position}] probe completion binding")
        if (probe_started <= prior_command_completed or probe_completed < probe_started
                or command_started < probe_completed or command_completed < command_started
                or (command_started - probe_completed).total_seconds() > 1):
            fail("each diagnostic probe must immediately precede its canonical command")
        if (probe_started < envelope_started or command_started < service_started
                or command_completed > service_completed):
            fail("owned search service and isolation must cover every canonical command")
        prior_command_completed = command_completed
        index_name = metadata.get("name")
        if not isinstance(index_name, str) or not index_name:
            fail("index metadata name must be non-empty")
        options = probe_argv_options(
            command["argv"], f"search_evidence[{position}] command")
        vdbbench_argv = command["vdbbench_argv"]
        exact(options["--base-url"], expected_base_url,
              f"search_evidence[{position}] executed service base URL")
        if (not isinstance(vdbbench_argv, list) or not vdbbench_argv
                or vdbbench_argv[0] != python_executable):
            fail(f"search_evidence[{position}] canonical command must execute the frozen Python interpreter")
        exact(
            vdbbench_argv[1:],
            expected_vdbbench_argv(
                run_row, index_name, item["kind"], item["route"], expected_base_url),
            f"search_evidence[{position}] canonical VectorDBBench argv",
        )
        exact(command["vdbbench_env"], {
            "GOMAXPROCS": str(contract["experiment"]["isolation_and_noise"]["gomaxprocs"]),
            "RESULTS_LOCAL_DIR": str(
                root / f"vdbbench-results-{item['route']}-{item['kind']}"),
            "PYTHONPATH": os.pathsep.join((
                expected_vdbbench_dir,
                str(Path(expected_gomap_root) / "clients/python/treedb_client/src"),
            )),
            "LOG_FILE": str(root / f"vdbbench-{item['route']}-{item['kind']}.log"),
            "NUM_PER_BATCH": "500",
        }, f"search_evidence[{position}] canonical VectorDBBench environment")
        group = rows[(position // 2) * 2:(position // 2) * 2 + 2]
        exact({
            "--index-name": index_name,
            "--metadata-out": str((root / group[0]["index_metadata"]["path"]).resolve()),
            "--diagnostic-response-out": str((root / group[0]["response"]["path"]).resolve()),
            "--production-response-out": str((root / group[1]["response"]["path"]).resolve()),
            "--diagnostic-result": str((root / group[0]["result"]["path"]).resolve()),
            "--production-result": str((root / group[1]["result"]["path"]).resolve()),
            "--diagnostic-origin-out": str((root / group[0]["origin"]["path"]).resolve()),
            "--production-origin-out": str((root / group[1]["origin"]["path"]).resolve()),
            "--command-ledger": str((root / origin["command_ledger_path"]).resolve()),
            "--run-id": run_row["run_id"],
            "--artifact-root": str(root),
            "--manifest-sha256": run_row["artifact"]["manifest_sha256"],
            "--execution-commit": run_row["execution_commit"],
            "--dataset-sha256": canonical_sha256(run_row["dataset"]),
            "--scale": str(run_row["scale"]),
            "--role": run_row["role"],
            "--partition": run_row["partition"],
            "--lifecycle-sha256": timing["sha256"],
            "--lifecycle-started-at": timing["started_at"],
            "--lifecycle-completed-at": timing["completed_at"],
            "--ef-construction": str(run_row["ef_construction"]),
            "--expected-generation": str(metadata.get("generation")),
            "--route": item["route"],
            "--dataset-name": run_row["dataset"]["name"],
            "--dataset-dir": run_row["dataset"]["directory"],
            "--vectordbbench-dir": expected_vdbbench_dir,
            "--service-bin": contract["commands"]["binary"],
            "--python-executable": python_executable,
            "--python-sha256": python_sha256,
            "--service-binary-sha256": expected_binary_sha256,
            "--search-isolation-out": str(root / f"search-isolation-{item['route']}.json"),
            "--exclusive-lock": contract["experiment"]["isolation_and_noise"]["lock_path"],
            "--diagnostics-interval": str(
                contract["experiment"]["isolation_and_noise"]["diagnostics_interval_seconds"]),
            "--service-health-timeout": "60",
        }, {key: options[key] for key in options if key not in {"--base-url", "--query-json"}},
              f"search_evidence[{position}] executed command identity")
        query_path = Path(options["--query-json"]).resolve()
        try:
            query_path.relative_to(root)
        except ValueError:
            fail("probe query path must be inside artifact root")
        exact(sha256_file(query_path), command["query_sha256"],
              f"search_evidence[{position}] command query checksum")
        result, task, metrics = parse_result(result_path, index_name, f"search_evidence[{position}].result")
        db = object_at(task.get("db_config"), f"search_evidence[{position}].db_config")
        case = object_at(task.get("db_case_config"), f"search_evidence[{position}].db_case_config")
        case_cfg = object_at(task.get("case_config"), f"search_evidence[{position}].case_config")
        custom_case = object_at(case_cfg.get("custom_case"), f"search_evidence[{position}].custom_case")
        dataset_cfg = object_at(custom_case.get("dataset_config"),
                                f"search_evidence[{position}].dataset_config")
        expected_dataset_dir = run_row["dataset"]["directory"]
        exact(
            (dataset_cfg.get("name"), dataset_cfg.get("dir"), dataset_cfg.get("size"),
             dataset_cfg.get("dim"), dataset_cfg.get("metric_type"), dataset_cfg.get("file_count"),
             dataset_cfg.get("use_shuffled"), dataset_cfg.get("with_gt")),
            (run_row["dataset"]["name"], expected_dataset_dir, str(run_row["scale"]), "768",
             "COSINE", "1", False, True),
            f"search_evidence[{position}] canonical dataset config",
        )
        concurrency = object_at(case_cfg.get("concurrency_search_config"),
                                f"search_evidence[{position}].concurrency_search_config")
        exact((concurrency.get("num_concurrency"), concurrency.get("concurrency_duration")),
              ([32], 30), f"search_evidence[{position}] concurrency config")
        exact(db.get("query_embedding_encoding"), "f32_le",
              f"search_evidence[{position}] query embedding encoding")
        exact(metrics.get("conc_num_list"), [32], f"search_evidence[{position}] result concurrency")
        expected_mode = "exact" if item["route"] == "exact" else "quantized_rerank"
        exact((case.get("metric_type"), case.get("m"), case.get("ef_construction"), case.get("ef_search")),
              ("COSINE", config["m"], config["ef_construction"], config["ef_search"]),
              f"search_evidence[{position}] graph config")
        exact((case.get("strategy"), case.get("use_vector_index"), case.get("query_mode")),
              ("column_graph", True, expected_mode), f"search_evidence[{position}] route config")
        rerank = config["configured_rerank_candidates"] if expected_mode == "quantized_rerank" else 0
        qname = "embedding.scalar_u8.fast" if expected_mode == "quantized_rerank" else ""
        exact((case.get("quantized_index_name"), case.get("quantized_rerank_candidates")), (qname, rerank),
              f"search_evidence[{position}] rerank config")
        exact(case_cfg.get("k"), config["top_k"], f"search_evidence[{position}] topK")
        exact(task.get("stages"), ["search_serial", "search_concurrent"],
              f"search_evidence[{position}] stages")
        generation = validate_index_metadata(metadata, run_row, index_name, f"search_evidence[{position}].index_metadata")
        identities.add((index_name, generation, canonical_sha256(metadata)))
        timestamp = nonnegative_number(result.get("timestamp"), f"search_evidence[{position}].timestamp")
        if timestamp < prior_timestamp:
            fail("search evidence is not chronologically ordered")
        prior_timestamp = timestamp
        if item["kind"] == "diagnostic":
            exact((db.get("stats_mode"), db.get("response_format"), case.get("require_vector_index_guards")),
                  ("full_diagnostics", "full", True), f"search_evidence[{position}] diagnostic transport")
            exact(response.get("no_documents"), True, f"search_evidence[{position}] no-document boundary")
            exact(response.get("query_mode"), expected_mode, f"search_evidence[{position}] response route")
            results = response.get("results")
            if not isinstance(results, list) or len(results) != config["top_k"]:
                fail(f"search_evidence[{position}] diagnostic result count must equal topK")
            diagnostics = object_at(response.get("diagnostics"), f"search_evidence[{position}].diagnostics")
            exact(diagnostics.get("fallback_reason") or "none", "none",
                  f"search_evidence[{position}] fallback reason")
            stats = object_at(response.get("stats"), f"search_evidence[{position}].stats")
            if not {"documents_fetched", "document_bytes"} <= set(stats):
                fail(f"search_evidence[{position}] document counters are required")
            exact((stats["documents_fetched"], stats["document_bytes"]), (0, 0),
                  f"search_evidence[{position}] document boundary")
            expected_route = "exact_hnsw_search_pack_v1" if expected_mode == "exact" else "quantized_rerank"
            exact(diagnostics.get("route"), expected_route, f"search_evidence[{position}] intended route")
            if expected_mode == "quantized_rerank":
                calls = positive_int(stats.get("quantized_rerank_exact_score_calls"),
                                     f"search_evidence[{position}] exact rerank calls")
                if not config["top_k"] <= calls <= config["effective_rerank_candidates"]:
                    fail(f"search_evidence[{position}] exact rerank calls are outside the frozen boundary")
        else:
            exact((db.get("stats_mode"), db.get("response_format"), case.get("require_vector_index_guards")),
                  ("production", "ids", False), f"search_evidence[{position}] production transport")
            exact(response.get("response_format"), "ids", f"search_evidence[{position}] IDs response")
            ids = response.get("ids")
            if not isinstance(ids, list) or len(ids) != config["top_k"]:
                fail(f"search_evidence[{position}] production result count must equal topK")
            exact(metrics.get("payload_profile"), "ids_only", f"search_evidence[{position}] payload boundary")
            conc = metrics.get("conc_latency_p99_list")
            if not isinstance(conc, list) or len(conc) != 1:
                fail(f"search_evidence[{position}] must contain exactly one concurrent p99")
            production[item["route"]] = {
                "recall": unit_interval_number(metrics.get("recall"), f"search_evidence[{position}].recall"),
                "ndcg": unit_interval_number(metrics.get("ndcg"), f"search_evidence[{position}].ndcg"),
                "qps": positive_number(metrics.get("qps"), f"search_evidence[{position}].qps"),
                "concurrent_p99_ms": 1000 * positive_number(conc[0], f"search_evidence[{position}].p99"),
            }
    if len(identities) != 1:
        fail("diagnostic and production search rows do not bind one exact existing index identity")
    exact(set(production), {"exact", "scalar_u8_rerank"}, "production search routes")
    return production, prior_envelope_completed


def validate_nonoverlapping_lifecycles(rows: list[dict[str, Any]], name: str) -> None:
    for previous, current in zip(rows, rows[1:]):
        if previous["timing"]["completed"] >= current["timing"]["started"]:
            fail(f"{name} lifecycle order must be strictly non-overlapping")


def validate_manifest(run_row: dict[str, Any], root: Path, manifest: dict[str, Any], contract: dict[str, Any],
                      packet_commit: str, run_base_validator: bool) -> str:
    if run_base_validator:
        harness = Path(contract["source_identity"]["gomap_root"]) / "scripts/treedb_vectordbbench_artifact.py"
        result = subprocess.run([sys.executable, str(harness), "--validate-lifecycle", str(root)], text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            fail(f"base lifecycle validator rejected {root}: {result.stdout or result.stderr}")
    exact(manifest.get("schema_version"), "treedb-vectordbbench-artifact/v1", "artifact schema")
    exact(manifest.get("artifact_root"), str(root), "artifact root")
    context = object_at(manifest.get("context"), "manifest.context")
    host = object_at(context.get("host"), "manifest.context.host")
    runtime = contract["source_identity"]["runtime"]
    exact(host.get("go"), f"go version {runtime['go']}", "artifact Go runtime")
    if not isinstance(host.get("python"), str) or not host["python"].startswith(runtime["python"] + " "):
        fail("artifact Python runtime does not match frozen identity")
    exact(host.get("pyarrow"), runtime["pyarrow"], "artifact PyArrow runtime")
    exact(host.get("cpu_brand"), runtime["host_cpu"], "artifact CPU identity")
    exact((host.get("physical_cpu_count"), host.get("logical_cpu_count")),
          (runtime["physical_cores"], runtime["logical_cpus"]), "artifact CPU topology")
    exact(host.get("gomaxprocs"), str(runtime["gomaxprocs"]), "artifact GOMAXPROCS")
    storage = object_at(host.get("storage"), "manifest.context.host.storage")
    exact((storage.get("mount"), storage.get("filesystem")),
          (runtime["storage_mount"], runtime["filesystem"]), "artifact storage identity")
    gomap = object_at(context.get("gomap"), "manifest.context.gomap")
    vdb = object_at(context.get("vectordbbench"), "manifest.context.vectordbbench")
    exact((gomap.get("commit"), gomap.get("dirty")), (packet_commit, False), "artifact gomap identity")
    exact((vdb.get("commit"), vdb.get("dirty")),
          (contract["source_identity"]["vectordbbench"]["commit"], False), "artifact VectorDBBench identity")
    lifecycle = object_at(manifest.get("lifecycle"), "manifest.lifecycle")
    exact(lifecycle.get("result_status"), "completed", "lifecycle result status")
    exact(lifecycle.get("expected_rows"), run_row["scale"], "lifecycle expected rows")
    identity = object_at(lifecycle.get("identity"), "lifecycle.identity")
    exact(identity.get("gomap_commit"), packet_commit, "lifecycle gomap commit")
    binary_sha = full_sha(identity.get("service_binary_sha256"), "service binary SHA-256", 64)
    exact(binary_sha, manifest["service"]["binary"]["sha256"], "binary identity")
    exact(identity.get("vectordbbench_commit"), contract["source_identity"]["vectordbbench"]["commit"],
          "lifecycle VectorDBBench commit")
    dataset = object_at(lifecycle.get("dataset"), "lifecycle.dataset")
    expected_dataset = run_row["dataset"]
    exact((dataset.get("name"), dataset.get("vectors"), dataset.get("dimensions"), dataset.get("sha256")),
          (expected_dataset["name"], expected_dataset["vectors"], expected_dataset["dimensions"],
           expected_dataset["train_sha256"]), "lifecycle dataset")
    harness_cfg = object_at(manifest.get("harness"), "manifest.harness")
    exact(harness_cfg.get("construction_decision_diagnostics"), True,
          "lifecycle construction-decision diagnostics")
    exact(
        (harness_cfg.get("python_executable"), harness_cfg.get("python_sha256"),
         harness_cfg.get("use_uv")),
        (runtime["python_executable"], runtime["python_sha256"], "off"),
        "lifecycle harness Python environment",
    )
    commands = contract["commands"]
    exact(harness_cfg.get("service_environment"),
          commands["lifecycle_service_environment"],
          "lifecycle service subprocess environment")
    environment_template = commands["lifecycle_vdbbench_environment_template"]
    expected_environment = {
        key: value.format(artifact_root=str(root), row="scalar")
        for key, value in environment_template.items()
    }
    exact(harness_cfg.get("vdbbench_environments"), {"scalar": expected_environment},
          "lifecycle VectorDBBench subprocess environments")
    config = run_row["configuration"]
    exact((harness_cfg.get("m"), harness_cfg.get("ef_construction"), harness_cfg.get("ef_search"),
           harness_cfg.get("k"), harness_cfg.get("rerank_candidates"), harness_cfg.get("rows")),
          (config["m"], config["ef_construction"], config["ef_search"], config["top_k"],
           config["configured_rerank_candidates"], "scalar"), "lifecycle harness config")
    data_dir = Path(manifest["service"]["data_dir"]).resolve()
    exact(data_dir, (root / "treedb-data").resolve(), "fresh artifact-owned data root")
    return binary_sha
def nested_numeric_values(value: Any, key: str) -> list[float]:
    found: list[float] = []
    if isinstance(value, dict):
        for current_key, current_value in value.items():
            if current_key == key:
                if isinstance(current_value, (int, float)) and not isinstance(current_value, bool):
                    found.append(float(current_value))
                elif isinstance(current_value, str) and re.fullmatch(r"\d+(?:\.\d+)?", current_value):
                    found.append(float(current_value))
            found.extend(nested_numeric_values(current_value, key))
    elif isinstance(value, list):
        for item in value:
            found.extend(nested_numeric_values(item, key))
    return found


def measurement_source_values(
    root: Path, binding: Any, producer_phases: dict[str, float],
    construction_decisions: dict[str, Any], configuration: dict[str, Any],
    manifest: dict[str, Any], expected_isolation_binding: dict[str, Any],
) -> dict[str, Any]:
    source, _ = read_bound_json(root, binding, "measurements.source")
    exact_keys(source, {
        "schema_version", "adapter_lifecycle", "diagnostics", "isolation",
        "data_root", "data_files",
    }, "measurement source")
    exact(source["schema_version"], "treedb-construction-policy-4587-measurement-source/v2",
          "measurement source schema")
    adapter_binding = object_at(source["adapter_lifecycle"], "measurement source adapter")
    diagnostics_binding = object_at(source["diagnostics"], "measurement source diagnostics")
    isolation_binding = object_at(source["isolation"], "measurement source isolation")
    exact_keys(adapter_binding, {"path", "sha256"}, "measurement source adapter binding")
    exact_keys(diagnostics_binding, {"path", "sha256"}, "measurement source diagnostics binding")
    exact_keys(isolation_binding, {"path", "sha256"}, "measurement source isolation binding")
    exact(isolation_binding, expected_isolation_binding,
          "measurement source isolation run binding")
    read_bound_json(root, isolation_binding, "measurement source isolation")
    raw_artifacts = object_at(manifest.get("lifecycle"), "manifest.lifecycle").get("raw_artifacts")
    if not isinstance(raw_artifacts, list):
        fail("manifest.lifecycle.raw_artifacts must be a list")
    for expected_path, source_binding, name in (
        ("adapter-lifecycle.jsonl", adapter_binding, "adapter"),
        ("diagnostics.jsonl", diagnostics_binding, "diagnostics"),
    ):
        matches = [item for item in raw_artifacts if isinstance(item, dict)
                   and item.get("path") == expected_path]
        if len(matches) != 1:
            fail(f"manifest lifecycle must bind exactly one measurement source {name}")
        exact(source_binding, matches[0], f"measurement source {name} manifest binding")
    adapter_relative = Path(adapter_binding["path"])
    if adapter_relative.is_absolute() or ".." in adapter_relative.parts:
        fail("measurement source adapter path must be artifact-root relative")
    adapter_path = (root / adapter_relative).resolve()
    exact(sha256_file(adapter_path), adapter_binding["sha256"],
          "measurement source adapter checksum")
    diagnostics_path = Path(diagnostics_binding["path"])
    if diagnostics_path.is_absolute() or ".." in diagnostics_path.parts:
        fail("measurement source diagnostics path must be artifact-root relative")
    diagnostics_path = (root / diagnostics_path).resolve()
    exact(sha256_file(diagnostics_path), diagnostics_binding["sha256"],
          "measurement source diagnostics checksum")
    diagnostics = [object_at(json.loads(line), "measurement diagnostic record")
                   for line in diagnostics_path.read_text().splitlines()]
    boundaries = {
        sample.get("boundary"): sample for sample in diagnostics
        if isinstance(sample.get("boundary"), str)
    }
    start = object_at(boundaries.get("optimize_start", {}).get("process"), "optimize start process")
    end = object_at(boundaries.get("optimize_end", {}).get("process"), "optimize end process")
    elapsed = producer_phases["optimize"]
    cpu = positive_number(
        (positive_int(end.get("cpu_nanoseconds"), "optimize end CPU")
         - nonnegative_int(start.get("cpu_nanoseconds"), "optimize start CPU"))
        / 1_000_000_000 / elapsed,
        "raw optimize CPU utilization",
    )
    allocated = [
        metric for sample in diagnostics
        for metric in nested_numeric_values(
            sample.get("snapshot"), "treedb.process.memory.total_alloc_bytes")
    ]
    if not allocated:
        fail("measurement source is missing cumulative allocation telemetry")
    data_root = Path(source["data_root"]).resolve()
    manifest_data_root = Path(
        object_at(manifest.get("service"), "manifest.service").get("data_dir", "")).resolve()
    exact(data_root, manifest_data_root, "measurement source manifest data root binding")
    data_files = source["data_files"]
    if not isinstance(data_files, list) or not data_files:
        fail("measurement source data file ledger must be non-empty")
    expected_paths = set()
    expected_file_ids = set()
    persisted = 0
    for position, item in enumerate(data_files):
        item = object_at(item, f"measurement source data_files[{position}]")
        exact_keys(item, {"path", "size", "sha256"}, f"measurement source data_files[{position}]")
        relative = Path(item["path"])
        if relative.is_absolute() or ".." in relative.parts:
            fail("measurement source data file path must be relative")
        if not is_treedb_data_file(relative):
            fail(f"unexpected TreeDB data file path: {relative.as_posix()}")
        path = (data_root / relative).resolve()
        try:
            path.relative_to(data_root)
        except ValueError:
            fail("measurement source data file path must remain inside data root")
        if path in expected_paths:
            fail("measurement source data file paths must be unique")
        file_stat = path.stat()
        exact(file_stat.st_nlink, 1, "measurement source data file link count")
        file_id = (file_stat.st_dev, file_stat.st_ino)
        if file_id in expected_file_ids:
            fail("measurement source data files must have unique file identities")
        exact(file_stat.st_size, nonnegative_int(item["size"], "measurement source file size"),
              "measurement source file size")
        exact(sha256_file(path), item["sha256"], "measurement source file checksum")
        expected_paths.add(path)
        expected_file_ids.add(file_id)
        persisted += item["size"]
    actual_paths = {path.resolve() for path in data_root.rglob("*") if path.is_file()}
    exact(actual_paths, expected_paths, "measurement source complete data file ledger")
    data_digest = canonical_sha256(data_files)
    adapter_digest = sha256_file(adapter_path)
    peak_rss = max(
        positive_int(object_at(sample.get("process"), "measurement process").get("peak_rss_bytes"),
                     "measurement process peak RSS")
        for sample in diagnostics if isinstance(sample.get("process"), dict)
    )
    return {
        "phase_seconds": producer_phases,
        "cpu_utilization_logical_cores": cpu,
        "determinism": {
            "graph_config_checksum": canonical_sha256(configuration),
            "persisted_data_ledger_checksum": data_digest,
            "adapter_lifecycle_checksum": adapter_digest,
        },
        "diagnostic_work_profile": construction_decisions,
        "resources": {
            "peak_rss_bytes": peak_rss,
            "persisted_bytes": persisted,
            "cumulative_allocated_bytes": max(allocated),
        },
        "projected_10m_adjacency_reduction_fraction": projected_10m_reduction(
            producer_phases["adjacency"],
            object_at(manifest.get("lifecycle"), "manifest.lifecycle").get("expected_rows")),
    }




def validate_run(row: dict[str, Any], contract: dict[str, Any], packet_commit: str,
                 run_base_validator: bool, authorized_helper_sha256: str) -> dict[str, Any]:
    exact_keys(row, RUN_KEYS, "run")
    if not isinstance(row["run_id"], str) or not row["run_id"]:
        fail("run.run_id must be a non-empty string")
    if row["ef_construction"] not in COORDINATES:
        fail("run ef_construction is outside C0")
    exact(row["execution_commit"], packet_commit, "run execution commit")
    full_sha(row["execution_commit"], "run.execution_commit")
    exact(row["dataset"], dataset_expected(contract, row["scale"], row["partition"]), "run dataset binding")
    graph = contract["experiment"]["graph"]
    search = contract["experiment"]["search"]
    expected_config = {"dimensions": graph["dimensions"], "metric": graph["metric"], "m": graph["m"],
                       "ef_construction": row["ef_construction"], "ef_search": search["ef_search"],
                       "configured_rerank_candidates": search["configured_rerank_candidates"],
                       "effective_rerank_candidates": search["effective_rerank_candidates"],
                       "top_k": search["top_k"]}
    exact(row["configuration"], expected_config, "run configuration")
    artifact = object_at(row["artifact"], "run.artifact")
    exact_keys(artifact, {"root", "manifest_sha256"}, "run.artifact")
    if not isinstance(artifact["root"], str) or not artifact["root"]:
        fail("run.artifact.root must be a non-empty path")
    root = Path(artifact["root"]).resolve()
    manifest_path = root / "manifest.json"
    full_sha(artifact["manifest_sha256"], "run.artifact.manifest_sha256", 64)
    exact(sha256_file(manifest_path), artifact["manifest_sha256"], "artifact manifest SHA-256")
    manifest = object_at(json.loads(manifest_path.read_text()), "manifest")
    service_binary_sha256 = validate_manifest(
        row, root, manifest, contract, packet_commit, run_base_validator)
    construction_decisions, producer_phases = validate_construction_evidence(root, manifest)
    timing = lifecycle_timing(root, manifest)
    isolation, _ = read_bound_json(root, row["isolation_evidence"], "run.isolation_evidence")
    exact_keys(isolation, ISOLATION_KEYS, "isolation")
    exact(isolation.get("schema_version"), ISOLATION_SCHEMA, "isolation schema")
    exact(isolation.get("artifact_root"), str(root), "isolation artifact root")
    exact(isolation.get("lock_path"), contract["experiment"]["isolation_and_noise"]["lock_path"], "isolation lock")
    exact(isolation.get("lock_held_through_evidence"), True, "isolation lock coverage")
    exact(isolation.get("gomaxprocs"), contract["experiment"]["isolation_and_noise"]["gomaxprocs"],
          "isolation GOMAXPROCS")
    exact(isolation.get("competing_processes"), [], "competing process series")
    samples = isolation.get("samples")
    if not isinstance(samples, list) or len(samples) < 2:
        fail("isolation samples must contain at least start and completion observations")
    sample_times = []
    for position, sample in enumerate(samples):
        sample = object_at(sample, f"isolation.samples[{position}]")
        exact_keys(sample, ISOLATION_SAMPLE_KEYS, f"isolation.samples[{position}]")
        sample_times.append(utc_timestamp(sample["timestamp"], f"isolation.samples[{position}].timestamp"))
        exact(sample["competing_processes"], [], f"isolation.samples[{position}].competing_processes")
        exact(nonnegative_number(sample["swap_used_bytes"],
                                 f"isolation.samples[{position}].swap_used_bytes"),
              0.0, f"isolation.samples[{position}].swap_used_bytes")
    if sample_times != sorted(sample_times):
        fail("isolation sample timestamps must be ordered")
    maximum_gap = (
        contract["experiment"]["isolation_and_noise"]["diagnostics_interval_seconds"]
        + contract["experiment"]["isolation_and_noise"]["sampling_gap_tolerance_seconds"])
    for previous, current in zip(sample_times, sample_times[1:]):
        gap = (current - previous).total_seconds()
        if gap <= 0 or gap > maximum_gap:
            fail("isolation sampling gap exceeds frozen interval and tolerance")
    lock_acquired = utc_timestamp(isolation["lock_acquired_at"], "isolation.lock_acquired_at")
    coverage_completed = utc_timestamp(isolation["coverage_completed_at"], "isolation.coverage_completed_at")
    if lock_acquired > timing["started"] or sample_times[0] > timing["started"]:
        fail("isolation lock and sampling must precede lifecycle start")
    if coverage_completed < timing["completed"] or sample_times[-1] < timing["completed"]:
        fail("isolation sampling must cover lifecycle completion")
    exact(coverage_completed, sample_times[-1], "isolation completion timestamp")
    exact(nonnegative_number(isolation["peak_swap_used_bytes"], "isolation.peak_swap_used_bytes"),
          0.0, "isolation.peak_swap_used_bytes")
    measurements, _ = read_bound_json(root, row["measurement_evidence"], "run.measurement_evidence")
    exact(measurements.get("schema_version"), MEASUREMENT_SCHEMA, "measurement schema")
    exact_keys(measurements, MEASUREMENT_KEYS, "measurements")
    origin = object_at(measurements.get("origin"), "measurements.origin")
    exact_keys(origin, MEASUREMENT_ORIGIN_KEYS, "measurements.origin")
    exact(origin, {
        "run_id": row["run_id"],
        "artifact_root": str(root),
        "execution_commit": row["execution_commit"],
        "dataset_sha256": canonical_sha256(row["dataset"]),
        "scale": row["scale"],
        "role": row["role"],
        "partition": row["partition"],
        "ef_construction": row["ef_construction"],
        "lifecycle_sha256": timing["sha256"],
        "lifecycle_started_at": timing["started_at"],
        "lifecycle_completed_at": timing["completed_at"],
    }, "measurement originating run binding")
    expected_measurements = measurement_source_values(
        root, measurements["source"], producer_phases, construction_decisions,
        row["configuration"], manifest, row["isolation_evidence"])
    for key, expected_value in expected_measurements.items():
        if key != "projected_10m_adjacency_reduction_fraction":
            exact(measurements[key], expected_value, f"measurements.{key} raw source binding")
    cpu_utilization = positive_number(
        measurements["cpu_utilization_logical_cores"], "measurements.cpu_utilization_logical_cores")
    if cpu_utilization > contract["source_identity"]["runtime"]["logical_cpus"]:
        fail("measurements.cpu_utilization_logical_cores must not exceed logical_cpus")
    phases = object_at(measurements["phase_seconds"], "measurements.phase_seconds")
    resources = object_at(measurements["resources"], "measurements.resources")
    for key, value in resources.items():
        positive_number(value, f"resources.{key}")
    exact(measurements["projected_10m_adjacency_reduction_fraction"], None,
          "raw producer projection field")
    projection = expected_measurements["projected_10m_adjacency_reduction_fraction"]
    manifest_data_root = Path(
        object_at(manifest["service"], "manifest.service")["data_dir"]).resolve()
    exact(manifest_data_root, (root / "treedb-data").resolve(),
          "search manifest retained data root")
    production, search_completed = validate_search_evidence(
        row, root, timing, object_at(manifest["service"], "manifest.service").get("base_url"),
        contract["source_identity"]["vectordbbench"]["root"],
        contract["source_identity"]["gomap_root"], contract, service_binary_sha256,
        authorized_helper_sha256)
    return {"row": row, "phases": phases, "resources": resources, "production": production,
            "projection": projection, "service_binary_sha256": service_binary_sha256,
            "timing": timing, "search_completed": search_completed}


def clears_gates(candidate: dict[str, Any], control: dict[str, Any], require_projection: bool,
                 gates: dict[str, Any]) -> bool:
    cand_exact, ctrl_exact = candidate["production"]["exact"], control["production"]["exact"]
    cand_prod = candidate["production"]["scalar_u8_rerank"]
    ctrl_prod = control["production"]["scalar_u8_rerank"]
    reduction = 1 - candidate["phases"]["adjacency"] / control["phases"]["adjacency"]
    increase = gates["maximum_unexplained_persisted_or_allocation_increase_bytes"]
    checks = [
        reduction >= gates["minimum_adjacency_reduction_fraction"],
        ctrl_exact["recall"] - cand_exact["recall"] <= gates["maximum_absolute_recall_loss"],
        ctrl_exact["ndcg"] - cand_exact["ndcg"] <= gates["maximum_absolute_ndcg_loss"],
        ctrl_prod["recall"] - cand_prod["recall"] <= gates["maximum_absolute_recall_loss"],
        ctrl_prod["ndcg"] - cand_prod["ndcg"] <= gates["maximum_absolute_ndcg_loss"],
        cand_prod["qps"] >= gates["minimum_production_qps_ratio"] * ctrl_prod["qps"],
        cand_prod["concurrent_p99_ms"]
        <= gates["maximum_concurrent_p99_ratio"] * ctrl_prod["concurrent_p99_ms"],
        candidate["resources"]["peak_rss_bytes"]
        <= gates["maximum_peak_rss_ratio"] * control["resources"]["peak_rss_bytes"],
        candidate["resources"]["persisted_bytes"] <= control["resources"]["persisted_bytes"] + increase,
        candidate["resources"]["cumulative_allocated_bytes"]
        <= control["resources"]["cumulative_allocated_bytes"] + increase,
    ]
    if require_projection:
        checks.append(
            finite_number(candidate["projection"], "candidate projected 10M adjacency reduction")
            >= gates["minimum_projected_10m_adjacency_reduction_fraction"]
        )
    return all(checks)


def select_screening_winner(screening: list[dict[str, Any]], gates: dict[str, Any]) -> dict[str, Any] | None:
    control = screening[-1]
    eligible = [item for item in screening[:-1] if clears_gates(item, control, False, gates)]
    if not eligible:
        return None
    return sorted(eligible, key=lambda item: (
        -(1 - item["phases"]["adjacency"] / control["phases"]["adjacency"]),
        max(
            control["production"][route][metric] - item["production"][route][metric]
            for route in ("exact", "scalar_u8_rerank")
            for metric in ("recall", "ndcg")
        ),
        item["row"]["ef_construction"],
    ))[0]


def validate_winner_selection(binding: Any, contract: dict[str, Any], authorization_sha256: str,
                              screening: list[dict[str, Any]], winner: dict[str, Any]) -> datetime:
    event, _ = read_bound_json(
        Path(contract["commands"]["artifact_root"]), binding, "decision.winner_selection")
    exact_keys(event, {
        "schema_version", "execution_commit", "contract_sha256", "authorization_sha256",
        "screening_runs", "selected_ef_construction", "selected_at",
    }, "winner selection event")
    exact(event["schema_version"], WINNER_SELECTION_SCHEMA, "winner selection schema")
    exact(event["execution_commit"], winner["row"]["execution_commit"],
          "winner selection execution commit")
    exact(event["contract_sha256"], canonical_sha256(contract), "winner selection contract SHA-256")
    exact(event["authorization_sha256"], authorization_sha256,
          "winner selection authorization SHA-256")
    expected_runs = [{
        "run_id": item["row"]["run_id"],
        "artifact_root": item["row"]["artifact"]["root"],
        "measurement_sha256": item["row"]["measurement_evidence"]["sha256"],
        "lifecycle_sha256": item["timing"]["sha256"],
        "completed_at": item["timing"]["completed_at"],
        "search_completed_at": item["search_completed"].isoformat(),
    } for item in screening]
    exact(event["screening_runs"], expected_runs, "winner selection screening evidence")
    exact(event["selected_ef_construction"], winner["row"]["ef_construction"],
          "winner selection coordinate")
    selected_at = utc_timestamp(event["selected_at"], "winner selection timestamp")
    if any(item["search_completed"] >= selected_at for item in screening):
        fail("winner selection must occur after all screening search envelopes complete")
    return selected_at


def validate_decision(packet: dict[str, Any], contract: dict[str, Any], *, run_base_validator: bool = True,
                      require_clean_head: bool = True,
                      expected_authorization: dict[str, Any] | None = None) -> dict[str, Any]:
    exact_keys(packet, {
        "schema_version", "execution_commit", "contract_sha256", "authorization",
        "winner_selection", "verdict", "runs",
    }, "decision")
    exact(packet.get("schema_version"), RESULT_SCHEMA, "result schema_version")
    gates = validate_go_gates(contract)
    validate_python_command_contract(contract)
    if packet.get("verdict") not in {"GO", "C0_NO_GO"}:
        fail("result verdict must be GO or C0_NO_GO")
    commit = full_sha(packet.get("execution_commit"), "decision.execution_commit")
    root = Path(contract["source_identity"]["gomap_root"])
    if require_clean_head:
        exact(run("git", "rev-parse", "HEAD", cwd=root), commit, "decision execution commit")
        exact(run("git", "status", "--porcelain=v1", cwd=root), "", "decision source cleanliness")
    exact(packet.get("contract_sha256"), canonical_sha256(contract), "decision contract SHA-256")
    authorization_path, expected_checksum = resolve_authorization_binding(packet["authorization"], contract)
    exact(sha256_file(authorization_path), expected_checksum, "decision authorization SHA-256")
    authorization = validate_authorization(
        contract, authorization_path, require_clean_head=require_clean_head)
    exact(authorization["execution_commit"], commit, "decision authorized execution commit")
    if expected_authorization is not None:
        exact((authorization["path"], authorization["sha256"]),
              (expected_authorization["path"], expected_authorization["sha256"]),
              "decision preflight authorization")
    runs = packet.get("runs")
    if not isinstance(runs, list):
        fail("result runs must be a list")
    validated = [
        validate_run(
            object_at(row, f"runs[{index}]"), contract, commit, run_base_validator,
            authorization["protocol_files"][SEARCH_HELPER_PATH])
        for index, row in enumerate(runs)
    ]
    binary_digests = {item["service_binary_sha256"] for item in validated}
    exact(binary_digests, {authorization["service_binary_sha256"]},
          "one authorized service binary across all runs")
    roots = [item["row"]["artifact"]["root"] for item in validated]
    run_ids = [item["row"]["run_id"] for item in validated]
    if len(set(roots)) != len(roots) or len(set(run_ids)) != len(run_ids):
        fail("artifact roots and run IDs must be unique")
    for previous, current in zip(validated, validated[1:]):
        if previous["search_completed"] >= current["timing"]["started"]:
            fail("each lifecycle and search envelope must complete before the next lifecycle starts")
    screening = validated[:4]
    exact(len(screening), 4, "screening cardinality")
    exact([(item["row"]["scale"], item["row"]["ef_construction"], item["row"]["partition"], item["row"]["role"])
           for item in screening],
          [(250000, 128, "selection", "screening_candidate"),
           (250000, 192, "selection", "screening_candidate"),
           (250000, 256, "selection", "screening_candidate"),
           (250000, 300, "selection", "screening_control")], "screening cardinality and order")
    validate_nonoverlapping_lifecycles(screening, "screening")
    winner = select_screening_winner(screening, gates)
    if winner is None:
        exact(packet["winner_selection"], None, "no-winner selection event")
        exact(len(validated), 4, "no-winner run cardinality")
        exact(packet["verdict"], "C0_NO_GO", "no-winner verdict")
        return {"verdict": "C0_NO_GO", "winner": None, "performance_gate": "NO-GO",
                "service_binary_sha256": authorization["service_binary_sha256"]}
    exact(len(validated), 6, "winner run cardinality")
    selected_at = validate_winner_selection(
        packet["winner_selection"], contract, expected_checksum, screening, winner)
    decision = validated[4:]
    if any(item["search_completed"] >= selected_at for item in screening):
        fail("winner selection must follow every completed screening search envelope")
    exact([
        (item["row"]["scale"], item["row"]["ef_construction"],
         item["row"]["partition"], item["row"]["role"])
        for item in decision
    ], [
        (1000000, 300, "holdout", "decision_control"),
        (1000000, winner["row"]["ef_construction"], "holdout", "decision_candidate"),
    ], "holdout cardinality and order")
    if any(item["timing"]["started"] <= selected_at for item in decision):
        fail("holdout lifecycle must start after the checksum-bound winner selection")
    if decision[0]["search_completed"] >= decision[1]["timing"]["started"]:
        fail("decision control lifecycle and search envelope must complete before candidate lifecycle starts")
    for item in decision:
        for key in RESOURCE_KEYS:
            positive_number(item["resources"][key], f"decision {item['row']['role']} resources.{key}")
    passed = clears_gates(decision[1], decision[0], True, gates)
    exact(packet["verdict"], "GO" if passed else "C0_NO_GO", "computed verdict")
    return {"verdict": packet["verdict"], "winner": winner["row"]["ef_construction"],
            "performance_gate": "GO" if passed else "NO-GO",
            "service_binary_sha256": authorization["service_binary_sha256"]}


def generate_winner_selection(contract: dict[str, Any], runs_path: Path, output_path: Path,
                              authorization_path: Path) -> dict[str, str]:
    authorization = validate_authorization(contract, authorization_path)
    rows = json.loads(runs_path.read_text())
    if not isinstance(rows, list) or len(rows) != 4:
        fail("screening runs file must contain exactly four rows")
    commit = authorization["execution_commit"]
    screening = [
        validate_run(
            object_at(row, f"screening runs[{index}]"), contract, commit, True,
            authorization["protocol_files"][SEARCH_HELPER_PATH])
        for index, row in enumerate(rows)
    ]
    roots = [item["row"]["artifact"]["root"] for item in screening]
    run_ids = [item["row"]["run_id"] for item in screening]
    if len(set(roots)) != 4 or len(set(run_ids)) != 4:
        fail("screening artifact roots and run IDs must be unique")
    exact([
        (item["row"]["scale"], item["row"]["ef_construction"], item["row"]["partition"], item["row"]["role"])
        for item in screening
    ], [
        (250000, 128, "selection", "screening_candidate"),
        (250000, 192, "selection", "screening_candidate"),
        (250000, 256, "selection", "screening_candidate"),
        (250000, 300, "selection", "screening_control"),
    ], "screening cardinality and order")
    validate_nonoverlapping_lifecycles(screening, "screening")
    for previous, current in zip(screening, screening[1:]):
        if previous["search_completed"] >= current["timing"]["started"]:
            fail("each screening search envelope must complete before the next lifecycle starts")
    exact(
        {item["service_binary_sha256"] for item in screening},
        {authorization["service_binary_sha256"]},
        "one authorized service binary across screening runs",
    )
    winner = select_screening_winner(screening, validate_go_gates(contract))
    if winner is None:
        fail("screening has no winner; holdout execution is forbidden")
    selected_at = datetime.now(timezone.utc)
    if any(item["search_completed"] >= selected_at for item in screening):
        fail("winner selection timestamp does not follow all screening search envelopes")
    root = Path(contract["commands"]["artifact_root"]).resolve()
    output_path = output_path.resolve()
    try:
        relative = output_path.relative_to(root)
    except ValueError:
        fail("winner selection output must be inside commands.artifact_root")
    event = {
        "schema_version": WINNER_SELECTION_SCHEMA,
        "execution_commit": commit,
        "contract_sha256": canonical_sha256(contract),
        "authorization_sha256": authorization["sha256"],
        "screening_runs": [{
            "run_id": item["row"]["run_id"],
            "artifact_root": item["row"]["artifact"]["root"],
            "measurement_sha256": item["row"]["measurement_evidence"]["sha256"],
            "lifecycle_sha256": item["timing"]["sha256"],
            "completed_at": item["timing"]["completed_at"],
            "search_completed_at": item["search_completed"].isoformat(),
        } for item in screening],
        "selected_ef_construction": winner["row"]["ef_construction"],
        "selected_at": selected_at.isoformat(),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(event, indent=2, sort_keys=True) + "\n")
    return {"path": relative.as_posix(), "sha256": sha256_file(output_path)}


def print_screening_commands(contract: dict[str, Any]) -> None:
    template = contract["commands"]["lifecycle_harness_argv_template"]
    root = contract["commands"]["artifact_root"]
    dataset = contract["datasets"]["screening"]
    for ef in COORDINATES:
        values = {
            "artifact_root": f"{root}/screening-ef{ef}",
            "ef_construction": str(ef),
            "dataset_dir": dataset["directory"],
            "dataset_name": dataset["name"],
            "dataset_sha256": canonical_sha256(
                dataset_expected(contract, 250000, "selection")),
            "vectors": str(dataset["vectors"]),
            "index_prefix": f"treedb_4587_c0_250k_ef{ef}",
            "db_label": f"treedb-4587-c0-250k-ef{ef}",
            "case_name": f"cohere250k-selection-ef{ef}",
            "run_id": f"screening-ef{ef}",
            "role": "screening_control" if ef == CONTROL else "screening_candidate",
            "partition": "selection",
        }
        argv = [token.format(**values) for token in template]
        print("GOMAXPROCS=12 " + subprocess.list2cmdline(argv))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("contract", type=Path)
    parser.add_argument("--draft", action="store_true", help="validate a draft without authorizing execution")
    parser.add_argument("--authorization", type=Path,
                        help="external authorization path; defaults to commands.authorization_manifest")
    parser.add_argument("--generate-authorization", type=Path)
    parser.add_argument("--service-binary", type=Path)
    parser.add_argument("--reviewed-head")
    parser.add_argument("--generate-winner-selection", type=Path)
    parser.add_argument("--screening-runs", type=Path)
    parser.add_argument("--decision", type=Path)
    parser.add_argument("--print-screening-commands", action="store_true")
    args = parser.parse_args()
    try:
        contract = json.loads(args.contract.read_text())
        if args.generate_authorization:
            if (args.draft or args.decision or args.authorization or args.print_screening_commands
                    or args.generate_winner_selection or args.screening_runs
                    or not args.service_binary or not args.reviewed_head):
                fail("--generate-authorization requires --service-binary and --reviewed-head only")
            generated = generate_authorization(
                contract, args.generate_authorization, args.service_binary, args.reviewed_head)
            print(json.dumps({"authorization": generated}, indent=2, sort_keys=True))
            return 0
        if args.generate_winner_selection:
            if (args.draft or args.decision or args.print_screening_commands or args.service_binary
                    or args.reviewed_head or not args.authorization or not args.screening_runs):
                fail("--generate-winner-selection requires --authorization and --screening-runs only")
            validate_contract(contract, False, args.authorization)
            binding = generate_winner_selection(
                contract, args.screening_runs, args.generate_winner_selection, args.authorization)
            print(json.dumps({"winner_selection": binding}, indent=2, sort_keys=True))
            return 0
        if args.screening_runs:
            fail("--screening-runs requires --generate-winner-selection")
        decision = json.loads(args.decision.read_text()) if args.decision else None
        authorization_path = args.authorization
        if decision is not None and authorization_path is None:
            authorization_path, _ = resolve_authorization_binding(decision.get("authorization"), contract)
        report = validate_contract(contract, args.draft, authorization_path)
        if args.print_screening_commands:
            if not report["execution_authorized"]:
                fail("screening commands require final external authorization")
            print_screening_commands(contract)
        if decision is not None:
            if args.draft:
                fail("draft mode cannot validate a decision packet")
            report["decision"] = validate_decision(
                decision, contract, expected_authorization=report["authorization"])
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError,
            json.JSONDecodeError) as exc:
        print(f"INVALID: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
