#!/usr/bin/env python3
"""Fail-closed consumer for the Q5 Cohere SQ8 qualification packet.

The packet is an externally SHA-256-pinned inventory.  This program validates
the retained producer evidence and derives the qualification result; it does
not run a benchmark or infer an independently verified final-state digest.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import stat
import statistics
import subprocess
import sys

import numpy as np

import minima_cohere_native_diagnostic as native
import minima_cohere_qdrant_rss_diagnostic as qdrant

PACKET_SCHEMA = "treedb_cohere_q5_packet/v2"
ANALYSIS_SCHEMA = "treedb_cohere_q5_analysis/v2"
NORMALIZED_PACKET_SCHEMA = "treedb_cohere_normalized_v4_packet/v1"
NORMALIZED_ANALYSIS_SCHEMA = "treedb_cohere_normalized_v4_analysis/v1"
RECEIPT_SCHEMA = "treedb_cohere_q5_command_receipts/v2"
TRADEOFF_REVIEW_SCHEMA = "treedb_cohere_q5_tradeoff_review/v1"
MAX_PACKET_BYTES = 1 << 20
MAX_JSON_BYTES = 64 << 20
MAX_JSONL_BYTES = 1 << 30
MAX_JSONL_LINE_BYTES = 8 << 20
MAX_JSONL_LINES = 1_000_000
MAX_RETAINED_NATIVE_EVENTS = 100_000
SHA256_HEX = frozenset("0123456789abcdef")
FULL_ELIGIBLE_COUNTS = native.counts(500000)
NORMALIZED_RUN_FILES = {"plan", "events", "truth", "resources"}
NORMALIZED_SQ8_RUN_FILES = NORMALIZED_RUN_FILES | {"matrix", "engine"}
NORMALIZED_INPUT_KINDS = {"treedb_service_binary", "go_helper", "serving"}
NORMALIZED_PREDECESSORS = ((4730, 4731), (4723, 4732), (4724, 4733), (4725, 4735))
HISTORICAL_EXACT_QPS = 3281.0
HISTORICAL_EXACT_P50_NS = 305_303.0
HISTORICAL_EXACT_P95_NS = 367_344.0
HISTORICAL_CANDIDATE_NS = 83_400.0
HISTORICAL_PACKED_QPS = 144_421.0

ARM_KINDS = {
    "smoke_exact", "smoke_sq8", "bounded_exact", "bounded_sq8",
    "treedb_fp32_rss", "treedb_sq8_rss", "qdrant_fp32_rss",
    "fp32_comparison", "three_arm_comparison", "full_sq8_events",
    "command_receipts",
}
RUN_ARM_KINDS = ARM_KINDS - {
    "fp32_comparison", "three_arm_comparison", "command_receipts",
}
DATASET_KINDS = {"manifest", "documents", "queries", "truth"}
PLAN_KINDS = {
    "smoke_exact", "smoke_sq8", "treedb_fp32_rss", "treedb_sq8_rss",
    "qdrant_fp32_rss", "full_sq8_events",
}
INPUT_KINDS = {
    "bounded_validator_binary", "bounded_manifest", "bounded_sq8_plan",
    "treedb_service_binary", "serving", "qdrant_binary",
}
REQUIRED_RECEIPT_ENV = {
    "HOME", "PATH", "TMPDIR", "TZ", "LANG", "LC_ALL", "GOWORK", "GOMAXPROCS",
    "GOGC", "GOMEMLIMIT", "GODEBUG", "PYTHONHASHSEED", "PYTHONDONTWRITEBYTECODE",
    "OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS",
}
HARNESS_TREE_PATHS = (
    "benchmarks/vector_db_compare", "clients/python/treedb_client",
)
PRODUCT_TREE_PATHS = (
    "TreeDB", "cmd/treedb-document-service", "internal", "go.mod", "go.sum",
)
NORMALIZED_CONSUMER_ONLY_PATHS = frozenset({
    "benchmarks/vector_db_compare/minima_cohere_q5_analyze.py",
    "benchmarks/vector_db_compare/test_minima_cohere_q5_analyze.py",
    "benchmarks/vector_db_compare/cohere_scale_harness.md",
})
BOUNDED_MANIFEST_SCHEMA = "treedb_rag_minima_manifest/v2"
BOUNDED_ARTIFACT_SCHEMA = "treedb_rag_application/minima_diagnostic_v1"
BOUNDED_SQ8_ARTIFACT_SCHEMA = "treedb_rag_application/minima_quantized_diagnostic_v1"
BOUNDED_SQ8_PLAN_SCHEMA = "treedb_minima_quantized_plan/v1"
LEGACY_BASELINE_CLASSIFICATION = "known_legacy_complete_finite_ann_failure"
LEGACY_BASELINE_FAILURES = [
    "initial exact oracle mismatch for broad_10pct",
    "RuntimeError: timed query 3 does not match its frozen oracle",
]
LEGACY_BASELINE_IDS = [f"minima/broad_10pct/{ordinal:06d}" for ordinal in range(1000, 1005)]
# The frozen exact control uses EF=128. Its native runtime can search one base
# and one live-delta plane, each bounded by EF * nativeScalarANNVisitFactor.
LEGACY_BASELINE_ANN_MAX_SCORED = 2 * 128 * 16
# Vector-aligned routes share this many eligible-region probe rows across both
# planes; complete-finite routes do not perform eligible-region seeding.
LEGACY_BASELINE_ANN_MAX_SEED_ROWS = 4096
LEGACY_BASELINE_ANN_ROUTES = {
    "all_match": "vector_aligned_ann",
    "over_limit_4097": "vector_aligned_ann",
    "broad_10pct": "complete_finite_ann",
    "sparse_over_limit": "vector_aligned_ann",
}
LEGACY_BASELINE_RAW_KEYS = {
    "diagnostic_resume", "diagnostics", "events", "final_scroll_state",
    "native_route_responses", "phase_attribution", "phase_latency_distributions",
    "resource_availability", "resource_measurement", "restart_boundary", "service_log",
    "timed_overlap", "upsert_batch_correlation_contract", "upsert_batch_correlations",
}
LEGACY_BASELINE_DIAGNOSTICS = {
    "capture_timeout_seconds": 10, "directory": None, "enabled": False,
    "nonqualifying": False, "profile_seconds": 5, "slow_batch_seconds": 30,
}
LEGACY_BASELINE_CORRELATION_CONTRACT = {
    "compact_completed_records": 0, "compact_record_max_bytes": 2048,
    "full_diagnostic_records": 0,
    "full_stats_retention": ["failed", "timeout", "slow", "profile_captured"],
    "maximum_record_count": 205, "record_count": 0,
    "schema": "treedb-minima-upsert-batch-correlations/v1",
}
LEGACY_BASELINE_PHASES = [
    ("initial_durable_load", "production_path"),
    ("warmup_search", "production_path"),
    ("timed_search_write_overlap", "production_path"),
]


class EvidenceError(ValueError):
    pass


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise EvidenceError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _constant(value):
    raise EvidenceError(f"non-finite JSON number: {value}")


def _float(value):
    number = float(value)
    if not math.isfinite(number):
        raise EvidenceError(f"non-finite JSON number: {value}")
    return number


def decode_json(raw, label="JSON"):
    try:
        return json.loads(
            raw, object_pairs_hook=_pairs, parse_constant=_constant, parse_float=_float,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceError(f"{label} is not strict JSON: {exc}") from exc


def read_json(path, label, maximum=MAX_JSON_BYTES):
    path = Path(path)
    try:
        with path.open("rb") as source:
            raw = source.read(maximum + 1)
    except OSError as exc:
        raise EvidenceError(f"{label} is unavailable: {exc}") from exc
    if len(raw) > maximum:
        raise EvidenceError(f"{label} exceeds {maximum} bytes")
    value = decode_json(raw, label)
    if not isinstance(value, dict):
        raise EvidenceError(f"{label} must be one JSON object")
    return value


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def valid_sha256(value):
    return isinstance(value, str) and len(value) == 64 and set(value) <= SHA256_HEX


def valid_commit(value):
    return isinstance(value, str) and len(value) == 40 and set(value) <= SHA256_HEX


def valid_git_oid(value):
    return isinstance(value, str) and len(value) in (40, 64) and set(value) <= SHA256_HEX


def read_jsonl(path):
    """Stream strict JSONL with per-line, total-byte, and line-count bounds."""
    total = 0
    try:
        source = Path(path).open("rb")
    except OSError as exc:
        raise EvidenceError(f"JSONL is unavailable: {exc}") from exc
    with source:
        number = 0
        while True:
            raw = source.readline(MAX_JSONL_LINE_BYTES + 1)
            if not raw:
                break
            number += 1
            total += len(raw)
            if len(raw) > MAX_JSONL_LINE_BYTES:
                raise EvidenceError(f"JSONL line {number} exceeds line byte limit")
            if total > MAX_JSONL_BYTES:
                raise EvidenceError("JSONL exceeds total byte limit")
            if number > MAX_JSONL_LINES:
                raise EvidenceError("JSONL exceeds line-count limit")
            if not raw.strip():
                raise EvidenceError(f"JSONL line {number} is empty")
            value = decode_json(raw, f"JSONL line {number}")
            if not isinstance(value, dict):
                raise EvidenceError(f"JSONL line {number} must be one object")
            yield value


def read_native_events(path):
    events = []
    for event in read_jsonl(path):
        if len(events) >= MAX_RETAINED_NATIVE_EVENTS:
            raise EvidenceError("native log exceeds retained event limit")
        events.append(event)
    return events


def load_packet(packet_path, expected_sha256):
    packet_path = Path(packet_path).resolve()
    try:
        with packet_path.open("rb") as source:
            raw = source.read(MAX_PACKET_BYTES + 1)
    except OSError as exc:
        raise EvidenceError(f"packet is unavailable: {exc}") from exc
    if len(raw) > MAX_PACKET_BYTES:
        raise EvidenceError("packet exceeds 1 MiB")
    if not valid_sha256(expected_sha256) or hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise EvidenceError("packet differs from its external SHA-256 pin")
    packet = decode_json(raw, "packet")
    if isinstance(packet, dict) and packet.get("schema") == NORMALIZED_PACKET_SCHEMA:
        expected = {
            "schema", "candidate_commit", "files", "dataset", "inputs",
            "runs", "predecessors",
        }
        if set(packet) != expected or not valid_commit(packet.get("candidate_commit")):
            raise EvidenceError("normalized-v4 packet has an unknown or incomplete schema")
        return packet_path, packet
    expected = {
        "schema", "candidate_commit", "files", "arms", "plans", "dataset", "inputs",
        "declared_quality_outcome", "tradeoff_review",
    }
    if not isinstance(packet, dict) or set(packet) != expected or packet["schema"] != PACKET_SCHEMA:
        raise EvidenceError("packet has an unknown or incomplete schema")
    if not valid_commit(packet["candidate_commit"]):
        raise EvidenceError("candidate commit must be a full lowercase Git object ID")
    if packet["declared_quality_outcome"] not in ("pass", "miss"):
        raise EvidenceError("declared quality outcome must be pass or miss")
    return packet_path, packet


def validate_tradeoff_review(review, quality_passed):
    if (not isinstance(review, dict)
            or set(review) != {"schema", "disposition", "rationale", "authority"}
            or review.get("schema") != TRADEOFF_REVIEW_SCHEMA
            or not isinstance(review.get("rationale"), str) or not review["rationale"]):
        raise EvidenceError("packet tradeoff review is incomplete")
    disposition = review.get("disposition")
    if quality_passed:
        if disposition not in {
                "no_material_regression", "owner_accepted_material_regression",
                "unaccepted_material_regression"}:
            raise EvidenceError("passing quality has an invalid tradeoff disposition")
    elif disposition != "not_reached_after_quality_miss":
        raise EvidenceError("quality miss must stop before a tradeoff disposition")
    authority = review.get("authority")
    if disposition == "owner_accepted_material_regression":
        if not isinstance(authority, str) or not authority:
            raise EvidenceError("material regression lacks explicit owner acceptance")
    elif authority is not None:
        raise EvidenceError("tradeoff authority is only valid for explicit owner acceptance")
    return disposition in {"no_material_regression", "owner_accepted_material_regression"}


def resolve_inventory(packet_path, packet):
    root = packet_path.parent.resolve()
    files = packet["files"]
    if not isinstance(files, dict) or not files:
        raise EvidenceError("packet file inventory is empty")
    resolved, paths, hashes = {}, set(), set()
    for logical, entry in files.items():
        if (not isinstance(logical, str) or not logical or not isinstance(entry, dict)
                or set(entry) != {"path", "sha256", "bytes"}):
            raise EvidenceError("invalid logical file inventory entry")
        if not isinstance(entry["path"], str) or not entry["path"]:
            raise EvidenceError("inventory paths must be nonempty strings")
        relative = Path(entry["path"])
        path = (root / relative).resolve()
        if relative.is_absolute() or not path.is_relative_to(root):
            raise EvidenceError(f"inventory path escapes packet directory: {logical}")
        if path in paths:
            raise EvidenceError("two logical files resolve to the same path")
        if not valid_sha256(entry["sha256"]) or entry["sha256"] in hashes:
            raise EvidenceError("logical file hashes must be valid and unique")
        if type(entry["bytes"]) is not int or entry["bytes"] < 0:
            raise EvidenceError("logical file byte size is invalid")
        try:
            file_stat = path.stat()
        except OSError as exc:
            raise EvidenceError(f"inventory file is unavailable: {logical}: {exc}") from exc
        if not stat.S_ISREG(file_stat.st_mode):
            raise EvidenceError(f"inventory entry is not a regular file: {logical}")
        size = file_stat.st_size
        if size != entry["bytes"] or sha256_file(path) != entry["sha256"]:
            raise EvidenceError(f"inventory file changed after packet freeze: {logical}")
        resolved[logical] = path
        paths.add(path)
        hashes.add(entry["sha256"])

    arms = packet["arms"]
    plans = packet["plans"]
    dataset = packet["dataset"]
    inputs = packet["inputs"]
    if not isinstance(arms, dict) or set(arms) != ARM_KINDS:
        raise EvidenceError("packet does not contain the exact Q5 arm map")
    if not isinstance(dataset, dict) or set(dataset) != DATASET_KINDS:
        raise EvidenceError("packet does not contain the exact dataset map")
    if not isinstance(plans, dict) or set(plans) != PLAN_KINDS:
        raise EvidenceError("packet does not contain the exact frozen plan map")
    if not isinstance(inputs, dict) or set(inputs) != INPUT_KINDS:
        raise EvidenceError("packet frozen input map is invalid")
    references = (list(arms.values()) + list(plans.values())
                  + list(dataset.values()) + list(inputs.values()))
    if any(not isinstance(name, str) or name not in resolved for name in references):
        raise EvidenceError("packet references an unknown logical file")
    if len(references) != len(set(references)) or set(references) != set(resolved):
        raise EvidenceError("every logical file must have exactly one semantic role")
    return resolved


def resolve_normalized_inventory(packet_path, packet):
    root = packet_path.parent.resolve()
    files = packet.get("files")
    if not isinstance(files, dict) or not files:
        raise EvidenceError("normalized-v4 packet file inventory is empty")
    resolved, paths = {}, set()
    for logical, entry in files.items():
        if (not isinstance(logical, str) or not logical or not isinstance(entry, dict)
                or set(entry) != {"path", "sha256", "bytes"}
                or not isinstance(entry.get("path"), str)
                or not valid_sha256(entry.get("sha256"))
                or type(entry.get("bytes")) is not int or entry["bytes"] < 0):
            raise EvidenceError("normalized-v4 logical file inventory entry is invalid")
        relative = Path(entry["path"])
        path = (root / relative).resolve()
        if relative.is_absolute() or not path.is_relative_to(root) or path in paths:
            raise EvidenceError("normalized-v4 inventory path escapes or aliases another role")
        try:
            file_stat = path.stat()
        except OSError as exc:
            raise EvidenceError(f"normalized-v4 inventory file is unavailable: {logical}: {exc}") from exc
        if (not stat.S_ISREG(file_stat.st_mode) or file_stat.st_size != entry["bytes"]
                or sha256_file(path) != entry["sha256"]):
            raise EvidenceError(f"normalized-v4 inventory file changed after packet freeze: {logical}")
        resolved[logical] = path
        paths.add(path)

    dataset = packet.get("dataset")
    inputs = packet.get("inputs")
    runs = packet.get("runs")
    if not isinstance(dataset, dict) or set(dataset) != DATASET_KINDS:
        raise EvidenceError("normalized-v4 packet dataset map is incomplete")
    if not isinstance(inputs, dict) or set(inputs) != NORMALIZED_INPUT_KINDS:
        raise EvidenceError("normalized-v4 packet input map is incomplete")
    if not isinstance(runs, dict) or set(runs) != {"exact", "sq8"}:
        raise EvidenceError("normalized-v4 packet must contain exact and SQ8 runs")
    references = list(dataset.values()) + list(inputs.values())
    for mode, required in (("exact", NORMALIZED_RUN_FILES), ("sq8", NORMALIZED_SQ8_RUN_FILES)):
        run = runs[mode]
        if (not isinstance(run, dict)
                or set(run) != {"files", "support", "freeze_argv", "run_argv", "exit_code"}
                or not isinstance(run["files"], dict) or set(run["files"]) != required
                or not isinstance(run["support"], dict)
                or type(run["exit_code"]) is not int or run["exit_code"] != 0):
            raise EvidenceError(f"normalized-v4 {mode} run receipt is incomplete")
        for argv_name in ("freeze_argv", "run_argv"):
            if (not isinstance(run[argv_name], list) or not run[argv_name]
                    or any(not isinstance(value, str) or not value for value in run[argv_name])):
                raise EvidenceError(f"normalized-v4 {mode} command receipt is invalid")
        if any(not isinstance(key, str) or not key or not isinstance(value, str)
               for key, value in run["support"].items()):
            raise EvidenceError(f"normalized-v4 {mode} support inventory is invalid")
        references.extend(run["files"].values())
        references.extend(run["support"].values())
    if (any(not isinstance(name, str) or name not in resolved for name in references)
            or len(references) != len(set(references)) or set(references) != set(resolved)):
        raise EvidenceError("every normalized-v4 file must have exactly one semantic role")

    predecessors = packet.get("predecessors")
    if not isinstance(predecessors, list) or len(predecessors) != len(NORMALIZED_PREDECESSORS):
        raise EvidenceError("normalized-v4 predecessor receipt is incomplete")
    for row, (issue, pull) in zip(predecessors, NORMALIZED_PREDECESSORS):
        if (not isinstance(row, dict)
                or set(row) != {"issue", "pull", "reviewed_head", "merge_commit", "reviewed_tree", "merge_tree"}
                or (row.get("issue"), row.get("pull")) != (issue, pull)
                or any(not valid_commit(row.get(field)) for field in (
                    "reviewed_head", "merge_commit", "reviewed_tree", "merge_tree",
                ))
                or row["reviewed_tree"] != row["merge_tree"]):
            raise EvidenceError("normalized-v4 predecessor merge tree differs from its reviewed head")
    return resolved


def validate_dataset(packet, paths):
    mapped = {kind: paths[packet["dataset"][kind]] for kind in DATASET_KINDS}
    manifest = read_json(mapped["manifest"], "dataset manifest", MAX_PACKET_BYTES)
    if (manifest.get("dimensions"), manifest.get("top_k"), manifest.get("rows"),
            manifest.get("query_count")) != (768, 10, 500000, 200):
        raise EvidenceError("dataset manifest is not the frozen 500K x 768D, 200-query export")
    if manifest.get("exact_train_query_overlap") != 0:
        raise EvidenceError("dataset manifest does not attest a disjoint query/train byte export")
    exporter = Path(__file__).with_name("prepare_cohere_scale.py")
    if (manifest.get("exporter_sha256") != sha256_file(exporter)
            or not isinstance(manifest.get("train_source"), str) or not manifest["train_source"]
            or not isinstance(manifest.get("query_source"), str) or not manifest["query_source"]):
        raise EvidenceError("dataset manifest does not bind the reviewed exporter and source paths")
    for kind in ("documents", "queries", "truth"):
        logical = packet["dataset"][kind]
        if manifest.get(kind + "_sha256") != packet["files"][logical]["sha256"]:
            raise EvidenceError(f"dataset manifest does not bind {kind}")
    expected_sizes = {"documents": 500000 * 768 * 4, "queries": 200 * 768 * 4}
    for kind, size in expected_sizes.items():
        if packet["files"][packet["dataset"][kind]]["bytes"] != size:
            raise EvidenceError(f"dataset {kind} byte count is invalid")
    return mapped, manifest


def _receipt_path(value, cwd):
    path = Path(value)
    return (path if path.is_absolute() else Path(cwd) / path).resolve()


def _receipt_executable(value, cwd, environment):
    if "/" in value:
        return _receipt_path(value, cwd)
    resolved = shutil.which(value, path=environment["PATH"])
    if resolved is None:
        raise EvidenceError("command interpreter is unavailable on the recorded PATH")
    return Path(resolved).resolve()


def _taskset_cpu_list(value):
    cpus = []
    try:
        for item in value.split(","):
            bounds = item.split("-")
            if len(bounds) == 1:
                cpus.append(int(bounds[0]))
            elif len(bounds) == 2:
                first, last = map(int, bounds)
                if last < first:
                    raise ValueError
                cpus.extend(range(first, last + 1))
            else:
                raise ValueError
    except (AttributeError, TypeError, ValueError):
        raise EvidenceError("command has an invalid taskset CPU list") from None
    if not cpus or any(cpu < 0 for cpu in cpus) or len(cpus) != len(set(cpus)):
        raise EvidenceError("command has an invalid taskset CPU list")
    return sorted(cpus)


def _validate_python_argv(
        argv, cwd, environment, script, cpu_affinity,
        expected_values, expected_flags, path_options):
    """Parse one closed Python CLI grammar and bind every option to its frozen value."""
    expected_taskset = shutil.which("taskset", path=environment["PATH"])
    if (not isinstance(argv, list) or len(argv) < 5
            or not all(isinstance(value, str) and value for value in argv)
            or expected_taskset is None
            or _receipt_executable(argv[0], cwd, environment) != Path(expected_taskset).resolve()
            or argv[1] not in {"-c", "--cpu-list"}
            or _taskset_cpu_list(argv[2]) != cpu_affinity
            or _receipt_executable(argv[3], cwd, environment) != Path(sys.executable).resolve()
            or _receipt_path(argv[4], cwd) != Path(script).resolve()):
        raise EvidenceError(
            "command does not use its frozen CPU affinity, Python interpreter, and script",
        )
    found_values, found_flags = {}, set()
    index = 5
    while index < len(argv):
        token = argv[index]
        if token in expected_flags:
            if token in found_flags:
                raise EvidenceError(f"command repeats {token}")
            found_flags.add(token)
            index += 1
            continue
        option, separator, inline = token.partition("=")
        if option not in expected_values:
            raise EvidenceError(f"command contains an unknown or inapplicable option: {option}")
        if option in found_values:
            raise EvidenceError(f"command repeats {option}")
        if separator:
            value = inline
            index += 1
        else:
            if index + 1 >= len(argv):
                raise EvidenceError(f"command omits the value for {option}")
            value = argv[index + 1]
            index += 2
        if not value:
            raise EvidenceError(f"command omits the value for {option}")
        found_values[option] = value
    if set(found_values) != set(expected_values) or found_flags != set(expected_flags):
        raise EvidenceError("command omits or adds a role-specific option")
    for option, expected in expected_values.items():
        matches = (_receipt_path(found_values[option], cwd) == Path(expected).resolve()
                   if option in path_options else found_values[option] == str(expected))
        if not matches:
            raise EvidenceError(f"command option differs from its frozen plan: {option}")


def _inventory_path(packet, paths, group, role):
    return paths[packet[group][role]].resolve()


def _inventory_hash(packet, group, role):
    return packet["files"][packet[group][role]]["sha256"]


def _dataset_root(packet, paths):
    expected = {
        "manifest": "manifest.json", "documents": "documents.f32",
        "queries": "queries.f32", "truth": "truth.json",
    }
    manifest = _inventory_path(packet, paths, "dataset", "manifest")
    root = manifest.parent
    if any(_inventory_path(packet, paths, "dataset", role) != root / filename
           for role, filename in expected.items()):
        raise EvidenceError("dataset command root does not own the exact inventoried files")
    return root


def _absolute_plan_path(plan, field, expected):
    value = plan.get(field)
    return (isinstance(value, str) and Path(value).is_absolute()
            and Path(value).resolve() == Path(expected).resolve())


def _qdrant_plan_matches_comparison_contract(plan):
    contract = plan.get("comparison_contract") or {}
    controls = contract.get("ann_controls") or {}
    return (
        tuple(plan.get(field) for field in ("rows", "dimensions", "top_k", "batch_size"))
        == tuple(contract.get(field) for field in ("rows", "dimensions", "top_k", "batch_size"))
        and plan.get("dataset_manifest_sha256") == contract.get("dataset_manifest_sha256")
        and native.same_json(plan.get("dataset_files_sha256"),
                             contract.get("dataset_files_sha256"))
        and native.same_json(plan.get("cpu_affinity"), contract.get("cpu_affinity"))
        and plan.get("platform") == contract.get("platform")
        and native.same_json(plan.get("controls"), controls.get("qdrant_hnsw_ef"))
        and native.same_json(controls.get("treedb_ef_search"), native.RSS_CONTROLS)
    )


def _native_receipt_contract(packet, paths, role, plan):
    quantized = role in {"smoke_sq8", "treedb_sq8_rss", "full_sq8_events"}
    rss_only = role in {"treedb_fp32_rss", "treedb_sq8_rss"}
    rows = 512 if role.startswith("smoke_") else 500000
    dataset = _dataset_root(packet, paths)
    service = _inventory_path(packet, paths, "inputs", "treedb_service_binary")
    serving = _inventory_path(packet, paths, "inputs", "serving")
    run_dir_value = plan.get("run_dir")
    run_dir = Path(run_dir_value).resolve() if isinstance(run_dir_value, str) else Path()
    output = _inventory_path(packet, paths, "arms", role)
    expected_output = run_dir / ("rss.json" if rss_only else "events.jsonl")
    if (not _absolute_plan_path(plan, "dataset", dataset)
            or not _absolute_plan_path(plan, "service_bin", service)
            or not _absolute_plan_path(plan, "serving_path", serving)
            or not isinstance(run_dir_value, str) or not Path(run_dir_value).is_absolute()
            or output != expected_output
            or plan.get("product_commit") != packet["candidate_commit"]
            or plan.get("dataset_manifest_sha256")
                != _inventory_hash(packet, "dataset", "manifest")
            or not native.same_json(plan.get("dataset_files_sha256"), _dataset_hashes(packet))
            or plan.get("service_sha256")
                != _inventory_hash(packet, "inputs", "treedb_service_binary")
            or plan.get("serving_sha256") != _inventory_hash(packet, "inputs", "serving")
            or plan.get("rows") != rows or plan.get("rss_only") is not rss_only
            or plan.get("query_mode", "exact") != ("quantized_rerank" if quantized else "exact")
            or plan.get("ef_construction") != native.DEFAULT_EF_CONSTRUCTION
            or any(not isinstance(plan.get(field), str) or not plan[field]
                   for field in ("url", "native_address", "diagnostics_url"))):
        raise EvidenceError(f"{role} command paths or hashes differ from packet inventory")
    values = {
        "--dataset": dataset, "--service-bin": service,
        "--product-commit": packet["candidate_commit"], "--serving": serving,
        "--run-dir": run_dir, "--rows": rows,
        "--query-mode": "quantized_rerank" if quantized else "exact",
        "--ef-construction": native.DEFAULT_EF_CONSTRUCTION,
        "--url": plan.get("url"), "--native-address": plan.get("native_address"),
        "--diagnostics-url": plan.get("diagnostics_url"),
    }
    flags = {"--rss-only"} if rss_only else set()
    if plan.get("construction_decisions") is True:
        flags.add("--construction-decisions")
    elif plan.get("construction_decisions") is not False:
        raise EvidenceError(f"{role} plan has an invalid construction observer mode")
    if quantized:
        values["--quantized-index-name"] = native.QUANTIZED_PROFILE_NAME
    if role == "full_sq8_events":
        sq8 = _inventory_path(packet, paths, "arms", "treedb_sq8_rss")
        sq8_sha = _inventory_hash(packet, "arms", "treedb_sq8_rss")
        lock = plan.get("all_rows_coordinate_lock") or {}
        if (not _absolute_plan_path(lock, "artifact_path", sq8)
                or lock.get("artifact_sha256") != sq8_sha):
            raise EvidenceError("full SQ8 command does not consume the inventoried RSS lock")
        values.update({
            "--all-rows-sq8-rss-artifact": sq8,
            "--expected-all-rows-sq8-rss-artifact-sha256": sq8_sha,
        })
    path_options = {
        "--dataset", "--service-bin", "--serving", "--run-dir",
        "--all-rows-sq8-rss-artifact",
    } & set(values)
    return Path(native.__file__).resolve(), values, flags, path_options


def _qdrant_receipt_contract(packet, paths, plan):
    dataset = _dataset_root(packet, paths)
    treedb = _inventory_path(packet, paths, "arms", "treedb_fp32_rss")
    sq8 = _inventory_path(packet, paths, "arms", "treedb_sq8_rss")
    binary = _inventory_path(packet, paths, "inputs", "qdrant_binary")
    run_dir_value, storage_value = plan.get("run_dir"), plan.get("storage_path")
    run_dir = Path(run_dir_value).resolve() if isinstance(run_dir_value, str) else Path()
    storage = Path(storage_value).resolve() if isinstance(storage_value, str) else Path()
    outputs = {
        "qdrant_fp32_rss": "qdrant-rss.json",
        "fp32_comparison": "comparison.json",
        "three_arm_comparison": "three-arm-comparison.json",
    }
    if (not _absolute_plan_path(plan, "dataset", dataset)
            or not _absolute_plan_path(plan, "treedb_artifact", treedb)
            or not _absolute_plan_path(plan, "treedb_sq8_artifact", sq8)
            or not _absolute_plan_path(plan, "qdrant_bin", binary)
            or not isinstance(run_dir_value, str) or not Path(run_dir_value).is_absolute()
            or not isinstance(storage_value, str) or not Path(storage_value).is_absolute()
            or any(_inventory_path(packet, paths, "arms", role) != run_dir / filename
                   for role, filename in outputs.items())
            or plan.get("dataset_manifest_sha256")
                != _inventory_hash(packet, "dataset", "manifest")
            or not native.same_json(plan.get("dataset_files_sha256"), _dataset_hashes(packet))
            or plan.get("treedb_artifact_sha256")
                != _inventory_hash(packet, "arms", "treedb_fp32_rss")
            or plan.get("treedb_sq8_artifact_sha256")
                != _inventory_hash(packet, "arms", "treedb_sq8_rss")
            or plan.get("qdrant_bin_sha256") != _inventory_hash(packet, "inputs", "qdrant_binary")
            or not _qdrant_plan_matches_comparison_contract(plan)
            or any(not isinstance(plan.get(field), str) or not plan[field]
                   for field in ("url", "collection"))
            or any(type(plan.get(field)) not in (int, float) or not math.isfinite(plan[field])
                   or plan[field] <= 0 for field in (
                       "operation_timeout_s", "startup_timeout_s",
                       "optimizer_timeout_s", "poll_interval_s",
                   ))):
        raise EvidenceError(
            "Qdrant command paths, hashes, or comparison contract differ from packet inventory",
        )
    values = {
        "--dataset": dataset, "--treedb-artifact": treedb,
        "--treedb-sq8-artifact": sq8, "--qdrant-bin": binary,
        "--storage-path": storage, "--url": plan.get("url"),
        "--collection": plan.get("collection"), "--run-dir": run_dir,
        "--operation-timeout": plan.get("operation_timeout_s"),
        "--startup-timeout": plan.get("startup_timeout_s"),
        "--optimizer-timeout": plan.get("optimizer_timeout_s"),
        "--poll-interval": plan.get("poll_interval_s"),
    }
    path_options = {
        "--dataset", "--treedb-artifact", "--treedb-sq8-artifact",
        "--qdrant-bin", "--storage-path", "--run-dir",
    }
    return Path(qdrant.__file__).resolve(), values, set(), path_options


def _source_trees(candidate_commit):
    source = Path(__file__).resolve().parents[2]

    def object_id(path):
        try:
            completed = subprocess.run(
                ["git", "rev-parse", f"{candidate_commit}:{path}"], cwd=source,
                text=True, capture_output=True, timeout=30, check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise EvidenceError(f"candidate source identity unavailable for {path}: {exc}") from exc
        value = completed.stdout.strip()
        if completed.returncode or not valid_git_oid(value):
            raise EvidenceError(f"candidate source identity unavailable for {path}")
        return value

    return (
        {path: object_id(path) for path in HARNESS_TREE_PATHS},
        {path: object_id(path) for path in PRODUCT_TREE_PATHS},
    )


def validate_consumer_source(candidate_commit, *, analyzer_commit=None):
    """Bind source to the producer, or to an explicit consumer-only descendant."""
    source = Path(__file__).resolve().parents[2]
    paths = [*HARNESS_TREE_PATHS]
    source_commit = candidate_commit
    changed_paths = []
    if analyzer_commit is not None:
        if not valid_git_oid(analyzer_commit):
            raise EvidenceError("normalized-v4 analyzer commit must be a full Git object ID")
        source_commit = analyzer_commit
        paths.extend(PRODUCT_TREE_PATHS)
        try:
            ancestor = subprocess.run(
                ["git", "merge-base", "--is-ancestor", candidate_commit, analyzer_commit],
                cwd=source, text=True, capture_output=True, timeout=30, check=False,
            )
            delta = subprocess.run(
                ["git", "diff", "--name-only", "--no-renames", "-z", candidate_commit,
                 analyzer_commit, "--", *paths],
                cwd=source, text=True, capture_output=True, timeout=30, check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise EvidenceError(f"consumer-only source delta is unavailable: {exc}") from exc
        changed_paths = sorted(set(delta.stdout.split("\0")) - {""})
        if (ancestor.returncode or delta.returncode
                or set(changed_paths) - NORMALIZED_CONSUMER_ONLY_PATHS):
            raise EvidenceError("analyzer source is not a permitted consumer-only descendant")
    commands = (
        ["git", "diff", "--quiet", source_commit, "--", *paths],
        ["git", "diff", "--cached", "--quiet", source_commit, "--", *paths],
    )
    try:
        for command in commands:
            completed = subprocess.run(
                command, cwd=source, text=True, capture_output=True, timeout=30, check=False,
            )
            if completed.returncode:
                raise EvidenceError("executing consumer/import tree differs from the candidate")
        status = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=all", "--", *paths],
            cwd=source, text=True, capture_output=True, timeout=30, check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise EvidenceError(f"consumer source identity is unavailable: {exc}") from exc
    if status.returncode or status.stdout:
        raise EvidenceError("executing consumer/import tree is dirty or untracked")
    imported = {Path(__file__).resolve()}
    for module in tuple(sys.modules.values()):
        module_file = getattr(module, "__file__", None)
        if not module_file:
            continue
        path = Path(module_file).resolve()
        if path.is_relative_to(source):
            if path.suffix != ".py":
                raise EvidenceError("local consumer import is not candidate Python source")
            imported.add(path)
    blobs = {}
    for path in sorted(imported):
        relative = str(path.relative_to(source))
        try:
            completed = subprocess.run(
                ["git", "show", f"{source_commit}:{relative}"], cwd=source,
                capture_output=True, timeout=30, check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise EvidenceError(f"candidate consumer blob unavailable for {relative}: {exc}") from exc
        digest = hashlib.sha256(completed.stdout).hexdigest()
        if completed.returncode or digest != sha256_file(path):
            raise EvidenceError(f"executing consumer import differs from candidate: {relative}")
        if analyzer_commit is not None and relative not in NORMALIZED_CONSUMER_ONLY_PATHS:
            try:
                producer_blob = subprocess.run(
                    ["git", "show", f"{candidate_commit}:{relative}"], cwd=source,
                    capture_output=True, timeout=30, check=False,
                )
            except (OSError, subprocess.TimeoutExpired) as exc:
                raise EvidenceError(f"producer import blob unavailable for {relative}: {exc}") from exc
            if producer_blob.returncode or hashlib.sha256(producer_blob.stdout).hexdigest() != digest:
                raise EvidenceError(f"consumer-only repair changed a producer import: {relative}")
        blobs[relative] = digest
    harness_trees, _ = _source_trees(source_commit)
    identity = {
        "candidate_commit": candidate_commit,
        "harness_trees": harness_trees,
        "analyzer_sha256": sha256_file(Path(__file__)),
        "imported_blobs_sha256": blobs,
    }
    if analyzer_commit is not None:
        identity.update({
            "analyzer_commit": analyzer_commit,
            "producer_harness_trees": _source_trees(candidate_commit)[0],
            "consumer_only_changed_paths": changed_paths,
        })
    return identity


def _go_binary_build(path, candidate_commit, expected_package, *, require_trimpath=True):
    try:
        completed = subprocess.run(
            ["go", "version", "-m", str(path)], text=True, capture_output=True,
            timeout=30, check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise EvidenceError(f"Go build metadata unavailable for {expected_package}: {exc}") from exc
    go_version, package, module, settings = "", "", "", {}
    lines = completed.stdout.splitlines()
    if lines and ": " in lines[0]:
        go_version = lines[0].rsplit(": ", 1)[1]
    for line in lines[1:]:
        fields = line.strip().split("\t")
        if len(fields) >= 2 and fields[0] == "path":
            package = fields[1]
        elif len(fields) >= 2 and fields[0] == "mod":
            module = fields[1]
        elif len(fields) == 2 and fields[0] == "build" and "=" in fields[1]:
            key, value = fields[1].split("=", 1)
            if key in settings:
                raise EvidenceError(f"Go build metadata repeats {key}: {expected_package}")
            settings[key] = value
    if (completed.returncode != 0
            or not (go_version == "go1.26" or go_version.startswith("go1.26."))
            or package != expected_package or module != "github.com/snissn/gomap"
            or settings.get("vcs") != "git"
            or settings.get("vcs.revision") != candidate_commit
            or settings.get("vcs.modified") != "false"
            or (require_trimpath and settings.get("-trimpath") != "true")):
        raise EvidenceError(f"Go executable is not a reproducible candidate build: {expected_package}")
    return {
        "go_version": go_version, "package": package, "module": module,
        "build_settings": settings,
    }


def validate_go_inputs(packet, paths):
    return {
        "treedb_service_binary": _go_binary_build(
            paths[packet["inputs"]["treedb_service_binary"]], packet["candidate_commit"],
            "github.com/snissn/gomap/cmd/treedb-document-service",
        ),
        "bounded_validator_binary": _go_binary_build(
            paths[packet["inputs"]["bounded_validator_binary"]], packet["candidate_commit"],
            "github.com/snissn/gomap/TreeDB/cmd/treedb_rag_benchmark",
        ),
    }


def _utc_timestamp(value):
    if not isinstance(value, str) or not value.endswith("Z"):
        raise EvidenceError("command receipt timestamp must be UTC RFC3339")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise EvidenceError("command receipt timestamp must be UTC RFC3339") from exc
    if parsed.tzinfo != timezone.utc:
        raise EvidenceError("command receipt timestamp must be UTC RFC3339")
    return parsed


def _validate_receipt_context(receipt, label):
    environment = receipt.get("environment") if isinstance(receipt, dict) else None
    cwd = receipt.get("cwd") if isinstance(receipt, dict) else None
    umask = receipt.get("umask") if isinstance(receipt, dict) else None
    if (not isinstance(environment, dict) or set(environment) != REQUIRED_RECEIPT_ENV
            or any(not isinstance(key, str) or not key or "\0" in key
                   or not isinstance(value, str) or "\0" in value
                   for key, value in environment.items())
            or any(not environment[key] for key in (
                "HOME", "PATH", "TMPDIR", "TZ", "LANG", "LC_ALL", "GOWORK",
                "GOMAXPROCS", "PYTHONHASHSEED", "OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS",
            ))
            or environment["TZ"] != "UTC"
            or environment["LANG"] != "C.UTF-8" or environment["LC_ALL"] != "C.UTF-8"
            or environment["GOWORK"] != "off"
            or environment["PYTHONHASHSEED"] != "0"
            or environment["PYTHONDONTWRITEBYTECODE"] != "1"
            or environment["OPENBLAS_NUM_THREADS"] != "1"
            or environment["OMP_NUM_THREADS"] != "1"
            or any(not entry or not Path(entry).is_absolute()
                   for entry in environment["PATH"].split(os.pathsep))
            or not environment["GOMAXPROCS"].isdigit()
            or int(environment["GOMAXPROCS"]) <= 0
            or not Path(environment["HOME"]).is_absolute()
            or not Path(environment["TMPDIR"]).is_absolute()
            or not isinstance(cwd, str) or not Path(cwd).is_absolute()
            or not isinstance(umask, str) or len(umask) != 4 or umask[0] != "0"
            or any(value not in "01234567" for value in umask)):
        raise EvidenceError(f"command receipt lacks a complete controlled environment: {label}")
    return environment


def _bounded_validator_argv(packet, paths, role):
    if role not in {"bounded_exact", "bounded_sq8"}:
        raise EvidenceError(f"unknown bounded validator role: {role}")
    binary = paths[packet["inputs"]["bounded_validator_binary"]]
    artifact = paths[packet["arms"][role]]
    argv = [str(binary), "-workload=minima", "-validate-minima-artifact", str(artifact)]
    if role == "bounded_sq8":
        plan_name = packet["inputs"]["bounded_sq8_plan"]
        argv.extend([
            "-minima-quantized-plan", str(paths[plan_name]),
            "-minima-expected-quantized-plan-sha256", packet["files"][plan_name]["sha256"],
        ])
    argv.extend(["-minima-expected-commit", packet["candidate_commit"]])
    return argv


def _bounded_manifest_argv(packet, paths):
    return [
        str(_inventory_path(packet, paths, "inputs", "bounded_validator_binary")),
        "-workload=minima", "-dump-minima-manifest",
        str(_inventory_path(packet, paths, "inputs", "bounded_manifest")),
        "-minima-bounded-total-rows", "50000",
    ]


def _bounded_python_command(packet, paths, role, phase, receipt):
    if role not in {"bounded_exact", "bounded_sq8"} or phase not in {"freeze", "run"}:
        raise EvidenceError("unknown bounded producer command")
    manifest = _inventory_path(packet, paths, "inputs", "bounded_manifest")
    artifact = _inventory_path(packet, paths, "arms", role)
    service = _inventory_path(packet, paths, "inputs", "treedb_service_binary")
    values = {
        "--manifest": manifest,
        "--profile": "command_wal_durable",
    }
    if role == "bounded_exact":
        if phase != "run":
            raise EvidenceError("bounded exact has no plan-freeze command")
        values.update({
            "--output": artifact, "--service-bin": service,
            "--url": "http://127.0.0.1:18040",
            "--data-dir": manifest.parent / "exact-db",
            "--collection": f"minima_q5_bounded_exact_{packet['candidate_commit'][:12]}",
            "--strategy": "native_runtime", "--operation-timeout": 120,
            "--startup-timeout": 120, "--ef-search": 128,
        })
    else:
        plan = _inventory_path(packet, paths, "inputs", "bounded_sq8_plan")
        values.update({
            "--strategy": "column_graph", "--transport": "native",
            "--column-graph-serving": _inventory_path(packet, paths, "inputs", "serving"),
            "--ef-construction": 32, "--query-mode": "quantized_rerank",
            "--quantized-index-name": "minima_sq8", "--ef-search": 64,
            "--quantized-rerank-candidates": 64,
        })
        if phase == "freeze":
            values["--write-quantized-plan"] = plan
        else:
            values.update({
                "--quantized-plan": plan,
                "--expected-quantized-plan-sha256":
                    _inventory_hash(packet, "inputs", "bounded_sq8_plan"),
                "--output": artifact, "--service-bin": service,
                "--url": "http://127.0.0.1:18050", "--native-address": "127.0.0.1:18052",
                "--data-dir": manifest.parent / "sq8-db",
                "--collection": f"minima_q5_bounded_sq8_{packet['candidate_commit'][:12]}",
                "--operation-timeout": 120, "--startup-timeout": 120,
            })
    path_options = {
        "--manifest", "--output", "--service-bin", "--data-dir",
        "--column-graph-serving", "--write-quantized-plan", "--quantized-plan",
    } & set(values)
    _validate_python_argv(
        receipt["argv"], receipt["cwd"], receipt["environment"],
        Path(__file__).with_name("minima_treedb_runner.py"), list(range(6)),
        values, set(), path_options,
    )


def _validate_bounded_preparations(packet, paths, preparations, baseline):
    if not isinstance(preparations, dict) or set(preparations) != {
            "bounded_exact", "bounded_sq8"}:
        raise EvidenceError("command receipts do not cover both bounded producers")
    classification = baseline.get("classification") if isinstance(baseline, dict) else None
    if classification not in {"completed_clean", LEGACY_BASELINE_CLASSIFICATION}:
        raise EvidenceError("bounded baseline classification is unavailable to receipts")
    intervals = {}
    for role, preparation in preparations.items():
        if (not isinstance(preparation, dict)
                or set(preparation) != {"cwd", "environment", "umask", "commands"}):
            raise EvidenceError(f"invalid bounded preparation receipt: {role}")
        environment = _validate_receipt_context(preparation, f"{role} preparation")
        commands = preparation["commands"]
        if not isinstance(commands, list) or len(commands) != 2:
            raise EvidenceError(f"bounded preparation must have exactly two commands: {role}")
        expected_hashes = (
            [_inventory_hash(packet, "inputs", "bounded_manifest"),
             _inventory_hash(packet, "arms", "bounded_exact")]
            if role == "bounded_exact" else
            [_inventory_hash(packet, "inputs", "bounded_sq8_plan"),
             _inventory_hash(packet, "arms", "bounded_sq8")]
        )
        expected_exits = [0, 1 if role == "bounded_exact"
                          and classification == LEGACY_BASELINE_CLASSIFICATION else 0]
        previous_end = None
        for index, command in enumerate(commands):
            expected = {
                "argv", "started_utc", "ended_utc", "exit_code", "output_sha256",
            }
            if (not isinstance(command, dict) or set(command) != expected
                    or not isinstance(command["argv"], list) or not command["argv"]
                    or not all(isinstance(value, str) and value for value in command["argv"])
                    or type(command["exit_code"]) is not int
                    or command["exit_code"] != expected_exits[index]
                    or command["output_sha256"] != expected_hashes[index]
                    or _utc_timestamp(command["started_utc"])
                        > _utc_timestamp(command["ended_utc"])):
                raise EvidenceError(f"invalid bounded producer command receipt: {role}[{index}]")
            started = _utc_timestamp(command["started_utc"])
            ended = _utc_timestamp(command["ended_utc"])
            if previous_end is not None and started < previous_end:
                raise EvidenceError(f"bounded producer commands overlap or reverse: {role}")
            previous_end = ended
            command_context = {
                "cwd": preparation["cwd"], "environment": environment,
                "umask": preparation["umask"], "argv": command["argv"],
            }
            if role == "bounded_exact" and index == 0:
                if command["argv"] != _bounded_manifest_argv(packet, paths):
                    raise EvidenceError("bounded exact manifest command differs from packet")
            else:
                phase = "run" if index == 1 else "freeze"
                _bounded_python_command(packet, paths, role, phase, command_context)
        intervals[role] = (
            _utc_timestamp(commands[0]["started_utc"]),
            _utc_timestamp(commands[-1]["ended_utc"]),
        )
    if intervals["bounded_sq8"][0] < intervals["bounded_exact"][1]:
        raise EvidenceError("bounded SQ8 preparation precedes the exact baseline")
    return intervals


def validate_receipts(packet, paths, dataset_manifest_sha256, dataset_hashes, baseline):
    receipts = read_json(paths[packet["arms"]["command_receipts"]], "command receipts")
    if set(receipts) != {"schema", "candidate_commit", "dataset_manifest_sha256",
                         "dataset_files_sha256", "preparations", "runs"}:
        raise EvidenceError("command receipts have an incomplete schema")
    if (receipts["schema"] != RECEIPT_SCHEMA
            or receipts["candidate_commit"] != packet["candidate_commit"]
            or receipts["dataset_manifest_sha256"] != dataset_manifest_sha256
            or not native.same_json(receipts["dataset_files_sha256"], dataset_hashes)):
        raise EvidenceError("command receipts are not bound to candidate and dataset")
    runs = receipts["runs"]
    if not isinstance(runs, dict) or set(runs) != RUN_ARM_KINDS:
        raise EvidenceError("command receipts do not cover every execution arm exactly once")
    for role, receipt in runs.items():
        output = packet["arms"][role]
        expected_exit = (1 if role == "full_sq8_events"
                         and packet["declared_quality_outcome"] == "miss" else 0)
        expected_fields = {
            "freeze_argv", "freeze_exit_code", "run_argv", "cwd", "environment", "umask",
            "started_utc", "ended_utc", "exit_code", "plan_sha256", "output_sha256",
        }
        if (not isinstance(receipt, dict)
                or set(receipt) != expected_fields
                or not isinstance(receipt["run_argv"], list) or not receipt["run_argv"]
                or not all(isinstance(value, str) and value for value in receipt["run_argv"])
                or type(receipt["exit_code"]) is not int
                or receipt["exit_code"] != expected_exit
                or receipt["output_sha256"] != packet["files"][output]["sha256"]):
            raise EvidenceError(f"invalid command receipt: {role}")
        environment = _validate_receipt_context(receipt, role)
        if _utc_timestamp(receipt["started_utc"]) > _utc_timestamp(receipt["ended_utc"]):
            raise EvidenceError(f"command receipt timestamps are reversed: {role}")
        if role in PLAN_KINDS:
            plan_name = packet["plans"][role]
            plan_sha = packet["files"][plan_name]["sha256"]
            freeze = receipt["freeze_argv"]
            if (not isinstance(freeze, list) or not freeze
                    or not all(isinstance(value, str) and value for value in freeze)
                    or type(receipt["freeze_exit_code"]) is not int
                    or receipt["freeze_exit_code"] != 0
                    or receipt["plan_sha256"] != plan_sha):
                raise EvidenceError(f"command receipt does not bind the frozen plan: {role}")
            plan = read_json(paths[plan_name], f"{role} receipt plan", MAX_PACKET_BYTES)
            if role == "qdrant_fp32_rss":
                script, values, flags, path_options = _qdrant_receipt_contract(
                    packet, paths, plan,
                )
            else:
                script, values, flags, path_options = _native_receipt_contract(
                    packet, paths, role, plan,
                )
            plan_path = paths[plan_name].resolve()
            _validate_python_argv(
                freeze, receipt["cwd"], environment, script, plan.get("cpu_affinity"),
                {**values, "--freeze": plan_path}, flags,
                path_options | {"--freeze"},
            )
            _validate_python_argv(
                receipt["run_argv"], receipt["cwd"], environment, script,
                plan.get("cpu_affinity"),
                {**values, "--run": plan_path, "--expected-plan-sha256": plan_sha},
                flags, path_options | {"--run"},
            )
            runtime = (plan.get("comparison_contract") or {}).get("treedb_go_runtime", {}) \
                if role == "qdrant_fp32_rss" else plan.get("treedb_go_runtime", {})
            if (not native.same_json(runtime, {
                        key: environment[key] for key in ("GOMAXPROCS", "GOGC", "GOMEMLIMIT")
                    })
                    or not native.same_json(plan.get("blas_threads"), {
                        key: environment[key] for key in (
                            "OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS",
                        )
                    })):
                raise EvidenceError(f"command environment differs from its frozen plan: {role}")
        else:
            if (receipt["freeze_argv"] is not None or receipt["freeze_exit_code"] is not None
                    or receipt["plan_sha256"] is not None):
                raise EvidenceError(f"unplanned bounded arm has a fabricated freeze receipt: {role}")
            if receipt["run_argv"] != _bounded_validator_argv(packet, paths, role):
                raise EvidenceError(f"bounded receipt does not bind the exact validator command: {role}")
    preparations = _validate_bounded_preparations(
        packet, paths, receipts["preparations"], baseline,
    )
    exact_run = receipts["runs"]["bounded_exact"]
    sq8_run = receipts["runs"]["bounded_sq8"]
    if (_utc_timestamp(exact_run["started_utc"]) < preparations["bounded_exact"][1]
            or preparations["bounded_sq8"][0] < _utc_timestamp(exact_run["ended_utc"])
            or _utc_timestamp(sq8_run["started_utc"]) < preparations["bounded_sq8"][1]):
        raise EvidenceError("bounded producer and validator receipts are not dependency ordered")


def _first_jsonl(path):
    events = read_jsonl(path)
    try:
        return next(events)
    except StopIteration:
        raise EvidenceError("native event log is empty") from None
    finally:
        events.close()


def validate_serving_configuration(serving):
    try:
        native.existing.validate_column_graph_serving(serving)
    except ValueError as exc:
        raise EvidenceError(f"invalid frozen TreeDB serving configuration: {exc}") from exc


def validate_plans(packet, paths):
    manifest_sha = packet["files"][packet["dataset"]["manifest"]]["sha256"]
    hashes = _dataset_hashes(packet)
    harness_trees, product_trees = _source_trees(packet["candidate_commit"])
    serving = read_json(
        paths[packet["inputs"]["serving"]], "frozen TreeDB serving configuration",
        MAX_PACKET_BYTES,
    )
    validate_serving_configuration(serving)
    native_roles = PLAN_KINDS - {"qdrant_fp32_rss"}
    for role in native_roles:
        plan = read_json(paths[packet["plans"][role]], f"{role} frozen plan", MAX_PACKET_BYTES)
        rows = 512 if role.startswith("smoke_") else 500000
        sq8 = role in {"smoke_sq8", "treedb_sq8_rss", "full_sq8_events"}
        rss_only = role in {"treedb_fp32_rss", "treedb_sq8_rss"}
        if (plan.get("schema") != native.SCHEMA
                or plan.get("harness_commit") != packet["candidate_commit"]
                or plan.get("product_commit") != packet["candidate_commit"]
                or plan.get("harness_source_sha256") != sha256_file(Path(native.__file__))
                or not native.same_json(plan.get("harness_trees"), harness_trees)
                or not native.same_json(plan.get("product_trees"), product_trees)
                or plan.get("dataset_manifest_sha256") != manifest_sha
                or not native.same_json(plan.get("dataset_files_sha256"), hashes)
                or plan.get("rows") != rows or plan.get("rss_only") is not rss_only
                or not _native_diagnostic_shape_valid(plan, rows)
                or plan.get("python") != sys.version or plan.get("numpy") != np.__version__
                or not isinstance(plan.get("platform"), str) or not plan["platform"]
                or plan.get("query_mode", "exact")
                    != ("quantized_rerank" if sq8 else "exact")
                or (sq8 and plan.get("quantized_index_name") != native.QUANTIZED_PROFILE_NAME)
                or (sq8 and (plan.get("vector_m") != 16
                             or not native.same_json(plan.get("representation_arm"),
                                                     native.quantized_representation_arm())
                             or plan.get("quantized_coordinate_policy")
                                != "ordered_ef_grid_with_requested_rerank_candidates_equal_ef"))
                or (not sq8 and "quantized_index_name" in plan)
                or plan.get("service_sha256")
                    != packet["files"][packet["inputs"]["treedb_service_binary"]]["sha256"]
                or plan.get("serving_sha256")
                    != packet["files"][packet["inputs"]["serving"]]["sha256"]
                or not native.same_json(plan.get("serving"), serving)):
            raise EvidenceError(f"{role} frozen plan is not the exact candidate workload")
        output = paths[packet["arms"][role]]
        if role in {"smoke_exact", "smoke_sq8", "full_sq8_events"}:
            first = _first_jsonl(output)
            if first.get("event") != "plan" or not native.same_json(first.get("plan"), plan):
                raise EvidenceError(f"{role} execution did not consume its frozen plan")
        else:
            artifact = read_json(output, f"{role} RSS artifact")
            provenance = artifact.get("provenance") or {}
            if (not native.same_json(artifact.get("comparison_contract"),
                                     native.rss_comparison_contract(plan))
                    or not native.same_json(artifact.get("construction_calibration_contract"),
                                            plan.get("construction_calibration_contract"))
                    or any(not native.same_json(provenance.get(key), plan.get(key)) for key in (
                        "harness_commit", "harness_source_sha256", "harness_trees",
                        "product_commit", "product_trees", "service_sha256",
                        "dataset_manifest_sha256", "dataset_files_sha256", "serving_sha256",
                    ))
                    or (sq8 and not native.same_json(
                        artifact.get("representation_arm"), plan.get("representation_arm")))):
                raise EvidenceError(f"{role} artifact is not bound to its frozen plan")

    role = "qdrant_fp32_rss"
    plan = read_json(paths[packet["plans"][role]], "Qdrant frozen plan", MAX_PACKET_BYTES)
    raw_run_dir, raw_storage = Path(plan.get("run_dir", "")), Path(plan.get("storage_path", ""))
    run_dir, storage = raw_run_dir.resolve(), raw_storage.resolve()
    qdrant_configuration = {
        key: plan.get(key) for key in (
            "production_hnsw", "production_optimizers",
            "initial_upload_hnsw", "initial_upload_optimizers",
        )
    }
    if (plan.get("schema") != "treedb_cohere_qdrant_rss_plan/v5_three_arm_guarded"
            or plan.get("harness_commit") != packet["candidate_commit"]
            or plan.get("harness_source_sha256") != sha256_file(Path(qdrant.__file__))
            or not native.same_json(plan.get("harness_trees"), harness_trees)
            or plan.get("dataset_manifest_sha256") != manifest_sha
            or not native.same_json(plan.get("dataset_files_sha256"), hashes)
            or plan.get("treedb_artifact_sha256")
                != packet["files"][packet["arms"]["treedb_fp32_rss"]]["sha256"]
            or plan.get("treedb_sq8_artifact_sha256")
                != packet["files"][packet["arms"]["treedb_sq8_rss"]]["sha256"]
            or plan.get("qdrant_server_version") != qdrant.existing.SERVER_VERSION
            or plan.get("qdrant_client_version") != qdrant.existing.CLIENT_VERSION
            or not _qdrant_diagnostic_shape_valid(plan)
            or plan.get("python") != sys.version or plan.get("numpy") != np.__version__
            or not isinstance(plan.get("platform"), str) or not plan["platform"]
            or plan.get("qdrant_bin_sha256")
                != packet["files"][packet["inputs"]["qdrant_binary"]]["sha256"]
            or not native.same_json(plan.get("resource_guard"), qdrant.RESOURCE_GUARD)
            or not raw_run_dir.is_absolute() or not raw_storage.is_absolute()
            or storage == run_dir or not storage.is_relative_to(run_dir)):
        raise EvidenceError("Qdrant frozen plan is not the exact guarded three-arm workload")
    artifact = read_json(paths[packet["arms"][role]], "Qdrant RSS artifact")
    provenance = artifact.get("provenance") or {}
    if (not native.same_json(artifact.get("comparison_contract"), plan.get("comparison_contract"))
            or not native.same_json(artifact.get("configuration"), qdrant_configuration)
            or any(not native.same_json(provenance.get(key), plan.get(key)) for key in (
                    "harness_commit", "harness_source_sha256", "harness_trees",
                    "qdrant_bin_sha256", "qdrant_server_version", "qdrant_client_version",
                    "dataset_manifest_sha256", "dataset_files_sha256", "treedb_artifact_sha256",
                    "treedb_sq8_artifact_sha256",
            ))):
        raise EvidenceError("Qdrant artifact is not bound to its frozen plan")


def default_validator_runner(argv):
    completed = subprocess.run(argv, text=True, capture_output=True, timeout=300, check=False)
    if completed.returncode:
        raise EvidenceError(
            "pinned Go validator rejected artifact: " + (completed.stderr or completed.stdout).strip())


def _bounded_manifest_contract(manifest):
    config = manifest.get("config") if isinstance(manifest, dict) else None
    corpora = manifest.get("corpora") if isinstance(manifest, dict) else None
    queries = manifest.get("queries") if isinstance(manifest, dict) else None
    expected_populations = {
        "small": (128, 16), "all_match": (7616, 7616),
        "over_limit_4097": (10000, 4097), "broad_10pct": (10000, 1000),
        "sparse_over_limit": (12000, 4097), "mixed_broad_narrow": (10000, 5),
        "empty_user": (128, 0), "empty_file": (128, 0),
    }
    populations = ({row.get("name"): (row.get("corpus_rows"), row.get("eligible_rows"))
                    for row in corpora} if isinstance(corpora, list)
                   and all(isinstance(row, dict) for row in corpora) else {})
    query_names = ([row.get("scenario") for row in queries]
                   if isinstance(queries, list)
                   and all(isinstance(row, dict) for row in queries) else [])
    if (manifest.get("schema") != BOUNDED_MANIFEST_SCHEMA
            or manifest.get("fixture") != "bounded-50k"
            or not isinstance(config, dict)
            or tuple(config.get(key) for key in (
                "dimension", "top_k", "batch_size", "lookup_limit",
                "warmup_queries", "timed_queries", "reader_concurrency", "writer_concurrency",
            )) != (8, 5, 256, 4096, 32, 1024, 4, 1)
            or not isinstance(corpora, list) or len(corpora) != 8
            or not isinstance(queries, list) or len(queries) != 8
            or populations != expected_populations
            or query_names != list(expected_populations)
            or any(not valid_sha256(manifest.get(key)) for key in (
                "corpus_sha256", "query_sha256", "operation_sha256", "expected_state_sha256",
            ))):
        raise EvidenceError("bounded manifest is not the frozen 50K regression workload")


def _bounded_plan_contract(packet, paths, manifest):
    plan_name = packet["inputs"]["bounded_sq8_plan"]
    plan = read_json(paths[plan_name], "bounded SQ8 plan", MAX_PACKET_BYTES)
    expected_manifest = {
        "schema": manifest["schema"], "fixture": manifest["fixture"],
        "config": manifest["config"], "corpus_sha256": manifest["corpus_sha256"],
        "query_sha256": manifest["query_sha256"],
        "operation_sha256": manifest["operation_sha256"],
        "expected_state_sha256": manifest["expected_state_sha256"],
    }
    expected_profile = {
        "schema": "treedb_minima_quantized_profile/v1", "name": "minima_sq8",
        "query_mode": "quantized_rerank", "index_name": "minima_sq8",
        "codec": "scalar_u8", "version": 1, "calibration": "legacy",
        "quantized_config_hash": 0, "requested_ef_search": 64,
        "requested_rerank_candidates": 64, "native_command_version": 3,
    }
    serving = read_json(
        _inventory_path(packet, paths, "inputs", "serving"),
        "bounded serving configuration", MAX_PACKET_BYTES,
    )
    if (set(plan) != {
            "schema", "manifest", "quantized_profile", "vector_strategy", "transport",
            "durability_profile", "vector_m", "ef_construction", "serving",
            }
            or plan.get("schema") != BOUNDED_SQ8_PLAN_SCHEMA
            or not native.same_json(plan.get("manifest"), expected_manifest)
            or not native.same_json(plan.get("quantized_profile"), expected_profile)
            or (plan.get("vector_strategy"), plan.get("transport"),
                plan.get("durability_profile"), plan.get("vector_m"),
                plan.get("ef_construction"))
                != ("column_graph", "native", "command_wal_durable", 16, 32)
            or not native.same_json(plan.get("serving"), serving)):
        raise EvidenceError("bounded SQ8 plan is not bound to the frozen manifest and profile")
    return plan


def _bounded_backend(packet, paths, artifact, role):
    backends = artifact.get("backends")
    if (not isinstance(backends, list) or len(backends) != 1
            or not isinstance(backends[0], dict) or backends[0].get("name") != "treedb"):
        raise EvidenceError(f"bounded {role} artifact requires exactly one TreeDB backend")
    backend = backends[0]
    config = backend.get("configuration") or {}
    manifest = artifact.get("manifest") or {}
    service = _inventory_path(packet, paths, "inputs", "treedb_service_binary")
    strategy, transport = (("native_runtime", "http") if role == "exact"
                           else ("column_graph", "native"))
    if (not isinstance(config, dict)
            or config.get("harness_commit") != packet["candidate_commit"]
            or config.get("product_commit") != packet["candidate_commit"]
            or config.get("runner_sha256")
                != sha256_file(Path(__file__).with_name("minima_treedb_runner.py"))
            or config.get("service_binary_sha256")
                != _inventory_hash(packet, "inputs", "treedb_service_binary")
            or config.get("service_binary_vcs_revision") != packet["candidate_commit"]
            or config.get("service_binary_vcs_modified") != "false"
            or not isinstance(config.get("service_binary"), str)
            or Path(config["service_binary"]).resolve() != service
            or config.get("vector_strategy") != strategy
            or config.get("transport") != transport
            or config.get("profile") != "command_wal_durable"
            or config.get("dimension") != "8"
            or (backend.get("manifest") or {}) != {
                "corpus_sha256": manifest.get("corpus_sha256"),
                "operation_sha256": manifest.get("operation_sha256"),
                "query_sha256": manifest.get("query_sha256"),
            }):
        raise EvidenceError(f"bounded {role} artifact provenance differs from the candidate")
    if role == "exact":
        if (config.get("url") != "http://127.0.0.1:18040"
                or config.get("collection")
                    != f"minima_q5_bounded_exact_{packet['candidate_commit'][:12]}"
                or config.get("ef_search") != "128"):
            raise EvidenceError("bounded exact artifact has an unexpected launch configuration")
    elif (config.get("url") != "http://127.0.0.1:18050"
          or config.get("collection")
              != f"minima_q5_bounded_sq8_{packet['candidate_commit'][:12]}"
          or config.get("ef_search") != "64"
          or config.get("ef_construction_requested") != "32"
          or config.get("query_mode") != "quantized_rerank"
          or config.get("quantized_index_name") != "minima_sq8"):
        raise EvidenceError("bounded SQ8 artifact has an unexpected launch configuration")
    return backend


def _bounded_completed_clean(artifact, backend):
    raw = artifact.get("backend_raw_evidence")
    return (
        (backend.get("operations") or {}).get("manifest_ordered") is True
        and artifact.get("failures") == []
        and isinstance(raw, dict) and set(raw) == {"treedb"}
        and (raw["treedb"].get("final_scroll_state") or {}).get("match") is True
    )


def _legacy_baseline_routes():
    definitions = {
        "small": ("complete_exact", "bounded_complete_set", 16, 16, 16, 16, 16, 5, 16),
        "all_match": ("vector_aligned_ann", "vector_aligned_scalar",
                      4096, 32, 32, None, None, 5, 32),
        "over_limit_4097": ("vector_aligned_ann", "vector_aligned_scalar",
                            4096, 32, 32, None, None, 5, 32),
        "broad_10pct": ("complete_finite_ann", "bounded_complete_set",
                        1000, 1000, 1000, None, None, 0, 1000),
        "sparse_over_limit": ("vector_aligned_ann", "vector_aligned_scalar",
                              4096, 32, 32, None, None, 5, 32),
        "mixed_broad_narrow": ("mixed_refined", "bounded_candidate_refinement",
                               4101, 5, 5, 5, 5, 5, 5),
        "empty_user": ("complete_exact", "bounded_complete_set", 0, 0, 0, 0, 0, 0, 0),
        "empty_file": ("complete_exact", "bounded_complete_set", 16, 0, 0, 0, 0, 0, 0),
    }
    routes = {}
    for name, (plan, membership, probe, candidates, retained, visited, scored,
               admitted, allowed) in definitions.items():
        routes[name] = ({
            "identity": "native_base_plus_live_delta", "declared_scalar_filtering": True,
            "native_base_plus_live_delta": True, "full_document_scan_fallbacks": 0,
            "scalar_filter_unbounded": 0, "probe_ids": probe, "candidate_ids": candidates,
            "retained_candidate_ids": retained, "refined_candidate_ids": retained,
            "membership_source": membership, "plan": plan,
            "allowed_id_materialization_rows": allowed, "primary_document_scans": 0,
            "visited_candidates": visited, "scored_candidates": scored,
            "admitted_candidates": admitted,
        }, {
            "membership_source": membership, "plan": plan, "probe_ids": probe,
            "candidates": scored, "candidate_ids": candidates, "retained": retained,
            "refined": retained, "visited": visited, "scored": scored,
            "admitted": admitted, "visibility_mismatches": 0, "visibility_retries": 0,
        })
    return routes


def _legacy_partial_phase_prefix_valid(value):
    if not isinstance(value, dict) or set(value) != {
        "clock", "total_start_nanos", "total_end_nanos", "total_duration_nanos",
        "unattributed_nanos", "unattributed_rule", "phases",
    }:
        return False
    total_start = value.get("total_start_nanos")
    total_end = value.get("total_end_nanos")
    total_duration = value.get("total_duration_nanos")
    unattributed = value.get("unattributed_nanos")
    phases = value.get("phases")
    if (value.get("clock") != "time.monotonic_ns"
            or value.get("unattributed_rule") != (
                "total_duration_nanos = sum(phase.duration_nanos) + unattributed_nanos; "
                "unattributed_nanos <= max(60000000000, total_duration_nanos / 100); "
                "unattributed covers only runner bookkeeping between declared boundaries"
            )
            or any(type(number) is not int for number in (
                total_start, total_end, total_duration, unattributed,
            ))
            or total_start <= 0 or total_end < total_start
            or total_duration != total_end - total_start or unattributed < 0
            or unattributed > max(60_000_000_000, total_duration // 100)
            or not isinstance(phases, list) or len(phases) != len(LEGACY_BASELINE_PHASES)):
        return False
    durations = 0
    previous_end = total_start
    for phase, (name, classification) in zip(phases, LEGACY_BASELINE_PHASES):
        if not isinstance(phase, dict) or set(phase) != {
            "classification", "duration_nanos", "end_nanos", "name", "resource_segments",
            "sample_count", "sample_duration_nanos", "start_nanos",
        }:
            return False
        start = phase.get("start_nanos")
        end = phase.get("end_nanos")
        duration = phase.get("duration_nanos")
        sample_count = phase.get("sample_count")
        sample_duration = phase.get("sample_duration_nanos")
        if ((phase.get("name"), phase.get("classification")) != (name, classification)
                or any(type(number) is not int for number in (
                    start, end, duration, sample_count, sample_duration,
                ))
                or start < previous_end or end <= start or duration != end - start
                or sample_count < 0 or sample_duration < 0
                or not isinstance(phase.get("resource_segments"), list)
                or not phase["resource_segments"]
                or not all(
                    isinstance(segment, dict) and set(segment) == {"start", "end"}
                    and isinstance(segment["start"], dict)
                    and isinstance(segment["end"], dict)
                    for segment in phase["resource_segments"]
                )):
            return False
        durations += duration
        previous_end = end
    return durations + unattributed == total_duration and previous_end <= total_end


def _legacy_partial_raw_valid(raw):
    return (
        isinstance(raw, dict) and set(raw) == LEGACY_BASELINE_RAW_KEYS
        and native.same_json(raw.get("final_scroll_state"), {})
        and native.same_json(raw.get("restart_boundary"), {})
        and native.same_json(raw.get("timed_overlap"), {})
        and raw.get("diagnostic_resume") is None
        and native.same_json(raw.get("upsert_batch_correlations"), [])
        and native.same_json(raw.get("diagnostics"), LEGACY_BASELINE_DIAGNOSTICS)
        and native.same_json(
            raw.get("upsert_batch_correlation_contract"),
            LEGACY_BASELINE_CORRELATION_CONTRACT,
        )
        and _legacy_partial_phase_prefix_valid(raw.get("phase_attribution"))
    )


def _legacy_route_valid(name, route, raw_route, contracts):
    if not isinstance(route, dict) or not isinstance(raw_route, dict):
        return False
    expected_route, expected_raw = contracts
    if set(route) != set(expected_route) or set(raw_route) != set(expected_raw):
        return False
    if name not in LEGACY_BASELINE_ANN_ROUTES:
        return native.same_json(route, expected_route) and native.same_json(raw_route, expected_raw)

    route_dynamic = {"visited_candidates", "scored_candidates"}
    raw_dynamic = {"candidates", "visited", "scored"}
    if (not native.same_json(
            {key: value for key, value in route.items() if key not in route_dynamic},
            {key: value for key, value in expected_route.items() if key not in route_dynamic},
        )
            or not native.same_json(
                {key: value for key, value in raw_route.items() if key not in raw_dynamic},
                {key: value for key, value in expected_raw.items() if key not in raw_dynamic},
            )):
        return False
    scored = route.get("scored_candidates")
    visited = route.get("visited_candidates")
    admitted = route.get("admitted_candidates")
    minimum_scored = max(1, admitted)
    if LEGACY_BASELINE_ANN_ROUTES[name] == "vector_aligned_ann":
        minimum_scored = max(minimum_scored, route["retained_candidate_ids"])
    if (type(scored) is not int or type(visited) is not int
            or type(raw_route.get("candidates")) is not int
            or type(raw_route.get("scored")) is not int
            or type(raw_route.get("visited")) is not int
            or scored != raw_route["scored"] or scored != raw_route["candidates"]
            or visited != raw_route["visited"]
            or not minimum_scored <= scored <= LEGACY_BASELINE_ANN_MAX_SCORED):
        return False
    if LEGACY_BASELINE_ANN_ROUTES[name] == "complete_finite_ann":
        return visited == scored
    return scored <= visited <= scored + LEGACY_BASELINE_ANN_MAX_SEED_ROWS


def _known_legacy_baseline_failure(artifact, backend):
    if not isinstance(artifact, dict) or not isinstance(backend, dict):
        return False
    operations = backend.get("operations") or {}
    expected_operations = {
        "batch_insert_during_search": False, "empty_cases_checked": False,
        "explicit_delete_visible": False, "explicit_update_visible": False,
        "manifest_ordered": False, "reindex_delete_replace": False,
        "reindex_execution_sha256": "",
        "reindex_execution_trace": {"operations": []},
        "reindex_operations_executed": 0, "timed_execution_sha256": "",
        "timed_execution_trace": {"queries": [], "rounds": []},
        "timed_queries_executed": 0, "timed_rounds_completed": 0,
    }
    expected_proof = {
        "schema": "treedb_minima_native_path_proof/v1",
        "strategy": "native_runtime", "availability": "unavailable",
        "counters": None,
        "reason": ("native baseline diagnostic; typed column_graph lifecycle counters "
                   "require M1-M4; bounded sparse scenario does not preserve full <1% "
                   "selectivity"),
    }
    proof = artifact.get("native_path_proof")
    raw_all = artifact.get("backend_raw_evidence")
    raw = raw_all.get("treedb") if isinstance(raw_all, dict) else None
    reopen = backend.get("reopen")
    if (artifact.get("schema") != BOUNDED_ARTIFACT_SCHEMA
            or artifact.get("state") != "partial"
            or artifact.get("passing") is not False
            or artifact.get("readiness_recommendation") != "not_evaluated"
            or not native.same_json(artifact.get("failures"), LEGACY_BASELINE_FAILURES)
            or not native.same_json(proof, expected_proof)
            or not native.same_json(operations, expected_operations)
            or not native.same_json(reopen, {
                "attempted": False, "committed_parity": False, "result_manifest_hash": "",
            })
            or not isinstance(raw_all, dict) or set(raw_all) != {"treedb"}
            or not _legacy_partial_raw_valid(raw)):
        return False

    manifest = artifact.get("manifest")
    if not isinstance(manifest, dict):
        return False
    queries = manifest.get("queries")
    corpora = manifest.get("corpora")
    scenarios = artifact.get("scenarios")
    events = raw.get("events")
    routes = raw.get("native_route_responses")
    if (not isinstance(queries, list) or not isinstance(corpora, list)
            or not isinstance(scenarios, list)
            or not isinstance(events, list) or not isinstance(routes, dict)
            or len(queries) != 8 or len(corpora) != 8
            or len(scenarios) != 8 or len(events) != 8
            or not all(isinstance(row, dict) for rows in (
                queries, corpora, scenarios, events,
            ) for row in rows)):
        return False
    names = [query.get("scenario") for query in queries]
    population = {row.get("name"): row for row in corpora if isinstance(row, dict)}
    route_contracts = _legacy_baseline_routes()
    if ([row.get("scenario") for row in scenarios] != names
            or [event.get("scenario") for event in events] != names
            or set(routes) != set(names) or set(population) != set(names)
            or set(route_contracts) != set(names)):
        return False
    manifest_config = manifest.get("config")
    if not isinstance(manifest_config, dict):
        return False
    order_tolerance = manifest_config.get("order_tolerance")
    score_tolerance = manifest_config.get("score_tolerance")
    if (type(order_tolerance) is not int or order_tolerance != 0
            or type(score_tolerance) not in (int, float)
            or not math.isfinite(score_tolerance) or score_tolerance != 0.000001):
        return False
    for query, row, event in zip(queries, scenarios, events):
        name = query.get("scenario")
        expected = query.get("initial_oracle_ids")
        expected_scores = query.get("initial_oracle_scores")
        final_expected = query.get("final_oracle_ids")
        final_expected_scores = query.get("final_oracle_scores")
        actual = row.get("initial_actual_ids")
        actual_scores = row.get("initial_actual_scores")
        mismatch = name == "broad_10pct"
        scores_valid = (
            isinstance(expected_scores, list) and isinstance(final_expected_scores, list)
            and isinstance(actual_scores, list)
            and all(type(value) in (int, float) and math.isfinite(value) for value in (
                *expected_scores, *final_expected_scores, *actual_scores,
            ))
        )
        deltas = ([] if mismatch and actual_scores == [] else
                  [abs(left - right) for left, right in zip(expected_scores, actual_scores)]
                  if scores_valid and len(expected_scores) == len(actual_scores) else None)
        maximum_delta = max(deltas, default=0.0) if deltas is not None else None
        observed_delta = event.get("maximum_score_delta")
        corpus = population[name]
        final_summary = 1.0 if final_expected == [] else 0.0
        if (not isinstance(expected, list) or not isinstance(final_expected, list)
                or not scores_valid or len(expected) != len(expected_scores)
                or len(final_expected) != len(final_expected_scores)
                or row.get("backend") != "treedb"
                or not native.same_json(row.get("initial_oracle_ids"), expected)
                or not native.same_json(row.get("initial_oracle_scores"), expected_scores)
                or not native.same_json(row.get("final_oracle_ids"), final_expected)
                or not native.same_json(row.get("final_oracle_scores"), final_expected_scores)
                or type(row.get("order_tolerance")) is not int
                or row.get("order_tolerance") != order_tolerance
                or type(row.get("score_tolerance")) not in (int, float)
                or row.get("score_tolerance") != score_tolerance
                or not native.same_json(row.get("corpus_rows"), corpus.get("corpus_rows"))
                or not native.same_json(row.get("expected_matches"), corpus.get("eligible_rows"))
                or not native.same_json(row.get("selectivity"), corpus.get("selectivity"))
                or not native.same_json(row.get("errors"), 0)
                or not native.same_json(row.get("timeouts"), 0)
                or not native.same_json(row.get("actual_ids"), [])
                or not native.same_json(row.get("actual_scores"), [])
                or type(row.get("recall")) not in (int, float)
                or type(row.get("overlap")) not in (int, float)
                or not native.same_json(row.get("recall"), final_summary)
                or not native.same_json(row.get("overlap"), final_summary)
                or not native.same_json(row.get("reopen_ids"), [])
                or row.get("reopen_parity") is not True
                or not native.same_json(row.get("correctness"), {
                    "cross_user_results": 0, "stale_delete_ids": 0,
                    "stale_insert_ids": 0, "stale_update_ids": 0,
                })
                or not _legacy_route_valid(
                    name, row.get("route"), routes.get(name), route_contracts[name],
                )
                or not native.same_json(row.get("visibility"), {
                    "generation_consistent": True, "visibility_mismatch_count": 0,
                    "visibility_retry_count": 0,
                })
                or event.get("kind") != "oracle_comparison"
                or event.get("operation") != "initial_oracle_comparison"
                or not native.same_json(event.get("expected_ids"), expected)
                or not native.same_json(event.get("actual_ids"), actual)
                or event.get("match") is not (not mismatch)
                or maximum_delta is None or not math.isfinite(maximum_delta)
                or maximum_delta > score_tolerance
                or type(observed_delta) not in (int, float) or not math.isfinite(observed_delta)
                or not math.isclose(observed_delta, maximum_delta,
                                    rel_tol=0, abs_tol=1e-12)
                or (mismatch and (expected != LEGACY_BASELINE_IDS or actual != []
                                  or actual_scores != []))
                or (not mismatch and (
                    actual != expected or len(actual_scores) != len(expected)
                    or route_contracts[name][0]["admitted_candidates"] != len(expected)
                ))):
            return False
    return True


def validate_bounded_artifacts(packet, paths, runner=default_validator_runner):
    exact = paths[packet["arms"]["bounded_exact"]]
    sq8 = paths[packet["arms"]["bounded_sq8"]]
    exact_json = read_json(exact, "bounded exact artifact")
    sq8_json = read_json(sq8, "bounded SQ8 artifact")
    manifest = read_json(
        _inventory_path(packet, paths, "inputs", "bounded_manifest"),
        "bounded manifest", MAX_JSON_BYTES,
    )
    _bounded_manifest_contract(manifest)
    if (not native.same_json(exact_json.get("manifest"), manifest)
            or not native.same_json(sq8_json.get("manifest"), manifest)):
        raise EvidenceError("bounded artifacts do not consume the inventoried manifest")
    plan = _bounded_plan_contract(packet, paths, manifest)
    proof = exact_json.get("native_path_proof") or {}
    if proof.get("strategy") != "native_runtime":
        raise EvidenceError("bounded exact artifact must use native_runtime")
    if sq8_json.get("schema") != BOUNDED_SQ8_ARTIFACT_SCHEMA:
        raise EvidenceError("bounded SQ8 artifact has the wrong schema")
    runner(_bounded_validator_argv(packet, paths, "bounded_exact"))
    runner(_bounded_validator_argv(packet, paths, "bounded_sq8"))
    exact_backend = _bounded_backend(packet, paths, exact_json, "exact")
    sq8_backend = _bounded_backend(packet, paths, sq8_json, "SQ8")
    if (sq8_json.get("quantized_plan_sha256")
            != _inventory_hash(packet, "inputs", "bounded_sq8_plan")
            or not native.same_json(sq8_json.get("quantized_profile"),
                                    plan["quantized_profile"])):
        raise EvidenceError("bounded SQ8 artifact does not bind the reviewed plan")
    if not _bounded_completed_clean(sq8_json, sq8_backend):
        raise EvidenceError("bounded SQ8 artifact is not a completed clean diagnostic")
    if _bounded_completed_clean(exact_json, exact_backend):
        classification = "completed_clean"
    elif _known_legacy_baseline_failure(exact_json, exact_backend):
        classification = LEGACY_BASELINE_CLASSIFICATION
    else:
        raise EvidenceError("bounded exact artifact is neither clean nor the frozen #4617 failure")
    clean = classification == "completed_clean"
    return {
        "classification": classification,
        "completed_clean": clean,
        "lifecycle_claim_available": clean,
        "latency_claim_available": clean,
        "representation_matched_comparison": False,
        "known_limitation": (None if clean else
            "#4617 bounded-50K broad_10pct complete_finite_ann recall failure"),
    }


def _dataset_hashes(packet):
    return {kind: packet["files"][packet["dataset"][kind]]["sha256"]
            for kind in ("documents", "queries", "truth")}


def _native_diagnostic_shape_valid(plan, rows):
    return (
        plan.get("eligible_counts") == native.counts(rows)
        and plan.get("efs") == [128, 256, 512, 1024, 2048]
        and native.same_json(plan.get("rss_controls"), native.RSS_CONTROLS)
        and native.same_json(plan.get("rss_calibration_queries"),
                             native.RSS_CALIBRATION_QUERIES)
        and native.same_json(plan.get("rss_revalidation_queries"),
                             native.RSS_REVALIDATION_QUERIES)
        and _close(plan.get("rss_recall_target"), native.RSS_RECALL_TARGET)
        and type(plan.get("ef_construction")) is int
        and plan["ef_construction"] == native.DEFAULT_EF_CONSTRUCTION
        and native.same_json(
            plan.get("construction_calibration_contract"),
            native.construction_calibration_contract(native.DEFAULT_EF_CONSTRUCTION),
        )
        and type(plan.get("construction_decisions")) is bool
        and type(plan.get("overlap_ef")) is int and plan["overlap_ef"] == 512
        and type(plan.get("overlap_eligible")) is int
        and plan["overlap_eligible"] == native.counts(rows)[1]
        and type(plan.get("reader_concurrency")) is int and plan["reader_concurrency"] == 4
        and type(plan.get("writer_calls")) is int and plan["writer_calls"] == 8
    )


def _qdrant_diagnostic_shape_valid(plan):
    configuration = {
        "production_hnsw": qdrant.existing.PRODUCTION_HNSW_CONFIG,
        "production_optimizers": qdrant.existing.PRODUCTION_OPTIMIZERS_CONFIG,
        "initial_upload_hnsw": qdrant.existing.INITIAL_UPLOAD_HNSW_CONFIG,
        "initial_upload_optimizers": qdrant.existing.INITIAL_UPLOAD_OPTIMIZERS_CONFIG,
    }
    return (
        (plan.get("rows"), plan.get("queries"), plan.get("dimensions"),
         plan.get("top_k"), plan.get("batch_size")) == (500000, 200, 768, 10, 256)
        and _qdrant_plan_matches_comparison_contract(plan)
        and native.same_json(plan.get("controls"), native.RSS_CONTROLS)
        and all(native.same_json(plan.get(key), value)
                for key, value in configuration.items())
    )


def _validate_log_envelope(events, rows, mode, *, defer_terminal=False):
    if not events or events[0].get("event") != "plan" or events[-1].get("event") != "terminal":
        raise EvidenceError("native log must begin with plan and end with terminal")
    plans = [event.get("plan") for event in events if event.get("event") == "plan"]
    terminals = [event for event in events if event.get("event") == "terminal"]
    full = [event for event in events if event.get("event") == "full_state_verified"]
    failures = [event for event in events if event.get("event") in {
        "failure", "guard_failed", "search_request_failure", "quantized_failure_evidence",
        "finalization_failure",
    } or event.get("outcome") == "error"]
    if len(plans) != 1 or len(terminals) != 1:
        raise EvidenceError("native log is incomplete or retains a harness failure")
    plan, terminal = plans[0], terminals[0]
    if (plan.get("schema") != native.SCHEMA
            or plan.get("qualification") != "not_evaluated"
            or plan.get("mode") != ("smoke" if rows == 512 else "diagnostic")
            or plan.get("rows") != rows or plan.get("dimensions") != 768
            or plan.get("queries") != (4 if rows == 512 else 200)
            or plan.get("top_k") != 10 or plan.get("batch_size") != 256
            or plan.get("query_mode", "exact") != mode or plan.get("rss_only") is not False
            or not _native_diagnostic_shape_valid(plan, rows)
            or plan.get("wall_limit_s") != 2700
            or plan.get("minimum_free_bytes") != 10 * native.GIB
            or plan.get("maximum_output_bytes") != 11 * native.GIB
            or plan.get("maximum_combined_rss_bytes") != 24 * native.GIB):
        raise EvidenceError("native log plan is invalid")
    _validate_resource_events(events, plan)
    if defer_terminal:
        return plan, full, terminal, failures
    if (len(full) != 1 or failures
            or terminal.get("lifecycle_complete") is not True or terminal.get("error") is not None
            or type(terminal.get("final_disk_bytes")) is not int
            or terminal["final_disk_bytes"] <= 0
            or full[0].get("rows") != rows or full[0].get("vectors_checked") != rows
            or not _close(full[0].get("normalized_full_vector_tolerance"), 1e-6)):
        raise EvidenceError("native log plan, terminal, or full-state attestation is invalid")
    lifetimes = terminal.get("process_lifetimes")
    try:
        native.validate_shutdowns(lifetimes, 2)
    except (RuntimeError, TypeError) as exc:
        raise EvidenceError(f"native service lifetime did not shut down cleanly: {exc}") from exc
    return plan, full[0], terminal


def _validate_resource_events(events, plan):
    resources = [event for event in events if event.get("event") == "resource"]
    if not resources:
        raise EvidenceError("native log has no retained resource-guard sample")
    for event in resources:
        if (type(event.get("elapsed_s")) not in (int, float)
                or not math.isfinite(event["elapsed_s"]) or event["elapsed_s"] < 0
                or type(event.get("free_bytes")) is not int
                or event["free_bytes"] < plan["minimum_free_bytes"]
                or type(event.get("output_bytes")) is not int
                or not 0 <= event["output_bytes"] <= plan["maximum_output_bytes"]
                or type(event.get("combined_rss_bytes")) is not int
                or not 0 < event["combined_rss_bytes"] <= plan["maximum_combined_rss_bytes"]
                or event["elapsed_s"] > plan["wall_limit_s"]):
            raise EvidenceError("native resource sample exceeds the frozen guard contract")


def _validate_quantized_search_path(event, rows):
    """Recheck the retained dense/score-plane proof available on search events."""
    try:
        eligible, control, ids = event["eligible"], event["ef"], event["ids"]
        work = native.dense_contract.DenseSearchWork.from_dict(event["dense_work"])
        proof = native.dense_contract.DenseScorePlaneProof.from_dict(event["score_plane"])
        filtered = eligible != rows
        live = work.graph.filter.eligible_rows if filtered else rows
        expected_route = (
            "typed_empty" if filtered and live == 0 else
            "typed_exact" if filtered and (live <= 4096 or proof.normalized_candidate_width == 0)
            else "quantized_rerank"
        )
        generation = proof.snapshot.schema_generation
        return (
            type(eligible) is int and 0 <= eligible <= rows
            and type(control) is int and control in native.RSS_CONTROLS
            and event.get("rerank_candidates") == control
            and isinstance(ids, list) and len(ids) == min(10, eligible)
            and proof.version == 1 and proof.available and proof.completed and not proof.reason
            and proof.requested_mode == "quantized_rerank"
            and proof.effective_mode == "quantized_rerank" and proof.route == expected_route
            and proof.quantized_index_name == native.QUANTIZED_PROFILE_NAME
            and proof.quantized_codec == "scalar_u8" and proof.quantized_version == 1
            and proof.quantized_config_hash == 0 and proof.requested_top_k == 10
            and proof.requested_ef_search == control
            and proof.requested_rerank_candidates == control
            and native.existing.quantized_snapshot_valid(proof.snapshot, generation)
            and work.graph.snapshot == proof.snapshot
            and work.graph.filter.attempted is filtered
            and (not filtered or live == eligible)
            and work.output.attempted and work.output.completed and work.output.missing == 0
            and work.output.requested == len(ids)
            and native.dense_contract.dense_score_plane_byte_counters_match(proof, 768)
            and native.dense_contract.dense_quantized_response_work_matches(
                work, proof, 10, len(ids), filtered,
            )
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        return False


def _metric(ids, expected):
    if (not isinstance(ids, list) or len(ids) != len(set(ids))
            or not all(isinstance(value, str) for value in ids)):
        raise EvidenceError("search result IDs are invalid")
    recall = len(set(ids) & set(expected)) / len(expected) if expected else (1.0 if not ids else 0.0)
    ndcg = native.binary_ndcg(ids, expected) if expected else (1.0 if not ids else 0.0)
    return recall, ndcg


def _close(left, right):
    return (type(left) in (int, float) and type(right) in (int, float)
            and math.isfinite(left) and math.isfinite(right)
            and math.isclose(left, right, rel_tol=0.0, abs_tol=1e-12))


def _check_search_metrics(event, truth, total_rows):
    try:
        eligible = event["eligible"]
        expected = [] if eligible == 0 else truth[str(eligible)][event["query"]]
        ids = event["ids"]
        if len(ids) != min(10, eligible):
            raise ValueError("result cardinality")
        for identifier in ids:
            ordinal = int(identifier.removeprefix("row-"))
            if (identifier != f"row-{ordinal:06d}" or not 0 <= ordinal < total_rows
                    or (eligible != total_rows
                        and (ordinal * 7919) % total_rows >= eligible)):
                raise ValueError("logical ID or filter membership")
        recall, ndcg = _metric(ids, expected)
    except (AttributeError, KeyError, IndexError, TypeError, ValueError) as exc:
        raise EvidenceError("search result cannot be bound to frozen truth") from exc
    if not _close(event.get("recall"), recall) or not _close(event.get("ndcg_at_10"), ndcg):
        raise EvidenceError("producer recall/NDCG differs from retained IDs and truth")
    return recall, ndcg


def _quality_call_for_search(events, search_index, search):
    index = search_index - 1
    while index >= 0 and events[index].get("event") == "resource":
        index -= 1
    call = events[index] if index >= 0 else {}
    start, end, duration = call.get("start_ns"), call.get("end_ns"), call.get("duration_ns")
    if (call.get("event") != "call" or call.get("outcome") != "completed"
            or call.get("phase") != search.get("phase")
            or call.get("eligible") != search.get("eligible")
            or call.get("ef") != search.get("ef") or call.get("query") != search.get("query")
            or call.get("writer_active") is not False
            or call.get("request_mode") != "quantized_rerank"
            or type(start) is not int or type(end) is not int or type(duration) is not int
            or start < 0 or end <= start or duration != end - start):
        raise EvidenceError("quality search lacks its matching completed public-call event")
    return index


def _search_call_key(row, default_mode=None):
    mode = row.get("request_mode")
    if mode is None:
        mode = default_mode or (
            "exact" if str(row.get("phase", "")).endswith("_exact") else "quantized_rerank"
        )
    return (
        row.get("phase"), row.get("eligible"), row.get("ef"), row.get("query"),
        row.get("writer_active"), mode,
    )


def _validate_search_call_ledger(events, searches, phases, default_mode=None):
    calls = [(index, event) for index, event in enumerate(events)
             if event.get("event") == "call" and event.get("phase") in phases]
    for _, call in calls:
        start, end, duration = call.get("start_ns"), call.get("end_ns"), call.get("duration_ns")
        if (call.get("outcome") != "completed"
                or call.get("request_mode") not in {"exact", "quantized_rerank"}
                or type(call.get("writer_active")) is not bool
                or type(start) is not int or type(end) is not int or type(duration) is not int
                or start < 0 or end <= start or duration != end - start):
            raise EvidenceError("post-selection search has an invalid public-call receipt")
    if Counter(_search_call_key(call, default_mode) for _, call in calls) != Counter(
            _search_call_key(search, default_mode) for _, search in searches):
        raise EvidenceError("post-selection searches and public-call receipts differ")
    for search_index, search in searches:
        if search.get("phase") == "overlap_search":
            continue
        call_index = search_index - 1
        while call_index >= 0 and events[call_index].get("event") == "resource":
            call_index -= 1
        if (call_index < 0 or events[call_index].get("event") != "call"
                or _search_call_key(events[call_index], default_mode)
                    != _search_call_key(search, default_mode)):
            raise EvidenceError("ordered search lacks its immediately preceding public-call receipt")
    return {index for index, _ in calls}


def _paired_records_in_order(artifact):
    records = []
    for batch in [artifact["warmup"], *artifact["repetitions"]]:
        for mode in batch["arm_order"]:
            records.extend((mode, record) for record in batch["arms"][mode]["requests"])
    return records


def _post_selection_phases():
    return {
        native.paired_phase("warmup", -1, mode) for mode in ("exact", "quantized_rerank")
    } | {
        native.paired_phase("measured", repetition, mode)
        for repetition in range(native.PAIRED_TIMING_REPETITIONS)
        for mode in ("exact", "quantized_rerank")
    } | {"fixed_coordinate_curve", "overlap_search", "empty_user", "post_reopen_curve"}


def _completed_phase_calls(events, phase, expected_count):
    rows = [(index, event) for index, event in enumerate(events)
            if event.get("event") == "call" and event.get("phase") == phase]
    if len(rows) != expected_count:
        raise EvidenceError(f"lifecycle phase {phase} has the wrong call count")
    for _, call in rows:
        start, end, duration = call.get("start_ns"), call.get("end_ns"), call.get("duration_ns")
        if (call.get("outcome") != "completed"
                or type(start) is not int or type(end) is not int or type(duration) is not int
                or start < 0 or end <= start or duration != end - start):
            raise EvidenceError(f"lifecycle phase {phase} lacks a completed call receipt")
    return rows


def _validate_complete_call_ledger(events, plan, *consumed_call_groups):
    """Accept exactly one producer call grammar; resources may interleave anywhere."""
    all_calls = [(index, event) for index, event in enumerate(events)
                 if event.get("event") == "call"]
    construction = [
        ("service_start", None, None),
        ("schema_ensure", None, None),
        *(("initial_durable_ingest", start, min(256, plan["rows"] - start))
          for start in range(0, plan["rows"], 256)),
        ("initial_graph_build", None, None),
    ]
    if len(all_calls) < len(construction):
        raise EvidenceError("native call ledger omits the frozen construction prefix")
    construction_indices = set()
    for (index, call), (phase, first_row, rows) in zip(all_calls, construction):
        start, end, duration = call.get("start_ns"), call.get("end_ns"), call.get("duration_ns")
        if (call.get("phase") != phase or call.get("outcome") != "completed"
                or type(start) is not int or type(end) is not int or type(duration) is not int
                or start < 0 or end <= start or duration != end - start
                or (phase == "initial_durable_ingest"
                    and (call.get("first_row"), call.get("rows")) != (first_row, rows))):
            raise EvidenceError("native call ledger changed the frozen construction prefix")
        construction_indices.add(index)
    expected = set(construction_indices)
    for group in consumed_call_groups:
        expected.update(group)
    actual = {index for index, _ in all_calls}
    if actual != expected:
        raise EvidenceError("native log contains a missing, duplicate, or unknown public call")


def _validate_lifecycle_call_ledger(
        events, plan, fixed_last, overlap_searches, empty_search_index,
        reopened_first, reopened_last, full_state_index):
    overlap_writes = _completed_phase_calls(events, "overlap_replace", plan["writer_calls"])
    expected_starts = [
        (plan["rows"] - 256 * (number + 1)) % plan["rows"]
        for number in range(plan["writer_calls"])
    ]
    if ([(call.get("first_row"), call.get("rows")) for _, call in overlap_writes]
            != [(start, 256) for start in expected_starts]):
        raise EvidenceError("overlap write calls differ from the frozen replacement workload")
    single_phases = [
        "explicit_update", "native_get_many", "http_delete_file",
        "native_deleted_visibility", "reindex_replacement", "native_reinsert_visibility",
        "empty_user", "pre_close_fold", "close", "reopen", "idempotent_ensure",
        "reopen_graph_ensure", "verification_only_full_scroll",
    ]
    calls = {phase: _completed_phase_calls(events, phase, 1)[0]
             for phase in single_phases}
    for phase in ("explicit_update", "reindex_replacement"):
        if (calls[phase][1].get("first_row"), calls[phase][1].get("rows")) != (0, 256):
            raise EvidenceError(f"lifecycle phase {phase} changed its 256-row batch")
    overlap_indices = [index for index, _ in overlap_searches] + [
        index for index, _ in overlap_writes
    ]
    ordered = [
        calls[phase][0] for phase in (
            "explicit_update", "native_get_many", "http_delete_file",
            "native_deleted_visibility", "reindex_replacement", "native_reinsert_visibility",
            "empty_user", "pre_close_fold", "close", "reopen", "idempotent_ensure",
            "reopen_graph_ensure",
        )
    ]
    if (not overlap_indices or not fixed_last < min(overlap_indices)
            or not max(overlap_indices) < ordered[0]
            or ordered != sorted(ordered) or len(set(ordered)) != len(ordered)
            or (empty_search_index is not None and not (
                calls["empty_user"][0] < empty_search_index < calls["pre_close_fold"][0]))
            or not calls["reopen_graph_ensure"][0] < reopened_first <= reopened_last
            or not reopened_last < full_state_index < calls["verification_only_full_scroll"][0]
            or calls["verification_only_full_scroll"][0] >= len(events) - 1):
        raise EvidenceError("mutation, cleanup, reopen, or verification phases are out of order")
    return {index for index, _ in overlap_writes} | {
        index for index, _ in calls.values()
    }


def _validate_post_selection_searches(
        events, quality_index, paired_index, full_state_index, plan, truth, cohorts,
        paired_artifact, initial_owner):
    phases = _post_selection_phases()
    paired_phases = phases - {
        "fixed_coordinate_curve", "overlap_search", "empty_user", "post_reopen_curve",
    }
    if any(event.get("event") in {"call", "search_result"}
           and event.get("phase") in phases
           and not quality_index < index < full_state_index
           for index, event in enumerate(events)):
        raise EvidenceError("post-selection search or receipt occurred outside its phase boundary")
    searches = [(index, event) for index, event in enumerate(events)
                if quality_index < index < full_state_index
                and event.get("event") == "search_result"]
    if any(event.get("phase") not in phases for _, event in searches):
        raise EvidenceError("post-selection search has an unknown or retried phase")
    selected = {}
    try:
        for eligible in plan["eligible_counts"]:
            row = cohorts[str(eligible)]
            coordinate = row.get("coordinate") or row.get("selected_coordinate")
            selected[eligible] = coordinate["ef_search"]
    except (AttributeError, KeyError, TypeError):
        raise EvidenceError("post-selection cohort coordinate is unavailable") from None

    paired_rows = [(index, row) for index, row in searches if row.get("phase") in paired_phases]
    fixed = [(index, row) for index, row in searches
             if row.get("phase") == "fixed_coordinate_curve"]
    overlap = [(index, row) for index, row in searches if row.get("phase") == "overlap_search"]
    empty = [(index, row) for index, row in searches if row.get("phase") == "empty_user"]
    reopened = [(index, row) for index, row in searches
                if row.get("phase") == "post_reopen_curve"]
    if (not paired_rows or not fixed or not overlap or len(empty) != 1 or not reopened
            or not quality_index < paired_rows[0][0] <= paired_rows[-1][0] < paired_index
            or not paired_index < fixed[0][0] <= fixed[-1][0] < overlap[0][0]
            or not overlap[-1][0] < empty[0][0] < reopened[0][0]
            or reopened[-1][0] >= full_state_index):
        raise EvidenceError("post-selection phases are missing or out of producer order")
    if (not _paired_artifact_owner_matches(initial_owner, paired_artifact)
            or any(not native.same_json(_quantized_event_owner(row), initial_owner)
                   for _, row in fixed)):
        raise EvidenceError("paired or fixed-coordinate search changed the pre-mutation graph owner")

    expected_curve = [(eligible, query) for eligible in plan["eligible_counts"]
                      for query in range(plan["queries"])]
    for label, rows in (("fixed", fixed), ("post-reopen", reopened)):
        if [(row.get("eligible"), row.get("query")) for _, row in rows] != expected_curve:
            raise EvidenceError(f"{label} curve is not one exact ordered cohort/query pass")
        if any(row.get("ef") != selected[row.get("eligible")] for _, row in rows):
            raise EvidenceError(f"{label} curve changed a selected coordinate")

    overlap_eligible = plan["overlap_eligible"]
    expected_overlap = Counter(
        (n * plan["reader_concurrency"] + worker) % plan["queries"]
        for worker in range(plan["reader_concurrency"])
        for n in range(64)
    )
    if (len(overlap) != plan["reader_concurrency"] * 64
            or Counter(row.get("query") for _, row in overlap) != expected_overlap
            or any(row.get("eligible") != overlap_eligible
                   or row.get("ef") != selected[overlap_eligible]
                   or type(row.get("writer_active")) is not bool for _, row in overlap)):
        raise EvidenceError("overlap search ledger is incomplete or changed its fixed workload")
    empty_row = empty[0][1]
    if (empty_row.get("eligible"), empty_row.get("query"), empty_row.get("ef"),
            empty_row.get("ids"), empty_row.get("writer_active")) != (
            0, 0, selected[plan["rows"]], [], False):
        raise EvidenceError("empty-user lifecycle search is not the exact typed-empty case")

    expected_paired = _paired_records_in_order(paired_artifact)
    if len(paired_rows) != len(expected_paired):
        raise EvidenceError("paired event ledger differs from the embedded artifact")
    for (_, event), (mode, record) in zip(paired_rows, expected_paired):
        expected_event = {
            "phase": record["phase"], "eligible": record["eligible"],
            "ef": record["requested_ef_search"], "query": record["query"],
            "ids": [result["id"] for result in record["results"]],
            "recall": record["recall"], "ndcg_at_10": record["ndcg_at_10"],
            "writer_active": False, "dense_work": record["dense_work"],
        }
        if mode == "quantized_rerank":
            expected_event.update(
                rerank_candidates=record["requested_rerank_candidates"],
                score_plane=record["score_plane"],
            )
        if (any(not native.same_json(event.get(key), value)
                for key, value in expected_event.items())
                or (mode == "exact" and (event.get("rerank_candidates") is not None
                                         or event.get("score_plane") is not None))):
            raise EvidenceError("paired event differs from its embedded same-owner record")

    for _, event in searches:
        _check_search_metrics(event, truth, plan["rows"])
        paired_exact = event.get("phase") in paired_phases and "_exact" in event["phase"]
        if not paired_exact and not _validate_quantized_search_path(event, plan["rows"]):
            raise EvidenceError("post-selection SQ8 search lacks retained native-v3 path proof")
        eligible = event.get("eligible")
        if (eligible not in {0, plan["rows"]} and eligible <= 4096
                and event.get("ids") != truth[str(eligible)][event["query"]]):
            raise EvidenceError("typed-exact post-selection result changed frozen order")
    search_calls = _validate_search_call_ledger(events, searches, phases)
    lifecycle_calls = _validate_lifecycle_call_ledger(
        events, plan, fixed[-1][0], overlap, empty[0][0], reopened[0][0],
        reopened[-1][0], full_state_index,
    )
    return search_calls | lifecycle_calls


def _recompute_smoke_quality(rows, truth, plan):
    cursor, cohorts = 0, {}
    for eligible in sorted(plan["eligible_counts"], key=lambda value: (value != 512, value)):
        expected = truth[str(eligible)]
        if eligible != 512:
            observed = rows[cursor:cursor + 4]
            if (len(observed) != 4
                    or [row[1].get("query") for row in observed] != list(range(4))
                    or any(row[1].get("eligible") != eligible
                           or row[1].get("ef") != native.RSS_CONTROLS[0]
                           or row[1].get("rerank_candidates") != native.RSS_CONTROLS[0]
                           or row[1].get("phase") != (
                               "predicate_first_quality_query" if index == 0 else "quality_selection")
                           for index, row in enumerate(observed))):
                raise EvidenceError("smoke typed-exact selection is not one ordered query pass")
            matches = [row[1]["ids"] == expected[row[1]["query"]] for row in observed]
            passed = all(matches)
            cohorts[str(eligible)] = {
                "selection_protocol": "typed_exact_correctness_at_first_predeclared_coordinate/v1",
                "target_mean_recall_at_10": 1.0,
                "selected_control": native.RSS_CONTROLS[0] if passed else None,
                "selected_coordinate": ({"ef_search": native.RSS_CONTROLS[0],
                                         "rerank_candidates": native.RSS_CONTROLS[0]}
                                        if passed else None),
                "calibration": {"queries": [0, 1], "curve": []},
                "revalidation": {"queries": [2, 3], "curve": [], "passed": passed},
                "exact_query_matches": matches,
            }
            cursor += 4
            continue
        calibration, revalidation, selected = [], [], None
        for control in native.RSS_CONTROLS:
            observed = rows[cursor:cursor + 4]
            if (len(observed) != 4
                    or [row[1].get("query") for row in observed] != list(range(4))
                    or any(row[1].get("eligible") != eligible
                           or row[1].get("ef") != control
                           or row[1].get("rerank_candidates") != control
                           or row[1].get("phase") != (
                               "predicate_first_quality_query" if cursor == 0 and index == 0
                               else "quality_selection")
                           for index, row in enumerate(observed))):
                raise EvidenceError("smoke all-row selection is not the ordered frozen grid")
            calibration.append(_quality_curve_row(control, observed[:2]))
            revalidation.append(_quality_curve_row(control, observed[2:]))
            cursor += 4
            if (calibration[-1]["mean_recall_at_10"] >= 1.0
                    and revalidation[-1]["mean_recall_at_10"] >= 1.0):
                selected = control
                break
        cohorts[str(eligible)] = {
            "selection_protocol": native.RSS_SELECTION_PROTOCOL,
            "target_mean_recall_at_10": 1.0, "selected_control": selected,
            "calibration": {"queries": [0, 1], "curve": calibration},
            "revalidation": {"queries": [2, 3], "curve": revalidation,
                             "passed": selected is not None},
            "selected_coordinate": (None if selected is None else
                                    {"ef_search": selected, "rerank_candidates": selected}),
        }
    if cursor != len(rows):
        raise EvidenceError("smoke quality selection contains extra or retried calls")
    return cohorts


def _validate_exact_smoke_searches(events, plan, truth, full_state_index):
    phases = {"predicate_first_query", "warm_curve", "overlap_search", "post_reopen_curve"}
    searches = [(index, event) for index, event in enumerate(events)
                if event.get("event") == "search_result"]
    if any(event.get("phase") not in phases for _, event in searches):
        raise EvidenceError("exact smoke has an unknown search phase")
    pre = [(index, row) for index, row in searches
           if row.get("phase") in {"predicate_first_query", "warm_curve"}]
    overlap = [(index, row) for index, row in searches if row.get("phase") == "overlap_search"]
    reopened = [(index, row) for index, row in searches if row.get("phase") == "post_reopen_curve"]
    expected_pre = []
    for eligible in plan["eligible_counts"]:
        expected_pre.append(("predicate_first_query", eligible, 512, 0))
        expected_pre.extend(("warm_curve", eligible, ef, query)
                            for ef in plan["efs"] for query in range(plan["queries"]))
    if [(row.get("phase"), row.get("eligible"), row.get("ef"), row.get("query"))
            for _, row in pre] != expected_pre:
        raise EvidenceError("exact smoke pre-lifecycle curve is incomplete or retried")
    expected_overlap = Counter(
        (n * plan["reader_concurrency"] + worker) % plan["queries"]
        for worker in range(plan["reader_concurrency"]) for n in range(64)
    )
    if (not pre or len(overlap) != plan["reader_concurrency"] * 64
            or Counter(row.get("query") for _, row in overlap) != expected_overlap
            or any(row.get("eligible") != plan["overlap_eligible"]
                   or row.get("ef") != plan["overlap_ef"]
                   or type(row.get("writer_active")) is not bool for _, row in overlap)):
        raise EvidenceError("exact smoke overlap ledger is incomplete")
    expected_reopen = [(eligible, query) for eligible in plan["eligible_counts"]
                       for query in range(plan["queries"])]
    if ([(row.get("eligible"), row.get("query")) for _, row in reopened] != expected_reopen
            or any(row.get("ef") != 512 for _, row in reopened)
            or not pre[-1][0] < overlap[0][0] < overlap[-1][0] < reopened[0][0]
            or reopened[-1][0] >= full_state_index):
        raise EvidenceError("exact smoke reopen ledger is incomplete or out of order")
    for _, event in searches:
        _check_search_metrics(event, truth, 512)
    search_calls = _validate_search_call_ledger(events, searches, phases, "exact")
    lifecycle_calls = _validate_lifecycle_call_ledger(
        events, plan, pre[-1][0], overlap, None, reopened[0][0], reopened[-1][0],
        full_state_index,
    )
    return search_calls | lifecycle_calls


def validate_smoke_log(path, expected_mode, packet, truth):
    events = read_native_events(path)
    plan, full_state, _ = _validate_log_envelope(events, 512, expected_mode)
    if (plan.get("product_commit") != packet["candidate_commit"]
            or plan.get("harness_commit") != packet["candidate_commit"]
            or plan.get("dataset_manifest_sha256")
                != packet["files"][packet["dataset"]["manifest"]]["sha256"]
            or plan.get("dataset_files_sha256") != _dataset_hashes(packet)):
        raise EvidenceError("smoke log is not bound to candidate and dataset")
    full_state_index = events.index(full_state)
    if expected_mode == "exact":
        consumed = _validate_exact_smoke_searches(events, plan, truth, full_state_index)
        _validate_complete_call_ledger(events, plan, consumed)
        return

    quality_events = [(index, event) for index, event in enumerate(events)
                      if event.get("event") == "quantized_quality"]
    paired = [(index, event.get("artifact")) for index, event in enumerate(events)
              if event.get("event") == "paired_query_timing"]
    if (not native.same_json(plan.get("paired_timing"), native.paired_timing_plan(4))
            or len(quality_events) != 1 or len(paired) != 1
            or not native.paired_query_artifact_valid(paired[0][1], plan)):
        raise EvidenceError("smoke SQ8 log lacks one quality decision and paired artifact")
    quality_index, quality_event = quality_events[0]
    selection_phases = {"predicate_first_quality_query", "quality_selection"}
    known_phases = selection_phases | _post_selection_phases()
    if any(event.get("event") == "search_result" and event.get("phase") not in known_phases
           for event in events):
        raise EvidenceError("smoke SQ8 log has an unknown search phase")
    quality_rows, quality_calls = [], set()
    for index, event in enumerate(events):
        if event.get("event") == "search_result" and event.get("phase") in selection_phases:
            recall, ndcg = _check_search_metrics(event, truth, 512)
            if not _validate_quantized_search_path(event, 512):
                raise EvidenceError("smoke quality search lacks retained native-v3 path proof")
            quality_calls.add(_quality_call_for_search(events, index, event))
            quality_rows.append((index, event, recall, ndcg))
    selection_start = min(quality_calls) if quality_calls else quality_index
    if (not quality_rows or quality_rows[-1][0] >= quality_index
            or any(event.get("event") not in {"resource", "call", "search_result"}
                   or (event.get("event") == "search_result"
                       and event.get("phase") not in selection_phases)
                   for event in events[selection_start:quality_index])
            or {index for index in range(selection_start, quality_index)
                if events[index].get("event") == "call"} != quality_calls):
        raise EvidenceError("smoke quality selection is interleaved or has unmatched calls")
    if {index for index, event in enumerate(events)
        if event.get("event") == "call" and event.get("phase") in selection_phases} != quality_calls:
        raise EvidenceError("smoke quality has a public call outside its selection ledger")
    cohorts = _recompute_smoke_quality(quality_rows, truth, plan)
    if not native.same_json(quality_event.get("cohorts"), cohorts):
        raise EvidenceError("smoke quality declaration differs from retained ordered IDs")
    if any((row.get("coordinate") or row.get("selected_coordinate")) is None
           for row in cohorts.values()):
        raise EvidenceError("smoke SQ8 quality did not select every cohort")
    initial_owner = _quantized_event_owner(quality_rows[0][1])
    if (initial_owner is None
            or any(not native.same_json(_quantized_event_owner(row[1]), initial_owner)
                   for row in quality_rows)):
        raise EvidenceError("smoke quality selection changed its initial graph owner")
    post_selection_calls = _validate_post_selection_searches(
        events, quality_index, paired[0][0], full_state_index, plan, truth,
        cohorts, paired[0][1], initial_owner,
    )
    _validate_complete_call_ledger(events, plan, quality_calls, post_selection_calls)


def _quality_curve_row(control, rows):
    recalls, ndcgs = [row[2] for row in rows], [row[3] for row in rows]
    return {
        "control": control,
        "mean_recall_at_10": sum(recalls) / len(recalls),
        "mean_ndcg_at_10": sum(ndcgs) / len(ndcgs),
        "per_query": recalls,
        "per_query_ndcg_at_10": ndcgs,
        "ef_search": control,
        "rerank_candidates": control,
    }


def _recompute_filtered_quality(rows, truth, controls):
    """Consume the exact producer order and rebuild every filtered declaration."""
    cursor, cohorts, all_passed = 0, {}, True
    for eligible in (value for value in FULL_ELIGIBLE_COUNTS if value != 500000):
        if eligible <= 4096:
            observed = rows[cursor:cursor + 200]
            if (len(observed) != 200
                    or [row[1].get("query") for row in observed] != list(range(200))
                    or any(row[1].get("eligible") != eligible
                           or row[1].get("ef") != controls[0]
                           or row[1].get("rerank_candidates") != controls[0]
                           or row[1].get("phase") != (
                               "predicate_first_quality_query" if index == 0 else "quality_selection")
                           for index, row in enumerate(observed))):
                raise EvidenceError("typed-exact filtered cohort is not one ordered fixed-set pass")
            matches = [row[1]["ids"] == truth[str(eligible)][row[1]["query"]]
                       for row in observed]
            passed = all(matches)
            cohorts[str(eligible)] = {
                "selection_protocol": "typed_exact_correctness_at_first_predeclared_coordinate/v1",
                "target_mean_recall_at_10": 1.0,
                "selected_control": controls[0] if passed else None,
                "selected_coordinate": ({"ef_search": controls[0],
                                         "rerank_candidates": controls[0]} if passed else None),
                "calibration": {"queries": list(range(100)), "curve": []},
                "revalidation": {"queries": list(range(100, 200)), "curve": [],
                                 "passed": passed},
                "exact_query_matches": matches,
            }
            cursor += 200
            all_passed = all_passed and passed
            continue

        calibration, revalidation, selected = [], [], None
        for control_index, control in enumerate(controls):
            observed = rows[cursor:cursor + 200]
            if (len(observed) != 200
                    or [row[1].get("query") for row in observed] != list(range(200))
                    or any(row[1].get("eligible") != eligible
                           or row[1].get("ef") != control
                           or row[1].get("rerank_candidates") != control
                           or row[1].get("phase") != (
                               "predicate_first_quality_query"
                               if control_index == 0 and index == 0 else "quality_selection")
                           for index, row in enumerate(observed))):
                raise EvidenceError("filtered ANN cohort does not follow the ordered frozen grid")
            left, right = observed[:100], observed[100:]
            calibration.append(_quality_curve_row(control, left))
            revalidation.append(_quality_curve_row(control, right))
            cursor += 200
            if (calibration[-1]["mean_recall_at_10"] >= native.RSS_RECALL_TARGET
                    and revalidation[-1]["mean_recall_at_10"] >= native.RSS_RECALL_TARGET):
                selected = control
                break
        cohorts[str(eligible)] = {
            "selection_protocol": native.RSS_SELECTION_PROTOCOL,
            "target_mean_recall_at_10": native.RSS_RECALL_TARGET,
            "selected_control": selected,
            "calibration": {"queries": list(range(100)), "curve": calibration},
            "revalidation": {"queries": list(range(100, 200)), "curve": revalidation,
                             "passed": selected is not None},
            "selected_coordinate": (None if selected is None else
                                    {"ef_search": selected, "rerank_candidates": selected}),
        }
        all_passed = all_passed and selected is not None
    if cursor != len(rows):
        raise EvidenceError("filtered quality selection contains extra or out-of-order calls")
    return cohorts, all_passed


def validate_full_sq8_log(path, packet, truth, sq8_rss, vectors, queries):
    events = read_native_events(path)
    plan, full_states, terminal, failures = _validate_log_envelope(
        events, 500000, "quantized_rerank", defer_terminal=True,
    )
    if (plan.get("product_commit") != packet["candidate_commit"]
            or plan.get("harness_commit") != packet["candidate_commit"]
            or plan.get("dataset_manifest_sha256")
                != packet["files"][packet["dataset"]["manifest"]]["sha256"]
            or plan.get("dataset_files_sha256") != _dataset_hashes(packet)
            or plan.get("rss_controls") != native.RSS_CONTROLS
            or plan.get("rss_calibration_queries") != native.RSS_CALIBRATION_QUERIES
            or plan.get("rss_revalidation_queries") != native.RSS_REVALIDATION_QUERIES
            or not _close(plan.get("rss_recall_target"), native.RSS_RECALL_TARGET)
            or plan.get("quantized_index_name") != native.QUANTIZED_PROFILE_NAME
            or plan.get("quantized_coordinate_policy")
                != "ordered_ef_grid_with_requested_rerank_candidates_equal_ef"
            or not native.same_json(plan.get("representation_arm"),
                                    native.quantized_representation_arm())
            or not native.same_json(plan.get("paired_timing"), native.paired_timing_plan(200))):
        raise EvidenceError("full SQ8 log is not bound to candidate and dataset")
    if native.sq8_rss_artifact_reasons(sq8_rss, plan):
        raise EvidenceError("pinned SQ8 RSS decision is not intrinsically valid for the full plan")
    locks = [(index, event) for index, event in enumerate(events)
             if event.get("event") == "coordinate_lock_consumed"]
    locked_quality = [(index, event) for index, event in enumerate(events)
                      if event.get("event") == "locked_all_rows_quality"]
    paired = [(index, event.get("artifact")) for index, event in enumerate(events)
              if event.get("event") == "paired_query_timing"]
    quality_events = [(index, event) for index, event in enumerate(events)
                      if event.get("event") == "quantized_quality"]
    if len(locks) != 1 or len(locked_quality) != 1 or len(paired) > 1 or len(quality_events) != 1:
        raise EvidenceError("full SQ8 log is missing a unique lock, quality, or paired artifact")
    lock_index, lock = locks[0]
    quality_index, locked_declared = locked_quality[0]
    quality_event_index, quality_event = quality_events[0]
    rss_name = packet["arms"]["treedb_sq8_rss"]
    coordinate = sq8_rss.get("quality", {}).get("selected_coordinate")
    plan_lock = plan.get("all_rows_coordinate_lock") or {}
    lifetimes = terminal.get("process_lifetimes")
    initial_identity = (lifetimes[0].get("linux_process_identity")
                        if isinstance(lifetimes, list) and lifetimes
                        and isinstance(lifetimes[0], dict) else None)
    if (lock.get("source_artifact_sha256") != packet["files"][rss_name]["sha256"]
            or lock.get("source_process_identity") != sq8_rss.get("rss", {}).get("process_identity")
            or not native.same_json(lock.get("coordinate"), coordinate)
            or not native.linux_process_identity_valid(lock.get("source_process_identity"))
            or not native.linux_process_identity_valid(lock.get("fresh_process_identity"))
            or lock.get("fresh_process_identity") == lock.get("source_process_identity")
            or lock.get("fresh_process_identity") != initial_identity
            or plan_lock.get("schema") != "treedb_cohere_sq8_coordinate_lock/v1"
            or plan_lock.get("artifact_schema") != native.QUANTIZED_RSS_ARTIFACT_SCHEMA
            or plan_lock.get("artifact_sha256") != packet["files"][rss_name]["sha256"]
            or plan_lock.get("source_process_identity") != lock.get("source_process_identity")
            or not native.same_json(plan_lock.get("selected_coordinate"), coordinate)):
        raise EvidenceError("all-row coordinate lock is not bound to a fresh owner and pinned RSS source")

    quality_rows = []
    quality_call_indices = set()
    selection_phases = {
        "locked_all_rows_revalidation", "quality_selection",
        "predicate_first_quality_query",
    }
    if any(event.get("event") == "search_result"
           and event.get("phase") not in selection_phases | _post_selection_phases()
           for event in events):
        raise EvidenceError("full SQ8 log has an unknown search phase")
    for index, event in enumerate(events):
        if event.get("event") != "search_result":
            continue
        phase, eligible = event.get("phase"), event.get("eligible")
        if eligible == 500000 and phase in ("quality_selection", "predicate_first_quality_query"):
            raise EvidenceError("full SQ8 log retuned the all-row coordinate")
        if phase in ("locked_all_rows_revalidation", "quality_selection",
                     "predicate_first_quality_query"):
            recall, ndcg = _check_search_metrics(event, truth, 500000)
            if not _validate_quantized_search_path(event, 500000):
                raise EvidenceError("quality search lacks retained native-v3 path proof")
            quality_call_indices.add(_quality_call_for_search(events, index, event))
            quality_rows.append((index, event, recall, ndcg))
    if {index for index, event in enumerate(events)
        if event.get("event") == "call" and event.get("phase") in selection_phases} \
            != quality_call_indices:
        raise EvidenceError("full SQ8 quality has a public call outside its selection ledger")

    locked = [(index, row, recall, ndcg) for index, row, recall, ndcg in quality_rows
              if row.get("phase") == "locked_all_rows_revalidation"]
    if (len(locked) != 200 or lock_index >= locked[0][0] or locked[-1][0] >= quality_index
            or [row[1].get("query") for row in locked] != list(range(200))):
            raise EvidenceError("locked all-row revalidation is not exactly one ordered 200-query pass")
    locked_interval = range(lock_index + 1, quality_index)
    if (any(event.get("event") not in {"resource", "call", "search_result"}
           or (event.get("event") == "search_result"
               and event.get("phase") != "locked_all_rows_revalidation")
           for event in events[lock_index + 1:quality_index])
            or {index for index in locked_interval if events[index].get("event") == "call"}
                != {index for index in quality_call_indices if index in locked_interval}):
        raise EvidenceError("locked all-row pass was interleaved with another benchmark phase")
    selected = coordinate.get("ef_search") if isinstance(coordinate, dict) else None
    if (type(selected) is not int
            or selected not in native.RSS_CONTROLS
            or coordinate != {"ef_search": selected, "rerank_candidates": selected}
            or any(row[1].get("eligible") != 500000 or row[1].get("ef") != selected
                   or row[1].get("rerank_candidates") != selected for row in locked)):
        raise EvidenceError("locked all-row requests changed the selected E=R coordinate")
    if not native.sq8_requests_share_initial_owner(
            [{"score_plane": row[1]["score_plane"]} for row in locked]):
        raise EvidenceError("locked all-row requests do not share one immutable initial graph owner")
    locked_owner = _quantized_event_owner(locked[0][1])
    if (locked_owner is None
            or any(not native.same_json(_quantized_event_owner(row[1]), locked_owner)
                   for row in quality_rows)):
        raise EvidenceError("full quality selection changed the locked initial graph owner")

    first, second = locked[:100], locked[100:]
    def fixed(rows, queries):
        return {
            "queries": queries,
            "mean_recall_at_10": sum(row[2] for row in rows) / len(rows),
            "mean_ndcg_at_10": sum(row[3] for row in rows) / len(rows),
            "per_query_recall": [row[2] for row in rows],
            "per_query_ndcg_at_10": [row[3] for row in rows],
        }
    recomputed_locked = {
        "schema": "treedb_cohere_sq8_locked_all_rows_confirmation/v1",
        "selection_protocol": "prior_sq8_rss_coordinate_revalidated_on_fresh_graph/v1",
        "coordinate": coordinate,
        "fixed_sets": {"queries_0_99": fixed(first, list(range(100))),
                       "queries_100_199": fixed(second, list(range(100, 200)))},
        "target_mean_recall_at_10": native.RSS_RECALL_TARGET,
        "passed": (sum(row[2] for row in first) / 100 >= native.RSS_RECALL_TARGET
                   and sum(row[2] for row in second) / 100 >= native.RSS_RECALL_TARGET),
    }
    declared_payload = {key: value for key, value in locked_declared.items() if key != "event"}
    if not native.same_json(declared_payload, recomputed_locked):
        raise EvidenceError("locked all-row quality differs from retained IDs and truth")

    if quality_index >= quality_event_index:
        raise EvidenceError("quantized quality was emitted before locked all-row confirmation")
    filtered_rows = [row for row in quality_rows
                     if quality_index < row[0] < quality_event_index]
    filtered_interval = range(quality_index + 1, quality_event_index)
    if (any(event.get("event") not in {"resource", "call", "search_result"}
           or (event.get("event") == "search_result" and event.get("phase") not in {
               "quality_selection", "predicate_first_quality_query",
           }) for event in events[quality_index + 1:quality_event_index])
            or {index for index in filtered_interval if events[index].get("event") == "call"}
                != {index for index in quality_call_indices if index in filtered_interval}):
        raise EvidenceError("filtered selection was interleaved with another benchmark phase")
    if any(row[0] < lock_index or row[0] > quality_event_index for row in quality_rows):
        raise EvidenceError("quality-selection calls occurred outside the frozen selection phase")
    if recomputed_locked["passed"]:
        recomputed_filtered, filtered_passed = _recompute_filtered_quality(
            filtered_rows, truth, native.RSS_CONTROLS,
        )
    else:
        if filtered_rows:
            raise EvidenceError("full SQ8 producer continued after the locked all-row quality miss")
        recomputed_filtered, filtered_passed = {}, False
    recomputed_cohorts = {"500000": recomputed_locked, **recomputed_filtered}
    cohorts = quality_event.get("cohorts")
    if not native.same_json(cohorts, recomputed_cohorts):
        raise EvidenceError("quantized cohort ledger is incomplete or changes the all-row decision")
    all_passed = recomputed_locked["passed"] and filtered_passed
    if all_passed:
        paired_index = paired[0][0] if len(paired) == 1 else None
        full_state_index = events.index(full_states[0]) if len(full_states) == 1 else None
        if (failures or len(paired) != 1 or len(full_states) != 1
                or not quality_event_index < paired_index < full_state_index < len(events) - 1
                or terminal.get("lifecycle_complete") is not True
                or terminal.get("qualification") != "producer_gates_passed"
                or terminal.get("error") is not None
                or type(terminal.get("final_disk_bytes")) is not int
                or terminal["final_disk_bytes"] <= 0
                or full_states[0].get("rows") != 500000
                or full_states[0].get("vectors_checked") != 500000
                or not _close(full_states[0].get("normalized_full_vector_tolerance"), 1e-6)):
            raise EvidenceError("passing quality lacks complete paired/lifecycle/final-state evidence")
        try:
            native.validate_shutdowns(terminal.get("process_lifetimes"), 2)
        except (RuntimeError, TypeError) as exc:
            raise EvidenceError(f"passing SQ8 lifetimes did not shut down cleanly: {exc}") from exc
        paired_artifact = paired[0][1]
        if not native.paired_query_artifact_valid(paired_artifact, plan):
            raise EvidenceError("paired same-graph artifact failed its native validator")
        if not native.same_json(paired_artifact.get("selected_coordinate"), coordinate):
            raise EvidenceError("paired timing changed the locked all-row coordinate")
        if not _paired_owner_matches_locked_calls(locked, paired_artifact):
            raise EvidenceError("paired timing owner differs from the locked all-row graph owner")
        locked_ids = {row[1]["query"]: row[1]["ids"] for row in locked}
        if not paired_results_match_dataset(
                paired_artifact, vectors, queries, truth["500000"], locked_ids):
            raise EvidenceError(
                "paired same-graph IDs, metrics, scores, or ordering differ from frozen evidence")
        post_selection_calls = _validate_post_selection_searches(
            events, quality_event_index, paired_index, full_state_index, plan, truth,
            cohorts, paired_artifact, locked_owner,
        )
        _validate_complete_call_ledger(
            events, plan, quality_call_indices, post_selection_calls,
        )
        return True, paired_statistics(paired_artifact), full_states[0]

    typed_failures = [event for event in events if event.get("event") == "failure"]
    unexpected_failures = [event for event in events if event.get("event") in {
        "guard_failed", "search_request_failure", "quantized_failure_evidence",
        "finalization_failure",
    } or event.get("outcome") == "error"]
    error = typed_failures[0].get("error") if len(typed_failures) == 1 else None
    failed_cohorts = [
        key for key, row in recomputed_cohorts.items()
        if ((row.get("coordinate") if "coordinate" in row else row.get("selected_coordinate"))
            is None
            or (row.get("passed") if "passed" in row
                else row.get("revalidation", {}).get("passed")) is not True)
    ]
    expected_error = (
        "QualityUnqualified: quantized fixed-set quality failed for cohorts "
        + ",".join(failed_cohorts)
    )
    later = events[quality_event_index + 1:-1]
    if (len(typed_failures) != 1 or unexpected_failures or paired or full_states
            or error != expected_error
            or terminal.get("lifecycle_complete") is not False
            or terminal.get("qualification") != "valid_unqualified"
            or terminal.get("error") != error
            or type(terminal.get("final_disk_bytes")) is not int
            or terminal["final_disk_bytes"] <= 0
            or any(event.get("event") not in {"failure", "resource"} for event in later)
            or sum(event.get("event") == "failure" for event in later) != 1):
        raise EvidenceError("quality miss does not have the exact fail-closed terminal disposition")
    try:
        native.validate_shutdowns(terminal.get("process_lifetimes"), 1)
    except (RuntimeError, TypeError) as exc:
        raise EvidenceError(f"unqualified SQ8 lifetime did not shut down cleanly: {exc}") from exc
    _validate_complete_call_ledger(events, plan, quality_call_indices)
    return False, None, None


def _nearest_rank(values, fraction):
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * fraction) - 1]


def _median(values):
    ordered = sorted(values)
    middle = len(ordered) // 2
    return (ordered[middle] if len(ordered) % 2 else
            (ordered[middle - 1] + ordered[middle]) / 2)


def paired_statistics(artifact):
    """Raw calls -> per-repetition nearest-rank metrics -> median and MAD."""
    arms = {}
    for mode in ("exact", "quantized_rerank"):
        raw, repetitions = [], []
        for batch in artifact["repetitions"]:
            arm = batch["arms"][mode]
            requests = arm["requests"]
            values = [request["duration_ns"] for request in requests]
            if (not values or any(type(value) is not int or value <= 0 for value in values)):
                raise EvidenceError("paired timing contains an invalid duration")
            elapsed = requests[-1]["ended_monotonic_ns"] - requests[0]["started_monotonic_ns"]
            if type(elapsed) is not int or elapsed <= 0:
                raise EvidenceError("paired timing batch span is invalid")
            resources = arm["resources"]["delta"]
            resource_metrics = {
                name: resources[name] for name in (
                    "server_cpu_ns_per_call", "client_harness_cpu_ns_per_call",
                    "total_alloc_bytes_per_call", "mallocs_per_call",
                )
            }
            if any(type(value) not in (int, float) or not math.isfinite(value) or value < 0
                   for value in resource_metrics.values()):
                raise EvidenceError("paired resource-per-call metric is invalid")
            raw.append(values)
            repetitions.append({
                "repetition": batch["repetition"], "calls": len(values), "raw_duration_ns": values,
                "batch_elapsed_ns": elapsed,
                "p50_ns": _nearest_rank(values, .50), "p95_ns": _nearest_rank(values, .95),
                "p99_ns": _nearest_rank(values, .99), "qps": len(values) * 1e9 / elapsed,
                "resources_per_call": resource_metrics,
            })
        summary = {}
        metrics = {metric: [row[metric] for row in repetitions]
                   for metric in ("p50_ns", "p95_ns", "p99_ns", "qps")}
        for metric in next(iter(repetitions))["resources_per_call"]:
            metrics[metric] = [row["resources_per_call"][metric] for row in repetitions]
        for metric, values in metrics.items():
            center = _median(values)
            summary[metric] = {"median": center,
                               "mad": _median([abs(value - center) for value in values])}
        arms[mode] = {"raw_repetitions_ns": raw, "per_repetition": repetitions,
                      "across_repetitions": summary}
    return {"schema": "treedb_cohere_q5_paired_statistics/v1",
            "aggregation": "nearest_rank_within_repetition_then_median_and_MAD; no_call_pooling",
            "arms": arms}


def paired_results_match_dataset(artifact, vectors, queries, truth, locked_ids=None):
    """Recompute every paired result's IDs, metrics, scores, and stable ordering."""
    try:
        for batch in [artifact["warmup"], *artifact["repetitions"]]:
            for mode, arm in batch["arms"].items():
                for record in arm["requests"]:
                    query_index = record["query"]
                    expected = truth[query_index]
                    results = record["results"]
                    ids = [result["id"] for result in results]
                    if (len(ids) != len(expected) or len(ids) != len(set(ids))
                            or (mode == "quantized_rerank" and locked_ids is not None
                                and ids != locked_ids[query_index])):
                        return False
                    recall, ndcg = _metric(ids, expected)
                    if (not _close(record.get("recall"), recall)
                            or not _close(record.get("ndcg_at_10"), ndcg)):
                        return False
                    query = np.asarray(queries[query_index], dtype=np.float64)
                    query_norm = np.linalg.norm(query)
                    if not math.isfinite(query_norm) or query_norm <= 0:
                        return False
                    scores = []
                    for result in results:
                        ordinal = int(result["id"].removeprefix("row-"))
                        if (result["id"] != f"row-{ordinal:06d}"
                                or not 0 <= ordinal < len(vectors)):
                            return False
                        vector = np.asarray(vectors[ordinal], dtype=np.float64)
                        vector_norm = np.linalg.norm(vector)
                        if not math.isfinite(vector_norm) or vector_norm <= 0:
                            return False
                        score = np.dot(vector, query) / (vector_norm * query_norm)
                        if (not math.isfinite(score)
                                or abs(score - result["score"]) > 2e-5):
                            return False
                        scores.append(score)
                    if list(zip((-score for score in scores), ids)) != sorted(
                            zip((-score for score in scores), ids)):
                        return False
        return True
    except (AttributeError, KeyError, TypeError, ValueError, IndexError):
        return False


def _quantized_event_owner(event):
    try:
        snapshot = event["score_plane"]["snapshot"]
        return {
            key: snapshot[key] for key in (
                "schema_hash", "schema_generation", "base_manifest", "current_manifest",
                "base_coverage_lsn", "current_coverage_lsn",
            )
        }
    except (KeyError, TypeError):
        return None


def _paired_owner_matches_locked_calls(locked, artifact):
    try:
        return _paired_artifact_owner_matches(_quantized_event_owner(locked[0][1]), artifact)
    except (IndexError, KeyError, TypeError):
        return False


def _paired_artifact_owner_matches(owner, artifact):
    try:
        return owner is not None and native.same_json(owner, artifact["warmup"]["common_owner"])
    except (KeyError, TypeError):
        return False


def _validate_fp32_quality_observations(artifact, truth, control_name, total_rows):
    quality = artifact.get("quality") or {}
    observed = artifact.get("observed_quality") or {}
    requests = observed.get("requests")
    if (set(observed) != {"schema", "control_name", "requests", "exact_reference"}
            or observed.get("schema") != "cohere_500k_768d_quality_observations/v1"
            or observed.get("control_name") != control_name
            or not isinstance(requests, list) or not requests):
        raise EvidenceError(f"{control_name} RSS artifact lacks ordered quality observations")
    expected_order = [
        (row["control"], query)
        for row in quality["calibration"]["curve"]
        for query in quality["calibration"]["queries"] + quality["revalidation"]["queries"]
    ]
    recomputed = {}
    for sequence, record in enumerate(requests, 1):
        if (not isinstance(record, dict)
                or set(record) != {"request_sequence", "control", "query", "ids"}
                or record.get("request_sequence") != sequence
                or sequence > len(expected_order)
                or (record.get("control"), record.get("query")) != expected_order[sequence - 1]):
            raise EvidenceError(f"{control_name} RSS observations are missing, extra, or reordered")
        event = {
            "eligible": total_rows, "query": record["query"], "ids": record["ids"],
            "recall": 0.0, "ndcg_at_10": 0.0,
        }
        expected = truth[record["query"]]
        recall, ndcg = _metric(record["ids"], expected)
        event.update(recall=recall, ndcg_at_10=ndcg)
        _check_search_metrics(event, {str(total_rows): truth}, total_rows)
        key = (record["control"], record["query"])
        if key in recomputed:
            raise EvidenceError(f"{control_name} RSS observations repeat a coordinate/query")
        recomputed[key] = (recall, ndcg)
    if len(requests) != len(expected_order):
        raise EvidenceError(f"{control_name} RSS observations are truncated")
    for split in ("calibration", "revalidation"):
        queries = quality[split]["queries"]
        for row in quality[split]["curve"]:
            values = [recomputed[(row["control"], query)] for query in queries]
            expected_row = {
                "control": row["control"],
                "mean_recall_at_10": sum(value[0] for value in values) / len(values),
                "mean_ndcg_at_10": sum(value[1] for value in values) / len(values),
                "per_query": [value[0] for value in values],
                "per_query_ndcg_at_10": [value[1] for value in values],
            }
            if not native.same_json(row, expected_row):
                raise EvidenceError(f"{control_name} RSS aggregate quality differs from retained IDs")
    reference = observed["exact_reference"]
    if control_name == "hnsw_ef":
        if (not isinstance(reference, dict) or set(reference) != {"query", "ids"}
                or reference.get("query") != 0 or reference.get("ids") != truth[0]):
            raise EvidenceError("Qdrant exact correctness reference changed frozen truth order")
    elif reference is not None:
        raise EvidenceError("TreeDB FP32 RSS artifact fabricates an exact reference")


def _artifact_dataset_contract(artifact, manifest_sha256, dataset_hashes):
    contract = artifact.get("comparison_contract") or {}
    return (contract.get("dataset_manifest_sha256") == manifest_sha256
            and native.same_json(contract.get("dataset_files_sha256"), dataset_hashes))


def _fp32_rss_readiness_valid(tree):
    construction = tree.get("construction_calibration_contract") or {}
    ef_construction = construction.get("ef_construction")
    readiness = tree.get("readiness") or {}
    quality = tree.get("quality") or {}
    successful = sum(
        len(row.get("per_query", []))
        for split in (quality.get("calibration") or {}, quality.get("revalidation") or {})
        for row in split.get("curve", [])
    )
    return (
        type(ef_construction) is int and ef_construction in (32, 64, 96, 128)
        and native.same_json(
            construction, native.construction_calibration_contract(ef_construction),
        )
        and readiness.get("graph_action") == "build"
        and type(readiness.get("successful_ann_queries")) is int
        and readiness["successful_ann_queries"] == successful
        and native.column_graph_build_valid(
            readiness.get("column_graph_build"), expect_quantized_assets=False,
        )
        and native.same_json(readiness.get("effective_index"), {
            "m": 16, "ef_construction": ef_construction,
        })
    )


def validate_rss_and_comparisons(
        packet, paths, dataset_paths, full_truth=None, vectors=None, queries=None):
    tree = read_json(paths[packet["arms"]["treedb_fp32_rss"]], "TreeDB FP32 RSS")
    sq8 = read_json(paths[packet["arms"]["treedb_sq8_rss"]], "TreeDB SQ8 RSS")
    qd = read_json(paths[packet["arms"]["qdrant_fp32_rss"]], "Qdrant FP32 RSS")
    stored_fp32 = read_json(paths[packet["arms"]["fp32_comparison"]], "stored FP32 comparison")
    stored_three = read_json(paths[packet["arms"]["three_arm_comparison"]], "stored three-arm comparison")
    hashes = _dataset_hashes(packet)
    manifest_sha = packet["files"][packet["dataset"]["manifest"]]["sha256"]
    if not all(_artifact_dataset_contract(artifact, manifest_sha, hashes)
               for artifact in (tree, sq8, qd)):
        raise EvidenceError("RSS artifacts are not bound to the packet dataset")
    expected_tree_provenance = {
        "harness_commit", "harness_source_sha256", "harness_trees", "product_commit",
        "product_trees", "service_sha256", "dataset_manifest_sha256",
        "dataset_files_sha256", "serving_sha256",
    }
    for artifact in (tree, sq8):
        provenance = artifact.get("provenance") or {}
        if (set(provenance) != expected_tree_provenance
                or provenance.get("harness_commit") != packet["candidate_commit"]
                or provenance.get("product_commit") != packet["candidate_commit"]
                or provenance.get("harness_source_sha256") != sha256_file(Path(native.__file__))
                or not valid_sha256(provenance.get("service_sha256"))
                or not valid_sha256(provenance.get("serving_sha256"))
                or provenance.get("dataset_manifest_sha256") != manifest_sha
                or not native.same_json(provenance.get("dataset_files_sha256"), hashes)):
            raise EvidenceError("TreeDB RSS artifact is not from the exact candidate")
    if not _fp32_rss_readiness_valid(tree):
        raise EvidenceError("TreeDB FP32 RSS readiness is incomplete")
    qd_provenance = qd.get("provenance") or {}
    expected_qdrant_provenance = {
        "harness_commit", "harness_source_sha256", "harness_trees", "qdrant_bin_sha256",
        "qdrant_server_version", "qdrant_client_version", "dataset_manifest_sha256",
        "dataset_files_sha256", "treedb_artifact_sha256", "treedb_sq8_artifact_sha256",
        "process_identity",
        "process_command_identity",
    }
    if (set(qd_provenance) != expected_qdrant_provenance
            or qd_provenance.get("harness_commit") != packet["candidate_commit"]
            or qd_provenance.get("harness_source_sha256") != sha256_file(Path(qdrant.__file__))
            or qd_provenance.get("qdrant_server_version") != qdrant.existing.SERVER_VERSION
            or qd_provenance.get("qdrant_client_version") != qdrant.existing.CLIENT_VERSION
            or not valid_sha256(qd_provenance.get("qdrant_bin_sha256"))
            or qd_provenance.get("treedb_artifact_sha256")
                != packet["files"][packet["arms"]["treedb_fp32_rss"]]["sha256"]
            or qd_provenance.get("treedb_sq8_artifact_sha256")
                != packet["files"][packet["arms"]["treedb_sq8_rss"]]["sha256"]
            or not isinstance(qd_provenance.get("process_command_identity"), str)
            or not qd_provenance["process_command_identity"]
            or qd_provenance.get("dataset_manifest_sha256") != manifest_sha
            or not native.same_json(qd_provenance.get("dataset_files_sha256"), hashes)):
        raise EvidenceError("Qdrant RSS artifact is not from the exact candidate harness")
    if (not qdrant.ready_snapshot(qd.get("readiness") or {}, 500000)
            or not native.process_peak_rss_valid(qd.get("fresh_start_peak_rss") or {})
            or qd["fresh_start_peak_rss"].get("process_identity")
                != qd_provenance.get("process_identity")):
        raise EvidenceError("Qdrant RSS readiness or fresh-process boundary is incomplete")
    if not qdrant._qdrant_resource_guard_valid(qd.get("resource_guard")):
        raise EvidenceError("Qdrant resource guard did not pass")
    if not qdrant._qdrant_cleanup_valid(qd.get("cleanup")):
        raise EvidenceError("Qdrant owned cleanup is incomplete")
    if not native.boundary_storage_valid(qd.get("storage")):
        raise EvidenceError("Qdrant boundary storage is unavailable")
    vectors = vectors if vectors is not None else np.memmap(
        dataset_paths["documents"], mode="r", dtype="<f4", shape=(500000, 768),
    )
    queries = queries if queries is not None else np.memmap(
        dataset_paths["queries"], mode="r", dtype="<f4", shape=(200, 768),
    )
    frozen_truth = (full_truth or _full_truth(dataset_paths["truth"]))["500000"]
    _validate_fp32_quality_observations(tree, frozen_truth, "ef_search", 500000)
    _validate_fp32_quality_observations(qd, frozen_truth, "hnsw_ef", 500000)
    if not native.sq8_results_match_dataset(sq8, vectors, queries, frozen_truth):
        raise EvidenceError("TreeDB SQ8 RSS IDs, quality, or FP32 scores differ from frozen data")
    recomputed_fp32 = qdrant.compare_artifacts(tree, qd)
    if not native.same_json(stored_fp32, recomputed_fp32):
        raise EvidenceError("stored FP32 comparison differs from recomputation")
    recomputed_three = qdrant.compare_three_arms(tree, qd, sq8)
    recomputed_three["input_artifact_sha256"] = {
        "treedb_fp32": packet["files"][packet["arms"]["treedb_fp32_rss"]]["sha256"],
        "treedb_sq8": packet["files"][packet["arms"]["treedb_sq8_rss"]]["sha256"],
        "qdrant_fp32": packet["files"][packet["arms"]["qdrant_fp32_rss"]]["sha256"],
    }
    if not native.same_json(stored_three, recomputed_three):
        raise EvidenceError("stored three-arm comparison differs from recomputation")
    if recomputed_three.get("state") != "complete":
        raise EvidenceError("three-arm RSS evidence is incomplete")
    return tree, sq8, qd, recomputed_fp32, recomputed_three


def _smoke_truth(dataset_paths):
    vectors = np.memmap(dataset_paths["documents"], mode="r", dtype="<f4", shape=(500000, 768))
    queries = np.memmap(dataset_paths["queries"], mode="r", dtype="<f4", shape=(200, 768))
    return native.exact_truth(vectors[:512], queries[:4], native.counts(512))


def _full_truth(path):
    truth = read_json(path, "frozen truth")
    if set(truth) != {str(value) for value in FULL_ELIGIBLE_COUNTS}:
        raise EvidenceError("frozen truth has unknown or missing cohorts")
    for eligible in FULL_ELIGIBLE_COUNTS:
        rows = truth.get(str(eligible))
        if not isinstance(rows, list) or len(rows) != 200:
            raise EvidenceError("frozen truth lacks a required 200-query cohort")
        for result in rows:
            if (not isinstance(result, list) or len(result) != 10
                    or len(result) != len(set(result))):
                raise EvidenceError("frozen truth has invalid top-10 result cardinality")
            for identifier in result:
                try:
                    ordinal = int(identifier.removeprefix("row-"))
                except (AttributeError, TypeError, ValueError):
                    raise EvidenceError("frozen truth contains an invalid logical ID") from None
                if (identifier != f"row-{ordinal:06d}" or not 0 <= ordinal < 500000
                        or (ordinal * 7919) % 500000 >= eligible):
                    raise EvidenceError("frozen truth ID is outside its declared cohort")
    return truth


def _normalized_file(packet, paths, group, role):
    return paths[packet[group][role]]


def _normalized_dataset(packet, paths):
    mapped = {role: _normalized_file(packet, paths, "dataset", role)
              for role in DATASET_KINDS}
    manifest = read_json(mapped["manifest"], "normalized-v4 dataset manifest", MAX_PACKET_BYTES)
    if ((manifest.get("rows"), manifest.get("dimensions"), manifest.get("query_count"),
         manifest.get("top_k"), manifest.get("exact_train_query_overlap"))
            != (500000, 768, 200, 10, 0)):
        raise EvidenceError("normalized-v4 dataset shape differs from the frozen Cohere export")
    for role, suffix, size in (
            ("documents", "documents_sha256", 500000 * 768 * 4),
            ("queries", "queries_sha256", 200 * 768 * 4),
            ("truth", "truth_sha256", None)):
        entry = packet["files"][packet["dataset"][role]]
        if (manifest.get(suffix) != entry["sha256"]
                or (size is not None and entry["bytes"] != size)):
            raise EvidenceError(f"normalized-v4 dataset {role} differs from its manifest")
    return mapped, manifest


def _normalized_run_path(packet, paths, mode, role):
    return paths[packet["runs"][mode]["files"][role]]


def _normalized_command_flags(argv, plan):
    executable = plan.get("python_executable")
    if (len(argv) < 2 or not isinstance(executable, str)
            or not Path(executable).is_absolute()
            or not Path(argv[0]).is_absolute()
            or argv[0] != executable
            or not argv[1].endswith("minima_cohere_native_diagnostic.py")):
        raise EvidenceError("normalized-v4 command does not invoke the reviewed producer")
    flags, index = {}, 2
    while index < len(argv):
        key = argv[index]
        if not key.startswith("--") or key in flags:
            raise EvidenceError("normalized-v4 command contains an unknown or duplicate argument")
        if key == "--construction-decisions":
            flags[key] = True
            index += 1
            continue
        if index + 1 >= len(argv):
            raise EvidenceError("normalized-v4 command argument omits its value")
        flags[key] = argv[index + 1]
        index += 2
    return flags


def _normalized_validate_infrastructure(plan, mode):
    infrastructure = plan.get("infrastructure") or {}
    try:
        host_memory = native.physical_memory_bytes(plan.get("host_memory_bytes"))
        effective = native.normalized_v4_effective_resources(plan)
        required_cpu_millis = int(plan["gomaxprocs"]) * 1000
    except (KeyError, TypeError, ValueError) as exc:
        raise EvidenceError(f"normalized-v4 {mode} resource receipt is invalid") from exc
    expected_checks = {
        "cpu_affinity": True,
        "cgroup_cpu_quota": effective["effective_cpu_quota_millis"] >= required_cpu_millis,
        "cgroup_memory": effective["effective_memory_bytes"] >= 24 * native.GIB,
        "disk_headroom": True,
        "host_memory": host_memory >= 24 * native.GIB,
        "linux_procfs": True,
    }
    if (infrastructure.get("schema") != "treedb_normalized_v4_infrastructure/v1"
            or infrastructure.get("state") != "available"
            or infrastructure.get("runner") != "shared_workstation_serialized_quiet_window"
            or infrastructure.get("dataset_cache") != "persistent_local"
            or infrastructure.get("artifact_storage") != "local_owned_directory"
            or infrastructure.get("checks") != expected_checks
            or not all(expected_checks.values())
            or infrastructure.get("requirements") != {
                "minimum_free_bytes": 21 * native.GIB,
                "minimum_host_memory_bytes": 24 * native.GIB,
                "minimum_effective_memory_bytes": 24 * native.GIB,
                "minimum_cpu_quota_millis": required_cpu_millis,
            }
            or infrastructure.get("effective_resources") != effective
            or infrastructure.get("operator_contract") != {
                "other_benchmark_load": "excluded",
                "run_policy": "one_arm_at_a_time",
            }
            or infrastructure.get("reasons") != []):
        raise EvidenceError(f"normalized-v4 {mode} infrastructure preflight is unavailable")
    return effective


def _normalized_producer_coordinates(plans):
    fields = (
        "python", "numpy", "platform", "python_executable", "python_executable_sha256",
        "cpu_affinity", "gomaxprocs", "treedb_go_runtime", "blas_threads",
        "host_memory_bytes", "host_resource_identity",
    )
    coordinates = {
        mode: {field: plan.get(field) for field in fields}
        for mode, plan in plans.items()
    }
    for mode, plan in plans.items():
        coordinates[mode]["effective_resources"] = (
            (plan.get("infrastructure") or {}).get("effective_resources")
        )
    if set(coordinates) != {"exact", "sq8"} \
            or not native.same_json(coordinates["exact"], coordinates["sq8"]):
        raise EvidenceError("normalized-v4 exact and SQ8 runs used different producer coordinates")
    return coordinates["exact"]


def _consumer_runtime_identity():
    return native.python_runtime_identity()


def _normalized_validate_plan_and_receipt(packet, paths, mode, dataset_manifest):
    plan_path = _normalized_run_path(packet, paths, mode, "plan")
    plan = read_json(plan_path, f"normalized-v4 {mode} plan", MAX_PACKET_BYTES)
    expected_mode = "quantized_rerank" if mode == "sq8" else "exact"
    dataset_hashes = {
        role: packet["files"][packet["dataset"][role]]["sha256"]
        for role in ("documents", "queries", "truth")
    }
    input_hash = lambda role: packet["files"][packet["inputs"][role]]["sha256"]
    harness_trees, product_trees = _source_trees(packet["candidate_commit"])
    if (plan.get("schema") != native.NORMALIZED_CAMPAIGN_SCHEMA
            or plan.get("campaign_profile") != native.CAMPAIGN_PROFILE_NORMALIZED_V4
            or plan.get("product_commit") != packet["candidate_commit"]
            or plan.get("harness_commit") != packet["candidate_commit"]
            or plan.get("harness_source_sha256") != sha256_file(Path(native.__file__))
            or plan.get("harness_trees") != harness_trees
            or plan.get("product_trees") != product_trees
            or not isinstance(plan.get("python"), str) or not plan["python"]
            or not isinstance(plan.get("numpy"), str) or not plan["numpy"]
            or not isinstance(plan.get("platform"), str) or not plan["platform"]
            or not isinstance(plan.get("python_executable"), str)
            or not Path(plan["python_executable"]).is_absolute()
            or not valid_sha256(plan.get("python_executable_sha256"))
            or plan.get("mode") != "diagnostic" or plan.get("rss_only") is not False
            or plan.get("rows") != 500000 or plan.get("dimensions") != 768
            or plan.get("queries") != 200 or plan.get("top_k") != 10
            or plan.get("batch_size") != 256 or plan.get("overlap_eligible") != 4097
            or plan.get("reader_concurrency") != 4 or plan.get("writer_calls") != 8
            or plan.get("operation_timeout_s") != 600 or plan.get("wall_limit_s") != 2700
            or plan.get("minimum_free_bytes") != 10 * native.GIB
            or plan.get("maximum_output_bytes") != 11 * native.GIB
            or plan.get("maximum_combined_rss_bytes") != 24 * native.GIB
            or plan.get("construction_decisions") is not False
            or plan.get("eligible_counts") != FULL_ELIGIBLE_COUNTS
            or plan.get("representation") != native.NORMALIZED_REPRESENTATION
            or plan.get("dataset_manifest_sha256")
                != packet["files"][packet["dataset"]["manifest"]]["sha256"]
            or plan.get("dataset_files_sha256") != dataset_hashes
            or plan.get("service_sha256") != input_hash("treedb_service_binary")
            or plan.get("go_helper_sha256") != input_hash("go_helper")
            or plan.get("serving_sha256") != input_hash("serving")
            or plan.get("fixed_coordinate") != {
                "ef_search": 64, "rerank_candidates": 64, "top_k": 10,
            }
            or plan.get("production_timing") != {
                "schema": "treedb_cohere_v4_production_timing/v1",
                "queries": list(range(200)), "warmup_queries_per_arm": 20,
                "measured_repetitions": 6,
                "arm_order": "alternate_first_complete_arm_batch_by_repetition",
                "diagnostics": False, "return_embedding": False,
            }
            or plan.get("blas_threads") != {
                "OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1",
            }
            or not isinstance(plan.get("cpu_affinity"), list) or not plan["cpu_affinity"]
            or any(type(cpu) is not int or cpu < 0 for cpu in plan["cpu_affinity"])
            or not isinstance(plan.get("gomaxprocs"), str) or not plan["gomaxprocs"].isdigit()
            or int(plan["gomaxprocs"]) <= 0
            or (plan.get("treedb_go_runtime") or {}).get("GOMAXPROCS") != plan["gomaxprocs"]):
        raise EvidenceError(f"normalized-v4 {mode} plan differs from the frozen campaign")
    _normalized_validate_infrastructure(plan, mode)
    try:
        host_memory = native.physical_memory_bytes(plan.get("host_memory_bytes"))
    except (TypeError, ValueError) as exc:
        raise EvidenceError(f"normalized-v4 {mode} host-memory receipt is invalid") from exc
    host = plan.get("host_resource_identity") or {}
    if (host_memory < 24 * native.GIB
            or set(host) != {
                "machine_id", "boot_id", "page_size_bytes", "numa_mems",
                "cgroup_membership", "cgroup_limits", "cpu_model", "cpu_features",
            }
            or any(not isinstance(host.get(field), str) or not host[field] for field in (
                "machine_id", "boot_id", "numa_mems", "cgroup_membership", "cpu_model",
            ))
            or type(host.get("page_size_bytes")) is not int or host["page_size_bytes"] <= 0
            or not isinstance(host.get("cgroup_limits"), dict)
            or not isinstance(host.get("cpu_features"), list) or not host["cpu_features"]
            or host["cpu_features"] != sorted(set(host["cpu_features"]))):
        raise EvidenceError(f"normalized-v4 {mode} host identity is incomplete")
    if mode == "sq8":
        if (plan.get("query_mode") != expected_mode
                or plan.get("quantized_index_name") != native.QUANTIZED_PROFILE_NAME
                or plan.get("vector_m") != 16):
            raise EvidenceError("normalized-v4 SQ8 plan omitted its selected code plane")
    elif plan.get("query_mode", "exact") != expected_mode or plan.get("quantized_index_name") is not None:
        raise EvidenceError("normalized-v4 exact plan is not FP32-only")
    serving = read_json(
        _normalized_file(packet, paths, "inputs", "serving"),
        "normalized-v4 serving input", MAX_PACKET_BYTES,
    )
    if not native.same_json(plan.get("serving"), serving):
        raise EvidenceError(f"normalized-v4 {mode} plan serving limits differ from the pinned input")

    receipt = packet["runs"][mode]
    freeze = _normalized_command_flags(receipt["freeze_argv"], plan)
    run = _normalized_command_flags(receipt["run_argv"], plan)
    common = {
        "--dataset": plan["dataset"], "--service-bin": plan["service_bin"],
        "--product-commit": packet["candidate_commit"],
        "--serving": plan["serving_path"], "--run-dir": plan["run_dir"],
        "--rows": "500000", "--campaign-profile": native.CAMPAIGN_PROFILE_NORMALIZED_V4,
        "--query-mode": expected_mode, "--ef-construction": str(plan["ef_construction"]),
        "--go-helper": plan["go_helper"], "--go": plan["go_tool"],
        "--url": plan["url"], "--native-address": plan["native_address"],
        "--diagnostics-url": plan["diagnostics_url"],
    }
    if mode == "sq8":
        common["--quantized-index-name"] = native.QUANTIZED_PROFILE_NAME
    freeze_expected = {**common, "--freeze": str(plan_path)}
    run_expected = {
        **common, "--run": str(plan_path),
        "--expected-plan-sha256": packet["files"][receipt["files"]["plan"]]["sha256"],
    }
    # Packet assembly may copy the frozen plan; allow only that path relocation.
    freeze["--freeze"] = str(plan_path) if "--freeze" in freeze else None
    run["--run"] = str(plan_path) if "--run" in run else None
    if freeze != freeze_expected or run != run_expected:
        raise EvidenceError(f"normalized-v4 {mode} command differs from its frozen plan")
    if dataset_manifest.get("documents_sha256") != plan["dataset_files_sha256"]["documents"]:
        raise EvidenceError("normalized-v4 plan dataset binding is inconsistent")
    return plan


def _normalized_truth_artifact(path, source_truth, source_truth_sha256):
    artifact = read_json(path, "normalized-v4 truth", MAX_JSON_BYTES)
    if (not isinstance(artifact, dict)
            or artifact.get("schema") != "treedb_cohere_normalized_v4_truth/v1"
            or artifact.get("rows") != 500000 or artifact.get("dimensions") != 768
            or artifact.get("query_order") != list(range(200))
            or artifact.get("eligible_counts") != FULL_ELIGIBLE_COUNTS
            or artifact.get("source_truth_sha256") != source_truth_sha256):
        raise EvidenceError("normalized-v4 truth artifact identity is invalid")
    canonical_truth = (artifact.get("canonical_normalized_f32_dot") or {}).get("ordered_ids")
    canonical_scores = (artifact.get("canonical_normalized_f32_dot") or {}).get("ordered_scores")
    original_truth = (artifact.get("original_fp32_cosine") or {}).get("ordered_ids")
    expected_keys = {str(value) for value in FULL_ELIGIBLE_COUNTS}
    if (not isinstance(canonical_truth, dict) or set(canonical_truth) != expected_keys
            or not isinstance(canonical_scores, dict) or set(canonical_scores) != expected_keys
            or not isinstance(original_truth, dict) or set(original_truth) != expected_keys):
        raise EvidenceError("normalized-v4 truth artifact lacks a required truth family")
    for eligible in FULL_ELIGIBLE_COUNTS:
        key = str(eligible)
        for rows in (canonical_truth[key], canonical_scores[key], original_truth[key]):
            if not isinstance(rows, list) or len(rows) != 200:
                raise EvidenceError("normalized-v4 truth artifact query cardinality is invalid")
        for query in range(200):
            ids, scores, original = canonical_truth[key][query], canonical_scores[key][query], original_truth[key][query]
            if (len(ids) != 10 or len(set(ids)) != 10 or len(scores) != 10
                    or len(original) != 10 or len(set(original)) != 10
                    or any(not isinstance(score, (int, float)) or not math.isfinite(score)
                           for score in scores)):
                raise EvidenceError("normalized-v4 truth top-10 row is invalid")
            for identifier in ids + original:
                try:
                    ordinal = int(identifier.removeprefix("row-"))
                except (AttributeError, TypeError, ValueError):
                    raise EvidenceError("normalized-v4 truth contains an invalid ID") from None
                if (identifier != f"row-{ordinal:06d}" or not 0 <= ordinal < 500000
                        or (ordinal * 7919) % 500000 >= eligible):
                    raise EvidenceError("normalized-v4 truth ID is outside its cohort")
    if not native.same_json(original_truth, source_truth):
        raise EvidenceError("normalized-v4 original-cosine truth differs from the frozen oracle")
    return artifact, canonical_truth, canonical_scores, original_truth


def _normalized_recompute_canonical_truth(dataset_paths, canonical_truth, canonical_scores):
    """Recompute from source with the shared, Go-golden-checked oracle."""
    source_vectors = np.memmap(
        dataset_paths["documents"], mode="r", dtype="<f4", shape=(500000, 768),
    )
    source_queries = np.memmap(
        dataset_paths["queries"], mode="r", dtype="<f4", shape=(200, 768),
    )
    try:
        normalized = native.canonical_normalized_f32_rows(source_vectors)
        queries = native.canonical_normalized_f32_rows(source_queries)
        expected_ids, expected_scores = native.canonical_normalized_f32_topk(
            normalized, queries, FULL_ELIGIBLE_COUNTS,
        )
    except ValueError as exc:
        raise EvidenceError("normalized-v4 source vectors are invalid") from exc
    for key in expected_ids:
        if (canonical_truth[key] != expected_ids[key]
                or not np.allclose(canonical_scores[key], expected_scores[key], rtol=2e-6, atol=2e-6)):
            raise EvidenceError("normalized-v4 canonical truth differs from source recomputation")
    return normalized, queries


def _normalized_result_scores_match(ids, scores, query, normalized_vectors, normalized_queries):
    if ids == []:
        return scores == [] and type(query) is int and 0 <= query < len(normalized_queries)
    try:
        ordinals = np.asarray(
            [int(identifier.removeprefix("row-")) for identifier in ids], dtype=np.int64,
        )
        expected = np.clip(normalized_vectors[ordinals] @ normalized_queries[query], -1, 1)
        observed = np.asarray(scores, dtype=np.float64)
    except (AttributeError, IndexError, TypeError, ValueError):
        return False
    return (expected.shape == observed.shape and np.isfinite(observed).all()
            and np.all(np.abs(observed) <= 1)
            and np.allclose(observed, expected, rtol=2e-6, atol=2e-6))


def _normalized_route_identity(identity, mode, *, eligible, filtered, result_count):
    expected_routes = (("typed_hnsw",) if not filtered else
                       ("typed_empty",) if eligible == 0 else
                       ("typed_exact",) if eligible <= 4096 else
                       ("typed_exact", "typed_hnsw") if eligible <= 5000 else
                       ("typed_hnsw",))
    if (not isinstance(identity, dict) or identity.get("version") != 1
            or identity.get("representation") != native.NORMALIZED_REPRESENTATION
            or identity.get("query_mode") != mode
            or type(eligible) is not int or eligible < 0
            or identity.get("execution_route") not in expected_routes
            or identity.get("return_embedding") is not False
            or identity.get("diagnostics") is not True
            or identity.get("filter") is not filtered
            or identity.get("top_k") != 10 or identity.get("ef_search") != 64
            or identity.get("result_count") != result_count
            or any(type(identity.get(field)) is not int or identity[field] <= 0 for field in (
                "schema_hash", "schema_generation", "base_manifest_generation",
                "base_manifest_checksum", "current_manifest_generation",
                "current_manifest_checksum", "current_coverage_lsn",
            ))
            or identity["current_manifest_generation"] < identity["base_manifest_generation"]
            or (identity["current_manifest_generation"] == identity["base_manifest_generation"]
                and identity["current_manifest_checksum"] != identity["base_manifest_checksum"])
            or type(identity.get("fp32_score_calls")) is not int
            or type(identity.get("fp32_vector_bytes_read")) is not int
            or identity["fp32_vector_bytes_read"] != identity["fp32_score_calls"] * 768 * 4
            or (identity.get("execution_route") == "typed_empty")
                != (identity["fp32_score_calls"] == 0)
            or any(identity.get(field) != 0 for field in (
                "embedding_vector_reads", "embedding_vector_bytes", "embedding_output_bytes",
            ))):
        raise EvidenceError("normalized-v4 diagnostic route identity is incomplete")
    if mode == "exact":
        if (identity.get("rerank_candidates") != 0
                or identity.get("quantized_index_name") != ""
                or identity.get("quantized_codec") != ""
                or identity.get("quantized_version") != 0
                or identity.get("quantized_score_calls") != 0
                or identity.get("quantized_code_bytes_read") != 0):
            raise EvidenceError("normalized-v4 exact diagnostic crossed the SQ8 score plane")
        packed = tuple(identity.get(field, 0) for field in (
            "packed_score_calls", "packed_score_candidates", "packed_vector_bytes_read",
        ))
        if identity.get("execution_route") != "typed_empty":
            if (not 0 <= packed[0] <= packed[1] <= identity["fp32_score_calls"]
                    or (packed[0] == 0) != (packed[1] == 0)
                    or packed[2] != packed[1] * 768 * 4):
                raise EvidenceError("normalized-v4 exact route omitted packed FP32 work")
        elif any(packed):
            raise EvidenceError("normalized-v4 exact empty route carried packed work")
    elif (identity.get("rerank_candidates") != 64
            or identity.get("quantized_index_name") != native.QUANTIZED_PROFILE_NAME
            or identity.get("quantized_codec") != "scalar_u8"
            or identity.get("quantized_version") != 1):
        raise EvidenceError("normalized-v4 SQ8 diagnostic identity is incomplete")
    elif (type(identity.get("packed_score_candidates")) is not int
            or not 0 <= identity["packed_score_candidates"] <= identity["fp32_score_calls"]
            or identity.get("packed_score_calls") != int(identity["packed_score_candidates"] > 0)
            or (identity["current_manifest_generation"] == identity["base_manifest_generation"]
                and identity["fp32_score_calls"] != identity["packed_score_candidates"])):
        raise EvidenceError("normalized-v4 SQ8 base/suffix packed accounting is invalid")
    elif identity.get("execution_route") == "typed_hnsw":
        if (type(identity.get("quantized_score_calls")) is not int
                or identity["quantized_score_calls"] <= 0
                or identity.get("quantized_code_bytes_read")
                    != identity["quantized_score_calls"] * 768
                or not 0 <= identity["packed_score_candidates"] <= 64
                or identity.get("packed_vector_bytes_read")
                    != identity["packed_score_candidates"] * 768 * 4):
            raise EvidenceError("normalized-v4 SQ8 diagnostic omitted candidate or packed work")
    elif identity.get("execution_route") == "typed_exact":
        if (identity.get("quantized_score_calls") != 0
                or identity.get("quantized_code_bytes_read") != 0
                or not 0 <= identity["packed_score_candidates"] <= 5000
                or identity.get("packed_vector_bytes_read")
                    != identity["packed_score_candidates"] * 768 * 4):
            raise EvidenceError("normalized-v4 SQ8 exact route omitted packed FP32 work")
    elif any(identity.get(field, 0) != 0 for field in (
            "quantized_score_calls", "quantized_code_bytes_read", "packed_score_calls",
            "packed_score_candidates", "packed_vector_bytes_read")):
        raise EvidenceError("normalized-v4 SQ8 empty route carried score work")


def _normalized_dense_event(event, request_mode, *, filtered, result_count):
    """Recompute the complete graph/output/score-plane contract from retained raw work."""
    try:
        work = native.dense_contract.DenseSearchWork.from_dict(event.get("dense_work"))
    except (TypeError, ValueError) as exc:
        raise EvidenceError("normalized-v4 diagnostic dense work is invalid") from exc
    identity = event.get("route_identity") or {}
    snapshot = work.graph.snapshot
    if (not work.completed or not work.graph.available or not work.graph.completed
            or work.graph.route != identity.get("execution_route")
            or work.graph.filter.attempted is not filtered
            or (filtered and (not work.graph.filter.completed
                              or work.graph.filter.eligible_rows != event.get("eligible")))
            or not work.graph.snapshot.available
            or identity.get("schema_hash") != snapshot.schema_hash
            or identity.get("schema_generation") != snapshot.schema_generation
            or identity.get("base_manifest_generation") != snapshot.base_manifest.generation
            or identity.get("base_manifest_checksum") != snapshot.base_manifest.checksum
            or identity.get("current_manifest_generation") != snapshot.current_manifest.generation
            or identity.get("current_manifest_checksum") != snapshot.current_manifest.checksum
            or identity.get("current_coverage_lsn") != snapshot.current_coverage_lsn
            or not work.output.attempted or not work.output.completed
            or work.output.requested != result_count or work.output.fetched != result_count
            or work.output.missing != 0
            or work.output.retained_payload_fetches != result_count
            or work.output.json_reconstruction_rows != result_count
            or work.output.typed_column_rows > result_count
            or (work.output.output_bytes == 0) != (result_count == 0)):
        raise EvidenceError("normalized-v4 diagnostic graph/output work is incomplete")
    if request_mode == "exact":
        if (event.get("score_plane") is not None
                or identity.get("fp32_score_calls")
                    != work.graph.base_ann_scored + work.graph.delta_scored
                or identity.get("packed_score_candidates", 0) > work.graph.base_ann_scored):
            raise EvidenceError("normalized-v4 exact diagnostic work does not bind its FP32 route")
        return
    try:
        proof = native.dense_contract.DenseScorePlaneProof.from_dict(event.get("score_plane"))
    except (TypeError, ValueError) as exc:
        raise EvidenceError("normalized-v4 SQ8 diagnostic score-plane proof is invalid") from exc
    if (not native.dense_contract.dense_quantized_response_work_matches(
                work, proof, 10, result_count, filtered)
            or not native.dense_contract.dense_score_plane_byte_counters_match(proof, 768)
            or not native.dense_contract.dense_score_plane_packed_counters_match(proof, 768)
            or proof.requested_mode != "quantized_rerank"
            or proof.effective_mode != "quantized_rerank"
            or proof.requested_ef_search != 64
            or proof.requested_rerank_candidates != 64
            or proof.quantized_index_name != native.QUANTIZED_PROFILE_NAME
            or identity.get("quantized_score_calls") != proof.quantized_score_calls
            or identity.get("fp32_score_calls") != (
                proof.exact_base_rerank_score_calls
                + proof.exact_small_filter_score_calls
                + proof.exact_suffix_score_calls
            )
            or identity.get("packed_score_calls") != proof.packed_score_batch_calls
            or identity.get("packed_score_candidates") != proof.packed_score_candidates
            or identity.get("packed_vector_bytes_read") != proof.packed_vector_bytes_read):
        raise EvidenceError("normalized-v4 SQ8 diagnostic work does not bind one packed route")


def _normalized_expected_projection_digest(rows):
    updated = set(range(256))
    for number in range(8):
        start = (rows - 256 * (number + 1)) % rows
        updated.update(range(start, start + 256))
    digest = hashlib.sha256()
    for row in range(rows):
        payload = native.canonical({
            "id": f"row-{row:06d}",
            "content": f"minima-cohere:{row}" + (":updated" if row in updated else ""),
            "meta": {
                "user_id": f"{(row * 7919) % rows:06d}",
                "fpath": f"/cohere/{row // 256:06d}.txt",
            },
        })
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _normalized_validate_events(path, plan, mode, canonical_truth, original_truth,
                                normalized_vectors, normalized_queries):
    events = read_native_events(path)
    if not events or events[0].get("event") != "plan" or not native.same_json(events[0].get("plan"), plan):
        raise EvidenceError(f"normalized-v4 {mode} event log does not start with its frozen plan")
    terminal = [event for event in events if event.get("event") == "terminal"]
    if (len(terminal) != 1 or terminal[0].get("qualification") != "producer_gates_passed"
            or terminal[0].get("lifecycle_complete") is not True or terminal[0].get("error") is not None
            or events[-1] is not terminal[0]
            or type(terminal[0].get("final_disk_bytes")) is not int
            or terminal[0]["final_disk_bytes"] <= 0
            or any(event.get("event") in {"failure", "finalization_failure"} for event in events)):
        raise EvidenceError(f"normalized-v4 {mode} producer did not complete cleanly")
    try:
        native.validate_shutdowns(
            terminal[0].get("process_lifetimes"),
            native.expected_shutdown_lifetimes(plan, None, "producer_gates_passed"),
        )
    except (RuntimeError, TypeError, KeyError, AttributeError) as exc:
        raise EvidenceError(f"normalized-v4 {mode} service lifetimes did not shut down cleanly: {exc}") from exc
    calls = [event for event in events if event.get("event") == "call"]
    ingests = [call for call in calls if call.get("phase") == "initial_durable_ingest"]
    if (len(ingests) != math.ceil(500000 / 256)
            or [(row.get("first_row"), row.get("rows")) for row in ingests]
                != [(start, min(256, 500000 - start)) for start in range(0, 500000, 256)]
            or any(row.get("outcome") != "completed" for row in calls)):
        raise EvidenceError(f"normalized-v4 {mode} construction call ledger is incomplete")
    required_single = {
        "service_start", "schema_ensure", "initial_graph_build", "explicit_update",
        "native_get_many", "http_delete_file", "native_deleted_visibility",
        "reindex_replacement", "native_reinsert_visibility", "pre_close_fold", "close",
        "reopen", "idempotent_ensure", "reopen_graph_ensure",
        "verification_only_full_scroll",
    }
    counts = Counter(call.get("phase") for call in calls)
    if any(counts[phase] != 1 for phase in required_single) or counts["overlap_replace"] != 8:
        raise EvidenceError(f"normalized-v4 {mode} lifecycle call ledger is incomplete")

    searches = [event for event in events if event.get("event") == "search_result"]
    expected_modes = ("exact", "quantized_rerank") if mode == "sq8" else ("exact",)
    fixed = [row for row in searches if row.get("phase") == "fixed_coordinate_curve"]
    reopened = [row for row in searches if row.get("phase") == "post_reopen_curve"]
    expected_curve = [(request_mode, eligible, query)
                      for request_mode in expected_modes
                      for eligible in FULL_ELIGIBLE_COUNTS for query in range(200)]
    for label, rows in (("fixed", fixed), ("post-reopen", reopened)):
        if [(row.get("request_mode"), row.get("eligible"), row.get("query"))
                for row in rows] != expected_curve:
            raise EvidenceError(f"normalized-v4 {mode} {label} curve is incomplete or reordered")
    # Small filters are exhaustive; ANN candidates may legitimately change when
    # process-local filter navigation warms or the graph is rebuilt on reopen.
    if any(before["eligible"] <= 4096
           and (before.get("ids"), before.get("scores"))
           != (after.get("ids"), after.get("scores"))
           for before, after in zip(fixed, reopened)):
        raise EvidenceError(f"normalized-v4 {mode} post-reopen search decisions changed")
    quality = {request_mode: [] for request_mode in expected_modes}
    post_reopen_quality = {request_mode: [] for request_mode in expected_modes}
    for position, event in enumerate(fixed + reopened):
        request_mode, eligible, query = event.get("request_mode"), event.get("eligible"), event.get("query")
        ids, scores = event.get("ids"), event.get("scores")
        if (request_mode not in expected_modes or eligible not in FULL_ELIGIBLE_COUNTS
                or type(query) is not int or not 0 <= query < 200
                or not isinstance(ids, list) or len(ids) != 10 or len(set(ids)) != 10
                or not isinstance(scores, list) or len(scores) != len(ids)
                or any(not isinstance(score, (int, float)) or not math.isfinite(score)
                       for score in scores)
                or not _normalized_result_scores_match(
                    ids, scores, query, normalized_vectors, normalized_queries,
                )
                or list(zip((-score for score in scores), ids))
                    != sorted(zip((-score for score in scores), ids))):
            raise EvidenceError("normalized-v4 diagnostic result ordering is invalid")
        for identifier in ids:
            ordinal = int(identifier.removeprefix("row-"))
            if identifier != f"row-{ordinal:06d}" or (ordinal * 7919) % 500000 >= eligible:
                raise EvidenceError("normalized-v4 diagnostic result left its scalar cohort")
        expected = canonical_truth[str(eligible)][query]
        original = original_truth[str(eligible)][query]
        recall = len(set(ids) & set(expected)) / 10
        original_recall = len(set(ids) & set(original)) / 10
        if (event.get("recall") != recall or event.get("original_cosine_recall") != original_recall
                or event.get("command_version") != 4 or event.get("diagnostics") is not True):
            raise EvidenceError("normalized-v4 diagnostic quality declaration was not recomputed")
        _normalized_route_identity(
            event.get("route_identity"), request_mode,
            eligible=eligible, filtered=eligible != 500000, result_count=10,
        )
        _normalized_dense_event(
            event, request_mode, filtered=eligible != 500000, result_count=10,
        )
        if request_mode == "quantized_rerank":
            proof = event.get("score_plane")
            if (not isinstance(proof, dict) or proof.get("forbidden_stable_score_calls") != 0
                    or proof.get("packed_score_batch_calls")
                        != event["route_identity"].get("packed_score_calls")
                    or proof.get("packed_score_candidates")
                        != event["route_identity"].get("packed_score_candidates")):
                raise EvidenceError("normalized-v4 SQ8 diagnostic proof is not packed")
        if eligible == 500000:
            phase_quality = quality if position < len(fixed) else post_reopen_quality
            phase_quality[request_mode].append(recall)

    overlap = [row for row in searches if row.get("phase") == "overlap_search"]
    post_update = [row for row in searches if row.get("phase") == "post_update_visibility"]
    post_replace = [row for row in searches if row.get("phase") == "post_replacement_visibility"]
    empty = [row for row in searches if row.get("phase") == "empty_user"]
    if (len(overlap) != 256 or len(post_update) != len(expected_modes)
            or len(post_replace) != len(expected_modes) or len(empty) != len(expected_modes)
            or [row.get("request_mode") for row in post_update] != list(expected_modes)
            or [row.get("request_mode") for row in post_replace] != list(expected_modes)
            or [row.get("request_mode") for row in empty] != list(expected_modes)
            or any(row.get("ids") for row in empty)
            or any(row.get("eligible") != 4097 or type(row.get("writer_active")) is not bool
                   for row in overlap)):
        raise EvidenceError(f"normalized-v4 {mode} mutable lifecycle search ledger is incomplete")
    expected_overlap_modes = ({"exact": 256} if mode == "exact"
                              else {"exact": 128, "quantized_rerank": 128})
    if Counter(row.get("request_mode") for row in overlap) != expected_overlap_modes:
        raise EvidenceError(f"normalized-v4 {mode} overlap mode ledger is incomplete")
    for event in overlap + post_update + post_replace + empty:
        request_mode = event.get("request_mode")
        ids, scores = event.get("ids"), event.get("scores")
        eligible = event.get("eligible")
        if (request_mode not in expected_modes or type(eligible) is not int
                or type(event.get("query")) is not int or not 0 <= event["query"] < 200
                or not isinstance(ids, list) or len(ids) != len(set(ids))
                or not isinstance(scores, list) or len(scores) != len(ids)
                or any(not isinstance(score, (int, float)) or not math.isfinite(score)
                       for score in scores)
                or not _normalized_result_scores_match(
                    ids, scores, event["query"], normalized_vectors, normalized_queries,
                )
                or list(zip((-score for score in scores), ids))
                    != sorted(zip((-score for score in scores), ids))
                or event.get("command_version") != 4 or event.get("diagnostics") is not True):
            raise EvidenceError("normalized-v4 mutable lifecycle search result is invalid")
        for identifier in ids:
            try:
                ordinal = int(identifier.removeprefix("row-"))
            except (AttributeError, TypeError, ValueError):
                raise EvidenceError("normalized-v4 lifecycle result contains an invalid ID") from None
            if (identifier != f"row-{ordinal:06d}" or not 0 <= ordinal < 500000
                    or (ordinal * 7919) % 500000 >= eligible):
                raise EvidenceError("normalized-v4 lifecycle result left its scalar cohort")
        filtered = eligible != 500000
        _normalized_route_identity(
            event.get("route_identity"), request_mode,
            eligible=eligible, filtered=filtered, result_count=len(ids),
        )
        _normalized_dense_event(
            event, request_mode, filtered=filtered, result_count=len(ids),
        )
    full = [event for event in events if event.get("event") == "full_state_verified"]
    expected_digest = _normalized_expected_projection_digest(500000)
    if (len(full) != 1 or full[0].get("rows") != 500000
            or full[0].get("vectors_checked") != 500000
            or full[0].get("projection_sha256") != expected_digest
            or not 0 <= full[0].get("maximum_vector_error", math.inf) <= 1e-6
            or not 0 <= full[0].get("maximum_norm_error", math.inf) <= 1e-5):
        raise EvidenceError(f"normalized-v4 {mode} exhaustive final state is invalid")
    return {
        "events": events, "fixed": fixed, "terminal": terminal[0],
        "quality": {key: statistics.mean(values) for key, values in quality.items()},
        "post_reopen_quality": {
            key: statistics.mean(values) for key, values in post_reopen_quality.items()
        },
        "full_state": full[0],
    }


def _normalized_nearest_rank(values, fraction):
    ordered = sorted(values)
    if not ordered:
        raise EvidenceError("normalized-v4 timing sample is empty")
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def _normalized_production_identity(route, mode, lane):
    if lane.startswith("collection_"):
        if not isinstance(route, dict) or not isinstance(route.get("receipt"), dict):
            raise EvidenceError("normalized-v4 collection timing omitted its route receipt")
        receipt = route["receipt"]
        if (receipt.get("Route") != "typed_hnsw" or receipt.get("Available") is not True
                or receipt.get("Representation") != native.NORMALIZED_REPRESENTATION
                or receipt.get("QueryMode") != mode or receipt.get("ResultCount") != 10
                or any(type(receipt.get(field)) is not int or receipt[field] <= 0 for field in (
                    "SchemaHash", "SchemaGeneration", "BaseManifestGeneration",
                    "BaseManifestChecksum", "CurrentManifestGeneration",
                    "CurrentManifestChecksum", "CurrentCoverageLSN",
                ))
                or receipt["CurrentManifestGeneration"] < receipt["BaseManifestGeneration"]
                or (receipt["CurrentManifestGeneration"] == receipt["BaseManifestGeneration"]
                    and receipt["CurrentManifestChecksum"] != receipt["BaseManifestChecksum"])
                or route.get("embedding_vector_reads") != 0
                or route.get("embedding_vector_bytes") != 0
                or route.get("embedding_output_bytes") != 0):
            raise EvidenceError("normalized-v4 collection timing left the production route")
        packed_calls = route.get("packed_score_calls")
        packed_candidates = route.get("packed_score_candidates")
        packed_bytes = route.get("packed_vector_bytes_read")
        if (type(packed_calls) is not int or type(packed_candidates) is not int
                or type(packed_bytes) is not int or not 0 < packed_calls <= packed_candidates
                or packed_bytes != packed_candidates * 768 * 4):
            raise EvidenceError("normalized-v4 collection timing omitted packed FP32 work")
        if mode == "quantized_rerank":
            if (receipt.get("QuantizedIndexName") != native.QUANTIZED_PROFILE_NAME
                    or receipt.get("QuantizedCodec") != "scalar_u8"
                    or receipt.get("QuantizedVersion") != 1
                    or type(route.get("quantized_score_calls")) is not int
                    or route["quantized_score_calls"] <= 0
                    or route.get("fp32_score_calls") != packed_candidates
                    or packed_calls != 1 or packed_candidates > 64):
                raise EvidenceError("normalized-v4 collection SQ8 timing omitted packed work")
        elif (receipt.get("QuantizedIndexName") != ""
                or receipt.get("QuantizedCodec") != ""
                or receipt.get("QuantizedVersion") != 0
                or route.get("quantized_score_calls", 0) != 0
                or route.get("fp32_score_calls", 0) != 0):
            raise EvidenceError("normalized-v4 collection exact timing crossed score planes")
        return
    if (not isinstance(route, dict) or route.get("version") != 1
            or route.get("representation") != native.NORMALIZED_REPRESENTATION
            or route.get("query_mode") != mode or route.get("execution_route") != "typed_hnsw"
            or route.get("return_embedding") is not False or route.get("diagnostics") is not False
            or route.get("filter") is not False or route.get("top_k") != 10
            or route.get("ef_search") != 64 or route.get("result_count") != 10
            or any(type(route.get(field)) is not int or route[field] <= 0 for field in (
                "schema_hash", "schema_generation", "base_manifest_generation",
                "base_manifest_checksum", "current_manifest_generation",
                "current_manifest_checksum", "current_coverage_lsn", "fp32_score_calls",
            ))
            or route["current_manifest_generation"] < route["base_manifest_generation"]
            or (route["current_manifest_generation"] == route["base_manifest_generation"]
                and route["current_manifest_checksum"] != route["base_manifest_checksum"])
            or route.get("fp32_vector_bytes_read") != route["fp32_score_calls"] * 768 * 4
            or any(route.get(field) != 0 for field in (
                "embedding_vector_reads", "embedding_vector_bytes", "embedding_output_bytes",
            ))):
        raise EvidenceError("normalized-v4 timed client route identity is incomplete")
    packed_calls = route.get("packed_score_calls")
    packed_candidates = route.get("packed_score_candidates")
    packed_bytes = route.get("packed_vector_bytes_read")
    if (type(packed_calls) is not int or type(packed_candidates) is not int
            or type(packed_bytes) is not int or not 0 < packed_calls <= packed_candidates
            or packed_candidates > route["fp32_score_calls"]
            or packed_bytes != packed_candidates * 768 * 4):
        raise EvidenceError("normalized-v4 timed client omitted packed FP32 work")
    if mode == "quantized_rerank":
        if (route.get("rerank_candidates") != 64
                or route.get("quantized_index_name") != native.QUANTIZED_PROFILE_NAME
                or route.get("quantized_codec") != "scalar_u8"
                or route.get("quantized_version") != 1
                or type(route.get("quantized_score_calls")) is not int
                or route["quantized_score_calls"] <= 0
                or route.get("quantized_code_bytes_read")
                    != route["quantized_score_calls"] * 768
                or packed_calls != 1 or packed_candidates > 64
                or route["fp32_score_calls"] != packed_candidates):
            raise EvidenceError("normalized-v4 timed SQ8 client omitted packed work")
    elif (any(type(route.get(field, "")) is not str or route.get(field, "") != ""
              for field in ("quantized_index_name", "quantized_codec"))
            or any(type(route.get(field, 0)) is not int or route.get(field, 0) != 0
                   for field in ("rerank_candidates", "quantized_version",
                                 "quantized_score_calls", "quantized_code_bytes_read"))):
        raise EvidenceError("normalized-v4 timed exact client crossed the SQ8 plane")


def _normalized_matrix_owner_identity(matrix, lanes):
    try:
        observed = native.v4_gate.immutable_owner_identity(lanes)
    except (AttributeError, TypeError, ValueError) as exc:
        raise EvidenceError("normalized-v4 production lanes cross graph publications") from exc
    if not native.same_json(matrix.get("immutable_owner_identity"), observed):
        raise EvidenceError("normalized-v4 immutable owner receipt is missing or inconsistent")
    return observed


def _normalized_ready_graph_owner(graph, owner, generation, label):
    base = (graph or {}).get("base_manifest") or {}
    current = (graph or {}).get("current_manifest") or {}
    if (not isinstance(graph, dict)
            or graph.get("index") != "embedding"
            or generation != owner.get("schema_generation")
            or graph.get("publication_present") is not True
            or graph.get("publication_unchanged") is not True
            or graph.get("serving_ready") is not True
            or graph.get("invalid") is not False
            or graph.get("reconciling") is not False
            or base.get("generation") != owner.get("base_manifest_generation")
            or base.get("checksum") != owner.get("base_manifest_checksum")
            or current.get("generation") != owner.get("current_manifest_generation")
            or current.get("checksum") != owner.get("current_manifest_checksum")
            or graph.get("current_coverage_lsn") != owner.get("current_coverage_lsn")):
        raise EvidenceError(f"normalized-v4 {label} differs from the timed serving owner")


def _normalized_lane_statistics(lane, lane_name, observations):
    if (not isinstance(lane, dict) or lane.get("query_count") != 200
            or lane.get("warmup_count") != 20 or lane.get("top_k") != 10
            or lane.get("ef_search") != 64):
        raise EvidenceError(f"normalized-v4 {lane_name} timing identity is invalid")
    result = {}
    for arm, mode in (("exact", "exact"), ("sq8", "quantized_rerank")):
        records = (lane.get(arm) or {}).get("repetitions")
        if not isinstance(records, list) or len(records) != 6:
            raise EvidenceError(f"normalized-v4 {lane_name}/{arm} lacks six repetitions")
        repetitions = []
        for ordinal, record in enumerate(records):
            calls, cpu, retained = (record.get("call_wall_nanos"),
                                    record.get("call_cpu_nanos"), record.get("observations"))
            expected_order = ordinal % 2 if arm == "exact" else 1 - ordinal % 2
            if (record.get("ordinal") != ordinal or record.get("arm_order") != expected_order
                    or not isinstance(calls, list) or len(calls) != 200
                    or not isinstance(cpu, list) or len(cpu) != 200
                    or any(type(value) is not int or value <= 0 for value in calls)
                    or any(type(value) is not int or value < 0 for value in cpu)
                    or not isinstance(retained, list) or len(retained) != 200
                    or [row.get("query") for row in retained] != list(range(200))):
                raise EvidenceError(f"normalized-v4 {lane_name}/{arm} raw repetition is invalid")
            for query, row in enumerate(retained):
                results = row.get("results")
                if (not isinstance(results, list) or len(results) != 10
                        or any(set(item) != {"id", "score"} for item in results)
                        or any(not isinstance(item["score"], (int, float))
                               or not math.isfinite(item["score"]) for item in results)
                        or [(item["id"], item["score"]) for item in results]
                            != observations.setdefault((mode, query), [
                                (item["id"], item["score"]) for item in results
                            ])):
                    raise EvidenceError("normalized-v4 timed results differ across lanes or repetitions")
                _normalized_production_identity(row.get("route"), mode, lane_name)
            wall = sum(calls)
            repetitions.append({
                "ordinal": ordinal, "arm_order": record["arm_order"],
                "qps": 200 * 1e9 / wall, "wall_ns_per_query": wall / 200,
                "cpu_ns_per_query": sum(cpu) / 200,
                "p50_ns": _normalized_nearest_rank(calls, .50),
                "p95_ns": _normalized_nearest_rank(calls, .95),
            })
        result[arm] = {
            "per_repetition": repetitions,
            "median": {field: statistics.median(row[field] for row in repetitions)
                       for field in ("qps", "wall_ns_per_query", "cpu_ns_per_query", "p50_ns", "p95_ns")},
        }
    return result


def _normalized_validate_matrix(matrix, diagnostic_fixed):
    if (matrix.get("schema") != "treedb_cohere_normalized_v4_production_matrix/v1"
            or matrix.get("rows") != 500000 or matrix.get("dimensions") != 768
            or matrix.get("query_order") != list(range(200))
            or matrix.get("top_k") != 10 or matrix.get("ef_search") != 64
            or matrix.get("rerank_candidates") != 64
            or matrix.get("representation") != native.NORMALIZED_REPRESENTATION
            or matrix.get("diagnostics") is not False or matrix.get("return_embedding") is not False):
        raise EvidenceError("normalized-v4 production matrix identity is invalid")
    lanes = matrix.get("lanes")
    required = {"go_native", "python_native", "collection_search", "collection_fetch", "service"}
    if not isinstance(lanes, dict) or set(lanes) != required:
        raise EvidenceError("normalized-v4 production matrix lane set is incomplete")
    owner = _normalized_matrix_owner_identity(matrix, lanes)
    for label in ("live_resource_before", "live_resource_after"):
        endpoint = matrix.get(label) or {}
        _normalized_ready_graph_owner(
            endpoint.get("typed_graph"), owner, endpoint.get("generation"), label,
        )
    observations, statistics_by_lane = {}, {}
    for lane_name in ("collection_search", "collection_fetch", "service", "go_native", "python_native"):
        statistics_by_lane[lane_name] = _normalized_lane_statistics(
            lanes[lane_name], lane_name, observations,
        )
    diagnostic = {
        (row["request_mode"], row["query"]): list(zip(row["ids"], row["scores"]))
        for row in diagnostic_fixed if row["eligible"] == 500000
    }
    if observations != diagnostic:
        raise EvidenceError("normalized-v4 production and diagnostic decisions differ")

    failures = []
    for lane_name in ("collection_search", "go_native", "python_native"):
        exact = statistics_by_lane[lane_name]["exact"]
        sq8 = statistics_by_lane[lane_name]["sq8"]
        checks = {
            "qps_gain_at_least_10pct": sq8["median"]["qps"] >= exact["median"]["qps"] * 1.10,
            "p50_lower": sq8["median"]["p50_ns"] < exact["median"]["p50_ns"],
            "p95_no_worse": sq8["median"]["p95_ns"] <= exact["median"]["p95_ns"],
            "majority_repetitions_faster": sum(
                sq8["per_repetition"][i]["qps"] > exact["per_repetition"][i]["qps"]
                for i in range(6)) >= 4,
        }
        failures.extend(f"{lane_name}: {name}" for name, passed in checks.items() if not passed)
    collection_exact = statistics_by_lane["collection_search"]["exact"]["median"]
    historical = {
        "qps_within_5pct": collection_exact["qps"] >= HISTORICAL_EXACT_QPS * .95,
        "p50_within_5pct": collection_exact["p50_ns"] <= HISTORICAL_EXACT_P50_NS * 1.05,
        "p95_within_10pct": collection_exact["p95_ns"] <= HISTORICAL_EXACT_P95_NS * 1.10,
    }
    failures.extend(f"historical exact: {name}" for name, passed in historical.items() if not passed)
    client_cpu = {}
    for lane_name in ("go_native", "python_native"):
        delta = (statistics_by_lane[lane_name]["sq8"]["median"]["cpu_ns_per_query"]
                 - statistics_by_lane[lane_name]["exact"]["median"]["cpu_ns_per_query"])
        client_cpu[lane_name] = delta
        if delta > 25_000:
            failures.append(f"{lane_name}: client CPU delta exceeds 25 us")
    advantage = (statistics_by_lane["collection_search"]["exact"]["median"]["wall_ns_per_query"]
                 - statistics_by_lane["collection_search"]["sq8"]["median"]["wall_ns_per_query"])
    transport = {}
    for lane_name in ("go_native", "python_native"):
        exact_overhead = (statistics_by_lane[lane_name]["exact"]["median"]["wall_ns_per_query"]
                          - statistics_by_lane["service"]["exact"]["median"]["wall_ns_per_query"])
        sq8_overhead = (statistics_by_lane[lane_name]["sq8"]["median"]["wall_ns_per_query"]
                        - statistics_by_lane["service"]["sq8"]["median"]["wall_ns_per_query"])
        consumed = sq8_overhead - exact_overhead
        transport[lane_name] = consumed
        if advantage <= 0 or max(0, consumed) > advantage / 2:
            failures.append(f"{lane_name}: transport consumes over half the collection advantage")
    return {
        "lanes": statistics_by_lane, "historical_exact": historical,
        "immutable_owner_identity": owner,
        "client_cpu_delta_ns": client_cpu,
        "collection_advantage_ns": advantage, "transport_consumption_ns": transport,
    }, failures


def _normalized_validate_engine(engine, expected_owner=None):
    if (engine.get("schema") != "treedb_cosine_normalized_f32_campaign_engine/v1"
            or engine.get("rows") != 500000 or engine.get("dimensions") != 768
            or engine.get("query_count") != 200 or engine.get("top_k") != 10
            or engine.get("ef_search") != 64 or engine.get("rerank_candidates") != 64
            or engine.get("representation") != native.NORMALIZED_REPRESENTATION
            or engine.get("index") != "minima_cohere"
            or engine.get("quantized_index") != native.QUANTIZED_PROFILE_NAME
            or type(engine.get("collection_generation")) is not int
            or engine["collection_generation"] <= 0
            or engine.get("stable_duplicate_score_calls") != 0):
        raise EvidenceError("normalized-v4 engine diagnostic identity is invalid")
    if expected_owner is not None:
        _normalized_ready_graph_owner(
            engine.get("serving_owner"), expected_owner,
            engine.get("collection_generation"), "engine serving owner",
        )
    shortlists = engine.get("shortlists")
    if not isinstance(shortlists, list) or len(shortlists) != 200:
        raise EvidenceError("normalized-v4 engine diagnostic lacks 200 actual shortlists")
    # resolve_normalized_inventory verifies the original file's SHA256. Keep
    # semantic checks here; do not invent another JSON serialization to hash.
    for query, row in enumerate(shortlists):
        ordinals = row.get("ordinals")
        work = row.get("candidate_work") or {}
        if (row.get("query") != query or not isinstance(ordinals, list) or len(ordinals) != 64
                or len(set(ordinals)) != 64
                or any(type(value) is not int or not 0 <= value < 500000 for value in ordinals)
                or type(work.get("quantized_score_calls")) is not int
                or work["quantized_score_calls"] <= 0
                or work.get("quantized_code_bytes_read")
                    != work["quantized_score_calls"] * 768
                or work.get("prepared_graph_search_views") != 1
                or row.get("packed_score_calls") != 1
                or row.get("packed_score_candidates") != 64
                or row.get("packed_vector_bytes_read") != 64 * 768 * 4):
            raise EvidenceError("normalized-v4 actual shortlist work is incomplete")
    summaries = {}
    for arm in ("candidate_only", "packed_same_shortlist"):
        rows = engine.get(arm)
        if not isinstance(rows, list) or len(rows) != 6:
            raise EvidenceError(f"normalized-v4 engine {arm} lacks six repetitions")
        expected_orders = [ordinal % 2 if arm == "candidate_only" else 1 - ordinal % 2
                           for ordinal in range(6)]
        if ([row.get("ordinal") for row in rows] != list(range(6))
                or [row.get("arm_order") for row in rows] != expected_orders
                or any(type(row.get("iterations")) is not int or row["iterations"] <= 0
                       or type(row.get("elapsed_nanos")) is not int or row["elapsed_nanos"] <= 0
                       or type(row.get("nanos_per_query")) is not int or row["nanos_per_query"] <= 0
                       or not isinstance(row.get("qps"), (int, float)) or not math.isfinite(row["qps"])
                       or abs(row["qps"] - 1e9 / row["nanos_per_query"]) > row["qps"] * 1e-12
                       or type(row.get("bytes_per_query")) is not int or row["bytes_per_query"] < 0
                       or type(row.get("allocs_per_query")) is not int or row["allocs_per_query"] < 0
                       for row in rows)):
            raise EvidenceError(f"normalized-v4 engine {arm} raw benchmark is invalid")
        summaries[arm] = {
            "median_ns": statistics.median(row["nanos_per_query"] for row in rows),
            "median_qps": statistics.median(row["qps"] for row in rows),
            "per_repetition": rows,
        }
    packed = summaries["packed_same_shortlist"]
    candidate = summaries["candidate_only"]
    checks = {
        "packed_at_most_10us": packed["median_ns"] <= 10_000,
        "packed_at_least_100k_qps": packed["median_qps"] >= 100_000,
        "packed_within_20pct_historical_qps": packed["median_qps"] >= HISTORICAL_PACKED_QPS * .80,
        "candidate_within_20pct_historical_latency": candidate["median_ns"] <= HISTORICAL_CANDIDATE_NS * 1.20,
    }
    return {"summaries": summaries, "checks": checks}, [
        f"engine: {name}" for name, passed in checks.items() if not passed
    ]


def _normalized_asset_inventory(inventory, mode):
    graph = inventory.get("typed_graph") or {}
    assets = graph.get("vector_assets")
    parts = graph.get("typed_column_parts")
    owned = inventory.get("owned_files") or {}
    categories = owned.get("category_bytes") or {}
    files = owned.get("files")
    process_memory = inventory.get("process_memory")
    go_memory = inventory.get("go_memory")
    if (inventory.get("schema") != "treedb_cohere_normalized_v4_resource_inventory/v1"
            or graph.get("publication_present") is not True
            or graph.get("serving_ready") is not True
            or graph.get("publication_unchanged") is not True
            or graph.get("invalid") is not False or graph.get("reconciling") is not False
            or graph.get("base_rows") != 500000
            or type(graph.get("suffix_rows")) is not int or graph["suffix_rows"] < 0
            or not isinstance(assets, list) or not isinstance(parts, list)
            or owned.get("schema") != "treedb_owned_db_file_inventory/v1"
            or set(categories) != {"column_assets", "command_wal", "value_log", "leaf_log", "other"}
            or owned.get("total_bytes") != sum(categories.values())
            or owned.get("total_bytes", 0) <= 0 or categories.get("command_wal", 0) <= 0
            or not isinstance(files, list) or not files
            or not isinstance(process_memory, dict) or not isinstance(go_memory, dict)
            or set(process_memory) != {"vmrss_bytes", "vmhwm_bytes", "vmsize_bytes", "vmpeak_bytes"}
            or any(type(value) is not int or value <= 0 for value in process_memory.values())):
        raise EvidenceError(f"normalized-v4 {mode} resource inventory is incomplete")
    seen_paths = set()
    file_categories = {name: 0 for name in categories}
    for row in files:
        if (not isinstance(row, dict) or set(row) != {"path", "bytes", "category"}
                or not isinstance(row["path"], str) or not row["path"]
                or row["path"].startswith("/") or ".." in Path(row["path"]).parts
                or row["path"] in seen_paths or row["category"] not in file_categories
                or type(row["bytes"]) is not int or row["bytes"] < 0):
            raise EvidenceError(f"normalized-v4 {mode} owned-file ledger is invalid")
        seen_paths.add(row["path"])
        file_categories[row["category"]] += row["bytes"]
    if file_categories != categories or sum(row["bytes"] for row in files) != owned["total_bytes"]:
        raise EvidenceError(f"normalized-v4 {mode} owned-file ledger does not reproduce totals")
    owned_by_path = {row["path"]: row for row in files}

    def owned_asset(asset):
        ref = asset.get("ref") or {}
        namespace = ref.get("Namespace")
        file_id = ref.get("FileID")
        offset = ref.get("Offset")
        length = ref.get("Length")
        if (not isinstance(namespace, str) or not namespace
                or type(file_id) is not int or file_id <= 0
                or type(offset) is not int or offset < 0
                or type(length) is not int or length <= 0
                or asset.get("bytes") != length):
            return False
        suffix = f"column_assets/{namespace}/assets/segments/segment-{file_id:06d}.tca"
        matches = [row for path, row in owned_by_path.items() if path.endswith(suffix)]
        return (len(matches) == 1 and matches[0]["category"] == "column_assets"
                and offset + length <= matches[0]["bytes"])

    if any(not isinstance(asset, dict) or not owned_asset(asset) for asset in assets):
        raise EvidenceError(f"normalized-v4 {mode} vector assets are not owned column assets")
    by_role = {}
    for asset in assets:
        by_role.setdefault(asset.get("role"), []).append(asset)
    normalized_rows = by_role.get("normalized_vectors", [])
    topology = by_role.get("hnsw_search_pack", [])
    if (len(normalized_rows) != 1 or len(topology) != 1
            or by_role.get("inverse_norm")
            or normalized_rows[0].get("asset_id") != native.NORMALIZED_REPRESENTATION
            or normalized_rows[0].get("physical_encoding") != "raw_float32_vector"
            or normalized_rows[0].get("rows") != 500000
            or normalized_rows[0].get("bytes", 0) <= 0
            or normalized_rows[0].get("logical_payload_bytes") != 500000 * 768 * 4
            or topology[0].get("asset_id") != "hnsw_topology_pack_v2"
            or topology[0].get("physical_encoding") != "hnsw_topology_pack_v2"
            or topology[0].get("rows") != 500000
            or any(asset.get("physical_encoding") == "raw_float32_vector"
                   for asset in assets if asset is not normalized_rows[0])
            or not any(part.get("ref") == normalized_rows[0].get("ref") for part in parts)):
        raise EvidenceError(f"normalized-v4 {mode} does not expose one canonical FP32 authority")
    codes = by_role.get("quantized_codes", [])
    alpha = by_role.get("quantized_alpha", [])
    if mode == "sq8":
        if (len(codes) != 1 or alpha
                or codes[0].get("asset_id") != "quantized/minima_sq8/codes"
                or codes[0].get("logical_type") != "byte_vector"
                or codes[0].get("rows") != 500000
                or codes[0].get("bytes", 0) <= 0
                or codes[0].get("logical_payload_bytes") != 500000 * 768
                or codes[0].get("physical_encoding") != "raw_fixed_bytes"):
            raise EvidenceError("normalized-v4 SQ8 inventory lacks its derived byte plane")
    elif codes or alpha:
        raise EvidenceError("normalized-v4 FP32-only inventory unexpectedly owns SQ8 assets")
    logical = graph.get("logical_resources") or {}
    physical = graph.get("physical") or {}
    zero_physical = (
        "descriptors_in_flight", "fallback_backings", "fallback_bytes",
        "fallback_bytes_in_flight", "fallback_segments", "mapped_bytes_in_flight",
    )
    if (logical.get("active_heap_copy_bytes") != 0
            or logical.get("fallback_reads") != 0
            or logical.get("active_mapped_bytes", 0) <= 0
            or physical.get("closed_db") is not False
            or physical.get("mapped_bytes", 0) <= 0
            or any(physical.get(field) != 0 for field in zero_physical)):
        raise EvidenceError(f"normalized-v4 {mode} inventory does not prove mapped one-plane serving")
    return {
        "total_owned_bytes": owned["total_bytes"],
        "category_bytes": owned["category_bytes"],
        "normalized_asset": normalized_rows[0], "topology_asset": topology[0],
        "quantized_assets": codes + alpha,
        "go_memory": inventory.get("go_memory"),
        "process_memory": inventory.get("process_memory"),
    }


def _normalized_validate_resources(artifact, mode, expected_owner=None):
    if (artifact.get("schema") != "treedb_cohere_normalized_v4_resource_inventories/v1"
            or artifact.get("rows") != 500000 or artifact.get("dimensions") != 768
            or artifact.get("query_mode") != ("quantized_rerank" if mode == "sq8" else "exact")
            or artifact.get("representation") != native.NORMALIZED_REPRESENTATION):
        raise EvidenceError(f"normalized-v4 {mode} resource artifact identity is invalid")
    inventories = artifact.get("inventories")
    phases = ["initial_ready", "pre_fold", "post_fold", "post_reopen", "final_verified"]
    if not isinstance(inventories, list) or [row.get("phase") for row in inventories] != phases:
        raise EvidenceError(f"normalized-v4 {mode} resource lifecycle is incomplete")
    suffixes = [(row.get("typed_graph") or {}).get("suffix_rows") for row in inventories]
    if not (suffixes[0] == 0 and suffixes[1] > 0 and suffixes[2:] == [0, 0, 0]):
        raise EvidenceError(f"normalized-v4 {mode} resource lifecycle suffix states are invalid")
    if expected_owner is not None:
        _normalized_ready_graph_owner(
            inventories[0].get("typed_graph"), expected_owner,
            inventories[0].get("generation"), f"{mode} initial-ready owner",
        )
    summaries = [_normalized_asset_inventory(row, mode) for row in inventories]
    return {"phases": phases, "inventories": summaries, "final": summaries[-1]}


def _normalized_disk_comparison(evidence, resources):
    totals = {mode: evidence[mode]["terminal"]["final_disk_bytes"] for mode in ("exact", "sq8")}
    quantized_physical = sum(asset.get("bytes", 0)
                             for asset in resources["sq8"]["final"]["quantized_assets"])
    delta = totals["sq8"] - totals["exact"]
    return {
        "final_disk_boundary": "post-clean-shutdown owned-directory bytes, including retained WAL",
        "final_disk_bytes": totals,
        "disk_delta_bytes": delta, "quantized_physical_bytes": quantized_physical,
        "disk_checks": {
            "sq8_delta_nonnegative": delta >= 0,
            "sq8_delta_explained_by_derived_plane": delta <= quantized_physical * 1.25 + (64 << 20),
        },
    }


def _normalized_phase_timings(events):
    phases = ("service_start", "schema_ensure", "initial_graph_build", "pre_close_fold",
              "close", "reopen", "idempotent_ensure", "reopen_graph_ensure",
              "verification_only_full_scroll")
    result = {}
    for phase in phases:
        rows = [row for row in events if row.get("event") == "call" and row.get("phase") == phase]
        if len(rows) != 1 or type(rows[0].get("duration_ns")) is not int or rows[0]["duration_ns"] <= 0:
            raise EvidenceError(f"normalized-v4 phase timing {phase} is unavailable")
        result[phase] = rows[0]["duration_ns"]
    return result


def _analyze_normalized(packet_path, packet, expected_sha256, *, analyzer_commit=None):
    consumer_source = validate_consumer_source(packet["candidate_commit"], analyzer_commit=analyzer_commit)
    paths = resolve_normalized_inventory(packet_path, packet)
    dataset_paths, manifest = _normalized_dataset(packet, paths)
    service = _normalized_file(packet, paths, "inputs", "treedb_service_binary")
    helper = _normalized_file(packet, paths, "inputs", "go_helper")
    go_builds = {
        "treedb_service_binary": _go_binary_build(
            service, packet["candidate_commit"], "github.com/snissn/gomap/cmd/treedb-document-service",
            require_trimpath=False,
        ),
        "go_helper": _go_binary_build(
            helper, packet["candidate_commit"], "github.com/snissn/gomap/TreeDB/cmd/treedb_v4_production_gate",
            require_trimpath=False,
        ),
    }
    plans = {
        mode: _normalized_validate_plan_and_receipt(packet, paths, mode, manifest)
        for mode in ("exact", "sq8")
    }
    producer_coordinates = _normalized_producer_coordinates(plans)
    source_truth = _full_truth(dataset_paths["truth"])
    source_truth_sha256 = packet["files"][packet["dataset"]["truth"]]["sha256"]
    truth_rows = {}
    for mode in ("exact", "sq8"):
        truth_rows[mode] = _normalized_truth_artifact(
            _normalized_run_path(packet, paths, mode, "truth"),
            source_truth, source_truth_sha256,
        )
    if not native.same_json(truth_rows["exact"][0], truth_rows["sq8"][0]):
        raise EvidenceError("normalized-v4 exact and SQ8 runs used different truth artifacts")
    _, canonical_truth, canonical_scores, original_truth = truth_rows["sq8"]
    normalized_vectors, normalized_queries = _normalized_recompute_canonical_truth(
        dataset_paths, canonical_truth, canonical_scores,
    )
    evidence = {
        mode: _normalized_validate_events(
            _normalized_run_path(packet, paths, mode, "events"), plans[mode], mode,
            canonical_truth, original_truth, normalized_vectors, normalized_queries,
        ) for mode in ("exact", "sq8")
    }
    del normalized_vectors, normalized_queries
    phase_quality = {}
    failures = []
    for phase in ("quality", "post_reopen_quality"):
        standalone_exact = evidence["exact"][phase]["exact"]
        same_build_exact = evidence["sq8"][phase]["exact"]
        sq8_quality = evidence["sq8"][phase]["quantized_rerank"]
        quality_checks = {
            "fp32_recall_at_least_0_90": same_build_exact >= .90,
            "sq8_recall_at_least_0_90": sq8_quality >= .90,
            "sq8_within_0_01_of_fp32": sq8_quality >= same_build_exact - .01,
            "fp32_only_consistent": abs(standalone_exact - same_build_exact) <= .01,
        }
        phase_quality[phase] = {
            "standalone_fp32_recall_at_10": standalone_exact,
            "same_build_fp32_recall_at_10": same_build_exact,
            "sq8_recall_at_10": sq8_quality,
            "checks": quality_checks,
        }
        failures.extend(f"{phase}: {name}" for name, passed in quality_checks.items() if not passed)

    matrix = read_json(
        _normalized_run_path(packet, paths, "sq8", "matrix"),
        "normalized-v4 production matrix", MAX_JSON_BYTES,
    )
    matrix_statistics, matrix_failures = _normalized_validate_matrix(matrix, evidence["sq8"]["fixed"])
    owner = matrix_statistics["immutable_owner_identity"]
    failures.extend(matrix_failures)
    engine = read_json(
        _normalized_run_path(packet, paths, "sq8", "engine"),
        "normalized-v4 engine diagnostic", MAX_JSON_BYTES,
    )
    engine_receipt = matrix.get("engine_diagnostic") or {}
    engine_entry = packet["files"][packet["runs"]["sq8"]["files"]["engine"]]
    if (not native.same_json(engine_receipt.get("artifact"), engine)
            or engine_receipt.get("sha256") != engine_entry["sha256"]
            or engine_receipt.get("bytes") != engine_entry["bytes"]
            or engine.get("source_commit") != packet["candidate_commit"]
            or engine.get("dataset_manifest_sha256")
                != packet["files"][packet["dataset"]["manifest"]]["sha256"]
            or engine.get("queries_sha256")
                != packet["files"][packet["dataset"]["queries"]]["sha256"]
            or engine.get("serving_sha256")
                != packet["files"][packet["inputs"]["serving"]]["sha256"]):
        raise EvidenceError("normalized-v4 matrix did not retain its exact engine diagnostic")
    engine_statistics, engine_failures = _normalized_validate_engine(engine, owner)
    failures.extend(engine_failures)

    resources = {
        mode: _normalized_validate_resources(read_json(
            _normalized_run_path(packet, paths, mode, "resources"),
            f"normalized-v4 {mode} resources", MAX_JSON_BYTES,
        ), mode, owner if mode == "sq8" else None) for mode in ("exact", "sq8")
    }
    disk = _normalized_disk_comparison(evidence, resources)
    failures.extend(f"disk: {name}" for name, passed in disk["disk_checks"].items() if not passed)
    exact_peak = max(row["process_memory"].get("vmhwm_bytes", 0)
                     for row in resources["exact"]["inventories"])
    sq8_peak = max(row["process_memory"].get("vmhwm_bytes", 0)
                   for row in resources["sq8"]["inventories"])
    rss_delta = sq8_peak - exact_peak
    rss_check = rss_delta <= 500000 * 768 * 1.25 + (256 << 20)
    if not rss_check:
        failures.append("RSS: SQ8 peak increase is not explained by the derived code plane")

    support = packet["runs"]["sq8"]["support"]
    required_support = {"service_cpu_pprof", "service_cpu_top", "profile_manifest", "service_stats"}
    if set(support) != required_support or packet["runs"]["exact"]["support"]:
        raise EvidenceError("normalized-v4 profile support inventory is incomplete")
    cpu_top = paths[support["service_cpu_top"]].read_text(encoding="utf-8")
    forbidden = (
        "decodeDenseV3ResultDocument", "denseV3EmbeddingScoreMatches",
        "documentservice.scoreEmbedding", "documentservice.(*Service).scanDocuments",
    )
    if any(symbol in cpu_top for symbol in forbidden):
        failures.append("profile: production service contains a full-document rescore symbol")
    profile_capture = matrix.get("profile_capture") or {}
    cpu_capture = (profile_capture.get("captures") or {}).get("cpu") or {}
    if (cpu_capture.get("status") != "captured"
            or cpu_capture.get("bytes")
                != packet["files"][support["service_cpu_pprof"]]["bytes"]
            or (matrix.get("profile_analysis") or {}).get("status") != "captured"):
        raise EvidenceError("normalized-v4 service profile capture is incomplete")

    qualified = not failures
    return {
        "schema": NORMALIZED_ANALYSIS_SCHEMA,
        "state": "qualified" if qualified else "valid_unqualified",
        "candidate_commit": packet["candidate_commit"],
        "packet_sha256": expected_sha256,
        "consumer_source": consumer_source,
        "consumer_runtime": _consumer_runtime_identity(),
        "producer_coordinates": producer_coordinates,
        "go_binary_builds": go_builds,
        "quality": {
            **phase_quality["quality"],
            "post_reopen": phase_quality["post_reopen_quality"],
            "original_cosine_reported": True,
        },
        "production_matrix": matrix_statistics,
        "engine": engine_statistics,
        "resources": {
            "exact": resources["exact"], "sq8": resources["sq8"],
            **disk, "peak_rss_delta_bytes": rss_delta,
            "rss_explained_by_derived_plane": rss_check,
        },
        "phase_timings_ns": {
            mode: _normalized_phase_timings(evidence[mode]["events"])
            for mode in ("exact", "sq8")
        },
        "final_state": {
            mode: evidence[mode]["full_state"] for mode in ("exact", "sq8")
        },
        "predecessors": packet["predecessors"],
        "failures": failures,
        "limitations": [
            "opt-in cosine_normalized_f32_v1 only; no default promotion is implied",
            "the frozen 200 Cohere queries are observed qualification queries, not an unseen holdout",
            "SQ8 storage is an accepted additive derived plane; canonical FP32 remains authoritative",
            "phase file/category inventories are live observations, not atomic or post-shutdown totals",
        ] + ([
            "untrimmed Go inputs are bound to frozen bytes and recorded build context; "
            "reproducibility across arbitrary source paths is not claimed",
        ] if any(build["build_settings"].get("-trimpath") != "true" for build in go_builds.values()) else []),
    }


def _analyze(packet_path, expected_sha256, validator_runner, *, analyzer_commit=None):
    packet_path, packet = load_packet(packet_path, expected_sha256)
    if packet["schema"] == NORMALIZED_PACKET_SCHEMA:
        return _analyze_normalized(packet_path, packet, expected_sha256, analyzer_commit=analyzer_commit)
    if analyzer_commit is not None:
        raise EvidenceError("analyzer commit override is supported only for normalized-v4 packets")
    consumer_source = validate_consumer_source(packet["candidate_commit"])
    paths = resolve_inventory(packet_path, packet)
    dataset_paths, _ = validate_dataset(packet, paths)
    go_builds = validate_go_inputs(packet, paths)
    validate_plans(packet, paths)
    baseline = validate_bounded_artifacts(packet, paths, validator_runner)
    validate_receipts(
        packet, paths, packet["files"][packet["dataset"]["manifest"]]["sha256"],
        _dataset_hashes(packet), baseline,
    )
    full_truth = _full_truth(dataset_paths["truth"])
    vectors = np.memmap(
        dataset_paths["documents"], mode="r", dtype="<f4", shape=(500000, 768),
    )
    queries = np.memmap(
        dataset_paths["queries"], mode="r", dtype="<f4", shape=(200, 768),
    )
    _, sq8_rss, _, fp32, three = validate_rss_and_comparisons(
        packet, paths, dataset_paths, full_truth, vectors, queries,
    )
    smoke_truth = _smoke_truth(dataset_paths)
    validate_smoke_log(paths[packet["arms"]["smoke_exact"]], "exact", packet, smoke_truth)
    validate_smoke_log(paths[packet["arms"]["smoke_sq8"]], "quantized_rerank", packet, smoke_truth)
    passed, statistics, final_state = validate_full_sq8_log(
        paths[packet["arms"]["full_sq8_events"]], packet,
        full_truth, sq8_rss, vectors, queries,
    )
    declared = packet["declared_quality_outcome"]
    if (declared == "pass") != passed:
        raise EvidenceError("declared quality outcome differs from independent ID/truth recomputation")
    tradeoff_accepted = validate_tradeoff_review(packet["tradeoff_review"], passed)
    assurance = ({
        "availability": "producer_attested",
        "scope": "producer_attested_exhaustive_semantic_final_state_verification",
        "producer_attested": True,
        "independently_recomputed": False,
        "digest_claim": False,
        "rows": final_state["rows"],
        "vectors_checked": final_state["vectors_checked"],
    } if final_state is not None else {
        "availability": "not_reached_after_frozen_quality_miss",
        "scope": "not_executed",
        "producer_attested": False,
        "independently_recomputed": False,
        "digest_claim": False,
        "rows": None,
        "vectors_checked": None,
    })
    qualified = passed and tradeoff_accepted
    reasons = []
    if not passed:
        reasons.append("frozen quality gate missed; no coordinate or graph retry is authorized")
    elif not tradeoff_accepted:
        reasons.append("material regression remains unaccepted by the issue owner")
    return {
        "schema": ANALYSIS_SCHEMA,
        "state": "qualified" if qualified else "valid_unqualified",
        "candidate_commit": packet["candidate_commit"],
        "consumer_source": consumer_source,
        "go_binary_builds": go_builds,
        "bounded_legacy_control": baseline,
        "packet_sha256": expected_sha256,
        "declared_quality_outcome": declared,
        "quality_recomputed_from_retained_ids": True,
        "paired_fp32_scores_recomputed_from_frozen_vectors": passed,
        "tradeoff_review": packet["tradeoff_review"],
        "paired_query_statistics": statistics,
        "fp32_comparison": fp32,
        "three_arm_comparison": three,
        "final_state_assurance": assurance,
        "limitations": [
            "bounded opt-in scalar-u8/v1 rerank diagnostic; no default promotion",
            "both fixed query sets are observed and neither is an unseen holdout",
            "smaller score codes do not imply removal of canonical FP32 storage",
            "the FP32/Qdrant result is a new mirror-bounded control, not a replay of #4672",
            ("the bounded native_runtime control reproduces the frozen #4617 recall failure; "
             "it contributes no lifecycle, latency, or representation-comparison claim"
             if baseline["classification"] == LEGACY_BASELINE_CLASSIFICATION else
             "the bounded native_runtime control completed cleanly but is not a "
             "representation-matched exact-versus-SQ8 comparison"),
        ],
        "reasons": reasons,
    }


def analyze(packet_path, expected_sha256, validator_runner=default_validator_runner, *, analyzer_commit=None):
    try:
        return _analyze(packet_path, expected_sha256, validator_runner, analyzer_commit=analyzer_commit)
    except Exception as exc:
        schema = ANALYSIS_SCHEMA
        try:
            with Path(packet_path).open("rb") as source:
                raw = source.read(MAX_PACKET_BYTES + 1)
            if len(raw) <= MAX_PACKET_BYTES:
                candidate = json.loads(raw)
                if isinstance(candidate, dict) and candidate.get("schema") == NORMALIZED_PACKET_SCHEMA:
                    schema = NORMALIZED_ANALYSIS_SCHEMA
        except (OSError, TypeError, ValueError):
            pass
        return {"schema": schema, "state": "invalid", "reasons": [str(exc)]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", required=True, type=Path)
    parser.add_argument("--expected-packet-sha256", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--analyzer-commit", help="reviewed consumer-only descendant; normalized-v4 only")
    args = parser.parse_args()
    result = analyze(args.packet, args.expected_packet_sha256, analyzer_commit=args.analyzer_commit)
    raw = json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() + b"\n"
    with args.output.open("xb") as output:
        output.write(raw)
    print(raw.decode().rstrip())
    return int(result["state"] == "invalid")


if __name__ == "__main__":
    sys.exit(main())
