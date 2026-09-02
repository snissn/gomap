#!/usr/bin/env python3
"""Fail-closed preflight and decision validator for issue #4587 C0."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import struct
import subprocess
import sys
from typing import Any

SCHEMA = "treedb-construction-policy-4587/v2"
RESULT_SCHEMA = "treedb-construction-policy-4587-results/v2"
MEASUREMENT_SCHEMA = "treedb-construction-policy-4587-measurements/v1"
ISOLATION_SCHEMA = "treedb-construction-policy-4587-isolation/v1"
COORDINATES = [128, 192, 256, 300]
CONTROL = 300
DRAFT_PATHS = {
    "docs/benchmarks/treedb_construction_policy_c0_4587.json",
    "scripts/treedb_construction_policy_4587.py",
    "scripts/test_treedb_construction_policy_4587.py",
    "scripts/treedb_vdbbench_search_existing_index.py",
}
HISTORY_PATH = "docs/benchmarks/treedb_construction_policy_history_2026-09-02.md"
WORK_KEYS = {
    "direct_exact_fp32_rows", "direct_exact_fp32_calls",
    "indexed_exact_fp32_rows", "indexed_exact_fp32_calls",
    "approximate_score_rows", "approximate_score_calls",
    "search_visited_candidates_by_layer", "diversity_candidates",
    "diversity_comparisons_requested", "diversity_comparisons_executed",
    "diversity_rejection_position_distribution", "reciprocal_prune_work",
    "worker_active_seconds", "dependency_barrier_wait_seconds",
    "goroutine_scheduler_seconds",
}
RESOURCE_KEYS = {
    "peak_rss_bytes", "peak_rss_anon_bytes", "peak_rss_file_bytes",
    "peak_mapped_bytes", "peak_live_heap_bytes", "cumulative_allocated_bytes",
    "graph_bytes", "search_pack_bytes", "persisted_bytes",
}
DETERMINISM_KEYS = {
    "graph_config_checksum", "small_repeat_checksum_a", "small_repeat_checksum_b",
    "tie_row_order_digest_a", "tie_row_order_digest_b",
}
MEASUREMENT_KEYS = {
    "schema_version", "phase_seconds", "cpu_utilization_logical_cores", "determinism",
    "diagnostic_work_profile", "resources", "projected_10m_adjacency_reduction_fraction",
}
RUN_KEYS = {
    "run_id", "scale", "role", "partition", "ef_construction",
    "execution_commit", "dataset", "configuration", "artifact",
    "isolation_evidence", "measurement_evidence", "search_evidence",
}
SEARCH_ORDER = [
    ("diagnostic", "exact"),
    ("production", "exact"),
    ("diagnostic", "scalar_u8_rerank"),
    ("production", "scalar_u8_rerank"),
]


def fail(message: str) -> None:
    raise ValueError(message)


def run(*argv: str, cwd: Path | None = None) -> str:
    result = subprocess.run(argv, cwd=cwd, check=True, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result.stdout.strip()


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


def positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        fail(f"{name} must be a positive integer")
    return value


def full_sha(value: Any, name: str, length: int = 40) -> str:
    if not isinstance(value, str) or re.fullmatch(rf"[0-9a-f]{{{length}}}", value) is None:
        fail(f"{name} must be a {length}-character lowercase hexadecimal digest")
    return value


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
    return {"canonical_queries": len(ids), "selection_rows": len(selection), "holdout_rows": len(holdout),
            "selection_digest": canonical_sha256(selection), "holdout_digest": canonical_sha256(holdout)}


def validate_contract(contract: dict[str, Any], allow_draft: bool) -> dict[str, Any]:
    exact(contract.get("schema_version"), SCHEMA, "schema_version")
    exact(contract.get("authority"), "FROZEN_AUTHORITATIVE", "authority")
    exact(contract.get("engineering_status"), "QUALIFIED", "engineering_status")
    exact(contract.get("execution_validity"), "AUTHORIZED_NOT_STARTED", "execution_validity")
    exact(contract.get("protocol_verdict"), "PROTOCOL_ACCEPTED", "protocol_verdict")
    exact(contract.get("stage"), "frozen", "stage")
    exact(contract.get("trial_started"), False, "trial_started")
    exact(contract.get("scope"), "C0_ONLY", "scope")
    source = object_at(contract.get("source_identity"), "source_identity")
    exact(source["definition_base_commit"], "6beea3ace082eee8afe5dccf629cc1a533823bfc", "definition base commit")
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
    exact(set(contract["experiment"]["required_metrics_per_run"]["diagnostic_work_profile"]), WORK_KEYS,
          "required diagnostic work metrics")
    exact(set(contract["experiment"]["required_metrics_per_run"]["resources"]), RESOURCE_KEYS,
          "required resource metrics")
    required = contract["experiment"]["required_metrics_per_run"]
    exact(required["cpu_utilization"], "cpu_utilization_logical_cores", "required CPU utilization metric")
    exact(set(required["determinism"]), DETERMINISM_KEYS, "required deterministic identity metrics")
    gomap = verify_git_identity(Path(source["gomap_root"]), source, allow_draft)
    external = verify_external_git(source)
    datasets = verify_datasets(contract)
    execution_authorized = not allow_draft and not gomap["dirty"]
    reason = (
        "clean frozen protocol is authorized for bounded C0 execution"
        if execution_authorized
        else "draft mode never authorizes execution"
    )
    return {"contract": "valid", "gomap": gomap, "vectordbbench": external, "datasets": datasets,
            "execution_authorized": execution_authorized, "reason": reason}


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


def validate_search_evidence(run_row: dict[str, Any], root: Path) -> dict[str, dict[str, float]]:
    rows = run_row.get("search_evidence")
    if not isinstance(rows, list) or len(rows) != len(SEARCH_ORDER):
        fail("run.search_evidence must contain exactly four ordered diagnostic/production rows")
    config = run_row["configuration"]
    production: dict[str, dict[str, float]] = {}
    identities: set[tuple[str, int, str]] = set()
    prior_timestamp = -math.inf
    for position, (entry, expected) in enumerate(zip(rows, SEARCH_ORDER, strict=True)):
        item = object_at(entry, f"search_evidence[{position}]")
        exact_keys(item, {"kind", "route", "result", "response", "index_metadata"},
                   f"search_evidence[{position}]")
        exact((item["kind"], item["route"]), expected, f"search_evidence[{position}] ordering")
        _, result_path = read_bound_json(root, item["result"], f"search_evidence[{position}].result")
        response, _ = read_bound_json(root, item["response"], f"search_evidence[{position}].response")
        metadata, _ = read_bound_json(root, item["index_metadata"], f"search_evidence[{position}].index_metadata")
        index_name = metadata.get("name")
        if not isinstance(index_name, str) or not index_name:
            fail("index metadata name must be non-empty")
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
            exact((stats.get("documents_fetched", 0), stats.get("document_bytes", 0)), (0, 0),
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
                "recall": nonnegative_number(metrics.get("recall"), f"search_evidence[{position}].recall"),
                "ndcg": nonnegative_number(metrics.get("ndcg"), f"search_evidence[{position}].ndcg"),
                "qps": nonnegative_number(metrics.get("qps"), f"search_evidence[{position}].qps"),
                "concurrent_p99_ms": 1000 * nonnegative_number(conc[0], f"search_evidence[{position}].p99"),
            }
    if len(identities) != 1:
        fail("diagnostic and production search rows do not bind one exact existing index identity")
    exact(set(production), {"exact", "scalar_u8_rerank"}, "production search routes")
    return production


def validate_manifest(run_row: dict[str, Any], root: Path, manifest: dict[str, Any], contract: dict[str, Any],
                      packet_commit: str, run_base_validator: bool) -> None:
    if run_base_validator:
        harness = Path(contract["source_identity"]["gomap_root"]) / "scripts/treedb_vectordbbench_artifact.py"
        result = subprocess.run([sys.executable, str(harness), "--validate-lifecycle", str(root)], text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            fail(f"base lifecycle validator rejected {root}: {result.stdout or result.stderr}")
    exact(manifest.get("schema_version"), "treedb-vectordbbench-artifact/v1", "artifact schema")
    exact(manifest.get("artifact_root"), str(root), "artifact root")
    context = object_at(manifest.get("context"), "manifest.context")
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
    full_sha(identity.get("service_binary_sha256"), "service binary SHA-256", 64)
    exact(identity.get("service_binary_sha256"), manifest["service"]["binary"]["sha256"], "binary identity")
    exact(identity.get("vectordbbench_commit"), contract["source_identity"]["vectordbbench"]["commit"],
          "lifecycle VectorDBBench commit")
    dataset = object_at(lifecycle.get("dataset"), "lifecycle.dataset")
    expected_dataset = run_row["dataset"]
    exact((dataset.get("name"), dataset.get("vectors"), dataset.get("dimensions"), dataset.get("sha256")),
          (expected_dataset["name"], expected_dataset["vectors"], expected_dataset["dimensions"],
           expected_dataset["train_sha256"]), "lifecycle dataset")
    harness_cfg = object_at(manifest.get("harness"), "manifest.harness")
    config = run_row["configuration"]
    exact((harness_cfg.get("m"), harness_cfg.get("ef_construction"), harness_cfg.get("ef_search"),
           harness_cfg.get("k"), harness_cfg.get("rerank_candidates"), harness_cfg.get("rows")),
          (config["m"], config["ef_construction"], config["ef_search"], config["top_k"],
           config["configured_rerank_candidates"], "scalar"), "lifecycle harness config")
    data_dir = Path(manifest["service"]["data_dir"]).resolve()
    exact(data_dir, (root / "treedb-data").resolve(), "fresh artifact-owned data root")


def validate_run(row: dict[str, Any], contract: dict[str, Any], packet_commit: str,
                 run_base_validator: bool) -> dict[str, Any]:
    exact_keys(row, RUN_KEYS, "run")
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
    root = Path(artifact["root"]).resolve()
    manifest_path = root / "manifest.json"
    full_sha(artifact["manifest_sha256"], "run.artifact.manifest_sha256", 64)
    exact(sha256_file(manifest_path), artifact["manifest_sha256"], "artifact manifest SHA-256")
    manifest = object_at(json.loads(manifest_path.read_text()), "manifest")
    validate_manifest(row, root, manifest, contract, packet_commit, run_base_validator)
    isolation, _ = read_bound_json(root, row["isolation_evidence"], "run.isolation_evidence")
    exact(isolation.get("schema_version"), ISOLATION_SCHEMA, "isolation schema")
    exact(isolation.get("artifact_root"), str(root), "isolation artifact root")
    exact(isolation.get("lock_path"), contract["experiment"]["isolation_and_noise"]["lock_path"], "isolation lock")
    exact(isolation.get("gomaxprocs"), 12, "isolation GOMAXPROCS")
    exact(isolation.get("competing_processes"), [], "competing process snapshot")
    swap = object_at(isolation.get("swap"), "isolation.swap")
    for key in ("before_used_bytes", "after_used_bytes", "peak_used_bytes"):
        exact(nonnegative_number(swap.get(key), f"isolation.swap.{key}"), 0.0, f"isolation.swap.{key}")
    measurements, _ = read_bound_json(root, row["measurement_evidence"], "run.measurement_evidence")
    exact(measurements.get("schema_version"), MEASUREMENT_SCHEMA, "measurement schema")
    exact_keys(measurements, MEASUREMENT_KEYS, "measurements")
    cpu_utilization = nonnegative_number(
        measurements.get("cpu_utilization_logical_cores"), "measurements.cpu_utilization_logical_cores")
    if cpu_utilization <= 0 or cpu_utilization > contract["source_identity"]["runtime"]["logical_cpus"]:
        fail("measurements.cpu_utilization_logical_cores must be in (0, logical_cpus]")
    determinism = object_at(measurements.get("determinism"), "measurements.determinism")
    exact_keys(determinism, DETERMINISM_KEYS, "measurements.determinism")
    for key, value in determinism.items():
        full_sha(value, f"measurements.determinism.{key}", 64)
    exact(determinism["graph_config_checksum"], canonical_sha256(row["configuration"]),
          "graph/config checksum binding")
    exact(determinism["small_repeat_checksum_a"], determinism["small_repeat_checksum_b"],
          "small-repeat graph determinism")
    exact(determinism["tie_row_order_digest_a"], determinism["tie_row_order_digest_b"],
          "small-repeat tie/row-order determinism")
    phases = object_at(measurements.get("phase_seconds"), "measurements.phase_seconds")
    for key in ("adjacency", "optimize"):
        if nonnegative_number(phases.get(key), f"phase_seconds.{key}") <= 0:
            fail(f"phase_seconds.{key} must be positive")
    work = object_at(measurements.get("diagnostic_work_profile"), "measurements.diagnostic_work_profile")
    exact(set(work), WORK_KEYS, "diagnostic work profile keys")
    for key, value in work.items():
        if key in {"search_visited_candidates_by_layer", "diversity_rejection_position_distribution"}:
            if not isinstance(value, (dict, list)) or not value:
                fail(f"diagnostic_work_profile.{key} must be non-empty structured evidence")
        else:
            nonnegative_number(value, f"diagnostic_work_profile.{key}")
    resources = object_at(measurements.get("resources"), "measurements.resources")
    exact(set(resources), RESOURCE_KEYS, "measurement resource keys")
    for key, value in resources.items():
        nonnegative_number(value, f"resources.{key}")
    projection = measurements.get("projected_10m_adjacency_reduction_fraction")
    if projection is not None:
        nonnegative_number(projection, "projected 10M adjacency reduction")
    production = validate_search_evidence(row, root)
    return {"row": row, "phases": phases, "resources": resources, "production": production,
            "projection": projection}


def clears_gates(candidate: dict[str, Any], control: dict[str, Any], require_projection: bool) -> bool:
    cand_exact, ctrl_exact = candidate["production"]["exact"], control["production"]["exact"]
    cand_prod, ctrl_prod = candidate["production"]["scalar_u8_rerank"], control["production"]["scalar_u8_rerank"]
    reduction = 1 - candidate["phases"]["adjacency"] / control["phases"]["adjacency"]
    checks = [
        reduction >= 0.30,
        ctrl_exact["recall"] - cand_exact["recall"] <= 0.002,
        ctrl_exact["ndcg"] - cand_exact["ndcg"] <= 0.002,
        ctrl_prod["recall"] - cand_prod["recall"] <= 0.002,
        ctrl_prod["ndcg"] - cand_prod["ndcg"] <= 0.002,
        cand_prod["qps"] >= 0.95 * ctrl_prod["qps"],
        cand_prod["concurrent_p99_ms"] <= 1.05 * ctrl_prod["concurrent_p99_ms"],
        candidate["resources"]["peak_rss_bytes"] <= control["resources"]["peak_rss_bytes"],
        candidate["resources"]["persisted_bytes"] <= control["resources"]["persisted_bytes"],
        candidate["resources"]["cumulative_allocated_bytes"] <= control["resources"]["cumulative_allocated_bytes"],
    ]
    if require_projection:
        checks.append(candidate["projection"] is not None and candidate["projection"] >= 0.30)
    return all(checks)


def validate_decision(packet: dict[str, Any], contract: dict[str, Any], *, run_base_validator: bool = True,
                      require_clean_head: bool = True) -> dict[str, Any]:
    exact_keys(packet, {"schema_version", "execution_commit", "contract_sha256", "verdict", "runs"}, "decision")
    exact(packet.get("schema_version"), RESULT_SCHEMA, "result schema_version")
    if packet.get("verdict") not in {"GO", "C0_NO_GO"}:
        fail("result verdict must be GO or C0_NO_GO")
    commit = full_sha(packet.get("execution_commit"), "decision.execution_commit")
    root = Path(contract["source_identity"]["gomap_root"])
    if require_clean_head:
        exact(run("git", "rev-parse", "HEAD", cwd=root), commit, "decision execution commit")
        exact(run("git", "status", "--porcelain=v1", cwd=root), "", "decision source cleanliness")
    exact(packet.get("contract_sha256"), canonical_sha256(contract), "decision contract SHA-256")
    runs = packet.get("runs")
    if not isinstance(runs, list):
        fail("result runs must be a list")
    validated = [validate_run(object_at(row, f"runs[{index}]"), contract, commit, run_base_validator)
                 for index, row in enumerate(runs)]
    roots = [item["row"]["artifact"]["root"] for item in validated]
    run_ids = [item["row"]["run_id"] for item in validated]
    if len(set(roots)) != len(roots) or len(set(run_ids)) != len(run_ids):
        fail("artifact roots and run IDs must be unique")
    screening = validated[:4]
    exact(len(screening), 4, "screening cardinality")
    exact([(item["row"]["scale"], item["row"]["ef_construction"], item["row"]["partition"], item["row"]["role"])
           for item in screening],
          [(250000, 128, "selection", "screening_candidate"),
           (250000, 192, "selection", "screening_candidate"),
           (250000, 256, "selection", "screening_candidate"),
           (250000, 300, "selection", "screening_control")], "screening cardinality and order")
    control = screening[-1]
    eligible = [item for item in screening[:-1] if clears_gates(item, control, False)]
    if not eligible:
        exact(len(validated), 4, "no-winner run cardinality")
        exact(packet["verdict"], "C0_NO_GO", "no-winner verdict")
        return {"verdict": "C0_NO_GO", "winner": None, "performance_gate": "NO-GO"}
    winner = sorted(eligible, key=lambda item: (
        -(1 - item["phases"]["adjacency"] / control["phases"]["adjacency"]),
        max(
            control["production"]["exact"]["recall"] - item["production"]["exact"]["recall"],
            control["production"]["exact"]["ndcg"] - item["production"]["exact"]["ndcg"],
            control["production"]["scalar_u8_rerank"]["recall"]
            - item["production"]["scalar_u8_rerank"]["recall"],
            control["production"]["scalar_u8_rerank"]["ndcg"]
            - item["production"]["scalar_u8_rerank"]["ndcg"],
        ),
        item["row"]["ef_construction"],
    ))[0]
    exact(len(validated), 6, "winner run cardinality")
    decision = validated[4:]
    exact([(item["row"]["scale"], item["row"]["ef_construction"], item["row"]["partition"], item["row"]["role"])
           for item in decision],
          [(1000000, 300, "holdout", "decision_control"),
           (1000000, winner["row"]["ef_construction"], "holdout", "decision_candidate")],
          "holdout cardinality and order")
    passed = clears_gates(decision[1], decision[0], True)
    exact(packet["verdict"], "GO" if passed else "C0_NO_GO", "computed verdict")
    return {"verdict": packet["verdict"], "winner": winner["row"]["ef_construction"],
            "performance_gate": "GO" if passed else "NO-GO"}


def print_screening_commands(contract: dict[str, Any]) -> None:
    template = contract["commands"]["lifecycle_harness_argv_template"]
    root = contract["commands"]["artifact_root"]
    dataset = contract["datasets"]["screening"]
    for ef in COORDINATES:
        values = {"artifact_root": f"{root}/screening-ef{ef}", "ef_construction": str(ef),
                  "dataset_dir": dataset["directory"], "dataset_name": dataset["name"],
                  "vectors": str(dataset["vectors"]), "index_prefix": f"treedb_4587_c0_250k_ef{ef}",
                  "db_label": f"treedb-4587-c0-250k-ef{ef}",
                  "case_name": f"cohere250k-selection-ef{ef}"}
        argv = [token.format(**values) for token in template]
        print("GOMAXPROCS=12 " + subprocess.list2cmdline(argv))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("contract", type=Path)
    parser.add_argument("--draft", action="store_true", help="allow only the uncommitted exact-byte review paths")
    parser.add_argument("--decision", type=Path)
    parser.add_argument("--print-screening-commands", action="store_true")
    args = parser.parse_args()
    try:
        contract = json.loads(args.contract.read_text())
        report = validate_contract(contract, args.draft)
        if args.print_screening_commands:
            print_screening_commands(contract)
        if args.decision:
            report["decision"] = validate_decision(json.loads(args.decision.read_text()), contract)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        print(f"INVALID: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
