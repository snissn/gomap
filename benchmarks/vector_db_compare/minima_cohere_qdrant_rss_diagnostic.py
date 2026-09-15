#!/usr/bin/env python3
"""Matched 500Kx768D Cohere query-ready RSS boundary for Qdrant and TreeDB."""
from __future__ import annotations

import argparse
import importlib.metadata
import inspect
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
import urllib.parse

import numpy as np

import minima_cohere_native_diagnostic as native
import minima_qdrant_runner as existing

SCHEMA = native.RSS_ARTIFACT_SCHEMA
CONTROLS = native.RSS_CONTROLS
RESOURCE_GUARD = {
    "poll_interval_s": 1,
    "wall_limit_s": 2700,
    "minimum_free_bytes": 10 << 30,
    "maximum_owned_bytes": 11 << 30,
    "maximum_combined_rss_bytes": 24 << 30,
}
QDRANT_CREDENTIAL_ENV = ("QDRANT_API_KEY", "QDRANT__SERVICE__API_KEY")


def qdrant_point(document):
    return {
        "logical_id": document["id"], "vector": document["embedding"],
        "payload": {"id": document["id"], "content": document["content"], "meta": document["meta"]},
    }


def ready_snapshot(snapshot, rows):
    schema = snapshot.get("payload_schema") or {}
    config = snapshot.get("config") or {}
    hnsw, optimizer, params = (config.get("hnsw_config") or {}, config.get("optimizer_config") or {},
                               config.get("params") or {})
    vector = params.get("vectors") or {}
    exact_int = lambda value, expected: type(value) is int and value == expected
    config_matches = not (any((schema.get(field) or {}).get("data_type") != "keyword"
            for field in ("meta.user_id", "meta.fpath"))
            or not exact_int(hnsw.get("m"), 16) or not exact_int(hnsw.get("ef_construct"), 100)
            or not exact_int(hnsw.get("full_scan_threshold"), 10000) or hnsw.get("on_disk") is not False
            or not exact_int(optimizer.get("indexing_threshold"), 10000)
            or not exact_int(optimizer.get("max_optimization_threads"), 1)
            or params.get("on_disk_payload") is not True or not exact_int(vector.get("size"), 768)
            or str(vector.get("distance", "")).lower() != "cosine" or vector.get("on_disk") is not False)
    return (config_matches and snapshot.get("status") == "green"
            and existing.optimizer_is_ok(snapshot.get("optimizer_status"))
            and exact_int(snapshot.get("points_count"), rows)
            and exact_int(snapshot.get("exact_points_count"), rows)
            and exact_int(snapshot.get("indexed_vectors_count"), rows)
            and all(exact_int((schema.get(field) or {}).get("points"), rows)
                    for field in ("meta.user_id", "meta.fpath")))


def validate_ready_snapshot(snapshot, rows):
    if not ready_snapshot(snapshot, rows):
        raise RuntimeError("Qdrant did not reach the matched query-ready boundary")


def _process_peak_rss_valid(rss):
    return native.process_peak_rss_valid(rss)


def _boundary_storage_valid(storage):
    return native.boundary_storage_valid(storage)


def _qdrant_resource_guard_valid(guard):
    def process_valid(process):
        return (
            isinstance(process, dict)
            and type(process.get("pid")) is int and process["pid"] > 0
            and native.linux_process_identity_valid(
                process.get("process_identity"), process["pid"],
            )
            and type(process.get("current_rss_bytes")) is int
            and process["current_rss_bytes"] > 0
        )

    def sample_valid(sample, server_required):
        server = sample.get("server") if isinstance(sample, dict) else None
        return (
            isinstance(sample, dict)
            and type(sample.get("elapsed_s")) in (int, float) and sample["elapsed_s"] >= 0
            and type(sample.get("free_bytes")) is int and sample["free_bytes"] >= 0
            and type(sample.get("owned_bytes")) is int and sample["owned_bytes"] >= 0
            and type(sample.get("combined_current_rss_bytes")) is int
            and sample["combined_current_rss_bytes"] > 0
            and process_valid(sample.get("harness"))
            and (process_valid(server) if server_required else server is None or process_valid(server))
            and sample["combined_current_rss_bytes"]
                == sample["harness"]["current_rss_bytes"] + (
                    0 if server is None else server["current_rss_bytes"])
        )

    try:
        limits = guard["limits"]
        first, last, cleanup = (
            guard["first_sample"], guard["last_sample"], guard["cleanup_sample"],
        )
        return (
            isinstance(guard, dict)
            and guard.get("schema") == "treedb_cohere_qdrant_resource_guard/v1"
            and guard.get("scope") == "harness_start_through_owned_process_cleanup"
            and guard.get("status") == "passed" and guard.get("failure") is None
            and native.same_json(limits, RESOURCE_GUARD)
            and type(guard.get("sample_count")) is int and guard["sample_count"] >= 3
            and type(guard.get("max_combined_current_rss_bytes")) is int
            and guard["max_combined_current_rss_bytes"] > 0
            and guard["max_combined_current_rss_bytes"] <= limits["maximum_combined_rss_bytes"]
            and type(guard.get("max_owned_bytes")) is int and guard["max_owned_bytes"] >= 0
            and guard["max_owned_bytes"] <= limits["maximum_owned_bytes"]
            and type(guard.get("minimum_free_bytes_observed")) is int
            and guard["minimum_free_bytes_observed"] >= limits["minimum_free_bytes"]
            and sample_valid(first, False) and first["server"] is None
            and sample_valid(last, True)
            and sample_valid(cleanup, False) and cleanup["server"] is None
            and first["elapsed_s"] <= last["elapsed_s"] <= cleanup["elapsed_s"]
            and len({first["harness"]["process_identity"],
                     last["harness"]["process_identity"],
                     cleanup["harness"]["process_identity"]}) == 1
            and guard["minimum_free_bytes_observed"] <= min(
                first["free_bytes"], last["free_bytes"], cleanup["free_bytes"])
            and cleanup["elapsed_s"] <= limits["wall_limit_s"]
            and guard["max_combined_current_rss_bytes"] >= max(
                first["combined_current_rss_bytes"], last["combined_current_rss_bytes"],
                cleanup["combined_current_rss_bytes"])
            and guard["max_owned_bytes"] >= max(
                first["owned_bytes"], last["owned_bytes"], cleanup["owned_bytes"])
        )
    except (KeyError, TypeError):
        return False


def _qdrant_cleanup_valid(cleanup):
    try:
        return (
            isinstance(cleanup, dict)
            and cleanup.get("schema") == "treedb_owned_qdrant_cleanup/v1"
            and cleanup.get("status") == "clean"
            and native.linux_process_identity_valid(cleanup.get("process_identity"))
            and type(cleanup.get("harness_pgid")) is int and cleanup["harness_pgid"] > 0
            and cleanup.get("server_pgid") == cleanup["harness_pgid"]
            and cleanup.get("term_sent") is True
            and cleanup.get("kill_sent") is False
            and cleanup.get("exit_code") == 0
            and cleanup.get("client_closed") is True
            and cleanup.get("log_closed") is True
            and cleanup.get("failure") is None
        )
    except (KeyError, TypeError, ValueError, IndexError):
        return False


def fp32_quality_valid(quality, control_name):
    common = {
        "selection_protocol", "target_mean_recall_at_10", "selected_control",
        "calibration", "revalidation", "control_name", "exact_mode",
    }
    expected = set(common)
    if control_name == "hnsw_ef":
        expected |= {"exact_correctness_reference_recall_at_10",
                     "exact_correctness_reference_timing"}
    try:
        return (
            control_name in ("ef_search", "hnsw_ef")
            and isinstance(quality, dict) and set(quality) == expected
            and quality["control_name"] == control_name and quality["exact_mode"] is False
            and native.fixed_set_quality_valid(quality)
            and (control_name != "hnsw_ef" or (
                native.same_json(quality["exact_correctness_reference_recall_at_10"], 1.0)
                and quality["exact_correctness_reference_timing"] == "after_rss_boundary"
            ))
        )
    except (KeyError, TypeError, ValueError):
        return False


def compare_artifacts(treedb, qdrant):
    reasons = []
    for artifact, backend in ((treedb, "treedb"), (qdrant, "qdrant")):
        if (artifact.get("schema") != SCHEMA or artifact.get("backend") != backend
                or artifact.get("state") != "calibrated" or artifact.get("reasons") != []):
            reasons.append(f"{backend} artifact is not calibrated")
        control_name = "ef_search" if backend == "treedb" else "hnsw_ef"
        if not fp32_quality_valid(artifact.get("quality"), control_name):
            reasons.append(f"{backend} fixed-set quality missed the target")
        if not native.rss_comparison_contract_valid(artifact.get("comparison_contract")):
            reasons.append(f"{backend} matched RSS contract is invalid")
        rss = artifact.get("rss", {})
        if not _process_peak_rss_valid(rss):
            reasons.append(f"{backend} server VmHWM is unavailable")
        if not _boundary_storage_valid(artifact.get("storage")):
            reasons.append(f"{backend} initial-ready storage boundary is unavailable")
        if backend == "qdrant":
            if not _qdrant_resource_guard_valid(artifact.get("resource_guard")):
                reasons.append("qdrant resource guard did not pass")
            if not _qdrant_cleanup_valid(artifact.get("cleanup")):
                reasons.append("qdrant owned cleanup did not complete cleanly")
            try:
                identities = {
                    artifact["rss"]["process_identity"],
                    artifact["provenance"]["process_identity"],
                    artifact["resource_guard"]["last_sample"]["server"]["process_identity"],
                    artifact["cleanup"]["process_identity"],
                }
                if len(identities) != 1:
                    raise ValueError("Qdrant evidence spans multiple process identities")
            except (KeyError, TypeError, ValueError):
                reasons.append("qdrant guard, RSS, provenance, and cleanup identities differ")
    if not native.same_json(treedb.get("comparison_contract"), qdrant.get("comparison_contract")):
        reasons.append("TreeDB and Qdrant comparison contracts differ")
    tree_provenance, qdrant_provenance = treedb.get("provenance", {}), qdrant.get("provenance", {})
    if (not tree_provenance.get("harness_commit")
            or tree_provenance.get("harness_commit") != qdrant_provenance.get("harness_commit")
            or not native.same_json(tree_provenance.get("harness_trees"),
                                    qdrant_provenance.get("harness_trees"))):
        reasons.append("TreeDB and Qdrant harness provenance differs")
    if treedb.get("rss", {}).get("process_identity") == qdrant.get("rss", {}).get("process_identity"):
        reasons.append("backend process identities are not distinct")
    if reasons:
        return {"state": "uncalibrated", "reasons": reasons}
    tree_bytes, qdrant_bytes = treedb["rss"]["bytes"], qdrant["rss"]["bytes"]
    accepted = tree_bytes <= qdrant_bytes
    return {
        "state": "accept" if accepted else "investigate", "treedb_rss_bytes": tree_bytes,
        "qdrant_rss_bytes": qdrant_bytes, "delta_bytes": tree_bytes - qdrant_bytes,
        "treedb_to_qdrant_ratio": tree_bytes / qdrant_bytes,
        "recommendation": ("stop prioritizing TreeDB RSS for this 500K x 768D initial-ready workload"
                           if accepted else "quantify the higher TreeDB owner before redesign"),
    }


def _sq8_artifact_reasons(sq8, fp32_treedb, fp32_qdrant):
    fp32_decisions = ((fp32_treedb.get("readiness") or {}).get("column_graph_build") or {}).get(
        "construction_decisions",
    )
    reasons = native.sq8_rss_artifact_reasons(sq8, {
        "comparison_contract": fp32_treedb.get("comparison_contract"),
        "provenance": fp32_treedb.get("provenance"),
        "construction_calibration_contract": fp32_treedb.get(
            "construction_calibration_contract"),
        "construction_decisions": fp32_decisions is not None,
    })
    if not native.same_json(
            sq8.get("comparison_contract"), fp32_qdrant.get("comparison_contract")):
        reasons.append("TreeDB SQ8 workload/RSS contract differs from Qdrant")
    rss_identity = sq8.get("rss", {}).get("process_identity")
    if rss_identity in {
        fp32_treedb.get("rss", {}).get("process_identity"),
        fp32_qdrant.get("rss", {}).get("process_identity"),
    }:
        reasons.append("TreeDB SQ8 process identity is not a fresh resource arm")
    return reasons


def compare_three_arms(fp32_treedb, fp32_qdrant, sq8_treedb):
    """Nest the frozen FP32 decision and add a non-promotional representation row."""
    fp32_decision = compare_artifacts(fp32_treedb, fp32_qdrant)
    reasons = _sq8_artifact_reasons(sq8_treedb, fp32_treedb, fp32_qdrant)
    if fp32_decision.get("state") == "uncalibrated":
        reasons.append("the nested FP32 comparison is uncalibrated")
    if reasons:
        sq8_row = {"state": "unavailable", "reasons": reasons}
    else:
        tree_bytes = sq8_treedb["rss"]["bytes"]
        qdrant_bytes = fp32_qdrant["rss"]["bytes"]
        sq8_row = {
            "state": "quality_matched_observation",
            "comparison_kind": "TreeDB_scalar_u8_rerank_vs_Qdrant_FP32",
            "claim_boundary": "different representations; does not revise the nested FP32 decision",
            "treedb_sq8_rss_bytes": tree_bytes,
            "qdrant_fp32_rss_bytes": qdrant_bytes,
            "delta_bytes": tree_bytes - qdrant_bytes,
            "treedb_sq8_to_qdrant_fp32_ratio": tree_bytes / qdrant_bytes,
        }
    return {
        "schema": "treedb_cohere_768_three_arm_comparison/v1",
        "state": "complete" if not reasons else "partial",
        "fp32_treedb_vs_fp32_qdrant": fp32_decision,
        "sq8_treedb_vs_fp32_qdrant": sq8_row,
    }


def comparison_contract(dataset_manifest_sha256, dataset_files_sha256, cpu_affinity):
    return native.rss_comparison_contract({
        "rows": 500000, "dimensions": 768, "top_k": 10, "batch_size": 256,
        "dataset_manifest_sha256": dataset_manifest_sha256,
        "dataset_files_sha256": dataset_files_sha256, "cpu_affinity": cpu_affinity,
        "host_memory_bytes": existing.memory_bytes(), "platform": native.platform.platform(),
        "treedb_go_runtime": {key: os.environ.get(key, "") for key in ("GOMAXPROCS", "GOGC", "GOMEMLIMIT")},
        "host_resource_identity": native.host_resource_identity(),
        "rss_recall_target": native.RSS_RECALL_TARGET,
        "rss_controls": native.RSS_CONTROLS,
        "rss_calibration_queries": native.RSS_CALIBRATION_QUERIES,
        "rss_revalidation_queries": native.RSS_REVALIDATION_QUERIES,
    })


def validate_imports(source):
    for imported in (native, existing):
        if not Path(inspect.getfile(imported)).resolve().is_relative_to(source / "benchmarks/vector_db_compare"):
            raise ValueError("imported RSS dependency is outside the frozen source tree")


def validate_storage_containment(run_dir, storage_path):
    run_dir, storage_path = Path(run_dir).resolve(), Path(storage_path).resolve()
    if storage_path == run_dir or not storage_path.is_relative_to(run_dir):
        raise ValueError("Qdrant storage must be a strict descendant of the owned run directory")
    return run_dir, storage_path


def prepare(args):
    source = Path(__file__).resolve().parents[2]
    validate_imports(source)
    harness = native.existing.repository_commit()
    dataset, _, files, query_count = native.dataset_identity(args.dataset, 500000)
    tree, tree_raw = native.strict_json_object(
        args.treedb_artifact, "TreeDB RSS artifact", native.EVIDENCE_JSON_MAX_BYTES,
    )
    binary = args.qdrant_bin.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError("Qdrant binary must be an executable file")
    parsed = urllib.parse.urlparse(args.url)
    if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.port is None:
        raise ValueError("Qdrant URL must name an explicit 127.0.0.1 HTTP port")
    run_dir, storage_path = validate_storage_containment(args.run_dir, args.storage_path)
    if storage_path.exists() or run_dir.exists():
        raise ValueError("Qdrant storage and run directories must not exist before the owned launch")
    if importlib.metadata.version("qdrant-client") != existing.CLIENT_VERSION:
        raise RuntimeError(f"qdrant-client must be exactly {existing.CLIENT_VERSION}")
    affinity = sorted(os.sched_getaffinity(0))
    root_tree = lambda path: subprocess.check_output(
        ["git", "rev-parse", "HEAD:" + path], cwd=source, text=True,
    ).strip()
    harness_trees = {path: root_tree(path) for path in (
        "benchmarks/vector_db_compare", "clients/python/treedb_client",
    )}
    contract = comparison_contract(native.digest(dataset / "manifest.json"), files, affinity)
    construction = tree.get("construction_calibration_contract", {})
    construction_ef = construction.get("ef_construction")
    if (tree.get("schema") != SCHEMA or tree.get("backend") != "treedb" or tree.get("state") != "calibrated"
            or tree.get("reasons") != []
            or not fp32_quality_valid(tree.get("quality"), "ef_search")
            or not native.same_json(tree.get("comparison_contract"), contract)
            or not native.rss_comparison_contract_valid(tree.get("comparison_contract"))
            or not _process_peak_rss_valid(tree.get("rss", {}))
            or not _boundary_storage_valid(tree.get("storage"))
            or type(construction_ef) is not int or construction_ef not in (32, 64, 96, 128)
            or not native.same_json(
                construction, native.construction_calibration_contract(construction_ef))
            or tree.get("provenance", {}).get("harness_commit") != harness
            or not native.same_json(tree.get("provenance", {}).get("harness_trees"), harness_trees)):
        raise RuntimeError("TreeDB RSS artifact quality, contract, or harness provenance is not current")
    plan = {
        "schema": "treedb_cohere_qdrant_rss_plan/v4_guarded", "harness_commit": harness,
        "harness_source_sha256": native.digest(Path(__file__)),
        "harness_trees": harness_trees,
        "dataset": str(dataset), "dataset_manifest_sha256": native.digest(dataset / "manifest.json"),
        "dataset_files_sha256": files, "treedb_artifact": str(args.treedb_artifact.resolve()),
        "treedb_artifact_sha256": native.bytes_digest(tree_raw),
        "comparison_contract": contract, "qdrant_bin": str(binary),
        "qdrant_bin_sha256": native.digest(binary), "qdrant_server_version": existing.SERVER_VERSION,
        "qdrant_client_version": existing.CLIENT_VERSION,
        "storage_path": str(storage_path), "url": args.url, "collection": args.collection,
        "run_dir": str(run_dir), "cpu_affinity": affinity,
        "batch_size": 256, "rows": 500000, "queries": query_count, "dimensions": 768, "top_k": 10,
        "controls": CONTROLS, "operation_timeout_s": args.operation_timeout,
        "startup_timeout_s": args.startup_timeout,
        "optimizer_timeout_s": args.optimizer_timeout, "poll_interval_s": args.poll_interval,
        "resource_guard": dict(RESOURCE_GUARD),
        "python": sys.version, "numpy": np.__version__, "platform": native.platform.platform(),
        "blas_threads": {key: os.environ.get(key, "")
                         for key in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS")},
        "production_hnsw": existing.PRODUCTION_HNSW_CONFIG,
        "production_optimizers": existing.PRODUCTION_OPTIMIZERS_CONFIG,
        "initial_upload_hnsw": existing.INITIAL_UPLOAD_HNSW_CONFIG,
        "initial_upload_optimizers": existing.INITIAL_UPLOAD_OPTIMIZERS_CONFIG,
    }
    sq8_path = getattr(args, "treedb_sq8_artifact", None)
    if sq8_path is not None:
        sq8, sq8_raw = native.strict_json_object(
            sq8_path, "TreeDB SQ8 artifact", native.EVIDENCE_JSON_MAX_BYTES,
        )
        if _sq8_artifact_reasons(sq8, tree, {
            "comparison_contract": contract, "provenance": tree["provenance"],
            "rss": {"process_identity": "pending-owned-qdrant-process"},
        }):
            raise RuntimeError("TreeDB SQ8 artifact is not a current complete representation arm")
        vectors = np.memmap(dataset / "documents.f32", mode="r", dtype="<f4", shape=(500000, 768))
        queries = np.memmap(dataset / "queries.f32", mode="r", dtype="<f4", shape=(query_count, 768))
        truth, _ = native.strict_json_object(
            dataset / "truth.json", "frozen truth", native.EVIDENCE_JSON_MAX_BYTES,
        )
        truth = truth["500000"]
        if not native.sq8_results_match_dataset(sq8, vectors, queries, truth):
            raise RuntimeError("TreeDB SQ8 results differ from frozen IDs, recall, or canonical FP32 scores")
        plan.update(
            schema="treedb_cohere_qdrant_rss_plan/v5_three_arm_guarded",
            treedb_sq8_artifact=str(sq8_path.resolve()),
            treedb_sq8_artifact_sha256=native.bytes_digest(sq8_raw),
        )
    return plan


def validate_plan(plan, expected):
    if native.canonical(plan) != native.canonical(expected):
        raise ValueError("frozen Qdrant RSS plan differs from current source/runtime/dataset")


class Run:
    optimization_snapshot = existing.QdrantMinimaRunner.optimization_snapshot
    server_log_snapshot = existing.QdrantMinimaRunner.server_log_snapshot

    def __init__(self, plan, client_factory, models):
        self.plan, self.client_factory, self.models = plan, client_factory, models
        self.client = self.process = self.process_identity = self.process_command_identity = None
        self.server_pid = self.server_pgid = None
        self.harness_identity = existing.linux_process_identity(os.getpid())
        self.harness_pgid = os.getpgid(os.getpid())
        if not self.harness_identity:
            raise RuntimeError("harness Linux process identity is unavailable")
        self.server_log = None
        self.output = Path(plan["run_dir"])
        self.output.mkdir(parents=True, exist_ok=False)
        self.collection = plan["collection"]
        self.operation_timeout = plan["operation_timeout_s"]
        self.optimizer_timeout = plan["optimizer_timeout_s"]
        self.poll_interval = plan["poll_interval_s"]
        self.storage_path = Path(plan["storage_path"])
        if self.storage_path == self.output or not self.storage_path.is_relative_to(self.output):
            raise RuntimeError("Qdrant storage escaped the owned run directory")
        self.resource_server_name = "Qdrant"
        self.server_log_path = self.output / "qdrant.log"
        self.cancel = threading.Event()
        self.resource_lock = threading.Lock()
        self.resource_failure = None
        self.started = None
        self.guard_summary = {
            "schema": "treedb_cohere_qdrant_resource_guard/v1",
            "scope": "harness_start_through_owned_process_cleanup",
            "status": "running", "sample_count": 0,
            "max_combined_current_rss_bytes": 0, "max_owned_bytes": 0,
            "minimum_free_bytes_observed": None,
            "first_sample": None, "last_sample": None, "cleanup_sample": None,
            "failure": None,
            "limits": dict(plan["resource_guard"]),
        }
        self.guard_termination_sent = False
        self.cleaning_up = False
        self.config = {"scalar_fields": ["meta.user_id", "meta.fpath"]}
        self.readiness_evidence = []
        data = Path(plan["dataset"])
        self.vectors = np.memmap(data / "documents.f32", mode="r", dtype="<f4", shape=(500000, 768))
        self.queries = np.memmap(
            data / "queries.f32", mode="r", dtype="<f4", shape=(plan["queries"], 768),
        )
        truth, _ = native.strict_json_object(
            data / "truth.json", "frozen truth", native.EVIDENCE_JSON_MAX_BYTES,
        )
        self.truth = truth["500000"]

    @staticmethod
    def current_rss(pid, expected_identity):
        if type(pid) is not int or pid <= 0 or not expected_identity:
            raise RuntimeError("resource sample lacks an owned process identity")
        before = existing.linux_process_identity(pid)
        if before != expected_identity:
            raise RuntimeError("resource sample process identity changed before /proc read")
        try:
            status = Path(f"/proc/{pid}/status").read_text(encoding="utf-8")
        except OSError as exc:
            raise RuntimeError("resource sample /proc status is unavailable") from exc
        after = existing.linux_process_identity(pid)
        if after != before:
            raise RuntimeError("resource sample process identity changed across /proc read")
        for line in status.splitlines():
            if line.startswith("VmRSS:"):
                _, value, unit = line.split()
                if unit == "kB" and int(value) >= 0:
                    return {"pid": pid, "process_identity": before, "current_rss_bytes": int(value) * 1024}
        raise RuntimeError("resource sample current RSS is unavailable")

    def check_resources(self):
        if self.started is None:
            return
        with self.resource_lock:
            harness = self.current_rss(os.getpid(), self.harness_identity)
            server = None
            if self.process is not None:
                if self.process.poll() is not None:
                    if not self.cleaning_up:
                        raise RuntimeError(
                            f"owned Qdrant exited during resource monitoring with {self.process.returncode}",
                        )
                else:
                    try:
                        server = self.current_rss(self.process.pid, self.process_identity)
                    except RuntimeError:
                        # TERM may land between the identity/read/identity checks.
                        # That is expected only while this owner is synchronously
                        # cleaning up; poll must prove the exact child has exited.
                        if not self.cleaning_up or self.process.poll() is None:
                            raise
            owned_bytes = existing.disk_bytes(self.output)
            free_bytes = shutil.disk_usage(self.output).free
            combined_rss = harness["current_rss_bytes"] + (
                server["current_rss_bytes"] if server is not None else 0)
            elapsed = time.monotonic() - self.started
            sample = {
                "elapsed_s": elapsed, "free_bytes": free_bytes, "owned_bytes": owned_bytes,
                "combined_current_rss_bytes": combined_rss,
                "harness": harness, "server": server,
            }
            summary = self.guard_summary
            summary["sample_count"] += 1
            summary["max_combined_current_rss_bytes"] = max(
                summary["max_combined_current_rss_bytes"], combined_rss)
            summary["max_owned_bytes"] = max(summary["max_owned_bytes"], owned_bytes)
            observed_free = summary["minimum_free_bytes_observed"]
            summary["minimum_free_bytes_observed"] = (
                free_bytes if observed_free is None else min(observed_free, free_bytes)
            )
            summary["first_sample"] = summary["first_sample"] or sample
            if self.cleaning_up:
                summary["cleanup_sample"] = sample
            else:
                summary["last_sample"] = sample
            guard = self.plan["resource_guard"]
            if (free_bytes < guard["minimum_free_bytes"]
                    or owned_bytes > guard["maximum_owned_bytes"]
                    or combined_rss > guard["maximum_combined_rss_bytes"]
                    or elapsed > guard["wall_limit_s"]):
                raise RuntimeError("frozen Qdrant disk/RAM/wall budget exceeded")

    def guard_resources(self):
        while not self.cancel.wait(self.plan["resource_guard"]["poll_interval_s"]):
            if self.resource_failure is None:
                try:
                    self.check_resources()
                except BaseException as exc:
                    self.resource_failure = f"resource guard: {type(exc).__name__}: {exc}"
                    self.guard_summary["failure"] = self.resource_failure
            if self.resource_failure:
                process = self.process
                if (process is not None and process.poll() is None and self.process_identity
                        and existing.linux_process_identity(process.pid) == self.process_identity
                        and os.getpgid(process.pid) == self.harness_pgid):
                    process.terminate()
                    self.guard_termination_sent = True
                return

    def raise_resource_failure(self):
        if self.resource_failure:
            raise RuntimeError(self.resource_failure)

    def wait_ready(self, rows, phase, production=True):
        deadline = time.monotonic() + self.optimizer_timeout
        while True:
            saved_timeout = self.optimizer_timeout
            self.optimizer_timeout = max(0, deadline - time.monotonic())
            try:
                existing.QdrantMinimaRunner.wait_ready(self, expected_count=rows, phase=phase)
            finally:
                self.optimizer_timeout = saved_timeout
            snapshot = self.readiness_evidence[-1]["snapshots"][-1]
            if not production or ready_snapshot(snapshot, rows):
                return snapshot
            if time.monotonic() >= deadline:
                raise TimeoutError("Qdrant full query-ready boundary exceeded optimizer timeout")
            time.sleep(self.poll_interval)

    def start_server(self):
        if self.storage_path.exists():
            raise RuntimeError("owned Qdrant storage path already exists")
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self.server_log = (self.output / "qdrant.log").open("xb")
        parsed = urllib.parse.urlparse(self.plan["url"])
        env = {key: os.environ[key] for key in ("HOME", "PATH", "TMPDIR", "TZ") if key in os.environ}
        env.update(QDRANT__SERVICE__HOST="127.0.0.1", QDRANT__SERVICE__HTTP_PORT=str(parsed.port),
                   QDRANT__STORAGE__STORAGE_PATH=str(self.storage_path))
        with self.resource_lock:
            process = subprocess.Popen([self.plan["qdrant_bin"]], stdin=subprocess.DEVNULL,
                                       stdout=self.server_log, stderr=subprocess.STDOUT,
                                       cwd=self.output, env=env)
            try:
                process_identity = existing.linux_process_identity(process.pid)
                if not process_identity:
                    raise RuntimeError(
                        "owned Qdrant process identity is unavailable immediately after launch",
                    )
                server_pgid = os.getpgid(process.pid)
                if server_pgid != self.harness_pgid:
                    raise RuntimeError("owned Qdrant did not remain in the harness process group")
                process_command_identity = existing.server_process_identity(process.pid)
            except BaseException:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=10)
                raise
            self.process, self.server_pid = process, process.pid
            self.process_identity, self.server_pgid = process_identity, server_pgid
            self.process_command_identity = process_command_identity
        deadline, last = time.monotonic() + self.plan["startup_timeout_s"], None
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"owned Qdrant exited during startup with {self.process.returncode}")
            try:
                info = existing.server_info(self.plan["url"], "")
                if (existing.linux_process_identity(self.server_pid) == self.process_identity
                        and os.getpgid(self.server_pid) == self.harness_pgid
                        and info.get("version") == self.plan["qdrant_server_version"]
                        and Path(f"/proc/{self.server_pid}/exe").resolve() == Path(self.plan["qdrant_bin"])
                        and existing.server_process_owns_endpoint(self.server_pid, self.plan["url"])):
                    if existing.server_process_identity(self.server_pid) != self.process_command_identity:
                        raise RuntimeError("owned Qdrant command identity changed during startup")
                    self.client = self.client_factory()
                    return
            except Exception as exc:
                last = exc
            time.sleep(self.plan["poll_interval_s"])
        raise TimeoutError(f"owned Qdrant startup exceeded timeout: {last}")

    def cleanup_owned(self):
        cleanup = {
            "schema": "treedb_owned_qdrant_cleanup/v1", "status": "failed",
            "process_identity": self.process_identity or "",
            "harness_pgid": self.harness_pgid, "server_pgid": self.server_pgid,
            "term_sent": False, "kill_sent": False, "exit_code": None,
            "client_closed": False, "log_closed": False, "failure": None,
        }
        failures = []
        try:
            if self.client is not None:
                self.client.close()
            cleanup["client_closed"] = True
        except BaseException as exc:
            failures.append(f"client close: {type(exc).__name__}: {exc}")
        try:
            if self.process is None:
                failures.append("owned Qdrant was never launched")
            elif self.process.poll() is not None:
                cleanup["exit_code"] = self.process.returncode
                failures.append(f"owned Qdrant exited before cleanup with {self.process.returncode}")
            elif (not self.process_identity
                    or existing.linux_process_identity(self.server_pid) != self.process_identity
                    or os.getpgid(self.server_pid) != self.harness_pgid):
                failures.append("owned Qdrant identity or process group changed; refusing to signal")
            else:
                self.process.terminate()
                cleanup["term_sent"] = True
                try:
                    self.process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    if (existing.linux_process_identity(self.server_pid) != self.process_identity
                            or os.getpgid(self.server_pid) != self.harness_pgid):
                        failures.append("owned Qdrant identity changed; refusing forced shutdown")
                    else:
                        self.process.kill()
                        cleanup["kill_sent"] = True
                        self.process.wait(timeout=10)
                        failures.append("owned Qdrant required forced shutdown")
                cleanup["exit_code"] = self.process.returncode
                if self.process.returncode != 0:
                    failures.append(f"owned Qdrant shutdown exited with {self.process.returncode}")
        except BaseException as exc:
            failures.append(f"server cleanup: {type(exc).__name__}: {exc}")
        finally:
            try:
                if self.server_log is not None:
                    self.server_log.close()
                cleanup["log_closed"] = True
            except BaseException as exc:
                failures.append(f"server log close: {type(exc).__name__}: {exc}")
        cleanup["failure"] = "; ".join(failures) if failures else None
        if not failures:
            cleanup["status"] = "clean"
        return cleanup

    def stop_server(self):
        """Compatibility entry point retained for focused ownership tests."""
        cleanup = self.cleanup_owned()
        if cleanup["status"] != "clean":
            raise RuntimeError(cleanup["failure"])

    def validate_fresh(self):
        identity = existing.linux_process_identity(self.server_pid)
        collections = getattr(self.client.get_collections(), "collections", None)
        collection_dir = self.storage_path / "collections"
        if (identity != self.process_identity or os.getpgid(self.server_pid) != self.harness_pgid
                or sorted(os.sched_getaffinity(self.server_pid)) != self.plan["cpu_affinity"]
                or collections is None or collections or (collection_dir.exists() and any(collection_dir.iterdir()))):
            raise RuntimeError("Qdrant process/backend is not fresh or changed before load")
        peak = native.process_peak_at_boundary(self.server_pid, identity, self.plan["cpu_affinity"])
        if peak.get("availability") != "measured":
            raise RuntimeError("fresh Qdrant process VmHWM is unavailable")
        return peak

    def create(self):
        self.client.create_collection(
            collection_name=self.collection,
            vectors_config=self.models.VectorParams(size=768, distance=self.models.Distance.COSINE, on_disk=False),
            hnsw_config=self.models.HnswConfigDiff(**self.plan["initial_upload_hnsw"]),
            optimizers_config=self.models.OptimizersConfigDiff(**self.plan["initial_upload_optimizers"]),
            on_disk_payload=True,
            timeout=self.operation_timeout,
        )
        for field in self.config["scalar_fields"]:
            self.client.create_payload_index(collection_name=self.collection, field_name=field,
                field_schema=self.models.PayloadSchemaType.KEYWORD, wait=True, timeout=self.operation_timeout)
        self.wait_ready(0, "fresh_collection", production=False)

    def load(self):
        for start in range(0, self.plan["rows"], self.plan["batch_size"]):
            points = []
            for row in range(start, min(start + self.plan["batch_size"], self.plan["rows"])):
                document = native.make_document(self.vectors, row, self.plan["rows"])
                value = qdrant_point(document)
                points.append(self.models.PointStruct(
                    id=existing.point_id(value["logical_id"]), vector=value["vector"], payload=value["payload"],
                ))
            self.client.upsert(collection_name=self.collection, points=points, wait=True,
                               timeout=self.operation_timeout)
        self.client.update_collection(
            collection_name=self.collection,
            hnsw_config=self.models.HnswConfigDiff(**self.plan["production_hnsw"]),
            optimizers_config=self.models.OptimizersConfigDiff(**self.plan["production_optimizers"]),
            timeout=self.operation_timeout,
        )
        return self.wait_ready(self.plan["rows"], "initial_load_to_ann_ready")

    def search(self, control, query, exact=False):
        response = self.client.query_points(
            collection_name=self.collection, query=self.queries[query].tolist(), limit=10,
            with_payload=True, with_vectors=False,
            search_params=self.models.SearchParams(hnsw_ef=control, exact=exact),
            timeout=self.operation_timeout,
        )
        ids = []
        for point in getattr(response, "points", response):
            payload = getattr(point, "payload", None) or {}
            if (not isinstance(payload.get("id"), str) or not isinstance(payload.get("content"), str)
                    or set((payload.get("meta") or {})) != {"user_id", "fpath"}):
                raise RuntimeError("Qdrant query returned a mismatched logical document")
            try:
                row = int(payload["id"].removeprefix("row-"))
                expected = native.make_document(self.vectors, row, self.plan["rows"])
            except (IndexError, ValueError):
                raise RuntimeError("Qdrant query returned an invalid logical ID") from None
            if payload["id"] != expected["id"] or payload["content"] != expected["content"] or payload["meta"] != expected["meta"]:
                raise RuntimeError("Qdrant query returned mismatched logical payload values")
            ids.append(payload["id"])
        return ids

    def execute(self):
        artifact, tree, monitor = None, None, None
        storage = {"availability": "unavailable", "owned_bytes_at_rss_boundary": None,
                   "scope": "backend_owned_directory_at_initial_ready_quality_boundary"}
        try:
            self.started = time.monotonic()
            self.check_resources()
            monitor = threading.Thread(target=self.guard_resources, daemon=True)
            monitor.start()
            self.start_server()
            baseline = self.validate_fresh()
            tree, tree_raw = native.strict_json_object(
                self.plan["treedb_artifact"], "TreeDB RSS artifact", native.EVIDENCE_JSON_MAX_BYTES,
            )
            if native.bytes_digest(tree_raw) != self.plan["treedb_artifact_sha256"]:
                raise RuntimeError("TreeDB RSS artifact changed after freeze")
            if not native.same_json(tree.get("comparison_contract"), self.plan["comparison_contract"]):
                raise RuntimeError("TreeDB RSS artifact does not match the Qdrant plan")
            self.create()
            readiness = self.load()
            self.raise_resource_failure()
            quality_requests = []

            def quality_search(control, query):
                ids = self.search(control, query)
                quality_requests.append({
                    "request_sequence": len(quality_requests) + 1,
                    "control": control, "query": query, "ids": ids,
                })
                return ids

            quality = native.calibrate_ann_control(
                quality_search, self.truth, self.plan["controls"],
                native.RSS_CALIBRATION_QUERIES, native.RSS_REVALIDATION_QUERIES, native.RSS_RECALL_TARGET,
            )
            rss = native.process_peak_at_boundary(
                self.server_pid, self.process_identity, self.plan["cpu_affinity"],
            )
            storage = {
                "availability": "measured",
                "owned_bytes_at_rss_boundary": existing.disk_bytes(self.storage_path),
                "scope": "backend_owned_directory_at_initial_ready_quality_boundary",
            }
            # Keep the exact correctness reference outside the sampled ANN RSS boundary.
            exact_ids = self.search(self.plan["controls"][-1], 0, exact=True)
            self.raise_resource_failure()
            if exact_ids != self.truth[0]:
                raise RuntimeError("Qdrant exact correctness reference differs from exhaustive truth")
            reasons = []
            if not quality["revalidation"]["passed"]:
                reasons.append("no Qdrant hnsw_ef passed both fixed recall query sets")
            if rss.get("availability") != "measured":
                reasons.append("Qdrant server VmHWM unavailable or process drifted")
            if (existing.server_process_identity(self.server_pid) != self.process_command_identity
                    or Path(f"/proc/{self.server_pid}/exe").resolve() != Path(self.plan["qdrant_bin"])
                    or not existing.server_process_owns_endpoint(self.server_pid, self.plan["url"])):
                reasons.append("Qdrant binary, command, or listener changed before result finalization")
            artifact = {
                "schema": SCHEMA, "state": "calibrated" if not reasons else "uncalibrated",
                "backend": "qdrant", "comparison_contract": self.plan["comparison_contract"],
                "quality": {**quality, "control_name": "hnsw_ef", "exact_mode": False,
                            "exact_correctness_reference_recall_at_10": 1.0,
                            "exact_correctness_reference_timing": "after_rss_boundary"},
                "observed_quality": {
                    "schema": "cohere_500k_768d_quality_observations/v1",
                    "control_name": "hnsw_ef", "requests": quality_requests,
                    "exact_reference": {"query": 0, "ids": exact_ids},
                },
                "rss": rss, "storage": storage, "reasons": reasons, "readiness": readiness,
                "fresh_start_peak_rss": baseline,
                "durability_visibility": "Qdrant 1.19.0 upsert wait=true",
                "physical_id_mapping": "uuid5(logical row-*); payload id preserves the matched logical ID",
                "configuration": {key: self.plan[key] for key in (
                    "production_hnsw", "production_optimizers", "initial_upload_hnsw", "initial_upload_optimizers",
                )},
                "provenance": {key: self.plan[key] for key in (
                    "harness_commit", "harness_source_sha256", "harness_trees", "qdrant_bin_sha256",
                    "qdrant_server_version", "qdrant_client_version", "dataset_manifest_sha256",
                    "dataset_files_sha256", "treedb_artifact_sha256",
                )},
            }
            if "treedb_sq8_artifact_sha256" in self.plan:
                artifact["provenance"]["treedb_sq8_artifact_sha256"] = (
                    self.plan["treedb_sq8_artifact_sha256"]
                )
            artifact["provenance"].update(process_identity=self.process_identity,
                                          process_command_identity=self.process_command_identity)
        except BaseException as exc:
            reason = f"{type(exc).__name__}: {exc}"
            artifact = artifact or {"schema": SCHEMA, "backend": "qdrant",
                                    "comparison_contract": self.plan["comparison_contract"],
                                    "quality": {"revalidation": {"passed": False}},
                                    "rss": {"availability": "unavailable", "bytes": None,
                                            "process_identity": self.process_identity or ""},
                                    "storage": storage,
                                    "readiness": self.readiness_evidence[-1] if self.readiness_evidence else {}}
            artifact["state"] = "uncalibrated"
            artifact.setdefault("reasons", []).append(reason)
        finally:
            if self.resource_failure is None:
                try:
                    self.check_resources()
                except BaseException as exc:
                    self.resource_failure = f"final resource guard: {type(exc).__name__}: {exc}"
                    self.guard_summary["failure"] = self.resource_failure
            self.cleaning_up = True
            cleanup = self.cleanup_owned()
            try:
                self.check_resources()
            except BaseException as exc:
                if self.resource_failure is None:
                    self.resource_failure = f"cleanup resource guard: {type(exc).__name__}: {exc}"
                    self.guard_summary["failure"] = self.resource_failure
            self.cancel.set()
            if monitor is not None:
                monitor.join(timeout=5)
                if monitor.is_alive() and self.resource_failure is None:
                    self.resource_failure = "resource guard did not stop"
                    self.guard_summary["failure"] = self.resource_failure
            self.guard_summary["status"] = (
                "passed" if self.resource_failure is None and not self.guard_termination_sent
                and monitor is not None and not monitor.is_alive() else "failed"
            )
            artifact["resource_guard"] = self.guard_summary
            artifact["cleanup"] = cleanup
            lifecycle_failures = []
            if not _qdrant_resource_guard_valid(self.guard_summary):
                lifecycle_failures.append(self.resource_failure or "resource guard did not pass")
            if not _qdrant_cleanup_valid(cleanup):
                lifecycle_failures.append(cleanup["failure"] or "owned cleanup did not complete cleanly")
            if not _boundary_storage_valid(artifact.get("storage")):
                lifecycle_failures.append("initial-ready storage boundary is unavailable")
            artifact.setdefault("reasons", []).extend(
                reason for reason in lifecycle_failures if reason not in artifact.get("reasons", []))
            if artifact["reasons"]:
                artifact["state"] = "uncalibrated"
        comparison = (compare_artifacts(tree, artifact) if tree is not None else
                      {"state": "uncalibrated", "reasons": artifact["reasons"]})
        (self.output / "qdrant-rss.json").write_bytes(native.canonical(artifact))
        (self.output / "comparison.json").write_bytes(native.canonical(comparison))
        three_arm = None
        if "treedb_sq8_artifact" in self.plan:
            try:
                sq8, sq8_raw = native.strict_json_object(
                    self.plan["treedb_sq8_artifact"], "TreeDB SQ8 artifact",
                    native.EVIDENCE_JSON_MAX_BYTES,
                )
                if native.bytes_digest(sq8_raw) != self.plan["treedb_sq8_artifact_sha256"]:
                    raise RuntimeError("TreeDB SQ8 artifact changed after freeze")
                three_arm = compare_three_arms(tree, artifact, sq8)
                three_arm["input_artifact_sha256"] = {
                    "treedb_fp32": self.plan["treedb_artifact_sha256"],
                    "treedb_sq8": self.plan["treedb_sq8_artifact_sha256"],
                    "qdrant_fp32": native.digest(self.output / "qdrant-rss.json"),
                }
            except BaseException as exc:
                three_arm = {
                    "schema": "treedb_cohere_768_three_arm_comparison/v1", "state": "partial",
                    "fp32_treedb_vs_fp32_qdrant": comparison,
                    "sq8_treedb_vs_fp32_qdrant": {
                        "state": "unavailable", "reasons": [f"{type(exc).__name__}: {exc}"],
                    },
                }
            (self.output / "three-arm-comparison.json").write_bytes(native.canonical(three_arm))
        print(json.dumps(comparison, sort_keys=True, allow_nan=False))
        return int(comparison["state"] == "uncalibrated" or (three_arm is not None and three_arm["state"] != "complete"))


def main():
    credentials = [key for key in QDRANT_CREDENTIAL_ENV if os.environ.get(key)]
    if credentials:
        raise ValueError(
            "Q5 Qdrant RSS evidence requires an unauthenticated environment; unset "
            + ", ".join(credentials),
        )
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--freeze", type=Path)
    mode.add_argument("--run", type=Path)
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--treedb-artifact", required=True, type=Path)
    parser.add_argument("--treedb-sq8-artifact", type=Path)
    parser.add_argument("--qdrant-bin", required=True, type=Path)
    parser.add_argument("--storage-path", required=True, type=Path)
    parser.add_argument("--url", required=True)
    parser.add_argument("--collection", default="minima_cohere_rss")
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--operation-timeout", type=int, default=600)
    parser.add_argument("--startup-timeout", type=float, default=120)
    parser.add_argument("--optimizer-timeout", type=float, default=2700)
    parser.add_argument("--poll-interval", type=float, default=.25)
    args = parser.parse_args()
    plan = prepare(args)
    if args.freeze:
        print(native.write_frozen_json(args.freeze, plan, "frozen Qdrant RSS plan"), args.freeze)
        return 0
    frozen, frozen_raw = native.strict_json_object(args.run, "frozen Qdrant RSS plan")
    if not args.expected_plan_sha256 or native.bytes_digest(frozen_raw) != args.expected_plan_sha256:
        raise ValueError("externally pinned Qdrant RSS plan hash required")
    validate_plan(frozen, plan)
    from qdrant_client import QdrantClient, models
    factory = lambda: QdrantClient(url=args.url, timeout=args.operation_timeout,
                                   prefer_grpc=False)
    return Run(plan, factory, models).execute()


if __name__ == "__main__":
    raise SystemExit(main())
