#!/usr/bin/env python3
"""Matched 500Kx768D TreeDB/Qdrant application-path performance campaign."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
import threading
import time
import urllib.parse

import numpy as np

import minima_cohere_native_diagnostic as native
import minima_cohere_qdrant_rss_diagnostic as qdrant_rss


SCHEMA = "treedb_qdrant_cohere_768_performance/v1"
PLAN_SCHEMA = "treedb_qdrant_cohere_768_performance_plan/v1"
ROWS = 500_000
DIMENSIONS = 768
TOP_K = 10
QUALITY_TARGET = 0.90
CONTROL_NAMES = {"treedb": "ef_search", "qdrant": "hnsw_ef"}
MEASUREMENT_QUERIES = list(range(100, 200))
PAIR_ORDER = [["treedb", "qdrant"], ["qdrant", "treedb"], ["treedb", "qdrant"]]
HARNESS_PATHS = ("benchmarks/vector_db_compare", "clients/python/treedb_client")
PRODUCT_PATHS = ("TreeDB", "cmd/treedb-document-service", "go.mod", "go.sum", "internal")
SOURCE_PATHS = HARNESS_PATHS + PRODUCT_PATHS
THREAD_ENVIRONMENT = {"GOMAXPROCS": "6", "OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1"}


def canonical(value):
    return (json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":")) + "\n").encode()


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            result.update(chunk)
    return result.hexdigest()


def distribution(values):
    values = sorted(values)
    if not values:
        return {"count": 0}
    return {
        "count": len(values), "mean_ns": sum(values) / len(values),
        **{name + "_ns": values[math.ceil(len(values) * fraction) - 1]
           for name, fraction in (("p50", .50), ("p95", .95), ("p99", .99), ("max", 1.0))},
    }


def mean_recall(actual, truth):
    values = [len(set(got) & set(want)) / len(want) for got, want in zip(actual, truth, strict=True)]
    return {"mean_recall_at_10": sum(values) / len(values), "per_query": values}


def reviewed_selected_control(artifact, backend, candidates):
    quality = artifact.get("quality") or {}
    selected = quality.get("selected_control")
    if selected not in candidates:
        raise RuntimeError(f"{backend} selected control is outside the reviewed candidates")
    prefix = candidates[:candidates.index(selected) + 1]
    calibration = quality.get("calibration") or {}
    revalidation = quality.get("revalidation") or {}
    calibration_curve = calibration.get("curve") or []
    revalidation_curve = revalidation.get("curve") or []

    def verified_mean(record, count):
        samples, reported = record.get("per_query"), record.get("mean_recall_at_10")
        if (not isinstance(samples, list) or len(samples) != count
                or any(not isinstance(value, (int, float)) or isinstance(value, bool)
                       or not 0 <= value <= 1 for value in samples)
                or not isinstance(reported, (int, float)) or isinstance(reported, bool)):
            raise RuntimeError(f"{backend} artifact has invalid retained recall samples")
        recomputed = sum(samples) / len(samples)
        if not math.isclose(reported, recomputed, rel_tol=0, abs_tol=1e-12):
            raise RuntimeError(f"{backend} artifact recall summary differs from retained samples")
        return recomputed

    if ([point.get("control") for point in calibration_curve] != prefix
            or [point.get("control") for point in revalidation_curve] != prefix):
        raise RuntimeError(f"{backend} artifact does not retain both candidate prefixes")
    calibration_means = [verified_mean(point, 100) for point in calibration_curve]
    revalidation_means = [verified_mean(point, len(MEASUREMENT_QUERIES)) for point in revalidation_curve]
    if (quality.get("selection_protocol") != native.RSS_SELECTION_PROTOCOL
            or calibration.get("queries") != list(range(100))
            or revalidation.get("queries") != MEASUREMENT_QUERIES
            or quality.get("target_mean_recall_at_10") != QUALITY_TARGET
            or any(first >= QUALITY_TARGET and second >= QUALITY_TARGET
                   for first, second in zip(calibration_means[:-1], revalidation_means[:-1], strict=True))
            or not calibration_means or calibration_means[-1] < QUALITY_TARGET
            or revalidation_means[-1] < QUALITY_TARGET
            or revalidation.get("passed") is not True):
        raise RuntimeError(f"{backend} artifact does not prove the lowest passing control")
    return selected


def repository_commit(source):
    status = subprocess.check_output(["git", "status", "--porcelain"], cwd=source, text=True)
    if status:
        raise RuntimeError("campaign source tree must be clean")
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()


def repository_trees(source):
    return {path: subprocess.check_output(
        ["git", "rev-parse", "HEAD:" + path], cwd=source, text=True).strip() for path in SOURCE_PATHS}


def go_binary_commit(binary):
    output = subprocess.check_output(["go", "version", "-m", str(binary)], text=True)
    values = dict(line.strip().split("=", 1) for line in output.splitlines() if "vcs." in line and "=" in line)
    revision = values.get("build\tvcs.revision") or values.get("vcs.revision")
    modified = values.get("build\tvcs.modified") or values.get("vcs.modified")
    if not revision or modified != "false":
        raise RuntimeError("TreeDB binary lacks clean VCS provenance")
    return revision


def measured_rss(sample, backend, boundary):
    if sample.get("availability") != "measured" or not isinstance(sample.get("bytes"), int):
        raise RuntimeError(f"{backend} {boundary} VmHWM is unavailable")
    return sample


def validate_control_provenance(tree, qdrant_result, commit, trees, hashes):
    tree_provenance = tree.get("provenance") or {}
    qdrant_provenance = qdrant_result.get("provenance") or {}
    harness_trees = {path: trees[path] for path in HARNESS_PATHS}
    product_trees = {path: trees[path] for path in PRODUCT_PATHS}
    if (tree_provenance.get("harness_commit") != commit
            or tree_provenance.get("product_commit") != commit
            or tree_provenance.get("harness_trees") != harness_trees
            or tree_provenance.get("product_trees") != product_trees
            or tree_provenance.get("service_sha256") != hashes["service"]
            or tree_provenance.get("serving_sha256") != hashes["serving"]
            or qdrant_provenance.get("harness_commit") != commit
            or qdrant_provenance.get("harness_trees") != harness_trees
            or qdrant_provenance.get("qdrant_bin_sha256") != hashes["qdrant"]
            or qdrant_provenance.get("qdrant_client_version") != "1.19.0"):
        raise RuntimeError("reviewed control artifacts were not calibrated from the campaign checkout")


def validate_host_identity(plan):
    if (platform.platform() != plan["platform"]
            or native.host_resource_identity() != plan["host_resource_identity"]
            or native.existing.common.memory_bytes() != plan["host_memory_bytes"]
            or {key: os.environ.get(key, "") for key in ("GOMAXPROCS", "GOGC", "GOMEMLIMIT")}
               != plan["treedb_go_runtime"]):
        raise RuntimeError("campaign host resource identity drifted")


def validate_control_environment(contract):
    expected = {
        "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "host_resource_identity": native.host_resource_identity(),
        "platform": platform.platform(),
        "host_memory_bytes": native.existing.common.memory_bytes(),
        "treedb_go_runtime": {
            key: os.environ.get(key, "") for key in ("GOMAXPROCS", "GOGC", "GOMEMLIMIT")
        },
    }
    if any(contract.get(key) != value for key, value in expected.items()):
        raise RuntimeError("reviewed controls were calibrated in a different environment")
    return expected


def python_environment():
    packages = sorted([distribution.metadata.get("Name") or "", distribution.version]
                      for distribution in importlib.metadata.distributions())
    return {"executable": str(Path(sys.executable).resolve()), "executable_sha256": digest(sys.executable),
            "version": sys.version, "numpy": np.__version__, "packages": packages}


def check_final_tree_resources(run, monitor):
    if monitor and monitor.is_alive():
        raise RuntimeError("resource guard did not stop")
    run.check_resources()
    if run.failure:
        raise RuntimeError(run.failure)


def validate_thread_environment():
    if any(os.environ.get(key) != value for key, value in THREAD_ENVIRONMENT.items()):
        raise RuntimeError("campaign requires GOMAXPROCS=6 and one BLAS thread")


def check_qdrant_resources(run, started):
    rss, peak_rss = 0, 0
    for pid in (os.getpid(), run.process.pid if run.process else None):
        if pid is not None:
            try:
                for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                    if line.startswith("VmRSS:"):
                        rss += int(line.split()[1]) * 1024
                    elif line.startswith("VmHWM:"):
                        peak_rss += int(line.split()[1]) * 1024
            except FileNotFoundError:
                pass
    with run.resource_lock:
        run.combined_peak_rss_bytes = max(run.combined_peak_rss_bytes, peak_rss)
        combined_peak_rss = run.combined_peak_rss_bytes
    if (shutil.disk_usage(run.output).free < run.plan["minimum_free_bytes"]
            or qdrant_rss.existing.disk_bytes(run.output) > run.plan["maximum_output_bytes"]
            or rss > run.plan["maximum_combined_rss_bytes"]
            or combined_peak_rss > run.plan["maximum_combined_rss_bytes"]
            or time.monotonic() - started > run.plan["wall_limit_s"]):
        raise RuntimeError("frozen Qdrant disk/RAM/wall budget exceeded")


def guard_qdrant_resources(run, started, cancel, failures):
    while not cancel.wait(1):
        if not failures:
            try:
                check_qdrant_resources(run, started)
            except BaseException as exc:
                failures.append(f"resource guard: {exc}")
        if failures:
            process = run.process
            if (process and process.poll() is None
                    and qdrant_rss.existing.linux_process_identity(process.pid) == run.process_identity):
                process.terminate()


def frozen_plan(args):
    source = Path(__file__).resolve().parents[2]
    commit = repository_commit(source)
    source_trees = repository_trees(source)
    dataset, _, files, query_count = native.dataset_identity(args.dataset, ROWS)
    if query_count != 200 or sorted(os.sched_getaffinity(0)) != list(range(6)):
        raise RuntimeError("freeze requires the 200-query fixture and CPU affinity 0-5")
    validate_thread_environment()
    service, qdrant = args.service_bin.resolve(), args.qdrant_bin.resolve()
    serving = args.serving.resolve()
    for path in (service, qdrant, serving, args.control_evidence.resolve()):
        if not path.is_file():
            raise ValueError(f"missing campaign input: {path}")
    if go_binary_commit(service) != commit:
        raise RuntimeError("TreeDB binary does not match campaign commit")
    version = subprocess.check_output([str(qdrant), "--version"], text=True).strip()
    if version != "qdrant 1.19.0" or importlib.metadata.version("qdrant-client") != "1.19.0":
        raise RuntimeError("Qdrant server and client must both be 1.19.0")
    evidence = json.loads(args.control_evidence.read_text())
    if (evidence.get("state") != "accept" and evidence.get("state") != "investigate"):
        raise RuntimeError("fixed-control evidence is not a completed matched comparison")
    evidence_root = args.control_evidence.parent.parent
    tree_artifact = evidence_root / "treedb" / "rss.json"
    qdrant_artifact = evidence_root / "qdrant" / "qdrant-rss.json"
    tree, qdrant_result = json.loads(tree_artifact.read_text()), json.loads(qdrant_artifact.read_text())
    validate_control_provenance(tree, qdrant_result, commit, source_trees, {
        "service": digest(service), "serving": digest(serving), "qdrant": digest(qdrant),
    })
    expected_comparison = qdrant_rss.compare_artifacts(tree, qdrant_result)
    contract = tree.get("comparison_contract") or {}
    if (evidence != expected_comparison
            or qdrant_result.get("comparison_contract") != contract
            or (qdrant_result.get("provenance") or {}).get("treedb_artifact_sha256") != digest(tree_artifact)
            or contract.get("rows") != ROWS or contract.get("dimensions") != DIMENSIONS
            or contract.get("top_k") != TOP_K or contract.get("quality_target") != QUALITY_TARGET
            or contract.get("dataset_manifest_sha256") != digest(dataset / "manifest.json")
            or contract.get("dataset_files_sha256") != files
            or contract.get("quality_selection_protocol") != native.RSS_SELECTION_PROTOCOL
            or contract.get("calibration_queries") != list(range(100))
            or contract.get("revalidation_queries") != MEASUREMENT_QUERIES):
        raise RuntimeError("reviewed control artifacts do not share the frozen comparison contract")
    control_environment = validate_control_environment(contract)
    candidates = {"treedb": contract.get("ann_controls", {}).get("treedb_ef_search"),
                  "qdrant": contract.get("ann_controls", {}).get("qdrant_hnsw_ef")}
    if any(value != native.RSS_CONTROLS for value in candidates.values()):
        raise RuntimeError("reviewed control candidate sets differ from the v3 policy")
    selected = {backend: reviewed_selected_control(artifact, backend, candidates[backend])
                for backend, artifact in (("treedb", tree), ("qdrant", qdrant_result))}
    if args.run_root.exists():
        raise ValueError("campaign run root must not exist at freeze")
    return {
        "schema": PLAN_SCHEMA, "campaign_commit": commit,
        "harness_sha256": digest(__file__),
        "source_trees": source_trees,
        "dataset": str(dataset), "dataset_manifest_sha256": digest(dataset / "manifest.json"),
        "dataset_files_sha256": files, "rows": ROWS, "dimensions": DIMENSIONS, "queries": query_count,
        "top_k": TOP_K,
        "service_bin": str(service), "service_sha256": digest(service),
        "qdrant_bin": str(qdrant), "qdrant_sha256": digest(qdrant),
        "qdrant_server_version": "1.19.0", "qdrant_client_version": "1.19.0",
        "qdrant_configuration": {
            "initial_upload_hnsw": qdrant_rss.existing.INITIAL_UPLOAD_HNSW_CONFIG,
            "initial_upload_optimizers": qdrant_rss.existing.INITIAL_UPLOAD_OPTIMIZERS_CONFIG,
            "production_hnsw": qdrant_rss.existing.PRODUCTION_HNSW_CONFIG,
            "production_optimizers": qdrant_rss.existing.PRODUCTION_OPTIMIZERS_CONFIG,
        },
        "serving": json.loads(serving.read_text()), "serving_path": str(serving),
        "serving_sha256": digest(serving), "run_root": str(args.run_root.resolve()),
        "control_evidence": str(args.control_evidence.resolve()),
        "control_evidence_sha256": digest(args.control_evidence),
        "control_artifacts_sha256": {"treedb": digest(tree_artifact), "qdrant": digest(qdrant_artifact)},
        "control_selection": {backend: {"name": CONTROL_NAMES[backend], "value": control,
            "policy": "lowest candidate passing both fixed query sets in reviewed v3 calibration"}
            for backend, control in selected.items()},
        "quality_target_mean_recall_at_10": QUALITY_TARGET,
        "quality_queries": MEASUREMENT_QUERIES,
        "pair_order": PAIR_ORDER, "repetitions": 3, "batch_size": 256,
        "warmup_queries": 100, "query_window_seconds": args.query_window_seconds,
        "query_concurrency": 4, "mixed_reader_concurrency": 4,
        "mixed_queries_per_reader": 64, "mixed_write_batches": 8, "mixed_write_batch_rows": 256,
        "cpu_affinity": list(range(6)), "gomaxprocs": "6",
        "host_memory_bytes": control_environment["host_memory_bytes"],
        "treedb_go_runtime": control_environment["treedb_go_runtime"],
        "blas_threads": {"OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1"},
        "python_environment": python_environment(),
        "platform": platform.platform(), "host_resource_identity": native.host_resource_identity(),
        "metric_scope": {
            "query": "caller-visible public request only; correctness checks outside timed windows",
            "ingest": "caller wall and acknowledged public-call time; document conversion reported separately by wall",
            "ready": "fresh process through scalar schema, durable-visible load, and production ANN readiness",
            "disk": "recursive apparent bytes including WAL, indexes, and live temporary files",
            "rss": "server-process lifetime VmHWM; initial-ready, extended-live, and restarted lifetimes separate",
            "restart": "clean shutdown through successful reopened ANN query; not crash recovery",
        },
    }


def validate_runtime(plan, expected_sha256, plan_path):
    if digest(plan_path) != expected_sha256:
        raise RuntimeError("campaign plan hash mismatch")
    if plan.get("schema") != PLAN_SCHEMA or plan.get("pair_order") != PAIR_ORDER:
        raise RuntimeError("unsupported campaign plan")
    if digest(__file__) != plan["harness_sha256"]:
        raise RuntimeError("campaign harness drifted")
    source = Path(__file__).resolve().parents[2]
    native.validate_imports(source)
    qdrant_rss.validate_imports(source)
    if (repository_commit(source) != plan["campaign_commit"]
            or repository_trees(source) != plan["source_trees"]):
        raise RuntimeError("campaign source provenance drifted")
    if importlib.metadata.version("qdrant-client") != plan["qdrant_client_version"]:
        raise RuntimeError("Qdrant client version drifted")
    if python_environment() != plan["python_environment"]:
        raise RuntimeError("campaign Python environment drifted")
    validate_host_identity(plan)
    if sorted(os.sched_getaffinity(0)) != plan["cpu_affinity"]:
        raise RuntimeError("campaign CPU affinity drifted")
    validate_thread_environment()
    for key, hash_key in (("service_bin", "service_sha256"), ("qdrant_bin", "qdrant_sha256"),
                          ("serving_path", "serving_sha256"), ("control_evidence", "control_evidence_sha256")):
        if digest(plan[key]) != plan[hash_key]:
            raise RuntimeError(f"campaign input drifted: {key}")
    if go_binary_commit(plan["service_bin"]) != plan["campaign_commit"]:
        raise RuntimeError("TreeDB binary VCS provenance drifted")
    dataset = Path(plan["dataset"])
    if digest(dataset / "manifest.json") != plan["dataset_manifest_sha256"]:
        raise RuntimeError("dataset manifest drifted")
    for name, expected in plan["dataset_files_sha256"].items():
        suffix = ".json" if name == "truth" else ".f32"
        if digest(dataset / (name + suffix)) != expected:
            raise RuntimeError(f"dataset input drifted: {name}")


def campaign_sequence(plan):
    return [(repeat, backend, order) for repeat, pair in enumerate(plan["pair_order"], 1)
            for order, backend in enumerate(pair, 1)]


def validate_result_identity(result, plan, plan_sha256, repeat, backend, order):
    expected = {
        "state": "complete", "backend": backend, "repetition": repeat, "order": order,
        "plan_sha256": plan_sha256, "campaign_commit": plan["campaign_commit"],
        "harness_sha256": plan["harness_sha256"],
        "dataset_manifest_sha256": plan["dataset_manifest_sha256"],
        "dataset_files_sha256": plan["dataset_files_sha256"],
    }
    if any(result.get(key) != value for key, value in expected.items()):
        raise RuntimeError(f"campaign result identity mismatch for repetition {repeat} {backend}")


def require_predecessors(plan, root, plan_sha256, repeat, backend):
    sequence = campaign_sequence(plan)
    position = next(index for index, item in enumerate(sequence)
                    if item[:2] == (repeat, backend))
    for prior_repeat, prior_backend, prior_order in sequence[:position]:
        path = root / f"repeat-{prior_repeat}-{prior_backend}" / "result.json"
        if not path.is_file():
            raise RuntimeError(f"campaign predecessor is incomplete: {prior_repeat} {prior_backend}")
        validate_result_identity(json.loads(path.read_text()), plan, plan_sha256,
                                 prior_repeat, prior_backend, prior_order)
    return sequence[position][2]


def acquire_campaign_lock(root):
    root.mkdir(parents=True, exist_ok=True)
    handle = (root / "campaign.lock").open("a+")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise RuntimeError("another campaign backend is running") from None
    return handle


def timed_window(call, validate, query_ordinals, concurrency, seconds):
    gate = threading.Barrier(concurrency + 1)
    stop_ns = [0]

    def worker(worker_id):
        samples, responses, index, first = [], [], worker_id, True
        gate.wait(timeout=30)
        while first or time.monotonic_ns() < stop_ns[0]:
            first = False
            query = query_ordinals[index % len(query_ordinals)]
            started = time.monotonic_ns()
            value = call(query)
            ended = time.monotonic_ns()
            samples.append((started, ended))
            responses.append(value)
            index += concurrency
        return samples, responses

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker, worker_id) for worker_id in range(concurrency)]
        started = time.monotonic_ns()
        stop_ns[0] = started + int(seconds * 1e9)
        gate.wait(timeout=30)
        batches = [future.result() for future in futures]
    samples = [sample for batch, _ in batches for sample in batch]
    for response in (response for _, batch in batches for response in batch):
        validate(response)
    ended = max(end for _, end in samples)
    durations = [end - start for start, end in samples]
    return {"wall_ns": ended - started, "qps": len(samples) * 1e9 / (ended - started),
            "latency": distribution(durations)}, samples


def mixed_window(read, write_batches, query_ordinals, readers, reads_per_reader, rows_per_batch):
    gate = threading.Barrier(readers + 2)

    def reader(worker_id):
        result = []
        gate.wait(timeout=30)
        for index in range(reads_per_reader):
            started = time.monotonic_ns()
            value = read(query_ordinals[(index * readers + worker_id) % len(query_ordinals)])
            ended = time.monotonic_ns()
            result.append((started, ended, value))
        return result

    def writer():
        result = []
        gate.wait(timeout=30)
        for rows, write in write_batches:
            started = time.monotonic_ns()
            value = write()
            ended = time.monotonic_ns()
            result.append((started, ended, value, rows))
        return result

    with ThreadPoolExecutor(max_workers=readers + 1) as pool:
        reads = [pool.submit(reader, worker) for worker in range(readers)]
        writes = pool.submit(writer)
        started = time.monotonic_ns()
        gate.wait(timeout=30)
        read_samples = [sample for future in reads for sample in future.result()]
        write_samples = writes.result()
    ended = max(max(end for _, end, _ in read_samples), max(end for _, end, _, _ in write_samples))
    write_start = min(start for start, _, _, _ in write_samples)
    write_end = max(end for _, end, _, _ in write_samples)
    overlap = [(start, end) for start, end, _ in read_samples if start < write_end and end > write_start]
    if not overlap:
        raise RuntimeError("mixed phase produced no observed read/write overlap")
    return {
        "wall_ns": ended - started,
        "reads": {"qps": len(read_samples) * 1e9 / (ended - started),
                  "latency": distribution([end - start for start, end, _ in read_samples])},
        "overlapping_reads": {"interval_ns": write_end - write_start,
                              "latency": distribution([end - start for start, end in overlap])},
        "writes": {"batches": len(write_samples), "rows": len(write_samples) * rows_per_batch,
                   "rows_per_second": len(write_samples) * rows_per_batch * 1e9 / (write_end - write_start),
                   "latency": distribution([end - start for start, end, _, _ in write_samples])},
    }, read_samples, write_samples


def mixed_read_state(start, end, write_samples):
    updated, transitioning = set(), set()
    for write_start, write_end, _, rows in write_samples:
        if write_end <= start:
            updated.update(rows)
        elif write_start < end and write_end > start:
            transitioning.update(rows)
    return updated, transitioning


def tree_plan(plan, run_dir):
    return {
        **plan, "run_dir": str(run_dir), "url": "http://127.0.0.1:17720",
        "native_address": "127.0.0.1:17722", "diagnostics_url": "http://127.0.0.1:17721",
        "operation_timeout_s": 600, "minimum_free_bytes": 10 << 30,
        "maximum_output_bytes": 12 << 30, "maximum_combined_rss_bytes": 26 << 30,
        "wall_limit_s": 3600, "mode": "diagnostic", "queries": 200,
        "eligible_counts": [ROWS],
    }


def tree_query(run, query, control):
    return run.clients.native.query_by_embedding(
        "minima_cohere", run.queries[query].tolist(), TOP_K, None,
        route="ann", ef_search=control, index_info=run.info,
    )


def validate_tree_upsert(response, documents):
    if response.upserted != len(documents) or response.ids != [doc["id"] for doc in documents]:
        raise RuntimeError("TreeDB public upsert completion mismatch")


def validate_tree_queries(responses, generation, updated_rows=(), transitioning_rows=()):
    updated_rows, transitioning_rows = set(updated_rows), set(transitioning_rows)
    for response in responses:
        work = response.dense_work
        if (response.native_command_version != 2 or len(response.documents) != TOP_K
                or response.route != "ann" or response.index.generation != generation
                or response.index.vector_strategy != "column_graph"
                or response.index.extra.get("typed_input") is not True
                or response.exact_fallbacks or response.full_document_scan_fallbacks
                or work is None or not work.completed or work.output.fetched != TOP_K):
            raise RuntimeError("TreeDB read left the required native ANN route")
        ids = [document.id for document in response.documents]
        if len(set(ids)) != TOP_K:
            raise RuntimeError("TreeDB read returned duplicate logical IDs")
        for document in response.documents:
            try:
                row = int(document.id.removeprefix("row-"))
                content = f"minima-cohere:{row}"
                contents = ({content, content + ":updated"} if row in transitioning_rows else
                            {content + (":updated" if row in updated_rows else "")})
                valid = (document.id == f"row-{row:06d}" and 0 <= row < ROWS
                         and document.content in contents
                         and document.meta == {
                             "user_id": f"{(row * 7919) % ROWS:06d}",
                             "fpath": f"/cohere/{row // 256:06d}.txt",
                         }
                         and document.embedding is None and document.score is not None
                         and math.isfinite(document.score))
            except (AttributeError, TypeError, ValueError, OverflowError):
                valid = False
            if not valid:
                raise RuntimeError("TreeDB read returned an invalid logical document")


def validate_tree_updates(run, rows):
    for first in range(0, len(rows), 256):
        ids = [f"row-{row:06d}" for row in rows[first:first + 256]]
        documents = run.clients.native.get_many("minima_cohere", ids, index_info=run.info)
        found = {doc.id: doc for doc in documents if doc is not None}
        if set(found) != set(ids) or any(not found[doc_id].content.endswith(":updated") for doc_id in ids):
            raise RuntimeError("TreeDB replacement verification failed")


def run_treedb(plan, run_dir):
    run = native.Run(tree_plan(plan, run_dir))
    monitor, failure = None, None
    started = time.monotonic_ns()
    try:
        run.started = time.monotonic()
        monitor = threading.Thread(target=run.guard, daemon=True)
        monitor.start()
        phase = {}
        mark = time.monotonic_ns(); run.controller.start(); phase["service_start_ns"] = time.monotonic_ns() - mark
        mark = time.monotonic_ns(); run.ensure(); phase["schema_ns"] = time.monotonic_ns() - mark
        ingest_start, ingest_calls = time.monotonic_ns(), []
        for first in range(0, ROWS, 256):
            rows = list(range(first, min(first + 256, ROWS)))
            docs = [native.make_document(run.vectors, row, ROWS) for row in rows]
            before = time.monotonic_ns()
            response = run.clients.native.upsert_documents("minima_cohere", docs, index_info=run.info)
            ingest_calls.append(time.monotonic_ns() - before)
            validate_tree_upsert(response, docs)
        phase["ingest_wall_ns"] = time.monotonic_ns() - ingest_start
        build_start = time.monotonic_ns(); run.optimize("build"); phase["ann_ready_transition_ns"] = time.monotonic_ns() - build_start
        phase["fresh_process_to_ann_ready_ns"] = time.monotonic_ns() - started

        control = plan["control_selection"]["treedb"]["value"]
        actual = []
        for query in MEASUREMENT_QUERIES:
            actual.append([doc.id for doc in run.search("profile_quality", ROWS, control, query).documents])
        quality = mean_recall(actual, [run.truth[str(ROWS)][query] for query in MEASUREMENT_QUERIES])
        if quality["mean_recall_at_10"] < QUALITY_TARGET:
            raise RuntimeError("TreeDB fixed control missed the recall target")
        initial_rss = measured_rss(native.process_peak_at_boundary(
            run.controller.process.pid, run.controller._owned_identity, plan["cpu_affinity"]),
            "TreeDB", "initial-ready")
        initial_disk = native.existing.common.disk_bytes(run.output / "db")
        for query in MEASUREMENT_QUERIES:
            validate_tree_queries([tree_query(run, query, control)], run.info.generation)
        validate = lambda response: validate_tree_queries([response], run.info.generation)
        single, _ = timed_window(lambda query: tree_query(run, query, control), validate,
                                 MEASUREMENT_QUERIES, 1, plan["query_window_seconds"])
        concurrent, _ = timed_window(lambda query: tree_query(run, query, control), validate,
                                     MEASUREMENT_QUERIES, plan["query_concurrency"],
                                     plan["query_window_seconds"])

        prepared = []
        for index in range(plan["mixed_write_batches"]):
            first = ROWS - (index + 1) * plan["mixed_write_batch_rows"]
            rows = list(range(first, first + plan["mixed_write_batch_rows"]))
            docs = [native.make_document(run.vectors, row, ROWS, updated=True) for row in rows]
            prepared.append((rows, docs))
        writes = [(rows, lambda docs=docs: run.clients.native.upsert_documents(
            "minima_cohere", docs, index_info=run.info)) for rows, docs in prepared]
        mixed, mixed_reads, mixed_writes = mixed_window(
            lambda query: tree_query(run, query, control), writes,
            MEASUREMENT_QUERIES, plan["mixed_reader_concurrency"],
            plan["mixed_queries_per_reader"], plan["mixed_write_batch_rows"])
        for start, end, response in mixed_reads:
            updated, transitioning = mixed_read_state(start, end, mixed_writes)
            validate_tree_queries([response], run.info.generation, updated, transitioning)
        for (_, docs), (_, _, response, _) in zip(prepared, mixed_writes, strict=True):
            validate_tree_upsert(response, docs)
        updated_rows = [row for rows, _ in prepared for row in rows]
        validate_tree_updates(run, updated_rows)
        run.updated.update(updated_rows)
        post_mixed_ready = time.monotonic_ns()
        run.optimize("ensure")
        phase["post_mixed_ann_ready_ns"] = time.monotonic_ns() - post_mixed_ready
        extended_rss = measured_rss(native.process_peak_at_boundary(
            run.controller.process.pid, run.controller._owned_identity, plan["cpu_affinity"]),
            "TreeDB", "extended-live")
        final_live_disk = native.existing.common.disk_bytes(run.output / "db")

        restart_start = time.monotonic_ns()
        run.clients.close(); native.validate_shutdowns(run.controller.lifetimes, 1)
        shutdown_disk = native.existing.common.disk_bytes(run.output / "db")
        run.check_resources()
        if run.failure:
            raise RuntimeError(run.failure)
        run.controller.start(); run.ensure(); run.optimize("ensure")
        response = tree_query(run, MEASUREMENT_QUERIES[0], control)
        restart_ns = time.monotonic_ns() - restart_start
        validate_tree_queries([response], run.info.generation, updated_rows)
        validate_tree_updates(run, updated_rows)
        restart_rss = measured_rss(native.process_peak_at_boundary(
            run.controller.process.pid, run.controller._owned_identity, plan["cpu_affinity"]),
            "TreeDB", "restart")
        result = {"schema": SCHEMA, "state": "complete", "backend": "treedb", "phases": phase,
                  "ingest": {"rows": ROWS, "calls": distribution(ingest_calls),
                             "rows_per_second": ROWS * 1e9 / phase["ingest_wall_ns"]},
                  "quality": quality, "control": plan["control_selection"]["treedb"],
                  "query": {"single": single, "concurrent": concurrent}, "mixed": mixed,
                  "restart": {"clean_restart_to_query_ready_ns": restart_ns,
                              "acknowledged_replacement_survived": True},
                  "resources": {"initial_ready_peak_rss": initial_rss, "extended_live_peak_rss": extended_rss,
                                "restart_peak_rss": restart_rss, "initial_ready_disk_bytes": initial_disk,
                                "final_live_disk_bytes": final_live_disk, "shutdown_disk_bytes": shutdown_disk}}
    except BaseException as exc:
        failure = f"{type(exc).__name__}: {exc}"
        result = {"schema": SCHEMA, "state": "failed", "backend": "treedb", "failure": failure}
    finally:
        try:
            run.clients.close()
            if result.get("state") == "complete":
                native.validate_shutdowns(run.controller.lifetimes, 2)
        except BaseException as exc:
            result = {"schema": SCHEMA, "state": "failed", "backend": "treedb",
                      "failure": failure or f"shutdown: {type(exc).__name__}: {exc}"}
        run.cancel.set()
        if monitor:
            monitor.join(timeout=5)
        try:
            check_final_tree_resources(run, monitor)
        except BaseException as exc:
            result = {"schema": SCHEMA, "state": "failed", "backend": "treedb",
                      "failure": result.get("failure") or f"final resource guard: {type(exc).__name__}: {exc}"}
        run.events.close()
    return result


def qdrant_plan(plan, run_dir):
    config = plan["qdrant_configuration"]
    return {
        **plan, "run_dir": str(run_dir), "storage_path": str(run_dir / "storage"),
        "url": "http://127.0.0.1:17733", "collection": "minima_cohere",
        "operation_timeout_s": 600, "startup_timeout_s": 120,
        "optimizer_timeout_s": 2700, "poll_interval_s": .25,
        "minimum_free_bytes": 10 << 30, "maximum_output_bytes": 12 << 30,
        "maximum_combined_rss_bytes": 26 << 30, "wall_limit_s": 3600,
        "initial_upload_hnsw": config["initial_upload_hnsw"],
        "initial_upload_optimizers": config["initial_upload_optimizers"],
        "production_hnsw": config["production_hnsw"],
        "production_optimizers": config["production_optimizers"],
    }


def qdrant_query(run, client, query, control):
    return client.query_points(
        collection_name=run.collection, query=run.queries[query].tolist(), limit=TOP_K,
        with_payload=True, with_vectors=False,
        search_params=run.models.SearchParams(hnsw_ef=control, exact=False), timeout=run.operation_timeout,
    )


def validate_qdrant_upserts(responses):
    for response in responses:
        status = getattr(response, "status", None)
        if getattr(status, "value", status) != "completed":
            raise RuntimeError("Qdrant wait=true upsert did not complete")


def validate_qdrant_queries(responses, updated_rows=(), transitioning_rows=()):
    updated_rows, transitioning_rows = set(updated_rows), set(transitioning_rows)
    for response in responses:
        points = list(getattr(response, "points", response))
        ids = []
        for point in points:
            payload = getattr(point, "payload", None) or {}
            try:
                row = int(payload["id"].removeprefix("row-"))
                base_content = f"minima-cohere:{row}"
                contents = ({base_content, base_content + ":updated"}
                            if row in transitioning_rows else
                            {base_content + (":updated" if row in updated_rows else "")})
                score = getattr(point, "score", None)
                valid = (set(payload) == {"id", "content", "meta"}
                         and payload["id"] == f"row-{row:06d}" and 0 <= row < ROWS
                         and payload["content"] in contents
                         and payload["meta"] == {
                             "user_id": f"{(row * 7919) % ROWS:06d}",
                             "fpath": f"/cohere/{row // 256:06d}.txt",
                         }
                         and getattr(point, "id", None) == qdrant_rss.existing.point_id(payload["id"])
                         and getattr(point, "vector", None) is None
                         and isinstance(score, (int, float)) and not isinstance(score, bool)
                         and math.isfinite(score))
            except (AttributeError, KeyError, TypeError, ValueError, OverflowError):
                valid = False
            if not valid:
                raise RuntimeError("Qdrant read returned an invalid logical document")
            ids.append(payload["id"])
        if len(ids) != TOP_K or len(set(ids)) != TOP_K:
            raise RuntimeError("Qdrant read returned invalid logical IDs")


def validate_qdrant_updates(run, rows):
    for first in range(0, len(rows), 256):
        logical_ids = [f"row-{row:06d}" for row in rows[first:first + 256]]
        points = run.client.retrieve(collection_name=run.collection,
            ids=[qdrant_rss.existing.point_id(value) for value in logical_ids], with_payload=True)
        found = {(point.payload or {}).get("id"): point.payload or {} for point in points}
        if set(found) != set(logical_ids) or any(
                not found[doc_id].get("content", "").endswith(":updated") for doc_id in logical_ids):
            raise RuntimeError("Qdrant replacement verification failed")


def restart_qdrant(run):
    run.client.close(); run.stop_server()
    shutdown_disk = qdrant_rss.existing.disk_bytes(run.storage_path)
    run.process = None
    run.server_log = run.server_log_path.open("ab")
    parsed = urllib.parse.urlparse(run.plan["url"])
    env = {key: os.environ[key] for key in ("HOME", "PATH", "TMPDIR", "TZ") if key in os.environ}
    env.update(QDRANT__SERVICE__HOST="127.0.0.1", QDRANT__SERVICE__HTTP_PORT=str(parsed.port),
               QDRANT__STORAGE__STORAGE_PATH=str(run.storage_path))
    run.process = subprocess.Popen([run.plan["qdrant_bin"]], stdin=subprocess.DEVNULL,
        stdout=run.server_log, stderr=subprocess.STDOUT, cwd=run.output, env=env, start_new_session=True)
    run.server_pid = run.process.pid
    deadline, last = time.monotonic() + run.plan["startup_timeout_s"], None
    while time.monotonic() < deadline:
        if run.process.poll() is not None:
            raise RuntimeError(f"owned Qdrant exited during restart with {run.process.returncode}")
        try:
            identity = qdrant_rss.existing.linux_process_identity(run.server_pid)
            info = qdrant_rss.existing.server_info(run.plan["url"], "")
            if (identity and info.get("version") == run.plan["qdrant_server_version"]
                    and Path(f"/proc/{run.server_pid}/exe").resolve() == Path(run.plan["qdrant_bin"])
                    and qdrant_rss.existing.server_process_owns_endpoint(run.server_pid, run.plan["url"])):
                run.process_identity = identity
                run.process_command_identity = qdrant_rss.existing.server_process_identity(run.server_pid)
                run.client = run.client_factory()
                run.wait_ready(ROWS, "profile_restart_ready")
                return shutdown_disk
        except Exception as exc:
            last = exc
        time.sleep(run.plan["poll_interval_s"])
    raise TimeoutError(f"Qdrant restart exceeded timeout: {last}")


def run_qdrant(plan, run_dir):
    from qdrant_client import QdrantClient, models
    local = qdrant_plan(plan, run_dir)
    factory = lambda: QdrantClient(url=local["url"], timeout=local["operation_timeout_s"], prefer_grpc=False)
    run = qdrant_rss.Run(local, factory, models)
    run.resource_lock, run.combined_peak_rss_bytes = threading.Lock(), 0
    failure, monitor = None, None
    guard_cancel, guard_failures = threading.Event(), []
    started_at, started = time.monotonic(), time.monotonic_ns()
    try:
        monitor = threading.Thread(target=guard_qdrant_resources,
                                   args=(run, started_at, guard_cancel, guard_failures), daemon=True)
        monitor.start()
        phase = {}
        mark = time.monotonic_ns(); run.start_server(); phase["service_start_ns"] = time.monotonic_ns() - mark
        run.validate_fresh()
        mark = time.monotonic_ns(); run.create(); phase["schema_ns"] = time.monotonic_ns() - mark
        ingest_start, ingest_calls = time.monotonic_ns(), []
        for first in range(0, ROWS, 256):
            points = []
            for row in range(first, min(first + 256, ROWS)):
                value = qdrant_rss.qdrant_point(native.make_document(run.vectors, row, ROWS))
                points.append(models.PointStruct(id=qdrant_rss.existing.point_id(value["logical_id"]),
                    vector=value["vector"], payload=value["payload"]))
            before = time.monotonic_ns()
            response = run.client.upsert(
                collection_name=run.collection, points=points, wait=True, timeout=run.operation_timeout)
            ingest_calls.append(time.monotonic_ns() - before)
            validate_qdrant_upserts([response])
        phase["ingest_wall_ns"] = time.monotonic_ns() - ingest_start
        build_start = time.monotonic_ns()
        run.client.update_collection(collection_name=run.collection,
            hnsw_config=models.HnswConfigDiff(**local["production_hnsw"]),
            optimizers_config=models.OptimizersConfigDiff(**local["production_optimizers"]),
            timeout=run.operation_timeout)
        run.wait_ready(ROWS, "profile_initial_ready")
        phase["ann_ready_transition_ns"] = time.monotonic_ns() - build_start
        phase["fresh_process_to_ann_ready_ns"] = time.monotonic_ns() - started

        control = plan["control_selection"]["qdrant"]["value"]
        actual = [run.search(control, query) for query in MEASUREMENT_QUERIES]
        quality = mean_recall(actual, [run.truth[query] for query in MEASUREMENT_QUERIES])
        if quality["mean_recall_at_10"] < QUALITY_TARGET:
            raise RuntimeError("Qdrant fixed control missed the recall target")
        initial_rss = measured_rss(native.process_peak_at_boundary(
            run.server_pid, run.process_identity, plan["cpu_affinity"]), "Qdrant", "initial-ready")
        initial_disk = qdrant_rss.existing.disk_bytes(run.storage_path)
        for query in MEASUREMENT_QUERIES:
            validate_qdrant_queries([qdrant_query(run, run.client, query, control)])
        single, _ = timed_window(lambda query: qdrant_query(run, run.client, query, control),
                                 lambda response: validate_qdrant_queries([response]),
                                 MEASUREMENT_QUERIES, 1, plan["query_window_seconds"])

        clients = []
        lock = threading.Lock()
        local_clients = threading.local()
        def concurrent_query(query):
            if not hasattr(local_clients, "client"):
                local_clients.client = factory()
                with lock:
                    clients.append(local_clients.client)
            return qdrant_query(run, local_clients.client, query, control)
        concurrent, _ = timed_window(concurrent_query, lambda response: validate_qdrant_queries([response]),
                                     MEASUREMENT_QUERIES, plan["query_concurrency"],
                                     plan["query_window_seconds"])
        for client in clients:
            client.close()
        clients.clear()

        prepared = []
        for index in range(plan["mixed_write_batches"]):
            first = ROWS - (index + 1) * plan["mixed_write_batch_rows"]
            rows = list(range(first, first + plan["mixed_write_batch_rows"]))
            points = []
            for row in rows:
                value = qdrant_rss.qdrant_point(native.make_document(run.vectors, row, ROWS, updated=True))
                points.append(models.PointStruct(id=qdrant_rss.existing.point_id(value["logical_id"]),
                    vector=value["vector"], payload=value["payload"]))
            prepared.append((rows, points))
        writes = [(rows, lambda points=points: run.client.upsert(collection_name=run.collection, points=points,
            wait=True, timeout=run.operation_timeout)) for rows, points in prepared]
        mixed, mixed_reads, mixed_writes = mixed_window(
            concurrent_query, writes, MEASUREMENT_QUERIES, plan["mixed_reader_concurrency"],
            plan["mixed_queries_per_reader"], plan["mixed_write_batch_rows"])
        for start, end, response in mixed_reads:
            updated, transitioning = mixed_read_state(start, end, mixed_writes)
            validate_qdrant_queries([response], updated, transitioning)
        validate_qdrant_upserts([response for _, _, response, _ in mixed_writes])
        updated_rows = [row for rows, _ in prepared for row in rows]
        validate_qdrant_updates(run, updated_rows)
        post_mixed_ready = time.monotonic_ns()
        run.wait_ready(ROWS, "profile_post_mixed_ready")
        phase["post_mixed_ann_ready_ns"] = time.monotonic_ns() - post_mixed_ready
        for client in clients:
            try:
                client.close()
            except Exception:
                pass
        extended_rss = measured_rss(native.process_peak_at_boundary(
            run.server_pid, run.process_identity, plan["cpu_affinity"]), "Qdrant", "extended-live")
        final_live_disk = qdrant_rss.existing.disk_bytes(run.storage_path)

        check_qdrant_resources(run, started_at)
        restart_start = time.monotonic_ns(); shutdown_disk = restart_qdrant(run)
        response = qdrant_query(run, run.client, MEASUREMENT_QUERIES[0], control)
        restart_ns = time.monotonic_ns() - restart_start
        validate_qdrant_queries([response], updated_rows)
        validate_qdrant_updates(run, updated_rows)
        restart_rss = measured_rss(native.process_peak_at_boundary(
            run.server_pid, run.process_identity, plan["cpu_affinity"]), "Qdrant", "restart")
        result = {"schema": SCHEMA, "state": "complete", "backend": "qdrant", "phases": phase,
                  "ingest": {"rows": ROWS, "calls": distribution(ingest_calls),
                             "rows_per_second": ROWS * 1e9 / phase["ingest_wall_ns"]},
                  "quality": quality, "control": plan["control_selection"]["qdrant"],
                  "query": {"single": single, "concurrent": concurrent}, "mixed": mixed,
                  "restart": {"clean_restart_to_query_ready_ns": restart_ns,
                              "acknowledged_replacement_survived": True},
                  "resources": {"initial_ready_peak_rss": initial_rss, "extended_live_peak_rss": extended_rss,
                                "restart_peak_rss": restart_rss, "initial_ready_disk_bytes": initial_disk,
                                "final_live_disk_bytes": final_live_disk, "shutdown_disk_bytes": shutdown_disk}}
    except BaseException as exc:
        failure = f"{type(exc).__name__}: {exc}"
        result = {"schema": SCHEMA, "state": "failed", "backend": "qdrant", "failure": failure}
    finally:
        guard_cancel.set()
        if monitor:
            monitor.join(timeout=5)
        try:
            if monitor and monitor.is_alive():
                raise RuntimeError("Qdrant resource guard did not stop")
            check_qdrant_resources(run, started_at)
            if guard_failures:
                raise RuntimeError(guard_failures[0])
            if result.get("state") == "complete":
                result["resources"]["combined_lifetime_peak_rss_bytes"] = run.combined_peak_rss_bytes
        except BaseException as exc:
            result = {"schema": SCHEMA, "state": "failed", "backend": "qdrant",
                      "failure": result.get("failure") or f"final resource guard: {type(exc).__name__}: {exc}"}
        try:
            if run.client:
                run.client.close()
            if run.process and run.process.poll() is None:
                run.stop_server()
        except BaseException as exc:
            result = {"schema": SCHEMA, "state": "failed", "backend": "qdrant",
                      "failure": result.get("failure") or f"shutdown: {type(exc).__name__}: {exc}"}
    return result


def add_provenance(result, plan, plan_path, plan_sha256, repeat, order):
    result.update(repetition=repeat, order=order,
                  plan=str(plan_path.resolve()), plan_sha256=plan_sha256,
                  campaign_commit=plan["campaign_commit"], harness_sha256=plan["harness_sha256"],
                  dataset_manifest_sha256=plan["dataset_manifest_sha256"],
                  dataset_files_sha256=plan["dataset_files_sha256"])
    return result


def summarize(plan, root, plan_sha256):
    results, result_identities = [], []
    for repeat, order in enumerate(plan["pair_order"], 1):
        for position, backend in enumerate(order, 1):
            path = root / f"repeat-{repeat}-{backend}" / "result.json"
            result = json.loads(path.read_text())
            if result.get("schema") != SCHEMA:
                raise RuntimeError(f"incomplete campaign result: {path}")
            validate_result_identity(result, plan, plan_sha256, repeat, backend, position)
            results.append(result)
            result_identities.append({
                "backend": backend, "repetition": repeat, "order": position,
                "path": str(path.relative_to(root)), "sha256": digest(path),
            })

    def value(result, path):
        current = result
        for key in path:
            current = current[key]
        return current

    metrics = {
        "fresh_process_to_ann_ready_seconds": ("phases", "fresh_process_to_ann_ready_ns"),
        "durable_ingest_rows_per_second": ("ingest", "rows_per_second"),
        "single_query_p50_ms": ("query", "single", "latency", "p50_ns"),
        "single_query_p95_ms": ("query", "single", "latency", "p95_ns"),
        "single_query_qps": ("query", "single", "qps"),
        "concurrent4_query_p95_ms": ("query", "concurrent", "latency", "p95_ns"),
        "concurrent4_query_qps": ("query", "concurrent", "qps"),
        "mixed_overlap_query_p95_ms": ("mixed", "overlapping_reads", "latency", "p95_ns"),
        "mixed_writer_rows_per_second": ("mixed", "writes", "rows_per_second"),
        "clean_restart_to_query_ready_seconds": ("restart", "clean_restart_to_query_ready_ns"),
        "initial_ready_peak_rss_bytes": ("resources", "initial_ready_peak_rss", "bytes"),
        "initial_ready_disk_bytes": ("resources", "initial_ready_disk_bytes"),
        "final_live_disk_bytes": ("resources", "final_live_disk_bytes"),
        "mean_recall_at_10": ("quality", "mean_recall_at_10"),
    }
    scaled = {name: (1e-9 if name.endswith("seconds") else 1e-6 if name.endswith("_ms") else 1.0)
              for name in metrics}
    summary = {"schema": SCHEMA + "/summary", "state": "complete", "repetitions": 3,
               "pair_order": plan["pair_order"], "plan_sha256": plan_sha256,
               "campaign_commit": plan["campaign_commit"], "harness_sha256": plan["harness_sha256"],
               "dataset_manifest_sha256": plan["dataset_manifest_sha256"],
               "dataset_files_sha256": plan["dataset_files_sha256"],
               "control_artifacts_sha256": plan["control_artifacts_sha256"],
               "results": result_identities, "backends": {}}
    for backend in ("treedb", "qdrant"):
        rows = [result for result in results if result["backend"] == backend]
        summary["backends"][backend] = {"control": plan["control_selection"][backend], "metrics": {}}
        for name, path in metrics.items():
            values = [value(result, path) * scaled[name] for result in rows]
            summary["backends"][backend]["metrics"][name] = {
                "per_run": values, "median": statistics.median(values), "min": min(values), "max": max(values),
            }
    summary["median_ratios_treedb_over_qdrant"] = {
        name: summary["backends"]["treedb"]["metrics"][name]["median"] /
              summary["backends"]["qdrant"]["metrics"][name]["median"]
        for name in metrics if name != "mean_recall_at_10"
    }
    return summary


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze")
    freeze.add_argument("--output", required=True, type=Path)
    freeze.add_argument("--run-root", required=True, type=Path)
    freeze.add_argument("--dataset", required=True, type=Path)
    freeze.add_argument("--service-bin", required=True, type=Path)
    freeze.add_argument("--qdrant-bin", required=True, type=Path)
    freeze.add_argument("--serving", required=True, type=Path)
    freeze.add_argument("--control-evidence", required=True, type=Path)
    freeze.add_argument("--query-window-seconds", type=float, default=30)
    run = sub.add_parser("run")
    run.add_argument("--plan", required=True, type=Path)
    run.add_argument("--expected-plan-sha256", required=True)
    run.add_argument("--backend", choices=("treedb", "qdrant"), required=True)
    run.add_argument("--repetition", type=int, choices=(1, 2, 3), required=True)
    summary = sub.add_parser("summarize")
    summary.add_argument("--plan", required=True, type=Path)
    summary.add_argument("--expected-plan-sha256", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    if args.command == "freeze":
        if args.query_window_seconds <= 0:
            raise SystemExit("query window must be positive")
        plan = frozen_plan(args)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(canonical(plan))
        print(digest(args.output))
        return 0
    plan = json.loads(args.plan.read_text())
    validate_runtime(plan, args.expected_plan_sha256, args.plan)
    root = Path(plan["run_root"])
    if args.command == "summarize":
        result = summarize(plan, root, args.expected_plan_sha256)
        path = root / "summary.json"
        path.write_bytes(canonical(result))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if plan["pair_order"][args.repetition - 1].count(args.backend) != 1:
        raise SystemExit("backend is not in the frozen repetition")
    run_dir = root / f"repeat-{args.repetition}-{args.backend}"
    lock = acquire_campaign_lock(root)
    try:
        order = require_predecessors(plan, root, args.expected_plan_sha256,
                                     args.repetition, args.backend)
        if run_dir.exists():
            raise SystemExit(f"run directory already exists: {run_dir}")
        if shutil.disk_usage(root.parent).free < 20 << 30:
            raise SystemExit("less than 20 GiB free before campaign run")
        result = run_treedb(plan, run_dir) if args.backend == "treedb" else run_qdrant(plan, run_dir)
        result = add_provenance(result, plan, args.plan, args.expected_plan_sha256, args.repetition, order)
        (run_dir / "result.json").write_bytes(canonical(result))
    finally:
        lock.close()
    print(json.dumps({"backend": args.backend, "repetition": args.repetition,
                      "state": result["state"], "failure": result.get("failure")}, sort_keys=True))
    return int(result["state"] != "complete")


if __name__ == "__main__":
    raise SystemExit(main())
