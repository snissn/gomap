#!/usr/bin/env python3
"""Run the bounded Q3 native-v4 production gate on the fixed Cohere export."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import asdict
import hashlib
import json
import math
import os
from pathlib import Path
import socket
import statistics
import subprocess
import sys
import time
from typing import Any

import numpy as np

SOURCE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SOURCE / "clients/python/treedb_client/src"))

from minima_treedb_runner import ServiceController, validate_column_graph_serving
from treedb_client import Document, TreeDBClient

SCHEMA = "treedb_cohere_v4_production_gate/v1"
ROWS = 100_000
SOURCE_ROWS = 500_000
DIMENSIONS = 768
QUERY_COUNT = 200
DIAGNOSTIC_QUERY_COUNT = 20
TOP_K = 10
EF_SEARCH = 64
RERANK_CANDIDATES = 64
REPETITIONS = 6
WARMUP_QUERIES = 20
INDEX = "q3-production-gate"
QUANTIZED_INDEX = "minima_sq8"
REPRESENTATION = "cosine_normalized_f32_v1"
FROZEN_MANIFEST_SHA256 = "9144cbf1a20f7e8dad47eafb66173c738444017656ba1e0fdebab5cf3a0ac2f3"
FROZEN_DOCUMENTS_SHA256 = "d4f332061a17d7728ff2c44f0859130e1b43f43360143dc3d754733dcf4e04d8"
FROZEN_FIRST_100K_DOCUMENTS_SHA256 = "dedeeb3ed5b475c4c34632f44e4802c5e8ed4e1106abe224b9c1b6021b8853a1"
FROZEN_QUERIES_SHA256 = "d52ee3198dfc5641a6f2668b22b5994b489e86807616b3d7d77ac87de03cf1ed"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(8 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def prefix_sha256(path: Path, size: int) -> str:
    digest, remaining = hashlib.sha256(), size
    with path.open("rb") as source:
        while remaining:
            chunk = source.read(min(8 << 20, remaining))
            if not chunk:
                raise ValueError(f"{path} ended before the fixed prefix")
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest()


def free_address() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


def strict_json(path: Path) -> dict[str, Any]:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, value in pairs:
            if key in out:
                raise ValueError(f"duplicate key {key!r} in {path}")
            out[key] = value
        return out

    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=no_duplicates)
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain one JSON object")
    return value


def load_inputs(
    dataset: Path, *, rows: int = ROWS
) -> tuple[np.memmap, list[list[float]], dict[str, Any]]:
    manifest_path = dataset / "manifest.json"
    manifest = strict_json(manifest_path)
    documents_path, queries_path = dataset / "documents.f32", dataset / "queries.f32"
    manifest_hash = sha256(manifest_path)
    documents_hash = sha256(documents_path)
    first_100k_hash = prefix_sha256(documents_path, ROWS * DIMENSIONS * 4)
    queries_hash = sha256(queries_path)
    if (
        manifest.get("dimensions") != DIMENSIONS
        or manifest.get("query_count") != QUERY_COUNT
        or manifest.get("rows") != SOURCE_ROWS
        or manifest_hash != FROZEN_MANIFEST_SHA256
        or manifest.get("documents_sha256") != FROZEN_DOCUMENTS_SHA256
        or manifest.get("queries_sha256") != FROZEN_QUERIES_SHA256
        or documents_path.stat().st_size != SOURCE_ROWS * DIMENSIONS * 4
        or queries_path.stat().st_size != QUERY_COUNT * DIMENSIONS * 4
        or documents_hash != FROZEN_DOCUMENTS_SHA256
        or first_100k_hash != FROZEN_FIRST_100K_DOCUMENTS_SHA256
        or queries_hash != FROZEN_QUERIES_SHA256
    ):
        raise ValueError("dataset bytes do not match the fixed 500K×768 source manifest")
    vectors = np.memmap(
        documents_path, dtype="<f4", mode="r", shape=(SOURCE_ROWS, DIMENSIONS)
    )
    query_array = np.memmap(
        queries_path, dtype="<f4", mode="r", shape=(QUERY_COUNT, DIMENSIONS)
    )
    if not 0 < rows <= SOURCE_ROWS or not np.isfinite(vectors[:rows]).all() or not np.isfinite(query_array).all():
        raise ValueError("fixed production gate vectors must be finite")
    identity = {
        "source_manifest": manifest,
        "manifest_sha256": manifest_hash,
        "documents_sha256": documents_hash,
        "first_100k_documents_sha256": first_100k_hash,
        "queries_sha256": queries_hash,
    }
    return vectors, query_array.tolist(), identity


def create_and_load(
    control: TreeDBClient,
    native: TreeDBClient,
    vectors: np.memmap,
    serving: dict[str, Any],
    *,
    rows: int = ROWS,
    index: str = INDEX,
    quantized_index: str | None = QUANTIZED_INDEX,
) -> Any:
    quantized_indexes = [] if quantized_index is None else [
        {"name": quantized_index, "codec": "scalar_u8", "version": 1}
    ]
    info = control.create_index(
        index,
        DIMENSIONS,
        "cosine",
        typed_input=True,
        vector_index_options={
            "strategy": "column_graph",
            "representation": REPRESENTATION,
            "m": 16,
            "ef_construction": 32,
            "ef_search": EF_SEARCH,
            "quantized_indexes": quantized_indexes,
        },
    )
    if (
        info.name != index
        or info.dimension != DIMENSIONS
        or info.vector_representation != REPRESENTATION
        or info.vector_strategy != "column_graph"
        or info.extra.get("typed_input") is not True
    ):
        raise RuntimeError("created index does not expose the canonical production shape")
    started = time.monotonic()
    for start in range(0, rows, 256):
        end = min(rows, start + 256)
        documents = [
            Document(
                id=f"row-{row:06d}",
                content=f"minima-cohere:{row}",
                embedding=vectors[row].tolist(),
            )
            for row in range(start, end)
        ]
        result = native.upsert_documents(index, documents, index_info=info)
        if result.upserted != len(documents):
            raise RuntimeError(f"typed upsert {start}:{end} did not complete")
        if end % 10_000 == 0 or end == rows:
            print(
                f"loaded {end}/{rows} rows in {time.monotonic() - started:.1f}s",
                flush=True,
            )
    build_started = time.monotonic()
    control.optimize_index(
        index,
        expected_generation=info.generation,
        column_graph_action="build",
        column_graph_serving=serving,
    )
    print(f"built/admitted canonical graph in {time.monotonic() - build_started:.1f}s", flush=True)
    return info


def validate_response(
    response: Any,
    mode: str,
    *,
    diagnostics: bool = False,
    top_k: int = TOP_K,
    expected_route: str = "typed_hnsw",
) -> dict[str, Any]:
    expected_routes = ((expected_route,) if isinstance(expected_route, str)
                       else tuple(expected_route) if isinstance(expected_route, tuple) else ())
    if (not expected_routes or len(set(expected_routes)) != len(expected_routes)
            or any(route not in ("typed_empty", "typed_exact", "typed_hnsw")
                   for route in expected_routes)):
        raise ValueError("expected normalized-v4 route is invalid")
    identity = response.route_identity
    if (
        response.native_command_version != 4
        or len(response.documents) != top_k
        or (response.dense_work is not None) != diagnostics
        or (response.score_plane is not None) != (diagnostics and mode == "quantized_rerank")
        or identity is None
        or identity.query_mode != mode
        or identity.execution_route not in expected_routes
        or identity.diagnostics != diagnostics
        or identity.return_embedding
        or identity.embedding_vector_reads != 0
        or identity.embedding_vector_bytes != 0
        or identity.embedding_output_bytes != 0
        or any(document.embedding is not None for document in response.documents)
    ):
        observed_route = None if identity is None else identity.execution_route
        expected_description = (expected_routes[0] if len(expected_routes) == 1
                                else expected_routes)
        raise RuntimeError(
            f"Python {mode} call left the v4 production route "
            f"(expected={expected_description!r}, observed={observed_route!r})"
        )
    if mode == "exact":
        if identity.quantized_score_calls != 0 or identity.quantized_code_bytes_read != 0:
            raise RuntimeError("Python exact call crossed the SQ8 score plane")
        if identity.execution_route != "typed_empty" and (
            (identity.packed_score_calls == 0) != (identity.packed_score_candidates == 0)
            or identity.packed_score_calls > identity.packed_score_candidates
            or identity.packed_score_candidates > identity.fp32_score_calls
            or identity.packed_vector_bytes_read
                != identity.packed_score_candidates * DIMENSIONS * 4
        ):
            raise RuntimeError("Python exact route omitted consistent packed FP32 work")
        if identity.execution_route == "typed_empty" and any((
            identity.packed_score_calls,
            identity.packed_score_candidates,
            identity.packed_vector_bytes_read,
        )):
            raise RuntimeError("Python exact empty route unexpectedly carried packed work")
    if mode == "quantized_rerank":
        # The indexed packed batch scores live base rows, not mutable suffix
        # rows. A shadowed base may still traverse SQ8 codes but rerank no rows.
        if (identity.packed_score_candidates > identity.fp32_score_calls
                or identity.packed_score_calls != int(identity.packed_score_candidates > 0)
                or (identity.current_manifest_generation == identity.base_manifest_generation
                    and identity.fp32_score_calls != identity.packed_score_candidates)):
            raise RuntimeError("Python SQ8 route has inconsistent base/suffix packed FP32 work")
        if identity.execution_route == "typed_hnsw" and (
            identity.quantized_score_calls == 0
            or identity.quantized_code_bytes_read
                != identity.quantized_score_calls * DIMENSIONS
            or identity.packed_vector_bytes_read
                != identity.packed_score_candidates * DIMENSIONS * 4
        ):
            raise RuntimeError("Python SQ8 call omitted candidate generation or packed rerank")
        if identity.execution_route == "typed_exact" and (
            identity.quantized_score_calls != 0
            or identity.quantized_code_bytes_read != 0
            or identity.packed_vector_bytes_read
                != identity.packed_score_candidates * DIMENSIONS * 4
        ):
            raise RuntimeError("Python SQ8 exact route omitted the packed FP32 batch")
        if identity.execution_route == "typed_empty" and any((
            identity.quantized_score_calls,
            identity.quantized_code_bytes_read,
            identity.packed_score_calls,
            identity.packed_score_candidates,
            identity.packed_vector_bytes_read,
        )):
            raise RuntimeError("Python SQ8 empty route unexpectedly carried score work")
    if diagnostics:
        work, proof = response.dense_work, response.score_plane
        if work is None or not work.completed or not work.graph.completed or not work.output.completed:
            raise RuntimeError("Python diagnostic response omitted completed dense work")
        if mode == "exact" and proof is not None:
            raise RuntimeError("Python exact diagnostic unexpectedly returned score-plane proof")
        if mode == "exact" and (
            identity.fp32_score_calls != work.graph.base_ann_scored + work.graph.delta_scored
            or identity.packed_score_candidates > work.graph.base_ann_scored
        ):
            raise RuntimeError("Python exact diagnostic has inconsistent base/suffix FP32 work")
        if mode == "quantized_rerank" and (
            proof is None
            or not proof.completed
            or proof.packed_score_batch_calls != identity.packed_score_calls
            or proof.packed_score_candidates != identity.packed_score_candidates
            or proof.packed_vector_bytes_read != identity.packed_vector_bytes_read
            or identity.packed_score_candidates
                != proof.exact_base_rerank_score_calls + proof.exact_small_filter_score_calls
            or identity.fp32_score_calls
                != identity.packed_score_candidates + proof.exact_suffix_score_calls
            or work.graph.delta_scored != proof.exact_suffix_score_calls
            or proof.forbidden_stable_score_calls != 0
        ):
            raise RuntimeError("Python SQ8 diagnostic omitted packed same-owner proof")
    return {
        "native_command_version": response.native_command_version,
        "results": [
            {"id": document.id, "score": float(document.score)}
            for document in response.documents
        ],
        "route": asdict(identity),
    }


def python_call(
    client: TreeDBClient,
    info: Any,
    query: list[float],
    mode: str,
    *,
    diagnostics: bool = False,
    index: str = INDEX,
    top_k: int = TOP_K,
    ef_search: int = EF_SEARCH,
    rerank_candidates: int = RERANK_CANDIDATES,
    quantized_index: str = QUANTIZED_INDEX,
) -> Any:
    options: dict[str, Any] = {
        "route": "ann",
        "ef_search": ef_search,
        "query_mode": mode,
        "return_embedding": False,
        "diagnostics": diagnostics,
        "index_info": info,
    }
    if mode == "quantized_rerank":
        options.update(
            quantized_index_name=quantized_index,
            quantized_rerank_candidates=rerank_candidates,
        )
    return client.query_by_embedding(index, query, top_k, **options)


def measure_python(
    client: TreeDBClient,
    info: Any,
    queries: list[list[float]],
    *,
    index: str = INDEX,
    top_k: int = TOP_K,
    ef_search: int = EF_SEARCH,
    rerank_candidates: int = RERANK_CANDIDATES,
    quantized_index: str = QUANTIZED_INDEX,
    repetitions: int = REPETITIONS,
    warmup_queries: int = WARMUP_QUERIES,
) -> dict[str, Any]:
    for mode in ("exact", "quantized_rerank"):
        for query in queries[:warmup_queries]:
            validate_response(
                python_call(
                    client, info, query, mode, index=index, top_k=top_k,
                    ef_search=ef_search, rerank_candidates=rerank_candidates,
                    quantized_index=quantized_index,
                ),
                mode,
                top_k=top_k,
            )
    arms: dict[str, dict[str, Any]] = {
        "exact": {"mode": "exact", "repetitions": []},
        "sq8": {"mode": "quantized_rerank", "repetitions": []},
    }
    for repetition in range(repetitions):
        order = ("exact", "quantized_rerank") if repetition % 2 == 0 else (
            "quantized_rerank",
            "exact",
        )
        for arm_order, mode in enumerate(order):
            call_wall, call_cpu, observations = [], [], []
            batch_wall, batch_cpu = time.perf_counter_ns(), time.process_time_ns()
            for position, query in enumerate(queries):
                started_wall, started_cpu = time.perf_counter_ns(), time.process_time_ns()
                response = python_call(
                    client, info, query, mode, index=index, top_k=top_k,
                    ef_search=ef_search, rerank_candidates=rerank_candidates,
                    quantized_index=quantized_index,
                )
                call_cpu.append(time.process_time_ns() - started_cpu)
                call_wall.append(time.perf_counter_ns() - started_wall)
                observations.append({
                    "query": position,
                    **validate_response(response, mode, top_k=top_k),
                })
            record = {
                "ordinal": repetition,
                "arm_order": arm_order,
                "wall_nanos": time.perf_counter_ns() - batch_wall,
                "cpu_nanos": time.process_time_ns() - batch_cpu,
                "call_wall_nanos": call_wall,
                "call_cpu_nanos": call_cpu,
                "observations": observations,
            }
            arms["exact" if mode == "exact" else "sq8"]["repetitions"].append(record)
    return {
        "schema": "treedb_v4_production_gate/v1",
        "lane": "python_native_v4",
        "python": sys.version,
        "query_count": len(queries),
        "warmup_count": min(warmup_queries, len(queries)),
        "top_k": top_k,
        "ef_search": ef_search,
        "rerank_candidates": rerank_candidates,
        **arms,
    }


def measure_python_diagnostics(
    client: TreeDBClient, info: Any, queries: list[list[float]]
) -> dict[str, Any]:
    samples: dict[str, dict[str, list[int]]] = {
        "exact": {"wall_nanos": [], "cpu_nanos": []},
        "sq8": {"wall_nanos": [], "cpu_nanos": []},
    }
    for position, query in enumerate(queries[:DIAGNOSTIC_QUERY_COUNT]):
        order = ("exact", "quantized_rerank") if position % 2 == 0 else (
            "quantized_rerank",
            "exact",
        )
        for mode in order:
            started_wall, started_cpu = time.perf_counter_ns(), time.process_time_ns()
            response = python_call(client, info, query, mode, diagnostics=True)
            elapsed_cpu = time.process_time_ns() - started_cpu
            elapsed_wall = time.perf_counter_ns() - started_wall
            validate_response(response, mode, diagnostics=True)
            arm = samples["exact" if mode == "exact" else "sq8"]
            arm["wall_nanos"].append(elapsed_wall)
            arm["cpu_nanos"].append(elapsed_cpu)

    def summarize(sample: dict[str, list[int]]) -> dict[str, Any]:
        count = len(sample["wall_nanos"])
        return {
            "calls": count,
            "wall_ns_per_query": sum(sample["wall_nanos"]) / count,
            "cpu_ns_per_query": sum(sample["cpu_nanos"]) / count,
            "p50_ns": nearest_rank(sample["wall_nanos"], 0.50),
            "p95_ns": nearest_rank(sample["wall_nanos"], 0.95),
            **sample,
        }

    return {
        "schema": "treedb_v4_diagnostic_cost/v1",
        "qualifying": False,
        "reason": "bounded post-gate observability sample; excluded from production comparisons",
        "query_count_per_arm": DIAGNOSTIC_QUERY_COUNT,
        "order": "paired_alternating_first_arm",
        "exact": summarize(samples["exact"]),
        "sq8": summarize(samples["sq8"]),
    }


def run_go_helper(helper: Path, arguments: list[str]) -> dict[str, Any]:
    completed = subprocess.run(
        [str(helper), *arguments],
        cwd=SOURCE,
        text=True,
        capture_output=True,
        timeout=1800,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"Go gate helper failed: {completed.stderr.strip()}")
    value = json.loads(completed.stdout)
    if value.get("schema") != "treedb_v4_production_gate/v1":
        raise RuntimeError("Go gate helper returned an unexpected schema")
    return value


def capture_profile(
    controller: ServiceController,
    client: TreeDBClient,
    info: Any,
    queries: list[list[float]],
    directory: Path,
    *,
    index: str = INDEX,
    top_k: int = TOP_K,
    ef_search: int = EF_SEARCH,
    rerank_candidates: int = RERANK_CANDIDATES,
    quantized_index: str = QUANTIZED_INDEX,
) -> dict[str, Any]:
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            controller.capture_profiles,
            directory,
            profile_seconds=5,
            capture_timeout=12,
        )
        calls = 0
        while not future.done():
            mode = "exact" if calls % 2 == 0 else "quantized_rerank"
            validate_response(
                python_call(
                    client, info, queries[calls % len(queries)], mode,
                    index=index, top_k=top_k, ef_search=ef_search,
                    rerank_candidates=rerank_candidates, quantized_index=quantized_index,
                ),
                mode,
                top_k=top_k,
            )
            calls += 1
        capture = future.result()
    capture["production_calls_during_capture"] = calls
    return capture


def nearest_rank(values: list[int], fraction: float) -> int:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def summarize_arm(arm: dict[str, Any]) -> dict[str, Any]:
    repetitions = []
    for record in arm["repetitions"]:
        calls = len(record["call_wall_nanos"])
        public_wall = sum(record["call_wall_nanos"])
        public_cpu = sum(record["call_cpu_nanos"])
        repetitions.append(
            {
                "ordinal": record["ordinal"],
                "arm_order": record["arm_order"],
                "qps": calls * 1e9 / public_wall,
                "wall_ns_per_query": public_wall / calls,
                "cpu_ns_per_query": public_cpu / calls,
                "p50_ns": nearest_rank(record["call_wall_nanos"], 0.50),
                "p95_ns": nearest_rank(record["call_wall_nanos"], 0.95),
            }
        )
    keys = ("qps", "wall_ns_per_query", "cpu_ns_per_query", "p50_ns", "p95_ns")
    return {
        "per_repetition": repetitions,
        "median": {key: statistics.median(row[key] for row in repetitions) for key in keys},
    }


def lane_arms(lane: dict[str, Any]) -> dict[str, Any]:
    return {"exact": summarize_arm(lane["exact"]), "sq8": summarize_arm(lane["sq8"])}


_OWNER_IDENTITY_FIELDS = (
    ("schema_hash", "SchemaHash"),
    ("schema_generation", "SchemaGeneration"),
    ("base_manifest_generation", "BaseManifestGeneration"),
    ("base_manifest_checksum", "BaseManifestChecksum"),
    ("current_manifest_generation", "CurrentManifestGeneration"),
    ("current_manifest_checksum", "CurrentManifestChecksum"),
    ("current_coverage_lsn", "CurrentCoverageLSN"),
)


def immutable_owner_identity(lanes: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Prove every compared observation used one immutable graph publication."""
    if not lanes:
        raise ValueError("production matrix has no lanes")
    expected: dict[str, int] | None = None
    checked = 0
    for lane_name, lane in lanes.items():
        for arm_name in ("exact", "sq8"):
            repetitions = lane.get(arm_name, {}).get("repetitions")
            if not isinstance(repetitions, list) or not repetitions:
                raise ValueError(f"{lane_name}.{arm_name} has no repetitions")
            for repetition_index, repetition in enumerate(repetitions):
                observations = repetition.get("observations")
                if not isinstance(observations, list) or not observations:
                    raise ValueError(
                        f"{lane_name}.{arm_name}[{repetition_index}] has no observations"
                    )
                for observation_index, observation in enumerate(observations):
                    route = observation.get("route")
                    if not isinstance(route, dict):
                        raise ValueError(
                            f"{lane_name}.{arm_name}[{repetition_index}]"
                            f".observations[{observation_index}] has no route identity"
                        )
                    source = route.get("receipt", route)
                    if not isinstance(source, dict):
                        raise ValueError(
                            f"{lane_name}.{arm_name}[{repetition_index}]"
                            f".observations[{observation_index}] has an invalid route receipt"
                        )
                    identity: dict[str, int] = {}
                    for canonical, receipt_name in _OWNER_IDENTITY_FIELDS:
                        value = source.get(receipt_name if "receipt" in route else canonical)
                        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                            raise ValueError(
                                f"{lane_name}.{arm_name}[{repetition_index}]"
                                f".observations[{observation_index}] has invalid {canonical}"
                            )
                        identity[canonical] = value
                    if expected is None:
                        expected = identity
                    elif identity != expected:
                        raise ValueError(
                            "production lanes crossed graph publications: "
                            f"expected {expected}, observed {identity} at "
                            f"{lane_name}.{arm_name}[{repetition_index}]"
                            f".observations[{observation_index}]"
                        )
                    checked += 1
    if expected is None:
        raise ValueError("production matrix has no route observations")
    return {
        **expected,
        "validated_lanes": sorted(lanes),
        "validated_observations": checked,
    }


def evaluate(lanes: dict[str, dict[str, Any]]) -> tuple[dict[str, Any], list[str]]:
    summaries = {name: lane_arms(lane) for name, lane in lanes.items()}
    checks: dict[str, Any] = {}
    failures: list[str] = []
    for name in ("go_native", "python_native"):
        exact, sq8 = summaries[name]["exact"]["median"], summaries[name]["sq8"]["median"]
        lane_checks = {
            "qps_gain_at_least_10pct": sq8["qps"] >= exact["qps"] * 1.10,
            "p50_lower": sq8["p50_ns"] < exact["p50_ns"],
            "p95_no_worse": sq8["p95_ns"] <= exact["p95_ns"],
            "client_cpu_delta_ns_per_query": sq8["cpu_ns_per_query"] - exact["cpu_ns_per_query"],
        }
        lane_checks["client_cpu_delta_at_most_25us"] = (
            lane_checks["client_cpu_delta_ns_per_query"] <= 25_000
        )
        checks[name] = lane_checks
        for check, passed in lane_checks.items():
            if check != "client_cpu_delta_ns_per_query" and not passed:
                failures.append(f"{name}: {check}")

    collection = summaries["collection"]
    service = summaries["service"]
    advantage = (
        collection["exact"]["median"]["wall_ns_per_query"]
        - collection["sq8"]["median"]["wall_ns_per_query"]
    )
    overhead_checks: dict[str, Any] = {"collection_sq8_advantage_ns": advantage}
    for name in ("go_native", "python_native"):
        exact_overhead = (
            summaries[name]["exact"]["median"]["wall_ns_per_query"]
            - service["exact"]["median"]["wall_ns_per_query"]
        )
        sq8_overhead = (
            summaries[name]["sq8"]["median"]["wall_ns_per_query"]
            - service["sq8"]["median"]["wall_ns_per_query"]
        )
        consumed = sq8_overhead - exact_overhead
        passed = advantage > 0 and max(0, consumed) <= advantage / 2
        overhead_checks[name] = {
            "exact_overhead_above_service_ns": exact_overhead,
            "sq8_overhead_above_service_ns": sq8_overhead,
            "sq8_specific_overhead_consumption_ns": consumed,
            "at_most_half_collection_advantage": passed,
        }
        if not passed:
            failures.append(f"{name}: transport overhead exceeds half the collection advantage")
    checks["transport"] = overhead_checks
    return {"summaries": summaries, "checks": checks}, failures


def pprof_top(go: str, service_binary: Path, capture: dict[str, Any], run_dir: Path) -> dict[str, Any]:
    cpu = capture.get("captures", {}).get("cpu", {})
    if cpu.get("status") != "captured":
        return {"status": "failed", "reason": "CPU profile was not captured"}
    output = run_dir / "profiles" / "cpu_top.txt"
    completed = subprocess.run(
        [go, "tool", "pprof", "-top", "-nodecount=0", str(service_binary), cpu["path"]],
        cwd=SOURCE,
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    output.write_text(completed.stdout + completed.stderr, encoding="utf-8")
    forbidden = [
        name
        for name in (
            "decodeDenseV3ResultDocument",
            "denseV3EmbeddingScoreMatches",
            "documentservice.scoreEmbedding",
            "documentservice.(*Service).scanDocuments",
        )
        if name in completed.stdout
    ]
    return {
        "status": "captured" if completed.returncode == 0 else "failed",
        "path": str(output),
        "forbidden_full_document_rescore_symbols": forbidden,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--serving", type=Path, required=True)
    parser.add_argument("--service-bin", type=Path, required=True)
    parser.add_argument("--go-helper", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--go", default="go")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_dir = args.run_dir.resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    vectors, queries, dataset_identity = load_inputs(args.dataset.resolve())
    serving = strict_json(args.serving.resolve())
    validate_column_graph_serving(serving)
    service_binary, helper = args.service_bin.resolve(), args.go_helper.resolve()
    if not service_binary.is_file() or not helper.is_file():
        raise ValueError("service and Go gate helper binaries must exist")
    http_address, native_address, diagnostic_address = free_address(), free_address(), free_address()
    controller = ServiceController(
        service_binary,
        "http://" + http_address,
        run_dir / "data",
        "command_wal_durable",
        120,
        120,
        diagnostics_url="http://" + diagnostic_address,
        block_profile_rate=0,
        mutex_profile_fraction=0,
        diagnostics_timeout=5,
        native_address=native_address,
    )
    control = native = None
    artifact: dict[str, Any] = {
        "schema": SCHEMA,
        "rows": ROWS,
        "dimensions": DIMENSIONS,
        "queries": QUERY_COUNT,
        "top_k": TOP_K,
        "ef_search": EF_SEARCH,
        "rerank_candidates": RERANK_CANDIDATES,
        "repetitions": REPETITIONS,
        "warmup_queries_per_arm": WARMUP_QUERIES,
        "arm_order": "alternate_first_complete_arm_batch_by_repetition",
        "dataset": dataset_identity,
        "serving_sha256": sha256(args.serving.resolve()),
        "service_binary_sha256": sha256(service_binary),
        "go_helper_sha256": sha256(helper),
        "runner_sha256": sha256(Path(__file__).resolve()),
        "return_embedding": False,
        "diagnostics": False,
    }
    try:
        controller.start()
        control = TreeDBClient(controller.url, timeout=600)
        native = TreeDBClient(controller.url, timeout=60, native_address=native_address)
        info = create_and_load(control, native, vectors, serving)
        go_native = run_go_helper(
            helper,
            [
                "-lane=native",
                f"-address={native_address}",
                f"-dataset={args.dataset.resolve()}",
                f"-index={INDEX}",
                f"-generation={info.generation}",
                f"-quantized-index={QUANTIZED_INDEX}",
            ],
        )
        print("completed Go native v4 lane", flush=True)
        python_native = measure_python(native, info, queries)
        print("completed Python native v4 lane", flush=True)
        artifact["diagnostic_cost"] = measure_python_diagnostics(native, info, queries)
        print("completed non-qualifying Python diagnostic-cost sample", flush=True)
        profile_capture = capture_profile(controller, native, info, queries, run_dir / "profiles")
        artifact["profile_capture"] = profile_capture
        artifact["profile_analysis"] = pprof_top(args.go, service_binary, profile_capture, run_dir)
        native.close()
        native = None
        control.close()
        control = None
        controller.stop()
        seams = run_go_helper(
            helper,
            [
                "-lane=seams",
                f"-dir={run_dir / 'data'}",
                f"-dataset={args.dataset.resolve()}",
                f"-serving={args.serving.resolve()}",
                f"-index={INDEX}",
                f"-quantized-index={QUANTIZED_INDEX}",
            ],
        )
        seam_records = seams.get("sub_lanes")
        if not isinstance(seam_records, list) or len(seam_records) != 3 or any(
            not isinstance(lane, dict) for lane in seam_records
        ):
            raise RuntimeError("Go seam lane inventory is malformed")
        sub_lanes = {lane.get("lane"): lane for lane in seam_records}
        if set(sub_lanes) != {"collection_search", "collection_fetch", "service"}:
            raise RuntimeError("Go seam lane inventory is incomplete or duplicated")
        lanes = {
            "go_native": go_native,
            "python_native": python_native,
            "collection_search": sub_lanes["collection_search"],
            "collection_fetch": sub_lanes["collection_fetch"],
            "service": sub_lanes["service"],
        }
        owner_identity: dict[str, Any]
        owner_identity_failure = ""
        try:
            owner_identity = immutable_owner_identity(lanes)
        except ValueError as exc:
            owner_identity = {"error": str(exc)}
            owner_identity_failure = str(exc)
        evaluation, failures = evaluate({
            "go_native": go_native,
            "python_native": python_native,
            "collection": sub_lanes["collection_fetch"],
            "service": sub_lanes["service"],
        })
        if owner_identity_failure:
            failures.insert(0, owner_identity_failure)
        for arm in ("exact", "sq8"):
            production = evaluation["summaries"]["python_native"][arm]["median"]
            diagnostic = artifact["diagnostic_cost"][arm]
            diagnostic["production_reference_wall_ns_per_query"] = production[
                "wall_ns_per_query"
            ]
            diagnostic["production_reference_cpu_ns_per_query"] = production[
                "cpu_ns_per_query"
            ]
            diagnostic["wall_cost_above_production_ns_per_query"] = (
                diagnostic["wall_ns_per_query"] - production["wall_ns_per_query"]
            )
            diagnostic["cpu_cost_above_production_ns_per_query"] = (
                diagnostic["cpu_ns_per_query"] - production["cpu_ns_per_query"]
            )
        profile_ok = (
            artifact["profile_analysis"].get("status") == "captured"
            and not artifact["profile_analysis"].get("forbidden_full_document_rescore_symbols")
        )
        if not profile_ok:
            failures.append("production CPU profile is missing or contains a full-document rescore symbol")
        artifact.update(
            state="passed" if not failures else "failed",
            failures=failures,
            lanes=lanes,
            immutable_owner_identity=owner_identity,
            evaluation=evaluation,
            service_log=str(controller.log_path),
        )
    finally:
        if native is not None:
            native.close()
        if control is not None:
            control.close()
        controller.stop()
    artifact_path = run_dir / "q3_v4_production_gate.json"
    artifact_path.write_text(
        json.dumps(artifact, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(f"wrote {artifact_path}", flush=True)
    if artifact["state"] != "passed":
        for failure in artifact["failures"]:
            print(f"gate failure: {failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
