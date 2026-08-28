#!/usr/bin/env python3
"""Execute the frozen Minima operation manifest against Qdrant.

The output is deliberately raw/partial evidence. Only the Go validator may
turn backend evidence into a qualification result.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import argparse
import errno
import hashlib
from decimal import Decimal
import importlib.metadata
import json
import math
import os
import socket
import platform
import stat
import struct
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.parse
import uuid
from pathlib import Path
from typing import Any, Callable, Iterable

RESOURCE_SEMANTICS = {
    "rss_bytes": "sum of positive per-process end-minus-baseline RSS growth; endpoint delta, not peak RSS",
    "cpu_seconds": "sum of positive per-process end-minus-baseline CPU seconds",
    "disk_bytes": "sum of positive per-process-segment end-minus-baseline durable storage bytes",
}

MANIFEST_SCHEMA = "treedb_rag_minima_manifest/v1"
ARTIFACT_SCHEMA = "treedb_rag_application/minima_v4"
SERVER_VERSION = CLIENT_VERSION = "1.19.0"
READINESS_SNAPSHOT_LIMIT = 256
READINESS_RESOURCE_INTERVAL_SECONDS = 5.0
READINESS_DIAGNOSTIC_TIMEOUT_SECONDS = 1.0
PRODUCTION_HNSW_CONFIG = {
    "m": 16, "ef_construct": 100, "full_scan_threshold": 10000,
    "max_indexing_threads": 0, "on_disk": False,
}
PRODUCTION_OPTIMIZERS_CONFIG = {
    "deleted_threshold": 0.2, "vacuum_min_vector_number": 1000,
    "default_segment_number": 0, "indexing_threshold": 10000,
    "flush_interval_sec": 5, "max_optimization_threads": 1,
}
INITIAL_UPLOAD_HNSW_CONFIG = {**PRODUCTION_HNSW_CONFIG, "m": 0}
INITIAL_UPLOAD_OPTIMIZERS_CONFIG = {**PRODUCTION_OPTIMIZERS_CONFIG, "indexing_threshold": 0}
RESOURCE_STARVATION_MARKERS = (
    "out of memory", "cannot allocate memory", "memory allocation failed",
    "no space left on device", "disk full",
)
GENERATOR = (
    "ordinal-v3:id=minima/<scenario>/<ordinal:06d>;content=minima:<scenario>:<ordinal>;"
    "vector=[s,sqrt(1-s*s),0x6],s=0.9-ordinal*0.000003;"
    "oracle=cosine(float32(vector),float32([1,0x7]));"
    "defaults=<scenario>-other-user-%02d(ordinal%31),/<scenario>/other/%02d.txt(ordinal%97)"
)
MANIFEST_FLOAT_TOLERANCE = 1e-15
FROZEN_HASHES = {
    "corpus_sha256": "0b1a213652fc97a4460f254f4d9e90f027e4b30ef6111a26807591ade10923e1",
    "query_sha256": "eb4f076023e361b9a2cf18a06a5e1d69e5023c304da25d38848fc7011575288a",
    "operation_sha256": "08f38acec8a5ad746dbffadef5ad9c198852c88d1920746229cb0733bfd9c434",
    "expected_state_sha256": "c2986f2b44e67b33e7bb3f92f5f92b1316e60117ed2505bef73327e0b1e5687f",
}
TIMED_EXECUTION_SHA256 = "84b8eb10e5f86c558264d00e8cae2c6844683aff2b8bca1d76cafe6b06890ea4"
REINDEX_EXECUTION_SHA256 = "99823f1eac0fb27dce81e21e0cf5884019c6a911c410be11b675b2315cbde534"
OPERATION_NAMES = [
    "ensure_compatible_collection", "initial_batch_insert", "warmup_search",
    "timed_search_with_batch_insert", "reindex_delete_by_user_and_fpath_while_reading",
    "reindex_replacement_insert_while_reading", "reindex_visibility_probe", "explicit_update",
    "update_visibility_probe", "explicit_delete", "delete_visibility_probe",
    "empty_user_and_file_probes", "close", "reopen", "idempotent_ensure_after_reopen",
    "final_manifest_and_oracle_comparison",
]
CONFIG_KEYS = [
    "collection", "vector_field", "content_field", "dimension", "metric", "scalar_fields", "top_k",
    "batch_size", "reader_concurrency", "writer_concurrency", "warmup_queries", "timed_queries",
    "lookup_limit", "order_tolerance", "score_tolerance", "ordering", "completion_boundary", "timing_boundary",
]
SCENARIO_KEYS = [
    "name", "shape", "corpus_rows", "eligible_start", "eligible_rows", "broad_start", "broad_rows",
    "narrow_start", "narrow_rows", "filter", "user_id", "fpath", "selectivity",
    "closer_cross_tenant_distractor_rows", "generator",
]
QUERY_KEYS = [
    "scenario", "vector", "initial_oracle_ids", "initial_oracle_scores", "final_oracle_ids", "final_oracle_scores",
]
DOCUMENT_KEYS = ["id", "content", "vector", "user_id", "fpath"]
OPERATION_KEYS = ["ordinal", "name", "target", "timed", "effect", "insert_ranges", "filter", "ids", "documents", "schedule", "timed_reader_plan", "concurrent_mutation_plan"]


def require_object(value: Any, keys: list[str], label: str, optional: set[str] = frozenset()) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    unknown, missing = set(value) - set(keys), set(keys) - set(optional) - set(value)
    if unknown or missing:
        raise ValueError(f"{label} keys mismatch: missing={sorted(missing)} unknown={sorted(unknown)}")
    return value


def exact_int(value: Any, label: str, minimum: int) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{label} must be an integer >= {minimum}")
    return value


def finite(value: Any, label: str) -> float:
    if type(value) not in (int, float) or not math.isfinite(float(value)):
        raise ValueError(f"{label} must be finite")
    return float(value)


def ordered(value: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    return {key: value[key] for key in keys if key in value}


def canonical_document(value: dict[str, Any]) -> dict[str, Any]:
    return ordered(value, DOCUMENT_KEYS)


def canonical_operation(value: dict[str, Any]) -> dict[str, Any]:
    out = ordered(value, OPERATION_KEYS)
    if "insert_ranges" in out:
        out["insert_ranges"] = [ordered(row, ["scenario", "start", "rows"]) for row in out["insert_ranges"]]
    if "filter" in out:
        out["filter"] = ordered(out["filter"], ["user_id", "fpath"])
    if "documents" in out:
        out["documents"] = [canonical_document(row) for row in out["documents"]]
    if "schedule" in out:
        out["schedule"] = [ordered(row, ["ordinal", "actor", "scenario", "query_ordinal", "insert_start", "insert_rows"]) for row in out["schedule"]]
    if "timed_reader_plan" in out:
        plan = ordered(out["timed_reader_plan"], [
            "query_count", "scenario_order", "reader_concurrency", "writer_concurrency", "assignment", "rounds",
        ])
        plan["rounds"] = [
            {
                **ordered(row, ["ordinal", "query_start", "query_count"]),
                "insert_range": ordered(row["insert_range"], ["scenario", "start", "rows"]),
                **ordered(row, ["start_barrier", "end_barrier"]),
            }
            for row in plan["rounds"]
        ]
        out["timed_reader_plan"] = plan
    if "concurrent_mutation_plan" in out:
        plan = out["concurrent_mutation_plan"]
        out["concurrent_mutation_plan"] = {
            **ordered(plan, ["mutation", "reader_concurrency"]),
            "reader_assignments": [
                ordered(row, ["reader", "query_ordinal", "scenario"])
                for row in plan["reader_assignments"]
            ],
            **ordered(plan, ["start_barrier", "end_barrier"]),
        }
    return out


def go_json(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if type(value) is int:
        return str(value)
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("Go JSON does not support non-finite floats")
        if value == 0:
            return "-0" if math.copysign(1, value) < 0 else "0"
        text = repr(value)
        absolute = abs(value)
        if 1e-6 <= absolute < 1e21:
            return format(Decimal(text), "f")
        if "e" not in text:
            text = format(value, ".15e").rstrip("0").rstrip(".")
        mantissa, exponent = text.lower().split("e")
        sign = "+" if exponent.startswith("+") else "-"
        digits = exponent.lstrip("+-0") or "0"
        return f"{mantissa}e{sign}{digits}"
    if isinstance(value, str):
        text = json.dumps(value, ensure_ascii=False)
        return text.replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    if isinstance(value, list):
        return "[" + ",".join(go_json(item) for item in value) + "]"
    if isinstance(value, dict):
        return "{" + ",".join(f"{go_json(str(key))}:{go_json(item)}" for key, item in value.items()) + "}"
    raise TypeError(f"unsupported Go JSON value {type(value).__name__}")


def go_digest(value: Any) -> str:
    return hashlib.sha256(go_json(value).encode()).hexdigest()


def manifest_hashes(manifest: dict[str, Any]) -> dict[str, str]:
    return {
        "corpus_sha256": go_digest([ordered(row, SCENARIO_KEYS) for row in manifest["corpora"]]),
        "query_sha256": go_digest([ordered(row, QUERY_KEYS) for row in manifest["queries"]]),
        "operation_sha256": go_digest([canonical_operation(row) for row in manifest["operations"]]),
    }


def scenario_map(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {row["name"]: row for row in manifest["corpora"]}


def generated_document(spec: dict[str, Any], ordinal: int) -> dict[str, Any]:
    rows = exact_int(spec["corpus_rows"], "corpus_rows", 1)
    if not 0 <= ordinal < rows:
        raise ValueError(f"document ordinal {ordinal} outside [0,{rows})")
    user_id = f"{spec['name']}-other-user-{ordinal % 31:02d}"
    fpath = f"/{spec['name']}/other/{ordinal % 97:02d}.txt"
    if spec["filter"] == "user_id" and spec["eligible_start"] <= ordinal < spec["eligible_start"] + spec["eligible_rows"]:
        user_id = spec["user_id"]
    if spec["filter"] == "user_id+fpath":
        broad_start, broad_rows = spec.get("broad_start", 0), spec.get("broad_rows", 0)
        narrow_start, narrow_rows = spec.get("narrow_start", 0), spec.get("narrow_rows", 0)
        if broad_start <= ordinal < broad_start + broad_rows:
            user_id = spec["user_id"]
        if narrow_start <= ordinal < narrow_start + narrow_rows:
            fpath = spec["fpath"]
    score, vector = 0.9 - ordinal * 0.000003, [0.0] * 8
    vector[0], vector[1] = score, math.sqrt(1 - score * score)
    return {"id": f"minima/{spec['name']}/{ordinal:06d}", "content": f"minima:{spec['name']}:{ordinal}", "vector": vector, "user_id": user_id, "fpath": fpath}


def matches(document: dict[str, Any], spec: dict[str, Any]) -> bool:
    return document["user_id"] == spec.get("user_id") and (spec["filter"] == "user_id" or document["fpath"] == spec.get("fpath"))


def cosine(vector: list[float], query: list[float]) -> float:
    dot = sum(a * b for a, b in zip(vector, query, strict=True))
    norm = math.sqrt(sum(v * v for v in vector)) * math.sqrt(sum(v * v for v in query))
    return dot / norm if norm else 0.0

def normalized_f32_vector(vector: list[float]) -> list[float]:
    values = [f32(finite(value, "vector component")) for value in vector]
    norm = math.sqrt(sum(value * value for value in values))
    if not norm:
        raise ValueError("vector norm must be positive")
    return [f32(value / norm) for value in values]

def f32(value: float) -> float:
    return struct.unpack(">f", struct.pack(">f", value))[0]


def document_score(document: dict[str, Any]) -> float:
    stored = [f32(value) for value in document["vector"]]
    squared = sum(value * value for value in stored)
    return stored[0] / math.sqrt(squared) if squared else -math.inf


def final_oracle(manifest: dict[str, Any], spec: dict[str, Any]) -> tuple[list[str], list[float]]:
    deleted: set[str] = set()
    overrides: dict[str, dict[str, Any]] = {}
    additions: dict[str, dict[str, Any]] = {}
    for operation in manifest["operations"]:
        if operation["target"] != spec["name"]:
            continue
        if operation["effect"] == "delete":
            deleted.update(operation.get("ids", []))
        elif operation["effect"] == "update":
            overrides.update((row["id"], row) for row in operation.get("documents", []))
        elif operation["effect"] == "insert" and operation.get("documents"):
            additions.update((row["id"], row) for row in operation["documents"])
    candidates: list[dict[str, Any]] = []
    needed = manifest["config"]["top_k"] + len(deleted) + len(overrides) + 1
    for ordinal in range(spec["eligible_start"], spec["eligible_start"] + spec["eligible_rows"]):
        document = generated_document(spec, ordinal)
        if document["id"] in deleted:
            continue
        document = overrides.get(document["id"], document)
        if matches(document, spec):
            candidates.append(document)
        if len(candidates) >= needed:
            break
    candidates.extend(row for row in additions.values() if row["id"] not in deleted and matches(row, spec))
    ranked = sorted(candidates, key=lambda row: (-document_score(row), row["id"]))[:manifest["config"]["top_k"]]
    return [row["id"] for row in ranked], [document_score(row) for row in ranked]


def payload_corpus_digest(corpora: list[dict[str, Any]]) -> str:
    payload = []
    for spec in corpora:
        row = ordered(spec, ["name", "shape", "corpus_rows", "eligible_start", "eligible_rows"])
        for key in ("broad_start", "broad_rows", "narrow_start", "narrow_rows"):
            if spec.get(key):
                row[key] = spec[key]
        row["filter"] = spec["filter"]
        if spec.get("user_id"):
            row["user_id"] = spec["user_id"]
        if spec.get("fpath"):
            row["fpath"] = spec["fpath"]
        row["selectivity"] = spec["selectivity"]
        row["payload_generator"] = (
            "id=minima/<scenario>/<ordinal:06d>;content=minima:<scenario>:<ordinal>;"
            "defaults=<scenario>-other-user-%02d(ordinal%31),/<scenario>/other/%02d.txt(ordinal%97)"
        )
        payload.append(row)
    return go_digest(payload)


def expected_state_hash(manifest: dict[str, Any]) -> str:
    specs, live_rows, mutations = scenario_map(manifest), {}, []
    live_rows.update((name, 0) for name in sorted(specs))
    for operation in manifest["operations"]:
        effect = operation["effect"]
        if effect == "none":
            continue
        if effect == "insert":
            for insertion in operation.get("insert_ranges", []):
                spec = specs.get(insertion["scenario"])
                if spec is None or insertion["start"] < 0 or insertion["rows"] <= 0 or insertion["start"] + insertion["rows"] > spec["corpus_rows"]:
                    raise ValueError(f"invalid insert range in operation {operation['ordinal']}")
                live_rows[insertion["scenario"]] += insertion["rows"]
            if operation.get("documents"):
                if operation["target"] not in specs:
                    raise ValueError(f"insert targets unknown scenario {operation['target']!r}")
                live_rows[operation["target"]] += len(operation["documents"])
        elif effect == "delete":
            if operation["target"] not in specs or not operation.get("ids"):
                raise ValueError(f"invalid delete operation {operation['ordinal']}")
            live_rows[operation["target"]] -= len(operation["ids"])
        elif effect == "update":
            if operation["target"] not in specs or not operation.get("documents"):
                raise ValueError(f"invalid update operation {operation['ordinal']}")
        else:
            raise ValueError(f"unknown effect {effect!r}")
        mutation = {"ordinal": operation["ordinal"], "effect": effect, "target": operation["target"]}
        if operation.get("insert_ranges"):
            mutation["insert_ranges"] = [ordered(row, ["scenario", "start", "rows"]) for row in operation["insert_ranges"]]
        if operation.get("ids"):
            mutation["ids"] = operation["ids"]
        if operation.get("documents"):
            mutation["documents"] = [
                ordered(row, ["id", "content", "user_id", "fpath"])
                for row in operation["documents"]
            ]
        mutations.append(mutation)
    for name, spec in specs.items():
        want = spec["corpus_rows"] - (1 if name == "small" else 0)
        if live_rows[name] != want:
            raise ValueError(f"{name} live rows={live_rows[name]} want {want}")
    return go_digest({"base_payload_sha256": payload_corpus_digest(manifest["corpora"]), "live_rows": live_rows, "mutations": mutations})


def intervals_overlap(first_start: int, first_end: int, second_start: int, second_end: int) -> bool:
    return (
        0 <= first_start < first_end
        and 0 <= second_start < second_end
        and first_start < second_end
        and second_start < first_end
    )


def await_concurrent_futures(futures: Iterable[Any]) -> None:
    errors: list[BaseException] = []
    for future in futures:
        try:
            future.result()
        except BaseException as exc:
            errors.append(exc)
    for error in errors:
        if not isinstance(error, threading.BrokenBarrierError):
            raise error
    if errors:
        raise errors[0]


def timed_trace_digest(trace: dict[str, list[dict[str, Any]]]) -> str:
    lines = [
        f"query|ordinal={row['ordinal']}|round={row['round']}|reader={row['reader']}|scenario={row['scenario']}|"
        f"started_monotonic_ns={row['started_monotonic_ns']}|ended_monotonic_ns={row['ended_monotonic_ns']}\n"
        for row in trace["queries"]
    ]
    for row in trace["rounds"]:
        insertion = row["insert_range"]
        lines.append(
            f"round|ordinal={row['ordinal']}|query_start={row['query_start']}|query_count={row['query_count']}|"
            f"insert={insertion['scenario']}:{insertion['start']}:{insertion['rows']}|"
            f"start={row['start_barrier']}|end={row['end_barrier']}|"
            f"writer_started_monotonic_ns={row['writer_started_monotonic_ns']}|"
            f"writer_ended_monotonic_ns={row['writer_ended_monotonic_ns']}\n"
        )
    return hashlib.sha256("".join(lines).encode()).hexdigest()


def timed_execution_digest(plan: dict[str, Any]) -> str:
    rounds = []
    queries = []
    for round_value in plan["rounds"]:
        base = (round_value["ordinal"] + 1) * 1_000_000
        rounds.append({
            **round_value,
            "writer_started_monotonic_ns": base + 100,
            "writer_ended_monotonic_ns": base + 900,
        })
        begin = round_value["query_start"]
        end = begin + round_value["query_count"]
        for ordinal in range(begin, end):
            started = base + 200 + (ordinal - begin) * 2
            queries.append({
                "ordinal": ordinal, "round": round_value["ordinal"],
                "reader": ordinal % plan["reader_concurrency"],
                "scenario": plan["scenario_order"][ordinal % len(plan["scenario_order"])],
                "started_monotonic_ns": started, "ended_monotonic_ns": started + 1,
            })
    return timed_trace_digest({"queries": queries, "rounds": rounds})


def reindex_trace_digest(trace: dict[str, list[dict[str, Any]]]) -> str:
    lines = []
    for operation in trace["operations"]:
        lines.append(
            f"reindex|operation={operation['operation_ordinal']}|mutation={operation['mutation']}|"
            f"start={operation['start_barrier']}|end={operation['end_barrier']}|"
            f"mutation_started_monotonic_ns={operation['mutation_started_monotonic_ns']}|"
            f"mutation_ended_monotonic_ns={operation['mutation_ended_monotonic_ns']}\n"
        )
        for query in operation["reader_queries"]:
            lines.append(
                f"reindex_query|operation={operation['operation_ordinal']}|reader={query['reader']}|"
                f"query_ordinal={query['query_ordinal']}|scenario={query['scenario']}|"
                f"started_monotonic_ns={query['started_monotonic_ns']}|"
                f"ended_monotonic_ns={query['ended_monotonic_ns']}\n"
            )
    return hashlib.sha256("".join(lines).encode()).hexdigest()
def expected_reindex_execution(manifest: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    operations = []
    for operation in manifest["operations"]:
        plan = operation.get("concurrent_mutation_plan")
        if plan is None:
            continue
        base = (operation["ordinal"] + 1) * 1_000_000
        reader_queries = []
        for assignment in plan["reader_assignments"]:
            started = base + 200 + assignment["reader"] * 10
            reader_queries.append({
                **assignment,
                "started_monotonic_ns": started,
                "ended_monotonic_ns": started + 1,
            })
        operations.append({
            "operation_ordinal": operation["ordinal"], "mutation": plan["mutation"],
            "start_barrier": plan["start_barrier"], "end_barrier": plan["end_barrier"],
            "mutation_started_monotonic_ns": base + 100,
            "mutation_ended_monotonic_ns": base + 900,
            "reader_queries": reader_queries,
        })
    return {"operations": operations}



def validate_timed_plan(manifest: dict[str, Any]) -> dict[str, Any]:
    operation = manifest["operations"][3]
    if operation.get("schedule"):
        raise ValueError("timed operation must use timed_reader_plan, not a serial schedule")
    plan = require_object(operation.get("timed_reader_plan"), [
        "query_count", "scenario_order", "reader_concurrency", "writer_concurrency", "assignment", "rounds",
    ], "timed_reader_plan")
    config, ranges = manifest["config"], operation.get("insert_ranges", [])
    scenario_order = [row["name"] for row in manifest["corpora"]]
    if not ranges or config["timed_queries"] % len(ranges):
        raise ValueError("timed query count must divide evenly across insert ranges")
    per_round = config["timed_queries"] // len(ranges)
    assignment = (
        f"round=ordinal/{per_round};reader=ordinal%{config['reader_concurrency']};"
        f"scenario=scenario_order[ordinal%{len(scenario_order)}]"
    )
    if (
        plan["query_count"] != config["timed_queries"]
        or plan["scenario_order"] != scenario_order
        or plan["reader_concurrency"] != config["reader_concurrency"]
        or plan["writer_concurrency"] != config["writer_concurrency"]
        or plan["assignment"] != assignment
        or len(plan["rounds"]) != len(ranges)
    ):
        raise ValueError("timed_reader_plan does not match frozen config/assignment")
    for ordinal, (round_value, insertion) in enumerate(zip(plan["rounds"], ranges, strict=True)):
        require_object(round_value, [
            "ordinal", "query_start", "query_count", "insert_range", "start_barrier", "end_barrier",
        ], f"timed_reader_plan.rounds[{ordinal}]")
        require_object(round_value["insert_range"], ["scenario", "start", "rows"], f"timed round {ordinal} insert_range")
        if (
            round_value["ordinal"] != ordinal
            or round_value["query_start"] != ordinal * per_round
            or round_value["query_count"] != per_round
            or round_value["insert_range"] != insertion
            or round_value["start_barrier"] != "round_start_readers_and_writer"
            or round_value["end_barrier"] != "round_end_queries_and_insert_complete"
        ):
            raise ValueError(f"timed_reader_plan round {ordinal} is not frozen")
    return plan


def validate_concurrent_mutation_plans(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    expected = {4: "delete_by_user_id_and_fpath", 5: "replacement_insert"}
    plans = []
    for ordinal, mutation in expected.items():
        operation = manifest["operations"][ordinal]
        if operation.get("schedule"):
            raise ValueError(f"concurrent mutation operation {ordinal} must not use a serial schedule")
        plan = require_object(operation.get("concurrent_mutation_plan"), [
            "mutation", "reader_concurrency", "reader_assignments", "start_barrier", "end_barrier",
        ], f"operation {ordinal} concurrent_mutation_plan")
        assignments = plan["reader_assignments"]
        if (
            plan["mutation"] != mutation
            or plan["reader_concurrency"] != manifest["config"]["reader_concurrency"]
            or not isinstance(assignments, list)
            or len(assignments) != plan["reader_concurrency"]
            or plan["start_barrier"] != "reindex_start_all_readers_and_writer"
            or plan["end_barrier"] != "reindex_end_all_readers_and_mutation_complete"
        ):
            raise ValueError(f"concurrent mutation operation {ordinal} does not match the frozen plan")
        for reader, assignment in enumerate(assignments):
            require_object(assignment, ["reader", "query_ordinal", "scenario"],
                           f"operation {ordinal} reader assignment {reader}")
            if (
                assignment["reader"] != reader
                or assignment["query_ordinal"] != reader
                or assignment["scenario"] != operation["target"]
            ):
                raise ValueError(f"concurrent mutation operation {ordinal} reader {reader} does not match the frozen plan")
        plans.append(plan)
    for operation in manifest["operations"]:
        if operation["ordinal"] not in expected and operation.get("concurrent_mutation_plan") is not None:
            raise ValueError(f"unexpected concurrent mutation plan at operation {operation['ordinal']}")
    return plans


def validate_manifest(manifest: dict[str, Any], *, require_frozen: bool = True) -> dict[str, Any]:
    require_object(manifest, ["schema", "config", "corpora", "queries", "operations", "corpus_sha256", "query_sha256", "operation_sha256", "expected_state_sha256"], "manifest")
    if manifest["schema"] != MANIFEST_SCHEMA:
        raise ValueError("unsupported Minima manifest schema")
    config = require_object(manifest["config"], CONFIG_KEYS, "config")
    frozen_config = {
        "collection": "minima", "vector_field": "embedding", "content_field": "content", "dimension": 8,
        "metric": "cosine", "scalar_fields": ["user_id", "fpath"], "top_k": 5, "batch_size": 256,
        "reader_concurrency": 4, "writer_concurrency": 1, "warmup_queries": 32, "timed_queries": 1024,
        "lookup_limit": 4096, "order_tolerance": 0, "score_tolerance": 0.000001,
        "ordering": "manifest_ordinal_serial; timed_search_round_robin",
        "completion_boundary": "successful_mutation_response_before_visibility_probe",
        "timing_boundary": "storage_calls_only; embeddings_and_llm_excluded; fetch_and_decode_separate",
    }
    if require_frozen and config != frozen_config:
        raise ValueError("manifest config is not the frozen Minima workload")
    if not all(isinstance(manifest[key], list) for key in ("corpora", "queries", "operations")) or len(manifest["corpora"]) != len(manifest["queries"]):
        raise ValueError("manifest arrays/cardinality are invalid")
    names: set[str] = set()
    scenario_optional = {"broad_start", "broad_rows", "narrow_start", "narrow_rows", "user_id", "fpath", "closer_cross_tenant_distractor_rows"}
    for index, spec in enumerate(manifest["corpora"]):
        require_object(spec, SCENARIO_KEYS, f"corpora[{index}]", scenario_optional)
        if not isinstance(spec["name"], str) or not spec["name"] or spec["name"] in names:
            raise ValueError("scenario names must be unique nonempty strings")
        names.add(spec["name"])
        rows = exact_int(spec["corpus_rows"], "corpus_rows", 1)
        start, eligible = exact_int(spec["eligible_start"], "eligible_start", 0), exact_int(spec["eligible_rows"], "eligible_rows", 0)
        if start + eligible > rows or finite(spec["selectivity"], "selectivity") != eligible / rows:
            raise ValueError(f"invalid selectivity/cardinality for {spec['name']}")
        if spec["filter"] not in ("user_id", "user_id+fpath") or spec["generator"] != GENERATOR:
            raise ValueError(f"unsupported filter/generator for {spec['name']}")
    queries = {row.get("scenario"): row for row in manifest["queries"] if isinstance(row, dict)}
    if set(queries) != names or len(queries) != len(names):
        raise ValueError("queries must map one-to-one to scenarios")
    for spec in manifest["corpora"]:
        query = require_object(queries[spec["name"]], QUERY_KEYS, f"query[{spec['name']}]")
        if not isinstance(query["vector"], list) or len(query["vector"]) != config["dimension"]:
            raise ValueError(f"invalid query vector for {spec['name']}")
        for value in query["vector"]:
            finite(value, "query vector")
        stop = min(spec["eligible_start"] + spec["eligible_rows"], spec["eligible_start"] + config["top_k"])
        initial_ordinals = range(spec["eligible_start"], stop)
        initial_ids = [f"minima/{spec['name']}/{ordinal:06d}" for ordinal in initial_ordinals]
        initial_scores = [document_score(generated_document(spec, ordinal)) for ordinal in range(spec["eligible_start"], stop)]
        final_ids, final_scores = final_oracle(manifest, spec)
        for label, actual, expected in (
            ("initial IDs", query["initial_oracle_ids"], initial_ids), ("final IDs", query["final_oracle_ids"], final_ids),
        ):
            if actual != expected:
                raise ValueError(f"exact {label} mismatch for {spec['name']}")
        for label, actual, expected in (
            ("initial scores", query["initial_oracle_scores"], initial_scores),
            ("final scores", query["final_oracle_scores"], final_scores),
        ):
            if (
                not isinstance(actual, list)
                or len(actual) != len(expected)
                or any(abs(finite(got, label) - want) > MANIFEST_FLOAT_TOLERANCE for got, want in zip(actual, expected, strict=True))
            ):
                raise ValueError(f"exact {label} mismatch for {spec['name']}")
    if len(manifest["operations"]) != len(OPERATION_NAMES):
        raise ValueError("operation count mismatch")
    operation_optional = {"timed", "insert_ranges", "filter", "ids", "documents", "schedule",
                          "timed_reader_plan", "concurrent_mutation_plan"}
    for ordinal, operation in enumerate(manifest["operations"]):
        require_object(operation, OPERATION_KEYS, f"operations[{ordinal}]", operation_optional)
        if operation.get("ordinal") != ordinal or operation.get("name") != OPERATION_NAMES[ordinal]:
            raise ValueError("operations are not in the frozen order")
        if operation.get("effect") not in ("none", "insert", "update", "delete"):
            raise ValueError(f"invalid operation effect at {ordinal}")
        for document in operation.get("documents", []):
            require_object(document, DOCUMENT_KEYS, f"operation {ordinal} document")
    timed_plan = validate_timed_plan(manifest)
    validate_concurrent_mutation_plans(manifest)
    if require_frozen and timed_execution_digest(timed_plan) != TIMED_EXECUTION_SHA256:
        raise ValueError("frozen timed execution digest mismatch")
    if require_frozen and reindex_trace_digest(expected_reindex_execution(manifest)) != REINDEX_EXECUTION_SHA256:
        raise ValueError("frozen reindex execution digest mismatch")
    for key, actual in manifest_hashes(manifest).items():
        if manifest[key] != actual:
            raise ValueError(f"manifest {key} mismatch: got {manifest[key]} recomputed {actual}")
    state_hash = expected_state_hash(manifest)
    if manifest["expected_state_sha256"] != state_hash:
        raise ValueError(f"manifest expected_state_sha256 mismatch: got {manifest['expected_state_sha256']} recomputed {state_hash}")
    if require_frozen:
        for key, expected in FROZEN_HASHES.items():
            if manifest[key] != expected:
                raise ValueError(f"manifest {key} is not frozen: got {manifest[key]} want {expected}")
    return manifest


def load_manifest(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return validate_manifest(json.load(handle))


def point_id(document_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "snissn/gomap/minima-qdrant/v1/" + document_id))


def payload_filter(models: Any, value: dict[str, Any]) -> Any:
    must = [models.FieldCondition(key="user_id", match=models.MatchValue(value=value["user_id"]))]
    if value.get("fpath"):
        must.append(models.FieldCondition(key="fpath", match=models.MatchValue(value=value["fpath"])))
    return models.Filter(must=must)


def model_value(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "dict"):
        return value.dict()
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, dict):
        return {str(key): model_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [model_value(item) for item in value]
    if hasattr(value, "__dict__"):
        return {key: model_value(item) for key, item in vars(value).items() if not key.startswith("_")}
    return value


def enum_text(value: Any) -> str:
    return str(getattr(value, "value", value)).lower()


def optimizer_is_ok(value: Any) -> bool:
    raw = model_value(value)
    return raw == "ok" or isinstance(raw, dict) and raw.get("ok") is True


def readiness_disposition(snapshots: list[dict[str, Any]],
                          server_logs: list[dict[str, Any]],
                          deadline_seconds: float | None = None) -> str:
    diagnostic_text = json.dumps([
        {
            "optimizer_status": row.get("optimizer_status"),
            "optimizations": row.get("optimizations"),
            "collection_error": row.get("collection_error"),
        }
        for row in snapshots
    ] + server_logs, sort_keys=True, default=str).lower()
    if any(marker in diagnostic_text for marker in RESOURCE_STARVATION_MARKERS):
        return "resource starvation"
    if not snapshots:
        return "unknown"
    latest = snapshots[-1]
    optimizer = latest.get("optimizer_status")
    if latest.get("status") == "red" or (
        isinstance(optimizer, dict)
        and (optimizer.get("error") or optimizer.get("ok") is False)
    ):
        return "optimizer error"

    if deadline_seconds is None:
        deadline_seconds = float(latest.get("elapsed_seconds", 0) or 0)
    progress_window = min(60.0, max(0.0, deadline_seconds) * 0.1)
    cutoff = max(0.0, deadline_seconds - progress_window)
    recent = [
        row for row in snapshots
        if isinstance(row.get("elapsed_seconds"), (int, float))
        and row["elapsed_seconds"] >= cutoff
        and row.get("collection_error") is None
    ]

    def progress_signature(row: dict[str, Any]) -> tuple[Any, ...]:
        detail = row.get("optimizations", {}).get("detail", {})
        return (
            row.get("points_count"),
            row.get("indexed_vectors_count"),
            row.get("segments_count"),
            json.dumps(detail.get("summary"), sort_keys=True, default=str),
            json.dumps(detail.get("running"), sort_keys=True, default=str),
        )

    if len(recent) > 1 and any(
        progress_signature(current) != progress_signature(previous)
        for previous, current in zip(recent, recent[1:])
    ):
        return "active progress"

    latest_optimization = latest.get("optimizations", {})
    latest_detail = (
        latest_optimization.get("detail", {})
        if latest_optimization.get("available")
        and isinstance(latest_optimization.get("detail"), dict)
        else {}
    )
    summary = latest_detail.get("summary") or {}
    if (
        latest_detail.get("running")
        or latest_detail.get("queued")
        or any(int(summary.get(key, 0) or 0) > 0 for key in (
            "queued_optimizations", "queued_points", "queued_segments", "idle_segments",
        ))
        or latest.get("status") == "grey"
    ):
        return "queued/idle"
    return "unknown"


def is_timeout(exc: BaseException) -> bool:
    return isinstance(exc, TimeoutError) or "timeout" in type(exc).__name__.lower() or "timed out" in str(exc).lower()


def memory_bytes() -> str:
    try:
        return str(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"))
    except (AttributeError, OSError, ValueError):
        return "unavailable"


def disk_bytes(path: Path | None) -> int:
    if path is None:
        return 0
    transient_missing = {errno.ENOENT, getattr(errno, "ESTALE", errno.ENOENT)}

    def disappeared(exc: OSError) -> bool:
        return exc.errno in transient_missing

    try:
        root = path.stat()
    except OSError as exc:
        if disappeared(exc):
            return 0
        raise
    if not stat.S_ISDIR(root.st_mode):
        return 0
    total = 0
    pending = [path]
    while pending:
        directory = pending.pop()
        try:
            entries = os.scandir(directory)
        except OSError as exc:
            if disappeared(exc):
                continue
            raise
        with entries:
            for entry in entries:
                try:
                    info = entry.stat(follow_symlinks=False)
                except OSError as exc:
                    if disappeared(exc):
                        continue
                    raise
                if stat.S_ISDIR(info.st_mode):
                    pending.append(Path(entry.path))
                elif stat.S_ISREG(info.st_mode):
                    total += info.st_size
    return total


def cpu_time_seconds(value: str) -> float:
    days, clock = (value.split("-", 1) if "-" in value else ("0", value))
    parts = [float(part) for part in clock.split(":")]
    if len(parts) == 3:
        hours, minutes, seconds = parts
    elif len(parts) == 2:
        hours, (minutes, seconds) = 0, parts
    else:
        hours, minutes, seconds = 0, 0, parts[0]
    return float(days) * 86400 + hours * 3600 + minutes * 60 + seconds


def server_resource_usage(pid: int | None, storage_path: Path | None, server_name: str) -> dict[str, Any]:
    rss: int | None = None
    cpu: float | None = None
    error = ""
    if pid is not None:
        try:
            result = subprocess.run(
                ["ps", "-o", "rss=", "-o", "time=", "-p", str(pid)],
                check=True, capture_output=True, text=True, timeout=5,
            )
            fields = result.stdout.split()
            if len(fields) != 2:
                raise ValueError(f"unexpected ps output {result.stdout!r}")
            rss, cpu = int(fields[0]) * 1024, cpu_time_seconds(fields[1])
        except (OSError, subprocess.SubprocessError, ValueError) as exc:
            error = f"{type(exc).__name__}: {exc}"
    disk_available = storage_path is not None and storage_path.exists()
    captured = rss is not None and cpu is not None and disk_available
    return {
        "captured": captured,
        "rss_bytes": rss or 0,
        "cpu_seconds": cpu or 0.0,
        "disk_bytes": disk_bytes(storage_path),
        "availability": {
            "rss_bytes": f"{server_name} server PID {pid}" if rss is not None else "unavailable",
            "cpu_seconds": f"{server_name} server PID {pid}" if cpu is not None else "unavailable",
            "disk_bytes": str(storage_path) if disk_available else "unavailable",
            "bytes_per_op": "unavailable", "allocs_per_op": "unavailable",
            "measurement_error": error,
        },
    }
def server_process_identity(pid: int) -> str:
    if type(pid) is not int or pid <= 0:
        raise RuntimeError("server process identity requires a positive PID")
    try:
        result = subprocess.run(
            ["ps", "-o", "lstart=", "-o", "command=", "-p", str(pid)],
            check=True, capture_output=True, text=True, timeout=5,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"cannot capture server process identity for PID {pid}: {exc}") from exc
    identity = " ".join(result.stdout.split())
    if not identity:
        raise RuntimeError(f"cannot capture server process identity for PID {pid}")
    return identity
def server_process_running(pid: int) -> bool:
    if type(pid) is not int or pid <= 0:
        return False
    try:
        result = subprocess.run(
            ["ps", "-o", "stat=", "-p", str(pid)],
            check=False, capture_output=True, text=True, timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    state = result.stdout.strip()
    return result.returncode == 0 and bool(state) and not state.startswith("Z")


def linux_process_socket_inodes(pid: int) -> set[str]:
    inodes: set[str] = set()
    for fd in (Path("/proc") / str(pid) / "fd").iterdir():
        try:
            target = os.readlink(fd)
        except OSError as exc:
            if exc.errno in (errno.ENOENT, errno.ESTALE):
                continue
            raise
        if target.startswith("socket:[") and target.endswith("]"):
            inodes.add(target[8:-1])
    return inodes


def server_process_owns_endpoint(pid: int, url: str, listener_port: int | None = None) -> bool:
    if type(pid) is not int or pid <= 0:
        return False
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in ("http", "https") or parsed.hostname is None:
        return False
    try:
        endpoint_port = parsed.port or (443 if parsed.scheme == "https" else 80)
        port = endpoint_port if listener_port is None else listener_port
        if type(port) is not int or port <= 0 or port > 65535:
            return False
        endpoint_addresses = {row[4][0] for row in socket.getaddrinfo(parsed.hostname, endpoint_port, type=socket.SOCK_STREAM)}
        local_addresses = {"127.0.0.1", "::1", "0.0.0.0", "::"}
        for name in (socket.gethostname(), socket.getfqdn()):
            try:
                local_addresses.update(row[4][0] for row in socket.getaddrinfo(name, None))
            except OSError:
                pass
    except (OSError, ValueError):
        return False
    if endpoint_addresses.isdisjoint(local_addresses):
        return False
    if sys.platform.startswith("linux"):
        try:
            socket_inodes = linux_process_socket_inodes(pid)
            for name in ("tcp", "tcp6"):
                table = Path("/proc") / str(pid) / "net" / name
                for line in table.read_text(encoding="ascii").splitlines()[1:]:
                    fields = line.split()
                    if (
                        len(fields) > 9
                        and fields[3] == "0A"
                        and int(fields[1].rsplit(":", 1)[1], 16) == port
                        and fields[9] in socket_inodes
                    ):
                        return True
        except (OSError, ValueError) as exc:
            raise RuntimeError(f"cannot inspect listener ownership for PID {pid}: {exc}") from exc
        return False
    if sys.platform == "darwin":
        try:
            result = subprocess.run(
                ["lsof", "-nP", "-a", "-p", str(pid), f"-iTCP:{port}", "-sTCP:LISTEN", "-Fn"],
                check=False, capture_output=True, text=True, timeout=5,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        return result.returncode == 0 and any(
            line.startswith("n") and line[1:].split(" ", 1)[0].rsplit(":", 1)[-1] == str(port)
            for line in result.stdout.splitlines()
        )
    return False






def resource_delta(baseline: dict[str, Any], end: dict[str, Any]) -> dict[str, Any]:
    captured = baseline["captured"] and end["captured"]
    return {
        "captured": captured,
        "rss_bytes": max(0, end["rss_bytes"] - baseline["rss_bytes"]) if captured else 0,
        "cpu_seconds": max(0.0, end["cpu_seconds"] - baseline["cpu_seconds"]) if captured else 0.0,
        "disk_bytes": max(0, end["disk_bytes"] - baseline["disk_bytes"]) if captured else 0,
        "baseline": baseline, "end": end,
    }


def latency_distribution(values: list[int]) -> dict[str, int]:
    values = sorted(values)
    if not values:
        return {"count": 0, "total_nanos": 0, "minimum_nanos": 0, "p50_nanos": 0,
                "p95_nanos": 0, "p99_nanos": 0, "maximum_nanos": 0}

    def percentile(fraction: float) -> int:
        return values[math.ceil(fraction * len(values)) - 1]

    return {
        "count": len(values), "total_nanos": sum(values), "minimum_nanos": values[0],
        "p50_nanos": percentile(0.50), "p95_nanos": percentile(0.95),
        "p99_nanos": percentile(0.99), "maximum_nanos": values[-1],
    }


class Evidence:
    def __init__(self, manifest: dict[str, Any]) -> None:
        names = [row["name"] for row in manifest["corpora"]]
        self.samples: list[dict[str, Any]] = []
        self.events: list[dict[str, Any]] = []
        self.failures: list[str] = []
        self.errors = dict.fromkeys(names, 0)
        self.timeouts = dict.fromkeys(names, 0)
        self.initial: dict[str, tuple[list[str], list[float]]] = {}
        self.preclose: dict[str, tuple[list[str], list[float]]] = {}
        self.reopen: dict[str, tuple[list[str], list[float]]] = {}
        self.final: dict[str, tuple[list[str], list[float]]] = {}
        self.cross_user = dict.fromkeys(names, 0)
        self.stale_insert = dict.fromkeys(names, 0)
        self.stale_update = dict.fromkeys(names, 0)
        self.stale_delete = dict.fromkeys(names, 0)

    def call(self, operation: str, category: str, scenario: str, function: Callable[[], Any],
             on_start: Callable[[], None] | None = None) -> Any:
        start = time.monotonic_ns()
        try:
            if on_start is not None:
                on_start()
            return function()
        except BaseException as exc:
            if scenario in self.errors:
                self.errors[scenario] += 1
                self.timeouts[scenario] += int(is_timeout(exc))
            self.events.append({"operation": operation, "scenario": scenario, "kind": "timeout" if is_timeout(exc) else "error", "error": f"{type(exc).__name__}: {exc}"})
            raise
        finally:
            end = time.monotonic_ns()
            self.samples.append({"operation": operation, "scenario": scenario, "category": category,
                                 "start_nanos": start, "end_nanos": end, "duration_nanos": end - start})


class StateAccumulator:
    """Order-independent digest of backend-neutral committed IDs and payload."""
    MODULUS = 1 << 256

    def __init__(self) -> None:
        self.count = self.xor = self.total = 0

    def add(self, document: dict[str, Any]) -> None:
        digest = hashlib.sha256()
        for key in ("id", "content", "user_id", "fpath"):
            digest.update(str(document[key]).encode())
            digest.update(b"\0")
        # Vector correctness is independently guarded by the frozen manifest and exact score oracle.
        number = int.from_bytes(digest.digest(), "big")
        self.count += 1
        self.xor ^= number
        self.total = (self.total + number) % self.MODULUS

    def hexdigest(self) -> str:
        return hashlib.sha256(f"minima-committed-payload-v1:{self.count}:{self.xor:064x}:{self.total:064x}".encode()).hexdigest()


def final_documents(manifest: dict[str, Any]) -> Iterable[dict[str, Any]]:
    deleted: set[str] = set()
    overrides: dict[str, dict[str, Any]] = {}
    additions: dict[str, dict[str, Any]] = {}
    for operation in manifest["operations"]:
        if operation["effect"] == "delete":
            deleted.update(operation.get("ids", []))
        elif operation["effect"] == "update":
            overrides.update((row["id"], row) for row in operation.get("documents", []))
        elif operation["effect"] == "insert" and operation.get("documents"):
            additions.update((row["id"], row) for row in operation["documents"])
    for spec in manifest["corpora"]:
        for ordinal in range(spec["corpus_rows"]):
            document = generated_document(spec, ordinal)
            if document["id"] not in deleted:
                yield overrides.get(document["id"], document)
    yield from (row for identifier, row in sorted(additions.items()) if identifier not in deleted)


class QdrantMinimaRunner:
    restart_requires_configuration_reassertion = True

    def __init__(self, manifest: dict[str, Any], *, client_factory: Callable[[], Any], models: Any, url: str,
                 collection: str, allow_drop: bool, operation_timeout: int, optimizer_timeout: float,
                 poll_interval: float, server_version: str, deployment: str, image: str,
                 storage_path: Path | None, server_pid: int | None,
                 restart_server: Callable[[], int] | None = None, restart_identity: str = "",
                 process_identity: Callable[[int], str] = server_process_identity,
                 process_running: Callable[[int], bool] = server_process_running,
                 process_owns_endpoint: Callable[[int, str, int | None], bool] = server_process_owns_endpoint,
                 resource_server_name: str = "Qdrant") -> None:
        self.manifest, self.config = manifest, manifest["config"]
        self.specs, self.queries = scenario_map(manifest), {row["scenario"]: row for row in manifest["queries"]}
        self.mutation_vectors = {
            document["id"]: document["vector"]
            for operation in manifest["operations"]
            for document in operation.get("documents", [])
        }
        self.client_factory, self.models = client_factory, models
        self.url, self.collection, self.allow_drop = url, collection, allow_drop
        self.operation_timeout, self.optimizer_timeout, self.poll_interval = operation_timeout, optimizer_timeout, poll_interval
        self.server_version, self.deployment, self.image, self.storage_path = server_version, deployment, image, storage_path
        self.server_pid, self.resource_server_name = server_pid, resource_server_name
        self.restart_server, self.restart_identity, self.process_identity = restart_server, restart_identity, process_identity
        self.process_running, self.process_owns_endpoint = process_running, process_owns_endpoint
        self.server_listener_port = 6333 if deployment == "docker" else None
        self.client: Any | None = None
        self.evidence = Evidence(manifest)
        self.operations = {
            "manifest_ordered": False, "batch_insert_during_search": False,
            "timed_queries_executed": 0, "timed_rounds_completed": 0, "timed_execution_sha256": "",
            "timed_execution_trace": {"queries": [], "rounds": []},
            "reindex_delete_replace": False, "reindex_operations_executed": 0,
            "reindex_execution_sha256": "", "reindex_execution_trace": {"operations": []},
            "explicit_update_visible": False, "explicit_delete_visible": False, "empty_cases_checked": False,
        }
        self.reopen_attempted = self.reopen_parity = False
        self.resource_baseline: dict[str, Any] | None = None
        self.completed_resource_segments: list[dict[str, Any]] = []
        self.restart_boundary: dict[str, Any] = {}
        self.restart_origin: tuple[int, str] | None = None
        self.state_scroll: dict[str, Any] = {}
        self.effective_collection: dict[str, Any] = {}
        self.overlap_evidence: dict[str, Any] = {}
        self.readiness_evidence: list[dict[str, Any]] = []
        self.configuration_transition: dict[str, Any] = {
            "boundary": "initial_batch_insert_to_warmup_search",
            "attempted": False, "completed": False,
            "initial_upload_hnsw": INITIAL_UPLOAD_HNSW_CONFIG,
            "initial_upload_optimizers": INITIAL_UPLOAD_OPTIMIZERS_CONFIG,
            "production_hnsw": PRODUCTION_HNSW_CONFIG,
            "production_optimizers": PRODUCTION_OPTIMIZERS_CONFIG,
        }
        self.production_restoration_attempted = False
        self.server_log_path = (
            storage_path.parent / "qdrant.log"
            if deployment == "standalone" and storage_path is not None else None
        )

    def connect(self) -> None:
        self.client = self.client_factory()

    def capture_restart_origin(self) -> None:
        old_pid = self.server_pid
        if type(old_pid) is not int or old_pid <= 0:
            raise RuntimeError("close/reopen requires the original backend server PID")
        old_process_identity = self.process_identity(old_pid)
        if self.resource_baseline is not None:
            self.completed_resource_segments.append(
                resource_delta(self.resource_baseline, server_resource_usage(
                    old_pid, self.storage_path, self.resource_server_name))
            )
        self.restart_origin = (old_pid, old_process_identity)

    def restart_backend(self) -> None:
        if self.restart_server is None or not self.restart_identity:
            raise RuntimeError("close/reopen requires an explicit backend restart hook")
        if self.restart_origin is None:
            self.capture_restart_origin()
        old_pid, old_process_identity = self.restart_origin
        self.restart_origin = None
        new_pid = self.restart_server()
        if type(new_pid) is not int or new_pid <= 0:
            raise RuntimeError("backend restart hook must return the restarted Qdrant server PID")
        if new_pid == old_pid:
            raise RuntimeError("backend restart hook returned the original PID; restart is unproven")
        if self.process_running(old_pid):
            raise RuntimeError(f"original backend PID {old_pid} is still running after restart hook")
        if not self.process_owns_endpoint(new_pid, self.url, self.server_listener_port):
            listener = self.server_listener_port or urllib.parse.urlsplit(self.url).port
            raise RuntimeError(f"restarted backend PID {new_pid} does not own listener port {listener}")
        new_process_identity = self.process_identity(new_pid)
        self.restart_boundary = {
            "hook_identity": self.restart_identity,
            "old_pid": old_pid, "new_pid": new_pid,
            "old_process_identity": old_process_identity,
            "new_process_identity": new_process_identity,
            "pid_changed": True, "verified": True,
        }
        self.server_pid = new_pid
        self.resource_baseline = server_resource_usage(
            self.server_pid, self.storage_path, self.resource_server_name)

    def resource_evidence(self) -> dict[str, Any]:
        baseline = self.resource_baseline or server_resource_usage(
            self.server_pid, self.storage_path, self.resource_server_name)
        segments = [*self.completed_resource_segments,
                    resource_delta(baseline, server_resource_usage(
                        self.server_pid, self.storage_path, self.resource_server_name))]
        captured = bool(segments) and all(segment["captured"] for segment in segments)
        return {
            "captured": captured,
            "rss_bytes": sum(segment["rss_bytes"] for segment in segments) if captured else 0,
            "cpu_seconds": sum(segment["cpu_seconds"] for segment in segments) if captured else 0.0,
            "disk_bytes": sum(segment["disk_bytes"] for segment in segments) if captured else 0,
            "semantics": RESOURCE_SEMANTICS,
            "segments": segments,
            "baseline": segments[0]["baseline"],
            "end": segments[-1]["end"],
        }

    def optimization_snapshot(self, timeout_seconds: float | None = None) -> dict[str, Any]:
        assert self.client is not None
        method = getattr(self.client, "get_optimizations", None)
        if not callable(method):
            return {"available": False, "reason": "qdrant-client get_optimizations unavailable"}
        try:
            if timeout_seconds is None:
                detail = method(collection_name=self.collection, _with="completed", completed_limit=16)
            elif timeout_seconds <= 0:
                return {"available": False, "reason": "readiness deadline has no optimizer diagnostic budget"}
            else:
                remote = getattr(self.client, "http", None)
                api_client = getattr(remote, "client", None)
                response_type = getattr(self.models, "InlineResponse20011", None)
                request = getattr(api_client, "request", None)
                if remote is None:
                    # Test/local clients have no remote HTTP transport.
                    detail = method(collection_name=self.collection, _with="completed", completed_limit=16)
                elif not callable(request) or response_type is None:
                    return {
                        "available": False,
                        "reason": "qdrant-client deadline-bounded optimizer transport unavailable",
                    }
                else:
                    response = request(
                        type_=response_type, method="GET",
                        url="/collections/{collection_name}/optimizations",
                        path_params={"collection_name": self.collection},
                        params={"with": "completed", "completed_limit": 16}, timeout=timeout_seconds,
                    )
                    detail = getattr(response, "result", response)
            return {"available": True, "detail": model_value(detail)}
        except Exception as exc:
            return {
                "available": False,
                "reason": f"{type(exc).__name__}: {exc}",
            }

    def server_log_snapshot(self) -> dict[str, Any]:
        path = self.server_log_path
        if path is None:
            return {
                "available": False,
                "reason": "server log path unavailable for this deployment",
            }
        try:
            size = path.stat().st_size
            with path.open("rb") as handle:
                handle.seek(max(0, size - 65536))
                tail = handle.read().decode("utf-8", errors="replace")
            return {
                "available": True, "path": str(path), "size_bytes": size,
                "tail": tail,
            }
        except OSError as exc:
            return {
                "available": False,
                "path": str(path),
                "reason": f"{type(exc).__name__}: {exc}",
            }

    def restore_production_configuration(self) -> None:
        assert self.client is not None
        if self.production_restoration_attempted:
            raise RuntimeError("Qdrant production configuration restoration was already attempted")
        self.production_restoration_attempted = True
        self.configuration_transition["attempted"] = True
        before = self.client.get_collection(self.collection)
        self.configuration_transition["before"] = model_value(getattr(before, "config", None))
        try:
            self.client.update_collection(
                collection_name=self.collection,
                hnsw_config=self.models.HnswConfigDiff(**PRODUCTION_HNSW_CONFIG),
                optimizers_config=self.models.OptimizersConfigDiff(**PRODUCTION_OPTIMIZERS_CONFIG),
                timeout=self.operation_timeout,
            )
        except Exception as exc:
            self.configuration_transition["error"] = f"{type(exc).__name__}: {exc}"
            raise
        self.configuration_transition["completed"] = True


    def reassert_production_configuration_after_restart(self) -> None:
        assert self.client is not None
        self.client.update_collection(
            collection_name=self.collection,
            hnsw_config=self.models.HnswConfigDiff(**PRODUCTION_HNSW_CONFIG),
            optimizers_config=self.models.OptimizersConfigDiff(**PRODUCTION_OPTIMIZERS_CONFIG),
            timeout=self.operation_timeout,
        )
        self.configuration_transition["restart_reassertions"] = (
            self.configuration_transition.get("restart_reassertions", 0) + 1
        )

    def initial_load_to_query_boundary(self) -> None:
        self.restore_production_configuration()



    def create_owned_collection(self) -> None:
        assert self.client is not None
        if self.client.collection_exists(self.collection):
            if not self.allow_drop:
                raise RuntimeError(f"Qdrant collection {self.collection!r} already exists; set ALLOW_DROP=true only for a disposable namespace")
            self.client.delete_collection(self.collection, timeout=self.operation_timeout)
        self.client.create_collection(
            collection_name=self.collection,
            vectors_config={
                self.config["vector_field"]: self.models.VectorParams(
                    size=self.config["dimension"], distance=self.models.Distance.COSINE,
                ),
            },
            hnsw_config=self.models.HnswConfigDiff(**INITIAL_UPLOAD_HNSW_CONFIG),
            optimizers_config=self.models.OptimizersConfigDiff(**INITIAL_UPLOAD_OPTIMIZERS_CONFIG),
            timeout=self.operation_timeout,
        )
        for field in self.config["scalar_fields"]:
            self.client.create_payload_index(collection_name=self.collection, field_name=field,
                field_schema=self.models.PayloadSchemaType.KEYWORD, wait=True, timeout=self.operation_timeout)
        self.wait_ready(expected_count=0, phase="initial_upload_collection_created")
        self.ensure_compatible()

    def ensure_compatible(self) -> None:
        assert self.client is not None
        if not self.client.collection_exists(self.collection):
            raise RuntimeError(f"Qdrant collection {self.collection!r} disappeared")
        info = self.client.get_collection(self.collection)
        value = model_value(info)
        self.effective_collection = value if isinstance(value, dict) else {"value": str(value)}
        params = getattr(getattr(info, "config", None), "params", None)
        vectors = getattr(params, "vectors", None)
        vector = vectors.get(self.config["vector_field"]) if isinstance(vectors, dict) else None
        if vector is None or getattr(vector, "size", None) != self.config["dimension"] or enum_text(getattr(vector, "distance", "")) != "cosine":
            raise RuntimeError("existing Qdrant collection has incompatible named cosine vector schema")
        payload_schema = getattr(info, "payload_schema", {}) or {}
        missing = [field for field in self.config["scalar_fields"] if field not in payload_schema]
        if missing:
            raise RuntimeError(f"Qdrant collection is missing keyword payload indexes: {missing}")

    def wait_ready(self, expected_count: int | None = None,
                   phase: str = "mutation_visibility") -> None:
        assert self.client is not None
        started = time.monotonic()
        deadline = started + self.optimizer_timeout
        next_resource_sample = started
        session: dict[str, Any] = {
            "phase": phase, "deadline_seconds": self.optimizer_timeout,
            "expected_points_count": expected_count,
            "snapshot_limit": READINESS_SNAPSHOT_LIMIT,
            "snapshots_dropped": 0, "snapshots": [],
            "resource_samples": [], "server_log_samples": [],
            "outcome": "polling", "disposition": "unknown",
        }
        self.readiness_evidence.append(session)

        def finish(outcome: str) -> None:
            session["outcome"] = outcome
            session["disposition"] = (
                "ready" if outcome == "ready"
                else readiness_disposition(
                    session["snapshots"], session["server_log_samples"],
                    self.optimizer_timeout,
                )
            )

        def bounded_optimization_snapshot() -> dict[str, Any]:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return {
                    "available": False,
                    "reason": "readiness deadline has no optimizer diagnostic budget",
                }
            # HTTPX applies a timeout to connect/read/write/pool separately.
            # Divide the remaining wall budget across those phases and cap the
            # optional diagnostic so it cannot inherit the operation timeout.
            timeout_seconds = min(READINESS_DIAGNOSTIC_TIMEOUT_SECONDS, remaining / 4)
            return self.optimization_snapshot(timeout_seconds)

        while time.monotonic() < deadline:
            now = time.monotonic()
            resource_index: int | None = None
            if now >= next_resource_sample:
                session["resource_samples"].append({
                    "elapsed_seconds": now - started,
                    "sample": server_resource_usage(
                        self.server_pid, self.storage_path, self.resource_server_name,
                    ),
                })
                resource_index = len(session["resource_samples"]) - 1
                log_payload = self.server_log_snapshot()
                if (
                    not session["server_log_samples"]
                    or any(
                        session["server_log_samples"][-1].get(key) != value
                        for key, value in log_payload.items()
                    )
                    or any(
                        key != "elapsed_seconds" and key not in log_payload
                        for key in session["server_log_samples"][-1]
                    )
                ):
                    session["server_log_samples"].append({
                        "elapsed_seconds": now - started, **log_payload,
                    })
                next_resource_sample = now + READINESS_RESOURCE_INTERVAL_SECONDS
            elif session["resource_samples"]:
                resource_index = len(session["resource_samples"]) - 1

            try:
                info = self.client.get_collection(self.collection)
            except Exception as exc:
                snapshot = {
                    "sequence": len(session["snapshots"]),
                    "elapsed_seconds": now - started,
                    "collection_error": f"{type(exc).__name__}: {exc}",
                    "optimizations": bounded_optimization_snapshot(),
                    "resource_sample_index": resource_index,
                }
                session["snapshots"].append(snapshot)
                finish("error")
                raise

            optimizer = getattr(info, "optimizer_status", None)
            schema = getattr(info, "payload_schema", {}) or {}
            snapshot = {
                "sequence": len(session["snapshots"]) + session["snapshots_dropped"],
                "elapsed_seconds": now - started,
                "status": enum_text(getattr(info, "status", "")),
                "optimizer_status": model_value(optimizer),
                "points_count": getattr(info, "points_count", None),
                "indexed_vectors_count": getattr(info, "indexed_vectors_count", None),
                "segments_count": getattr(info, "segments_count", None),
                "payload_schema": model_value(schema),
                "config": model_value(getattr(info, "config", None)),
                "optimizations": bounded_optimization_snapshot(),
                "resource_sample_index": resource_index,
            }
            ready_metadata = (
                snapshot["status"] == "green"
                and optimizer_is_ok(optimizer)
                and all(field in schema for field in self.config["scalar_fields"])
            )
            if ready_metadata and expected_count is not None:
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    try:
                        exact = self.client.count(
                            collection_name=self.collection, count_filter=None, exact=True,
                            timeout=min(self.operation_timeout, remaining),
                        )
                        snapshot["exact_points_count"] = getattr(exact, "count", None)
                    except Exception as exc:
                        snapshot["exact_count_error"] = f"{type(exc).__name__}: {exc}"
            snapshots = session["snapshots"]
            if len(snapshots) == READINESS_SNAPSHOT_LIMIT:
                del snapshots[1]
                session["snapshots_dropped"] += 1
            snapshots.append(snapshot)
            if (
                ready_metadata
                and (expected_count is None or snapshot.get("exact_points_count") == expected_count)
            ):
                finish("ready")
                return
            time.sleep(self.poll_interval)
        finish("timeout")
        last = session["snapshots"][-1] if session["snapshots"] else {}
        raise TimeoutError(
            f"Qdrant readiness exceeded {self.optimizer_timeout}s; "
            f"disposition={session['disposition']}; last={last}"
        )

    def point(self, document: dict[str, Any]) -> Any:
        return self.models.PointStruct(id=point_id(document["id"]), vector={self.config["vector_field"]: document["vector"]},
            payload={key: document[key] for key in ("id", "content", "user_id", "fpath")})

    def upsert(self, operation: str, scenario: str, documents: list[dict[str, Any]], wait_ready: bool = True,
               on_writer_start: Callable[[], None] | None = None) -> None:
        assert self.client is not None
        for start in range(0, len(documents), self.config["batch_size"]):
            points = [self.point(row) for row in documents[start:start + self.config["batch_size"]]]
            self.evidence.call(operation, "writer", scenario, lambda points=points: self.client.upsert(
                collection_name=self.collection, points=points, wait=True, timeout=self.operation_timeout),
                on_start=on_writer_start)
        if wait_ready:
            self.evidence.call(operation, "writer_wait", scenario, self.wait_ready)

    def insert_ranges(self, name: str, ranges: list[dict[str, Any]], wait_each: bool) -> None:
        for insertion in ranges:
            spec = self.specs[insertion["scenario"]]
            documents = [generated_document(spec, ordinal) for ordinal in range(insertion["start"], insertion["start"] + insertion["rows"])]
            self.upsert(name, spec["name"], documents, wait_each)
        if ranges and not wait_each:
            if name == "initial_batch_insert":
                self.initial_load_to_query_boundary()
            expected_count = sum(insertion["rows"] for insertion in ranges)
            phase = "initial_load_to_query" if name == "initial_batch_insert" else name
            self.evidence.call(
                name, "writer_wait", "all",
                lambda: self.wait_ready(expected_count=expected_count, phase=phase),
            )
            if name == "initial_batch_insert":
                self.ensure_compatible()

    def search(self, operation: str, scenario: str, interval: dict[str, int] | None = None) -> tuple[list[str], list[float]]:
        assert self.client is not None
        spec, query = self.specs[scenario], self.queries[scenario]
        if interval is not None:
            interval["started_monotonic_ns"] = time.monotonic_ns()
        try:
            response = self.evidence.call(operation, "search", scenario, lambda: self.client.query_points(
                collection_name=self.collection, query=query["vector"], using=self.config["vector_field"],
                query_filter=payload_filter(self.models, spec), limit=self.config["top_k"], with_payload=True,
                with_vectors=False, timeout=self.operation_timeout))
        finally:
            if interval is not None:
                interval["ended_monotonic_ns"] = time.monotonic_ns()
        started, ids, scores = time.monotonic_ns(), [], []
        for point in getattr(response, "points", response):
            payload = getattr(point, "payload", None) or {}
            identifier = payload.get("id")
            if not isinstance(identifier, str) or not isinstance(payload.get("content"), str):
                raise RuntimeError("Qdrant result is missing canonical ID/content payload")
            if payload.get("user_id") != spec.get("user_id") or (spec["filter"] == "user_id+fpath" and payload.get("fpath") != spec.get("fpath")):
                self.evidence.cross_user[scenario] += 1
            ids.append(identifier)
            scores.append(float(getattr(point, "score")))
        ended = time.monotonic_ns()
        self.evidence.samples.append({"operation": operation, "scenario": scenario, "category": "decode",
                                      "start_nanos": started, "end_nanos": ended, "duration_nanos": ended - started})
        return ids, scores

    def compare_oracle(self, phase: str, scenario: str, result: tuple[list[str], list[float]]) -> bool:
        ids, scores = result
        query = self.queries[scenario]
        expected_ids = query[f"{phase}_oracle_ids"]
        expected_scores = query[f"{phase}_oracle_scores"]
        deltas = [abs(actual - expected) for actual, expected in zip(scores, expected_scores, strict=False)]
        match = (
            ids == expected_ids
            and len(scores) == len(expected_scores)
            and all(delta <= self.config["score_tolerance"] for delta in deltas)
        )
        self.evidence.events.append({
            "operation": f"{phase}_oracle_comparison", "scenario": scenario, "kind": "oracle_comparison",
            "match": match, "actual_ids": ids, "expected_ids": expected_ids,
            "maximum_score_delta": max(deltas, default=0.0),
        })
        if not match:
            self.evidence.failures.append(f"{phase} exact oracle mismatch for {scenario}")
        return match

    def results_match(self, left: tuple[list[str], list[float]], right: tuple[list[str], list[float]]) -> bool:
        return (
            left[0] == right[0]
            and len(left[1]) == len(right[1])
            and all(abs(a - b) <= self.config["score_tolerance"] for a, b in zip(left[1], right[1], strict=True))
        )

    def retrieve(self, operation: str, scenario: str, ids: list[str]) -> list[Any]:
        assert self.client is not None
        return self.evidence.call(operation, "fetch", scenario, lambda: self.client.retrieve(
            collection_name=self.collection, ids=[point_id(value) for value in ids], with_payload=True,
            with_vectors=False, timeout=self.operation_timeout))

    def delete_filter(self, operation: dict[str, Any],
                      on_writer_start: Callable[[], None] | None = None) -> None:
        assert self.client is not None
        name, scenario = operation["name"], operation["target"]
        selector = payload_filter(self.models, operation["filter"])
        self.evidence.call(name, "writer", scenario, lambda: self.client.delete(
            collection_name=self.collection, points_selector=selector, wait=True, timeout=self.operation_timeout),
            on_start=on_writer_start)
        self.evidence.call(name, "writer_wait", scenario, self.wait_ready)
        result = self.evidence.call(name, "fetch", scenario, lambda: self.client.count(
            collection_name=self.collection, count_filter=selector, exact=True, timeout=self.operation_timeout))
        remaining = int(getattr(result, "count", result))
        self.evidence.stale_delete[scenario] += remaining
        if remaining:
            raise RuntimeError(f"filtered reindex delete left {remaining} matching rows")

    def delete_ids(self, operation: dict[str, Any]) -> None:
        assert self.client is not None
        name, scenario, ids = operation["name"], operation["target"], operation["ids"]
        selector = self.models.PointIdsList(points=[point_id(value) for value in ids])
        self.evidence.call(name, "writer", scenario, lambda: self.client.delete(
            collection_name=self.collection, points_selector=selector, wait=True, timeout=self.operation_timeout))
        self.evidence.call(name, "writer_wait", scenario, self.wait_ready)
        stale = len(self.retrieve(name, scenario, ids))
        self.evidence.stale_delete[scenario] += stale
        if stale:
            raise RuntimeError(f"explicit delete left {stale} IDs visible")

    def run_timed_overlap(self, operation: dict[str, Any]) -> None:
        plan = validate_timed_plan(self.manifest)
        readers = plan["reader_concurrency"]
        query_observations: list[dict[str, Any] | None] = [None] * plan["query_count"]
        round_observations: list[dict[str, Any]] = []
        round_evidence: list[dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=readers + 1, thread_name_prefix="minima") as pool:
            for round_value in plan["rounds"]:
                start_barrier = threading.Barrier(readers + 1)
                end_barrier = threading.Barrier(readers + 1)
                insertion = round_value["insert_range"]
                writer_interval: dict[str, int] = {}
                writer_started = threading.Event()

                def write_round() -> None:
                    start_barrier.wait()
                    try:
                        spec = self.specs[insertion["scenario"]]
                        documents = [
                            generated_document(spec, value)
                            for value in range(insertion["start"], insertion["start"] + insertion["rows"])
                        ]
                        sample_start = len(self.evidence.samples)
                        self.upsert(operation["name"], spec["name"], documents,
                                    on_writer_start=writer_started.set)
                        samples = [
                            row for row in self.evidence.samples[sample_start:]
                            if row["operation"] == operation["name"] and row["category"] == "writer"
                        ]
                        if not samples:
                            raise RuntimeError("timed insert produced no raw writer call interval")
                        writer_interval.update({
                            "writer_started_monotonic_ns": min(row["start_nanos"] for row in samples),
                            "writer_ended_monotonic_ns": max(row["end_nanos"] for row in samples),
                        })
                        visible = len(self.retrieve(operation["name"], spec["name"], [row["id"] for row in documents]))
                        missing = len(documents) - visible
                        self.evidence.stale_insert[spec["name"]] += missing
                        if missing:
                            raise RuntimeError(f"batch insert left {missing} IDs invisible")
                    except BaseException:
                        end_barrier.abort()
                        writer_started.set()
                        raise
                    end_barrier.wait()

                def read_round(worker: int) -> None:
                    start_barrier.wait()
                    try:
                        if not writer_started.wait(self.operation_timeout) or end_barrier.broken:
                            raise RuntimeError("timed writer did not start")
                        begin = round_value["query_start"] + worker
                        end = round_value["query_start"] + round_value["query_count"]
                        for query_ordinal in range(begin, end, readers):
                            scenario = plan["scenario_order"][query_ordinal % len(plan["scenario_order"])]
                            interval: dict[str, int] = {}
                            ids, scores = self.search(operation["name"], scenario, interval)
                            query = self.queries[scenario]
                            if not self.results_match(
                                (ids, scores),
                                (query["initial_oracle_ids"], query["initial_oracle_scores"]),
                            ):
                                raise RuntimeError(f"timed query {query_ordinal} does not match its frozen oracle")
                            query_observations[query_ordinal] = {
                                "ordinal": query_ordinal, "round": round_value["ordinal"],
                                "reader": worker, "scenario": scenario, **interval,
                                "result_captured": True, "actual_ids": ids, "actual_scores": scores,
                            }
                    except BaseException:
                        end_barrier.abort()
                        raise
                    end_barrier.wait()

                writer = pool.submit(write_round)
                reader_futures = [pool.submit(read_round, worker) for worker in range(readers)]
                await_concurrent_futures([writer, *reader_futures])

                round_observation = {
                    "ordinal": round_value["ordinal"], "query_start": round_value["query_start"],
                    "query_count": round_value["query_count"], "insert_range": dict(insertion),
                    "start_barrier": round_value["start_barrier"], "end_barrier": round_value["end_barrier"],
                    **writer_interval,
                }
                round_observations.append(round_observation)
                round_queries = [
                    row for row in query_observations
                    if row is not None and row["round"] == round_value["ordinal"]
                ]
                overlapping_readers = sorted({
                    row["reader"] for row in round_queries
                    if intervals_overlap(
                        row["started_monotonic_ns"], row["ended_monotonic_ns"],
                        writer_interval["writer_started_monotonic_ns"], writer_interval["writer_ended_monotonic_ns"],
                    )
                })
                round_evidence.append({
                    "ordinal": round_value["ordinal"], "queries_executed": len(round_queries),
                    "overlapping_readers": overlapping_readers,
                    "all_readers_overlap_observed": overlapping_readers == list(range(readers)),
                })

        observed_queries = [row for row in query_observations if row is not None]
        observed_trace = {"queries": observed_queries, "rounds": round_observations}
        trace_hash = timed_trace_digest(observed_trace)
        queries_executed = len(observed_queries)
        rounds_completed = len(round_observations)
        all_overlap = all(row["all_readers_overlap_observed"] for row in round_evidence)
        self.overlap_evidence = {
            "configured_searches": plan["query_count"], "executed_searches": queries_executed,
            "configured_reader_concurrency": readers, "configured_writer_concurrency": plan["writer_concurrency"],
            "rounds": round_evidence, "all_rounds_writer_search_overlap_observed": all_overlap,
            "timed_execution_sha256": trace_hash,
        }
        self.operations.update({
            "timed_queries_executed": queries_executed,
            "timed_rounds_completed": rounds_completed,
            "timed_execution_sha256": trace_hash,
            "timed_execution_trace": observed_trace,
            "batch_insert_during_search": (
                queries_executed == plan["query_count"]
                and rounds_completed == len(plan["rounds"])
                and all_overlap
            ),
        })
        if not self.operations["batch_insert_during_search"]:
            raise RuntimeError(f"timed writer/search overlap contract failed: {self.overlap_evidence}")

    def run_concurrent_mutation(self, operation: dict[str, Any]) -> None:
        plan = operation["concurrent_mutation_plan"]
        readers = plan["reader_concurrency"]
        start_barrier = threading.Barrier(readers + 1)
        end_barrier = threading.Barrier(readers + 1)
        mutation_interval: dict[str, int] = {}
        mutation_started = threading.Event()
        observations: list[dict[str, Any] | None] = [None] * readers

        def mutate() -> None:
            start_barrier.wait()
            try:
                sample_start = len(self.evidence.samples)
                if operation["effect"] == "delete":
                    self.delete_filter(operation, mutation_started.set)
                elif operation["effect"] == "insert":
                    self.upsert(operation["name"], operation["target"], operation["documents"],
                                on_writer_start=mutation_started.set)
                else:
                    raise RuntimeError(f"unsupported concurrent mutation {operation['effect']}")
                samples = [
                    row for row in self.evidence.samples[sample_start:]
                    if row["operation"] == operation["name"] and row["category"] == "writer"
                ]
                if not samples:
                    raise RuntimeError("concurrent reindex produced no raw mutation call interval")
                mutation_interval.update({
                    "mutation_started_monotonic_ns": min(row["start_nanos"] for row in samples),
                    "mutation_ended_monotonic_ns": max(row["end_nanos"] for row in samples),
                })
                if operation["effect"] == "insert":
                    visible = len(self.retrieve(operation["name"], operation["target"],
                                                [row["id"] for row in operation["documents"]]))
                    if visible != len(operation["documents"]):
                        raise RuntimeError("replacement insert did not become visible")
            except BaseException:
                end_barrier.abort()
                mutation_started.set()
                raise
            end_barrier.wait()

        def read_during_mutation(assignment: dict[str, Any]) -> None:
            start_barrier.wait()
            try:
                if not mutation_started.wait(self.operation_timeout) or end_barrier.broken:
                    raise RuntimeError("concurrent mutation writer did not start")
                interval: dict[str, int] = {}
                ids, scores = self.search(operation["name"], assignment["scenario"], interval)
                query = self.queries[assignment["scenario"]]
                oracles = (
                    ((query["initial_oracle_ids"], query["initial_oracle_scores"]), ([], []))
                    if operation["effect"] == "delete"
                    else (([], []), (query["final_oracle_ids"], query["final_oracle_scores"]))
                )
                if not any(self.results_match((ids, scores), oracle) for oracle in oracles):
                    raise RuntimeError(
                        f"concurrent mutation reader returned impossible mixed state for operation {operation['ordinal']}"
                    )
                observations[assignment["reader"]] = {
                    **assignment, **interval, "result_captured": True,
                    "actual_ids": ids, "actual_scores": scores,
                }
            except BaseException:
                end_barrier.abort()
                raise
            end_barrier.wait()

        with ThreadPoolExecutor(max_workers=readers + 1, thread_name_prefix="minima-reindex") as pool:
            writer = pool.submit(mutate)
            reader_futures = [
                pool.submit(read_during_mutation, assignment)
                for assignment in plan["reader_assignments"]
            ]
            await_concurrent_futures([writer, *reader_futures])

        reader_queries = [row for row in observations if row is not None]
        observed = {
            "operation_ordinal": operation["ordinal"], "mutation": plan["mutation"],
            "start_barrier": plan["start_barrier"], "end_barrier": plan["end_barrier"],
            **mutation_interval, "reader_queries": reader_queries,
        }
        overlaps = len(reader_queries) == readers and all(
            intervals_overlap(
                row["started_monotonic_ns"], row["ended_monotonic_ns"],
                mutation_interval["mutation_started_monotonic_ns"],
                mutation_interval["mutation_ended_monotonic_ns"],
            )
            for row in reader_queries
        )
        if not overlaps:
            raise RuntimeError(f"concurrent mutation overlap contract failed for operation {operation['ordinal']}")
        self.operations["reindex_execution_trace"]["operations"].append(observed)
        trace = self.operations["reindex_execution_trace"]
        digest = reindex_trace_digest(trace)
        self.operations["reindex_operations_executed"] = len(trace["operations"])
        self.operations["reindex_execution_sha256"] = digest
        self.operations["reindex_delete_replace"] = len(trace["operations"]) == 2


    def expected_vector(self, identifier: str) -> list[float]:
        if identifier in self.mutation_vectors:
            return normalized_f32_vector(self.mutation_vectors[identifier])
        parts = identifier.split("/")
        if len(parts) != 3 or parts[0] != "minima" or parts[1] not in self.specs:
            raise ValueError(f"unknown Minima document ID {identifier!r}")
        ordinal_text = parts[2]
        if len(ordinal_text) != 6 or not ordinal_text.isdigit():
            raise ValueError(f"noncanonical Minima document ID {identifier!r}")
        ordinal = int(ordinal_text)
        document = generated_document(self.specs[parts[1]], ordinal)
        if document["id"] != identifier:
            raise ValueError(f"noncanonical Minima document ID {identifier!r}")
        return normalized_f32_vector(document["vector"])


    def expected_scroll(self) -> tuple[str, int]:
        accumulator = StateAccumulator()
        for document in final_documents(self.manifest):
            accumulator.add(document)
        return accumulator.hexdigest(), accumulator.count

    def actual_scroll(self) -> tuple[str, int, dict[str, Any]]:
        assert self.client is not None
        accumulator, offset = StateAccumulator(), None
        mismatches, maximum_delta = 0, 0.0
        while True:
            previous_offset = offset
            rows, offset = self.client.scroll(collection_name=self.collection, limit=self.config["batch_size"], offset=offset,
                with_payload=True, with_vectors=[self.config["vector_field"]], timeout=self.operation_timeout)
            for row in rows:
                payload, vectors = getattr(row, "payload", None) or {}, getattr(row, "vector", None)
                vector = vectors.get(self.config["vector_field"]) if isinstance(vectors, dict) else None
                document = {**payload, "vector": vector}
                if set(document) != set(DOCUMENT_KEYS) or not isinstance(vector, list) or len(vector) != self.config["dimension"]:
                    raise RuntimeError("Qdrant scroll row does not match the Minima document schema")
                accumulator.add(document)
                try:
                    expected = self.expected_vector(document["id"])
                    actual = normalized_f32_vector(vector)
                except (KeyError, TypeError, ValueError):
                    mismatches += 1
                    continue
                deltas = [abs(left - right) for left, right in zip(actual, expected, strict=True)]
                maximum_delta = max(maximum_delta, max(deltas, default=0.0))
                mismatches += int(any(delta > self.config["score_tolerance"] for delta in deltas))
            if offset is not None and (offset == previous_offset or not rows):
                raise RuntimeError("Qdrant scroll cursor did not advance")
            if offset is None:
                return accumulator.hexdigest(), accumulator.count, {
                    "algorithm": "streaming per-record normalized-float32 full-vector comparison",
                    "checked_rows": accumulator.count,
                    "mismatch_rows": mismatches, "maximum_component_delta": maximum_delta,
                    "tolerance": self.config["score_tolerance"], "match": mismatches == 0,
                }

    def run(self) -> None:
        self.resource_baseline = server_resource_usage(
            self.server_pid, self.storage_path, self.resource_server_name)
        for ordinal, operation in enumerate(self.manifest["operations"]):
            if operation["ordinal"] != ordinal or operation["name"] != OPERATION_NAMES[ordinal]:
                raise RuntimeError("operation stream changed after validation")
            name = operation["name"]
            if name == "ensure_compatible_collection":
                self.connect()
                self.evidence.call(name, "writer", "all", self.create_owned_collection)
            elif name == "initial_batch_insert":
                self.insert_ranges(name, operation["insert_ranges"], False)
            elif name == "warmup_search":
                for step in operation["schedule"]:
                    scenario = step["scenario"]
                    result = self.search(name, scenario)
                    if scenario not in self.evidence.initial:
                        self.evidence.initial[scenario] = result
                        self.compare_oracle("initial", scenario, result)
            elif name == "timed_search_with_batch_insert":
                self.run_timed_overlap(operation)
            elif name in ("reindex_delete_by_user_and_fpath_while_reading", "reindex_replacement_insert_while_reading"):
                self.run_concurrent_mutation(operation)
            elif name in ("reindex_visibility_probe", "update_visibility_probe", "delete_visibility_probe", "empty_user_and_file_probes"):
                for step in operation["schedule"]:
                    ids, _ = self.search(name, step["scenario"])
                    if name == "empty_user_and_file_probes" and ids:
                        raise RuntimeError(f"empty scenario {step['scenario']} returned results")
                if name == "empty_user_and_file_probes":
                    self.operations["empty_cases_checked"] = True
            elif name == "explicit_update":
                self.upsert(name, operation["target"], operation["documents"])
                rows = self.retrieve(name, operation["target"], [operation["documents"][0]["id"]])
                visible = len(rows) == 1 and getattr(rows[0], "payload", {}).get("content") == operation["documents"][0]["content"]
                self.operations["explicit_update_visible"] = visible
                if not visible:
                    self.evidence.stale_update[operation["target"]] += 1
                    raise RuntimeError("explicit update did not become visible")
            elif name == "explicit_delete":
                self.delete_ids(operation)
                self.operations["explicit_delete_visible"] = True
            elif name == "close":
                assert self.client is not None
                for scenario in self.specs:
                    self.evidence.preclose[scenario] = self.search("preclose_reopen_baseline", scenario)
                self.capture_restart_origin()
                self.client.close()
                self.client = None
                self.restart_backend()
            elif name == "reopen":
                self.reopen_attempted = True
                self.connect()
                for scenario in self.specs:
                    self.evidence.reopen[scenario] = self.search("post_reopen_parity", scenario)
            elif name == "idempotent_ensure_after_reopen":
                if self.restart_requires_configuration_reassertion:
                    self.evidence.call(name, "writer", "all", self.reassert_production_configuration_after_restart)
                self.evidence.call(name, "fetch", "all", self.ensure_compatible)
                self.evidence.call(name, "fetch", "all", self.wait_ready)
            elif name == "final_manifest_and_oracle_comparison":
                for step in operation["schedule"]:
                    scenario = step["scenario"]
                    result = self.search(name, scenario)
                    self.evidence.final[scenario] = result
                    # Reopen evidence was captured immediately after reconnect, not assigned from this final query.
                    self.compare_oracle("final", scenario, result)
                expected_hash, expected_rows = self.expected_scroll()
                actual_hash, actual_rows, vector_evidence = self.evidence.call(name, "fetch", "all", self.actual_scroll)
                payload_match = expected_hash == actual_hash and expected_rows == actual_rows
                vector_evidence["expected_rows"] = expected_rows
                vector_evidence["match"] = vector_evidence["match"] and actual_rows == expected_rows
                self.state_scroll = {
                    "algorithm": "payload digest plus normalized-float32 full-vector comparison",
                    "expected_hash": expected_hash, "actual_hash": actual_hash,
                    "expected_rows": expected_rows, "actual_rows": actual_rows,
                    "payload": {"expected_hash": expected_hash, "actual_hash": actual_hash, "match": payload_match},
                    "vectors": vector_evidence,
                    "match": payload_match and vector_evidence["match"],
                }
                if not self.state_scroll["match"]:
                    self.evidence.failures.append("final Qdrant payload/vector scroll mismatch")
        self.operations["manifest_ordered"] = True
        self.reopen_parity = self.state_scroll.get("match", False) and all(
            name in self.evidence.preclose
            and name in self.evidence.reopen
            and name in self.evidence.final
            and self.results_match(self.evidence.preclose[name], self.evidence.reopen[name])
            and self.results_match(self.evidence.reopen[name], self.evidence.final[name])
            for name in self.specs
        )

    def artifact(self) -> dict[str, Any]:
        timing = {name: dict.fromkeys(("writer", "search", "fetch", "decode"), 0) for name in self.specs}
        latency_values: dict[str, list[int]] = {name: [] for name in ("writer", "search", "fetch", "decode")}
        for sample in self.evidence.samples:
            bucket = "writer" if sample["category"].startswith("writer") else sample["category"]
            if bucket in latency_values:
                latency_values[bucket].append(sample["duration_nanos"])
            if sample["scenario"] in timing and bucket in timing[sample["scenario"]]:
                timing[sample["scenario"]][bucket] += sample["duration_nanos"]
        latency_distributions = {name: latency_distribution(values) for name, values in latency_values.items()}
        resource = self.resource_evidence()
        scenarios = []
        for spec in self.manifest["corpora"]:
            name, query = spec["name"], self.queries[spec["name"]]
            initial_ids, initial_scores = self.evidence.initial.get(name, ([], []))
            actual_ids, actual_scores = self.evidence.final.get(name, ([], []))
            preclose = self.evidence.preclose.get(name, ([], []))
            reopened = self.evidence.reopen.get(name, ([], []))
            reopen_parity = self.results_match(preclose, reopened) and self.results_match(reopened, (actual_ids, actual_scores))
            expected_ids = query["final_oracle_ids"]
            intersection, union = len(set(actual_ids) & set(expected_ids)), len(set(actual_ids) | set(expected_ids))
            scenarios.append({
                "backend": "qdrant", "scenario": name, "corpus_rows": spec["corpus_rows"], "expected_matches": spec["eligible_rows"], "selectivity": spec["selectivity"],
                "initial_oracle_ids": query["initial_oracle_ids"], "initial_oracle_scores": query["initial_oracle_scores"],
                "final_oracle_ids": expected_ids, "final_oracle_scores": query["final_oracle_scores"],
                "initial_actual_ids": initial_ids, "initial_actual_scores": initial_scores, "actual_ids": actual_ids,
                "actual_scores": actual_scores, "reopen_ids": reopened[0],
                "reopen_parity": reopen_parity,
                "recall": intersection / len(expected_ids) if expected_ids else (1.0 if not actual_ids else 0.0),
                "overlap": intersection / union if union else 1.0, "order_tolerance": self.config["order_tolerance"],
                "score_tolerance": self.config["score_tolerance"], "errors": self.evidence.errors[name], "timeouts": self.evidence.timeouts[name],
                "correctness": {"cross_user_results": self.evidence.cross_user[name], "stale_insert_ids": self.evidence.stale_insert[name],
                    "stale_update_ids": self.evidence.stale_update[name], "stale_delete_ids": self.evidence.stale_delete[name]},
                "route": {"identity": "qdrant_filtered_hnsw", "declared_scalar_filtering": True, "native_base_plus_live_delta": False,
                    "full_document_scan_fallbacks": None, "scalar_filter_unbounded": None, "probe_ids": None, "candidate_ids": None,
                    "retained_candidate_ids": None, "refined_candidate_ids": None, "membership_source": "qdrant_keyword_payload_index",
                    "plan": "qdrant_filtered_hnsw", "allowed_id_materialization_rows": None, "primary_document_scans": None,
                    "visited_candidates": None, "scored_candidates": None, "admitted_candidates": None},
                "visibility": {"generation_consistent": self.evidence.errors[name] == 0, "visibility_mismatch_count": 0, "visibility_retry_count": 0},
                "timing": {"captured": True, "writer_millis": timing[name]["writer"] / 1e6, "search_millis": timing[name]["search"] / 1e6,
                    "fetch_millis": timing[name]["fetch"] / 1e6, "decode_millis": timing[name]["decode"] / 1e6,
                    "embedding_included": False, "llm_included": False},
                "resource": {"captured": resource["captured"], "bytes_per_op": None, "allocs_per_op": None,
                    "allocation_availability": "unavailable", "rss_bytes": resource["rss_bytes"],
                    "cpu_seconds": resource["cpu_seconds"], "disk_bytes": resource["disk_bytes"]},
            })
        result_hash = self.manifest["expected_state_sha256"] if self.state_scroll.get("match") else self.state_scroll.get("actual_hash", "")
        configuration = {"url": self.url, "collection": self.collection, "vector_field": self.config["vector_field"],
            "dimension": str(self.config["dimension"]), "metric": self.config["metric"], "scalar_fields": ",".join(self.config["scalar_fields"]),
            "top_k": str(self.config["top_k"]), "batch_size": str(self.config["batch_size"]),
            "operation_timeout_seconds": str(self.operation_timeout), "optimizer_timeout_seconds": str(self.optimizer_timeout),
            "write_wait": "true", "point_id_mapping": "uuid5(NAMESPACE_URL,snissn/gomap/minima-qdrant/v1/<manifest-id>)",
            "deployment": self.deployment, "image": self.image or "not_applicable",
            "server_pid": str(self.server_pid) if self.server_pid is not None else "unavailable",
            "server_listener_port": str(self.server_listener_port or urllib.parse.urlsplit(self.url).port or (443 if urllib.parse.urlsplit(self.url).scheme == "https" else 80)),
            "restart_identity": self.restart_identity,
            "initial_upload_hnsw": json.dumps(INITIAL_UPLOAD_HNSW_CONFIG, sort_keys=True, separators=(",", ":")),
            "initial_upload_optimizers": json.dumps(INITIAL_UPLOAD_OPTIMIZERS_CONFIG, sort_keys=True, separators=(",", ":")),
            "production_hnsw": json.dumps(PRODUCTION_HNSW_CONFIG, sort_keys=True, separators=(",", ":")),
            "production_optimizers": json.dumps(PRODUCTION_OPTIMIZERS_CONFIG, sort_keys=True, separators=(",", ":")),
            "effective_collection": json.dumps(self.effective_collection, sort_keys=True, separators=(",", ":"))}
        environment = {"os": platform.system() + " " + platform.release(), "arch": platform.machine() or "unavailable",
            "cpu": platform.processor() or "unavailable", "memory": memory_bytes(), "python": platform.python_version()}
        return {"schema": ARTIFACT_SCHEMA, "state": "partial", "passing": False, "manifest": self.manifest,
            "backends": [{"name": "qdrant", "server_version": self.server_version, "client_version": CLIENT_VERSION,
                "durability": "Qdrant wait=true; effective WAL/optimizer collection config recorded", "configuration": configuration,
                "environment": environment, "manifest": {key: self.manifest[key] for key in ("corpus_sha256", "query_sha256", "operation_sha256")},
                "operations": self.operations, "reopen": {"attempted": self.reopen_attempted, "committed_parity": self.reopen_parity,
                    "result_manifest_hash": result_hash}}],
            "scenarios": scenarios, "failures": self.evidence.failures, "readiness_recommendation": "not_evaluated",
            "backend_raw_evidence": {"qdrant": {
                "phase_latency_distributions": latency_distributions, "events": self.evidence.events,
                "timed_overlap": self.overlap_evidence, "final_scroll_state": self.state_scroll,
                "resource_measurement": resource,
                "restart_boundary": self.restart_boundary,
                "collection_configuration_transition": self.configuration_transition,
                "readiness": {
                    "sessions": self.readiness_evidence,
                    "latest_non_ready_disposition": next((
                        row["disposition"] for row in reversed(self.readiness_evidence)
                        if row["outcome"] != "ready"
                    ), "none"),
                },
                "resource_availability": {
                    "measurement": RESOURCE_SEMANTICS,
                    "baseline": resource["baseline"]["availability"],
                    "end": resource["end"]["availability"],
                },
            }}}

    def close(self) -> None:
        if self.client is not None:
            self.client.close()
            self.client = None


def server_info(url: str, api_key: str) -> dict[str, Any]:
    request = urllib.request.Request(url.rstrip("/") + "/")
    if api_key:
        request.add_header("api-key", api_key)
    with urllib.request.urlopen(request, timeout=10) as response:
        value = json.load(response)
    if not isinstance(value, dict) or value.get("version") != SERVER_VERSION:
        raise RuntimeError(f"Qdrant server must be exactly {SERVER_VERSION}; root response={value!r}")
    return value


def restart_from_hook(hook: Path, url: str, api_key: str, ready_timeout: float) -> int:
    result = subprocess.run([str(hook)], check=True, capture_output=True, text=True, timeout=ready_timeout)
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError("Qdrant restart hook did not print the restarted server PID")
    try:
        pid = int(lines[-1])
    except ValueError as exc:
        raise RuntimeError(f"Qdrant restart hook printed an invalid PID: {lines[-1]!r}") from exc
    if pid <= 0:
        raise RuntimeError(f"Qdrant restart hook printed an invalid PID: {pid}")
    deadline, last = time.monotonic() + ready_timeout, None
    while time.monotonic() < deadline:
        try:
            server_info(url, api_key)
            return pid
        except Exception as exc:
            last = exc
            time.sleep(0.25)
    raise TimeoutError(f"restarted Qdrant did not become ready within {ready_timeout}s: {last}")


def validate_qdrant_evidence_inputs(server_pid: int | None, storage_path: Path | None) -> None:
    if type(server_pid) is not int or server_pid <= 0:
        raise RuntimeError("Qdrant qualification requires an authoritative positive server PID")
    if storage_path is None or not storage_path.is_dir():
        raise RuntimeError("Qdrant qualification requires an existing authoritative storage path")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--api-key", default=os.environ.get("QDRANT_API_KEY", ""))
    parser.add_argument("--collection", required=True)
    parser.add_argument("--allow-drop", action="store_true")
    parser.add_argument("--operation-timeout", type=int, default=120)
    parser.add_argument("--optimizer-timeout", type=float, default=600)
    parser.add_argument("--poll-interval", type=float, default=0.25)
    parser.add_argument("--deployment", choices=("external", "standalone", "docker"), required=True)
    parser.add_argument("--image", default="")
    parser.add_argument("--storage-path", type=Path, required=True)
    parser.add_argument("--restart-hook", type=Path, required=True)
    parser.add_argument("--server-pid", type=int, required=True)
    return parser.parse_args()


def main() -> int:
    args, manifest = parse_args(), None
    validate_qdrant_evidence_inputs(args.server_pid, args.storage_path)
    manifest = load_manifest(args.manifest)
    installed = importlib.metadata.version("qdrant-client")
    if installed != CLIENT_VERSION:
        raise RuntimeError(f"qdrant-client must be exactly {CLIENT_VERSION}, got {installed}")
    from qdrant_client import QdrantClient, models
    info = server_info(args.url, args.api_key)
    if not args.restart_hook.is_file() or not os.access(args.restart_hook, os.X_OK):
        raise RuntimeError(f"Qdrant restart hook must be executable: {args.restart_hook}")
    runner = QdrantMinimaRunner(manifest, client_factory=lambda: QdrantClient(url=args.url, api_key=args.api_key or None,
        timeout=args.operation_timeout, prefer_grpc=False), models=models, url=args.url, collection=args.collection,
        allow_drop=args.allow_drop, operation_timeout=args.operation_timeout, optimizer_timeout=args.optimizer_timeout,
        poll_interval=args.poll_interval, server_version=str(info["version"]), deployment=args.deployment,
        image=args.image, storage_path=args.storage_path, server_pid=args.server_pid,
        restart_server=lambda: restart_from_hook(args.restart_hook, args.url, args.api_key, args.optimizer_timeout),
        restart_identity=str(args.restart_hook))
    exit_code = 0
    try:
        runner.run()
    except BaseException as exc:
        runner.evidence.failures.append(f"{type(exc).__name__}: {exc}")
        exit_code = 1
    finally:
        try:
            runner.close()
        except BaseException as exc:
            runner.evidence.failures.append(f"{type(exc).__name__}: {exc}")
            exit_code = 1
        finally:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(runner.artifact(), indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
