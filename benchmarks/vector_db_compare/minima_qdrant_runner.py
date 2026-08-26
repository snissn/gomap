#!/usr/bin/env python3
"""Execute the frozen Minima operation manifest against Qdrant.

The output is deliberately raw/partial evidence. Only the Go validator may
turn backend evidence into a qualification result.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import argparse
import hashlib
from decimal import Decimal
import importlib.metadata
import json
import math
import os
import platform
import struct
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Callable, Iterable

MANIFEST_SCHEMA = "treedb_rag_minima_manifest/v1"
ARTIFACT_SCHEMA = "treedb_rag_application/minima_v4"
SERVER_VERSION = CLIENT_VERSION = "1.19.0"
GENERATOR = (
    "ordinal-v2:id=minima/<scenario>/<ordinal:06d>;content=minima:<scenario>:<ordinal>;"
    "vector=[s,sqrt(1-s*s),0x6],s=0.9-ordinal*0.000003;"
    "oracle=cosine(float32(vector),float32([1,0x7]));"
    "defaults=other-user-%02d(ordinal%31),/other/%02d.txt(ordinal%97)"
)
MANIFEST_FLOAT_TOLERANCE = 1e-15
FROZEN_HASHES = {
    "corpus_sha256": "856df3d20b5177e0b7354aeac41b9d052e5f1075e00cec686ff823b110916ccc",
    "query_sha256": "eb4f076023e361b9a2cf18a06a5e1d69e5023c304da25d38848fc7011575288a",
    "operation_sha256": "f2d85501ae55255784749f042892836078335a99e7603ac254bd1a88eafa9179",
    "expected_state_sha256": "e74c2b4aaea81c3ad4ee0444bb706ca936f652dfa7ee173bf52d686f3a14480f",
}
TIMED_EXECUTION_SHA256 = "2ccd4e9badc7644e15cd4a5e4eeb68f59005c2e040506d5f9e4d5935e23f6fdd"
REINDEX_EXECUTION_SHA256 = "9ec2d96b41783bf9ac323f522244940b023c4d27efd759714c149e0ae4568ee0"
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
    user_id, fpath = f"other-user-{ordinal % 31:02d}", f"/other/{ordinal % 97:02d}.txt"
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
            "defaults=other-user-%02d(ordinal%31),/other/%02d.txt(ordinal%97)"
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

def timed_trace_digest(trace: dict[str, list[dict[str, Any]]]) -> str:
    lines = [
        f"query|ordinal={row['ordinal']}|round={row['round']}|reader={row['reader']}|scenario={row['scenario']}|"
        f"writer_in_flight={str(row['writer_in_flight']).lower()}\n"
        for row in trace["queries"]
    ]
    for row in trace["rounds"]:
        insertion = row["insert_range"]
        lines.append(
            f"round|ordinal={row['ordinal']}|query_start={row['query_start']}|query_count={row['query_count']}|"
            f"insert={insertion['scenario']}:{insertion['start']}:{insertion['rows']}|"
            f"start={row['start_barrier']}|end={row['end_barrier']}\n"
        )
    return hashlib.sha256("".join(lines).encode()).hexdigest()


def timed_execution_digest(plan: dict[str, Any]) -> str:
    queries = []
    for round_value in plan["rounds"]:
        begin = round_value["query_start"]
        end = begin + round_value["query_count"]
        for ordinal in range(begin, end):
            queries.append({
                "ordinal": ordinal, "round": round_value["ordinal"],
                "reader": ordinal % plan["reader_concurrency"],
                "scenario": plan["scenario_order"][ordinal % len(plan["scenario_order"])],
                "writer_in_flight": True,
            })
    return timed_trace_digest({"queries": queries, "rounds": plan["rounds"]})


def reindex_trace_digest(trace: dict[str, list[dict[str, Any]]]) -> str:
    lines = []
    for operation in trace["operations"]:
        lines.append(
            f"reindex|operation={operation['operation_ordinal']}|mutation={operation['mutation']}|"
            f"start={operation['start_barrier']}|end={operation['end_barrier']}\n"
        )
        for query in operation["reader_queries"]:
            lines.append(
                f"reindex_query|operation={operation['operation_ordinal']}|reader={query['reader']}|"
                f"query_ordinal={query['query_ordinal']}|scenario={query['scenario']}|"
                f"mutation_in_flight={str(query['mutation_in_flight']).lower()}\n"
            )
    return hashlib.sha256("".join(lines).encode()).hexdigest()

def expected_reindex_execution(manifest: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    operations = []
    for operation in manifest["operations"]:
        plan = operation.get("concurrent_mutation_plan")
        if plan is None:
            continue
        operations.append({
            "operation_ordinal": operation["ordinal"], "mutation": plan["mutation"],
            "start_barrier": plan["start_barrier"], "end_barrier": plan["end_barrier"],
            "reader_queries": [
                {**assignment, "mutation_in_flight": True}
                for assignment in plan["reader_assignments"]
            ],
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


def is_timeout(exc: BaseException) -> bool:
    return isinstance(exc, TimeoutError) or "timeout" in type(exc).__name__.lower() or "timed out" in str(exc).lower()


def memory_bytes() -> str:
    try:
        return str(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"))
    except (AttributeError, OSError, ValueError):
        return "unavailable"


def disk_bytes(path: Path | None) -> int:
    if path is None or not path.exists():
        return 0
    return sum(row.stat().st_size for row in path.rglob("*") if row.is_file())


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


def server_resource_usage(pid: int | None, storage_path: Path | None) -> dict[str, Any]:
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
    return {
        "captured": rss is not None and cpu is not None,
        "rss_bytes": rss or 0,
        "cpu_seconds": cpu or 0.0,
        "disk_bytes": disk_bytes(storage_path),
        "availability": {
            "rss_bytes": f"Qdrant server PID {pid}" if rss is not None else "unavailable",
            "cpu_seconds": f"Qdrant server PID {pid}" if cpu is not None else "unavailable",
            "disk_bytes": str(storage_path) if disk_available else "unavailable",
            "bytes_per_op": "unavailable", "allocs_per_op": "unavailable",
            "measurement_error": error,
        },
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

    def call(self, operation: str, category: str, scenario: str, function: Callable[[], Any]) -> Any:
        start = time.perf_counter_ns()
        try:
            return function()
        except BaseException as exc:
            if scenario in self.errors:
                self.errors[scenario] += 1
                self.timeouts[scenario] += int(is_timeout(exc))
            self.events.append({"operation": operation, "scenario": scenario, "kind": "timeout" if is_timeout(exc) else "error", "error": f"{type(exc).__name__}: {exc}"})
            raise
        finally:
            end = time.perf_counter_ns()
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
    def __init__(self, manifest: dict[str, Any], *, client_factory: Callable[[], Any], models: Any, url: str,
                 collection: str, allow_drop: bool, operation_timeout: int, optimizer_timeout: float,
                 poll_interval: float, server_version: str, deployment: str, image: str,
                 storage_path: Path | None, server_pid: int | None) -> None:
        self.manifest, self.config = manifest, manifest["config"]
        self.specs, self.queries = scenario_map(manifest), {row["scenario"]: row for row in manifest["queries"]}
        self.client_factory, self.models = client_factory, models
        self.url, self.collection, self.allow_drop = url, collection, allow_drop
        self.operation_timeout, self.optimizer_timeout, self.poll_interval = operation_timeout, optimizer_timeout, poll_interval
        self.server_version, self.deployment, self.image, self.storage_path = server_version, deployment, image, storage_path
        self.server_pid = server_pid
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
        self.state_scroll: dict[str, Any] = {}
        self.effective_collection: dict[str, Any] = {}
        self.overlap_evidence: dict[str, Any] = {}

    def connect(self) -> None:
        self.client = self.client_factory()

    def create_owned_collection(self) -> None:
        assert self.client is not None
        if self.client.collection_exists(self.collection):
            if not self.allow_drop:
                raise RuntimeError(f"Qdrant collection {self.collection!r} already exists; set ALLOW_DROP=true only for a disposable namespace")
            self.client.delete_collection(self.collection, timeout=self.operation_timeout)
        self.client.create_collection(collection_name=self.collection,
            vectors_config={self.config["vector_field"]: self.models.VectorParams(size=self.config["dimension"], distance=self.models.Distance.COSINE)},
            timeout=self.operation_timeout)
        for field in self.config["scalar_fields"]:
            self.client.create_payload_index(collection_name=self.collection, field_name=field,
                field_schema=self.models.PayloadSchemaType.KEYWORD, wait=True, timeout=self.operation_timeout)
        self.wait_ready(expected_count=0)
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

    def wait_ready(self, expected_count: int | None = None) -> None:
        assert self.client is not None
        deadline, last = time.monotonic() + self.optimizer_timeout, {}
        while time.monotonic() < deadline:
            info = self.client.get_collection(self.collection)
            status, optimizer = enum_text(getattr(info, "status", "")), getattr(info, "optimizer_status", None)
            optimizer_ok = getattr(optimizer, "ok", None) is True or enum_text(optimizer) == "ok"
            points, schema = getattr(info, "points_count", None), getattr(info, "payload_schema", {}) or {}
            last = {"status": status, "optimizer_status": model_value(optimizer), "points_count": points, "payload_fields": sorted(schema)}
            if status == "green" and optimizer_ok and (expected_count is None or points == expected_count) and all(field in schema for field in self.config["scalar_fields"]):
                return
            time.sleep(self.poll_interval)
        raise TimeoutError(f"Qdrant readiness exceeded {self.optimizer_timeout}s; last={last}")

    def point(self, document: dict[str, Any]) -> Any:
        return self.models.PointStruct(id=point_id(document["id"]), vector={self.config["vector_field"]: document["vector"]},
            payload={key: document[key] for key in ("id", "content", "user_id", "fpath")})

    def upsert(self, operation: str, scenario: str, documents: list[dict[str, Any]], wait_ready: bool = True) -> None:
        assert self.client is not None
        for start in range(0, len(documents), self.config["batch_size"]):
            points = [self.point(row) for row in documents[start:start + self.config["batch_size"]]]
            self.evidence.call(operation, "writer", scenario, lambda points=points: self.client.upsert(
                collection_name=self.collection, points=points, wait=True, timeout=self.operation_timeout))
        if wait_ready:
            self.evidence.call(operation, "writer_wait", scenario, self.wait_ready)

    def insert_ranges(self, name: str, ranges: list[dict[str, Any]], wait_each: bool) -> None:
        for insertion in ranges:
            spec = self.specs[insertion["scenario"]]
            documents = [generated_document(spec, ordinal) for ordinal in range(insertion["start"], insertion["start"] + insertion["rows"])]
            self.upsert(name, spec["name"], documents, wait_each)
        if ranges and not wait_each:
            self.evidence.call(name, "writer_wait", "all", self.wait_ready)

    def search(self, operation: str, scenario: str) -> tuple[list[str], list[float]]:
        assert self.client is not None
        spec, query = self.specs[scenario], self.queries[scenario]
        response = self.evidence.call(operation, "search", scenario, lambda: self.client.query_points(
            collection_name=self.collection, query=query["vector"], using=self.config["vector_field"],
            query_filter=payload_filter(self.models, spec), limit=self.config["top_k"], with_payload=True,
            with_vectors=False, timeout=self.operation_timeout))
        started, ids, scores = time.perf_counter_ns(), [], []
        for point in getattr(response, "points", response):
            payload = getattr(point, "payload", None) or {}
            identifier = payload.get("id")
            if not isinstance(identifier, str) or not isinstance(payload.get("content"), str):
                raise RuntimeError("Qdrant result is missing canonical ID/content payload")
            if payload.get("user_id") != spec.get("user_id") or (spec["filter"] == "user_id+fpath" and payload.get("fpath") != spec.get("fpath")):
                self.evidence.cross_user[scenario] += 1
            ids.append(identifier)
            scores.append(float(getattr(point, "score")))
        ended = time.perf_counter_ns()
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

    def delete_filter(self, operation: dict[str, Any]) -> None:
        assert self.client is not None
        name, scenario = operation["name"], operation["target"]
        selector = payload_filter(self.models, operation["filter"])
        self.evidence.call(name, "writer", scenario, lambda: self.client.delete(
            collection_name=self.collection, points_selector=selector, wait=True, timeout=self.operation_timeout))
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
                writer_in_flight = threading.Event()
                sample_start = len(self.evidence.samples)

                def write_round() -> None:
                    writer_in_flight.set()
                    start_barrier.wait()
                    try:
                        spec = self.specs[insertion["scenario"]]
                        documents = [
                            generated_document(spec, value)
                            for value in range(insertion["start"], insertion["start"] + insertion["rows"])
                        ]
                        self.upsert(operation["name"], spec["name"], documents)
                        visible = len(self.retrieve(operation["name"], spec["name"], [row["id"] for row in documents]))
                        missing = len(documents) - visible
                        self.evidence.stale_insert[spec["name"]] += missing
                        if missing:
                            raise RuntimeError(f"batch insert left {missing} IDs invisible")
                    except BaseException:
                        end_barrier.abort()
                        raise
                    finally:
                        try:
                            end_barrier.wait()
                        finally:
                            writer_in_flight.clear()

                def read_round(worker: int) -> None:
                    start_barrier.wait()
                    try:
                        begin = round_value["query_start"] + worker
                        end = round_value["query_start"] + round_value["query_count"]
                        for query_ordinal in range(begin, end, readers):
                            scenario = plan["scenario_order"][query_ordinal % len(plan["scenario_order"])]
                            active_before = writer_in_flight.is_set()
                            self.search(operation["name"], scenario)
                            query_observations[query_ordinal] = {
                                "ordinal": query_ordinal, "round": round_value["ordinal"],
                                "reader": worker, "scenario": scenario,
                                "writer_in_flight": active_before and writer_in_flight.is_set(),
                            }
                    except BaseException:
                        end_barrier.abort()
                        raise
                    end_barrier.wait()

                writer = pool.submit(write_round)
                reader_futures = [pool.submit(read_round, worker) for worker in range(readers)]
                writer.result()
                for future in reader_futures:
                    future.result()

                round_samples = self.evidence.samples[sample_start:]
                writer_samples = [row for row in round_samples if row["operation"] == operation["name"] and row["category"] == "writer"]
                search_samples = [row for row in round_samples if row["operation"] == operation["name"] and row["category"] == "search"]
                overlap = any(
                    writer_sample["start_nanos"] < search_sample["end_nanos"]
                    and search_sample["start_nanos"] < writer_sample["end_nanos"]
                    for writer_sample in writer_samples
                    for search_sample in search_samples
                )
                round_evidence.append({
                    "ordinal": round_value["ordinal"], "queries_executed": len(search_samples),
                    "writer_calls": len(writer_samples), "writer_search_overlap_observed": overlap,
                })
                round_observations.append({
                    "ordinal": round_value["ordinal"], "query_start": round_value["query_start"],
                    "query_count": round_value["query_count"], "insert_range": dict(insertion),
                    "start_barrier": round_value["start_barrier"], "end_barrier": round_value["end_barrier"],
                })

        observed_queries = [row for row in query_observations if row is not None]
        observed_trace = {"queries": observed_queries, "rounds": round_observations}
        trace_hash = timed_trace_digest(observed_trace)
        queries_executed = len(observed_queries)
        rounds_completed = len(round_observations)
        all_overlap = (
            all(row["writer_search_overlap_observed"] for row in round_evidence)
            and all(row["writer_in_flight"] for row in observed_queries)
        )
        self.overlap_evidence = {
            "configured_searches": plan["query_count"], "executed_searches": queries_executed,
            "configured_reader_concurrency": readers, "configured_writer_concurrency": plan["writer_concurrency"],
            "rounds": round_evidence, "all_rounds_writer_search_overlap_observed": all_overlap,
            "timed_execution_sha256": trace_hash,
        }
        expected_trace_hash = timed_execution_digest(plan)
        self.operations.update({
            "timed_queries_executed": queries_executed,
            "timed_rounds_completed": rounds_completed,
            "timed_execution_sha256": trace_hash,
            "timed_execution_trace": observed_trace,
            "batch_insert_during_search": (
                queries_executed == plan["query_count"]
                and rounds_completed == len(plan["rounds"])
                and trace_hash == expected_trace_hash
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
        mutation_in_flight = threading.Event()
        observations: list[dict[str, Any] | None] = [None] * readers

        def mutate() -> None:
            mutation_in_flight.set()
            start_barrier.wait()
            try:
                if operation["effect"] == "delete":
                    self.delete_filter(operation)
                elif operation["effect"] == "insert":
                    self.upsert(operation["name"], operation["target"], operation["documents"])
                    visible = len(self.retrieve(operation["name"], operation["target"],
                                                [row["id"] for row in operation["documents"]]))
                    if visible != len(operation["documents"]):
                        raise RuntimeError("replacement insert did not become visible")
                else:
                    raise RuntimeError(f"unsupported concurrent mutation {operation['effect']}")
            except BaseException:
                end_barrier.abort()
                raise
            finally:
                try:
                    end_barrier.wait()
                finally:
                    mutation_in_flight.clear()

        def read_during_mutation(assignment: dict[str, Any]) -> None:
            start_barrier.wait()
            try:
                active_before = mutation_in_flight.is_set()
                self.search(operation["name"], assignment["scenario"])
                observations[assignment["reader"]] = {
                    **assignment,
                    "mutation_in_flight": active_before and mutation_in_flight.is_set(),
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
            writer.result()
            for future in reader_futures:
                future.result()

        reader_queries = [row for row in observations if row is not None]
        observed = {
            "operation_ordinal": operation["ordinal"], "mutation": plan["mutation"],
            "start_barrier": plan["start_barrier"], "end_barrier": plan["end_barrier"],
            "reader_queries": reader_queries,
        }
        if len(reader_queries) != readers or not all(row["mutation_in_flight"] for row in reader_queries):
            raise RuntimeError(f"concurrent mutation overlap contract failed for operation {operation['ordinal']}")
        self.operations["reindex_execution_trace"]["operations"].append(observed)
        trace = self.operations["reindex_execution_trace"]
        digest = reindex_trace_digest(trace)
        self.operations["reindex_operations_executed"] = len(trace["operations"])
        self.operations["reindex_execution_sha256"] = digest
        self.operations["reindex_delete_replace"] = (
            len(trace["operations"]) == 2
            and all(row["mutation_in_flight"] for item in trace["operations"] for row in item["reader_queries"])
            and digest == reindex_trace_digest(expected_reindex_execution(self.manifest))
        )


    def expected_scroll(self) -> tuple[str, int]:
        accumulator = StateAccumulator()
        for document in final_documents(self.manifest):
            accumulator.add(document)
        return accumulator.hexdigest(), accumulator.count

    def actual_scroll(self) -> tuple[str, int]:
        assert self.client is not None
        accumulator, offset = StateAccumulator(), None
        while True:
            rows, offset = self.client.scroll(collection_name=self.collection, limit=self.config["batch_size"], offset=offset,
                with_payload=True, with_vectors=[self.config["vector_field"]], timeout=self.operation_timeout)
            for row in rows:
                payload, vectors = getattr(row, "payload", None) or {}, getattr(row, "vector", None)
                vector = vectors.get(self.config["vector_field"]) if isinstance(vectors, dict) else None
                document = {**payload, "vector": vector}
                if set(document) != set(DOCUMENT_KEYS) or not isinstance(vector, list) or len(vector) != self.config["dimension"]:
                    raise RuntimeError("Qdrant scroll row does not match the Minima document schema")
                accumulator.add(document)
            if offset is None:
                return accumulator.hexdigest(), accumulator.count

    def run(self) -> None:
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
                self.client.close()
                self.client = None
            elif name == "reopen":
                self.reopen_attempted = True
                self.connect()
                for scenario in self.specs:
                    self.evidence.reopen[scenario] = self.search("post_reopen_parity", scenario)
            elif name == "idempotent_ensure_after_reopen":
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
                actual_hash, actual_rows = self.evidence.call(name, "fetch", "all", self.actual_scroll)
                self.state_scroll = {"algorithm": "sha256(count,xor256,sum256) over canonical id/content/user_id/fpath digests",
                    "expected_hash": expected_hash, "actual_hash": actual_hash, "expected_rows": expected_rows,
                    "actual_rows": actual_rows, "match": expected_hash == actual_hash and expected_rows == actual_rows}
                if not self.state_scroll["match"]:
                    self.evidence.failures.append("final Qdrant scroll/state hash mismatch")
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
        resource = server_resource_usage(self.server_pid, self.storage_path)
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
                "resource": {"captured": resource["captured"], "bytes_per_op": 0.0, "allocs_per_op": 0.0,
                    "rss_bytes": resource["rss_bytes"], "cpu_seconds": resource["cpu_seconds"], "disk_bytes": resource["disk_bytes"]},
            })
        result_hash = self.manifest["expected_state_sha256"] if self.state_scroll.get("match") else self.state_scroll.get("actual_hash", "")
        configuration = {"url": self.url, "collection": self.collection, "vector_field": self.config["vector_field"],
            "dimension": str(self.config["dimension"]), "metric": self.config["metric"], "scalar_fields": ",".join(self.config["scalar_fields"]),
            "top_k": str(self.config["top_k"]), "batch_size": str(self.config["batch_size"]),
            "operation_timeout_seconds": str(self.operation_timeout), "optimizer_timeout_seconds": str(self.optimizer_timeout),
            "write_wait": "true", "point_id_mapping": "uuid5(NAMESPACE_URL,snissn/gomap/minima-qdrant/v1/<manifest-id>)",
            "deployment": self.deployment, "image": self.image or "not_applicable",
            "server_pid": str(self.server_pid) if self.server_pid is not None else "unavailable",
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
            "backend_raw_evidence": {
                "phase_latency_distributions": latency_distributions, "events": self.evidence.events,
                "timed_overlap": self.overlap_evidence, "final_scroll_state": self.state_scroll,
                "resource_availability": resource["availability"],
            }}

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
    parser.add_argument("--storage-path", type=Path)
    parser.add_argument("--server-pid", type=int)
    return parser.parse_args()


def main() -> int:
    args, manifest = parse_args(), None
    manifest = load_manifest(args.manifest)
    installed = importlib.metadata.version("qdrant-client")
    if installed != CLIENT_VERSION:
        raise RuntimeError(f"qdrant-client must be exactly {CLIENT_VERSION}, got {installed}")
    from qdrant_client import QdrantClient, models
    info = server_info(args.url, args.api_key)
    runner = QdrantMinimaRunner(manifest, client_factory=lambda: QdrantClient(url=args.url, api_key=args.api_key or None,
        timeout=args.operation_timeout, prefer_grpc=False), models=models, url=args.url, collection=args.collection,
        allow_drop=args.allow_drop, operation_timeout=args.operation_timeout, optimizer_timeout=args.optimizer_timeout,
        poll_interval=args.poll_interval, server_version=str(info["version"]), deployment=args.deployment,
        image=args.image, storage_path=args.storage_path, server_pid=args.server_pid)
    exit_code = 0
    try:
        runner.run()
    except BaseException as exc:
        runner.evidence.failures.append(f"{type(exc).__name__}: {exc}")
        exit_code = 1
    finally:
        runner.close()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(runner.artifact(), indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
