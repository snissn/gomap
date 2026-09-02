#!/usr/bin/env python3
"""Fail-closed route probes for VectorDBBench search on an existing TreeDB index.

This helper never creates, resets, optimizes, or mutates an index. It captures
checksum-ready metadata plus the required full-diagnostic and production/IDs
responses immediately before the unmodified VectorDBBench --skip-load
--skip-drop-old search command. The VectorDBBench adapter then independently
idempotently opens the same exact configuration.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, is_dataclass
import hashlib
import json
import math
import re
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "clients" / "python" / "treedb_client" / "src"))

from treedb_client import TreeDBClient


def fail(message: str) -> None:
    raise ValueError(message)


def positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        fail(f"{name} must be a positive integer")
    return value


def jsonable(value: Any) -> Any:
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {key: jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    return value


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(jsonable(value), indent=2, sort_keys=True) + "\n")

def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_origin(args: argparse.Namespace, kind: str, result: Path, response: Path, output: Path) -> None:
    write_json(output, {
        "schema_version": "treedb-construction-policy-4587-search-origin/v1",
        "run_id": args.run_id,
        "artifact_root": str(args.artifact_root),
        "manifest_sha256": args.manifest_sha256,
        "execution_commit": args.execution_commit,
        "dataset_sha256": args.dataset_sha256,
        "scale": args.scale,
        "role": args.role,
        "partition": args.partition,
        "ef_construction": args.ef_construction,
        "lifecycle_sha256": args.lifecycle_sha256,
        "lifecycle_started_at": args.lifecycle_started_at,
        "lifecycle_completed_at": args.lifecycle_completed_at,
        "kind": kind,
        "route": "exact" if args.route == "exact" else "scalar_u8_rerank",
        "result_sha256": sha256_file(result),
        "response_sha256": sha256_file(response),
        "index_metadata_sha256": sha256_file(args.metadata_out),
    })


def load_query(path: Path, dimensions: int) -> list[float]:
    value = json.loads(path.read_text())
    if isinstance(value, dict):
        value = value.get("query_embedding")
    if not isinstance(value, list) or len(value) != dimensions:
        fail(f"query must contain exactly {dimensions} coordinates")
    query = []
    for position, coordinate in enumerate(value):
        if isinstance(coordinate, bool) or not isinstance(coordinate, (int, float)) or not math.isfinite(coordinate):
            fail(f"query coordinate {position} must be finite")
        query.append(float(coordinate))
    return query


def validate_metadata(info: dict[str, Any], args: argparse.Namespace) -> None:
    expected = {
        "name": args.index_name,
        "dimension": args.dimensions,
        "metric": args.metric,
        "generation": args.expected_generation,
        "vector_strategy": "column_graph",
        "vector_m": args.m,
        "vector_ef_construction": args.ef_construction,
        "vector_ef_search": args.ef_search,
    }
    for key, wanted in expected.items():
        if info.get(key) != wanted:
            fail(f"existing index {key}={info.get(key)!r}, want {wanted!r}")
    quantized = info.get("quantized_indexes")
    if not isinstance(quantized, list) or not any(
        isinstance(item, dict) and item.get("name") == args.quantized_index_name for item in quantized
    ):
        fail(f"existing index is missing quantized score plane {args.quantized_index_name!r}")


def validate_diagnostic(response: dict[str, Any], args: argparse.Namespace) -> None:
    expected_mode = "exact" if args.route == "exact" else "quantized_rerank"
    expected_route = "exact_hnsw_search_pack_v1" if args.route == "exact" else "quantized_rerank"
    if response.get("no_documents") is not True:
        fail("diagnostic response did not use the no-document boundary")
    if response.get("query_mode") != expected_mode:
        fail("diagnostic response query mode does not match the requested route")
    results = response.get("results")
    if not isinstance(results, list) or len(results) != args.top_k:
        fail("diagnostic response result count does not equal topK")
    diagnostics = response.get("diagnostics")
    if not isinstance(diagnostics, dict):
        fail("diagnostic response is missing diagnostics")
    if (diagnostics.get("fallback_reason") or "none") != "none":
        fail("diagnostic response used a fallback")
    if diagnostics.get("route") != expected_route:
        fail("diagnostic response did not use the intended route")
    stats = response.get("stats")
    if not isinstance(stats, dict):
        fail("diagnostic response is missing stats")
    if not {"documents_fetched", "document_bytes"} <= set(stats):
        fail("diagnostic response is missing document counters")
    if stats["documents_fetched"] != 0 or stats["document_bytes"] != 0:
        fail("diagnostic response fetched or materialized documents")
    if args.route == "scalar_u8_rerank":
        calls = positive_int(stats.get("quantized_rerank_exact_score_calls"), "exact rerank calls")
        if not args.top_k <= calls <= args.effective_rerank_candidates:
            fail("diagnostic response exceeded the frozen exact-rerank boundary")


def validate_production(response: dict[str, Any], args: argparse.Namespace) -> None:
    if response.get("response_format") != "ids":
        fail("production response did not use the IDs-only transport")
    ids = response.get("ids")
    if not isinstance(ids, list) or len(ids) != args.top_k:
        fail("production response result count does not equal topK")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--index-name", required=True)
    parser.add_argument("--query-json", required=True, type=Path)
    parser.add_argument("--metadata-out", required=True, type=Path)
    parser.add_argument("--diagnostic-response-out", required=True, type=Path)
    parser.add_argument("--production-response-out", required=True, type=Path)
    parser.add_argument("--diagnostic-result", required=True, type=Path)
    parser.add_argument("--production-result", required=True, type=Path)
    parser.add_argument("--diagnostic-origin-out", required=True, type=Path)
    parser.add_argument("--production-origin-out", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--artifact-root", required=True, type=Path)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--execution-commit", required=True)
    parser.add_argument("--dataset-sha256", required=True)
    parser.add_argument("--scale", required=True, type=int)
    parser.add_argument("--role", required=True)
    parser.add_argument("--partition", required=True)
    parser.add_argument("--lifecycle-sha256", required=True)
    parser.add_argument("--lifecycle-started-at", required=True)
    parser.add_argument("--lifecycle-completed-at", required=True)
    parser.add_argument("--dimensions", type=int, default=768)
    parser.add_argument("--metric", default="cosine", choices=("cosine",))
    parser.add_argument("--m", type=int, default=16)
    parser.add_argument("--ef-construction", type=int, required=True)
    parser.add_argument("--ef-search", type=int, default=192)
    parser.add_argument("--expected-generation", type=int, required=True)
    parser.add_argument("--route", choices=("exact", "scalar_u8_rerank"), required=True)
    parser.add_argument("--top-k", type=int, default=100)
    parser.add_argument("--quantized-index-name", default="embedding.scalar_u8.fast")
    parser.add_argument("--rerank-candidates", type=int, default=400)
    parser.add_argument("--effective-rerank-candidates", type=int, default=192)
    parser.add_argument("--timeout", type=float, default=3600.0)
    args = parser.parse_args()
    for name in ("dimensions", "m", "ef_construction", "ef_search", "expected_generation", "top_k",
                 "rerank_candidates", "effective_rerank_candidates"):
        positive_int(getattr(args, name), name)
    if args.effective_rerank_candidates > args.rerank_candidates:
        fail("effective rerank candidates cannot exceed the configured shortlist")
    args.artifact_root = args.artifact_root.resolve()
    for name in ("manifest_sha256", "dataset_sha256", "lifecycle_sha256"):
        if not isinstance(getattr(args, name), str) or re.fullmatch(r"[0-9a-f]{64}", getattr(args, name)) is None:
            fail(f"{name} must be a lowercase SHA-256")
    if re.fullmatch(r"[0-9a-f]{40}", args.execution_commit) is None:
        fail("execution_commit must be a lowercase commit digest")
    if not args.run_id or not args.role or not args.partition:
        fail("run identity strings must be non-empty")
    positive_int(args.scale, "scale")
    for path in (args.diagnostic_result, args.production_result):
        if not path.is_file():
            fail(f"canonical result does not exist: {path}")
    paths = (
        args.metadata_out, args.diagnostic_response_out, args.production_response_out,
        args.diagnostic_result, args.production_result,
        args.diagnostic_origin_out, args.production_origin_out,
    )
    for path in paths:
        try:
            path.resolve().relative_to(args.artifact_root)
        except ValueError:
            fail(f"search evidence path escapes artifact root: {path}")
    manifest_path = args.artifact_root / "manifest.json"
    if sha256_file(manifest_path) != args.manifest_sha256:
        fail("manifest_sha256 does not match artifact manifest")
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("artifact_root") != str(args.artifact_root):
        fail("manifest artifact root does not match --artifact-root")
    lifecycle = manifest.get("lifecycle")
    if not isinstance(lifecycle, dict) or lifecycle.get("sha256") != args.lifecycle_sha256:
        fail("lifecycle_sha256 does not match artifact manifest")
    lifecycle_path = args.artifact_root / lifecycle.get("file", "")
    if sha256_file(lifecycle_path) != args.lifecycle_sha256:
        fail("lifecycle_sha256 does not match lifecycle file")
    events = [json.loads(line) for line in lifecycle_path.read_text().splitlines()]
    starts = [event.get("timestamp") for event in events if event.get("stage") == "startup"]
    completed = [event.get("timestamp") for event in events if event.get("stage") == "teardown"]
    if starts != [args.lifecycle_started_at] or completed != [args.lifecycle_completed_at]:
        fail("lifecycle timestamps do not match lifecycle file")
    return args


def main() -> int:
    args = parse_args()
    query = load_query(args.query_json, args.dimensions)
    client = TreeDBClient(args.base_url, timeout=args.timeout)
    try:
        before = jsonable(client.open_index(args.index_name))
        validate_metadata(before, args)
        common = {
            "ef_search": args.ef_search,
            "query_mode": "exact" if args.route == "exact" else "quantized_rerank",
            "expected_generation": args.expected_generation,
            "query_embedding_encoding": "f32_le",
        }
        if args.route == "scalar_u8_rerank":
            common.update({
                "quantized_index_name": args.quantized_index_name,
                "quantized_rerank_candidates": args.rerank_candidates,
            })
        diagnostic = jsonable(client.search_vector_index(
            args.index_name, query, args.top_k, stats_mode="full_diagnostics", response_format="full", **common
        ))
        validate_diagnostic(diagnostic, args)
        production = jsonable(client.search_vector_index(
            args.index_name, query, args.top_k, stats_mode="production", response_format="ids", **common
        ))
        validate_production(production, args)
        after = jsonable(client.open_index(args.index_name))
        validate_metadata(after, args)
        if after != before:
            fail("existing index identity changed during route probes")
        write_json(args.metadata_out, before)
        write_json(args.diagnostic_response_out, diagnostic)
        write_json(args.production_response_out, production)
        write_origin(
            args, "diagnostic", args.diagnostic_result,
            args.diagnostic_response_out, args.diagnostic_origin_out,
        )
        write_origin(
            args, "production", args.production_result,
            args.production_response_out, args.production_origin_out,
        )
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"INVALID: {exc}")
        raise SystemExit(1) from exc
