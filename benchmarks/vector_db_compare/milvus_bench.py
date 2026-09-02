#!/usr/bin/env python3
"""Persistent Milvus Standalone HNSW benchmark for TreeDB vector datasets."""

from __future__ import annotations

import argparse
import json
import re
import statistics
import threading
import time
from pathlib import Path
from typing import Any

import numpy as np

from common import load_exact_truth, load_vectors, max_rss_bytes, parse_ints, parse_ordered_ints, percentile, phase

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def checked_identifier(value: str, name: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{name} must be a simple Milvus identifier")
    return value


def positive_manifest_int(manifest: dict[str, Any], key: str) -> int:
    value = manifest.get(key)
    if isinstance(value, bool):
        raise ValueError(f"manifest {key!r} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"manifest {key!r} must be a positive integer") from exc
    if parsed < 1:
        raise ValueError(f"manifest {key!r} must be a positive integer")
    manifest[key] = parsed
    return parsed


def new_client(args: argparse.Namespace) -> Any:
    from pymilvus import MilvusClient

    return MilvusClient(uri=args.uri, token=args.token)


def search_one(client: Any, args: argparse.Namespace, query: np.ndarray, top_k: int) -> list[int]:
    rows = client.search(
        collection_name=args.collection,
        data=[query.tolist()],
        anns_field="embedding",
        limit=top_k,
        search_params={"metric_type": "COSINE", "params": {"ef": args.ef_search}},
        consistency_level="Strong",
    )
    if len(rows) != 1 or len(rows[0]) != top_k:
        count = len(rows[0]) if len(rows) == 1 else 0
        raise RuntimeError(f"Milvus returned {count} results, want {top_k}")
    ids = [int(row["id"]) for row in rows[0]]
    if len(set(ids)) != top_k:
        raise RuntimeError("Milvus returned duplicate document IDs")
    return ids


def build_database(args: argparse.Namespace, manifest: dict[str, Any], docs: np.ndarray, data_type: Any) -> tuple[dict[str, Any], Any]:
    started = time.perf_counter()
    client = new_client(args)
    if client.has_collection(collection_name=args.collection):
        if not args.allow_drop_collection:
            client.close()
            raise RuntimeError(
                f"collection {args.collection!r} already exists; use a fresh --collection or pass --allow-drop-collection"
            )
        client.drop_collection(collection_name=args.collection)
    schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field(field_name="id", datatype=data_type.INT64, is_primary=True)
    schema.add_field(field_name="embedding", datatype=data_type.FLOAT_VECTOR, dim=manifest["dimensions"])
    client.create_collection(collection_name=args.collection, schema=schema, consistency_level="Strong")
    args.created_collection = True

    insert_started = time.perf_counter()
    for offset in range(0, manifest["docs"], args.insert_batch):
        stop = min(offset + args.insert_batch, manifest["docs"])
        client.insert(
            collection_name=args.collection,
            data=[{"id": i + 1, "embedding": docs[i].tolist()} for i in range(offset, stop)],
        )
    client.flush(collection_name=args.collection)
    insert = phase(insert_started)

    build_started = time.perf_counter()
    index = client.prepare_index_params()
    index.add_index(
        field_name="embedding",
        index_name=args.index,
        index_type="HNSW",
        metric_type="COSINE",
        params={"M": args.m, "efConstruction": args.ef_construction},
    )
    client.create_index(collection_name=args.collection, index_params=index, sync=True)
    build = phase(build_started)
    client.close()
    return {"insert": insert, "build": build, "create_total": phase(started)}, index


def verify_hnsw_index(client: Any, args: argparse.Namespace) -> dict[str, Any]:
    description = client.describe_index(collection_name=args.collection, index_name=args.index)
    expected = {
        "index_type": "HNSW",
        "metric_type": "COSINE",
        "M": str(args.m),
        "efConstruction": str(args.ef_construction),
    }
    mismatches = {key: description.get(key) for key, value in expected.items() if str(description.get(key)) != value}
    if mismatches:
        raise RuntimeError(f"Milvus index does not match frozen HNSW configuration: {description}")
    return dict(description)


def reopen_database(args: argparse.Namespace, expected_docs: int) -> tuple[Any, dict[str, Any], dict[str, Any]]:
    started = time.perf_counter()
    client = new_client(args)
    client.load_collection(collection_name=args.collection)
    stats = client.get_collection_stats(collection_name=args.collection)
    rows = int(stats["row_count"])
    if rows != expected_docs:
        client.close()
        raise RuntimeError(f"Milvus collection has {rows} rows after reopen, want {expected_docs}")
    indexes = client.list_indexes(collection_name=args.collection)
    if args.index not in indexes:
        client.close()
        raise RuntimeError(f"Milvus collection omits HNSW index {args.index!r}: {indexes}")
    index_description = verify_hnsw_index(client, args)
    out = phase(started)
    out.update({"rows": rows, "indexes": list(indexes)})
    return client, out, index_description


def validate_recall(
    client: Any,
    args: argparse.Namespace,
    queries: np.ndarray,
    truth: list[set[int]],
) -> dict[str, Any]:
    started = time.perf_counter()
    exact_total = 0
    overlap = 0
    count = min(args.validate_queries, len(queries), len(truth))
    for i in range(count):
        exact = truth[i]
        ann = set(search_one(client, args, queries[i], args.top_k))
        exact_total += len(exact)
        overlap += len(exact & ann)
    recall = overlap / exact_total if exact_total else 1.0
    if recall < args.min_recall:
        raise RuntimeError(f"recall {recall:.4f} below minimum {args.min_recall:.4f}")
    out = phase(started)
    out.update(
        {
            "queries_checked": count,
            "exact_total": exact_total,
            "ann_total": count * args.top_k,
            "overlap": overlap,
            "recall": recall,
            "min_recall": args.min_recall,
        }
    )
    return out


def benchmark_search(args: argparse.Namespace, queries: np.ndarray, concurrency: int) -> dict[str, Any]:
    if len(queries) == 0:
        return {
            "concurrency": concurrency,
            "queries": 0,
            "total_duration_nanos": 0,
            "avg_nanos": 0,
            "avg_micros": 0,
            "ops_per_second": 0,
            "p50_nanos": 0,
            "p95_nanos": 0,
            "p99_nanos": 0,
        }
    latencies = [0] * len(queries)
    next_index = 0
    lock = threading.Lock()
    error: BaseException | None = None
    stop = threading.Event()
    clients = [new_client(args) for _ in range(concurrency)]

    def worker(client: Any) -> None:
        nonlocal error, next_index
        while not stop.is_set():
            with lock:
                i = next_index
                next_index += 1
            if i >= len(queries):
                return
            started = time.perf_counter_ns()
            try:
                search_one(client, args, queries[i], args.top_k)
            except BaseException as exc:  # noqa: BLE001
                with lock:
                    if error is None:
                        error = exc
                stop.set()
                return
            latencies[i] = time.perf_counter_ns() - started

    started = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(client,)) for client in clients]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    total = time.perf_counter() - started
    for client in clients:
        client.close()
    if error:
        raise error
    ordered = sorted(latencies)
    average = statistics.fmean(latencies)
    return {
        "concurrency": concurrency,
        "queries": len(queries),
        "total_duration_nanos": int(total * 1_000_000_000),
        "avg_nanos": average,
        "avg_micros": average / 1000,
        "ops_per_second": len(queries) / total,
        "p50_nanos": percentile(ordered, 0.50),
        "p95_nanos": percentile(ordered, 0.95),
        "p99_nanos": percentile(ordered, 0.99),
    }


def storage_usage(path: Path | None, docs: int) -> dict[str, Any]:
    available = path is not None and path.exists()
    files = [entry for entry in path.rglob("*") if entry.is_file()] if available else []
    total = sum(entry.stat().st_size for entry in files)
    out: dict[str, Any] = {
        "total_bytes": total,
        "files": len(files),
        "domains": {"milvus_standalone_system": total},
        "bytes_per_doc": total / docs,
    }
    if not available:
        out["unavailable_reason"] = "server storage directory was not supplied"
    return out


def drop_collection(args: argparse.Namespace) -> None:
    client = new_client(args)
    try:
        client.drop_collection(collection_name=args.collection)
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark Milvus Standalone HNSW against a TreeDB vector dataset")
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--uri", required=True)
    parser.add_argument("--token", default="root:Milvus")
    parser.add_argument("--collection", default="gomap_vector_bench")
    parser.add_argument("--index", default="embedding_hnsw")
    parser.add_argument("--allow-drop-collection", action="store_true")
    parser.add_argument("--drop-collection-after", action="store_true")
    parser.add_argument("--storage-dir")
    parser.add_argument("--output", required=True)
    parser.add_argument("--queries", type=int, default=10000)
    parser.add_argument("--validate-queries", type=int, default=64)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--search-concurrency", default="2,4,8,16,32,64,128")
    parser.add_argument("--insert-batch", type=int, default=1000)
    parser.add_argument("--m", type=int, default=16)
    parser.add_argument("--ef-construction", type=int, default=128)
    parser.add_argument("--ef-search", type=int, default=128)
    parser.add_argument("--ef-search-budgets", default="")
    parser.add_argument("--warmup", type=int, default=0)
    parser.add_argument("--min-recall", type=float, default=0.95)
    args = parser.parse_args()
    args.created_collection = False
    args.collection = checked_identifier(args.collection, "--collection")
    args.index = checked_identifier(args.index, "--index")
    for name in ("queries", "top_k", "insert_batch", "m", "ef_construction", "ef_search"):
        if getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be at least 1")
    if args.validate_queries < 0:
        parser.error("--validate-queries must be nonnegative")

    from pymilvus import DataType, __version__ as pymilvus_version

    dataset_dir = Path(args.dataset_dir)
    manifest = json.loads((dataset_dir / "manifest.json").read_text(encoding="utf-8"))
    for key in ("docs", "dimensions", "queries"):
        positive_manifest_int(manifest, key)
    if manifest["metric"] != "cosine" or not manifest["normalized"]:
        raise RuntimeError(f"unsupported dataset metric/normalization: {manifest}")
    docs = load_vectors(dataset_dir / manifest["document_vectors_file"], manifest["docs"], manifest["dimensions"])
    all_queries = load_vectors(dataset_dir / manifest["query_vectors_file"], manifest["queries"], manifest["dimensions"])
    queries = all_queries[: min(args.queries, len(all_queries))]
    truth = load_exact_truth(dataset_dir, manifest)
    if args.top_k != manifest["top_k"] or args.validate_queries > len(truth):
        parser.error("--top-k and --validate-queries must fit the manifest-bound exact truth")
    concurrency = parse_ints(args.search_concurrency)
    budgets = parse_ordered_ints(args.ef_search_budgets) if args.ef_search_budgets else [args.ef_search]
    if args.warmup < 0 or args.warmup > len(queries):
        parser.error("--warmup must be between zero and the available query count")

    try:
        phases, _ = build_database(args, manifest, docs, DataType)
        client, reopen, index_description = reopen_database(args, manifest["docs"])
        search_plan = {"verified_hnsw_index": True, **index_description}
        server_version = client.get_server_version()
        client.close()
        levels = sorted({1, *concurrency})
        budget_searches = []
        for budget in budgets:
            args.ef_search = budget
            client = new_client(args)
            validation = validate_recall(client, args, queries, truth)
            client.close()
            search_benchmarks = []
            for level in levels:
                if args.warmup:
                    benchmark_search(args, queries[: args.warmup], level)
                search_benchmarks.append(benchmark_search(args, queries, level))
            budget_searches.append({"ef": budget, "validation": validation, "search_benchmarks": search_benchmarks})
        first = budget_searches[0]
        result = {
            "backend": "milvus",
            "engine": "milvus_standalone",
            "engine_info": {"server": server_version, "pymilvus": pymilvus_version},
            "ann_index": "hnsw",
            "dataset_dir": str(dataset_dir),
            "collection": args.collection,
            "index": args.index,
            "docs": manifest["docs"],
            "dimensions": manifest["dimensions"],
            "queries": len(queries),
            "top_k": args.top_k,
            "m": args.m,
            "ef_construction": args.ef_construction,
            "ef_search": first["ef"],
            "ef_search_budgets": budgets,
            "warmup_queries_per_cell": args.warmup,
            "insert": phases["insert"],
            "build": phases["build"],
            "create_total": phases["create_total"],
            "reopen_load": reopen,
            "search_plan": search_plan,
            "validation": first["validation"],
            "search": first["search_benchmarks"][0],
            "search_benchmarks": first["search_benchmarks"],
            "budget_searches": budget_searches,
            "storage_after_build": storage_usage(Path(args.storage_dir) if args.storage_dir else None, manifest["docs"]),
            "memory": {"max_rss_bytes": max_rss_bytes()},
        }
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, indent=2))
    finally:
        if args.drop_collection_after and args.created_collection:
            drop_collection(args)


if __name__ == "__main__":
    main()
