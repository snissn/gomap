#!/usr/bin/env python3
"""MongoDB Vector Search benchmark for TreeDB vector datasets.

This benchmark requires MongoDB Vector Search, such as Atlas or a local Atlas
deployment with the MongoDB Search `mongot` process. A plain self-hosted
`mongod` without Vector Search support is intentionally not a comparator.
"""

from __future__ import annotations

import argparse
import json
import statistics
import threading
import time
from pathlib import Path
from typing import Any

import numpy as np
from pymongo import MongoClient
from pymongo.errors import OperationFailure

from common import load_vectors, max_rss_bytes, parse_ints, percentile, phase


def load_manifest(dataset_dir: Path) -> dict[str, Any]:
    return json.loads((dataset_dir / "manifest.json").read_text(encoding="utf-8"))


def connect(uri: str, max_pool_size: int = 100) -> MongoClient:
    return MongoClient(uri, serverSelectionTimeoutMS=5000, maxPoolSize=max_pool_size)


def vector_search_index_definition(dims: int) -> dict[str, Any]:
    return {
        "fields": [
            {
                "type": "vector",
                "path": "embedding",
                "numDimensions": dims,
                "similarity": "cosine",
            }
        ]
    }


def wait_for_index(collection, name: str, timeout_seconds: float) -> dict[str, Any]:
    start = time.perf_counter()
    last: dict[str, Any] = {}
    while time.perf_counter() - start < timeout_seconds:
        try:
            indexes = list(collection.aggregate([{"$listSearchIndexes": {"name": name}}]))
        except OperationFailure as exc:
            raise RuntimeError(
                "MongoDB Vector Search index inspection failed. This benchmark requires Atlas or local Atlas "
                "Vector Search and a user allowed to list search indexes."
            ) from exc
        if indexes:
            last = indexes[0]
            status = str(last.get("status") or last.get("queryable") or "").upper()
            if status in {"READY", "QUERYABLE", "TRUE"} or last.get("queryable") is True:
                out = phase(start)
                out["index_status"] = last
                return out
        time.sleep(1)
    raise RuntimeError(f"MongoDB Vector Search index {name!r} was not queryable within {timeout_seconds}s; last={last!r}")


def build_database(args: argparse.Namespace, manifest: dict[str, Any], docs: np.ndarray) -> tuple[dict[str, Any], str]:
    start = time.perf_counter()
    client = connect(args.uri)
    client.admin.command("ping")
    info = client.server_info().get("version", "unknown")
    db = client[args.database]
    if args.collection in db.list_collection_names():
        if not args.allow_drop_collection:
            raise RuntimeError(
                f"collection {args.database}.{args.collection} already exists; use a fresh collection or pass "
                "--allow-drop-collection for a disposable benchmark database"
            )
        db.drop_collection(args.collection)
    collection = db[args.collection]

    insert_start = time.perf_counter()
    batch: list[dict[str, Any]] = []
    for i in range(manifest["docs"]):
        batch.append(
            {
                "_id": i + 1,
                "id": f"doc-{i:06d}",
                "grp": i % 16,
                "embedding": [float(value) for value in docs[i]],
            }
        )
        if len(batch) >= args.batch_size:
            collection.insert_many(batch, ordered=True)
            batch.clear()
    if batch:
        collection.insert_many(batch, ordered=True)
    insert_phase = phase(insert_start)

    build_start = time.perf_counter()
    try:
        db.command(
            {
                "createSearchIndexes": args.collection,
                "indexes": [
                    {
                        "name": args.index_name,
                        "type": "vectorSearch",
                        "definition": vector_search_index_definition(manifest["dimensions"]),
                    }
                ],
            }
        )
    except OperationFailure as exc:
        raise RuntimeError(
            "MongoDB Vector Search index creation failed. This benchmark requires Atlas or local Atlas Vector Search; "
            "plain mongod does not provide the required createSearchIndexes/$vectorSearch path."
        ) from exc
    wait_phase = wait_for_index(collection, args.index_name, args.index_timeout_seconds)
    build_phase = phase(build_start)
    build_phase["wait"] = wait_phase
    build_phase["index_status"] = wait_phase.get("index_status")
    client.close()
    return {
        "insert": insert_phase,
        "build": build_phase,
        "create_total": phase(start),
    }, str(info)


def reopen_database(args: argparse.Namespace) -> tuple[MongoClient, Any, dict[str, Any]]:
    start = time.perf_counter()
    client = connect(args.uri)
    collection = client[args.database][args.collection]
    count = collection.count_documents({})
    probe = []
    if count:
        seed = collection.find_one({"_id": 1}, {"embedding": 1})
        if seed is None or "embedding" not in seed:
            raise RuntimeError("MongoDB reopen probe could not load seed document _id=1")
        probe = list(
            collection.aggregate(
                [
                    {
                        "$vectorSearch": {
                            "index": args.index_name,
                            "path": "embedding",
                            "queryVector": seed["embedding"],
                            "numCandidates": args.num_candidates,
                            "limit": 1,
                        }
                    },
                    {"$project": {"_id": 1}},
                ]
            )
        )
        if len(probe) != 1:
            raise RuntimeError(f"MongoDB reopen probe returned {len(probe)} rows, want 1")
    out = phase(start)
    out["rows"] = int(count)
    out["probe_rows"] = len(probe)
    return client, collection, out


def search_one(collection, query: list[float], index_name: str, top_k: int, num_candidates: int) -> list[int]:
    rows = list(
        collection.aggregate(
            [
                {
                    "$vectorSearch": {
                        "index": index_name,
                        "path": "embedding",
                        "queryVector": query,
                        "numCandidates": num_candidates,
                        "limit": top_k,
                    }
                },
                {"$project": {"_id": 1, "score": {"$meta": "vectorSearchScore"}}},
            ]
        )
    )
    if len(rows) != top_k:
        raise RuntimeError(f"MongoDB Vector Search returned {len(rows)} results, want {top_k}")
    return [int(row["_id"]) for row in rows]


def exact_topk(docs: np.ndarray, query: np.ndarray, top_k: int) -> set[int]:
    scores = docs @ query
    idx = np.argpartition(-scores, top_k - 1)[:top_k]
    idx = idx[np.argsort(-scores[idx])]
    return {int(i) + 1 for i in idx}


def validate_recall(
    collection,
    docs: np.ndarray,
    query_lists: list[list[float]],
    queries: np.ndarray,
    args: argparse.Namespace,
) -> dict[str, Any]:
    start = time.perf_counter()
    exact_total = 0
    ann_total = 0
    overlap = 0
    count = min(args.validate_queries, len(queries))
    for i in range(count):
        exact = exact_topk(docs, queries[i], args.top_k)
        ann = set(search_one(collection, query_lists[i], args.index_name, args.top_k, args.num_candidates))
        exact_total += len(exact)
        ann_total += len(ann)
        overlap += len(exact & ann)
    recall = overlap / exact_total if exact_total else 1.0
    if recall < args.min_recall:
        raise RuntimeError(f"recall {recall:.4f} below minimum {args.min_recall:.4f}")
    out = phase(start)
    out.update(
        {
            "queries_checked": count,
            "exact_total": exact_total,
            "ann_total": ann_total,
            "overlap": overlap,
            "recall": recall,
            "min_recall": args.min_recall,
        }
    )
    return out


def benchmark_search(args: argparse.Namespace, query_lists: list[list[float]], concurrency: int) -> dict[str, Any]:
    if not query_lists:
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
    latencies = [0] * len(query_lists)
    next_index = 0
    next_lock = threading.Lock()
    first_error: BaseException | None = None
    error_lock = threading.Lock()
    stop_event = threading.Event()
    client = connect(args.uri, max_pool_size=max(100, concurrency))
    collection = client[args.database][args.collection]

    def record_error(exc: BaseException) -> None:
        nonlocal first_error
        with error_lock:
            if first_error is None:
                first_error = exc
        stop_event.set()

    def warm_connection() -> None:
        try:
            client.admin.command("ping")
        except BaseException as exc:  # noqa: BLE001
            record_error(exc)

    warm_threads = [threading.Thread(target=warm_connection) for _ in range(concurrency)]
    for thread in warm_threads:
        thread.start()
    for thread in warm_threads:
        thread.join()
    if first_error:
        client.close()
        raise first_error
    stop_event.clear()

    def worker(collection) -> None:
        nonlocal next_index
        while True:
            if stop_event.is_set():
                return
            with next_lock:
                i = next_index
                next_index += 1
            if i >= len(query_lists):
                return
            start = time.perf_counter_ns()
            try:
                search_one(collection, query_lists[i], args.index_name, args.top_k, args.num_candidates)
            except BaseException as exc:  # noqa: BLE001
                record_error(exc)
                return
            latencies[i] = time.perf_counter_ns() - start

    start_all = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(collection,)) for _ in range(concurrency)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    total = time.perf_counter() - start_all
    client.close()
    if first_error:
        raise first_error
    sorted_latencies = sorted(latencies)
    avg = statistics.fmean(latencies)
    return {
        "concurrency": concurrency,
        "queries": len(query_lists),
        "total_duration_nanos": int(total * 1_000_000_000),
        "avg_nanos": avg,
        "avg_micros": avg / 1000,
        "ops_per_second": len(query_lists) / total,
        "p50_nanos": percentile(sorted_latencies, 0.50),
        "p95_nanos": percentile(sorted_latencies, 0.95),
        "p99_nanos": percentile(sorted_latencies, 0.99),
    }


def storage_usage(client: MongoClient, args: argparse.Namespace, docs: int) -> dict[str, Any]:
    stats = client[args.database].command("collStats", args.collection)
    storage_size = int(stats.get("storageSize", 0))
    index_size = int(stats.get("totalIndexSize", stats.get("indexSize", 0)))
    total = storage_size + index_size
    collection = client[args.database][args.collection]
    search_index: dict[str, Any] = {"name": args.index_name, "bytes_available": False}
    try:
        indexes = list(collection.aggregate([{"$listSearchIndexes": {"name": args.index_name}}]))
        if indexes:
            search_index["status"] = indexes[0].get("status")
            search_index["queryable"] = indexes[0].get("queryable")
    except OperationFailure as exc:
        search_index["error"] = str(exc)
    return {
        "total_bytes": total,
        "total_bytes_excludes_vector_search_index": True,
        "files": 0,
        "domains": {
            "collection_storageSize": storage_size,
            "collection_totalIndexSize": index_size,
            "vector_search_index": search_index,
        },
        "bytes_per_doc": total / docs if docs else 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark MongoDB Vector Search against a TreeDB vector dataset")
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--uri", required=True)
    parser.add_argument("--database", default="gomap_vector_bench")
    parser.add_argument("--collection", default="documents")
    parser.add_argument("--index-name", default="embedding_vector_index")
    parser.add_argument("--allow-drop-collection", action="store_true")
    parser.add_argument("--output", required=True)
    parser.add_argument("--queries", type=int, default=10000)
    parser.add_argument("--validate-queries", type=int, default=64)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--search-concurrency", default="2,4,8,16,32,64,128")
    parser.add_argument("--num-candidates", type=int, default=128)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--index-timeout-seconds", type=float, default=300)
    parser.add_argument("--min-recall", type=float, default=0.95)
    args = parser.parse_args()

    dataset_dir = Path(args.dataset_dir)
    manifest = load_manifest(dataset_dir)
    if manifest["metric"] != "cosine" or not manifest["normalized"]:
        raise RuntimeError(f"unsupported dataset metric/normalization: {manifest}")
    docs = load_vectors(dataset_dir / manifest["document_vectors_file"], manifest["docs"], manifest["dimensions"])
    all_queries = load_vectors(dataset_dir / manifest["query_vectors_file"], manifest["queries"], manifest["dimensions"])
    queries = all_queries[: min(args.queries, len(all_queries))]
    query_lists = [[float(value) for value in query] for query in queries]
    concurrency = parse_ints(args.search_concurrency)

    phases, mongo_info = build_database(args, manifest, docs)
    client, collection, reopen = reopen_database(args)
    validation = validate_recall(collection, docs, query_lists, queries, args)
    storage = storage_usage(client, args, manifest["docs"])
    client.close()

    levels = sorted({1, *concurrency})
    search_benchmarks = [benchmark_search(args, query_lists, level) for level in levels]

    result = {
        "backend": "mongodb_vector_search",
        "engine": "mongodb_vector_search",
        "engine_info": mongo_info,
        "ann_index": "hnsw",
        "dataset_dir": str(dataset_dir),
        "database": args.database,
        "collection": args.collection,
        "docs": manifest["docs"],
        "dimensions": manifest["dimensions"],
        "queries": len(queries),
        "top_k": args.top_k,
        "num_candidates": args.num_candidates,
        "insert": phases["insert"],
        "build": phases["build"],
        "create_total": phases["create_total"],
        "reopen_load": reopen,
        "validation": validation,
        "search": search_benchmarks[0],
        "search_benchmarks": search_benchmarks,
        "storage_after_build": storage,
        "memory": {"max_rss_bytes": max_rss_bytes()},
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
