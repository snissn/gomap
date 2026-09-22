#!/usr/bin/env python3
"""Persistent SQLite+Vectorlite HNSW benchmark for TreeDB vector datasets."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import statistics
import threading
import time
from pathlib import Path
from typing import Any

import numpy as np

from common import load_vectors, max_rss_bytes, parse_ints, percentile, phase


def storage_usage(path: Path) -> dict[str, Any]:
    total = 0
    files = 0
    domains: dict[str, int] = {}
    for root, _, names in os.walk(path):
        for name in names:
            file_path = Path(root) / name
            size = file_path.stat().st_size
            rel = file_path.relative_to(path)
            domain = rel.parts[0] if rel.parts else name
            total += size
            files += 1
            domains[domain] = domains.get(domain, 0) + size
    return {"total_bytes": total, "files": files, "domains": domains}


def load_manifest(dataset_dir: Path) -> dict[str, Any]:
    with (dataset_dir / "manifest.json").open("r", encoding="utf-8") as f:
        return json.load(f)


def load_document_payloads(dataset_dir: Path, manifest: dict[str, Any]) -> list[tuple[str, bytes]]:
    path = dataset_dir / manifest.get("documents_jsonl_file", "documents.jsonl")
    records: list[tuple[str, bytes]] = []
    with path.open("rb") as f:
        for i, raw_line in enumerate(f):
            line = raw_line.rstrip(b"\n")
            document = json.loads(line)
            if document.get("index") != i:
                raise ValueError(f"{path} row {i} has index={document.get('index')}, want {i}")
            document_id = document.get("id")
            if not document_id:
                raise ValueError(f"{path} row {i} has empty id")
            records.append((str(document_id), line))
    if len(records) != manifest["docs"]:
        raise ValueError(f"{path} has {len(records)} rows, want {manifest['docs']}")
    return records


def sqlite_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect(db_path: Path):
    import vectorlite_py

    db = sqlite3.connect(str(db_path), check_same_thread=False)
    try:
        db.enable_load_extension(True)
        db.load_extension(vectorlite_py.vectorlite_path())
    except (AttributeError, sqlite3.Error) as exc:
        db.close()
        raise RuntimeError(
            "SQLite loadable extensions are unavailable or Vectorlite could not be loaded; "
            "use a Python sqlite3 build with extension loading enabled and install vectorlite-py"
        ) from exc
    finally:
        try:
            db.enable_load_extension(False)
        except (AttributeError, sqlite3.Error):
            pass
    return db


def configure_sqlite(db: sqlite3.Connection, page_size: int, cache_mb: int, *, set_page_size: bool = False) -> None:
    if set_page_size:
        db.execute(f"pragma page_size={page_size}")
    db.execute("pragma journal_mode=wal")
    db.execute("pragma synchronous=normal")
    db.execute("pragma temp_store=memory")
    db.execute(f"pragma cache_size={-cache_mb * 1024}")


def prepare_empty_dir(path: Path, label: str) -> None:
    if path.exists():
        if not path.is_dir():
            raise RuntimeError(f"{label} {path} exists and is not a directory")
        if any(path.iterdir()):
            raise RuntimeError(f"{label} {path} already exists and is not empty")
        return
    path.mkdir(parents=True)


def build_database(args: argparse.Namespace, dataset_dir: Path, manifest: dict[str, Any]) -> tuple[dict[str, Any], str, np.ndarray]:
    db_dir = Path(args.db_dir)
    prepare_empty_dir(db_dir, "Vectorlite DB directory")
    db_path = db_dir / "vectorlite.db"
    index_path = db_dir / "vectorlite.hnsw"

    start = time.perf_counter()
    db = connect(db_path)
    configure_sqlite(db, args.page_size, args.cache_mb, set_page_size=True)
    version = db.execute("select vectorlite_info()").fetchone()[0]
    db.execute("create table documents(id text primary key, document blob not null)")
    db.execute(
        "create virtual table vectors using vectorlite("
        f"embedding float32[{manifest['dimensions']}] cosine, "
        f"hnsw(max_elements={manifest['docs']}, ef_construction={args.ef_construction}, M={args.m}), "
        f"{sqlite_quote(str(index_path))})"
    )
    insert_start = time.perf_counter()
    document_payloads = load_document_payloads(dataset_dir, manifest)
    with db:
        db.executemany(
            "insert into documents(rowid, id, document) values (?, ?, ?)",
            (
                (i + 1, document_payloads[i][0], sqlite3.Binary(document_payloads[i][1]))
                for i in range(manifest["docs"])
            ),
        )
    insert_phase = phase(insert_start)
    build_start = time.perf_counter()
    docs = load_vectors(dataset_dir / manifest["document_vectors_file"], manifest["docs"], manifest["dimensions"])
    with db:
        db.executemany(
            "insert into vectors(rowid, embedding) values (?, ?)",
            ((i + 1, memoryview(docs[i]).tobytes()) for i in range(manifest["docs"])),
        )
    db.execute("pragma wal_checkpoint(truncate)")
    db.close()
    build_phase = phase(build_start)
    total_phase = phase(start)
    return {
        "insert": insert_phase,
        "build": build_phase,
        "create_total": total_phase,
    }, version, docs


def reopen_database(args: argparse.Namespace) -> tuple[sqlite3.Connection, dict[str, Any]]:
    start = time.perf_counter()
    db = connect(Path(args.db_dir) / "vectorlite.db")
    configure_sqlite(db, args.page_size, args.cache_mb)
    count = db.execute("select count(*) from documents").fetchone()[0]
    if count > 0:
        payload = db.execute("select document from documents where rowid = 1").fetchone()[0]
        probe_document = json.loads(bytes(payload))
        probe = np.asarray(probe_document["embedding"], dtype="<f4").tobytes()
        probe_rows = db.execute(
            "select rowid from vectors where knn_search(embedding, knn_param(?, 1, ?))",
            [probe, args.ef_search],
        ).fetchall()
        if len(probe_rows) != 1:
            raise RuntimeError(f"vectorlite reopen probe returned {len(probe_rows)} rows, want 1")
    else:
        probe_rows = []
    out = phase(start)
    out["rows"] = count
    out["probe_rows"] = len(probe_rows)
    return db, out


def search_one(db: sqlite3.Connection, query: np.ndarray, top_k: int, ef_search: int) -> list[int]:
    rows = db.execute(
        "select rowid, distance from vectors where knn_search(embedding, knn_param(?, ?, ?))",
        [memoryview(query).tobytes(), top_k, ef_search],
    ).fetchall()
    if len(rows) != top_k:
        raise RuntimeError(f"vectorlite returned {len(rows)} results, want {top_k}")
    return [int(row[0]) for row in rows]


def exact_topk(docs: np.ndarray, query: np.ndarray, top_k: int) -> set[int]:
    scores = docs @ query
    idx = np.argpartition(-scores, top_k - 1)[:top_k]
    idx = idx[np.argsort(-scores[idx])]
    return {int(i) + 1 for i in idx}


def validate_recall(
    db: sqlite3.Connection,
    docs: np.ndarray,
    queries: np.ndarray,
    top_k: int,
    ef_search: int,
    validate_queries: int,
    min_recall: float,
) -> dict[str, Any]:
    start = time.perf_counter()
    exact_total = 0
    ann_total = 0
    overlap = 0
    count = min(validate_queries, len(queries))
    for i in range(count):
        exact = exact_topk(docs, queries[i], top_k)
        ann = set(search_one(db, queries[i], top_k, ef_search))
        exact_total += len(exact)
        ann_total += len(ann)
        overlap += len(exact & ann)
    recall = overlap / exact_total if exact_total else 1.0
    if recall < min_recall:
        raise RuntimeError(f"recall {recall:.4f} below minimum {min_recall:.4f}")
    out = phase(start)
    out.update(
        {
            "queries_checked": count,
            "exact_total": exact_total,
            "ann_total": ann_total,
            "overlap": overlap,
            "recall": recall,
            "min_recall": min_recall,
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
    db_path = Path(args.db_dir) / "vectorlite.db"
    latencies = [0] * len(queries)
    first_error: list[Exception] = []
    conns = [connect(db_path) for _ in range(concurrency)]
    for conn in conns:
        configure_sqlite(conn, args.page_size, args.cache_mb)
        search_one(conn, queries[0], 1, args.ef_search)

    def worker(conn: sqlite3.Connection, indexes: range) -> None:
        for i in indexes:
            start = time.perf_counter_ns()
            try:
                search_one(conn, queries[i], args.top_k, args.ef_search)
            except Exception as exc:  # noqa: BLE001
                first_error.append(exc)
                return
            latencies[i] = time.perf_counter_ns() - start

    start_all = time.perf_counter()
    threads = [
        threading.Thread(target=worker, args=(conn, range(worker_id, len(queries), concurrency)))
        for worker_id, conn in enumerate(conns)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    total = time.perf_counter() - start_all
    for conn in conns:
        conn.close()
    if first_error:
        raise first_error[0]
    sorted_latencies = sorted(latencies)
    avg = statistics.fmean(latencies)
    return {
        "concurrency": concurrency,
        "queries": len(queries),
        "total_duration_nanos": int(total * 1_000_000_000),
        "avg_nanos": avg,
        "avg_micros": avg / 1000,
        "ops_per_second": len(queries) / total if total > 0 else 0,
        "p50_nanos": percentile(sorted_latencies, 0.50),
        "p95_nanos": percentile(sorted_latencies, 0.95),
        "p99_nanos": percentile(sorted_latencies, 0.99),
    }

def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark persistent SQLite+Vectorlite HNSW against a TreeDB vector dataset")
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--db-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--queries", type=int, default=10000)
    parser.add_argument("--validate-queries", type=int, default=64)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--search-concurrency", default="2,4,8,16,32,64,128")
    parser.add_argument("--m", type=int, default=16)
    parser.add_argument("--ef-construction", type=int, default=128)
    parser.add_argument("--ef-search", type=int, default=128)
    parser.add_argument("--page-size", type=int, default=4096)
    parser.add_argument("--cache-mb", type=int, default=256)
    parser.add_argument("--min-recall", type=float, default=0.95)
    args = parser.parse_args()

    dataset_dir = Path(args.dataset_dir)
    manifest = load_manifest(dataset_dir)
    if manifest["metric"] != "cosine" or not manifest["normalized"]:
        raise RuntimeError(
            "unsupported dataset metric/normalization: "
            f"metric={manifest.get('metric')!r} normalized={manifest.get('normalized')!r}"
        )
    all_queries = load_vectors(dataset_dir / manifest["query_vectors_file"], manifest["queries"], manifest["dimensions"])
    queries = all_queries[: min(args.queries, len(all_queries))]
    concurrency = parse_ints(args.search_concurrency)

    phases, vectorlite_info, docs = build_database(args, dataset_dir, manifest)
    db, reopen = reopen_database(args)
    validation = validate_recall(db, docs, all_queries, args.top_k, args.ef_search, args.validate_queries, args.min_recall)
    db.close()

    levels = sorted({1, *concurrency})
    search_benchmarks = [benchmark_search(args, queries, level) for level in levels]
    storage = storage_usage(Path(args.db_dir))
    storage["bytes_per_doc"] = storage["total_bytes"] / manifest["docs"]
    memory: dict[str, Any] = {}
    rss = max_rss_bytes()
    if rss is not None:
        memory["max_rss_bytes"] = rss

    result = {
        "backend": "sqlite_vectorlite",
        "engine": "vectorlite",
        "engine_info": vectorlite_info,
        "ann_index": "hnsw",
        "db_dir": str(Path(args.db_dir)),
        "dataset_dir": str(dataset_dir),
        "docs": manifest["docs"],
        "dimensions": manifest["dimensions"],
        "queries": len(queries),
        "top_k": args.top_k,
        "m": args.m,
        "ef_construction": args.ef_construction,
        "ef_search": args.ef_search,
        "page_size": args.page_size,
        "cache_mb": args.cache_mb,
        "insert": phases["insert"],
        "build": phases["build"],
        "create_total": phases["create_total"],
        "reopen_load": reopen,
        "validation": validation,
        "search": search_benchmarks[0],
        "search_benchmarks": search_benchmarks,
        "storage_after_build": storage,
        "memory": memory,
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
