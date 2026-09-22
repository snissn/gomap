#!/usr/bin/env python3
"""Persistent PostgreSQL+pgvector HNSW benchmark for TreeDB vector datasets."""

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
import psycopg

from common import load_exact_truth, load_vectors, max_rss_bytes, parse_ints, parse_ordered_ints, percentile, phase

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_manifest(dataset_dir: Path) -> dict[str, Any]:
    return json.loads((dataset_dir / "manifest.json").read_text(encoding="utf-8"))


def checked_manifest_positive_int(manifest: dict[str, Any], key: str) -> int:
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


def vector_literal(vector: np.ndarray) -> str:
    return "[" + ",".join(f"{float(value):.9g}" for value in vector) + "]"


def connect(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, autocommit=True)


def checked_identifier(value: str, name: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{name} must be a simple PostgreSQL identifier")
    return value


def table_name(args: argparse.Namespace) -> str:
    schema = checked_identifier(args.schema, "--schema")
    table = checked_identifier(args.table, "--table")
    return f"{schema}.{table}"


def index_name(args: argparse.Namespace) -> str:
    raw = args.index or f"{args.table}_embedding_hnsw"
    return checked_identifier(raw, "--index")


def build_database(args: argparse.Namespace, manifest: dict[str, Any], docs: np.ndarray) -> tuple[dict[str, Any], str]:
    start = time.perf_counter()
    qualified_table = table_name(args)
    schema = checked_identifier(args.schema, "--schema")
    index = index_name(args)
    dimensions = checked_manifest_positive_int(manifest, "dimensions")
    doc_count = checked_manifest_positive_int(manifest, "docs")
    with connect(args.dsn) as conn:
        version = conn.execute("select version()").fetchone()[0]
        try:
            conn.execute("create extension if not exists vector")
        except psycopg.Error as exc:
            raise RuntimeError(
                "could not create or verify the pgvector extension; connect to a PostgreSQL database where "
                "pgvector is installed and the user can run CREATE EXTENSION, or enable the extension before running "
                f"the benchmark: {exc}"
            ) from exc
        exists = conn.execute("select exists(select 1 from information_schema.schemata where schema_name = %s)", [schema]).fetchone()[0]
        if exists:
            if not args.allow_drop_schema:
                raise RuntimeError(
                    f"schema {schema!r} already exists; use a fresh --schema or pass --allow-drop-schema for a disposable benchmark database"
                )
            conn.execute(f"drop schema {schema} cascade")
        conn.execute(f"create schema {schema}")
        args.created_schema = True
        conn.execute(
            f"create table {qualified_table} ("
            f"doc_id integer primary key, "
            f"id text not null, "
            f"grp integer not null, "
            f"embedding vector({dimensions}) not null)"
        )
        insert_start = time.perf_counter()
        prepare_seconds = 0.0
        copy_write_seconds = 0.0
        with conn.transaction():
            with conn.cursor().copy(f"copy {qualified_table} (doc_id, id, grp, embedding) from stdin") as copy:
                for i in range(doc_count):
                    prepare_start = time.perf_counter()
                    row = (i + 1, f"doc-{i:06d}", i % 16, vector_literal(docs[i]))
                    prepare_seconds += time.perf_counter() - prepare_start
                    copy_write_start = time.perf_counter()
                    copy.write_row(row)
                    copy_write_seconds += time.perf_counter() - copy_write_start
        insert_phase = phase(insert_start)
        insert_phase["client_prepare"] = {
            "duration_nanos": int(prepare_seconds * 1_000_000_000),
            "seconds": prepare_seconds,
        }
        insert_phase["copy_write"] = {
            "duration_nanos": int(copy_write_seconds * 1_000_000_000),
            "seconds": copy_write_seconds,
        }
        build_start = time.perf_counter()
        with conn.transaction():
            conn.execute(
                f"create index {index} "
                f"on {qualified_table} using hnsw (embedding vector_cosine_ops) "
                f"with (m = {args.m}, ef_construction = {args.ef_construction})"
            )
            conn.execute(f"analyze {qualified_table}")
        build_phase = phase(build_start)
    return {
        "insert": insert_phase,
        "build": build_phase,
        "create_total": phase(start),
    }, version


def configure_search_session(conn: psycopg.Connection, args: argparse.Namespace) -> None:
    conn.execute(f"set hnsw.ef_search = {int(args.ef_search)}")
    conn.execute("set enable_seqscan = off")
    conn.execute(f"set search_path = {checked_identifier(args.schema, '--schema')}, public")


def reopen_database(args: argparse.Namespace) -> tuple[psycopg.Connection, dict[str, Any]]:
    start = time.perf_counter()
    qualified_table = table_name(args)
    conn = connect(args.dsn)
    configure_search_session(conn, args)
    count = conn.execute(f"select count(*) from {qualified_table}").fetchone()[0]
    probe = conn.execute(
        f"select doc_id from {qualified_table} order by embedding <=> (select embedding from {qualified_table} where doc_id = 1) limit 1"
    ).fetchall()
    if len(probe) != 1:
        raise RuntimeError(f"pgvector reopen probe returned {len(probe)} rows, want 1")
    out = phase(start)
    out["rows"] = int(count)
    out["probe_rows"] = len(probe)
    return conn, out


def plan_uses_index(plan: Any, expected_index: str) -> bool:
    if isinstance(plan, list):
        return any(plan_uses_index(item, expected_index) for item in plan)
    if not isinstance(plan, dict):
        return False
    if plan.get("Index Name") == expected_index:
        return True
    return any(plan_uses_index(value, expected_index) for value in plan.values())


def verify_hnsw_search_plan(conn: psycopg.Connection, args: argparse.Namespace, query: str) -> dict[str, Any]:
    qualified_table = table_name(args)
    expected_index = index_name(args)
    row = conn.execute(
        f"explain (format json) select doc_id from {qualified_table} order by embedding <=> %s::vector limit %s",
        [query, args.top_k],
    ).fetchone()
    plan = row[0]
    if isinstance(plan, str):
        plan = json.loads(plan)
    if not plan_uses_index(plan, expected_index):
        raise RuntimeError(f"pgvector search plan did not use HNSW index {expected_index}: {json.dumps(plan)}")
    return {
        "verified_hnsw_index": True,
        "index": expected_index,
        "enable_seqscan": False,
    }


def search_one(conn: psycopg.Connection, args: argparse.Namespace, query: str, top_k: int) -> list[int]:
    qualified_table = table_name(args)
    rows = conn.execute(
        f"select doc_id from {qualified_table} order by embedding <=> %s::vector limit %s",
        [query, top_k],
    ).fetchall()
    if len(rows) != top_k:
        raise RuntimeError(f"pgvector returned {len(rows)} results, want {top_k}")
    return [int(row[0]) for row in rows]


def validate_recall(
    conn: psycopg.Connection,
    args: argparse.Namespace,
    query_literals: list[str],
    truth: list[set[int]],
    top_k: int,
    validate_queries: int,
    min_recall: float,
) -> dict[str, Any]:
    start = time.perf_counter()
    exact_total = 0
    ann_total = 0
    overlap = 0
    count = min(validate_queries, len(query_literals), len(truth))
    for i in range(count):
        exact = truth[i]
        ann = set(search_one(conn, args, query_literals[i], top_k))
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


def benchmark_search(args: argparse.Namespace, query_literals: list[str], concurrency: int) -> dict[str, Any]:
    if not query_literals:
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
    latencies = [0] * len(query_literals)
    next_index = 0
    next_lock = threading.Lock()
    first_error: BaseException | None = None
    error_lock = threading.Lock()
    stop_event = threading.Event()
    conns = [connect(args.dsn) for _ in range(concurrency)]
    for conn in conns:
        configure_search_session(conn, args)

    def worker(conn: psycopg.Connection) -> None:
        nonlocal first_error, next_index
        while True:
            if stop_event.is_set():
                return
            with next_lock:
                i = next_index
                next_index += 1
            if i >= len(query_literals):
                return
            start = time.perf_counter_ns()
            try:
                search_one(conn, args, query_literals[i], args.top_k)
            except BaseException as exc:  # noqa: BLE001
                with error_lock:
                    if first_error is None:
                        first_error = exc
                stop_event.set()
                return
            latencies[i] = time.perf_counter_ns() - start

    start_all = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(conn,)) for conn in conns]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    total = time.perf_counter() - start_all
    for conn in conns:
        conn.close()
    if first_error:
        raise first_error
    sorted_latencies = sorted(latencies)
    avg = statistics.fmean(latencies)
    return {
        "concurrency": concurrency,
        "queries": len(query_literals),
        "total_duration_nanos": int(total * 1_000_000_000),
        "avg_nanos": avg,
        "avg_micros": avg / 1000,
        "ops_per_second": len(query_literals) / total,
        "p50_nanos": percentile(sorted_latencies, 0.50),
        "p95_nanos": percentile(sorted_latencies, 0.95),
        "p99_nanos": percentile(sorted_latencies, 0.99),
    }


def storage_usage(conn: psycopg.Connection, args: argparse.Namespace, docs: int) -> dict[str, Any]:
    qualified = f"{checked_identifier(args.schema, '--schema')}.{checked_identifier(args.table, '--table')}"
    table = int(conn.execute("select pg_total_relation_size(%s::regclass)", [qualified]).fetchone()[0])
    indexes = int(conn.execute("select pg_indexes_size(%s::regclass)", [qualified]).fetchone()[0])
    return {
        "total_bytes": table,
        "files": 0,
        "domains": {
            "documents_total_relation": table,
            "documents_indexes": indexes,
        },
        "bytes_per_doc": table / docs,
    }


def drop_schema(args: argparse.Namespace) -> None:
    schema = checked_identifier(args.schema, "--schema")
    with connect(args.dsn) as conn:
        conn.execute(f"drop schema if exists {schema} cascade")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark PostgreSQL+pgvector HNSW against a TreeDB vector dataset")
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--schema", default="gomap_vector_bench")
    parser.add_argument("--table", default="documents")
    parser.add_argument("--index", default="", help="HNSW index name. Defaults to <table>_embedding_hnsw.")
    parser.add_argument("--allow-drop-schema", action="store_true")
    parser.add_argument("--drop-schema-after", action="store_true")
    parser.add_argument("--output", required=True)
    parser.add_argument("--queries", type=int, default=10000)
    parser.add_argument("--validate-queries", type=int, default=64)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--search-concurrency", default="2,4,8,16,32,64,128")
    parser.add_argument("--m", type=int, default=16)
    parser.add_argument("--ef-construction", type=int, default=128)
    parser.add_argument("--ef-search", type=int, default=128)
    parser.add_argument("--ef-search-budgets", default="")
    parser.add_argument("--warmup", type=int, default=0)
    parser.add_argument("--min-recall", type=float, default=0.95)
    args = parser.parse_args()
    args.created_schema = False

    dataset_dir = Path(args.dataset_dir)
    manifest = load_manifest(dataset_dir)
    checked_manifest_positive_int(manifest, "docs")
    checked_manifest_positive_int(manifest, "dimensions")
    checked_manifest_positive_int(manifest, "queries")
    if manifest["metric"] != "cosine" or not manifest["normalized"]:
        raise RuntimeError(f"unsupported dataset metric/normalization: {manifest}")
    try:
        docs = load_vectors(dataset_dir / manifest["document_vectors_file"], manifest["docs"], manifest["dimensions"])
        all_queries = load_vectors(dataset_dir / manifest["query_vectors_file"], manifest["queries"], manifest["dimensions"])
        queries = all_queries[: min(args.queries, len(all_queries))]
        query_literals = [vector_literal(query) for query in queries]
        truth = load_exact_truth(dataset_dir, manifest)
        if args.top_k != manifest["top_k"] or args.validate_queries > len(truth):
            parser.error("--top-k and --validate-queries must fit the manifest-bound exact truth")
        concurrency = parse_ints(args.search_concurrency)
        budgets = parse_ordered_ints(args.ef_search_budgets) if args.ef_search_budgets else [args.ef_search]
        if args.warmup < 0 or args.warmup > len(query_literals):
            parser.error("--warmup must be between zero and the available query count")

        phases, postgres_info = build_database(args, manifest, docs)
        conn, reopen = reopen_database(args)
        search_plan = verify_hnsw_search_plan(conn, args, query_literals[0]) if query_literals else {"verified_hnsw_index": False, "reason": "no queries"}
        storage = storage_usage(conn, args, manifest["docs"])
        levels = sorted({1, *concurrency})
        budget_searches = []
        for budget in budgets:
            args.ef_search = budget
            configure_search_session(conn, args)
            validation = validate_recall(conn, args, query_literals, truth, args.top_k, args.validate_queries, args.min_recall)
            search_benchmarks = []
            for level in levels:
                if args.warmup:
                    benchmark_search(args, query_literals[: args.warmup], level)
                search_benchmarks.append(benchmark_search(args, query_literals, level))
            budget_searches.append({"ef_search": budget, "validation": validation, "search_benchmarks": search_benchmarks})
        conn.close()
        first = budget_searches[0]

        result = {
            "backend": "pgvector",
            "engine": "pgvector",
            "engine_info": postgres_info,
            "ann_index": "hnsw",
            "dataset_dir": str(dataset_dir),
            "schema": args.schema,
            "table": args.table,
            "index": index_name(args),
            "docs": manifest["docs"],
            "dimensions": manifest["dimensions"],
            "queries": len(queries),
            "top_k": args.top_k,
            "m": args.m,
            "ef_construction": args.ef_construction,
            "ef_search": first["ef_search"],
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
            "storage_after_build": storage,
            "memory": {"max_rss_bytes": max_rss_bytes()},
        }
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, indent=2))
    finally:
        if args.drop_schema_after and args.created_schema:
            drop_schema(args)


if __name__ == "__main__":
    main()
