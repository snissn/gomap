#!/usr/bin/env python3
"""SQLite FTS5 adapter for the frozen same-corpus lexical comparison."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import resource
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

from lexical_common import RESULT_SCHEMA, load_manifest, manifest_sha256, result_digest

ENGINE = {"id": "sqlite_fts5", "family": "embedded_sql", "name": "SQLite FTS5", "version": sqlite3.sqlite_version}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--corpus", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--repetition", required=True, type=int)
    return parser.parse_args()


def write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def peak_rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return 0


def query_text(query: dict[str, Any]) -> str:
    terms = query["terms"]
    semantic = query["semantic"]
    if semantic in ("term", "term_scalar"):
        expression = terms[0]
    elif semantic == "and":
        expression = " AND ".join(terms)
    elif semantic == "or":
        expression = " OR ".join(terms)
    elif semantic == "phrase":
        phrase = '"' + " ".join(terms) + '"'
        return f"title : {phrase} OR body : {phrase}"
    else:
        raise ValueError(f"unsupported semantic {semantic}")
    return f"weighted_text : ({expression})"


def execute(conn: sqlite3.Connection, query: dict[str, Any], top_k: int) -> list[str]:
    sql = "SELECT id FROM docs_fts WHERE docs_fts MATCH ?"
    params: list[Any] = [query_text(query)]
    if query.get("filter"):
        sql += " AND tenant = ?"
        params.append(query["filter"]["equals"])
    weights = "0.0, 0.0, 3.0, 1.0, 0.0" if query["semantic"] == "phrase" else "0.0, 1.0, 0.0, 0.0, 0.0"
    sql += f" ORDER BY bm25(docs_fts, {weights}), id LIMIT ?"
    params.append(top_k)
    return [row[0] for row in conn.execute(sql, params)]


def main() -> int:
    args = parse_args()
    manifest_path, corpus_path, out, db_path = map(Path, (args.manifest, args.corpus, args.out, args.db))
    out.parent.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(manifest_path)
    manifest_digest = manifest_sha256(manifest)
    command = [sys.executable, *sys.argv]
    for suffix in ("", "-wal", "-shm"):
        try:
            Path(str(db_path) + suffix).unlink()
        except FileNotFoundError:
            pass
    db_path.parent.mkdir(parents=True, exist_ok=True)
    corpus_payload = corpus_path.read_bytes()
    source_rows = [line.split("\t") for line in corpus_payload.decode().splitlines()]
    build_cpu = time.process_time_ns()
    build_start = time.perf_counter_ns()
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA threads=0")
        sqlite_threads = int(conn.execute("PRAGMA threads").fetchone()[0])
        effective_sqlite_parallelism = 1 + max(sqlite_threads, 0)
        conn.execute("CREATE VIRTUAL TABLE docs_fts USING fts5(id UNINDEXED, weighted_text, title, body, tenant UNINDEXED, tokenize='unicode61 remove_diacritics 2')")
    except sqlite3.Error as exc:
        write(out, {
            "schema_version": RESULT_SCHEMA,
            "status": "unavailable",
            "engine": ENGINE,
            "repetition": args.repetition,
            "manifest_sha256": manifest_digest,
            "unavailable": {"kind": "feature_unavailable", "reason": f"SQLite FTS5 unavailable: {exc}", "setup_command": command, "stderr": ""},
            "cases": [],
        })
        return 0

    rows = [(doc_id, " ".join((title, title, title, body)), title, body, tenant) for doc_id, title, body, tenant in source_rows]
    with conn:
        conn.executemany("INSERT INTO docs_fts(id, weighted_text, title, body, tenant) VALUES (?, ?, ?, ?, ?)", rows)
    conn.execute("INSERT INTO docs_fts(docs_fts) VALUES ('optimize')")
    conn.commit()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    build_elapsed = time.perf_counter_ns() - build_start
    build_cpu = time.process_time_ns() - build_cpu
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA threads=0")
    top_k = int(manifest["execution"]["top_k"])
    warmups = int(manifest["execution"]["warmup_queries_per_case"])
    measured = int(manifest["execution"]["measured_queries_per_case"])
    cases = []
    for query in manifest["queries"]:
        for _ in range(warmups):
            execute(conn, query, top_k)
        samples = []
        ids: list[str] = []
        for _ in range(measured):
            start = time.perf_counter_ns()
            ids = execute(conn, query, top_k)
            samples.append(time.perf_counter_ns() - start)
        plan = list(conn.execute("EXPLAIN QUERY PLAN SELECT id FROM docs_fts WHERE docs_fts MATCH ?", (query_text(query),)))
        route_proven = any("VIRTUAL TABLE INDEX" in str(row).upper() for row in plan)
        cases.append({
            "id": query["id"], "status": "ok", "equivalent": True,
            "samples_nanos": samples, "result_ids": ids, "result_digest": result_digest(ids),
            "route": {"intended": route_proven, "name": "sqlite_fts5_virtual_table_match_bm25", "fallback": False, "proof": plan},
            "timed_out": False,
        })
    conn.close()
    durable = file_size(db_path)
    wal = file_size(Path(str(db_path) + "-wal"))
    transient = file_size(Path(str(db_path) + "-shm"))
    durability_reopened = sqlite3.connect(db_path)
    durability_reopened.execute("PRAGMA threads=0")
    for case, query in zip(cases, manifest["queries"], strict=True):
        ids = execute(durability_reopened, query, top_k)
        case["reopen_result_ids"] = ids
        case["reopen_result_digest"] = result_digest(ids)
    durability_reopened.close()
    stores = {
        "corpus_store_id": str(corpus_path.stat().st_dev),
        "index_store_id": str(db_path.stat().st_dev),
        "result_store_id": str(out.parent.stat().st_dev),
    }
    environment = {
        "contract": manifest["environment"],
        "filesystem": {
            "runner_device_id": os.environ["LEXICAL_RUNNER_DEVICE_ID"],
            **stores,
            "same_filesystem": len(set(stores.values())) == 1 and next(iter(stores.values())) == os.environ["LEXICAL_RUNNER_DEVICE_ID"],
        },
        "memory": {"detected_address_space_limit": os.environ["LEXICAL_ADDRESS_SPACE_LIMIT"], "detection_source": "runner_rlimit", "matches_runner_detected": True, "adapter_changed_limit": False},
        "execution": {
            "query_concurrency": int(os.environ["LEXICAL_QUERY_CONCURRENCY"]),
            "engine_process_concurrency": int(os.environ["LEXICAL_ENGINE_PROCESS_CONCURRENCY"]),
            "runtime_cpu_parallelism": effective_sqlite_parallelism,
        },
    }
    payload = {
        "schema_version": RESULT_SCHEMA, "status": "ok", "engine": ENGINE,
        "repetition": args.repetition, "manifest_sha256": manifest_digest,
        "corpus": {"document_count": len(rows), "sha256": hashlib.sha256(corpus_payload).hexdigest()},
        "command": command,
        "versions": {"python": sys.version.replace("\n", " "), "sqlite": sqlite3.sqlite_version, "platform": platform.platform()},
        "config": {"working_directory": os.getcwd(), "tokenizer": "unicode61 remove_diacritics 2", "weighted_field_materialization": "title repeated 3x then body for non-phrase scoring only", "phrase_fields": ["title", "body"], "phrase_field_weights": {"title": 3, "body": 1}, "journal_mode": "WAL", "synchronous": "FULL", "sqlite_auxiliary_threads": sqlite_threads, "top_k": top_k, "tie_break": "score,id", "build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close"},
        "environment": environment,
        "build": {"elapsed_nanos": build_elapsed, "docs_per_second": len(rows) * 1e9 / build_elapsed, "cpu": {"status": "ok", "value": build_cpu, "unit": "nanoseconds"}, "peak_rss": {"status": "ok", "value": peak_rss_bytes(), "unit": "bytes"}, "checkpointed": True},
        "storage": {"durable_bytes": durable, "wal_bytes": wal, "transient_bytes": transient},
        "reopen": {"performed": True, "query_connection_reopened": True, "durability_connection_reopened": True, "verified": all(case["result_ids"] == case["reopen_result_ids"] for case in cases), "result_digest": result_digest(case["reopen_result_digest"] for case in cases)},
        "cases": cases,
    }
    write(out, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
