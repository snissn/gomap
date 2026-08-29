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
    if query["semantic"] in ("term", "term_scalar"):
        return terms[0]
    if query["semantic"] == "and":
        return " AND ".join(terms)
    if query["semantic"] == "or":
        return " OR ".join(terms)
    if query["semantic"] == "phrase":
        return '"' + " ".join(terms) + '"'
    raise ValueError(f"unsupported semantic {query['semantic']}")


def execute(conn: sqlite3.Connection, query: dict[str, Any], top_k: int) -> list[str]:
    sql = "SELECT id FROM docs_fts WHERE docs_fts MATCH ?"
    params: list[Any] = [query_text(query)]
    if query.get("filter"):
        sql += " AND tenant = ?"
        params.append(query["filter"]["equals"])
    sql += " ORDER BY bm25(docs_fts, 0.0, 3.0, 1.0, 0.0), id LIMIT ?"
    params.append(top_k)
    return [row[0] for row in conn.execute(sql, params)]


def main() -> int:
    args = parse_args()
    manifest_path, corpus_path, out, db_path = map(Path, (args.manifest, args.corpus, args.out, args.db))
    manifest = load_manifest(manifest_path)
    manifest_digest = manifest_sha256(manifest)
    command = [sys.executable, *sys.argv]
    for suffix in ("", "-wal", "-shm"):
        try:
            Path(str(db_path) + suffix).unlink()
        except FileNotFoundError:
            pass
    db_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("CREATE VIRTUAL TABLE docs_fts USING fts5(id UNINDEXED, title, body, tenant UNINDEXED, tokenize='unicode61 remove_diacritics 2')")
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

    corpus_payload = corpus_path.read_bytes()
    rows = [line.split("\t") for line in corpus_payload.decode().splitlines()]
    build_cpu = time.process_time_ns()
    build_start = time.perf_counter_ns()
    with conn:
        conn.executemany("INSERT INTO docs_fts(id, title, body, tenant) VALUES (?, ?, ?, ?)", rows)
    conn.execute("INSERT INTO docs_fts(docs_fts) VALUES ('optimize')")
    conn.commit()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    build_elapsed = time.perf_counter_ns() - build_start
    build_cpu = time.process_time_ns() - build_cpu
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
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    durable = file_size(db_path)
    wal = file_size(Path(str(db_path) + "-wal"))
    transient = file_size(Path(str(db_path) + "-shm"))
    reopened = sqlite3.connect(db_path)
    for case, query in zip(cases, manifest["queries"], strict=True):
        ids = execute(reopened, query, top_k)
        case["reopen_result_ids"] = ids
        case["reopen_result_digest"] = result_digest(ids)
    reopened.close()
    payload = {
        "schema_version": RESULT_SCHEMA, "status": "ok", "engine": ENGINE,
        "repetition": args.repetition, "manifest_sha256": manifest_digest,
        "corpus": {"document_count": len(rows), "sha256": hashlib.sha256(corpus_payload).hexdigest()},
        "command": command,
        "versions": {"python": sys.version.replace("\n", " "), "sqlite": sqlite3.sqlite_version, "platform": platform.platform()},
        "config": {"working_directory": os.getcwd(), "tokenizer": "unicode61 remove_diacritics 2", "weights": {"title": 3.0, "body": 1.0}, "journal_mode": "WAL", "synchronous": "FULL", "top_k": top_k, "tie_break": "score,id"},
        "build": {"elapsed_nanos": build_elapsed, "docs_per_second": len(rows) * 1e9 / build_elapsed, "cpu_nanos": build_cpu, "peak_rss_bytes": peak_rss_bytes(), "checkpointed": True},
        "storage": {"durable_bytes": durable, "wal_bytes": wal, "transient_bytes": transient},
        "reopen": {"performed": True, "verified": all(case["result_ids"] == case["reopen_result_ids"] for case in cases), "result_digest": result_digest(case["reopen_result_digest"] for case in cases)},
        "cases": cases,
    }
    write(out, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
