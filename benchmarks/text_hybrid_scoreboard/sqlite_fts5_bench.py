#!/usr/bin/env python3
"""SQLite FTS5 text baseline for the TreeDB text/hybrid scoreboard.

The script intentionally uses only Python's stdlib sqlite3 module. It writes a
small JSON artifact that cmd/treedb_text_hybrid_scoreboard can ingest. If the
local sqlite3 build lacks FTS5 support, it writes an explicit unavailable JSON
instead of fabricating a row.
"""

from __future__ import annotations

import argparse
import json
import platform
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a deterministic SQLite FTS5 text baseline")
    parser.add_argument("--docs", type=int, default=10_000)
    parser.add_argument("--queries", type=int, default=1_000)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--query", default="refund policy")
    parser.add_argument("--out", required=True, help="JSON output path")
    parser.add_argument("--db", default="", help="SQLite DB path; defaults to a file beside --out")
    parser.add_argument("--keep-db", action="store_true")
    parser.add_argument("--tokenize", default="unicode61")
    return parser.parse_args()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def unavailable(args: argparse.Namespace, reason: str) -> dict[str, Any]:
    return {
        "schema_version": "treedb_text_hybrid_external/v1",
        "status": "unavailable",
        "system": "SQLite FTS5",
        "engine": "sqlite_fts5",
        "unavailable_reason": reason,
        "command": " ".join(sys.argv),
        "versions": versions(),
    }


def versions() -> dict[str, str]:
    return {
        "python": sys.version.replace("\n", " "),
        "sqlite": sqlite3.sqlite_version,
        "platform": platform.platform(),
    }


def configure(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-200000")


def fixture_row(i: int) -> tuple[str, str]:
    if i % 2 == 0:
        title = "refund policy"
        body = f"refund policy customer credit shard {i % 17} incident {i % 31}"
    else:
        title = "shipping status"
        body = f"shipping status update parcel route shard {i % 17} customer {i % 31}"
    return title, body


def storage_bytes(db_path: Path) -> int:
    total = 0
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(db_path) + suffix)
        if path.exists():
            total += path.stat().st_size
    return total


def percentile(sorted_values: list[int], pct: float) -> int:
    if not sorted_values:
        return 0
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (len(sorted_values) - 1) * pct / 100.0
    lo = int(rank)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = rank - lo
    return int(sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac)


def main() -> int:
    args = parse_args()
    out = Path(args.out)
    if args.docs <= 0 or args.queries <= 0 or args.top_k <= 0:
        raise SystemExit("--docs, --queries, and --top-k must be positive")

    db_path = Path(args.db) if args.db else out.with_suffix(".sqlite3")
    out.parent.mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    for suffix in ("-wal", "-shm"):
        p = Path(str(db_path) + suffix)
        if p.exists():
            p.unlink()

    try:
        conn = sqlite3.connect(str(db_path))
        configure(conn)
        conn.execute(f"CREATE VIRTUAL TABLE docs_fts USING fts5(title, body, tokenize='{args.tokenize}')")
    except sqlite3.Error as exc:
        write_json(out, unavailable(args, f"Python sqlite3/SQLite does not provide usable FTS5: {exc}"))
        return 0

    build_start = time.perf_counter()
    with conn:
        conn.executemany("INSERT INTO docs_fts(rowid, title, body) VALUES (?, ?, ?)", ((i + 1, *fixture_row(i)) for i in range(args.docs)))
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    build_seconds = time.perf_counter() - build_start

    sql = "SELECT rowid, bm25(docs_fts, 3.0, 1.0) AS score FROM docs_fts WHERE docs_fts MATCH ? ORDER BY score LIMIT ?"
    # Warm outside the timed loop and assert that the query shape has hits.
    warm = list(conn.execute(sql, (args.query, args.top_k)))
    if not warm:
        raise SystemExit(f"query {args.query!r} returned no rows")

    durations: list[int] = []
    total_results = 0
    search_start = time.perf_counter_ns()
    for i in range(args.queries):
        start = time.perf_counter_ns()
        rows = list(conn.execute(sql, (args.query, args.top_k)))
        durations.append(time.perf_counter_ns() - start)
        total_results += len(rows)
    total_nanos = time.perf_counter_ns() - search_start
    sorted_durations = sorted(durations)
    avg_nanos = total_nanos / args.queries
    ops_per_second = 1_000_000_000 / avg_nanos if avg_nanos > 0 else 0

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    total_storage = storage_bytes(db_path)
    if not args.keep_db:
        for suffix in ("", "-wal", "-shm"):
            p = Path(str(db_path) + suffix)
            try:
                p.unlink()
            except FileNotFoundError:
                pass

    payload = {
        "schema_version": "treedb_text_hybrid_external/v1",
        "status": "ok",
        "system": "SQLite FTS5",
        "engine": "sqlite_fts5",
        "modality": "text_only",
        "dataset": {
            "docs": args.docs,
            "queries": args.queries,
            "top_k": args.top_k,
            "fields": 2,
        },
        "query_set": "synthetic refund/shipping corpus",
        "query_shape": f"FTS5 MATCH {args.query!r} + bm25 top-{args.top_k}",
        "boundary": "no-document rowid+bm25 retrieval only; no primary JSON document fetch",
        "benchmark": "sqlite_fts5/search_rowid_bm25_no_docs",
        "command": " ".join(sys.argv),
        "versions": versions(),
        "build": {"seconds": build_seconds},
        "storage": {"total_bytes": total_storage, "bytes_per_doc": total_storage / args.docs},
        "search": {
            "queries": args.queries,
            "avg_nanos": avg_nanos,
            "ops_per_second": ops_per_second,
            "p50_nanos": percentile(sorted_durations, 50),
            "p95_nanos": percentile(sorted_durations, 95),
            "p99_nanos": percentile(sorted_durations, 99),
        },
        "metrics": {
            "docs_fetched/search": 0,
            "full_doc_fallbacks/search": 0,
            "fail_closed/search": 0,
            "results/search": total_results / args.queries,
        },
        "caveats": [
            "SQLite FTS5 is an embedded text baseline only; it is not a vector or hybrid-search engine.",
            "Storage includes the SQLite FTS table file after WAL checkpoint/truncate.",
        ],
    }
    write_json(out, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
