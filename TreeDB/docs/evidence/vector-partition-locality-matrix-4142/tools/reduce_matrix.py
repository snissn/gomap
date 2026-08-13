#!/usr/bin/env python3
"""Fail-closed reducer for #4142 locality-matrix rows.

This deliberately reduces only rows emitted by the M0 runner.  It does not
pretend that historical qualification assets are evidence for a new head.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


class ContractError(ValueError):
    pass


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ContractError(message)


def load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(value, dict), "row is not an object")
    return value


REQUIRED = {
    "schema_version", "result_kind", "row_id", "terminal", "source_head",
    "binary_sha256", "dataset_sha256", "truth_sha256", "graph_sha256",
    "membership_sha256", "router_sha256", "query_union_sha256", "layout",
    "partitions", "overlap", "probes", "ef", "split", "metrics",
}


def validate_row(row: dict[str, Any], expected: dict[str, Any]) -> None:
    require(set(row) == REQUIRED, "row fields are not exact")
    require(row["schema_version"] == 1 and row["result_kind"] == "vector_partition_locality_matrix_row_v1", "row schema is invalid")
    require(row["terminal"] is True and isinstance(row["row_id"], str) and row["row_id"], "row is nonterminal or unnamed")
    for field in ("source_head", "binary_sha256", "dataset_sha256", "truth_sha256", "graph_sha256", "membership_sha256", "router_sha256", "query_union_sha256"):
        value = row[field]
        require(isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value), f"{field} is not sha256")
        require(value == expected[field], f"mixed identity: {field}")
    require(row["layout"] in ("source-order", "entry-first-bfs"), "layout is invalid")
    require(row["partitions"] in (16, 32, 40), "partition count is invalid")
    require(row["overlap"] in ("exact-20%", "useful-only-20%-cap", "zero"), "overlap is invalid")
    require(row["probes"] in (1, 2, 4) and row["ef"] in (64, 80, 96, 128, 256), "search budget is invalid")
    require(row["split"] in ("train", "holdout"), "split is invalid")
    metrics = row["metrics"]
    require(isinstance(metrics, dict) and metrics.get("filler_replicas") == 0 and isinstance(metrics.get("unique_pages_per_query"), (int, float)), "metrics are incomplete")


def reduce_rows(paths: list[Path]) -> dict[str, Any]:
    require(paths, "matrix has no rows")
    rows = [load(path) for path in paths]
    first = rows[0]
    expected = {field: first[field] for field in ("source_head", "binary_sha256", "dataset_sha256", "truth_sha256", "graph_sha256", "membership_sha256", "router_sha256", "query_union_sha256")}
    seen: set[str] = set()
    for row in rows:
        validate_row(row, expected)
        require(row["row_id"] not in seen, "duplicate row")
        seen.add(row["row_id"])
    require([row["row_id"] for row in rows] == sorted(seen), "rows are reordered")
    return {"schema_version": 1, "result_kind": "vector_partition_locality_matrix_summary_v1", "identity": expected, "rows": len(rows)}
