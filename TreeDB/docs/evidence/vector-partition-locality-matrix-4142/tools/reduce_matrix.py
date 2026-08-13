#!/usr/bin/env python3
"""Fail-closed reducer for #4142 locality-matrix rows.

This deliberately reduces only rows emitted by the M0 runner.  It does not
pretend that historical qualification assets are evidence for a new head.
"""

from __future__ import annotations

import hashlib
import json
import math
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


CAMPAIGN_IDENTITIES = ("source_head", "binary_sha256", "dataset_sha256", "truth_sha256", "graph_sha256", "query_union_sha256")


AUTHORIZED_COORDINATES = frozenset(
    (layout, partitions, overlap, probes, ef, split)
    for layout in ("source-order", "entry-first-bfs")
    for partitions in (16, 32, 40)
    for overlap in ("exact-20%", "useful-only-20%-cap", "zero")
    for probes in (1, 2, 4)
    for ef in (64, 80, 96, 128, 256)
    for split in ("train", "holdout")
)


def coordinate(row: dict[str, Any]) -> tuple[Any, ...]:
    return (row["layout"], row["partitions"], row["overlap"], row["probes"], row["ef"], row["split"])


def validate_row(row: dict[str, Any], expected: dict[str, Any]) -> None:
    require(set(row) == REQUIRED, "row fields are not exact")
    require(row["schema_version"] == 1 and row["result_kind"] == "vector_partition_locality_matrix_row_v1", "row schema is invalid")
    require(row["terminal"] is True and isinstance(row["row_id"], str) and row["row_id"], "row is nonterminal or unnamed")
    for field in ("source_head", "binary_sha256", "dataset_sha256", "truth_sha256", "graph_sha256", "membership_sha256", "router_sha256", "query_union_sha256"):
        value = row[field]
        require(isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value), f"{field} is not sha256")
        if field in CAMPAIGN_IDENTITIES:
            require(value == expected[field], f"mixed identity: {field}")
    require(row["layout"] in ("source-order", "entry-first-bfs"), "layout is invalid")
    require(row["partitions"] in (16, 32, 40), "partition count is invalid")
    require(row["overlap"] in ("exact-20%", "useful-only-20%-cap", "zero"), "overlap is invalid")
    require(row["probes"] in (1, 2, 4) and row["ef"] in (64, 80, 96, 128, 256), "search budget is invalid")
    require(row["split"] in ("train", "holdout"), "split is invalid")
    metrics = row["metrics"]
    require(isinstance(metrics, dict) and isinstance(metrics.get("filler_replicas"), int) and not isinstance(metrics.get("filler_replicas"), bool) and isinstance(metrics.get("unique_pages_per_query"), (int, float)) and not isinstance(metrics.get("unique_pages_per_query"), bool), "metrics are incomplete")
    for name, value in metrics.items():
        require(isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0, f"invalid metric: {name}")
    if row["overlap"] in ("zero", "useful-only-20%-cap"):
        require(metrics["filler_replicas"] == 0, "selected useful/zero overlap has filler")


def reduce_rows(paths: list[Path]) -> dict[str, Any]:
    require(paths, "matrix has no rows")
    rows = [load(path) for path in paths]
    first = rows[0]
    expected = {field: first[field] for field in CAMPAIGN_IDENTITIES}
    seen: set[str] = set()
    coordinates: set[tuple[Any, ...]] = set()
    for row in rows:
        validate_row(row, expected)
        require(row["row_id"] not in seen, "duplicate row")
        seen.add(row["row_id"])
        point = coordinate(row)
        require(point in AUTHORIZED_COORDINATES, "unauthorized coordinate")
        require(point not in coordinates, "duplicate coordinate")
        coordinates.add(point)
    require([row["row_id"] for row in rows] == sorted(seen), "rows are reordered")
    require(coordinates == AUTHORIZED_COORDINATES, "incomplete matrix")
    return {"schema_version": 1, "result_kind": "vector_partition_locality_matrix_summary_v1", "identity": expected, "rows": len(rows)}
