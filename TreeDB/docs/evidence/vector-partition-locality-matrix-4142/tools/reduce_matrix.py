#!/usr/bin/env python3
"""Fail-closed reducer for #4142 locality-matrix rows.

This deliberately reduces only rows emitted by the M0 runner.  It does not
pretend that historical qualification assets are evidence for a new head.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import preflight_matrix as preflight_contract


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
    "membership_sha256", "router_sha256", "query_union_sha256",
    "query_split_sha256", "layout", "partitions", "overlap", "probes",
    "ef", "split", "metrics",
}


CAMPAIGN_IDENTITIES = ("source_head", "binary_sha256", "dataset_sha256", "truth_sha256", "graph_sha256", "query_union_sha256")

PREFLIGHT_FIELDS = {
    "schema_version", "result_kind", "source_head", "binary_sha256",
    "frozen_head", "campaign_sha256", "descriptor_sha256", "descriptor_head",
    "dataset_sha256", "truth_sha256", "graph_sha256", "calibration_sha256",
    "holdout_sha256", "query_union_sha256", "binary_vcs_revision",
    "binary_vcs_modified", "topology_contract_sha256", "status",
}


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
    require(isinstance(row["source_head"], str) and len(row["source_head"]) == 40 and all(c in "0123456789abcdef" for c in row["source_head"]), "source_head is not a git revision")
    for field in ("binary_sha256", "dataset_sha256", "truth_sha256", "graph_sha256", "membership_sha256", "router_sha256", "query_union_sha256", "query_split_sha256"):
        value = row[field]
        require(isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value), f"{field} is not sha256")
    for field in CAMPAIGN_IDENTITIES:
        require(row[field] == expected[field], f"mixed identity: {field}")
    require(row["layout"] in ("source-order", "entry-first-bfs"), "layout is invalid")
    require(row["partitions"] in (16, 32, 40), "partition count is invalid")
    require(row["overlap"] in ("exact-20%", "useful-only-20%-cap", "zero"), "overlap is invalid")
    require(row["probes"] in (1, 2, 4) and row["ef"] in (64, 80, 96, 128, 256), "search budget is invalid")
    require(row["split"] in ("train", "holdout"), "split is invalid")
    split_field = "calibration_sha256" if row["split"] == "train" else "holdout_sha256"
    require(row["query_split_sha256"] == expected[split_field], "mixed identity: query split")
    metrics = row["metrics"]
    require(isinstance(metrics, dict) and isinstance(metrics.get("queries"), int) and not isinstance(metrics.get("queries"), bool) and isinstance(metrics.get("filler_replicas"), int) and not isinstance(metrics.get("filler_replicas"), bool) and isinstance(metrics.get("unique_pages_per_query"), (int, float)) and not isinstance(metrics.get("unique_pages_per_query"), bool), "metrics are incomplete")
    for name, value in metrics.items():
        require(isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0, f"invalid metric: {name}")
    required_queries = 806 if row["split"] == "train" else 194
    require(metrics["queries"] == required_queries, "row does not cover the full frozen split")
    require(metrics["unique_pages_per_query"] > 0, "terminal row has no measured pages")
    if row["overlap"] in ("zero", "useful-only-20%-cap"):
        require(metrics["filler_replicas"] == 0, "selected useful/zero overlap has filler")


def validate_preflight(path: Path) -> dict[str, Any]:
    value = load(path)
    require(set(value) == PREFLIGHT_FIELDS, "preflight fields are not exact")
    require(
        value["schema_version"] == 1
        and value["result_kind"] == "vector_partition_locality_matrix_preflight_v1"
        and value["status"] == "ready"
        and value["source_head"] == preflight_contract.MEASURED_SOURCE_HEAD
        and value["binary_vcs_revision"] == value["source_head"]
        and value["binary_vcs_modified"] == "false"
        and value["frozen_head"] == preflight_contract.FROZEN_INPUT_HEAD
        and value["descriptor_head"] == preflight_contract.FROZEN_INPUT_HEAD
        and value["campaign_sha256"] == preflight_contract.FROZEN_CAMPAIGN_SHA256
        and value["descriptor_sha256"] == preflight_contract.FROZEN_DESCRIPTOR_SHA256,
        "preflight is not ready or pinned",
    )
    require(isinstance(value["binary_sha256"], str) and len(value["binary_sha256"]) == 64 and all(c in "0123456789abcdef" for c in value["binary_sha256"]), "preflight binary_sha256 is invalid")
    for field in ("dataset_sha256", "truth_sha256", "graph_sha256", "calibration_sha256", "holdout_sha256", "query_union_sha256"):
        require(value[field] == getattr(preflight_contract, f"FROZEN_{field.upper()}"), f"preflight {field} is not pinned")
    require(isinstance(value["topology_contract_sha256"], str) and len(value["topology_contract_sha256"]) == 64 and all(char in "0123456789abcdef" for char in value["topology_contract_sha256"]), "preflight topology contract is invalid")
    require(preflight_contract.topology_contract_is_approved(value["topology_contract_sha256"]), "preflight topology contract is not approved")
    return value


def reduce_rows(paths: list[Path], preflight_path: Path, topology_contract_path: Path) -> dict[str, Any]:
    require(paths, "matrix has no rows")
    preflight = validate_preflight(preflight_path)
    require(sha256(topology_contract_path) == preflight["topology_contract_sha256"], "topology contract does not match preflight")
    try:
        topology_contract = preflight_contract.topology_identities(load(topology_contract_path))
    except SystemExit as error:
        raise ContractError("topology contract is invalid") from error
    rows = [load(path) for path in paths]
    expected = {field: preflight[field] for field in (*CAMPAIGN_IDENTITIES, "calibration_sha256", "holdout_sha256")}
    seen: set[str] = set()
    coordinates: set[tuple[Any, ...]] = set()
    topology_identities: dict[tuple[Any, ...], tuple[str, str]] = {}
    for row in rows:
        validate_row(row, expected)
        require(row["row_id"] not in seen, "duplicate row")
        seen.add(row["row_id"])
        point = coordinate(row)
        require(point in AUTHORIZED_COORDINATES, "unauthorized coordinate")
        require(point not in coordinates, "duplicate coordinate")
        coordinates.add(point)
        topology = (row["layout"], row["partitions"], row["overlap"])
        identity = (row["membership_sha256"], row["router_sha256"])
        require(identity == topology_contract[topology], "topology identity does not match pinned contract")
        require(topology_identities.setdefault(topology, identity) == identity, "mixed topology identity")
    require([row["row_id"] for row in rows] == sorted(seen), "rows are reordered")
    require(coordinates == AUTHORIZED_COORDINATES, "incomplete matrix")
    return {"schema_version": 1, "result_kind": "vector_partition_locality_matrix_summary_v1", "identity": expected, "rows": len(rows)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight", type=Path, required=True)
    parser.add_argument("--topology-contract", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("rows", type=Path, nargs="+")
    args = parser.parse_args()
    summary = reduce_rows(args.rows, args.preflight, args.topology_contract)
    preflight_contract.write_json_exclusive(args.out, summary)


if __name__ == "__main__":
    main()
