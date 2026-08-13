#!/usr/bin/env python3
from __future__ import annotations

import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True

import reduce_matrix as reducer


IDENTITY = "a" * 64


def row(row_id: str) -> dict:
    return {
        "schema_version": 1, "result_kind": "vector_partition_locality_matrix_row_v1", "row_id": row_id, "terminal": True,
        "source_head": IDENTITY, "binary_sha256": IDENTITY, "dataset_sha256": IDENTITY, "truth_sha256": IDENTITY,
        "graph_sha256": IDENTITY, "membership_sha256": IDENTITY, "router_sha256": IDENTITY, "query_union_sha256": IDENTITY,
        "layout": "entry-first-bfs", "partitions": 16, "overlap": "zero", "probes": 2, "ef": 96, "split": "holdout",
        "metrics": {"filler_replicas": 0, "unique_pages_per_query": 1.0},
    }


def complete_rows() -> list[dict]:
    rows = []
    for index, point in enumerate(sorted(reducer.AUTHORIZED_COORDINATES)):
        value = row(f"{index:04d}")
        value["layout"], value["partitions"], value["overlap"], value["probes"], value["ef"], value["split"] = point
        if value["overlap"] == "exact-20%":
            value["metrics"]["filler_replicas"] = 1
        rows.append(value)
    return rows


class ReducerTest(unittest.TestCase):
    def test_row_identity_and_accounting_are_required(self) -> None:
        value = row("a")
        reducer.validate_row(value, {key: IDENTITY for key in reducer.REQUIRED if key.endswith("sha256") or key == "source_head"})
        for mutate, message in ((lambda x: x.update(source_head="b" * 64), "mixed identity"), (lambda x: x.update(terminal=False), "nonterminal"), (lambda x: x["metrics"].update(filler_replicas=1), "filler")):
            changed = copy.deepcopy(value)
            mutate(changed)
            with self.assertRaisesRegex(reducer.ContractError, message):
                reducer.validate_row(changed, {key: IDENTITY for key in reducer.REQUIRED if key.endswith("sha256") or key == "source_head"})

    def test_matrix_rejects_duplicate_reordered_mixed_and_incomplete_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def write(name: str, value: dict) -> Path:
                path = root / name
                path.write_text(json.dumps(value), encoding="utf-8")
                return path
            paths = [write(f"{value['row_id']}.json", value) for value in complete_rows()]
            self.assertEqual(reducer.reduce_rows(paths)["rows"], len(reducer.AUTHORIZED_COORDINATES))
            with self.assertRaisesRegex(reducer.ContractError, "incomplete"):
                reducer.reduce_rows(paths[:-1])
            with self.assertRaisesRegex(reducer.ContractError, "duplicate"):
                reducer.reduce_rows(paths + [paths[0]])
            with self.assertRaisesRegex(reducer.ContractError, "reordered"):
                reducer.reduce_rows([paths[1], paths[0]] + paths[2:])
            mixed = complete_rows()[-1]
            mixed["dataset_sha256"] = "b" * 64
            with self.assertRaisesRegex(reducer.ContractError, "mixed identity"):
                reducer.reduce_rows(paths[:-1] + [write("mixed.json", mixed)])

    def test_variant_identities_and_exact_baseline_filler_are_permitted(self) -> None:
        value = row("exact")
        value["overlap"] = "exact-20%"
        value["metrics"]["filler_replicas"] = 3
        value["membership_sha256"] = "b" * 64
        value["router_sha256"] = "c" * 64
        reducer.validate_row(value, {key: IDENTITY for key in reducer.CAMPAIGN_IDENTITIES})

    def test_rejects_invalid_numeric_metrics(self) -> None:
        for value in (True, -1, float("nan"), float("inf")):
            changed = row("metric")
            changed["metrics"]["unique_pages_per_query"] = value
            with self.assertRaisesRegex(reducer.ContractError, "metric"):
                reducer.validate_row(changed, {key: IDENTITY for key in reducer.CAMPAIGN_IDENTITIES})


if __name__ == "__main__":
    unittest.main()
