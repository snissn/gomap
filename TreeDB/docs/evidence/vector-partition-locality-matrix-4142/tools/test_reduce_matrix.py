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


class ReducerTest(unittest.TestCase):
    def test_row_identity_and_accounting_are_required(self) -> None:
        value = row("a")
        reducer.validate_row(value, {key: IDENTITY for key in reducer.REQUIRED if key.endswith("sha256") or key == "source_head"})
        for mutate, message in ((lambda x: x.update(source_head="b" * 64), "mixed identity"), (lambda x: x.update(terminal=False), "nonterminal"), (lambda x: x["metrics"].update(filler_replicas=1), "metrics")):
            changed = copy.deepcopy(value)
            mutate(changed)
            with self.assertRaisesRegex(reducer.ContractError, message):
                reducer.validate_row(changed, {key: IDENTITY for key in reducer.REQUIRED if key.endswith("sha256") or key == "source_head"})

    def test_matrix_rejects_duplicate_reordered_and_mixed_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def write(name: str, value: dict) -> Path:
                path = root / name
                path.write_text(json.dumps(value), encoding="utf-8")
                return path
            a, b = write("a.json", row("a")), write("b.json", row("b"))
            self.assertEqual(reducer.reduce_rows([a, b])["rows"], 2)
            with self.assertRaisesRegex(reducer.ContractError, "duplicate"):
                reducer.reduce_rows([a, a])
            with self.assertRaisesRegex(reducer.ContractError, "reordered"):
                reducer.reduce_rows([b, a])
            mixed = row("c")
            mixed["dataset_sha256"] = "b" * 64
            with self.assertRaisesRegex(reducer.ContractError, "mixed identity"):
                reducer.reduce_rows([a, write("c.json", mixed)])


if __name__ == "__main__":
    unittest.main()
