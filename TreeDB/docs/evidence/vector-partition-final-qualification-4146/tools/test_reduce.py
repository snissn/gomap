#!/usr/bin/env python3

import json
from pathlib import Path
import tempfile
import unittest

from reduce import CONCURRENCY, CORPORA, EFS, reduce_matrix


class ReduceTest(unittest.TestCase):
    def test_complete_matrix_and_missing_row(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for corpus in CORPORA:
                for repetition in (1, 2, 3):
                    for ef in EFS:
                        path = root / corpus / f"repeat-{repetition}" / f"search-ef{ef}.json"
                        path.parent.mkdir(parents=True, exist_ok=True)
                        cells = []
                        for concurrency in CONCURRENCY:
                            cells.append({"status": "valid", "budget": {"probes": 2}, "concurrency": concurrency,
                                "metrics": {"queries": 1000, "completed_queries": 1000, "errors": 0, "timeouts": 0,
                                    "recall_at_10": .95, "qps": 10000 / concurrency, "p50_nanos": 1,
                                    "p95_nanos": 2, "p99_nanos": 3},
                                "counters": {"retries": 0, "redirects": 0, "candidates": 4, "edges": 5,
                                    "requests": 6, "selected_partitions": 7, "selected_groups": 8}})
                        path.write_text(json.dumps({"schema_version": 1,
                            "result_kind": "vector_partition_system_bench_v1", "dataset_checksum": corpus,
                            "truth_artifact_sha256": corpus + "-truth", "top_k": 10, "ef_search": ef,
                            "warmup_queries": 1000, "search_mode": "strict", "cells": cells}))
            self.assertTrue(reduce_matrix(root)["gates"]["recall_at_10_gte_0_9500"])
            target = root / "250k/repeat-3/search-ef128.json"
            valid = target.read_text()
            for metric in ("recall_at_10", "qps", "p50_nanos", "p95_nanos", "p99_nanos"):
                for invalid in (float("inf"), float("-inf")):
                    bad = json.loads(valid)
                    bad["cells"][0]["metrics"][metric] = invalid
                    target.write_text(json.dumps(bad))
                    with self.assertRaises(ValueError):
                        reduce_matrix(root)
            duplicate = json.loads(valid)
            duplicate["cells"].append(duplicate["cells"][0])
            target.write_text(json.dumps(duplicate))
            with self.assertRaises(ValueError):
                reduce_matrix(root)
            target.write_text(valid)
            target.unlink()
            with self.assertRaises(FileNotFoundError):
                reduce_matrix(root)


if __name__ == "__main__":
    unittest.main()
