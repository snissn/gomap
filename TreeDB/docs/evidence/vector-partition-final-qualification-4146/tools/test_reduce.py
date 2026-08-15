#!/usr/bin/env python3

import json
from pathlib import Path
import tempfile
import unittest

from reduce import CONCURRENCY, CORPORA, CORPUS_IDENTITIES, EFS, reduce_matrix


class ReduceTest(unittest.TestCase):
    def test_complete_matrix_and_missing_row(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for corpus in CORPORA:
                for repetition in (1, 2, 3):
                    run = root / corpus / f"repeat-{repetition}"
                    (run / "state").mkdir(parents=True, exist_ok=True)
                    (run / "state/ready.json").write_text(json.dumps({
                        "result_kind": "vector_partition_system_node_ready_v1", "vcs_modified": False,
                        "source_revision": "a" * 40, "executable_sha256": "b" * 64}))
                    (run / "topology.json").write_text(json.dumps({
                        "result_kind": "vector_partition_system_topology_v1",
                        "topology_identity_sha256": "c" * 64}))
                    for ef in EFS:
                        path = root / corpus / f"repeat-{repetition}" / f"search-ef{ef}.json"
                        path.parent.mkdir(parents=True, exist_ok=True)
                        cells = []
                        for concurrency in CONCURRENCY:
                            cells.append({"status": "valid", "budget": {"probes": 2}, "concurrency": concurrency,
                                "generation": {"Index": "embedding_graph", "Generation": 1},
                                "metrics": {"queries": 1000, "completed_queries": 1000, "errors": 0, "timeouts": 0,
                                    "recall_at_10": .95, "qps": 10000 / concurrency, "p50_nanos": 1,
                                    "p95_nanos": 2, "p99_nanos": 3},
                                "counters": {"retries": 0, "redirects": 0, "candidates": 4, "edges": 5,
                                    "requests": 6, "selected_partitions": 7, "selected_groups": 8}})
                        path.write_text(json.dumps({"schema_version": 1,
                            "result_kind": "vector_partition_system_bench_v1", "repetition": repetition,
                            "dataset_checksum": CORPUS_IDENTITIES[corpus][0],
                            "truth_artifact_sha256": CORPUS_IDENTITIES[corpus][1], "top_k": 10, "ef_search": ef,
                            "warmup_queries": 1000, "search_mode": "strict",
                            "topology_identity_sha256": "c" * 64, "cells": cells}))
            self.assertTrue(reduce_matrix(root)["gates"]["recall_at_10_gte_0_9500"])
            target = root / "250k/repeat-3/search-ef128.json"
            valid = target.read_text()
            ready_path = root / "250k/repeat-3/state/ready.json"
            ready = ready_path.read_text()
            bad = json.loads(ready)
            bad["source_revision"] = "d" * 40
            ready_path.write_text(json.dumps(bad))
            with self.assertRaises(ValueError):
                reduce_matrix(root)
            ready_path.write_text(ready)
            bad = json.loads(valid)
            bad["dataset_checksum"] = CORPUS_IDENTITIES["100k"][0]
            target.write_text(json.dumps(bad))
            with self.assertRaises(ValueError):
                reduce_matrix(root)
            bad = json.loads(valid)
            bad["topology_identity_sha256"] = "d" * 64
            target.write_text(json.dumps(bad))
            with self.assertRaises(ValueError):
                reduce_matrix(root)
            bad = json.loads(valid)
            bad["cells"][0]["generation"]["Generation"] = 2
            target.write_text(json.dumps(bad))
            with self.assertRaises(ValueError):
                reduce_matrix(root)
            for metric in ("recall_at_10", "qps", "p50_nanos", "p95_nanos", "p99_nanos"):
                for invalid in (float("inf"), float("-inf")):
                    bad = json.loads(valid)
                    bad["cells"][0]["metrics"][metric] = invalid
                    target.write_text(json.dumps(bad))
                    with self.assertRaises(ValueError):
                        reduce_matrix(root)
            for metric, invalid in (("recall_at_10", 1.1), ("qps", -1),
                                    ("p50_nanos", -1), ("p95_nanos", -1), ("p99_nanos", -1)):
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
            target.write_bytes((root / "250k/repeat-2/search-ef128.json").read_bytes())
            with self.assertRaises(ValueError):
                reduce_matrix(root)
            target.write_text(valid)
            target.unlink()
            with self.assertRaises(FileNotFoundError):
                reduce_matrix(root)


if __name__ == "__main__":
    unittest.main()
