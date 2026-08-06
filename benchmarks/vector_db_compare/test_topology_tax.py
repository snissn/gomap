#!/usr/bin/env python3

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from system_qualification import ContractError
from topology_tax import summarize


class TopologyTaxTest(unittest.TestCase):
    def run_value(self, topology: str) -> dict:
        cells = []
        for probes in (2, 16):
            for concurrency in (1, 8):
                samples = [1, 2, 3] + [3] * 997
                cells.append({
                    "status": "valid", "budget": {"probes": probes}, "concurrency": concurrency,
                    "generation": {"Index": "embedding_graph", "Generation": 7},
                    "metrics": {"queries": 1000, "completed_queries": 1000, "result_count": 10000, "errors": 0, "timeouts": 0, "recall_at_10": .95, "qps": 1.0, "p50_nanos": 3, "p95_nanos": 3, "p99_nanos": 3},
                    "counters": {key: (0 if key in ("retries", "redirects") else 1) for key in ("selected_partitions", "selected_groups", "requests", "rpcs", "retries", "redirects", "candidates", "edges", "query_bytes", "request_bytes", "candidate_bytes", "response_bytes")},
                    "timings": {key: (0 if key in ("router_open", "placement", "queue", "network", "generation_open", "response", "dedupe", "merge") else 1) for key in ("router_open", "router_search", "placement", "queue", "rpc", "network", "read_index_apply", "generation_open", "shard_search", "response", "dedupe", "merge", "total")},
                    "elapsed_nanos": 1_000_000_000_000, "total_nanos": samples,
                })
        return {"schema_version": 1, "result_kind": "vector_partition_system_bench_v1", "topology": topology, "topology_identity_sha256": "a" * 64, "dataset_checksum": "b" * 64, "truth_artifact_sha256": "c" * 64, "top_k": 10, "ef_search": 128, "warmup_queries": 1000, "cells": cells}

    def files(self, root: Path) -> tuple[list[Path], list[Path]]:
        values: list[list[Path]] = [[], []]
        for topology_index, topology in enumerate(("single_daemon_four_group", "native_four_daemon_four_group")):
            for repetition in range(3):
                value = self.run_value(topology)
                path = root / f"{topology}-{repetition}.json"
                path.write_text(json.dumps(value), encoding="utf-8")
                values[topology_index].append(path)
        return values[0], values[1]

    def test_three_repetition_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = summarize(*self.files(Path(directory)))
        self.assertEqual(result["status"], "valid_baseline")
        self.assertEqual(len(result["inputs"]), 6)
        self.assertEqual(len(result["rows"]), 4)
        self.assertEqual(result["rows"][0]["native_over_single_qps"], 1)

    def test_changed_generation_or_percentile_rejects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            single, native = self.files(Path(directory))
            value = json.loads(native[2].read_text(encoding="utf-8"))
            value["cells"][1]["generation"]["Generation"] = 8
            native[2].write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "generation changed"):
                summarize(single, native)
        with tempfile.TemporaryDirectory() as directory:
            single, native = self.files(Path(directory))
            value = json.loads(native[2].read_text(encoding="utf-8"))
            value["cells"][0]["metrics"]["p95_nanos"] = 4
            native[2].write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "percentiles changed"):
                summarize(single, native)


if __name__ == "__main__":
    unittest.main()
