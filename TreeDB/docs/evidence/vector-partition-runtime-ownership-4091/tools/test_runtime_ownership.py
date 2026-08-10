import json
from pathlib import Path
import tempfile
import unittest

import run_runtime_ownership as runner


class RuntimeOwnershipRunnerTest(unittest.TestCase):
    def test_ready_binds_effective_ownership(self) -> None:
        provenance = {"source_head": "head", "binary_sha256": "binary"}
        ready = {
            "source_revision": "head", "vcs_modified": False, "executable_sha256": "binary",
            "production_topology": True, "m8_loopback": False,
            "runtime_ownership": {"cpu_set": "0-2", "gomaxprocs": 3, "go_memory_limit_bytes": runner.MEMORY_BYTES},
            "effective_cpu_set": "0-2", "gomaxprocs": 3, "go_memory_limit": runner.MEMORY_BYTES,
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ready.json"
            path.write_text(json.dumps(ready), encoding="utf-8")
            runner.validate_ready(path, provenance, "0-2")
            ready["effective_cpu_set"] = "0-3"
            path.write_text(json.dumps(ready), encoding="utf-8")
            with self.assertRaises(RuntimeError):
                runner.validate_ready(path, provenance, "0-2")

    def test_result_requires_two_correct_cells(self) -> None:
        cell = lambda concurrency: {
            "status": "valid", "concurrency": concurrency, "budget": {"probes": 2},
            "metrics": {"completed_queries": 1000, "errors": 0, "timeouts": 0, "recall_at_10": .9247},
        }
        value = {"cells": [cell(1), cell(32)]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "search.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            runner.validate_result(path)
            value["cells"][1]["metrics"]["recall_at_10"] = .89
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaises(RuntimeError):
                runner.validate_result(path)


if __name__ == "__main__":
    unittest.main()
