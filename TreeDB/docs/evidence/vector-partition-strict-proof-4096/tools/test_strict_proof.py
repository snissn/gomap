import json
from pathlib import Path
import tempfile
import unittest

import run_strict_proof as runner


class StrictProofRunnerTest(unittest.TestCase):
    def test_result_requires_one_ingress_proof_and_no_request_opens(self) -> None:
        def stage(reads: int) -> dict[str, int]:
            return {"reads": reads, "successes": reads, "failures": 0, "verify_leader_calls": reads, "no_log_proofs": reads, "log_barriers": 0}

        def cell(concurrency: int) -> dict[str, object]:
            return {
                "status": "valid", "concurrency": concurrency, "budget": {"probes": 2},
                "metrics": {"completed_queries": 1000, "errors": 0, "timeouts": 0, "recall_at_10": .9247},
                "counters": {"snapshot_pins": 1000, "read_proofs": 0, "generation_pins": 0, "partition_opens": 0},
                "catalog_reads": {"total": {"total": stage(1002), "strict_search": stage(1000), "serving_refresh": stage(2), "operations_health": stage(0), "coordinator_lifecycle": stage(0), "shard_lifecycle": stage(0), "unknown": stage(0)}},
            }

        value = {"cells": [cell(1), cell(32)]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "search.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            runner.validate_result(path)
            value["cells"][0]["counters"]["partition_opens"] = 1
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "request-side asset work"):
                runner.validate_result(path)


if __name__ == "__main__":
    unittest.main()
