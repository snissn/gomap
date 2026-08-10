#!/usr/bin/env python3

import unittest

from strict_proof import _proof_projection
from system_qualification import ContractError


class StrictProofTest(unittest.TestCase):
    def test_projection_rejects_duplicate_or_log_appending_proof(self) -> None:
        def stage(reads: int) -> dict[str, int]:
            return {
                "reads": reads, "successes": reads, "failures": 0, "verify_leader_calls": reads,
                "log_barriers": 0, "no_log_proofs": reads,
            }

        total = {name: stage(0) for name in ("operations_health", "coordinator_lifecycle", "shard_lifecycle", "unknown")}
        total.update({"total": stage(1002), "strict_search": stage(1000), "serving_refresh": stage(2)})
        for value in total.values():
            value["total_nanos"] = value["reads"] * 10
        self.assertEqual(_proof_projection({"catalog_reads": {"total": total}})["strict_search.reads"], 1000)
        total["coordinator_lifecycle"] = stage(1)
        total["coordinator_lifecycle"]["total_nanos"] = 10
        with self.assertRaisesRegex(ContractError, "totals"):
            _proof_projection({"catalog_reads": {"total": total}})
        total["total"] = stage(1003)
        total["total"]["total_nanos"] = 10030
        with self.assertRaisesRegex(ContractError, "duplicate"):
            _proof_projection({"catalog_reads": {"total": total}})


if __name__ == "__main__":
    unittest.main()
