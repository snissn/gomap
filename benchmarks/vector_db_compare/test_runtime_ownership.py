#!/usr/bin/env python3

import unittest

from system_qualification import ContractError
from runtime_ownership import MEMORY_BYTES, _proof_projection, _runtime_projection


class RuntimeOwnershipTest(unittest.TestCase):
    def test_runtime_projection_requires_effective_ownership(self) -> None:
        node = {"node_config_sha256": "a" * 64, "runtime_ownership": {"cpu_set": "0-2", "gomaxprocs": 3, "go_memory_limit_bytes": MEMORY_BYTES}}
        sample = {
            "sample_unix_nano": 1, "cpu_time_nanos": 1, "run_queue_delay_nanos": 0, "timeslices": 0,
            "voluntary_context_switches": 1, "nonvoluntary_context_switches": 1, "rss_bytes": 1, "peak_rss_bytes": 1,
            "heap_alloc_bytes": 1, "heap_objects": 1, "total_alloc_bytes": 1, "mallocs": 1, "frees": 1,
            "num_gc": 1, "pause_total_nanos": 1, "goroutines": 1, "logical_cpus": 12, "gomaxprocs": 3,
            "go_memory_limit_bytes": MEMORY_BYTES, "effective_cpu_set": "0-2",
        }
        after = {**sample, "sample_unix_nano": 2, "cpu_time_nanos": 3}
        cell = {"runtime": [{"node_config_sha256": "a" * 64, "before": sample, "after": after}]}
        self.assertEqual(_runtime_projection(cell, [node])["cpu_time_nanos"], 2)
        after["effective_cpu_set"] = "0-3"
        with self.assertRaisesRegex(ContractError, "declared ownership"):
            _runtime_projection(cell, [node])

    def test_proof_projection_rejects_log_barrier(self) -> None:
        def stage(reads: int) -> dict[str, int]:
            return {"reads": reads, "successes": reads, "failures": 0, "verify_leader_calls": reads, "log_barriers": 0, "no_log_proofs": reads}
        total = {
            "total": stage(3834), "operations_health": stage(1000), "coordinator_lifecycle": stage(1000),
            "shard_lifecycle": stage(1834), "unknown": stage(0),
        }
        self.assertEqual(_proof_projection({"catalog_reads": {"total": total}})["total.no_log_proofs"], 3834)
        total["total"]["log_barriers"] = 1
        with self.assertRaisesRegex(ContractError, "log-appending"):
            _proof_projection({"catalog_reads": {"total": total}})


if __name__ == "__main__":
    unittest.main()
