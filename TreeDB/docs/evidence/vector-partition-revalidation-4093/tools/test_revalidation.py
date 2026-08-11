#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


PATH = Path(__file__).with_name("reduce_revalidation.py")
SPEC = importlib.util.spec_from_file_location("reduce_revalidation", PATH)
assert SPEC is not None and SPEC.loader is not None
reduce = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(reduce)


def stage(reads: int = 0) -> dict[str, int]:
    return {
        "reads": reads, "successes": reads, "failures": 0, "verify_leader_calls": reads,
        "log_barriers": 0, "no_log_proofs": reads, "total_nanos": reads,
        "admission_nanos": 0, "verify_leader_nanos": reads, "barrier_nanos": 0,
        "current_term_nanos": 0, "raft_apply_nanos": 0, "applied_read_nanos": 0,
    }


def cell(mode: str = "strict", concurrency: int = 1) -> tuple[dict, dict]:
    samples = [1_000_000] * 1000
    strict = 1000 if mode == "strict" else 0
    total = stage(strict)
    catalog = {
        "total": total, "strict_search": stage(strict), "serving_refresh": stage(),
        "operations_health": stage(), "coordinator_lifecycle": stage(),
        "shard_lifecycle": stage(), "unknown": stage(),
    }
    runtime = {key: 0 for key in reduce.RUNTIME_DELTAS}
    runtime.update({
        "sample_unix_nano": 1, "rss_bytes": 1, "peak_rss_bytes": 1,
        "heap_alloc_bytes": 1, "heap_objects": 1, "goroutines": 1,
        "logical_cpus": 12, "gomaxprocs": 12, "go_memory_limit_bytes": 1,
    })
    after = dict(runtime)
    after["sample_unix_nano"] = 2
    for key in reduce.RUNTIME_DELTAS:
        after[key] = 1
    counters = {key: 1 for key in reduce.COUNTERS}
    counters.update({
        "retries": 0, "redirects": 0, "read_proofs": 0, "generation_pins": 0,
        "partition_opens": 0, "snapshot_pins": 0 if mode == "pinned" else 1000,
        "session_pins": concurrency if mode == "pinned" else 0,
    })
    value = {
        "status": "valid", "budget": {"probes": 2}, "concurrency": concurrency,
        "generation": {"Index": "fixture", "Generation": 1},
        "metrics": {
            "queries": 1000, "completed_queries": 1000, "result_count": 10000,
            "errors": 0, "timeouts": 0, "recall_at_10": .95, "qps": 1000.0,
            "p50_nanos": 1_000_000, "p95_nanos": 1_000_000, "p99_nanos": 1_000_000,
        },
        "counters": counters, "timings": {key: 1 for key in reduce.TIMINGS},
        "catalog_reads": {"nodes": [{"node_config_sha256": "a" * 64}], "total": catalog},
        "runtime": [{"node_config_sha256": "a" * 64, "before": runtime, "after": after}],
        "elapsed_nanos": 1_000_000_000, "total_nanos": samples, "search_mode": mode,
    }
    result = {"max_index_age_nanos": 3_600_000_000_000}
    if mode != "strict":
        value.update({
            "fast_evidence": {
                "Generation": value["generation"], "IndexedThrough": 1,
                "PublishedAt": "2026-08-10T00:00:00Z", "IndexAge": 1,
                "TopologyDigest": "topology", "AuthorizationOverlayDigest": "overlay",
            },
            "min_index_age_nanos": 1, "max_index_age_nanos": 2,
        })
    return value, result


class RevalidationTest(unittest.TestCase):
    def test_valid_strict_fast_and_pinned_cells(self) -> None:
        for mode in reduce.MODES:
            value, result = cell(mode)
            reduce._validate_cell(value, result, mode, 1, {"a" * 64})

    def test_log_barrier_is_rejected(self) -> None:
        value, result = cell()
        value["catalog_reads"]["total"]["total"]["log_barriers"] = 1
        with self.assertRaisesRegex(ValueError, "log barrier"):
            reduce._validate_cell(value, result, "strict", 1, {"a" * 64})

    def test_catalog_proof_must_succeed_without_a_log_entry(self) -> None:
        for key in ("successes", "verify_leader_calls", "no_log_proofs"):
            value, result = cell()
            value["catalog_reads"]["total"]["strict_search"][key] = 0
            with self.assertRaisesRegex(ValueError, "catalog proof"):
                reduce._validate_cell(value, result, "strict", 1, {"a" * 64})

    def test_elapsed_must_cover_the_slowest_worker_lane(self) -> None:
        value, result = cell(concurrency=32)
        value["elapsed_nanos"] = 31_000_000
        with self.assertRaisesRegex(ValueError, "elapsed"):
            reduce._validate_cell(value, result, "strict", 32, {"a" * 64})

    def test_fast_age_bound_is_fail_closed(self) -> None:
        value, result = cell("fast")
        value["max_index_age_nanos"] = result["max_index_age_nanos"] + 1
        with self.assertRaisesRegex(ValueError, "age"):
            reduce._validate_cell(value, result, "fast", 1, {"a" * 64})

    def test_fast_evidence_age_must_not_postdate_the_observed_range(self) -> None:
        value, result = cell("fast")
        value["fast_evidence"]["IndexAge"] = 3
        with self.assertRaisesRegex(ValueError, "evidence age"):
            reduce._validate_cell(value, result, "fast", 1, {"a" * 64})

    def test_retry_or_redirect_is_rejected(self) -> None:
        for counter in ("retries", "redirects"):
            value, result = cell()
            value["counters"][counter] = 1
            with self.assertRaisesRegex(ValueError, "retried or followed"):
                reduce._validate_cell(value, result, "strict", 1, {"a" * 64})

    def test_only_physical_request_bytes_are_excluded_from_semantic_work(self) -> None:
        self.assertEqual(set(reduce.LOGICAL_COUNTERS) - set(reduce.SEMANTIC_COUNTERS), {"request_bytes"})

    def test_tail_exception_requires_an_outlier_with_overlapping_spread(self) -> None:
        native = {"p95_nanos_min": 1, "p95_nanos_max": 3}
        container = {"p95_nanos_min": 2, "p95_nanos_max": 4}
        self.assertTrue(reduce._tail_explained(native, container, 1.14, 1.06))
        self.assertFalse(reduce._tail_explained(native, container, 1.05, 1.06))

    def test_capability_key_digest_is_verified(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "capability.key"
            path.write_bytes(b"expected")
            provenance = {
                "capability_key_path": str(path),
                "capability_key_sha256": reduce._sha256(path),
            }
            reduce._validate_capability_key(provenance)
            path.write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "capability key"):
                reduce._validate_capability_key(provenance)

    def test_command_requires_the_declared_concurrency_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory) / "repeat-1"
            run_dir.mkdir()
            result = {"endpoint": "127.0.0.1:1"}
            provenance = {
                "binary_path": "/bench", "dataset_directory": "/dataset",
                "truth_directory": "/truth", "truth_sha256": "a" * 64,
            }
            time_path = run_dir / "bench-strict.time"
            command_path = run_dir / "bench-strict.command.json"
            command = [
                "/usr/bin/time", "-v", "-o", str(time_path), "/bench", "system-bench",
                "-endpoint", result["endpoint"], "-topology", str(run_dir / "topology.json"),
                "-dataset", provenance["dataset_directory"], "-truth-cache", provenance["truth_directory"],
                "-truth-cache-sha256", provenance["truth_sha256"], "-probes", "2",
                "-concurrency", "1,32", "-top-k", "10", "-ef-search", "128", "-warmup", "1000",
                "-out", str(run_dir / "search-strict.json"), "-search-mode", "strict",
                "-max-index-age", "1h", "-max-session-age", "2m",
            ]
            command_path.write_text(json.dumps(command), encoding="utf-8")
            def attest() -> None:
                time_path.write_text(
                    f'\tCommand being timed: "{" ".join(command[4:])}"\n', encoding="utf-8",
                )

            attest()
            (run_dir / "bench-strict.rc").write_text("0\n", encoding="utf-8")
            reduce._validate_command(run_dir, result, "strict", "single", provenance, "1,32")
            time_path.write_text('\tCommand being timed: "stale"\n', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "process command attestation"):
                reduce._validate_command(run_dir, result, "strict", "single", provenance, "1,32")
            command[command.index("-concurrency") + 1] = "32,1"
            command_path.write_text(json.dumps(command), encoding="utf-8")
            attest()
            with self.assertRaisesRegex(ValueError, "benchmark command changed"):
                reduce._validate_command(run_dir, result, "strict", "single", provenance, "1,32")


if __name__ == "__main__":
    unittest.main()
