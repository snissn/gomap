#!/usr/bin/env python3

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from system_qualification import ContractError
from vector_onramp_profile import CATALOG_SOURCES, STAGE_FIELDS, _validate_command, validate_cell


NODE = "a" * 64


def stage(reads: int) -> dict[str, int]:
    value = {key: 0 for key in STAGE_FIELDS}
    value.update({"reads": reads, "successes": reads, "verify_leader_calls": reads, "log_barriers": reads, "total_nanos": reads * 4, "admission_nanos": reads, "verify_leader_nanos": reads, "barrier_nanos": reads, "applied_read_nanos": reads})
    return value


def catalog_stats(reads: dict[str, int], raft_log: int) -> dict:
    sources = {source: stage(reads[source]) for source in CATALOG_SOURCES}
    total = {key: sum(sources[source][key] for source in CATALOG_SOURCES) for key in STAGE_FIELDS}
    return {"total": total, **sources, "last_term": 2, "last_catalog_applied_index": 10, "last_raft_applied_index": raft_log, "last_raft_log_index": raft_log}


def runtime(sample: int, scale: int) -> dict[str, int]:
    return {
        "sample_unix_nano": sample, "cpu_time_nanos": 10 * scale, "run_queue_delay_nanos": scale, "timeslices": scale,
        "voluntary_context_switches": scale, "nonvoluntary_context_switches": scale, "rss_bytes": 100, "peak_rss_bytes": 100,
        "heap_alloc_bytes": 10, "heap_objects": 1, "total_alloc_bytes": 10 * scale, "mallocs": scale, "frees": scale - 1,
        "num_gc": scale - 1, "pause_total_nanos": scale - 1, "goroutines": 2,
    }


def cell() -> dict:
    counts = {"operations_health": 1000, "coordinator_lifecycle": 1000, "shard_lifecycle": 1834, "unknown": 0}
    before = catalog_stats({source: 0 for source in CATALOG_SOURCES}, 10)
    after = catalog_stats(counts, 3844)
    delta = catalog_stats(counts, 3844)
    timings = {key: 1 for key in (
        "admission", "operations_health", "service_adapter", "public_adapter", "router_open", "router_search", "placement",
        "coordinator_lifecycle", "dispatch", "queue", "rpc", "network", "read_index_apply", "generation_open", "shard_search",
        "response", "dedupe", "merge", "coordinator_total", "total", "client_encode", "client_write", "client_response_read",
        "client_decode", "client_total",
    )}
    timings.update({"operations_health": 100, "coordinator_lifecycle": 100, "dispatch": 600, "coordinator_total": 800, "total": 1000, "client_response_read": 1100, "client_total": 1200})
    samples = [3] * 1000
    return {
        "status": "valid", "budget": {"probes": 2}, "concurrency": 1, "generation": {"Index": "embedding", "Generation": 1},
        "metrics": {"queries": 1000, "completed_queries": 1000, "result_count": 10000, "errors": 0, "timeouts": 0, "recall_at_10": .95, "qps": 1_000_000_000_000 / 3000, "p50_nanos": 3, "p95_nanos": 3, "p99_nanos": 3},
        "counters": {key: (1834 if key == "selected_groups" else 0 if key in ("retries", "redirects") else 1) for key in (
            "selected_partitions", "selected_groups", "requests", "rpcs", "retries", "redirects", "candidates", "edges", "query_bytes", "request_bytes", "candidate_bytes", "response_bytes", "public_request_frame_bytes", "public_response_frame_bytes",
        )},
        "timings": timings,
        "catalog_reads": {"nodes": [{"node_config_sha256": NODE, "before": before, "after": after, "delta": delta}], "total": after},
        "runtime": [{"node_config_sha256": NODE, "before": runtime(1, 1), "after": runtime(2, 2)}],
        "elapsed_nanos": 3000, "total_nanos": samples,
    }


class VectorOnrampProfileTest(unittest.TestCase):
    def test_published_copy_keeps_exact_original_client_attestation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            published = Path(directory) / "search.json"
            original = Path("/scratch/repeat-1/search.json")
            command = [
                "/usr/bin/time", "-v", "-o", str(original.with_name("bench.time")), "/bench", "system-bench",
                "-endpoint", "127.0.0.1:1000", "-topology", str(original.with_name("topology.json")),
                "-dataset", "/dataset", "-truth-cache", "/truth", "-truth-cache-sha256", "a" * 64,
                "-probes", "2", "-concurrency", "1,32", "-top-k", "10", "-ef-search", "128",
                "-warmup", "1000", "-out", str(original),
            ]
            published.with_name("bench.command.json").write_text(json.dumps(command) + "\n", encoding="utf-8")
            published.with_name("bench.time").write_text('\tCommand being timed: "' + " ".join(command[4:]) + '"\n', encoding="utf-8")
            published.with_name("bench.rc").write_text("0\n", encoding="utf-8")
            attestation = _validate_command(
                published,
                {"endpoint": "127.0.0.1:1000", "truth_artifact_sha256": "a" * 64},
                {"dataset_directory": "/dataset"},
                "single",
                "/bench",
            )
            self.assertEqual(set(attestation), {"command", "time", "rc"})

    def test_exact_catalog_runtime_and_wall_evidence(self) -> None:
        evidence = validate_cell(cell(), {NODE})
        self.assertEqual(evidence["catalog"]["reads_per_query"], 3.834)
        self.assertEqual(evidence["catalog"]["log_barriers_per_query"], 3.834)
        self.assertGreater(evidence["runtime"]["cpu_time_nanos_per_query"], 0)
        self.assertNotIn("run_queue_delay_nanos_per_query", evidence["runtime"])
        self.assertNotIn("timeslices_per_query", evidence["runtime"])
        self.assertIn("operations_health", evidence["wall"]["public_exclusive_nanos_per_query"])

    def test_disappearing_thread_schedstat_is_not_process_evidence(self) -> None:
        value = cell()
        value["runtime"][0]["after"]["run_queue_delay_nanos"] = 0
        value["runtime"][0]["after"]["timeslices"] = 0
        validate_cell(value, {NODE})

    def test_worker_runtime_and_catalog_log_proof_fail_closed(self) -> None:
        value = cell()
        value["runtime"][0]["after"]["cpu_time_nanos"] = value["runtime"][0]["before"]["cpu_time_nanos"]
        with self.assertRaisesRegex(ContractError, "worker threads"):
            validate_cell(value, {NODE})
        value = cell()
        value["catalog_reads"]["nodes"][0]["after"]["last_raft_log_index"] -= 1
        value["catalog_reads"]["nodes"][0]["delta"]["last_raft_log_index"] -= 1
        with self.assertRaisesRegex(ContractError, "under-reported"):
            validate_cell(value, {NODE})

    def test_worker_lane_elapsed_is_not_average_bound(self) -> None:
        value = cell()
        value["elapsed_nanos"] = 2999
        with self.assertRaisesRegex(ContractError, "elapsed time"):
            validate_cell(value, {NODE})

    def test_concurrent_timings_are_aggregate_work_not_wall_time(self) -> None:
        value = cell()
        value["concurrency"] = 32
        value["elapsed_nanos"] = 96
        value["metrics"]["qps"] = 1_000_000_000_000 / 96
        evidence = validate_cell(value, {NODE})
        self.assertEqual(evidence["wall"]["accounting_kind"], "aggregate_request_work")
        self.assertEqual(evidence["wall"]["serial_scheduling_unexplained_nanos_per_query"], 0)


if __name__ == "__main__":
    unittest.main()
