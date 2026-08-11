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
RUNNER_PATH = Path(__file__).with_name("run_revalidation.py")
RUNNER_SPEC = importlib.util.spec_from_file_location("run_revalidation", RUNNER_PATH)
assert RUNNER_SPEC is not None and RUNNER_SPEC.loader is not None
runner = importlib.util.module_from_spec(RUNNER_SPEC)
RUNNER_SPEC.loader.exec_module(runner)
NODE = "a" * 64
OWNERS = {NODE: {"cpu_set": "0-11", "gomaxprocs": 12, "go_memory_limit_bytes": 1}}
EXPECTED_FAST = {"IndexedThrough": 1, "TopologyDigest": "topology", "AuthorizationOverlayDigest": "overlay"}
PROOF_IDENTITY = {
    "last_term": 1, "last_catalog_applied_index": 1,
    "last_raft_applied_index": 1, "last_raft_log_index": 1,
}


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
    catalog.update(PROOF_IDENTITY)
    before = {name: stage() for name in reduce.CATALOG_STAGES}
    before.update(PROOF_IDENTITY)

    def retained_catalog() -> dict:
        return {**{name: dict(catalog[name]) for name in reduce.CATALOG_STAGES}, **PROOF_IDENTITY}

    runtime = {key: 0 for key in reduce.RUNTIME_DELTAS}
    runtime.update({
        "sample_unix_nano": 1_000_000_001, "rss_bytes": 1, "peak_rss_bytes": 1,
        "heap_alloc_bytes": 1, "heap_objects": 1, "goroutines": 1,
        "logical_cpus": 12, "gomaxprocs": 12, "go_memory_limit_bytes": 1,
        "effective_cpu_set": "0-11",
    })
    after = dict(runtime)
    after["sample_unix_nano"] = 1_000_000_002
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
            "errors": 0, "timeouts": 0, "recall_at_10": reduce.EXPECTED_RECALL, "qps": 1000.0,
            "p50_nanos": 1_000_000, "p95_nanos": 1_000_000, "p99_nanos": 1_000_000,
        },
        "counters": counters, "timings": {key: 1_000_000_000 if key == "client_total" else 1 for key in reduce.TIMINGS},
        "catalog_reads": {"nodes": [{
            "node_config_sha256": NODE,
            "before": before, "after": retained_catalog(), "delta": retained_catalog(),
        }], "total": catalog},
        "runtime": [{"node_config_sha256": NODE, "before": runtime, "after": after}],
        "elapsed_nanos": 1_000_000_000, "total_nanos": samples, "search_mode": mode,
    }
    value["timings"].update({"rpc": 4, "total": 2})
    result = {
        "max_index_age_nanos": 3_600_000_000_000,
        "started_at": "1970-01-01T00:00:01.000000000Z",
        "completed_at": "1970-01-01T00:00:01.000000003Z",
    }
    if mode != "strict":
        value.update({
            "fast_evidence": {
                "Generation": value["generation"], "IndexedThrough": 1,
                "PublishedAt": "1970-01-01T00:00:00.999999Z", "IndexAge": 1000,
                "TopologyDigest": "topology", "AuthorizationOverlayDigest": "overlay",
            },
            "min_index_age_nanos": 1000, "max_index_age_nanos": 1000,
        })
    return value, result


def validate_cell(value: dict, result: dict, mode: str, concurrency: int,
                  owners: dict = OWNERS) -> dict:
    return reduce._validate_cell(value, result, mode, concurrency, owners, EXPECTED_FAST)


class RevalidationTest(unittest.TestCase):
    def test_valid_strict_fast_and_pinned_cells(self) -> None:
        for mode in reduce.MODES:
            value, result = cell(mode)
            validate_cell(value, result, mode, 1)

    def test_recall_is_frozen_for_matched_comparisons(self) -> None:
        value, result = cell()
        value["metrics"]["recall_at_10"] = .90
        with self.assertRaisesRegex(ValueError, "matched frozen value"):
            validate_cell(value, result, "strict", 1)

    def test_runtime_budget_is_frozen(self) -> None:
        ownership = {"cpu_set": "0-11", "gomaxprocs": 12, "go_memory_limit_bytes": 24 << 30}
        nodes = [{"node_config_sha256": NODE, "runtime_ownership": ownership}]
        self.assertEqual(reduce._runtime_owners("single", nodes), {NODE: ownership})
        ownership["go_memory_limit_bytes"] = 1
        with self.assertRaisesRegex(ValueError, "runtime budget"):
            reduce._runtime_owners("single", nodes)

    def test_logical_search_work_counters_are_positive(self) -> None:
        for counter in reduce.POSITIVE_COUNTERS:
            value, result = cell()
            value["counters"][counter] = 0
            with self.assertRaisesRegex(ValueError, "logical work"):
                validate_cell(value, result, "strict", 1)

    def test_logical_search_work_matches_across_concurrency(self) -> None:
        first, _ = cell(concurrency=1)
        reference = reduce._validate_logical_work(None, first["counters"])
        second, _ = cell(concurrency=32)
        reduce._validate_logical_work(reference, second["counters"])
        second["counters"][reduce.SEMANTIC_COUNTERS[0]] += 1
        with self.assertRaisesRegex(ValueError, "across concurrency"):
            reduce._validate_logical_work(reference, second["counters"])

    def test_generation_matches_the_retained_m3_snapshot(self) -> None:
        expected = {"Index": "embedding_graph", "Generation": 1}
        reduce._validate_generation(expected, expected)
        with self.assertRaisesRegex(ValueError, "retained M3 snapshot"):
            reduce._validate_generation({"Index": "embedding_graph", "Generation": 2}, expected)

    def test_fast_identity_is_bound_to_retained_inputs(self) -> None:
        for field, changed in (("IndexedThrough", 2), ("TopologyDigest", "other"),
                               ("AuthorizationOverlayDigest", "other")):
            value, result = cell("fast")
            value["fast_evidence"][field] = changed
            with self.assertRaisesRegex(ValueError, "snapshot identity"):
                validate_cell(value, result, "fast", 1)
        value, result = cell("fast")
        value["fast_evidence"]["PublishedAt"] = "2026-08-10T00:00:00Z"
        with self.assertRaisesRegex(ValueError, "publication"):
            validate_cell(value, result, "fast", 1)

    def test_log_barrier_is_rejected(self) -> None:
        value, result = cell()
        for retained in (value["catalog_reads"]["total"], value["catalog_reads"]["nodes"][0]["after"], value["catalog_reads"]["nodes"][0]["delta"]):
            retained["total"]["log_barriers"] = 1
        with self.assertRaisesRegex(ValueError, "log barrier"):
            validate_cell(value, result, "strict", 1)

    def test_catalog_proof_must_succeed_without_a_log_entry(self) -> None:
        for key in ("successes", "verify_leader_calls", "no_log_proofs"):
            value, result = cell()
            for retained in (value["catalog_reads"]["total"], value["catalog_reads"]["nodes"][0]["after"], value["catalog_reads"]["nodes"][0]["delta"]):
                retained["strict_search"][key] = 0
            with self.assertRaisesRegex(ValueError, "catalog proof"):
                validate_cell(value, result, "strict", 1)

    def test_catalog_node_delta_must_match_the_aggregate(self) -> None:
        value, result = cell()
        value["catalog_reads"]["nodes"][0]["delta"]["strict_search"]["log_barriers"] = 1
        with self.assertRaisesRegex(ValueError, "catalog node delta"):
            validate_cell(value, result, "strict", 1)

    def test_catalog_proof_identity_is_required_and_consistent(self) -> None:
        for mutation in ("missing", "contradictory"):
            value, result = cell()
            if mutation == "missing":
                del value["catalog_reads"]["nodes"][0]["after"]["last_term"]
            else:
                value["catalog_reads"]["nodes"][0]["after"]["last_raft_log_index"] = 0
            with self.assertRaisesRegex(ValueError, "catalog proof identity"):
                validate_cell(value, result, "strict", 1)

    def test_benchmark_endpoint_must_belong_to_the_public_node(self) -> None:
        specific = {"nodes": [{"public_listen": "127.0.0.1:10", "local_groups": [{"listen": "127.0.0.1:11"}]}]}
        reduce._validate_public_endpoint(specific, {"endpoint": "127.0.0.1:10"})
        wildcard = {"nodes": [{"public_listen": "0.0.0.0:10", "local_groups": [{"listen": "172.30.0.1:11"}]}]}
        reduce._validate_public_endpoint(wildcard, {"endpoint": "172.30.0.1:10"})
        with self.assertRaisesRegex(ValueError, "not owned"):
            reduce._validate_public_endpoint(wildcard, {"endpoint": "172.30.0.2:10"})

    def test_elapsed_must_cover_the_slowest_worker_lane(self) -> None:
        value, result = cell(concurrency=32)
        value["elapsed_nanos"] = 31_000_000
        with self.assertRaisesRegex(ValueError, "elapsed"):
            validate_cell(value, result, "strict", 32)

    def test_client_total_must_equal_raw_samples(self) -> None:
        value, result = cell()
        value["timings"]["client_total"] -= 1
        with self.assertRaisesRegex(ValueError, "client timing total"):
            validate_cell(value, result, "strict", 1)

    def test_onramp_timings_are_positive(self) -> None:
        for timing in reduce.REMOVABLE_TIMINGS:
            value, result = cell()
            value["timings"][timing] = 0
            with self.assertRaisesRegex(ValueError, "on-ramp"):
                validate_cell(value, result, "strict", 1)

    def test_server_timings_are_positive_and_consistent(self) -> None:
        for timing in reduce.SERVER_TIMINGS:
            value, result = cell()
            value["timings"][timing] = 0
            with self.assertRaisesRegex(ValueError, "server timings"):
                validate_cell(value, result, "strict", 1)
        for timing, changed in (("total", 1), ("network", 5), ("shard_search", 2)):
            value, result = cell()
            value["timings"][timing] = changed
            with self.assertRaisesRegex(ValueError, "server timings"):
                validate_cell(value, result, "strict", 1)

    def test_catalog_stage_timings_are_consistent(self) -> None:
        for field, changed in (("barrier_nanos", 1), ("total_nanos", 0)):
            value, result = cell()
            for retained in (value["catalog_reads"]["total"], value["catalog_reads"]["nodes"][0]["after"], value["catalog_reads"]["nodes"][0]["delta"]):
                retained["strict_search"][field] = changed
                retained["total"][field] = changed
            with self.assertRaisesRegex(ValueError, "catalog proof"):
                validate_cell(value, result, "strict", 1)
        value, result = cell()
        for retained in (value["catalog_reads"]["total"], value["catalog_reads"]["nodes"][0]["after"], value["catalog_reads"]["nodes"][0]["delta"]):
            retained["total"]["total_nanos"] += 1
        with self.assertRaisesRegex(ValueError, "catalog total stage"):
            validate_cell(value, result, "strict", 1)

    def test_fast_age_bound_is_fail_closed(self) -> None:
        value, result = cell("fast")
        value["max_index_age_nanos"] = result["max_index_age_nanos"] + 1
        with self.assertRaisesRegex(ValueError, "age"):
            validate_cell(value, result, "fast", 1)

    def test_fast_evidence_age_must_not_postdate_the_observed_range(self) -> None:
        value, result = cell("fast")
        value["fast_evidence"]["IndexAge"] = 3
        with self.assertRaisesRegex(ValueError, "evidence age"):
            validate_cell(value, result, "fast", 1)

    def test_retry_or_redirect_is_rejected(self) -> None:
        for counter in ("retries", "redirects"):
            value, result = cell()
            value["counters"][counter] = 1
            with self.assertRaisesRegex(ValueError, "retried or followed"):
                validate_cell(value, result, "strict", 1)

    def test_runtime_ownership_must_match_topology_during_measurement(self) -> None:
        value, result = cell()
        value["runtime"][0]["after"]["effective_cpu_set"] = "1-11"
        with self.assertRaisesRegex(ValueError, "runtime ownership"):
            validate_cell(value, result, "strict", 1)

    def test_runtime_samples_must_be_ordered_inside_the_result_interval(self) -> None:
        for before, after in ((1, 1), (2, 1), (0, 2), (1, 4)):
            value, result = cell()
            value["runtime"][0]["before"]["sample_unix_nano"] = before
            value["runtime"][0]["after"]["sample_unix_nano"] = after
            with self.assertRaisesRegex(ValueError, "sample chronology"):
                validate_cell(value, result, "strict", 1)

    def test_runtime_peak_rss_must_not_regress(self) -> None:
        value, result = cell()
        value["runtime"][0]["before"]["peak_rss_bytes"] = 2
        with self.assertRaisesRegex(ValueError, "peak RSS regressed"):
            validate_cell(value, result, "strict", 1)

    def test_runtime_cells_follow_the_declared_concurrency_order(self) -> None:
        first, _ = cell(concurrency=1)
        second, _ = cell(concurrency=32)
        second["runtime"][0]["before"]["sample_unix_nano"] = 1_000_000_003
        second["runtime"][0]["after"]["sample_unix_nano"] = 1_000_000_004
        reduce._validate_runtime_cell_order([first, second], (1, 32))
        with self.assertRaisesRegex(ValueError, "intervals overlap"):
            reduce._validate_runtime_cell_order([first, first], (1, 1))
        with self.assertRaisesRegex(ValueError, "concurrency order"):
            reduce._validate_runtime_cell_order([second, first], (1, 32))

    def test_completed_public_queries_require_frame_bytes(self) -> None:
        for counter in ("public_request_frame_bytes", "public_response_frame_bytes"):
            value, result = cell()
            value["counters"][counter] = 0
            with self.assertRaisesRegex(ValueError, "public frame"):
                validate_cell(value, result, "strict", 1)

    def test_execution_head_is_frozen(self) -> None:
        provenance = {
            "source_head": reduce.EXPECTED_SOURCE_HEAD,
            "vcs_modified": False,
            "binary_sha256": "a" * 64,
        }
        reduce._validate_provenance(provenance)
        provenance["source_head"] = "b" * 64
        with self.assertRaisesRegex(ValueError, "execution provenance"):
            reduce._validate_provenance(provenance)

    def test_only_physical_request_bytes_are_excluded_from_semantic_work(self) -> None:
        self.assertEqual(set(reduce.LOGICAL_COUNTERS) - set(reduce.SEMANTIC_COUNTERS), {"request_bytes"})

    def test_tail_exception_requires_an_outlier_with_overlapping_spread(self) -> None:
        native = {"p95_nanos_min": 1, "p95_nanos_max": 3}
        container = {"p95_nanos_min": 2, "p95_nanos_max": 4}
        self.assertTrue(reduce._tail_explained(native, container, 1.14, 1.06))
        self.assertFalse(reduce._tail_explained(native, container, 1.05, 1.06))
        self.assertFalse(reduce._tail_explained(native, container, 1.16, 1.06))

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

    def test_topology_nodes_use_the_provenance_capability_key(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "capability.key"
            path.write_bytes(b"expected")
            topology = {"nodes": [{"capability_key_path": str(path)}]}
            reduce._validate_topology_capability_key(topology, path.resolve())
            topology["nodes"][0]["capability_key_path"] = str(path.with_name("other.key"))
            with self.assertRaisesRegex(ValueError, "topology capability key"):
                reduce._validate_topology_capability_key(topology, path.resolve())

    def test_topology_uses_the_provenance_dataset(self) -> None:
        provenance = {"dataset_directory": "/dataset"}
        topology = {"dataset_directory": "/dataset"}
        reduce._validate_topology_dataset(topology, provenance)
        topology["dataset_directory"] = "/other"
        with self.assertRaisesRegex(ValueError, "topology dataset"):
            reduce._validate_topology_dataset(topology, provenance)

    def test_m3_descriptor_is_bound_to_execution_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "dataset"
            database = root / "database"
            artifacts = root / "250k/graph-overlap-020-out"
            dataset.mkdir()
            database.mkdir()
            artifacts.mkdir(parents=True)
            fixture = {"checksum": "fixture"}
            descriptor = {
                "base_sha": "base", "head_sha": "head", "executable_sha256": "b" * 64,
                "artifact_sha256": "", "fixture_checksum": fixture["checksum"],
                "partition_generation": 1,
            }
            (dataset / "fixture_manifest.json").write_text(json.dumps(fixture), encoding="utf-8")
            artifact = artifacts / "vector_partition_placeholder.json"
            artifact.write_bytes(b"artifact")
            descriptor["artifact_sha256"] = reduce._sha256(artifact)
            artifact.rename(artifacts / f"vector_partition_{descriptor['artifact_sha256'][:12]}_head.json")
            descriptor_path = database / "vector_partition_variant_v1.json"
            descriptor_path.write_text(json.dumps(descriptor), encoding="utf-8")
            provenance = {
                "base_sha": "base", "source_head": "head", "binary_sha256": "b" * 64,
                "m3_artifact_sha256": descriptor["artifact_sha256"],
                "m3_descriptor_sha256": reduce._sha256(descriptor_path),
                "m3_database_directory": str(database), "dataset_directory": str(dataset),
                "fixture_checksum": fixture["checksum"],
            }
            self.assertEqual(
                reduce._validate_m3(root, provenance),
                {"Index": "embedding_graph", "Generation": descriptor["partition_generation"]},
            )
            runner.validate_m3(root, provenance)
            descriptor_path.write_text(json.dumps(descriptor, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "M3 execution provenance"):
                runner.validate_m3(root, provenance)
            provenance["m3_descriptor_sha256"] = reduce._sha256(descriptor_path)
            descriptor["head_sha"] = "other"
            descriptor_path.write_text(json.dumps(descriptor), encoding="utf-8")
            provenance["m3_descriptor_sha256"] = reduce._sha256(descriptor_path)
            with self.assertRaisesRegex(ValueError, "M3 execution provenance"):
                reduce._validate_m3(root, provenance)

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
