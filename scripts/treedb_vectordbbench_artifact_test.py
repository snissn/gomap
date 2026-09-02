#!/usr/bin/env python3
"""Unit tests for treedb_vectordbbench_artifact.py helpers."""

from __future__ import annotations

import contextlib
import datetime as _dt
import functools
import gzip
import hashlib
import io
import inspect
import json
import os
import shlex
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock
from types import SimpleNamespace

import treedb_construction_policy_4587 as policy
import treedb_vectordbbench_artifact as harness


@functools.cache
def go_root() -> Path:
    return Path(subprocess.run(
        ("go", "env", "GOROOT"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=10,
        check=True,
    ).stdout.strip())


@functools.cache
def valid_pprof_fixture() -> bytes:
    return (go_root() / "src/cmd/compile/internal/test/testdata/pgo/inline/inline_hot.pprof").read_bytes()


@functools.cache
def valid_trace_fixture() -> bytes:
    return (
        go_root() / "src/internal/trace/internal/tracev1/testdata/user_task_region_1_19_good"
    ).read_bytes()


@functools.cache
def valid_heap_pprof_fixture() -> bytes:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source = root / "heap_profile.go"
        profile = root / "heap.pprof"
        source.write_text(
            """package main
import (
    "os"
    "runtime"
    "runtime/pprof"
)
func main() {
    data := make([]byte, 1<<20)
    runtime.GC()
    out, err := os.Create(os.Args[1])
    if err != nil { panic(err) }
    if err := pprof.Lookup("heap").WriteTo(out, 0); err != nil { panic(err) }
    if err := out.Close(); err != nil { panic(err) }
    runtime.KeepAlive(data)
}
""",
            encoding="utf-8",
        )
        subprocess.run(
            ("go", "run", str(source), str(profile)),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=30,
            check=True,
        )
        return profile.read_bytes()


def valid_perf_fixture() -> bytes:
    header = bytearray(104)
    header[:8] = b"PERFILE2"
    header[8:16] = (104).to_bytes(8, "little")
    header[16:24] = (80).to_bytes(8, "little")
    header[24:32] = (104).to_bytes(8, "little")
    header[32:40] = (80).to_bytes(8, "little")
    header[40:48] = (184).to_bytes(8, "little")
    header[48:56] = (16).to_bytes(8, "little")
    attrs = bytearray(80)
    attrs[4:8] = (64).to_bytes(4, "little")
    sample = (9).to_bytes(4, "little") + b"\x00\x00" + (16).to_bytes(2, "little") + b"sample!!"
    return bytes(header + attrs + sample)


def lifecycle_fixture(root: Path) -> tuple[dict, list[dict]]:
    data_dir = root / "treedb-data"
    dataset_path = root / "train.parquet"
    dataset_path.write_bytes(b"fixture dataset\n")
    task_config = {
        "db_config": {"index_name": "index-a"},
        "case_config": {"custom_case": {"dataset_config": {
            "size": "50000",
            "dim": "768",
            "dir": str(root),
            "file_count": "1",
            "use_shuffled": False,
        }}},
    }
    task_config_sha256 = harness.canonical_sha256(task_config)
    result_path = root / "vdbbench-result.json"
    harness.write_json(result_path, {
        "run_id": "fixture-run",
        "results": [{
            "label": ":)",
            "task_config": task_config,
            "metrics": {
                "inserted_count": 50_000,
                "insert_duration": 1.0,
                "optimize_duration": 2.0,
                "load_duration": 3.0,
            },
        }],
    })
    load_metrics = harness.load_metrics_from_result(
        result_path, "index-a", "PerformanceCustomDataset", root
    )
    service_binary = root / "bin" / "treedb-document-service"
    service_binary.parent.mkdir(parents=True)
    service_binary.write_bytes(b"fixture treedb document service\n")
    service_binary.chmod(0o755)
    service_binary_sha256 = harness.sha256_file(service_binary)
    vectordbbench_root = root / "vectordbbench"
    vectordbbench_root.mkdir()
    vdbbench_command = [
        "python", "-m", "vectordb_bench.cli.vectordbbench",
        "treedbcolumngraphexact",
        "--base-url", "http://127.0.0.1:9876",
        "--index-name", "index-a",
        "--timeout", "30.0",
        "--m", "16",
        "--ef-construction", "128",
        "--ef-search", "100",
        "--case-type", "PerformanceCustomDataset",
        "--k", "2",
        "--num-concurrency", "32",
        "--concurrency-duration", "30",
        "--db-label", "fixture-lifecycle",
    ]
    vdbbench_command_string = shlex.join(vdbbench_command)
    profile = root / "profiles" / "optimize.heap.pprof"
    profile.parent.mkdir(parents=True)
    profile.write_bytes(valid_heap_pprof_fixture())
    lifecycle_route_response = root / "lifecycle_route_response.json"
    harness.write_json(lifecycle_route_response, {
        "index": {"name": "index-a", "generation": 7},
        "vector_index_name": "vector_hnsw",
        "query_mode": "exact",
        "request_ef_search": 100,
        "quantized_index_name": None,
        "results": [{"id": "1"}, {"id": "2"}],
        "no_documents": True,
        "stats": {"search_route_hnsw_search_pack": 1},
        "diagnostics": {
            "route": "exact_hnsw_search_pack_v1",
            "fallback_reason": "none",
            "no_document_guardrails_ok": True,
            "exact_hnsw_search_pack_no_doc_route": True,
        },
    })
    lifecycle_count_response = root / "lifecycle_count_response.json"
    harness.write_json(lifecycle_count_response, {
        "index": {
            "name": "index-a",
            "vector_index_name": "vector_hnsw",
            "generation": 7,
        },
        "count": 50_000,
    })
    stages = [
        "startup", "reset", "load_start", "load_end", "drain_checkpoint",
        "optimize_start", "optimize_end", "cache_prime", "cache_warm",
        "graceful_close", "cold_open_ready", "exact_verify", "route_verify", "teardown",
    ]
    events = []
    for sequence, stage in enumerate(stages):
        loaded = sequence >= 3
        durable = sequence >= 4
        reopened = sequence >= 11
        state = {
            "rows": {
                "client_sent": 50_000 if loaded else 0,
                "server_accepted": 50_000 if loaded else 0,
                "server_durable": 50_000 if durable else 0,
                "reopened": 50_000 if reopened else 0,
            },
            "wal": {"frontier": sequence, "bytes_written_total": sequence * 100},
            "counters": {
                "commit_seq": 50_000 if loaded else 0,
                "wal_write_bytes_total": sequence * 100,
                "indexed_stage_docs_total": 50_000 if loaded else 0,
                "indexed_flush_docs_total": 50_000 if durable else 0,
            },
        }
        if sequence >= 6:
            state["index"] = {
                "identity": "index-a:vector_hnsw",
                "asset_generation": 7,
                "status": "ready",
            }
        if sequence >= 9:
            state["database"] = {"identity": "database-a", "commit_seq": 50_000}
        if stage == "route_verify":
            state["route"] = {
                "name": "exact_hnsw_search_pack_v1",
                "fallback_reason": "none",
                "optimized": True,
                "index_identity": "index-a:vector_hnsw",
                "index_asset_generation": 7,
                "service_generation": 7,
                "requested_top_k": 2,
                "result_count": 2,
                "effective_ef_search": 100,
            }
        events.append({
            "schema_version": harness.LIFECYCLE_EVENT_SCHEMA,
            "sequence": sequence,
            "stage": stage,
            "timestamp": f"2026-08-27T00:00:{sequence:02d}Z",
            "state": state,
        })
    lifecycle_path = root / "lifecycle.jsonl"
    lifecycle_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in events), encoding="utf-8")
    boundary_stages = ("load_end", "optimize_start", "optimize_end", "cache_prime", "cache_warm")
    boundary_ns = {
        stage: int(_dt.datetime.fromisoformat(events[stages.index(stage)]["timestamp"].replace("Z", "+00:00")).timestamp() * 1_000_000_000)
        for stage in boundary_stages
    }
    adapter_records = [
        {"event": "reset", "timestamp_ns": boundary_ns["load_end"] - 3_000_000_000, "response": {}},
        {"event": "load_start", "timestamp_ns": boundary_ns["load_end"] - 2_000_000_000},
        *[{
            "event": "batch_accepted",
            "timestamp_ns": boundary_ns["load_end"] - 1_000_000_000 + batch,
            "client_sent": 500,
            "server_accepted": 500,
        } for batch in range(100)],
        {"event": "load_end", "timestamp_ns": boundary_ns["load_end"]},
        {"event": "optimize_start", "timestamp_ns": boundary_ns["optimize_start"]},
        {
            "event": "optimize_end",
            "timestamp_ns": boundary_ns["optimize_end"],
            "response": {
                "index": {
                    "name": "index-a",
                    "generation": 7,
                    "vector_strategy": "column_graph",
                    "vector_m": 16,
                    "vector_ef_construction": 128,
                },
                "vector_index_name": "vector_hnsw",
                "status": {
                    "root_id": 0,
                    "strategy": "column_graph",
                    "state": "column_graph_loaded",
                    "loaded": True,
                    "rebuild_needed": False,
                    "column_graph_build": {
                        "adjacency_build_nanos": 1,
                        "construction_decisions": {
                            "planning": {"decisions": 1, "saturated": False},
                            "reciprocal": {"decisions": 0, "saturated": False},
                        },
                    },
                },
            },
        },
        {"event": "cache_prime", "timestamp_ns": boundary_ns["cache_prime"]},
        {"event": "cache_warm", "timestamp_ns": boundary_ns["cache_warm"]},
    ]
    adapter_path = root / "adapter-lifecycle.jsonl"
    adapter_path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in adapter_records), encoding="utf-8"
    )
    milestone_path = root / "lifecycle_load_milestones.json"
    harness.write_json(
        milestone_path, harness.lifecycle_load_milestone_document(adapter_records)
    )
    service_log = root / "service.log"
    service_log.write_text("fixture service started and stopped cleanly\n", encoding="utf-8")
    diagnostics = []
    for stage in boundary_stages:
        event_state = events[stages.index(stage)]["state"]
        diagnostics.append({
            "timestamp_ns": boundary_ns[stage] + 1,
            "boundary": stage,
            "boundary_timestamp_ns": boundary_ns[stage],
            "snapshot": {
                "database": {
                    "treedb.commit_seq": event_state["counters"]["commit_seq"],
                    "treedb.command_wal.write.bytes_total": event_state["counters"]["wal_write_bytes_total"],
                    "treedb.command_wal.durable_wal_lsn": event_state["wal"]["frontier"],
                },
                "collections": {
                    "treedb.collections.write_domain.indexed_stage.docs_total": event_state["counters"]["indexed_stage_docs_total"],
                    "treedb.collections.write_domain.indexed_flush.docs_total": event_state["counters"]["indexed_flush_docs_total"],
                },
            },
            "wal_filesystem": {"path": str(data_dir / "maindb" / "wal"), "files": 1, "bytes": 100},
        })
    diagnostics_path = root / "diagnostics.jsonl"
    diagnostics_path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in diagnostics), encoding="utf-8"
    )
    acknowledgement_path = root / "lifecycle-boundary-diagnostics.json"
    harness.write_json(acknowledgement_path, {
        "boundary": "cache_warm",
        "boundary_timestamp_ns": boundary_ns["cache_warm"],
        "sample_timestamp_ns": boundary_ns["cache_warm"] + 1,
    })
    manifest = {
        "schema_version": harness.ARTIFACT_SCHEMA,
        "context": {
            "gomap": {"commit": "1" * 40, "dirty": False},
            "vectordbbench": {
                "path": str(vectordbbench_root), "commit": "2" * 40, "dirty": False,
            },
            "host": {
                "logical_cpu_count": 16,
                "physical_cpu_count": 8,
                "memory_bytes": 64 * 1024**3,
                "storage": {
                    "path": str(root),
                    "method": "findmnt",
                    "device": "/dev/nvme0n1p1",
                    "filesystem": "xfs",
                    "mount": str(root),
                    "capacity_bytes": 1_000_000,
                },
            },
        },
        "service": {
            "base_url": "http://127.0.0.1:9876",
            "profile": "command_wal_durable",
            "data_dir": str(data_dir),
            "command": [
                str(service_binary), "-dir", str(data_dir),
                "-addr", "127.0.0.1:9876", "-pprof", "127.0.0.1:6060",
                "-profile", "command_wal_durable",
            ],
            "binary": {"path": str(service_binary), "sha256": service_binary_sha256},
        },
        "harness": {
            "mode": "vdbbench+lifecycle",
            "rows": "exact",
            "case_type": "PerformanceCustomDataset",
            "k": 2,
            "num_per_batch": 500,
            "num_concurrency": "32",
            "concurrency_duration": 30,
            "client_timeout": 30.0,
            "db_label": "fixture-lifecycle",
            "m": 16,
            "ef_construction": 128,
            "ef_search": 100,
            "rerank_candidates": 32,
            "quantized_index_name": "embedding_scalar_u8",
            "vdbbench_dry_run": False,
            "construction_decision_diagnostics": False,
        },
        "vdbbench": [{
            "row": "exact",
            "command": vdbbench_command_string,
            "exit_code": 0,
            "num_per_batch": 500,
            "load_metrics": load_metrics,
        }],
        "commands": [{
            "name": "vdbbench_exact",
            "command": vdbbench_command,
            "command_string": vdbbench_command_string,
            "cwd": str(vectordbbench_root),
            "started_at": "2026-08-27T00:00:00Z",
            "finished_at": "2026-08-27T00:00:03Z",
            "duration_seconds": 3.0,
            "exit_code": 0,
            "stdout": "commands/vdbbench_exact.stdout.txt",
            "stderr": "commands/vdbbench_exact.stderr.txt",
            "skipped": False,
            "skip_reason": None,
        }],
        "route_proof": None,
        "lifecycle_count_proof": "lifecycle_count_response.json",
        "lifecycle_route_proof": "lifecycle_route_response.json",
    }
    manifest["lifecycle"] = {
        "schema_version": harness.LIFECYCLE_SCHEMA,
        "result_status": "completed",
        "file": "lifecycle.jsonl",
        "sha256": harness.sha256_file(lifecycle_path),
        "expected_rows": 50_000,
        "dataset": {
            "name": "cohere-50k",
            "sha256": harness.sha256_file(dataset_path),
            "dimensions": 768,
            "vectors": 50_000,
        },
        "task_config_binding": {
            "result_file": "vdbbench-result.json",
            "result_sha256": harness.sha256_file(result_path),
            "task_config_sha256": task_config_sha256,
        },
        "identity": {
            "gomap_commit": "1" * 40,
            "vectordbbench_commit": "2" * 40,
            "service_binary_sha256": service_binary_sha256,
            "config_sha256": harness.lifecycle_config_sha256(manifest),
        },
        "raw_artifacts": [
            {"path": "profiles/optimize.heap.pprof", "sha256": harness.sha256_file(profile)},
            {
                "path": "lifecycle_route_response.json",
                "sha256": harness.sha256_file(lifecycle_route_response),
            },
            {
                "path": "lifecycle_count_response.json",
                "sha256": harness.sha256_file(lifecycle_count_response),
            },
            {"path": "adapter-lifecycle.jsonl", "sha256": harness.sha256_file(adapter_path)},
            {"path": "diagnostics.jsonl", "sha256": harness.sha256_file(diagnostics_path)},
            {
                "path": "lifecycle_load_milestones.json",
                "sha256": harness.sha256_file(milestone_path),
            },
            {"path": "service.log", "sha256": harness.sha256_file(service_log)},
            {
                "path": "lifecycle-boundary-diagnostics.json",
                "sha256": harness.sha256_file(acknowledgement_path),
            },
        ],
        "profiles": [{
            "path": "profiles/optimize.heap.pprof",
            "sha256": harness.sha256_file(profile),
            "kind": "heap",
            "before_sequence": 8,
            "after_sequence": 9,
        }],
    }
    harness.write_json(root / "manifest.json", manifest)
    return manifest, events


def set_fixture_vdbbench_command(manifest: dict, row: str) -> None:
    subcommand = {
        "exact": "treedbcolumngraphexact",
        "scalar": "treedbscalaru8rerank",
    }[row]
    load_metrics = manifest["vdbbench"][0]["load_metrics"]
    command = [
        "python", "-m", "vectordb_bench.cli.vectordbbench", subcommand,
        "--base-url", manifest["service"]["base_url"],
        "--index-name", load_metrics["index_name"],
        "--timeout", str(manifest["harness"]["client_timeout"]),
        "--m", str(manifest["harness"]["m"]),
        "--ef-construction", str(manifest["harness"]["ef_construction"]),
        "--ef-search", str(manifest["harness"]["ef_search"]),
        "--case-type", manifest["harness"]["case_type"],
        "--k", str(manifest["harness"]["k"]),
        "--num-concurrency", manifest["harness"]["num_concurrency"],
        "--concurrency-duration", str(manifest["harness"]["concurrency_duration"]),
        "--db-label", manifest["harness"]["db_label"],
    ]
    if row == "scalar":
        command.extend([
            "--quantized-index-name", manifest["harness"]["quantized_index_name"],
            "--quantized-rerank-candidates", str(manifest["harness"]["rerank_candidates"]),
        ])
    command_string = shlex.join(command)
    manifest["vdbbench"][0].update(row=row, command=command_string)
    manifest["commands"][0].update(
        name=f"vdbbench_{row}", command=command, command_string=command_string,
    )


def set_fixture_vdbbench_command_tokens(manifest: dict, command: list[str]) -> None:
    command_string = shlex.join(command)
    manifest["vdbbench"][0]["command"] = command_string
    manifest["commands"][0].update(command=command, command_string=command_string)


def rewrite_lifecycle_fixture(root: Path, manifest: dict, events: list[dict]) -> None:
    lifecycle_path = root / "lifecycle.jsonl"
    lifecycle_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in events), encoding="utf-8")
    manifest["lifecycle"]["sha256"] = harness.sha256_file(lifecycle_path)
    harness.write_json(root / "manifest.json", manifest)


class RouteProofSummaryTest(unittest.TestCase):
    def test_iso_now_is_utc(self) -> None:
        self.assertTrue(harness.iso_now().endswith("Z"))

    def test_nanosecond_timestamps_use_one_integer_safe_microsecond_policy(self) -> None:
        second = 1_777_000_000_000_000_000
        for timestamp_ns, expected in (
            (second + 123_456_789, "2026-04-24T03:06:40.123456Z"),
            (second + 999_999_999, "2026-04-24T03:06:40.999999Z"),
            (second + 1_000_000_000, "2026-04-24T03:06:41Z"),
        ):
            with self.subTest(timestamp_ns=timestamp_ns):
                self.assertEqual(harness.iso_from_ns(timestamp_ns), expected)
                self.assertEqual(
                    harness._utc_timestamp(expected, "timestamp", []),
                    harness._datetime_from_ns(timestamp_ns),
                )

    def test_nanosecond_timestamp_supported_extrema_and_overflow(self) -> None:
        minimum_ns = -62_135_596_800_000_000_000
        maximum_ns = 253_402_300_800_000_000_000 - 1
        self.assertEqual(harness.iso_from_ns(minimum_ns), "0001-01-01T00:00:00Z")
        self.assertEqual(harness.iso_from_ns(maximum_ns), "9999-12-31T23:59:59.999999Z")
        for timestamp_ns in (minimum_ns - 1, maximum_ns + 1):
            with self.subTest(timestamp_ns=timestamp_ns), self.assertRaisesRegex(
                ValueError, "outside the supported UTC datetime range"
            ):
                harness.iso_from_ns(timestamp_ns)

    def test_cpu_brand_is_recorded(self) -> None:
        self.assertTrue(harness.cpu_brand())

    def test_scalar_summary_extracts_required_counters(self) -> None:
        response = {
            "metric": "cosine",
            "query_mode": "quantized_rerank",
            "quantized_index_name": "embedding.scalar_u8.fast",
            "quantized_rerank_candidates": 32,
            "no_documents": True,
            "results": [{"id": "101", "ordinal": 0, "score": 1.0}],
            "stats": {
                "documents_fetched": 0,
                "document_bytes": 0,
                "quantized_scorer_active": 1,
                "quantized_score_calls": 7,
                "quantized_rerank_candidates": 4,
                "quantized_rerank_exact_score_calls": 4,
                "search_route_quantized_rerank": 1,
                "score_batch_calls": 1,
                "score_batch_optimized": 1,
            },
            "diagnostics": {"route": "quantized_rerank", "fallback_reason": "none"},
        }

        got = harness.proof_summary(
            "scalar",
            "idx",
            response,
            {"top_k": 2, "query_mode": "quantized_rerank", "quantized_rerank_candidates": 32},
        )

        self.assertEqual(got["route"], "quantized_rerank")
        self.assertEqual(got["fallback_reason"], "none")
        self.assertEqual(got["documents_fetched"], 0)
        self.assertEqual(got["quantized_scorer_active"], 1)
        self.assertEqual(got["quantized_rerank_exact_score_calls"], 4)
        self.assertEqual(got["score_batch_optimized"], 1)
        self.assertEqual(got["score_batch_fallback"], 0)
        self.assertEqual(got["response"]["quantized_rerank_candidates"], 32)

    def test_missing_fallback_reason_normalizes_to_none(self) -> None:
        self.assertEqual(harness.fallback_reason({"diagnostics": {}}), "none")


class ArtifactRootTest(unittest.TestCase):
    def test_prepare_artifact_root_rejects_non_empty_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "old-manifest.json").write_text("{}", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "must be new or empty"):
                harness.prepare_artifact_root(root)


class IsolationEvidenceTest(unittest.TestCase):
    def test_finalize_isolation_aggregates_timestamped_samples(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "isolation.json"
            harness.write_json(path, {
                "schema_version": "treedb-construction-policy-4587-isolation/v2",
                "samples": [],
            })
            monitor = mock.Mock()
            monitor.stop.return_value = [
                {"timestamp": "2026-09-02T00:00:00+00:00", "swap_used_bytes": 0,
                 "competing_processes": []},
                {"timestamp": "2026-09-02T00:01:00+00:00", "swap_used_bytes": 0,
                 "competing_processes": []},
            ]

            harness.finalize_isolation(path, monitor)

            evidence = json.loads(path.read_text())
            self.assertEqual(evidence["coverage_completed_at"], "2026-09-02T00:01:00+00:00")
            self.assertEqual(evidence["peak_swap_used_bytes"], 0)
            self.assertEqual(evidence["competing_processes"], [])

    def test_explicit_exclusive_lock_is_resolved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            args = harness.parse_args([
                "--out", str(Path(tmp) / "artifact"),
                "--exclusive-lock", str(Path(tmp) / "matrix.lock"),
            ])
            self.assertEqual(args.exclusive_lock, (Path(tmp) / "matrix.lock").resolve())


class HostContextTest(unittest.TestCase):
    def test_memory_bytes_uses_portable_sysconf_when_proc_is_unavailable(self) -> None:
        with mock.patch.object(Path, "read_text", side_effect=OSError("no proc")), \
                mock.patch.object(harness.os, "sysconf", side_effect=(4096, 1000)):
            self.assertEqual(harness.memory_bytes(), 4_096_000)

    def test_physical_cpu_count_uses_sysctl_without_logical_fallback(self) -> None:
        with mock.patch.object(Path, "read_text", side_effect=OSError("no proc")), \
                mock.patch.object(harness, "command_output", return_value="8"):
            self.assertEqual(harness.physical_cpu_count(), 8)
        with mock.patch.object(Path, "read_text", side_effect=OSError("no proc")), \
                mock.patch.object(harness, "command_output", return_value="unavailable"), \
                mock.patch.object(harness.os, "cpu_count", return_value=32) as logical_count:
            self.assertIsNone(harness.physical_cpu_count())
        logical_count.assert_not_called()

    def test_storage_context_uses_structured_findmnt_evidence(self) -> None:
        payload = json.dumps({"filesystems": [{
            "source": "/dev/nvme0n1p1", "fstype": "ext4", "target": "/mnt/fast4tb", "size": 4096,
        }]})
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            harness.subprocess,
            "run",
            return_value=subprocess.CompletedProcess([], 0, stdout=payload),
        ):
            got = harness.storage_context(Path(tmp))

        self.assertEqual(got["method"], "findmnt")
        self.assertEqual(got["capacity_bytes"], 4096)
        self.assertTrue(harness.valid_storage_context(got))

    def test_storage_context_falls_back_to_df_and_stat(self) -> None:
        df_output = "Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/disk1 1000 1 999 1% /\n"
        responses = [
            FileNotFoundError("findmnt"),
            subprocess.CompletedProcess([], 0, stdout=df_output),
            subprocess.CompletedProcess([], 0, stdout="apfs\n"),
        ]
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(harness.platform, "system", return_value="Darwin"), \
                mock.patch.object(harness.subprocess, "run", side_effect=responses):
            got = harness.storage_context(Path(tmp))

        self.assertEqual(got["method"], "df-p+stat")
        self.assertEqual(got["device"], "/dev/disk1")
        self.assertEqual(got["filesystem"], "apfs")
        self.assertEqual(got["capacity_bytes"], 1_024_000)

    def test_storage_context_fails_closed_when_discovery_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            harness.subprocess,
            "run",
            side_effect=FileNotFoundError("storage commands unavailable"),
        ):
            self.assertEqual(harness.storage_context(Path(tmp)), {})

    def test_lifecycle_fails_before_build_when_storage_identity_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifact"
            dataset = Path(tmp) / "train.parquet"
            dataset.write_bytes(b"dataset")
            context = {"host": {"memory_bytes": 1024, "physical_cpu_count": 1, "storage": {}}}
            stderr = io.StringIO()
            with mock.patch.object(harness, "collect_context", return_value=context), \
                    mock.patch.object(harness, "build_service") as build, \
                    contextlib.redirect_stderr(stderr):
                exit_code = harness.main([
                    "--out", str(root), "--run-vdbbench", "--rows", "exact",
                    "--case-type", "PerformanceCustomDataset", "--lifecycle",
                    "--lifecycle-dataset-file", str(dataset),
                    "--lifecycle-vectors", "1", "--lifecycle-dimensions", "1",
                ])

        self.assertEqual(exit_code, 2)
        self.assertIn("benchmark storage identity is unavailable", stderr.getvalue())
        build.assert_not_called()

    def test_lifecycle_fails_before_build_when_physical_cpu_count_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifact"
            dataset = Path(tmp) / "train.parquet"
            dataset.write_bytes(b"dataset")
            context = {"host": {"memory_bytes": 1024, "physical_cpu_count": None}}
            stderr = io.StringIO()
            with mock.patch.object(harness, "collect_context", return_value=context), \
                    mock.patch.object(harness, "build_service") as build, \
                    contextlib.redirect_stderr(stderr):
                exit_code = harness.main([
                    "--out", str(root), "--run-vdbbench", "--rows", "exact",
                    "--case-type", "PerformanceCustomDataset", "--lifecycle",
                    "--lifecycle-dataset-file", str(dataset),
                    "--lifecycle-vectors", "1", "--lifecycle-dimensions", "1",
                ])

        self.assertEqual(exit_code, 2)
        self.assertIn("positive physical CPU count is unavailable", stderr.getvalue())
        build.assert_not_called()

    def test_lifecycle_fails_before_build_when_source_is_dirty(self) -> None:
        storage = {
            "path": "/tmp", "method": "findmnt", "device": "/dev/x", "filesystem": "xfs",
            "mount": "/tmp", "capacity_bytes": 1024,
        }
        for dirty_source in ("gomap", "vectordbbench"):
            with self.subTest(dirty_source=dirty_source), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / "artifact"
                dataset = Path(tmp) / "train.parquet"
                dataset.write_bytes(b"dataset")
                context = {
                    "host": {"memory_bytes": 1024, "physical_cpu_count": 1, "storage": storage},
                    "gomap": {"commit": "1" * 40, "dirty": dirty_source == "gomap"},
                    "vectordbbench": {"commit": "2" * 40, "dirty": dirty_source == "vectordbbench"},
                }
                stderr = io.StringIO()
                with mock.patch.object(harness, "collect_context", return_value=context), \
                        mock.patch.object(harness, "build_service") as build, \
                        contextlib.redirect_stderr(stderr):
                    exit_code = harness.main([
                        "--out", str(root), "--run-vdbbench", "--rows", "exact",
                        "--case-type", "PerformanceCustomDataset", "--lifecycle",
                        "--lifecycle-dataset-file", str(dataset),
                        "--lifecycle-vectors", "1", "--lifecycle-dimensions", "1",
                    ])

            self.assertEqual(exit_code, 2)
            self.assertIn("clean source commit identity is unavailable", stderr.getvalue())
            build.assert_not_called()

    def test_lifecycle_fails_before_build_when_memory_size_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifact"
            dataset = Path(tmp) / "train.parquet"
            dataset.write_bytes(b"dataset")
            context = {"host": {"memory_bytes": None}}
            stderr = io.StringIO()
            with mock.patch.object(harness, "collect_context", return_value=context), \
                    mock.patch.object(harness, "build_service") as build, \
                    contextlib.redirect_stderr(stderr):
                exit_code = harness.main([
                    "--out", str(root), "--run-vdbbench", "--rows", "exact",
                    "--case-type", "PerformanceCustomDataset", "--lifecycle",
                    "--lifecycle-dataset-file", str(dataset),
                    "--lifecycle-vectors", "1", "--lifecycle-dimensions", "1",
                ])

        self.assertEqual(exit_code, 2)
        self.assertIn("positive host memory size is unavailable", stderr.getvalue())
        build.assert_not_called()

    def test_lifecycle_requires_loopback_host_but_normal_mode_is_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset = Path(tmp) / "train.parquet"
            dataset.write_bytes(b"dataset")
            lifecycle = [
                "--run-vdbbench", "--rows", "exact", "--lifecycle",
                "--case-type", "PerformanceCustomDataset",
                "--lifecycle-dataset-file", str(dataset),
                "--lifecycle-vectors", "1", "--lifecycle-dimensions", "1",
                "--port", "7120",
            ]
            with mock.patch.object(harness, "find_free_port", return_value=7121):
                for host in ("127.0.0.1", "::1", "localhost"):
                    with self.subTest(host=host):
                        args = harness.parse_args([*lifecycle, "--host", host])
                        self.assertTrue(harness.loopback_host(args.host))
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    harness.parse_args([*lifecycle, "--host", "192.168.1.20"])

            normal = harness.parse_args(["--host", "192.168.1.20", "--port", "7120"])

        self.assertEqual(normal.host, "192.168.1.20")

    def test_lifecycle_selects_distinct_service_and_pprof_ports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset = Path(tmp) / "train.parquet"
            dataset.write_bytes(b"dataset")
            lifecycle = [
                "--run-vdbbench", "--rows", "exact", "--lifecycle",
                "--case-type", "PerformanceCustomDataset",
                "--lifecycle-dataset-file", str(dataset),
                "--lifecycle-vectors", "1", "--lifecycle-dimensions", "1",
            ]
            with mock.patch.object(harness, "find_free_port", side_effect=[7120, 7120, 7121]):
                args = harness.parse_args(lifecycle)

        self.assertEqual(args.port, 7120)
        self.assertEqual(args.pprof_port, 7121)
        self.assertEqual(args.exclusive_lock, harness.LIFECYCLE_EXCLUSIVE_LOCK)
        with tempfile.TemporaryDirectory() as tmp:
            dataset = Path(tmp) / "train.parquet"
            dataset.write_bytes(b"dataset")
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                harness.parse_args([
                    "--run-vdbbench", "--rows", "exact", "--lifecycle",
                    "--case-type", "PerformanceCustomDataset",
                    "--lifecycle-dataset-file", str(dataset),
                    "--lifecycle-vectors", "1", "--lifecycle-dimensions", "1",
                    "--exclusive-lock", str(Path(tmp) / "run-specific.lock"),
                ])


class SmokeShapeTest(unittest.TestCase):
    def test_campaign_shape_is_valid_and_deterministic(self) -> None:
        harness.validate_smoke_shape(768, 256, 100, 192, 150)
        documents = harness.smoke_documents(256, 768)

        self.assertEqual(len(documents), 256)
        self.assertEqual(len(documents[0]["embedding"]), 768)
        self.assertEqual(documents[0], harness.smoke_documents(1, 768)[0])

    def test_rejects_rerank_shorter_than_top_k(self) -> None:
        with self.assertRaisesRegex(ValueError, "rerank candidates"):
            harness.validate_smoke_shape(768, 256, 100, 192, 32)

    def test_parse_args_rejects_invalid_shape_before_service_start(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            harness.parse_args(["--smoke-documents", "256", "--smoke-top-k", "100", "--rerank-candidates", "32"])

    def test_lifecycle_requires_a_vdbbench_search_phase(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset = Path(tmp) / "train.parquet"
            dataset.write_bytes(b"dataset")
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                harness.parse_args([
                    "--run-vdbbench", "--rows", "exact", "--case-type", "PerformanceCustomDataset",
                    "--lifecycle", "--lifecycle-dataset-file", str(dataset),
                    "--lifecycle-vectors", "1", "--lifecycle-dimensions", "1",
                    "--skip-search-serial", "--skip-search-concurrent",
                ])

    def test_lifecycle_rejects_all_harness_owned_options_in_extra_args(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset = Path(tmp) / "train.parquet"
            dataset.write_bytes(b"dataset")
            base = [
                "--run-vdbbench", "--rows", "exact", "--case-type", "PerformanceCustomDataset",
                "--lifecycle", "--lifecycle-dataset-file", str(dataset),
                "--lifecycle-vectors", "1", "--lifecycle-dimensions", "1",
            ]
            for option in sorted(harness.VDBBENCH_OWNED_OPTIONS):
                with self.subTest(option=option), contextlib.redirect_stderr(io.StringIO()), \
                        self.assertRaises(SystemExit):
                    harness.parse_args([*base, f"--vdbbench-extra-args={option}"])
            for argument in ("--base-url=http://other", "--skip-search-serial=true"):
                with self.subTest(argument=argument), contextlib.redirect_stderr(io.StringIO()), \
                        self.assertRaises(SystemExit):
                    harness.parse_args([*base, f"--vdbbench-extra-args={argument}"])

            for option in ("--skip-search-serial", "--skip-search-concurrent"):
                with self.subTest(option=option), contextlib.redirect_stderr(io.StringIO()), \
                        self.assertRaises(SystemExit):
                    harness.parse_args([*base, option])

    def test_lifecycle_preserves_adapter_specific_extra_args(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset = Path(tmp) / "train.parquet"
            dataset.write_bytes(b"dataset")
            args = harness.parse_args([
                "--run-vdbbench", "--rows", "exact", "--case-type", "PerformanceCustomDataset",
                "--lifecycle", "--lifecycle-dataset-file", str(dataset),
                "--lifecycle-vectors", "1", "--lifecycle-dimensions", "1",
                "--vdbbench-extra-args=--adapter-owned=value",
            ])

        self.assertEqual(args.vdbbench_extra_args, "--adapter-owned=value")


class VDBBenchBatchTest(unittest.TestCase):
    def test_vdbbench_rows_receive_default_and_override_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = harness.HarnessState(root=Path(tmp))
            with mock.patch.dict("os.environ", {}, clear=True):
                args = harness.parse_args([])
            default = harness.vdbbench_row_env(args, Path("/vdbbench"), Path("/gomap"), state)
            cli_override_args = harness.parse_args(["--num-per-batch", "500"])
            with mock.patch.dict("os.environ", {"TREEDB_VDBBENCH_NUM_PER_BATCH": "250"}, clear=True):
                override_args = harness.parse_args([])
            override = harness.vdbbench_row_env(override_args, Path("/vdbbench"), Path("/gomap"), state)

        self.assertEqual(default["NUM_PER_BATCH"], "1000")
        self.assertEqual(cli_override_args.num_per_batch, 500)
        self.assertEqual(override["NUM_PER_BATCH"], "250")

    def test_lifecycle_row_receives_boundary_diagnostics_ack_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = harness.HarnessState(root=Path(tmp))
            args = harness.parse_args([
                "--run-vdbbench", "--rows", "exact", "--lifecycle",
                "--case-type", "PerformanceCustomDataset",
                "--lifecycle-dataset-file", __file__,
                "--lifecycle-vectors", "1", "--lifecycle-dimensions", "1",
            ])
            env = harness.vdbbench_row_env(args, Path("/vdbbench"), Path("/gomap"), state)

        self.assertEqual(
            env["TREEDB_LIFECYCLE_BOUNDARY_ACK"],
            str(Path(tmp) / "lifecycle-boundary-diagnostics.json"),
        )

    def test_vdbbench_rows_use_separate_result_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = harness.HarnessState(root=Path(tmp))
            args = harness.parse_args([])
            exact = harness.vdbbench_row_env(args, Path("/vdbbench"), Path("/gomap"), state, "exact")
            scalar = harness.vdbbench_row_env(args, Path("/vdbbench"), Path("/gomap"), state, "scalar")

        self.assertEqual(exact["RESULTS_LOCAL_DIR"], str(Path(tmp) / "vdbbench-results" / "exact"))
        self.assertEqual(scalar["RESULTS_LOCAL_DIR"], str(Path(tmp) / "vdbbench-results" / "scalar"))

    def test_parse_args_rejects_nonpositive_batch_before_service_start(self) -> None:
        for value in ("0", "-1"):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                harness.parse_args(["--num-per-batch", value])

    def test_parse_args_rejects_nonpositive_batch_environment(self) -> None:
        for value in ("0", "-1"):
            with mock.patch.dict("os.environ", {"TREEDB_VDBBENCH_NUM_PER_BATCH": value}, clear=True):
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    harness.parse_args([])


class VDBBenchLoadMetricsTest(unittest.TestCase):
    def test_boundary_capture_reparses_sidecar_only_after_growth(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sidecar = root / "adapter-lifecycle.jsonl"
            acknowledgement = root / "lifecycle-boundary-diagnostics.json"
            sidecar.write_text("{}\n", encoding="utf-8")
            initial_size = sidecar.stat().st_size
            reads = []
            stop = threading.Event()

            def records(_path):
                reads.append(sidecar.stat().st_size)
                if sidecar.stat().st_size == initial_size:
                    return []
                return [{"event": "load_end", "timestamp_ns": 4}]

            sampler = mock.Mock()
            sampler.sample.return_value = {"timestamp_ns": 5, "snapshot": {}}
            errors = []

            def capture():
                try:
                    harness.capture_lifecycle_boundary_diagnostics(
                        sidecar, acknowledgement, sampler, stop
                    )
                except BaseException as exc:
                    errors.append(exc)

            thread = threading.Thread(target=capture)
            with mock.patch.object(harness, "read_adapter_lifecycle_records", side_effect=records):
                thread.start()
                deadline = time.monotonic() + 2
                while not reads and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertEqual(reads, [initial_size])
                time.sleep(0.25)
                self.assertEqual(reads, [initial_size])
                with sidecar.open("a", encoding="utf-8") as stream:
                    stream.write("x")
                deadline = time.monotonic() + 2
                while not acknowledgement.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                stop.set()
                thread.join(timeout=2)
                acknowledged_boundary = json.loads(acknowledgement.read_text())["boundary"]

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(reads, [initial_size, initial_size + 1])
        self.assertEqual(acknowledged_boundary, "load_end")

    def test_lifecycle_run_synchronously_captures_each_fast_load_build_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = root / "train.parquet"
            dataset.write_bytes(b"dataset")
            state = harness.HarnessState(root=root)
            args = harness.parse_args([
                "--out", str(root), "--run-vdbbench", "--rows", "exact", "--lifecycle",
                "--case-type", "PerformanceCustomDataset",
                "--lifecycle-dataset-file", str(dataset),
                "--lifecycle-vectors", "1", "--lifecycle-dimensions", "1",
            ])

            class Sampler:
                samples = [{"timestamp_ns": 1, "snapshot": {"stale": True}}]

                def sample(self, *, boundary=None, boundary_timestamp_ns=None):
                    record = {
                        "timestamp_ns": boundary_timestamp_ns + 1,
                        "snapshot": {"fresh": True},
                        "boundary": boundary,
                        "boundary_timestamp_ns": boundary_timestamp_ns,
                    }
                    self.samples.append(record)
                    return record

            def run(*_args, **_kwargs):
                records = [
                    {"event": "reset", "timestamp_ns": 1, "response": {}},
                    {"event": "load_start", "timestamp_ns": 2},
                    {"event": "batch_accepted", "timestamp_ns": 3, "client_sent": 1, "server_accepted": 1},
                    {"event": "load_end", "timestamp_ns": 4},
                ]
                sidecar = root / "adapter-lifecycle.jsonl"
                acknowledgement = root / "lifecycle-boundary-diagnostics.json"

                def write_records() -> None:
                    sidecar.write_text(
                        "".join(json.dumps(record) + "\n" for record in records), encoding="utf-8"
                    )

                def wait_for_ack(boundary: str, timestamp_ns: int) -> None:
                    deadline = time.monotonic() + 2
                    while time.monotonic() < deadline:
                        if acknowledgement.exists():
                            payload = json.loads(acknowledgement.read_text())
                            if (
                                payload.get("boundary") == boundary
                                and payload.get("boundary_timestamp_ns") == timestamp_ns
                            ):
                                return
                        time.sleep(0.01)
                    self.fail(f"missing acknowledgement for {boundary}")

                write_records()
                wait_for_ack("load_end", 4)
                records.append({"event": "optimize_start", "timestamp_ns": 6})
                write_records()
                wait_for_ack("optimize_start", 6)
                records.append({"event": "optimize_end", "timestamp_ns": 8, "response": {}})
                write_records()
                wait_for_ack("optimize_end", 8)
                records.append({"event": "cache_prime", "timestamp_ns": 10})
                write_records()
                wait_for_ack("cache_prime", 10)
                records.append({"event": "cache_warm", "timestamp_ns": 12})
                write_records()
                wait_for_ack("cache_warm", 12)
                return mock.Mock(command_string="vdbbench exact", exit_code=0)

            with mock.patch.object(harness, "run_command", side_effect=run), \
                    mock.patch.object(harness, "capture_vdbbench_load_metrics", return_value={}):
                harness.run_vdbbench_rows(
                    state,
                    args=args,
                    gomap_root=root,
                    vectordbbench_dir=root,
                    base_url="http://127.0.0.1:1",
                    index_prefix="test",
                    sampler=Sampler(),
                )

            acknowledgement = json.loads((root / "lifecycle-boundary-diagnostics.json").read_text())

        self.assertEqual(acknowledgement, {
            "boundary": "cache_warm",
            "boundary_timestamp_ns": 12,
            "sample_timestamp_ns": 13,
        })

    def test_canonical_result_records_separated_durations_and_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "vdbbench-results" / "TreeDB" / "result_test.json"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps({"run_id": "run-1", "results": [{
                "label": ":)",
                "metrics": {"insert_duration": 2.0, "optimize_duration": 3.0, "load_duration": 5.0, "inserted_count": 0},
                "task_config": {"db_config": {"index_name": "idx"}},
            }]}), encoding="utf-8")

            got = harness.load_metrics_from_result(path, "idx", "Performance1536D50K", root)

        self.assertEqual(got["vector_count"], 50_000)
        self.assertEqual(got["insert_vectors_per_second"], 25_000)
        self.assertEqual(got["throughput_vector_count"], 50_000)
        self.assertIn("sentinel inserted_count=0", got["throughput_vector_count_source"])
        self.assertEqual(got["offline_optimize_duration_seconds"], 3.0)
        self.assertEqual(len(got["result_sha256"]), 64)
        self.assertEqual(got["task_config_sha256"], "04fa505555bec57a85ed8491a57562ac60212fd9ea34871a1f9893622e68d394")

    def test_canonical_result_fails_closed_when_duration_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "result_test.json"
            path.write_text(json.dumps({"results": [{
                "label": ":)",
                "metrics": {"insert_duration": 2.0, "optimize_duration": 3.0, "load_duration": 0.0},
                "task_config": {"db_config": {"index_name": "idx"}},
            }]}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "load_duration"):
                harness.load_metrics_from_result(path, "idx", "Performance1536D50K", root)

    def test_canonical_result_fails_closed_when_duration_overflows_float(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "result_test.json"
            path.write_text(json.dumps({"results": [{
                "label": ":)",
                "metrics": {
                    "insert_duration": 10**400,
                    "optimize_duration": 3.0,
                    "load_duration": 5.0,
                    "inserted_count": 0,
                },
                "task_config": {"db_config": {"index_name": "idx"}},
            }]}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "insert_duration"):
                harness.load_metrics_from_result(path, "idx", "Performance1536D50K", root)

    def test_canonical_result_fails_closed_when_throughput_overflows_float(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "result_test.json"
            path.write_text(json.dumps({"results": [{
                "label": ":)",
                "metrics": {
                    "insert_duration": 2.0,
                    "optimize_duration": 3.0,
                    "load_duration": 5.0,
                    "inserted_count": 0,
                },
                "task_config": {
                    "db_config": {"index_name": "idx"},
                    "case_config": {"custom_case": {"dataset_config": {"size": 10**400}}},
                },
            }]}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "unrepresentable insert throughput"):
                harness.load_metrics_from_result(
                    path, "idx", "PerformanceCustomDataset", root,
                )

    def test_canonical_result_fails_closed_when_total_is_inconsistent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "result_test.json"
            path.write_text(json.dumps({"results": [{
                "label": ":)",
                "metrics": {"insert_duration": 2.0, "optimize_duration": 3.0, "load_duration": 6.0},
                "task_config": {"db_config": {"index_name": "idx"}},
            }]}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "load_duration !="):
                harness.load_metrics_from_result(path, "idx", "Performance1536D50K", root)

    def test_canonical_result_rejects_unsuccessful_case(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "result_test.json"
            path.write_text(json.dumps({"results": [{
                "label": ":(",
                "metrics": {"insert_duration": 2.0, "optimize_duration": 3.0, "load_duration": 5.0},
                "task_config": {"db_config": {"index_name": "idx"}},
            }]}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "did not report success"):
                harness.load_metrics_from_result(path, "idx", "Performance1536D50K", root)

    def test_canonical_result_rejects_partial_insert_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "result_test.json"
            path.write_text(json.dumps({"results": [{
                "label": ":)",
                "metrics": {
                    "insert_duration": 2.0,
                    "optimize_duration": 3.0,
                    "load_duration": 5.0,
                    "inserted_count": 49_999,
                },
                "task_config": {"db_config": {"index_name": "idx"}},
            }]}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "inserted_count 49999 != expected"):
                harness.load_metrics_from_result(path, "idx", "Performance1536D50K", root)

    def test_canonical_result_rejects_malformed_nested_task_config(self) -> None:
        for case_config in (None, [], "bad"):
            with self.subTest(case_config=case_config), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                path = root / "result_test.json"
                path.write_text(json.dumps({"results": [{
                    "label": ":)",
                    "metrics": {
                        "insert_duration": 2.0,
                        "optimize_duration": 3.0,
                        "load_duration": 5.0,
                        "inserted_count": 50_000,
                    },
                    "task_config": {
                        "db_config": {"index_name": "idx"},
                        "case_config": case_config,
                    },
                }]}), encoding="utf-8")

                with self.assertRaisesRegex(ValueError, "case_config"):
                    harness.load_metrics_from_result(
                        path, "idx", "PerformanceCustomDataset", root
                    )

    def test_partial_multirow_run_preserves_completed_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = harness.HarnessState(root=root)
            args = harness.parse_args(["--run-vdbbench"])
            record = mock.Mock(command_string="vdbbench exact", exit_code=0)
            with mock.patch.object(harness, "run_command", side_effect=[record, RuntimeError("second row failed")]), \
                    mock.patch.object(harness, "capture_vdbbench_load_metrics", return_value={"insert_duration_seconds": 1.0}):
                with self.assertRaisesRegex(RuntimeError, "second row failed"):
                    harness.run_vdbbench_rows(
                        state,
                        args=args,
                        gomap_root=root,
                        vectordbbench_dir=root,
                        base_url="http://127.0.0.1:1",
                        index_prefix="test",
                    )

            sidecar = json.loads((root / "vdbbench_load_metrics.json").read_text(encoding="utf-8"))
            self.assertEqual([row["row"] for row in sidecar["rows"]], ["exact"])

    def test_dry_run_does_not_write_load_metrics_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = harness.HarnessState(root=root)
            args = harness.parse_args(["--run-vdbbench", "--vdbbench-dry-run", "--rows", "exact"])
            record = mock.Mock(command_string="vdbbench exact --dry-run", exit_code=0)
            with mock.patch.object(harness, "run_command", return_value=record):
                harness.run_vdbbench_rows(
                    state,
                    args=args,
                    gomap_root=root,
                    vectordbbench_dir=root,
                    base_url="http://127.0.0.1:1",
                    index_prefix="test",
                )

            self.assertFalse((root / "vdbbench_load_metrics.json").exists())

    def test_capture_fails_closed_on_ambiguous_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            results = root / "results"
            results.mkdir()
            (results / "result_one.json").write_text("{}", encoding="utf-8")
            (results / "result_two.json").write_text("{}", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "expected exactly one"):
                harness.capture_vdbbench_load_metrics(results, set(), "idx", "Performance1536D50K", root)

    def test_standard_case_rejects_zero_vector_count(self) -> None:
        with self.assertRaisesRegex(ValueError, "positive vector count"):
            harness.case_vector_count("Performance1536D0K")

    def test_custom_dataset_uses_selected_result_size(self) -> None:
        task_config = {"case_config": {"custom_case": {"dataset_config": {"size": "50000"}}}}

        count, source = harness.result_vector_count(task_config, "PerformanceCustomDataset")

        self.assertEqual(count, 50_000)
        self.assertEqual(source, "task_config.case_config.custom_case.dataset_config.size")

    def test_custom_dataset_rejects_invalid_result_size(self) -> None:
        with self.assertRaisesRegex(ValueError, "PerformanceCustomDataset"):
            harness.result_vector_count({"case_config": {"custom_case": {"dataset_config": {"size": "0"}}}}, "PerformanceCustomDataset")


class ServiceProcessOwnershipTest(unittest.TestCase):
    def assert_startup_failure_cleans_child(
        self, failure: BaseException, *, append_log: bool
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = harness.HarnessState(root=root)
            proc = mock.Mock(pid=1234)
            proc.poll.return_value = None
            proc.wait.return_value = -signal.SIGTERM
            with mock.patch.object(harness.subprocess, "Popen", return_value=proc), \
                    mock.patch.object(harness, "wait_health", side_effect=failure), \
                    mock.patch.object(harness.os, "killpg") as killpg:
                with self.assertRaises(type(failure)):
                    harness.start_service(
                        state,
                        gomap_root=root,
                        service_bin=root / "treedb-document-service",
                        data_dir=root / "treedb-data",
                        host="127.0.0.1",
                        port=9876,
                        profile="command_wal_durable",
                        health_timeout=1.0,
                        append_log=append_log,
                    )

            killpg.assert_called_once_with(proc.pid, signal.SIGTERM)
            proc.wait.assert_called_once_with(timeout=10.0)

    def test_service_command_activates_diagnostics_once_for_initial_and_reopen(self) -> None:
        cases = (
            ("default-initial", False, False),
            ("enabled-initial", True, False),
            ("enabled-reopen", True, True),
        )
        for label, enabled, append_log in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                state = harness.HarnessState(root=root)
                environment = {"GOMAXPROCS": "12"}
                proc = mock.Mock(pid=1234)
                with mock.patch.object(harness.subprocess, "Popen", return_value=proc) as popen, \
                        mock.patch.object(harness, "wait_health", return_value={"ok": True}):
                    _proc, _health, command = harness.start_service(
                        state,
                        gomap_root=root,
                        service_bin=root / "treedb-document-service",
                        data_dir=root / "treedb-data",
                        host="127.0.0.1",
                        port=9876,
                        profile="command_wal_durable",
                        health_timeout=1.0,
                        construction_decision_diagnostics=enabled,
                        pprof_addr="127.0.0.1:6060",
                        append_log=append_log,
                        environment=environment,
                    )

                expected = [
                    str(root / "treedb-document-service"),
                    "-dir", str(root / "treedb-data"),
                    "-addr", "127.0.0.1:9876",
                    "-profile", "command_wal_durable",
                ]
                if enabled:
                    expected.append("-diagnostic-construction-decisions")
                expected.extend(["-pprof", "127.0.0.1:6060"])
                self.assertEqual(command, expected)
                self.assertEqual(command.count("-diagnostic-construction-decisions"), int(enabled))
                self.assertEqual(popen.call_args.args[0], expected)
                self.assertEqual(popen.call_args.kwargs["env"], environment)
                self.assertEqual(state.service_environment, environment)

    def test_lifecycle_vdbbench_environment_is_complete_and_non_inherited(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(
            harness.os.environ,
            {"GOMAXPROCS": "12", "GOGC": "off", "HTTPS_PROXY": "http://proxy",
             "PYTHONPATH": "/unreviewed"},
            clear=False,
        ):
            root = Path(tmp)
            state = harness.HarnessState(root=root)
            args = SimpleNamespace(lifecycle=True, num_per_batch=500)
            environment = harness.vdbbench_row_env(
                args, Path("/vectordbbench"), Path("/gomap"), state, "scalar",
            )

        self.assertEqual(set(environment), {
            "GOMAXPROCS", "PYTHONPATH", "RESULTS_LOCAL_DIR", "LOG_FILE",
            "NUM_PER_BATCH", "TREEDB_LIFECYCLE_SIDECAR",
            "TREEDB_LIFECYCLE_BOUNDARY_ACK",
        })
        self.assertEqual(
            environment["PYTHONPATH"],
            "/vectordbbench:/gomap/clients/python/treedb_client/src",
        )
        self.assertEqual(state.vdbbench_environments, {"scalar": environment})

    def test_construction_decision_diagnostics_cli_defaults_off(self) -> None:
        default = harness.parse_args(["--port", "9876"])
        enabled = harness.parse_args([
            "--port", "9876", "--construction-decision-diagnostics",
        ])

        self.assertFalse(default.construction_decision_diagnostics)
        self.assertTrue(enabled.construction_decision_diagnostics)


    def test_initial_start_interrupt_cleans_owned_child(self) -> None:
        self.assert_startup_failure_cleans_child(KeyboardInterrupt(), append_log=False)

    def test_reopened_start_interrupt_cleans_owned_child(self) -> None:
        self.assert_startup_failure_cleans_child(KeyboardInterrupt(), append_log=True)

    def test_start_service_preserves_ordinary_health_failure_cleanup(self) -> None:
        self.assert_startup_failure_cleans_child(RuntimeError("unhealthy"), append_log=False)


class HarnessOrderTest(unittest.TestCase):
    def test_vdbbench_rows_run_before_route_proof_smoke(self) -> None:
        source = inspect.getsource(harness.main)

        self.assertLess(source.index("run_vdbbench_rows("), source.index("run_route_proof_smoke("))

    def test_lifecycle_startup_means_healthy_observation_boundary(self) -> None:
        source = inspect.getsource(harness.main)

        self.assertLess(source.index("start_service("), source.index("initialize_lifecycle_capture("))

    def test_failure_stops_service_before_hashing_partial_evidence(self) -> None:
        source = inspect.getsource(harness.main)
        failure_path = source[source.index("except Exception as exc:"):]

        self.assertLess(
            failure_path.index("terminate_process_group("),
            failure_path.index("finalize_partial_lifecycle("),
        )

    def test_lifecycle_keyboard_interrupt_writes_analyzable_manifest_and_stops_service(self) -> None:
        storage = {
            "path": "/tmp", "method": "findmnt", "device": "/dev/x", "filesystem": "xfs",
            "mount": "/tmp", "capacity_bytes": 1024,
        }
        context = {
            "host": {
                "memory_bytes": 1024, "logical_cpu_count": 1,
                "physical_cpu_count": 1, "storage": storage,
            },
            "gomap": {"commit": "1" * 40, "dirty": False},
            "vectordbbench": {"commit": "2" * 40, "dirty": False},
        }

        class Sampler:
            def __init__(self, *_args, **_kwargs):
                self.samples = [{"timestamp_ns": time.time_ns(), "snapshot": {}}]

            def start(self):
                return None

            def stop(self):
                return None

            def at(self, _timestamp_ns):
                return {}

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifact"
            dataset = Path(tmp) / "train.parquet"
            binary = Path(tmp) / "treedb-document-service"
            dataset.write_bytes(b"dataset")
            binary.write_bytes(b"binary")
            binary.chmod(0o700)
            proc = mock.Mock(returncode=None)

            def start(_state, *, service_bin, data_dir, host, port, profile, pprof_addr, **_kwargs):
                command = [
                    str(service_bin), "-dir", str(data_dir), "-addr", harness.host_port(host, port),
                    "-profile", profile, "-pprof", pprof_addr,
                ]
                return proc, {}, command

            with mock.patch.object(harness, "collect_context", return_value=context), \
                    mock.patch.object(harness, "build_service", return_value=binary), \
                    mock.patch.object(harness, "start_service", side_effect=start), \
                    mock.patch.object(harness, "DiagnosticsSampler", Sampler), \
                    mock.patch.object(harness, "run_vdbbench_tests", side_effect=KeyboardInterrupt), \
                    mock.patch.object(harness, "terminate_process_group") as terminate, \
                    contextlib.redirect_stderr(io.StringIO()):
                exit_code = harness.main([
                    "--out", str(root), "--run-vdbbench", "--rows", "exact", "--lifecycle",
                    "--case-type", "PerformanceCustomDataset", "--run-tests", "required",
                    "--lifecycle-dataset-file", str(dataset),
                    "--lifecycle-vectors", "1", "--lifecycle-dimensions", "1",
                ])

            report = harness.validate_lifecycle_artifact(root)
            manifest = json.loads((root / "manifest.json").read_text())

        self.assertEqual(exit_code, 130)
        self.assertEqual(manifest["lifecycle"]["result_status"], "interrupted")
        self.assertTrue(report["analyzable"], report)
        self.assertFalse(report["complete"])
        terminate.assert_called_once_with(proc, graceful_timeout=300.0)

    def test_route_proof_can_be_skipped_for_measurement_only_runs(self) -> None:
        args = harness.parse_args(["--run-vdbbench", "--skip-route-proof"])

        self.assertTrue(args.skip_route_proof)

    def test_route_proof_skip_requires_real_load_evidence(self) -> None:
        invalid = [
            ["--skip-route-proof"],
            ["--run-vdbbench", "--skip-route-proof", "--vdbbench-dry-run"],
            ["--run-vdbbench", "--skip-route-proof", "--skip-load"],
            ["--run-vdbbench", "--skip-route-proof", "--rows", ""],
        ]
        for argv in invalid:
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                harness.parse_args(argv)


class LifecycleValidatorTest(unittest.TestCase):
    def test_complete_fixture_passes_and_reconstructs_t_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lifecycle_fixture(root)

            got = harness.validate_lifecycle_artifact(root)

        self.assertTrue(got["analyzable"])
        self.assertTrue(got["complete"])
        self.assertEqual(got["last_stage"], "teardown")
        self.assertEqual(got["counts"], {
            "client_sent": 50_000,
            "server_accepted": 50_000,
            "server_durable": 50_000,
            "reopened": 50_000,
        })
        self.assertEqual(got["t_ready_seconds"], 8.0)

    def test_lifecycle_manifest_rejects_generic_smoke_route_claim(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _events = lifecycle_fixture(root)
            manifest["harness"]["mode"] = "vdbbench+smoke"
            manifest["route_proof"] = "route_proof.json"
            manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("vdbbench+lifecycle" in error for error in got["errors"]), got)
        self.assertTrue(any("independent route_proof.json" in error for error in got["errors"]), got)

    def test_completed_lifecycle_rejects_vdbbench_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _events = lifecycle_fixture(root)
            manifest["harness"]["vdbbench_dry_run"] = True
            manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertTrue(got["analyzable"], got)
        self.assertFalse(got["complete"], got)
        self.assertIn("completed lifecycle requires vdbbench_dry_run=false", got["completion_errors"])

    def test_bound_vdbbench_row_must_match_lifecycle_route_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _events = lifecycle_fixture(root)
            manifest["vdbbench"][0]["row"] = "scalar"
            manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("VDBBench row" in error for error in got["errors"]), got)

    def test_bound_vdbbench_command_must_match_lifecycle_row(self) -> None:
        commands = (
            "python -m vectordb_bench.cli.vectordbbench treedbscalaru8rerank",
            "echo -m vectordb_bench.cli.vectordbbench treedbcolumngraphexact",
            "-m vectordb_bench.cli.vectordbbench treedbcolumngraphexact",
            (
                "python -m vectordb_bench.cli.vectordbbench treedbcolumngraphexact "
                "--dry-run"
            ),
            (
                "python -m vectordb_bench.cli.vectordbbench treedbcolumngraphexact "
                "--skip-search-concurrent=true"
            ),
        )
        for command in commands:
            with self.subTest(command=command), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                manifest["vdbbench"][0]["command"] = command
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any("VDBBench command" in error for error in got["errors"]), got)

    def test_bound_vdbbench_execution_must_record_zero_exit_code(self) -> None:
        for exit_code in (None, 1, True, "0"):
            with self.subTest(exit_code=exit_code), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                if exit_code is None:
                    manifest["vdbbench"][0].pop("exit_code")
                else:
                    manifest["vdbbench"][0]["exit_code"] = exit_code
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any("exit_code=0" in error for error in got["errors"]), got)

    def test_bound_vdbbench_row_must_match_harness_batch_size(self) -> None:
        for value in (None, 999, True):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                if value is None:
                    manifest["vdbbench"][0].pop("num_per_batch")
                else:
                    manifest["vdbbench"][0]["num_per_batch"] = value
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any("num_per_batch" in error for error in got["errors"]), got)

    def test_declared_batch_size_must_match_adapter_batch_distribution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _events = lifecycle_fixture(root)
            manifest["harness"]["num_per_batch"] = 999
            manifest["vdbbench"][0]["num_per_batch"] = 999
            manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("batch sizes" in error for error in got["errors"]), got)

    def test_adapter_remainder_batch_must_be_last(self) -> None:
        self.assertTrue(harness._batch_distribution_matches([500, 500, 1], 1001, 500))
        self.assertFalse(harness._batch_distribution_matches([500, 1, 500], 1001, 500))
        self.assertTrue(harness._batch_distribution_matches([500, 500], 1000, 500))

    def test_bound_vdbbench_command_options_must_match_manifest_exactly_once(self) -> None:
        options = (
            "--base-url", "--index-name", "--timeout", "--m",
            "--ef-construction", "--ef-search", "--case-type", "--k",
            "--num-concurrency", "--concurrency-duration", "--db-label",
        )
        for option in options:
            with self.subTest(option=option), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                command = list(manifest["commands"][0]["command"])
                command[command.index(option) + 1] = "mismatched-value"
                set_fixture_vdbbench_command_tokens(manifest, command)
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any(option in error for error in got["errors"]), got)

    def test_bound_vdbbench_command_rejects_option_termination_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _events = lifecycle_fixture(root)
            command = list(manifest["commands"][0]["command"])
            subcommand = command.index("treedbcolumngraphexact")
            command.insert(subcommand + 1, "--")
            set_fixture_vdbbench_command_tokens(manifest, command)
            manifest["lifecycle"]["identity"]["config_sha256"] = (
                harness.lifecycle_config_sha256(manifest)
            )
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("terminate option parsing" in error for error in got["errors"]), got)

    def test_bound_vdbbench_command_rejects_eager_exit_flags(self) -> None:
        for flag in ("--help", "-h", "--version"):
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                command = [*manifest["commands"][0]["command"], flag]
                set_fixture_vdbbench_command_tokens(manifest, command)
                manifest["lifecycle"]["identity"]["config_sha256"] = (
                    harness.lifecycle_config_sha256(manifest)
                )
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any("VDBBench command" in error for error in got["errors"]), got)

    def test_bound_vdbbench_typed_options_reject_unproducible_values(self) -> None:
        for key, option in (
            ("client_timeout", "--timeout"),
            ("concurrency_duration", "--concurrency-duration"),
        ):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                manifest["harness"][key] = "garbage"
                command = list(manifest["commands"][0]["command"])
                command[command.index(option) + 1] = "garbage"
                set_fixture_vdbbench_command_tokens(manifest, command)
                manifest["lifecycle"]["identity"]["config_sha256"] = (
                    harness.lifecycle_config_sha256(manifest)
                )
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any(key in error for error in got["errors"]), got)

    def test_client_timeout_overflow_is_structured_cli_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _events = lifecycle_fixture(root)
            oversized = 10 ** 400
            manifest["harness"]["client_timeout"] = oversized
            command = list(manifest["commands"][0]["command"])
            command[command.index("--timeout") + 1] = str(oversized)
            set_fixture_vdbbench_command_tokens(manifest, command)
            manifest["lifecycle"]["identity"]["config_sha256"] = (
                harness.lifecycle_config_sha256(manifest)
            )
            harness.write_json(root / "manifest.json", manifest)

            report = harness.validate_lifecycle_artifact(root)
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                exit_code = harness.main(["--validate-lifecycle", str(root), "--allow-partial"])

        self.assertFalse(report["analyzable"], report)
        self.assertTrue(any("client_timeout" in error for error in report["errors"]), report)
        self.assertEqual(exit_code, 1)

    def test_authoritative_vdbbench_command_cwd_matches_recorded_checkout(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _events = lifecycle_fixture(root)
            other_checkout = root / "other-vectordbbench"
            other_checkout.mkdir()
            manifest["commands"][0]["cwd"] = str(other_checkout)
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("command cwd" in error for error in got["errors"]), got)

    def test_storage_evidence_path_matches_artifact_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _events = lifecycle_fixture(root)
            unrelated_storage = root / "other-storage"
            unrelated_storage.mkdir()
            manifest["context"]["host"]["storage"].update({
                "path": str(unrelated_storage),
                "mount": str(unrelated_storage),
                "device": "/dev/other",
            })
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("storage.path" in error for error in got["errors"]), got)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _events = lifecycle_fixture(root)
            command = list(manifest["commands"][0]["command"])
            command.extend(["--k", "99"])
            set_fixture_vdbbench_command_tokens(manifest, command)
            harness.write_json(root / "manifest.json", manifest)

            duplicate = harness.validate_lifecycle_artifact(root)

        self.assertFalse(duplicate["analyzable"], duplicate)
        self.assertTrue(any("--k" in error for error in duplicate["errors"]), duplicate)

    def test_vdbbench_base_url_must_match_effective_service_address(self) -> None:
        for mutation in ("different", "omitted"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                if mutation == "different":
                    manifest["service"]["base_url"] = "http://127.0.0.1:9999"
                else:
                    addr = manifest["service"]["command"].index("-addr")
                    del manifest["service"]["command"][addr:addr + 2]
                set_fixture_vdbbench_command(manifest, "exact")
                manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any("base_url" in error and "-addr" in error for error in got["errors"]), got)

    def test_scalar_command_options_are_bound_to_quantized_manifest_values(self) -> None:
        for option in ("--quantized-index-name", "--quantized-rerank-candidates"):
            with self.subTest(option=option), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                manifest["harness"]["rows"] = "scalar"
                set_fixture_vdbbench_command(manifest, "scalar")
                command = list(manifest["commands"][0]["command"])
                command[command.index(option) + 1] = "mismatched-value"
                set_fixture_vdbbench_command_tokens(manifest, command)
                manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any(option in error for error in got["errors"]), got)

    def test_completed_lifecycle_requires_matching_authoritative_vdbbench_command(self) -> None:
        mutations = (
            ("missing", lambda manifest: manifest.pop("commands"), "manifest.commands"),
            ("duplicate", lambda manifest: manifest["commands"].append(dict(manifest["commands"][0])), "one authoritative"),
            ("argv", lambda manifest: manifest["commands"][0]["command"].append("--extra"), "does not match"),
            ("string", lambda manifest: manifest["commands"][0].__setitem__("command_string", "other"), "does not match"),
            ("nonzero", lambda manifest: manifest["commands"][0].__setitem__("exit_code", 1), "exit_code=0"),
            ("skipped", lambda manifest: manifest["commands"][0].__setitem__("skipped", True), "does not match"),
        )
        for label, mutation, expected in mutations:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                mutation(manifest)
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any(expected in error for error in got["errors"]), got)

    def test_lifecycle_route_response_must_match_route_verify(self) -> None:
        mutations = (
            (lambda response: response["diagnostics"].__setitem__("fallback_reason", "exact_scan"), "fallback"),
            (lambda response: response["diagnostics"].pop("fallback_reason"), "diagnostics"),
            (lambda response: response.__setitem__("no_documents", False), "no-document"),
            (lambda response: response.__setitem__("results", ["bad"]), "results"),
            (lambda response: response.__setitem__("results", [{}, {"id": "2"}]), "results"),
            (lambda response: response.__setitem__("results", [{"id": ""}, {"id": "2"}]), "results"),
            (lambda response: response.__setitem__("results", [{"id": 1}, {"id": "2"}]), "results"),
            (lambda response: response.__setitem__("results", [{"id": "1"}, {"id": "1"}]), "results"),
            (lambda response: response.__setitem__("results", []), "result count"),
            (lambda response: response.__setitem__("results", [{"id": "1"}]), "result count"),
            (lambda response: response.__setitem__("stats", []), "stats"),
            (lambda response: response.__setitem__("stats", "malformed"), "stats"),
            (lambda response: response["stats"].__setitem__("documents_fetched", 1), "guardrails"),
            (lambda response: response["stats"].__setitem__("document_bytes", 1), "guardrails"),
            (lambda response: response["stats"].__setitem__("document_output_bytes", 1), "guardrails"),
            (
                lambda response: response["stats"].__setitem__(
                    "search_route_hnsw_search_pack", 1e309
                ),
                "non-finite",
            ),
            (
                lambda response: response["stats"].__setitem__(
                    "search_route_hnsw_search_pack", "9" * 5_000
                ),
                "supported integer range",
            ),
            (
                lambda response: response["diagnostics"].__setitem__(
                    "no_document_guardrails_ok", False
                ),
                "guardrails",
            ),
            (lambda response: response["index"].__setitem__("generation", 8), "generation"),
            (lambda response: response["index"].__setitem__("name", "other-index"), "index identity"),
            (lambda response: response.__setitem__("vector_index_name", "other-vector"), "index identity"),
            (lambda response: response.__setitem__("query_mode", "quantized_rerank"), "search configuration"),
            (lambda response: response.__setitem__("quantized_index_name", "other-quantized"), "search configuration"),
            (lambda response: response.__setitem__("quantized_rerank_candidates", 64), "search configuration"),
            (lambda response: response.__setitem__("request_ef_search", 101), "ef_search"),
            (lambda response: response["diagnostics"].__setitem__("route", "quantized_rerank"), "route"),
        )
        for mutation, expected in mutations:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                response_path = root / "lifecycle_route_response.json"
                response = json.loads(response_path.read_text(encoding="utf-8"))
                mutation(response)
                harness.write_json(response_path, response)
                manifest["lifecycle"]["raw_artifacts"][1]["sha256"] = harness.sha256_file(response_path)
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["analyzable"], got)
                self.assertTrue(any(expected in error for error in got["errors"]), got)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _events = lifecycle_fixture(root)
            response_path = root / "lifecycle_route_response.json"
            response_path.write_text("{", encoding="utf-8")
            manifest["lifecycle"]["raw_artifacts"][1]["sha256"] = harness.sha256_file(response_path)
            harness.write_json(root / "manifest.json", manifest)

            malformed = harness.validate_lifecycle_artifact(root)

        self.assertFalse(malformed["analyzable"], malformed)
        self.assertTrue(any("cannot parse lifecycle route response" in error for error in malformed["errors"]), malformed)

    def test_lifecycle_count_response_must_prove_reopened_rows_and_index(self) -> None:
        mutations = (
            (lambda response: response.__setitem__("count", 49_999), "expected reopened rows"),
            (lambda response: response["index"].__setitem__("generation", 8), "expected reopened rows"),
            (lambda response: response["index"].__setitem__("name", "other-index"), "index identity"),
            (lambda response: response["index"].__setitem__("vector_index_name", "other-vector"), "index identity"),
        )
        for mutation, expected in mutations:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                response_path = root / "lifecycle_count_response.json"
                response = json.loads(response_path.read_text(encoding="utf-8"))
                mutation(response)
                harness.write_json(response_path, response)
                next(
                    artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                    if artifact["path"] == "lifecycle_count_response.json"
                )["sha256"] = harness.sha256_file(response_path)
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any(expected in error for error in got["errors"]), got)

    def test_reopened_generation_must_match_optimized_column_graph(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            events[12]["state"]["route"]["service_generation"] = 8
            for relative in ("lifecycle_route_response.json", "lifecycle_count_response.json"):
                response_path = root / relative
                response = json.loads(response_path.read_text(encoding="utf-8"))
                response["index"]["generation"] = 8
                harness.write_json(response_path, response)
                next(
                    artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                    if artifact["path"] == relative
                )["sha256"] = harness.sha256_file(response_path)
            rewrite_lifecycle_fixture(root, manifest, events)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("optimized column graph" in error for error in got["errors"]), got)

    def test_scalar_route_configuration_normalizes_row_and_uses_harness_k(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            manifest["harness"].update(rows=" Scalar ", k=4, rerank_candidates=1)
            set_fixture_vdbbench_command(manifest, "scalar")
            events[12]["state"]["route"]["name"] = "quantized_rerank"
            events[12]["state"]["route"]["requested_top_k"] = 4
            events[12]["state"]["route"]["result_count"] = 4
            response_path = root / "lifecycle_route_response.json"
            response = json.loads(response_path.read_text(encoding="utf-8"))
            response.update({
                "query_mode": "quantized_rerank",
                "quantized_index_name": manifest["harness"]["quantized_index_name"],
                "quantized_rerank_candidates": 4,
                "results": [{"id": str(value)} for value in range(1, 5)],
            })
            response["diagnostics"]["route"] = "quantized_rerank"
            harness.write_json(response_path, response)
            next(
                artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                if artifact["path"] == "lifecycle_route_response.json"
            )["sha256"] = harness.sha256_file(response_path)
            manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
            rewrite_lifecycle_fixture(root, manifest, events)

            got = harness.validate_lifecycle_artifact(root)

        self.assertTrue(got["complete"], got)

    def test_route_requested_top_k_must_match_harness_k(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _events = lifecycle_fixture(root)
            manifest["harness"]["k"] = 4
            manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("requested_top_k" in error for error in got["errors"]), got)

    def test_route_effective_ef_search_must_match_harness(self) -> None:
        for value in (None, 101):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                if value is None:
                    manifest["harness"].pop("ef_search")
                else:
                    manifest["harness"]["ef_search"] = value
                manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any("ef_search" in error for error in got["errors"]), got)

    def test_route_name_must_match_selected_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            events[12]["state"]["route"]["name"] = "quantized_rerank"
            response_path = root / "lifecycle_route_response.json"
            response = json.loads(response_path.read_text(encoding="utf-8"))
            response["diagnostics"]["route"] = "quantized_rerank"
            harness.write_json(response_path, response)
            next(
                artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                if artifact["path"] == "lifecycle_route_response.json"
            )["sha256"] = harness.sha256_file(response_path)
            rewrite_lifecycle_fixture(root, manifest, events)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("search configuration" in error for error in got["errors"]), got)

    def test_invalid_expected_rows_fails_structurally_without_top_k_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _events = lifecycle_fixture(root)
            manifest["lifecycle"]["expected_rows"] = None
            harness.write_json(root / "manifest.json", manifest)
            output = io.StringIO()

            got = harness.validate_lifecycle_artifact(root)
            with contextlib.redirect_stdout(output):
                exit_code = harness.main(["--validate-lifecycle", str(root)])

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("expected_rows" in error for error in got["errors"]), got)
        self.assertEqual(exit_code, 1)

    def test_manifest_vdbbench_must_be_a_list(self) -> None:
        for invalid in (None, {"row": "exact"}):
            with self.subTest(invalid=invalid), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                manifest["vdbbench"] = invalid
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any("manifest.vdbbench must be a list" in item for item in got["errors"]), got)

    def test_bound_index_name_must_match_canonical_task_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _events = lifecycle_fixture(root)
            manifest["vdbbench"][0]["load_metrics"]["index_name"] = "other-index"
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("canonical task_config" in error for error in got["errors"]), got)

    def test_completed_standard_case_without_dataset_binding_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _events = lifecycle_fixture(root)
            manifest["harness"]["case_type"] = "Performance768D50K"
            manifest["vdbbench"] = []
            manifest["lifecycle"].pop("task_config_binding")
            manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("PerformanceCustomDataset" in error for error in got["errors"]), got)

    def test_canonical_vdbbench_result_must_report_successful_load(self) -> None:
        mutations = (
            lambda row: row.__setitem__("label", ":("),
            lambda row: row.__setitem__("metrics", "bad"),
            lambda row: row["metrics"].__setitem__("insert_duration", 10**400),
            lambda row: row["task_config"].__setitem__("case_config", None),
        )
        for mutate in mutations:
            with self.subTest(mutate=mutate), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                result_path = root / "vdbbench-result.json"
                result = json.loads(result_path.read_text(encoding="utf-8"))
                mutate(result["results"][0])
                harness.write_json(result_path, result)
                result_sha = harness.sha256_file(result_path)
                manifest["vdbbench"][0]["load_metrics"]["result_sha256"] = result_sha
                manifest["lifecycle"]["task_config_binding"]["result_sha256"] = result_sha
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any("unsuccessful or invalid" in error for error in got["errors"]), got)

    def test_manifest_storage_requires_meaningful_identity_and_capacity(self) -> None:
        invalid_storage = (
            {"mount": "unavailable: findmnt missing"},
            {"path": "/tmp", "method": "findmnt", "device": "/dev/x", "filesystem": "xfs", "mount": "/tmp", "capacity_bytes": 0},
            {"method": {}},
            {"method": []},
        )
        for storage in invalid_storage:
            with self.subTest(storage=storage), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                manifest["context"]["host"]["storage"] = storage
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any("positive capacity" in item for item in got["errors"]), got)

    def test_custom_case_binds_canonical_task_config_and_dataset_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _events = lifecycle_fixture(root)
            dataset_dir = root / "cohere-50k"
            dataset_dir.mkdir()
            dataset_file = dataset_dir / "train.parquet"
            dataset_file.write_bytes(b"cohere vectors")
            task_config = {
                "db_config": {"index_name": "index-a"},
                "case_config": {"custom_case": {"dataset_config": {
                    "dir": str(dataset_dir), "size": 50000, "dim": 768,
                    "file_count": "1", "use_shuffled": False,
                }}},
            }
            result = root / "vdbbench-results" / "result.json"
            result.parent.mkdir()
            def write_successful_result() -> None:
                harness.write_json(result, {
                    "run_id": "custom-fixture",
                    "results": [{
                        "label": ":)",
                        "task_config": task_config,
                        "metrics": {
                            "inserted_count": 50_000,
                            "insert_duration": 1.0,
                            "optimize_duration": 2.0,
                            "load_duration": 3.0,
                        },
                    }],
                })

            write_successful_result()
            metrics = harness.load_metrics_from_result(
                result, "index-a", "PerformanceCustomDataset", root
            )
            manifest["harness"]["case_type"] = "PerformanceCustomDataset"
            manifest["vdbbench"][0]["load_metrics"] = metrics
            set_fixture_vdbbench_command(manifest, "exact")
            manifest["lifecycle"]["dataset"]["sha256"] = harness.sha256_file(dataset_file)
            manifest["lifecycle"]["task_config_binding"] = {
                key: metrics[key] for key in ("result_file", "result_sha256", "task_config_sha256")
            }
            manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)
            self.assertTrue(got["complete"], got)

            sidecar = root / "adapter-lifecycle.jsonl"
            records = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]
            records[-3]["response"]["index"]["name"] = "other"
            sidecar.write_text(
                "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
                encoding="utf-8",
            )
            sidecar_artifact = next(
                artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                if artifact["path"] == "adapter-lifecycle.jsonl"
            )
            sidecar_artifact["sha256"] = harness.sha256_file(sidecar)
            for event in _events:
                index = event["state"].get("index")
                if isinstance(index, dict):
                    index["identity"] = "other:vector_hnsw"
                route = event["state"].get("route")
                if isinstance(route, dict):
                    route["index_identity"] = "other:vector_hnsw"
            rewrite_lifecycle_fixture(root, manifest, _events)

            renamed = harness.validate_lifecycle_artifact(root)
            self.assertFalse(renamed["analyzable"], renamed)
            self.assertTrue(
                any("bound VDBBench result" in error for error in renamed["errors"]), renamed
            )

            records[-3]["response"]["index"]["name"] = "index-a"
            sidecar.write_text(
                "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
                encoding="utf-8",
            )
            sidecar_artifact["sha256"] = harness.sha256_file(sidecar)
            for event in _events:
                index = event["state"].get("index")
                if isinstance(index, dict):
                    index["identity"] = "index-a:vector_hnsw"
                route = event["state"].get("route")
                if isinstance(route, dict):
                    route["index_identity"] = "index-a:vector_hnsw"
            rewrite_lifecycle_fixture(root, manifest, _events)

            for malformed_results in (None, {"task_config": task_config}):
                with self.subTest(malformed_results=malformed_results):
                    result.write_text(json.dumps({"results": malformed_results}), encoding="utf-8")
                    metrics["result_sha256"] = harness.sha256_file(result)
                    manifest["lifecycle"]["task_config_binding"]["result_sha256"] = metrics["result_sha256"]
                    harness.write_json(root / "manifest.json", manifest)

                    malformed = harness.validate_lifecycle_artifact(root)

                    self.assertFalse(malformed["analyzable"], malformed)
                    self.assertTrue(any("results must be a list" in error for error in malformed["errors"]), malformed)

            task_config["case_config"]["custom_case"]["dataset_config"]["dim"] = 769
            write_successful_result()
            metrics = harness.load_metrics_from_result(
                result, "index-a", "PerformanceCustomDataset", root
            )
            manifest["vdbbench"] = [{
                "row": "exact",
                "command": "python -m vectordb_bench.cli.vectordbbench treedbcolumngraphexact",
                "exit_code": 0,
                "load_metrics": metrics,
            }]
            manifest["lifecycle"]["task_config_binding"] = {
                key: metrics[key] for key in ("result_file", "result_sha256", "task_config_sha256")
            }
            harness.write_json(root / "manifest.json", manifest)

            rejected = harness.validate_lifecycle_artifact(root)
            self.assertFalse(rejected["complete"])
            self.assertTrue(any("shape" in error for error in rejected["errors"]), rejected)

    def test_custom_task_config_nested_values_must_be_objects(self) -> None:
        mutations = (
            lambda config: config.__setitem__("case_config", []),
            lambda config: config["case_config"].__setitem__("custom_case", "bad"),
            lambda config: config["case_config"]["custom_case"].__setitem__("dataset_config", 42),
        )
        for mutate in mutations:
            with self.subTest(mutate=mutate), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                task_config = {"case_config": {"custom_case": {"dataset_config": {
                    "dir": str(root), "size": 50000, "dim": 768,
                    "file_count": "1", "use_shuffled": False,
                }}}}
                mutate(task_config)
                result = root / "vdbbench-results" / "result.json"
                result.parent.mkdir()
                result.write_text(json.dumps({"results": [{"task_config": task_config}]}), encoding="utf-8")
                metrics = {
                    "result_file": str(result.relative_to(root)),
                    "result_sha256": harness.sha256_file(result),
                    "task_config": task_config,
                    "task_config_sha256": harness.canonical_sha256(task_config),
                }
                manifest["harness"]["case_type"] = "PerformanceCustomDataset"
                manifest["vdbbench"] = [{"row": "exact", "load_metrics": metrics}]
                manifest["lifecycle"]["task_config_binding"] = {
                    key: metrics[key] for key in ("result_file", "result_sha256", "task_config_sha256")
                }
                manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any("must be an object" in error for error in got["errors"]), got)

    def test_completed_fixture_requires_positive_t_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            for event in events:
                event["timestamp"] = "2026-08-27T00:00:00Z"
            rewrite_lifecycle_fixture(root, manifest, events)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"])
        self.assertFalse(got["complete"])
        self.assertEqual(got["t_ready_seconds"], 0.0)
        self.assertIn("T_ready must be strictly positive", got["completion_errors"])
        self.assertTrue(any("does not match adapter" in item for item in got["errors"]), got)

    def test_completed_fixture_requires_checksum_bound_lifecycle_evidence(self) -> None:
        required = (
            "adapter-lifecycle.jsonl",
            "diagnostics.jsonl",
            "lifecycle-boundary-diagnostics.json",
            "lifecycle_load_milestones.json",
            "service.log",
        )
        for missing in required:
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _ = lifecycle_fixture(root)
                manifest["lifecycle"]["raw_artifacts"] = [
                    artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                    if artifact["path"] != missing
                ]
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertIn(
                f"completed lifecycle requires checksum-bound raw artifact {missing}", got["errors"]
            )

    def test_completed_fixture_rejects_invented_or_equal_cache_boundaries(self) -> None:
        for mutation in ("invented", "equal"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _ = lifecycle_fixture(root)
                sidecar = root / "adapter-lifecycle.jsonl"
                records = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]
                if mutation == "invented":
                    records[-2]["timestamp_ns"] += 500_000_000
                else:
                    records[-2]["timestamp_ns"] = records[-3]["timestamp_ns"]
                sidecar.write_text(
                    "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
                    encoding="utf-8",
                )
                next(
                    artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                    if artifact["path"] == "adapter-lifecycle.jsonl"
                )["sha256"] = harness.sha256_file(sidecar)
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any(
                "adapter lifecycle" in item or "timestamp does not match" in item
                for item in got["errors"]
            ), got)

    def test_completed_fixture_rejects_mismatched_or_unbound_diagnostics(self) -> None:
        for mutation in ("mismatched", "checksum"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _ = lifecycle_fixture(root)
                diagnostics = root / "diagnostics.jsonl"
                records = [json.loads(line) for line in diagnostics.read_text(encoding="utf-8").splitlines()]
                records[-1]["boundary_timestamp_ns"] += 1
                diagnostics.write_text(
                    "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
                    encoding="utf-8",
                )
                if mutation == "mismatched":
                    next(
                        artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                        if artifact["path"] == "diagnostics.jsonl"
                    )["sha256"] = harness.sha256_file(diagnostics)
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            expected = "matching tagged diagnostics" if mutation == "mismatched" else "checksum mismatch"
            self.assertTrue(any(expected in item for item in got["errors"]), got)

    def test_completed_fixture_binds_stage_state_to_tagged_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _ = lifecycle_fixture(root)
            diagnostics = root / "diagnostics.jsonl"
            records = [json.loads(line) for line in diagnostics.read_text(encoding="utf-8").splitlines()]
            records[0]["snapshot"]["database"]["treedb.command_wal.durable_wal_lsn"] = 0
            diagnostics.write_text(
                "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
                encoding="utf-8",
            )
            next(
                artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                if artifact["path"] == "diagnostics.jsonl"
            )["sha256"] = harness.sha256_file(diagnostics)
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertIn(
            "stage load_end wal does not match its tagged diagnostics snapshot",
            got["errors"],
        )

    def test_completed_fixture_binds_raw_optimize_response_to_index(self) -> None:
        for mutation, expected in (
            (lambda response: response.clear(), "index name"),
            (lambda response: response["index"].__setitem__("generation", 8), "does not match"),
            (lambda response: response["index"].__setitem__("name", "other"), "does not match"),
        ):
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _ = lifecycle_fixture(root)
                sidecar = root / "adapter-lifecycle.jsonl"
                records = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]
                mutation(records[-3]["response"])
                sidecar.write_text(
                    "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
                    encoding="utf-8",
                )
                next(
                    artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                    if artifact["path"] == "adapter-lifecycle.jsonl"
                )["sha256"] = harness.sha256_file(sidecar)
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any(expected in error for error in got["errors"]), got)

    def test_completed_fixture_binds_build_parameters_to_raw_optimize_response(self) -> None:
        for key, value in (("m", 17), ("ef_construction", 129)):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                manifest["harness"][key] = value
                manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any("build parameters" in error for error in got["errors"]), got)

        for harness_key, response_key in (
            ("m", "vector_m"),
            ("ef_construction", "vector_ef_construction"),
        ):
            with self.subTest(boolean_echo=response_key), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _events = lifecycle_fixture(root)
                manifest["harness"][harness_key] = 1
                sidecar = root / "adapter-lifecycle.jsonl"
                records = [
                    json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()
                ]
                records[-3]["response"]["index"][response_key] = True
                sidecar.write_text(
                    "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
                    encoding="utf-8",
                )
                next(
                    artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                    if artifact["path"] == "adapter-lifecycle.jsonl"
                )["sha256"] = harness.sha256_file(sidecar)
                manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any("build parameters" in error for error in got["errors"]), got)

    def test_completed_fixture_malformed_boundary_state_is_structured_error(self) -> None:
        for mutation, expected in (("snapshot", "snapshot must be an object"), ("rows", "state must contain")):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, events = lifecycle_fixture(root)
                if mutation == "snapshot":
                    diagnostics = root / "diagnostics.jsonl"
                    records = [json.loads(line) for line in diagnostics.read_text(encoding="utf-8").splitlines()]
                    records[0]["snapshot"] = []
                    diagnostics.write_text(
                        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
                        encoding="utf-8",
                    )
                    next(
                        artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                        if artifact["path"] == "diagnostics.jsonl"
                    )["sha256"] = harness.sha256_file(diagnostics)
                    harness.write_json(root / "manifest.json", manifest)
                else:
                    events[harness.LIFECYCLE_STAGES.index("load_end")]["state"]["rows"] = []
                    rewrite_lifecycle_fixture(root, manifest, events)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any(expected in error for error in got["errors"]), got)

    def test_completed_fixture_rejects_corrupt_or_mismatched_load_milestones(self) -> None:
        for mutation, expected in (("corrupt", "cannot parse"), ("mismatch", "do not match")):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _ = lifecycle_fixture(root)
                milestones = root / "lifecycle_load_milestones.json"
                if mutation == "corrupt":
                    milestones.write_text("{", encoding="utf-8")
                else:
                    document = json.loads(milestones.read_text(encoding="utf-8"))
                    document["milestones"][0]["server_accepted_cumulative"] += 1
                    harness.write_json(milestones, document)
                next(
                    artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                    if artifact["path"] == "lifecycle_load_milestones.json"
                )["sha256"] = harness.sha256_file(milestones)
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any(expected in error for error in got["errors"]), got)

    def test_completed_fixture_rejects_unbounded_milestone_counts_without_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _ = lifecycle_fixture(root)
            sidecar = root / "adapter-lifecycle.jsonl"
            records = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]
            batch = next(record for record in records if record["event"] == "batch_accepted")
            batch["client_sent"] = 10**309
            batch["server_accepted"] = 10**309
            sidecar.write_text(
                "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
                encoding="utf-8",
            )
            next(
                artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                if artifact["path"] == "adapter-lifecycle.jsonl"
            )["sha256"] = harness.sha256_file(sidecar)
            harness.write_json(root / "manifest.json", manifest)

            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                exit_code = harness.main([
                    "--validate-lifecycle", str(root), "--allow-partial",
                ])
            got = harness.validate_lifecycle_artifact(root)

        self.assertEqual(exit_code, 1)
        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any(
            "cannot reconstruct lifecycle load milestones" in error for error in got["errors"]
        ), got)

    def test_completed_fixture_binds_adapter_counts_to_expected_and_lifecycle_rows(self) -> None:
        for mutation, expected in (
            ("sidecar", "does not equal lifecycle.expected_rows"),
            ("load_end", "does not match stage load_end"),
            ("final", "does not match final"),
        ):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, events = lifecycle_fixture(root)
                if mutation == "sidecar":
                    sidecar = root / "adapter-lifecycle.jsonl"
                    records = [
                        json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()
                    ]
                    batch = next(record for record in records if record["event"] == "batch_accepted")
                    batch["client_sent"] = 49_999
                    batch["server_accepted"] = 49_999
                    sidecar.write_text(
                        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
                        encoding="utf-8",
                    )
                    milestones = root / "lifecycle_load_milestones.json"
                    harness.write_json(milestones, harness.lifecycle_load_milestone_document(records))
                    for artifact in manifest["lifecycle"]["raw_artifacts"]:
                        if artifact["path"] == "adapter-lifecycle.jsonl":
                            artifact["sha256"] = harness.sha256_file(sidecar)
                        elif artifact["path"] == "lifecycle_load_milestones.json":
                            artifact["sha256"] = harness.sha256_file(milestones)
                    harness.write_json(root / "manifest.json", manifest)
                else:
                    event = next(
                        event for event in events
                        if event["stage"] == ("load_end" if mutation == "load_end" else "teardown")
                    )
                    event["state"]["rows"]["client_sent"] = 49_999
                    rewrite_lifecycle_fixture(root, manifest, events)

                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    exit_code = harness.main([
                        "--validate-lifecycle", str(root), "--allow-partial",
                    ])
                got = harness.validate_lifecycle_artifact(root)

            self.assertEqual(exit_code, 1)
            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any(expected in error for error in got["errors"]), got)

    def test_milestone_builder_rejects_unbounded_counts(self) -> None:
        records = [
            {"event": "load_start", "timestamp_ns": 1},
            {
                "event": "batch_accepted",
                "timestamp_ns": 1_000_000_001,
                "client_sent": 10**309,
                "server_accepted": 10**309,
            },
        ]

        with self.assertRaisesRegex(ValueError, "supported finite milestone rate"):
            harness.lifecycle_load_milestone_document(records)

    def test_completed_fixture_rejects_samples_at_or_after_later_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _ = lifecycle_fixture(root)
            diagnostics = root / "diagnostics.jsonl"
            records = [json.loads(line) for line in diagnostics.read_text(encoding="utf-8").splitlines()]
            late_timestamp_ns = records[-1]["boundary_timestamp_ns"] + 1
            for record in records:
                record["timestamp_ns"] = late_timestamp_ns
            diagnostics.write_text(
                "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
                encoding="utf-8",
            )
            acknowledgement = root / "lifecycle-boundary-diagnostics.json"
            acknowledgement_payload = json.loads(acknowledgement.read_text(encoding="utf-8"))
            acknowledgement_payload["sample_timestamp_ns"] = late_timestamp_ns
            harness.write_json(acknowledgement, acknowledgement_payload)
            for artifact in manifest["lifecycle"]["raw_artifacts"]:
                if artifact["path"] == "diagnostics.jsonl":
                    artifact["sha256"] = harness.sha256_file(diagnostics)
                elif artifact["path"] == "lifecycle-boundary-diagnostics.json":
                    artifact["sha256"] = harness.sha256_file(acknowledgement)
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertEqual(
            sum("before the next boundary" in item for item in got["errors"]),
            len(harness.LIFECYCLE_DIAGNOSTIC_BOUNDARIES) - 1,
            got,
        )

    def test_completed_fixture_uses_emitter_microsecond_policy_for_boundary_ns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            sidecar = root / "adapter-lifecycle.jsonl"
            sidecar_records = [
                json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()
            ]
            load_end = next(record for record in sidecar_records if record["event"] == "load_end")
            load_end["timestamp_ns"] += 123_456_789
            next(event for event in events if event["stage"] == "load_end")["timestamp"] = (
                harness.iso_from_ns(load_end["timestamp_ns"])
            )
            sidecar.write_text(
                "".join(json.dumps(record, sort_keys=True) + "\n" for record in sidecar_records),
                encoding="utf-8",
            )
            diagnostics = root / "diagnostics.jsonl"
            diagnostics_records = [
                json.loads(line) for line in diagnostics.read_text(encoding="utf-8").splitlines()
            ]
            load_sample = next(
                record for record in diagnostics_records if record.get("boundary") == "load_end"
            )
            load_sample["boundary_timestamp_ns"] = load_end["timestamp_ns"]
            load_sample["timestamp_ns"] = load_end["timestamp_ns"]
            diagnostics.write_text(
                "".join(json.dumps(record, sort_keys=True) + "\n" for record in diagnostics_records),
                encoding="utf-8",
            )
            milestones = root / "lifecycle_load_milestones.json"
            harness.write_json(
                milestones, harness.lifecycle_load_milestone_document(sidecar_records)
            )
            for artifact in manifest["lifecycle"]["raw_artifacts"]:
                if artifact["path"] == "adapter-lifecycle.jsonl":
                    artifact["sha256"] = harness.sha256_file(sidecar)
                elif artifact["path"] == "diagnostics.jsonl":
                    artifact["sha256"] = harness.sha256_file(diagnostics)
                elif artifact["path"] == "lifecycle_load_milestones.json":
                    artifact["sha256"] = harness.sha256_file(milestones)
            rewrite_lifecycle_fixture(root, manifest, events)

            got = harness.validate_lifecycle_artifact(root)

        self.assertTrue(got["complete"], got)

    def test_raw_evidence_timestamp_range_errors_are_structured(self) -> None:
        for timestamp_ns in (10**30, -(10**30)):
            with self.subTest(timestamp_ns=timestamp_ns), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _ = lifecycle_fixture(root)
                sidecar = root / "adapter-lifecycle.jsonl"
                records = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]
                records[-1]["timestamp_ns"] = timestamp_ns
                sidecar.write_text(
                    "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
                    encoding="utf-8",
                )
                next(
                    artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                    if artifact["path"] == "adapter-lifecycle.jsonl"
                )["sha256"] = harness.sha256_file(sidecar)
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)
                with contextlib.redirect_stdout(io.StringIO()):
                    exit_code = harness.main([
                        "--validate-lifecycle", str(root), "--allow-partial",
                    ])

            self.assertFalse(got["analyzable"], got)
            self.assertEqual(exit_code, 1)
            if timestamp_ns > 0:
                self.assertTrue(any("outside the supported UTC datetime range" in item for item in got["errors"]), got)
            else:
                self.assertTrue(any("positive integer" in item for item in got["errors"]), got)

    def test_adapter_sidecar_reader_rejects_out_of_range_timestamp_before_reconstruction(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lifecycle_fixture(root)
            sidecar = root / "adapter-lifecycle.jsonl"
            records = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]
            records[2]["timestamp_ns"] = 10**30
            sidecar.write_text(
                "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "line 3 timestamp_ns.*supported UTC"):
                harness.read_adapter_lifecycle_sidecar(sidecar)

    def test_interrupted_fixture_is_analyzable_but_never_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            manifest["lifecycle"]["result_status"] = "interrupted"
            manifest["lifecycle"]["profiles"] = []
            manifest["lifecycle"]["raw_artifacts"] = [
                artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                if artifact["path"] not in {
                    "adapter-lifecycle.jsonl",
                    "diagnostics.jsonl",
                    "lifecycle-boundary-diagnostics.json",
                }
            ]
            rewrite_lifecycle_fixture(root, manifest, events[:4])

            got = harness.validate_lifecycle_artifact(root)

        self.assertTrue(got["analyzable"])
        self.assertFalse(got["complete"])
        self.assertEqual(got["last_stage"], "load_end")
        self.assertTrue(any("result_status" in item for item in got["completion_errors"]))

    def test_result_status_must_be_a_string_without_cli_traceback(self) -> None:
        for status, use_cli in (([], False), ({}, True)):
            with self.subTest(status=status, cli=use_cli):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    manifest["lifecycle"]["result_status"] = status
                    harness.write_json(root / "manifest.json", manifest)
                    if use_cli:
                        output = io.StringIO()
                        with contextlib.redirect_stdout(output):
                            exit_code = harness.main([
                                "--validate-lifecycle", str(root), "--allow-partial",
                            ])
                        got = json.loads(output.getvalue())
                        self.assertEqual(exit_code, 1)
                    else:
                        got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["analyzable"], got)
                self.assertTrue(any("result_status must be" in item for item in got["errors"]), got)

    def test_nonfinite_json_is_structurally_invalid_in_complete_and_partial_artifacts(self) -> None:
        cases = (
            ("complete-manifest-nan", "manifest", float("nan"), "completed", False),
            ("complete-event-infinity", "event", float("inf"), "completed", False),
            ("partial-event-negative-infinity", "event", float("-inf"), "partial", True),
        )
        for label, location, value, status, use_cli in cases:
            with self.subTest(case=label):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, events = lifecycle_fixture(root)
                    manifest["lifecycle"]["result_status"] = status
                    if location == "manifest":
                        manifest["ignored_nonfinite"] = value
                        harness.write_json(root / "manifest.json", manifest)
                    else:
                        events[0]["ignored_nonfinite"] = value
                        if status == "partial":
                            manifest["lifecycle"]["profiles"] = []
                            events = events[:4]
                        rewrite_lifecycle_fixture(root, manifest, events)
                    if use_cli:
                        output = io.StringIO()
                        with contextlib.redirect_stdout(output):
                            exit_code = harness.main([
                                "--validate-lifecycle", str(root), "--allow-partial",
                            ])
                        got = json.loads(output.getvalue())
                        self.assertEqual(exit_code, 1)
                    else:
                        got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["analyzable"], got)
                self.assertTrue(any("non-finite JSON number" in item for item in got["errors"]), got)

    def test_duplicate_json_keys_are_structurally_invalid_at_any_depth(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _ = lifecycle_fixture(root)
            manifest["ignored"] = {"sentinel": 1}
            harness.write_json(root / "manifest.json", manifest)
            manifest_path = root / "manifest.json"
            manifest_text = manifest_path.read_text(encoding="utf-8")
            manifest_path.write_text(
                manifest_text.replace('"sentinel": 1', '"sentinel": 1, "sentinel": 2'),
                encoding="utf-8",
            )

            complete = harness.validate_lifecycle_artifact(root)

        self.assertFalse(complete["analyzable"], complete)
        self.assertTrue(any("duplicate JSON object key 'sentinel'" in item for item in complete["errors"]), complete)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            manifest["lifecycle"]["result_status"] = "partial"
            manifest["lifecycle"]["profiles"] = []
            events[0]["ignored"] = {"sentinel": 1}
            rewrite_lifecycle_fixture(root, manifest, events[:4])
            lifecycle_path = root / "lifecycle.jsonl"
            lifecycle_text = lifecycle_path.read_text(encoding="utf-8")
            lifecycle_path.write_text(
                lifecycle_text.replace('"sentinel": 1', '"sentinel": 1, "sentinel": 2'),
                encoding="utf-8",
            )
            manifest["lifecycle"]["sha256"] = harness.sha256_file(lifecycle_path)
            harness.write_json(root / "manifest.json", manifest)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exit_code = harness.main(["--validate-lifecycle", str(root), "--allow-partial"])
            partial = json.loads(output.getvalue())

        self.assertEqual(exit_code, 1)
        self.assertFalse(partial["analyzable"], partial)
        self.assertTrue(any("duplicate JSON object key 'sentinel'" in item for item in partial["errors"]), partial)

    def test_partial_fixture_rejects_impossible_known_stage_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            manifest["lifecycle"]["result_status"] = "partial"
            manifest["lifecycle"]["profiles"] = []
            events[1]["stage"], events[2]["stage"] = events[2]["stage"], events[1]["stage"]
            rewrite_lifecycle_fixture(root, manifest, events[:3])

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"])
        self.assertFalse(got["complete"])
        self.assertIn("known lifecycle stages are out of order", got["errors"])

    def test_present_database_snapshot_is_structural_but_absent_future_stage_is_not(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            manifest["lifecycle"]["result_status"] = "partial"
            manifest["lifecycle"]["profiles"] = []
            rewrite_lifecycle_fixture(root, manifest, events[:4])

            early = harness.validate_lifecycle_artifact(root)

        self.assertTrue(early["analyzable"], early)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            manifest["lifecycle"]["result_status"] = "partial"
            manifest["lifecycle"]["profiles"] = []
            events[9]["state"]["database"]["commit_seq"] = "50000"
            rewrite_lifecycle_fixture(root, manifest, events[:10])
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exit_code = harness.main(["--validate-lifecycle", str(root), "--allow-partial"])
            malformed = json.loads(output.getvalue())

        self.assertEqual(exit_code, 1)
        self.assertFalse(malformed["analyzable"], malformed)
        self.assertTrue(any("database.commit_seq" in item for item in malformed["errors"]), malformed)

    def test_present_index_snapshot_types_are_structural_in_partial_artifacts(self) -> None:
        cases = (
            (3, lambda state: state.__setitem__("index", []), "index must be an object"),
            (6, lambda state: state.__setitem__("index", []), "index must be an object"),
            (7, lambda state: state["index"].__setitem__("identity", 7), "index.identity"),
            (8, lambda state: state["index"].__setitem__("asset_generation", 7.0), "index.asset_generation"),
            (9, lambda state: state["index"].__setitem__("status", []), "index.status"),
        )
        for event_position, mutation, expected in cases:
            with self.subTest(stage_position=event_position, expected=expected):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, events = lifecycle_fixture(root)
                    manifest["lifecycle"]["result_status"] = "partial"
                    manifest["lifecycle"]["profiles"] = []
                    mutation(events[event_position]["state"])
                    rewrite_lifecycle_fixture(root, manifest, events[:event_position + 1])

                    got = harness.validate_lifecycle_artifact(root)
                    with contextlib.redirect_stdout(io.StringIO()):
                        exit_code = harness.main(["--validate-lifecycle", str(root), "--allow-partial"])

                self.assertFalse(got["analyzable"], got)
                self.assertTrue(any(expected in item for item in got["errors"]), got)
                self.assertEqual(exit_code, 1)

    def test_present_route_snapshot_types_are_structural_in_partial_artifacts(self) -> None:
        cases = [
            (3, lambda state: state.__setitem__("route", []), "route must be an object"),
            (12, lambda state: state.pop("route"), "route proof object"),
            (12, lambda state: state.__setitem__("route", {}), "required field"),
            (12, lambda state: state["route"].__setitem__("name", 7), "route.name"),
            (12, lambda state: state["route"].__setitem__("fallback_reason", []), "route.fallback_reason"),
            (12, lambda state: state["route"].__setitem__("optimized", 1), "route.optimized"),
            (12, lambda state: state["route"].__setitem__("index_identity", 7), "route.index_identity"),
            (12, lambda state: state["route"].__setitem__("index_asset_generation", 7.0), "route.index_asset_generation"),
            (12, lambda state: state["route"].__setitem__("service_generation", 7.0), "route.service_generation"),
            (12, lambda state: state["route"].__setitem__("requested_top_k", "2"), "route.requested_top_k"),
            (12, lambda state: state["route"].__setitem__("result_count", []), "route.result_count"),
            (12, lambda state: state["route"].__setitem__("effective_ef_search", 7.0), "route.effective_ef_search"),
        ]
        for field in (
            "name", "fallback_reason", "optimized", "index_identity", "index_asset_generation",
            "service_generation", "requested_top_k", "result_count", "effective_ef_search",
        ):
            cases.append((12, lambda state, key=field: state["route"].pop(key), f"required field {field}"))
        for event_position, mutation, expected in cases:
            with self.subTest(stage_position=event_position, expected=expected):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, events = lifecycle_fixture(root)
                    manifest["lifecycle"]["result_status"] = "partial"
                    manifest["lifecycle"]["profiles"] = []
                    mutation(events[event_position]["state"])
                    rewrite_lifecycle_fixture(root, manifest, events[:event_position + 1])

                    got = harness.validate_lifecycle_artifact(root)
                    with contextlib.redirect_stdout(io.StringIO()):
                        exit_code = harness.main(["--validate-lifecycle", str(root), "--allow-partial"])

                self.assertFalse(got["analyzable"], got)
                self.assertTrue(any(expected in item for item in got["errors"]), got)
                self.assertEqual(exit_code, 1)

    def test_cli_fails_closed_unless_analyzable_partial_is_explicitly_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            manifest["lifecycle"]["result_status"] = "partial"
            manifest["lifecycle"]["profiles"] = []
            rewrite_lifecycle_fixture(root, manifest, events[:4])
            with contextlib.redirect_stdout(io.StringIO()):
                strict = harness.main(["--validate-lifecycle", str(root)])
                analyzable = harness.main(["--validate-lifecycle", str(root), "--allow-partial"])

        self.assertEqual(strict, 1)
        self.assertEqual(analyzable, 0)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            events.pop(5)
            rewrite_lifecycle_fixture(root, manifest, events)
            with contextlib.redirect_stdout(io.StringIO()):
                malformed_complete = harness.main([
                    "--validate-lifecycle", str(root), "--allow-partial",
                ])

        self.assertEqual(malformed_complete, 1)

        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            missing_manifest = harness.main([
                "--validate-lifecycle", tmp, "--allow-partial",
            ])

        self.assertEqual(missing_manifest, 1)

        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
            harness.main(["--self-test", "--validate-lifecycle", "/missing"])
        self.assertEqual(raised.exception.code, 2)

    def test_complete_gate_rejects_missing_or_out_of_order_stage(self) -> None:
        for mutation, expected in (
            (lambda rows: rows.pop(5), "missing required stage optimize_start"),
            (lambda rows: rows.__setitem__(slice(5, 7), [rows[6], rows[5]]), "sequence must increase"),
        ):
            with self.subTest(expected=expected):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, events = lifecycle_fixture(root)
                    mutation(events)
                    rewrite_lifecycle_fixture(root, manifest, events)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["complete"])
                self.assertTrue(any(expected in item for item in got["errors"] + got["completion_errors"]), got)

    def test_invalid_utf8_manifest_fails_closed_without_cli_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "manifest.json").write_bytes(b"\xff")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exit_code = harness.main(["--validate-lifecycle", str(root), "--allow-partial"])
            report = json.loads(output.getvalue())

        self.assertEqual(exit_code, 1)
        self.assertFalse(report["analyzable"])
        self.assertTrue(any("cannot read manifest.json" in item for item in report["errors"]), report)

    def test_identity_and_config_must_match_manifest(self) -> None:
        for key, value, expected in (
            ("gomap_commit", "f" * 40, "gomap_commit does not match"),
            ("config_sha256", "f" * 64, "config_sha256 does not match"),
        ):
            with self.subTest(key=key):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    manifest["lifecycle"]["identity"][key] = value
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["complete"])
                self.assertTrue(any(expected in item for item in got["errors"]), got)

    def test_manifest_and_lifecycle_identities_must_be_strings(self) -> None:
        cases = (
            (
                "manifest-gomap",
                lambda row: row["context"]["gomap"].__setitem__("commit", int("1" * 40)),
                "manifest gomap_commit",
            ),
            (
                "manifest-vdb",
                lambda row: row["context"]["vectordbbench"].__setitem__("commit", int("2" * 40)),
                "manifest vectordbbench_commit",
            ),
            (
                "manifest-binary",
                lambda row: row["service"]["binary"].__setitem__("sha256", int("3" * 64)),
                "manifest service_binary_sha256",
            ),
            (
                "identity-gomap",
                lambda row: row["lifecycle"]["identity"].__setitem__("gomap_commit", int("1" * 40)),
                "identity gomap_commit",
            ),
            (
                "identity-vdb",
                lambda row: row["lifecycle"]["identity"].__setitem__("vectordbbench_commit", int("2" * 40)),
                "identity vectordbbench_commit",
            ),
            (
                "identity-binary",
                lambda row: row["lifecycle"]["identity"].__setitem__("service_binary_sha256", int("3" * 64)),
                "identity service_binary_sha256",
            ),
        )
        for label, mutation, expected in cases:
            with self.subTest(identity=label):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    mutation(manifest)
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["analyzable"], got)
                self.assertTrue(any(expected in item for item in got["errors"]), got)

    def test_dataset_and_raw_artifact_digests_must_be_strings(self) -> None:
        cases = (
            (
                "dataset",
                lambda row: row["lifecycle"]["dataset"].__setitem__("sha256", int("4" * 64)),
                "lifecycle.dataset.sha256",
            ),
            (
                "raw-artifact",
                lambda row: row["lifecycle"]["raw_artifacts"][0].__setitem__("sha256", int("5" * 64)),
                "raw artifact 0 has invalid SHA-256",
            ),
        )
        for label, mutation, expected in cases:
            with self.subTest(digest=label):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    mutation(manifest)
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["analyzable"], got)
                self.assertTrue(any(expected in item for item in got["errors"]), got)

    def test_effective_service_and_harness_config_must_be_present(self) -> None:
        mutations = (
            (lambda row: row["service"].__setitem__("profile", ""), "service.profile"),
            (lambda row: row["service"].__setitem__("profile", "bench_unsafe"), "service.profile"),
            (lambda row: row["service"].__setitem__("profile", []), "service.profile"),
            (lambda row: row["harness"].__setitem__("case_type", ""), "harness.case_type"),
            (lambda row: row["harness"].__setitem__("num_concurrency", ""), "harness.num_concurrency"),
            (lambda row: row["harness"].__setitem__("num_concurrency", "1,nope"), "harness.num_concurrency"),
            (lambda row: row["harness"].__setitem__("num_per_batch", 0), "harness.num_per_batch"),
            (lambda row: row["harness"].__setitem__("m", None), "harness.m"),
            (lambda row: row["harness"].__setitem__("ef_construction", -1), "harness.ef_construction"),
        )
        for mutation, expected in mutations:
            with self.subTest(expected=expected):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    mutation(manifest)
                    manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["analyzable"])
                self.assertTrue(any(expected in item for item in got["errors"]), got)

    def test_service_command_is_exactly_bound_to_the_declared_profile(self) -> None:
        mutations = (
            ("absent", lambda row: row["service"].pop("command")),
            ("not-argv", lambda row: row["service"].__setitem__("command", "treedb-document-service")),
            ("empty-argv", lambda row: row["service"].__setitem__("command", [])),
            ("non-string-argument", lambda row: row["service"]["command"].append(7)),
            ("missing-selector", lambda row: row["service"].__setitem__("command", ["treedb-document-service"])),
            ("declared-command-mismatch", lambda row: row["service"].__setitem__("profile", "command_wal_relaxed")),
            ("duplicate-selector", lambda row: row["service"]["command"].extend(["-profile", "command_wal_durable"])),
            ("unknown-flag", lambda row: row["service"]["command"].append("-bogus")),
            ("unknown-double-dash-flag", lambda row: row["service"]["command"].append("--bogus")),
            ("unknown-inline-flag", lambda row: row["service"]["command"].append("-bogus=value")),
            ("empty-effective-dir", lambda row: row["service"]["command"].append("-dir=")),
            (
                "selector-after-positional",
                lambda row: row["service"]["command"].__setitem__(
                    slice(5, None), ["positional", "-profile", "command_wal_durable"],
                ),
            ),
            (
                "selector-after-double-dash",
                lambda row: row["service"]["command"].__setitem__(
                    slice(5, None), ["--", "-profile", "command_wal_durable"],
                ),
            ),
        )
        for label, mutation in mutations:
            with self.subTest(command=label):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    mutation(manifest)
                    manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["analyzable"], got)
                self.assertTrue(any("manifest.service.command" in item for item in got["errors"]), got)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _ = lifecycle_fixture(root)
            manifest["service"]["command"][2] = "/different/treedb-data"
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("config_sha256 does not match" in item for item in got["errors"]), got)

        for profile_selector in ("-profile=command_wal_durable", "--profile=command_wal_durable"):
            with self.subTest(profile_selector=profile_selector):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    manifest["service"]["command"][-2:] = [
                        "--pprof=127.0.0.1:6060",
                        "-block-profile-rate", "1",
                        "--mutex-profile-fraction=1",
                        profile_selector,
                    ]
                    manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertTrue(got["complete"], got)

    def test_construction_decision_diagnostics_command_is_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _ = lifecycle_fixture(root)
            manifest["harness"]["construction_decision_diagnostics"] = True
            manifest["service"]["command"].append("-diagnostic-construction-decisions")
            manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertTrue(got["complete"], got)

        mutations = (
            (
                "enabled-without-command-flag",
                lambda row: row["harness"].__setitem__("construction_decision_diagnostics", True),
            ),
            (
                "disabled-with-command-flag",
                lambda row: row["service"]["command"].append(
                    "-diagnostic-construction-decisions"
                ),
            ),
            (
                "duplicate",
                lambda row: (
                    row["harness"].__setitem__("construction_decision_diagnostics", True),
                    row["service"]["command"].extend([
                        "-diagnostic-construction-decisions",
                        "-diagnostic-construction-decisions",
                    ]),
                ),
            ),
            (
                "inline-value",
                lambda row: (
                    row["harness"].__setitem__("construction_decision_diagnostics", True),
                    row["service"]["command"].append(
                        "-diagnostic-construction-decisions=true"
                    ),
                ),
            ),
            (
                "separate-value",
                lambda row: (
                    row["harness"].__setitem__("construction_decision_diagnostics", True),
                    row["service"]["command"].extend([
                        "-diagnostic-construction-decisions", "true",
                    ]),
                ),
            ),
            (
                "double-dash",
                lambda row: (
                    row["harness"].__setitem__("construction_decision_diagnostics", True),
                    row["service"]["command"].append(
                        "--diagnostic-construction-decisions"
                    ),
                ),
            ),
            (
                "wrong-harness-type",
                lambda row: row["harness"].__setitem__(
                    "construction_decision_diagnostics", "true"
                ),
            ),
        )
        for label, mutation in mutations:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _ = lifecycle_fixture(root)
                mutation(manifest)
                manifest["lifecycle"]["identity"]["config_sha256"] = (
                    harness.lifecycle_config_sha256(manifest)
                )
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            expected_error = (
                "manifest.harness.construction_decision_diagnostics"
                if label == "wrong-harness-type"
                else "manifest.service.command"
            )
            self.assertTrue(
                any(expected_error in error for error in got["errors"]), got
            )

    def test_missing_construction_diagnostics_identity_remains_default_off(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _ = lifecycle_fixture(root)
            manifest["harness"].pop("construction_decision_diagnostics")
            manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertTrue(got["complete"], got)

    def test_service_pprof_address_matches_loopback_listener_contract(self) -> None:
        valid = ("127.0.0.1:6060", "localhost:6060", "[::1]:6060", "127.0.0.1:65535")
        invalid = (
            "localhost", "127.0.0.1", ":6060", "0.0.0.0:6060", "[::]:6060",
            "192.0.2.1:6060", "example.com:6060", "127.0.0.1:", "127.0.0.1:0",
            "127.0.0.1:http", "127.0.0.1:65536", "[::1%lo]:6060",
        )
        for address in valid:
            with self.subTest(pprof=address):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    manifest["service"]["command"].append(f"-pprof={address}")
                    manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertTrue(got["complete"], got)
        for address in invalid:
            with self.subTest(pprof=address):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    manifest["service"]["command"].append(f"-pprof={address}")
                    manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["analyzable"], got)
                self.assertTrue(any("service.command" in item for item in got["errors"]), got)

    def test_completed_lifecycle_requires_enabled_pprof_listener(self) -> None:
        for mutation in ("missing", "empty"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _ = lifecycle_fixture(root)
                command = manifest["service"]["command"]
                pprof_position = command.index("-pprof")
                if mutation == "missing":
                    del command[pprof_position:pprof_position + 2]
                else:
                    command[pprof_position + 1] = ""
                manifest["lifecycle"]["identity"]["config_sha256"] = (
                    harness.lifecycle_config_sha256(manifest)
                )
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any("service.command" in item for item in got["errors"]), got)

    def test_service_and_pprof_listeners_must_use_distinct_ports(self) -> None:
        for service_address, pprof_address in (
            ("127.0.0.1:9876", "127.0.0.1:9876"),
            ("127.0.0.1:9876", "localhost:9876"),
            ("127.0.0.1:9876", "[::1]:9876"),
            ("127.0.0.1:09876", "localhost:9876"),
        ):
            with self.subTest(addr=service_address, pprof=pprof_address), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _ = lifecycle_fixture(root)
                command = manifest["service"]["command"]
                command[command.index("-addr") + 1] = service_address
                command[command.index("-pprof") + 1] = pprof_address
                manifest["service"]["base_url"] = f"http://{service_address}"
                manifest["lifecycle"]["identity"]["config_sha256"] = (
                    harness.lifecycle_config_sha256(manifest)
                )
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any("service.command" in item for item in got["errors"]), got)

    def test_service_address_must_be_loopback(self) -> None:
        for address in ("0.0.0.0:9876", "192.0.2.1:9876", "example.com:9876"):
            with self.subTest(address=address), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _ = lifecycle_fixture(root)
                command = manifest["service"]["command"]
                command[command.index("-addr") + 1] = address
                manifest["service"]["base_url"] = f"http://{address}"
                set_fixture_vdbbench_command(manifest, "exact")
                manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                harness.write_json(root / "manifest.json", manifest)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["analyzable"], got)
            self.assertTrue(any("service.command" in item for item in got["errors"]), got)

    def test_service_command_executable_matches_declared_binary_path(self) -> None:
        cases = (
            ("missing-path", lambda row: row["service"]["binary"].pop("path"), "binary.path"),
            ("typed-path", lambda row: row["service"]["binary"].__setitem__("path", 7), "binary.path"),
            (
                "mismatched-command",
                lambda row: row["service"]["command"].__setitem__(0, "/different/treedb-document-service"),
                "command[0]",
            ),
        )
        for label, mutation, expected in cases:
            with self.subTest(binding=label):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    mutation(manifest)
                    manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["analyzable"], got)
                self.assertTrue(any(expected in item for item in got["errors"]), got)

    def test_service_data_dir_is_declared_and_artifact_owned(self) -> None:
        for label in ("external", "declared-mismatch"):
            with self.subTest(data_dir=label):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    if label == "external":
                        external = str(root.parent / "external-treedb-data")
                        manifest["service"]["command"][2] = external
                        manifest["service"]["data_dir"] = external
                    else:
                        manifest["service"]["data_dir"] = str(root / "other-data")
                    manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["analyzable"], got)
                self.assertTrue(any("service.data_dir" in item for item in got["errors"]), got)

    def test_relative_and_symlinked_artifact_roots_preserve_data_dir_binding(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            root = parent / "artifact"
            root.mkdir()
            lifecycle_fixture(root)
            (parent / "artifact-link").symlink_to(root, target_is_directory=True)

            previous_cwd = Path.cwd()
            try:
                os.chdir(parent)
                with contextlib.redirect_stdout(io.StringIO()):
                    got = harness.validate_lifecycle_artifact(Path("artifact"))
                    linked = harness.validate_lifecycle_artifact(Path("artifact-link"))
                    exit_code = harness.main(["--validate-lifecycle", "artifact"])
            finally:
                os.chdir(previous_cwd)

        self.assertTrue(got["complete"], got)
        self.assertTrue(linked["complete"], linked)
        self.assertEqual(exit_code, 0)

    def test_artifact_data_dir_symlink_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            root = parent / "artifact"
            root.mkdir()
            lifecycle_fixture(root)
            external = parent / "prepopulated"
            external.mkdir()
            (root / "treedb-data").symlink_to(external, target_is_directory=True)

            got = harness.validate_lifecycle_artifact(root)
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = harness.main(["--validate-lifecycle", str(root), "--allow-partial"])

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("data_dir must not be a symlink" in item for item in got["errors"]), got)
        self.assertEqual(exit_code, 1)

    def test_service_binary_must_remain_an_executable_with_matching_bytes(self) -> None:
        def missing(path: Path) -> None:
            path.unlink()

        def non_file(path: Path) -> None:
            path.unlink()
            path.mkdir()

        cases = (
            ("missing", missing),
            ("non-file", non_file),
            ("unreadable", lambda path: path.chmod(0)),
            ("changed", lambda path: path.write_bytes(b"different service binary\n")),
        )
        for label, mutation in cases:
            with self.subTest(binary=label):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    mutation(Path(manifest["service"]["binary"]["path"]))

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["analyzable"], got)
                self.assertTrue(any("service binary" in item for item in got["errors"]), got)

    def test_service_integer_flags_use_go_int64_syntax_and_bounds(self) -> None:
        for value in ("nope", "9223372036854775808", "-9223372036854775809", "9" * 5000):
            with self.subTest(invalid=value[:32]):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    manifest["service"]["command"][-2:-2] = ["-block-profile-rate", value]
                    manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)
                    with contextlib.redirect_stdout(io.StringIO()):
                        exit_code = harness.main(["--validate-lifecycle", str(root), "--allow-partial"])

                self.assertFalse(got["analyzable"], got)
                self.assertTrue(any("invalid flags" in item for item in got["errors"]), got)
                self.assertEqual(exit_code, 1)

        for value in ("42", "+0x2a", "-0o10", "0b10_10", "077"):
            with self.subTest(valid=value):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    manifest["service"]["command"][-2:-2] = ["--mutex-profile-fraction=" + value]
                    manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertTrue(got["complete"], got)

    def test_concurrency_tokens_are_bounded_ascii_decimals(self) -> None:
        for concurrency in ("²", "9" * 5000):
            with self.subTest(concurrency=concurrency[:10]):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    manifest["lifecycle"]["result_status"] = "partial"
                    manifest["harness"]["num_concurrency"] = concurrency
                    manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                    harness.write_json(root / "manifest.json", manifest)
                    output = io.StringIO()
                    with contextlib.redirect_stdout(output):
                        exit_code = harness.main(["--validate-lifecycle", str(root), "--allow-partial"])
                    report = json.loads(output.getvalue())

                self.assertEqual(exit_code, 1)
                self.assertFalse(report["analyzable"])
                self.assertTrue(any("num_concurrency" in item for item in report["errors"]), report)

    def test_case_shape_must_match_lifecycle_dataset(self) -> None:
        for case_type, expected in (
            ("Performance768D1M", "vector count"),
            ("Performance1536D50K", "dimensions"),
            ("Performance50K", "positive dimensions"),
        ):
            with self.subTest(case_type=case_type):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    manifest["harness"]["case_type"] = case_type
                    manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["complete"])
                self.assertTrue(any(expected in item for item in got["completion_errors"]), got)

    def test_teardown_counts_must_match_expected_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            events[-1]["state"]["rows"]["client_sent"] += 1
            events[-1]["state"]["rows"]["server_accepted"] += 1
            rewrite_lifecycle_fixture(root, manifest, events)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["complete"])
        self.assertTrue(any("stage teardown rows.server_accepted" in item for item in got["completion_errors"]), got)

    def test_reset_and_load_start_must_prove_an_empty_boundary(self) -> None:
        for sequence, stage in ((1, "reset"), (2, "load_start")):
            with self.subTest(stage=stage):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, events = lifecycle_fixture(root)
                    for key in ("client_sent", "server_accepted", "server_durable", "reopened"):
                        events[sequence]["state"]["rows"][key] = 1
                    rewrite_lifecycle_fixture(root, manifest, events)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["complete"])
                self.assertTrue(any(
                    f"stage {stage} rows.client_sent must be zero" in item
                    for item in got["completion_errors"]
                ), got)

    def test_reopened_rows_must_remain_zero_through_graceful_close(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            for event in events[8:]:
                event["state"]["rows"]["reopened"] = 50_000
            rewrite_lifecycle_fixture(root, manifest, events)

            got = harness.validate_lifecycle_artifact(root)

        self.assertTrue(got["analyzable"], got)
        self.assertFalse(got["complete"], got)
        self.assertTrue(
            any("stage cache_warm rows.reopened must remain zero" in item for item in got["completion_errors"]),
            got,
        )

    def test_teardown_must_be_the_terminal_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            late = json.loads(json.dumps(events[-1]))
            late["sequence"] += 1
            late["stage"] = "post_teardown"
            late["timestamp"] = "2026-08-27T00:00:14Z"
            late["state"]["counters"]["commit_seq"] += 1
            events.append(late)
            rewrite_lifecycle_fixture(root, manifest, events)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["complete"])
        self.assertEqual(got["last_stage"], "post_teardown")
        self.assertTrue(any("teardown must be the final" in item for item in got["completion_errors"]), got)

    def test_stale_index_generation_or_fallback_route_fails_closed(self) -> None:
        def stale_identity(rows: list[dict]) -> None:
            rows[10]["state"]["index"]["identity"] = "stale-index"

        def stale_generation(rows: list[dict]) -> None:
            rows[11]["state"]["index"]["asset_generation"] = 8

        def fallback_route(rows: list[dict]) -> None:
            rows[12]["state"]["route"]["fallback_reason"] = "exact_scan"

        def unknown_route(rows: list[dict]) -> None:
            rows[12]["state"]["route"]["name"] = "custom_optimized_route"

        def stale_route_identity(rows: list[dict]) -> None:
            rows[12]["state"]["route"]["index_identity"] = "stale-index"

        def float_route_generation(rows: list[dict]) -> None:
            rows[12]["state"]["route"]["index_asset_generation"] = 7.0

        def bool_route_generation(rows: list[dict]) -> None:
            rows[12]["state"]["route"]["index_asset_generation"] = True

        for mutation, expected in (
            (stale_identity, "index identity changed"),
            (stale_generation, "index asset generation changed"),
            (stale_route_identity, "optimized route proof failed"),
            (float_route_generation, "optimized route proof failed"),
            (bool_route_generation, "optimized route proof failed"),
            (unknown_route, "optimized route proof failed"),
            (fallback_route, "optimized route proof failed"),
        ):
            with self.subTest(mutation=mutation.__name__, expected=expected):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, events = lifecycle_fixture(root)
                    mutation(events)
                    rewrite_lifecycle_fixture(root, manifest, events)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["complete"])
                self.assertTrue(any(expected in item for item in got["completion_errors"]), got)

    def test_route_result_count_must_equal_positive_requested_top_k(self) -> None:
        for result_count in (0, 1, []):
            with self.subTest(result_count=result_count), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, events = lifecycle_fixture(root)
                events[12]["state"]["route"]["result_count"] = result_count
                rewrite_lifecycle_fixture(root, manifest, events)

                got = harness.validate_lifecycle_artifact(root)

            self.assertFalse(got["complete"], got)
            self.assertTrue(
                any("result_count" in item for item in got["errors"])
                or any("optimized route proof failed" in item for item in got["completion_errors"]),
                got,
            )

    def test_canonical_optimized_route_names_complete(self) -> None:
        for route_name in harness.OPTIMIZED_ROUTE_NAMES:
            with self.subTest(route=route_name):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, events = lifecycle_fixture(root)
                    events[12]["state"]["route"]["name"] = route_name
                    response_path = root / "lifecycle_route_response.json"
                    response = json.loads(response_path.read_text(encoding="utf-8"))
                    response["diagnostics"]["route"] = route_name
                    if route_name == "quantized_rerank":
                        manifest["harness"]["rows"] = "scalar"
                        set_fixture_vdbbench_command(manifest, "scalar")
                        response["query_mode"] = "quantized_rerank"
                        response["quantized_index_name"] = manifest["harness"]["quantized_index_name"]
                        response["quantized_rerank_candidates"] = manifest["harness"]["rerank_candidates"]
                        manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                    harness.write_json(response_path, response)
                    manifest["lifecycle"]["raw_artifacts"][1]["sha256"] = harness.sha256_file(response_path)
                    rewrite_lifecycle_fixture(root, manifest, events)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertTrue(got["complete"], got)

    def test_raw_checksum_and_profile_association_are_verified(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lifecycle_fixture(root)
            (root / "profiles" / "optimize.heap.pprof").write_bytes(b"corrupt")

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"])
        self.assertTrue(any("checksum mismatch" in item for item in got["errors"]), got)

        for label in (
            "empty", "truncated-gzip", "truncated-after-data", "valid-gzip-not-pprof", "relabeled-jsonl",
        ):
            with self.subTest(profile_payload=label):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    profile = root / "profiles" / "optimize.heap.pprof"
                    if label == "empty":
                        payload = b""
                    elif label == "truncated-gzip":
                        payload = b"\x1f\x8b\x08"
                    elif label == "truncated-after-data":
                        payload = profile.read_bytes()[:-8]
                    elif label == "valid-gzip-not-pprof":
                        payload = gzip.compress(b"not a pprof", mtime=0)
                    else:
                        payload = (root / "lifecycle.jsonl").read_bytes()
                    profile.write_bytes(payload)
                    checksum = harness.sha256_file(profile)
                    manifest["lifecycle"]["raw_artifacts"][0]["sha256"] = checksum
                    manifest["lifecycle"]["profiles"][0]["sha256"] = checksum
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["complete"])
                self.assertTrue(any("content does not match" in item for item in got["errors"]), got)

        for kind in ("heap", "allocs", "block", "mutex", "text", []):
            with self.subTest(profile_kind=kind):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    profile = root / "profiles" / "optimize.heap.pprof"
                    profile.write_bytes(valid_pprof_fixture())
                    checksum = harness.sha256_file(profile)
                    manifest["lifecycle"]["raw_artifacts"][0]["sha256"] = checksum
                    manifest["lifecycle"]["profiles"][0]["sha256"] = checksum
                    manifest["lifecycle"]["profiles"][0]["kind"] = kind
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["complete"])
                self.assertTrue(any("content does not match" in item for item in got["errors"]), got)

        for malformed in (999, []):
            with self.subTest(after_sequence=malformed):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    manifest["lifecycle"]["profiles"][0]["after_sequence"] = malformed
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["complete"])
                self.assertTrue(any("profile state association" in item for item in got["errors"]), got)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _ = lifecycle_fixture(root)
            manifest["lifecycle"]["profiles"] = []
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertTrue(got["analyzable"])
        self.assertFalse(got["complete"])
        self.assertTrue(any("at least one profile" in item for item in got["completion_errors"]), got)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _ = lifecycle_fixture(root)
            profile = root / "profiles" / "build.cpu.pprof"
            profile.write_bytes(valid_pprof_fixture())
            checksum = harness.sha256_file(profile)
            manifest["lifecycle"]["raw_artifacts"][0] = {"path": "profiles/build.cpu.pprof", "sha256": checksum}
            manifest["lifecycle"]["profiles"] = [{
                "path": "profiles/build.cpu.pprof",
                "sha256": checksum,
                "kind": "cpu",
                "before_sequence": 5,
                "after_sequence": 6,
            }]
            harness.write_json(root / "manifest.json", manifest)

            companion_only = harness.validate_lifecycle_artifact(root)

        self.assertTrue(companion_only["analyzable"], companion_only)
        self.assertFalse(companion_only["complete"], companion_only)
        self.assertTrue(any("canonical optimize heap" in item for item in companion_only["completion_errors"]), companion_only)

    def test_canonical_heap_profile_sequences_must_name_the_documented_stages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            for event in events:
                if event["stage"] in {
                    "cache_prime", "cache_warm", "graceful_close", "cold_open_ready",
                    "exact_verify", "route_verify", "teardown",
                }:
                    event["sequence"] += 1
            lifecycle_path = root / "lifecycle.jsonl"
            lifecycle_path.write_text(
                "".join(json.dumps(row, sort_keys=True) + "\n" for row in events),
                encoding="utf-8",
            )
            manifest["lifecycle"]["sha256"] = harness.sha256_file(lifecycle_path)
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertTrue(got["analyzable"], got)
        self.assertFalse(got["complete"], got)
        self.assertTrue(
            any("canonical optimize heap" in item for item in got["completion_errors"]), got
        )

    def test_pprof_profile_requires_at_least_one_actual_sample(self) -> None:
        metadata = b"\n".join((
            b"PeriodType: cpu nanoseconds",
            b"Samples:",
            b"samples/count cpu/nanoseconds",
            b"Locations",
            b"Mappings",
        ))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lifecycle_fixture(root)
            with mock.patch.object(harness, "_pprof_metadata", return_value=metadata):
                got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("content does not match" in item for item in got["errors"]), got)

    def test_go_1_26_heap_profile_without_default_marker_is_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _ = lifecycle_fixture(root)
            profile = root / "profiles" / "optimize.heap.pprof"
            profile.write_bytes(valid_heap_pprof_fixture())
            checksum = harness.sha256_file(profile)
            manifest["lifecycle"]["raw_artifacts"] = [
                {"path": "profiles/optimize.heap.pprof", "sha256": checksum},
                manifest["lifecycle"]["raw_artifacts"][1],
                *manifest["lifecycle"]["raw_artifacts"][2:],
            ]
            manifest["lifecycle"]["profiles"] = [{
                "path": "profiles/optimize.heap.pprof",
                "sha256": checksum,
                "kind": "heap",
                "before_sequence": 8,
                "after_sequence": 9,
            }]
            harness.write_json(root / "manifest.json", manifest)

            metadata = harness._pprof_metadata(profile)
            got = harness.validate_lifecycle_artifact(root)

        self.assertIsNotNone(metadata)
        samples = metadata.decode("utf-8").split("Samples:\n", 1)[1].splitlines()[0]
        self.assertNotIn("[dflt]", samples)
        self.assertTrue(got["complete"], got)

    def test_missing_native_profile_decoders_are_structural_errors(self) -> None:
        cases = (
            ("pprof", "cpu", "profiles/build.cpu.pprof", valid_pprof_fixture(), "go"),
            ("trace", "trace", "profiles/build.trace.out", valid_trace_fixture(), "go"),
            ("perf", "perf", "profiles/build.perf.data", valid_perf_fixture(), "perf"),
        )
        for label, kind, relative, payload, decoder in cases:
            with self.subTest(profile=label):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    profile = root / relative
                    profile.write_bytes(payload)
                    checksum = harness.sha256_file(profile)
                    manifest["lifecycle"]["raw_artifacts"] = [{"path": relative, "sha256": checksum}]
                    manifest["lifecycle"]["profiles"] = [{
                        "path": relative,
                        "sha256": checksum,
                        "kind": kind,
                        "before_sequence": 5,
                        "after_sequence": 6,
                    }]
                    harness.write_json(root / "manifest.json", manifest)

                    with mock.patch.object(harness.shutil, "which", return_value=None):
                        got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["analyzable"], got)
                self.assertTrue(
                    any(f"unavailable native decoder: {decoder}" in item for item in got["errors"]),
                    got,
                )

    def test_manifest_controlled_paths_fail_closed_without_cli_traceback(self) -> None:
        mutations = (
            ("lifecycle", lambda row: row["lifecycle"].__setitem__("file", "bad\x00lifecycle.jsonl")),
            (
                "raw",
                lambda row: row["lifecycle"]["raw_artifacts"][0].__setitem__("path", "bad\x00profile.pprof"),
            ),
            (
                "profile",
                lambda row: row["lifecycle"]["profiles"][0].__setitem__("path", "bad\x00profile.pprof"),
            ),
        )
        for label, mutation in mutations:
            with self.subTest(path=label):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    manifest["lifecycle"]["result_status"] = "partial"
                    mutation(manifest)
                    harness.write_json(root / "manifest.json", manifest)
                    output = io.StringIO()
                    with contextlib.redirect_stdout(output):
                        exit_code = harness.main(["--validate-lifecycle", str(root), "--allow-partial"])
                    report = json.loads(output.getvalue())

                self.assertEqual(exit_code, 1)
                self.assertFalse(report["analyzable"])
                self.assertTrue(any("path is invalid" in item for item in report["errors"]), report)

    def test_trace_profile_requires_native_decoder_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _ = lifecycle_fixture(root)
            trace = root / "profiles" / "build.trace.out"
            trace.write_bytes(valid_trace_fixture())
            checksum = harness.sha256_file(trace)
            manifest["lifecycle"]["raw_artifacts"].append(
                {"path": "profiles/build.trace.out", "sha256": checksum}
            )
            manifest["lifecycle"]["profiles"].append({
                "path": "profiles/build.trace.out",
                "sha256": checksum,
                "kind": "trace",
                "before_sequence": 5,
                "after_sequence": 6,
            })
            harness.write_json(root / "manifest.json", manifest)

            valid = harness.validate_lifecycle_artifact(root)

            trace.write_bytes(b"go 1.26 trace\x00\x00\x00")
            checksum = harness.sha256_file(trace)
            manifest["lifecycle"]["raw_artifacts"][-1]["sha256"] = checksum
            manifest["lifecycle"]["profiles"][-1]["sha256"] = checksum
            harness.write_json(root / "manifest.json", manifest)
            header_only = harness.validate_lifecycle_artifact(root)

        self.assertTrue(valid["complete"], valid)
        self.assertFalse(header_only["analyzable"])
        self.assertTrue(any("content does not match" in item for item in header_only["errors"]), header_only)

    def test_perf_profile_requires_native_sample_decoding(self) -> None:
        valid_payload = valid_perf_fixture()
        zero_size_record = bytearray(valid_payload + b"\x00" * 8)
        zero_size_record[48:56] = (24).to_bytes(8, "little")
        trailing_garbage = bytearray(valid_payload + b"junk")
        trailing_garbage[48:56] = (20).to_bytes(8, "little")
        no_sample = bytearray(valid_payload)
        no_sample[184:188] = (1).to_bytes(4, "little")

        def validate(payload: bytes, native: int | BaseException) -> dict:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, _ = lifecycle_fixture(root)
                perf = root / "profiles" / "build.perf.data"
                perf.write_bytes(payload)
                checksum = harness.sha256_file(perf)
                manifest["lifecycle"]["raw_artifacts"].append(
                    {"path": "profiles/build.perf.data", "sha256": checksum}
                )
                manifest["lifecycle"]["profiles"].append({
                    "path": "profiles/build.perf.data",
                    "sha256": checksum,
                    "kind": "perf",
                    "before_sequence": 5,
                    "after_sequence": 6,
                })
                harness.write_json(root / "manifest.json", manifest)
                if isinstance(native, BaseException):
                    decoder = mock.patch.object(harness.subprocess, "run", side_effect=native)
                else:
                    result = subprocess.CompletedProcess(("perf", "script"), native)
                    decoder = mock.patch.object(harness.subprocess, "run", return_value=result)
                with mock.patch.object(harness.shutil, "which", return_value="/usr/bin/perf"), decoder:
                    return harness.validate_lifecycle_artifact(root)

        cases = (
            ("native-accepted", valid_payload, 0, True),
            ("native-rejected", valid_payload, 1, False),
            ("missing-perf", valid_payload, FileNotFoundError("perf"), False),
            ("no-sample", bytes(no_sample), 0, False),
            ("sample-then-zero-size-record", bytes(zero_size_record), 0, False),
            ("sample-then-trailing-garbage", bytes(trailing_garbage), 0, False),
            ("header-only", b"PERFILE2", AssertionError("native decoder called"), False),
            ("truncated-data", valid_payload[:-1], AssertionError("native decoder called"), False),
            (
                "data-overlaps-header",
                valid_payload[:40] + (1).to_bytes(8, "little") + valid_payload[48:],
                AssertionError("native decoder called"),
                False,
            ),
        )
        for label, payload, native, expected_complete in cases:
            with self.subTest(payload=label):
                got = validate(payload, native)

                if expected_complete:
                    self.assertTrue(got["complete"], got)
                else:
                    self.assertFalse(got["analyzable"], got)
                    self.assertTrue(any("content does not match" in item for item in got["errors"]), got)

    def test_timestamp_overflow_is_structurally_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            events[-1]["timestamp"] = "9999-12-31T23:59:59-23:59"
            rewrite_lifecycle_fixture(root, manifest, events)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("RFC3339 timestamp" in item for item in got["errors"]), got)

    def test_timestamp_requires_lexical_rfc3339(self) -> None:
        malformed = (
            "2026-W35-4T00:00:13Z",
            "20260827T000013Z",
            "2026-08-27 00:00:13Z",
        )
        for timestamp in malformed:
            with self.subTest(timestamp=timestamp), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest, events = lifecycle_fixture(root)
                events[-1]["timestamp"] = timestamp
                rewrite_lifecycle_fixture(root, manifest, events)

                got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["analyzable"], got)
                self.assertTrue(any("RFC3339 timestamp" in item for item in got["errors"]), got)

        for timestamp in (
            "2026-08-27T00:00:13Z",
            "2026-08-27T00:00:13.500Z",
            "2026-08-27T00:00:13+00:00",
            "2026-08-26T14:00:13-10:00",
        ):
            with self.subTest(valid_timestamp=timestamp):
                errors = []
                self.assertIsNotNone(harness._utc_timestamp(timestamp, "timestamp", errors))
                self.assertEqual(errors, [])

    def test_rows_wal_and_counters_must_be_monotonic(self) -> None:
        mutations = (
            (lambda rows: rows[5]["state"]["rows"].__setitem__("server_durable", 0), "rows.server_durable decreased"),
            (lambda rows: rows[5]["state"]["wal"].__setitem__("frontier", 0), "wal.frontier decreased"),
            (
                lambda rows: rows[7]["state"]["counters"].__setitem__("indexed_stage_docs_total", 0),
                "counters.indexed_stage_docs_total decreased",
            ),
        )
        for mutation, expected in mutations:
            with self.subTest(expected=expected):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, events = lifecycle_fixture(root)
                    mutation(events)
                    rewrite_lifecycle_fixture(root, manifest, events)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["complete"])
                self.assertTrue(any(expected in item for item in got["errors"]), got)

    def test_completed_wal_profiles_require_progress_with_no_wal_and_partial_exemptions(self) -> None:
        for profile in ("command_wal_durable", "command_wal_relaxed", "no_wal_fast"):
            with self.subTest(profile=profile):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, events = lifecycle_fixture(root)
                    manifest["service"]["profile"] = profile
                    manifest["service"]["command"][-1] = profile
                    manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                    for event in events:
                        event["state"]["wal"] = {"frontier": 0, "bytes_written_total": 0}
                        event["state"]["counters"]["wal_write_bytes_total"] = 0
                    diagnostics = root / "diagnostics.jsonl"
                    diagnostic_records = [
                        json.loads(line) for line in diagnostics.read_text(encoding="utf-8").splitlines()
                    ]
                    for record in diagnostic_records:
                        record["snapshot"]["database"]["treedb.command_wal.durable_wal_lsn"] = 0
                        record["snapshot"]["database"]["treedb.command_wal.write.bytes_total"] = 0
                    diagnostics.write_text(
                        "".join(json.dumps(record, sort_keys=True) + "\n" for record in diagnostic_records),
                        encoding="utf-8",
                    )
                    next(
                        artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                        if artifact["path"] == "diagnostics.jsonl"
                    )["sha256"] = harness.sha256_file(diagnostics)
                    rewrite_lifecycle_fixture(root, manifest, events)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertTrue(got["analyzable"], got)
                self.assertFalse(got["complete"], got)
                if profile == "command_wal_durable":
                    self.assertTrue(
                        any("requires positive wal.frontier" in item for item in got["completion_errors"]), got,
                    )
                    self.assertTrue(
                        any("requires positive wal.bytes_written_total" in item for item in got["completion_errors"]),
                        got,
                    )
                else:
                    self.assertIn("completed lifecycle requires command_wal_durable", got["completion_errors"])

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            manifest["lifecycle"]["result_status"] = "partial"
            manifest["lifecycle"]["profiles"] = []
            for event in events:
                event["state"]["wal"] = {"frontier": 0, "bytes_written_total": 0}
            rewrite_lifecycle_fixture(root, manifest, events[:4])
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = harness.main(["--validate-lifecycle", str(root), "--allow-partial"])

        self.assertEqual(exit_code, 0)

    def test_empty_counters_are_structurally_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            manifest["lifecycle"]["result_status"] = "partial"
            for event in events:
                event["state"]["counters"] = {}
            rewrite_lifecycle_fixture(root, manifest, events)

            got = harness.validate_lifecycle_artifact(root)
            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = harness.main(["--validate-lifecycle", str(root), "--allow-partial"])

        self.assertFalse(got["analyzable"])
        self.assertFalse(got["complete"])
        self.assertTrue(any("non-empty cumulative counter" in item for item in got["errors"]), got)
        self.assertEqual(exit_code, 1)


class ProtocolMeasurementProducerTest(unittest.TestCase):
    def test_protocol_canonical_sha256_includes_record_terminator(self) -> None:
        value = {"b": 2, "a": 1}
        expected = hashlib.sha256(b'{"a":1,"b":2}\n').hexdigest()
        self.assertEqual(harness.protocol_canonical_sha256(value), expected)

    def test_isolation_monitor_rethrows_sampler_failure(self) -> None:
        initial = {
            "timestamp": "2026-09-02T00:00:00Z",
            "swap_used_bytes": 0,
            "competing_processes": [],
        }
        with mock.patch.object(
            harness, "isolation_sample",
            side_effect=[initial, RuntimeError("sample failed")],
        ):
            monitor = harness.IsolationMonitor(0.001)
            self.assertTrue(monitor.stop_event.wait(1))
            with self.assertRaisesRegex(RuntimeError, "isolation monitor failed"):
                monitor.stop()

    def test_construction_measurements_require_positive_work_in_each_phase(self) -> None:
        for phase in ("planning", "reciprocal"):
            for invalid in (None, False, 0, -1, 1.0):
                with self.subTest(phase=phase, invalid=invalid):
                    decisions = {
                        "planning": {"decisions": 1},
                        "reciprocal": {"decisions": 1},
                    }
                    decisions[phase]["decisions"] = invalid
                    with self.assertRaisesRegex(RuntimeError, f"positive {phase}"):
                        harness.require_positive_construction_work(decisions)

    def test_diagnostics_sampler_records_first_sample(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            harness, "http_json", return_value={"status": "ok"},
        ):
            root = Path(tmp)
            sampler = harness.DiagnosticsSampler(
                "http://127.0.0.1:1/diagnostics", root / "diagnostics.jsonl", 60, root,
            )
            sample = sampler.sample()

        self.assertEqual(sampler.samples, [sample])

    def test_linux_process_metrics_are_positive(self) -> None:
        got = harness.linux_process_metrics(os.getpid())
        self.assertGreaterEqual(got["cpu_nanoseconds"], 0)
        self.assertGreater(got["peak_rss_bytes"], 0)

    def test_measurements_are_derived_from_raw_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_dir = root / "treedb-data"
            (data_dir / "maindb").mkdir(parents=True)
            (data_dir / "maindb" / "index.db").write_bytes(b"persistent")
            records = [
                {"event": "reset", "timestamp_ns": 1, "response": {}},
                {"event": "load_start", "timestamp_ns": 2},
                {
                    "event": "batch_accepted",
                    "timestamp_ns": 3,
                    "client_sent": 1,
                    "server_accepted": 1,
                },
                {"event": "load_end", "timestamp_ns": 4},
                {"event": "optimize_start", "timestamp_ns": 1_000_000_000},
                {
                    "event": "optimize_end",
                    "timestamp_ns": 3_000_000_000,
                    "response": {
                        "status": {
                            "column_graph_build": {
                                "adjacency_build_nanos": 500_000_000,
                                "construction_decisions": {
                                    "planning": {"decisions": 1},
                                    "reciprocal": {"decisions": 1},
                                },
                            }
                        }
                    },
                },
                {"event": "cache_prime", "timestamp_ns": 4_000_000_000},
                {"event": "cache_warm", "timestamp_ns": 5_000_000_000},
            ]
            (root / "adapter-lifecycle.jsonl").write_text(
                "".join(json.dumps(record) + "\n" for record in records),
                encoding="utf-8",
            )
            diagnostic_records = [
                {
                    "boundary": "optimize_start",
                    "process": {"cpu_nanoseconds": 1_000_000_000, "peak_rss_bytes": 100},
                    "snapshot": {"treedb.process.memory.total_alloc_bytes": "1000"},
                },
                {
                    "boundary": "optimize_end",
                    "process": {"cpu_nanoseconds": 3_000_000_000, "peak_rss_bytes": 200},
                    "snapshot": {"treedb.process.memory.total_alloc_bytes": "2000"},
                },
            ]
            (root / "diagnostics.jsonl").write_text(
                "".join(json.dumps(record) + "\n" for record in diagnostic_records),
                encoding="utf-8",
            )
            (root / "lifecycle.jsonl").write_text(
                "\n".join([
                    json.dumps({"stage": "startup", "timestamp": "2026-09-02T00:00:00Z"}),
                    json.dumps({"stage": "teardown", "timestamp": "2026-09-02T00:01:00Z"}),
                    "",
                ]),
                encoding="utf-8",
            )
            harness.write_json(root / "isolation.json", {"complete": True})
            args = SimpleNamespace(
                data_dir=data_dir,
                lifecycle_dimensions=768,
                lifecycle_vectors=250000,
                m=16,
                ef_construction=128,
                ef_search=192,
                rerank_candidates=400,
                k=100,
                measurement_run_id="screening-ef128",
                measurement_dataset_sha256="d" * 64,
                measurement_role="screening_candidate",
                measurement_partition="selection",
            )
            harness.write_protocol_measurements(
                harness.HarnessState(root=root), args, "e" * 40
            )
            measurements = json.loads((root / "measurements.json").read_text())
            source = json.loads((root / "measurement-source.json").read_text())
            configuration = {
                "dimensions": 768,
                "metric": "cosine",
                "m": 16,
                "ef_construction": 128,
                "ef_search": 192,
                "configured_rerank_candidates": 400,
                "effective_rerank_candidates": 192,
                "top_k": 100,
            }
            manifest = {
                "lifecycle": {
                    "expected_rows": 250000,
                    "raw_artifacts": [
                        {"path": "adapter-lifecycle.jsonl",
                         "sha256": harness.sha256_file(root / "adapter-lifecycle.jsonl")},
                        {"path": "diagnostics.jsonl",
                         "sha256": harness.sha256_file(root / "diagnostics.jsonl")},
                    ],
                },
                "service": {"data_dir": str(data_dir)},
            }
            validator_values = policy.measurement_source_values(
                root,
                measurements["source"],
                measurements["phase_seconds"],
                measurements["diagnostic_work_profile"],
                configuration,
                manifest,
                source["isolation"],
            )
            self.assertEqual(measurements["determinism"], validator_values["determinism"])

        self.assertEqual(measurements["phase_seconds"], {"adjacency": 0.5, "optimize": 2.0})
        self.assertEqual(measurements["cpu_utilization_logical_cores"], 1.0)
        self.assertEqual(measurements["resources"], {
            "peak_rss_bytes": 200,
            "persisted_bytes": 10,
            "cumulative_allocated_bytes": 2000.0,
        })
        self.assertEqual(measurements["origin"]["run_id"], "screening-ef128")
        self.assertEqual(source["data_files"][0]["path"], "maindb/index.db")
        self.assertEqual(source["isolation"]["path"], "isolation.json")


class LifecycleIntegrationTest(unittest.TestCase):
    def _complete_fixture(self, root: Path):
        dataset = root / "train.parquet"
        dataset.write_bytes(b"dataset")
        args = harness.parse_args([
            "--out", str(root), "--run-vdbbench", "--rows", "exact",
            "--case-type", "PerformanceCustomDataset",
            "--lifecycle", "--lifecycle-dataset-file", str(dataset),
            "--lifecycle-vectors", "50000", "--lifecycle-dimensions", "768",
            "--k", "2",
            "--service-close-timeout", "1",
        ])
        state = harness.HarnessState(root=root, lifecycle_started_ns=1)
        state.lifecycle = harness.lifecycle_metadata(state, args)
        task_config = {
            "db_config": {"index_name": harness.lifecycle_index_name(args)},
            "case_config": {"custom_case": {"dataset_config": {
                "size": "50000", "dim": "768", "dir": str(root),
                "file_count": "1", "use_shuffled": False,
            }}},
        }
        state.vdbbench = [{"load_metrics": {
            "result_file": "vdbbench-results/result.json",
            "result_sha256": "a" * 64,
            "task_config": task_config,
            "task_config_sha256": harness.canonical_sha256(task_config),
        }}]
        snapshot = {
            "database": {
                "treedb.commit_seq": "10",
                "treedb.command_wal.write.bytes_total": "100",
                "treedb.command_wal.durable_wal_lsn": "5",
                "treedb.command_wal.live_accepted_max_lsn": "5",
            },
            "collections": {},
        }
        boundary_snapshots = {
            boundary: {**snapshot, "phase": boundary}
            for boundary in harness.LIFECYCLE_DIAGNOSTIC_BOUNDARIES
        }

        class Sampler:
            samples = [{"timestamp_ns": 1, "snapshot": snapshot}] + [
                {
                    "timestamp_ns": timestamp_ns,
                    "snapshot": boundary_snapshots[boundary],
                    "boundary": boundary,
                    "boundary_timestamp_ns": timestamp_ns,
                }
                for boundary, timestamp_ns in zip(
                    harness.LIFECYCLE_DIAGNOSTIC_BOUNDARIES,
                    (5, 6, 7, 1_000_000_008, 2_000_000_009),
                    strict=True,
                )
            ]

            def stop(self):
                return None

            def at(self, timestamp_ns):
                return snapshot

            def sample(self):
                record = {"timestamp_ns": 3_000_000_010, "snapshot": snapshot}
                self.samples.append(record)
                return record

        harness.LifecycleJournal(root / "lifecycle.jsonl").append(
            "startup", harness.LifecycleStateBuilder().build(snapshot, harness.lifecycle_rows()),
            timestamp=harness.iso_from_ns(1),
        )
        records = [
            {"event": "reset", "timestamp_ns": 2, "response": {}},
            {"event": "load_start", "timestamp_ns": 3},
            {"event": "batch_accepted", "timestamp_ns": 4, "client_sent": 50000, "server_accepted": 50000},
            {"event": "load_end", "timestamp_ns": 5},
            {"event": "optimize_start", "timestamp_ns": 6},
            {
                "event": "optimize_end", "timestamp_ns": 7,
                "response": {
                    "index": {
                        "name": harness.lifecycle_index_name(args),
                        "generation": 7,
                        "vector_strategy": "column_graph",
                        "vector_m": args.m,
                        "vector_ef_construction": args.ef_construction,
                    },
                    "vector_index_name": "vector_hnsw",
                    "status": {
                        "root_id": 0,
                        "strategy": "column_graph",
                        "state": "column_graph_loaded",
                        "loaded": True,
                        "rebuild_needed": False,
                    },
                },
            },
            {"event": "cache_prime", "timestamp_ns": 1_000_000_008},
            {"event": "cache_warm", "timestamp_ns": 2_000_000_009},
        ]
        (root / "adapter-lifecycle.jsonl").write_text(
            "".join(json.dumps(record) + "\n" for record in records), encoding="utf-8"
        )
        harness.write_json(root / "lifecycle-boundary-diagnostics.json", {
            "boundary": "cache_warm",
            "boundary_timestamp_ns": 2_000_000_009,
            "sample_timestamp_ns": 2_000_000_009,
        })
        proc = mock.Mock(returncode=None)
        return args, state, Sampler(), proc

    def test_build_boundaries_use_synchronous_samples_when_periodic_sample_is_stale(self) -> None:
        stale = {"phase": "stale"}
        snapshots = {
            "load_end": {"phase": "loaded"},
            "optimize_start": {"phase": "build-started"},
            "optimize_end": {"phase": "build-finished"},
            "cache_prime": {"phase": "serial-finished"},
            "cache_warm": {"phase": "concurrent-finished"},
        }

        class Sampler:
            samples = [{"timestamp_ns": 2, "snapshot": stale}] + [
                {
                    "timestamp_ns": timestamp_ns + 1,
                    "snapshot": snapshots[boundary],
                    "boundary": boundary,
                    "boundary_timestamp_ns": timestamp_ns,
                }
                for boundary, timestamp_ns in zip(
                    harness.LIFECYCLE_DIAGNOSTIC_BOUNDARIES, (4, 6, 8, 10, 12), strict=True
                )
            ]

        for boundary, timestamp_ns in zip(
            harness.LIFECYCLE_DIAGNOSTIC_BOUNDARIES, (4, 6, 8, 10, 12), strict=True
        ):
            with self.subTest(boundary=boundary):
                got = harness.boundary_diagnostics_snapshot(boundary, timestamp_ns, Sampler())
                self.assertIs(got, snapshots[boundary])

    def test_build_boundary_rejects_periodic_sample_without_exact_tag(self) -> None:
        sampler = mock.Mock(samples=[{"timestamp_ns": 9, "snapshot": {"periodic": True}}])

        with self.assertRaisesRegex(ValueError, "optimize_end diagnostics has no exact sampled snapshot"):
            harness.boundary_diagnostics_snapshot("optimize_end", 8, sampler)

    def test_lifecycle_rejects_standard_case_without_checksum_bound_task_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stderr(io.StringIO()) as stderr:
            dataset = Path(tmp) / "train.parquet"
            dataset.write_bytes(b"dataset")
            with self.assertRaises(SystemExit):
                harness.parse_args([
                    "--run-vdbbench", "--rows", "exact", "--lifecycle",
                    "--lifecycle-dataset-file", str(dataset),
                ])

        self.assertIn("requires PerformanceCustomDataset", stderr.getvalue())

    def test_complete_lifecycle_binds_heap_to_actual_capture_stages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args, state, sampler, proc = self._complete_fixture(root)
            args.construction_decision_diagnostics = True
            reopened = mock.Mock(returncode=None)
            snapshot = sampler.samples[0]["snapshot"]
            responses = iter([
                snapshot,
                {
                    "count": 50000,
                    "index": {
                        "name": harness.lifecycle_index_name(args),
                        "vector_index_name": "vector_hnsw",
                        "generation": 7,
                    },
                },
                {"index": {"generation": 7}},
                {
                    "index": {"name": harness.lifecycle_index_name(args), "generation": 7},
                    "vector_index_name": "vector_hnsw",
                    "query_mode": "exact",
                    "quantized_index_name": None,
                    "no_documents": True,
                    "results": [{"id": "1"}, {"id": "2"}],
                    "stats": {"search_route_hnsw_search_pack": 1},
                    "diagnostics": {
                        "route": "exact_hnsw_search_pack_v1",
                        "fallback_reason": "none",
                        "no_document_guardrails_ok": True,
                        "exact_hnsw_search_pack_no_doc_route": True,
                    },
                },
            ])

            def fetch(_url, path):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"profile")

            with mock.patch.object(harness, "fetch_file", side_effect=fetch), \
                    mock.patch.object(harness, "http_json", side_effect=lambda *_args, **_kwargs: next(responses)), \
                    mock.patch.object(harness, "close_process_group_cleanly"), \
                    mock.patch.object(
                        harness, "start_service", return_value=(reopened, {}, ["service"])
                    ) as start_service:
                harness.complete_lifecycle(state, args, root, root / "service", proc, sampler)
                self.assertTrue(
                    start_service.call_args.kwargs["construction_decision_diagnostics"]
                )

            profile = state.lifecycle["profiles"][0]
            raw_paths = {artifact["path"] for artifact in state.lifecycle["raw_artifacts"]}
            events = [
                json.loads(line)
                for line in (root / "lifecycle.jsonl").read_text(encoding="utf-8").splitlines()
            ]

        self.assertEqual((profile["before_sequence"], profile["after_sequence"]), (8, 9))
        self.assertIn("lifecycle_count_response.json", raw_paths)
        self.assertEqual(events[8]["stage"], "cache_warm")
        self.assertEqual(events[9]["stage"], "graceful_close")
        self.assertLess(events[6]["timestamp"], events[7]["timestamp"])
        self.assertLess(events[7]["timestamp"], events[8]["timestamp"])

    def test_loaded_route_proof_requires_exact_requested_results(self) -> None:
        valid_response = {
            "index": {"name": "cohere", "generation": 7},
            "vector_index_name": "vector_hnsw",
            "query_mode": "exact",
            "quantized_index_name": None,
            "no_documents": True,
            "results": [{"id": "1"}, {"id": "2"}],
            "stats": {"search_route_hnsw_search_pack": 1},
            "diagnostics": {
                "route": "exact_hnsw_search_pack_v1",
                "fallback_reason": "none",
                "no_document_guardrails_ok": True,
                "exact_hnsw_search_pack_no_doc_route": True,
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args, state, _sampler, _proc = self._complete_fixture(root)
            responses = iter([{"index": {"generation": 7}}, valid_response])
            with mock.patch.object(harness, "http_json", side_effect=lambda *_args, **_kwargs: next(responses)):
                route = harness.run_loaded_route_proof(state, args, "cohere", "cohere:vector_hnsw", 7, 7)
            persisted_response = json.loads(
                (root / "lifecycle_route_response.json").read_text(encoding="utf-8")
            )

        self.assertEqual(route["requested_top_k"], 2)
        self.assertEqual(route["result_count"], 2)
        self.assertEqual(route["effective_ef_search"], args.ef_search)
        self.assertEqual(persisted_response["request_ef_search"], args.ef_search)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args, state, _sampler, _proc = self._complete_fixture(root)
            args.rows = "scalar"
            scalar_response = json.loads(json.dumps(valid_response))
            scalar_response.update({
                "query_mode": "quantized_rerank",
                "quantized_index_name": args.quantized_index_name,
                "quantized_rerank_candidates": max(args.rerank_candidates, args.k),
            })
            scalar_response["diagnostics"]["route"] = "quantized_rerank"
            responses = iter([{"index": {"generation": 7}}, scalar_response])
            with mock.patch.object(
                harness, "http_json", side_effect=lambda *_args, **_kwargs: next(responses)
            ):
                scalar_route = harness.run_loaded_route_proof(
                    state, args, "cohere", "cohere:vector_hnsw", 7, 7
                )

        self.assertEqual(scalar_route["name"], "quantized_rerank")

        for results in (
            [],
            [{"id": "1"}],
            {"0": {"id": "1"}},
            ["malformed", {"id": "2"}],
            [{}, {"id": "2"}],
            [{"id": ""}, {"id": "2"}],
            [{"id": 1}, {"id": "2"}],
            [{"id": "1"}, {"id": "1"}],
        ):
            with self.subTest(results=results), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                args, state, _sampler, _proc = self._complete_fixture(root)
                response = dict(valid_response, results=results)
                responses = iter([{"index": {"generation": 7}}, response])
                with mock.patch.object(harness, "http_json", side_effect=lambda *_args, **_kwargs: next(responses)), \
                        self.assertRaisesRegex(RuntimeError, "exactly the requested results"):
                    harness.run_loaded_route_proof(state, args, "cohere", "cohere:vector_hnsw", 7, 7)

        for stats in ([], "malformed"):
            with self.subTest(stats=stats), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                args, state, _sampler, _proc = self._complete_fixture(root)
                response = dict(valid_response, stats=stats)
                responses = iter([{"index": {"generation": 7}}, response])
                with mock.patch.object(harness, "http_json", side_effect=lambda *_args, **_kwargs: next(responses)), \
                        self.assertRaisesRegex(RuntimeError, "stats must be an object"):
                    harness.run_loaded_route_proof(
                        state, args, "cohere", "cohere:vector_hnsw", 7, 7
                    )

        guardrail_mutations = (
            lambda response: response["stats"].__setitem__("documents_fetched", 1),
            lambda response: response["stats"].__setitem__("document_bytes", 1),
            lambda response: response["stats"].__setitem__("document_output_bytes", 1),
            lambda response: response["diagnostics"].__setitem__(
                "no_document_guardrails_ok", False
            ),
        )
        for mutation in guardrail_mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                args, state, _sampler, _proc = self._complete_fixture(root)
                response = json.loads(json.dumps(valid_response))
                mutation(response)
                responses = iter([{"index": {"generation": 7}}, response])
                with mock.patch.object(
                    harness, "http_json", side_effect=lambda *_args, **_kwargs: next(responses)
                ), self.assertRaisesRegex(RuntimeError, "zero-fetch no-document guardrails"):
                    harness.run_loaded_route_proof(
                        state, args, "cohere", "cohere:vector_hnsw", 7, 7
                    )

        malformed = (
            ([], "route proof response must be an object"),
            (dict(valid_response, index=[]), "index generation"),
            (dict(valid_response, index={"generation": 8}), "index generation"),
            (
                dict(valid_response, index={"name": "other", "generation": 7}),
                "index identity",
            ),
            (dict(valid_response, vector_index_name="other"), "index identity"),
            (dict(valid_response, query_mode="quantized_rerank"), "search configuration"),
            (dict(valid_response, quantized_index_name="other"), "search configuration"),
            (dict(valid_response, quantized_rerank_candidates=32), "search configuration"),
            (dict(valid_response, diagnostics=[]), "diagnostics"),
            (dict(valid_response, no_documents="true"), "no-document"),
        )
        for response, expected in malformed:
            with self.subTest(expected=expected), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                args, state, _sampler, _proc = self._complete_fixture(root)
                responses = iter([{"index": {"generation": 7}}, response])
                with mock.patch.object(harness, "http_json", side_effect=lambda *_args, **_kwargs: next(responses)), \
                        self.assertRaisesRegex(RuntimeError, expected):
                    harness.run_loaded_route_proof(
                        state, args, "cohere", "cohere:vector_hnsw", 7, 7
                    )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args, state, _sampler, _proc = self._complete_fixture(root)
            with mock.patch.object(harness, "http_json", return_value=[]), \
                    self.assertRaisesRegex(RuntimeError, "index response must be an object"):
                harness.run_loaded_route_proof(
                    state, args, "cohere", "cohere:vector_hnsw", 7, 7
                )

    def test_column_graph_ready_asset_uses_index_generation_not_root_id(self) -> None:
        optimize = {
            "index": {"name": "cohere", "generation": 7, "vector_strategy": "column_graph"},
            "vector_index_name": "embedding",
            "status": {
                "root_id": 0,
                "strategy": "column_graph",
                "state": "column_graph_loaded",
                "loaded": True,
                "rebuild_needed": False,
            },
        }

        identity, generation, reopen_generation = harness.lifecycle_ready_asset(optimize, "cohere")

        self.assertEqual(identity, "cohere:embedding")
        self.assertEqual(generation, 7)
        self.assertEqual(reopen_generation, 7)

    def test_column_graph_ready_asset_rejects_stale_or_root_substituted_generation(self) -> None:
        optimize = {
            "index": {"name": "cohere", "generation": 0, "vector_strategy": "column_graph"},
            "vector_index_name": "embedding",
            "status": {
                "root_id": 9,
                "strategy": "column_graph",
                "state": "column_graph_loaded",
                "loaded": True,
                "rebuild_needed": False,
            },
        }

        with self.assertRaisesRegex(RuntimeError, "durable column graph"):
            harness.lifecycle_ready_asset(optimize, "cohere")

    def test_completed_lifecycle_rejects_native_runtime_route_asset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _events = lifecycle_fixture(root)
            sidecar = root / "adapter-lifecycle.jsonl"
            records = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]
            optimize = records[-3]["response"]
            optimize["index"]["vector_strategy"] = "native_runtime"
            optimize["status"].update({
                "strategy": "native_runtime", "state": "native_runtime", "root_id": 7,
            })
            sidecar.write_text(
                "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
                encoding="utf-8",
            )
            next(
                artifact for artifact in manifest["lifecycle"]["raw_artifacts"]
                if artifact["path"] == "adapter-lifecycle.jsonl"
            )["sha256"] = harness.sha256_file(sidecar)
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"], got)
        self.assertTrue(any("column graph" in error for error in got["errors"]), got)

    def test_native_ready_asset_uses_positive_root_id(self) -> None:
        optimize = {
            "index": {"name": "cohere", "generation": 7, "vector_strategy": "native_runtime"},
            "vector_index_name": "embedding",
            "status": {
                "root_id": 9,
                "strategy": "native_runtime",
                "state": "native_runtime",
                "loaded": True,
                "rebuild_needed": False,
            },
        }

        self.assertEqual(
            harness.lifecycle_ready_asset(optimize, "cohere"),
            ("cohere:embedding", 9, None),
        )

    def test_column_graph_route_proof_rejects_stale_reopen_generation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = harness.HarnessState(root=Path(tmp))
            args = harness.parse_args(["--rows", "exact"])
            with mock.patch.object(harness, "http_json", return_value={"index": {"generation": 8}}):
                with self.assertRaisesRegex(RuntimeError, "does not match"):
                    harness.run_loaded_route_proof(
                        state,
                        args,
                        "cohere",
                        "cohere:embedding",
                        7,
                        expected_service_generation=7,
                    )

    def test_lifecycle_journal_is_append_only_and_sequence_bound(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "lifecycle.jsonl"
            journal = harness.LifecycleJournal(path)
            first = journal.append("startup", {"rows": {"client_sent": 0}})
            second = journal.append("reset", {"rows": {"client_sent": 0}})

            events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]

        self.assertEqual((first, second), (0, 1))
        self.assertEqual([event["stage"] for event in events], ["startup", "reset"])
        self.assertEqual([event["sequence"] for event in events], [0, 1])
        self.assertTrue(all(event["schema_version"] == harness.LIFECYCLE_EVENT_SCHEMA for event in events))

    def test_adapter_lifecycle_sidecar_reconciles_load_and_optimize(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "adapter-lifecycle.jsonl"
            records = (
                {"event": "reset", "timestamp_ns": 1, "response": {"generation": 1}},
                {"event": "load_start", "timestamp_ns": 2},
                {"event": "batch_accepted", "timestamp_ns": 4, "client_sent": 3, "server_accepted": 3},
                {"event": "batch_accepted", "timestamp_ns": 3, "client_sent": 2, "server_accepted": 2},
                {"event": "load_end", "timestamp_ns": 5},
                {"event": "optimize_start", "timestamp_ns": 6},
                {
                    "event": "optimize_end",
                    "timestamp_ns": 7,
                    "response": {"index": {"name": "cohere", "generation": 7}, "status": {"root_id": 9}},
                },
                {"event": "cache_prime", "timestamp_ns": 8},
                {"event": "cache_warm", "timestamp_ns": 9},
            )
            path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")

            got = harness.read_adapter_lifecycle_sidecar(path)

        self.assertEqual(got["client_sent"], 5)
        self.assertEqual(got["server_accepted"], 5)
        self.assertEqual(got["load_start_ns"], 2)
        self.assertEqual(got["load_end_ns"], 5)
        self.assertEqual(got["cache_prime_ns"], 8)
        self.assertEqual(got["cache_warm_ns"], 9)
        self.assertEqual(got["optimize_response"]["status"]["root_id"], 9)

    def test_adapter_lifecycle_sidecar_rejects_partial_or_missing_boundaries(self) -> None:
        cases = {
            "partial write": '{"event":"reset"',
            "missing optimize": "".join(
                json.dumps(record) + "\n"
                for record in (
                    {"event": "reset", "timestamp_ns": 1, "response": {}},
                    {"event": "load_start", "timestamp_ns": 2},
                    {"event": "batch_accepted", "timestamp_ns": 3, "client_sent": 1, "server_accepted": 1},
                    {"event": "load_end", "timestamp_ns": 4},
                )
            ),
            "missing cache warm": "".join(
                json.dumps(record) + "\n"
                for record in (
                    {"event": "reset", "timestamp_ns": 1, "response": {}},
                    {"event": "load_start", "timestamp_ns": 2},
                    {"event": "batch_accepted", "timestamp_ns": 3, "client_sent": 1, "server_accepted": 1},
                    {"event": "load_end", "timestamp_ns": 4},
                    {"event": "optimize_start", "timestamp_ns": 5},
                    {"event": "optimize_end", "timestamp_ns": 6, "response": {}},
                    {"event": "cache_prime", "timestamp_ns": 7},
                )
            ),
            "malformed cache order": "".join(
                json.dumps(record) + "\n"
                for record in (
                    {"event": "reset", "timestamp_ns": 1, "response": {}},
                    {"event": "load_start", "timestamp_ns": 2},
                    {"event": "batch_accepted", "timestamp_ns": 3, "client_sent": 1, "server_accepted": 1},
                    {"event": "load_end", "timestamp_ns": 4},
                    {"event": "optimize_start", "timestamp_ns": 5},
                    {"event": "optimize_end", "timestamp_ns": 6, "response": {}},
                    {"event": "cache_warm", "timestamp_ns": 7},
                    {"event": "cache_prime", "timestamp_ns": 8},
                )
            ),
        }
        for label, content in cases.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "adapter-lifecycle.jsonl"
                path.write_text(content, encoding="utf-8")
                with self.assertRaises(ValueError):
                    harness.read_adapter_lifecycle_sidecar(path)

    def test_adapter_lifecycle_sidecar_rejects_batch_outside_load_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "adapter-lifecycle.jsonl"
            records = (
                {"event": "reset", "timestamp_ns": 1, "response": {}},
                {"event": "load_start", "timestamp_ns": 3},
                {"event": "batch_accepted", "timestamp_ns": 2, "client_sent": 1, "server_accepted": 1},
                {"event": "load_end", "timestamp_ns": 4},
                {"event": "optimize_start", "timestamp_ns": 5},
                {"event": "optimize_end", "timestamp_ns": 6, "response": {}},
                {"event": "cache_prime", "timestamp_ns": 7},
                {"event": "cache_warm", "timestamp_ns": 8},
            )
            path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "outside the load boundaries"):
                harness.read_adapter_lifecycle_sidecar(path)

    def test_build_failure_preserves_truthful_partial_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args, state, sampler, _ = self._complete_fixture(root)
            sidecar = root / "adapter-lifecycle.jsonl"
            records = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]
            sidecar.write_text(
                "".join(json.dumps(record) + "\n" for record in records[:-1]), encoding="utf-8"
            )

            harness.finalize_partial_lifecycle(state, args, sampler)
            stages = [
                json.loads(line)["stage"]
                for line in (root / "lifecycle.jsonl").read_text(encoding="utf-8").splitlines()
            ]

        self.assertEqual(state.lifecycle["result_status"], "partial")
        self.assertEqual(stages, [
            "startup", "reset", "load_start", "load_end", "optimize_start",
            "optimize_end", "cache_prime",
        ])

    def test_partial_reset_without_load_start_preserves_recorded_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args, state, sampler, _ = self._complete_fixture(root)
            sidecar = root / "adapter-lifecycle.jsonl"
            records = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]
            sidecar.write_text(json.dumps(records[0]) + "\n", encoding="utf-8")

            harness.finalize_partial_lifecycle(state, args, sampler)
            stages = [
                json.loads(line)["stage"]
                for line in (root / "lifecycle.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            milestone_exists = (root / "lifecycle_load_milestones.json").exists()

        self.assertEqual(stages, ["startup", "reset"])
        self.assertFalse(milestone_exists)

    def test_partial_malformed_batch_preserves_prior_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args, state, sampler, _ = self._complete_fixture(root)
            sidecar = root / "adapter-lifecycle.jsonl"
            records = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]
            records[2].pop("server_accepted")
            sidecar.write_text(
                "".join(json.dumps(record) + "\n" for record in records[:3]),
                encoding="utf-8",
            )

            harness.finalize_partial_lifecycle(state, args, sampler)
            stages = [
                json.loads(line)["stage"]
                for line in (root / "lifecycle.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            milestone_exists = (root / "lifecycle_load_milestones.json").exists()

        self.assertEqual(stages, ["startup", "reset", "load_start"])
        self.assertFalse(milestone_exists)

    def test_partial_lifecycle_falls_back_when_exact_boundary_sample_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args, state, sampler, _ = self._complete_fixture(root)
            sidecar = root / "adapter-lifecycle.jsonl"
            records = [json.loads(line) for line in sidecar.read_text(encoding="utf-8").splitlines()]
            sidecar.write_text(
                "".join(json.dumps(record) + "\n" for record in records[:-1]), encoding="utf-8"
            )
            sampler.samples = [sampler.samples[0]]

            harness.finalize_partial_lifecycle(state, args, sampler)
            stages = [
                json.loads(line)["stage"]
                for line in (root / "lifecycle.jsonl").read_text(encoding="utf-8").splitlines()
            ]

        self.assertEqual(stages, [
            "startup", "reset", "load_start", "load_end", "optimize_start",
            "optimize_end", "cache_prime",
        ])

    def test_complete_lifecycle_still_requires_exact_boundary_samples(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args, state, sampler, proc = self._complete_fixture(root)
            sampler.samples = [sampler.samples[0]]

            with self.assertRaisesRegex(ValueError, "load_end diagnostics has no exact sampled snapshot"):
                harness.complete_lifecycle(
                    state, args, root, root / "service", proc, sampler
                )

    def test_close_failure_never_records_graceful_close(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args, state, sampler, proc = self._complete_fixture(root)

            with mock.patch.object(harness, "fetch_file", side_effect=lambda _url, path: path.parent.mkdir(parents=True, exist_ok=True) or path.write_bytes(b"x")), \
                    mock.patch.object(harness, "http_json", return_value={"database": {}, "collections": {}}), \
                    mock.patch.object(harness, "close_process_group_cleanly", side_effect=RuntimeError("close failed")):
                with self.assertRaisesRegex(RuntimeError, "close failed"):
                    harness.complete_lifecycle(state, args, Path(tmp), Path(tmp) / "service", proc, sampler)

            stages = [json.loads(line)["stage"] for line in (root / "lifecycle.jsonl").read_text().splitlines()]

        self.assertNotIn("graceful_close", stages)
        self.assertNotEqual(state.lifecycle["result_status"], "completed")
        self.assertEqual(stages[-1], "cache_warm")

    def test_reopen_verification_failure_never_records_teardown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args, state, sampler, proc = self._complete_fixture(root)
            reopened = mock.Mock(returncode=None)

            calls = []

            def http(_method, url, _payload=None, timeout=10.0):
                calls.append(url)
                if url.endswith("/documents/count"):
                    return {"count": 49999}
                return {"database": {"treedb.commit_seq": "10"}, "collections": {}}

            with mock.patch.object(harness, "fetch_file", side_effect=lambda _url, path: path.parent.mkdir(parents=True, exist_ok=True) or path.write_bytes(b"x")), \
                    mock.patch.object(harness, "http_json", side_effect=http), \
                    mock.patch.object(harness, "close_process_group_cleanly"), \
                    mock.patch.object(harness, "terminate_process_group") as terminate, \
                    mock.patch.object(harness, "start_service", return_value=(reopened, {}, ["service"])):
                with self.assertRaisesRegex(RuntimeError, "count mismatch"):
                    harness.complete_lifecycle(state, args, Path(tmp), Path(tmp) / "service", proc, sampler)

            stages = [json.loads(line)["stage"] for line in (root / "lifecycle.jsonl").read_text().splitlines()]

        self.assertIn("cold_open_ready", stages)
        self.assertNotIn("teardown", stages)
        self.assertEqual(calls[0], args.diagnostics_url + "/debug/treedb/stats")
        self.assertTrue(calls[1].endswith("/documents/count"))
        terminate.assert_called_once_with(reopened, graceful_timeout=1.0)

    def test_reopen_keyboard_interrupt_terminates_owned_service(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args, state, sampler, proc = self._complete_fixture(root)
            reopened = mock.Mock(returncode=None)

            with mock.patch.object(
                    harness, "fetch_file",
                    side_effect=lambda _url, path: path.parent.mkdir(parents=True, exist_ok=True)
                    or path.write_bytes(b"x")), \
                    mock.patch.object(harness, "http_json", side_effect=KeyboardInterrupt), \
                    mock.patch.object(harness, "close_process_group_cleanly"), \
                    mock.patch.object(harness, "terminate_process_group") as terminate, \
                    mock.patch.object(harness, "start_service", return_value=(reopened, {}, ["service"])):
                with self.assertRaises(KeyboardInterrupt):
                    harness.complete_lifecycle(
                        state, args, root, root / "service", proc, sampler
                    )

        terminate.assert_called_once_with(reopened, graceful_timeout=1.0)


class ManifestFileListTest(unittest.TestCase):
    def test_file_identity_records_size_and_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "service"
            path.write_bytes(b"treedb")

            got = harness.file_identity(path)

        self.assertEqual(got["bytes"], 6)
        self.assertEqual(len(got["sha256"]), 64)

    def test_artifact_file_list_skips_treedb_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "manifest.json").write_text("{}", encoding="utf-8")
            (root / "treedb-data" / "maindb").mkdir(parents=True)
            (root / "treedb-data" / "maindb" / "segment.log").write_text("data", encoding="utf-8")

            files, truncated = harness.artifact_file_list(root)

        self.assertEqual(files, ["manifest.json"])
        self.assertFalse(truncated)

    def test_manifest_does_not_reference_missing_metrics_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = harness.HarnessState(root=root, vdbbench=[{"row": "exact"}])

            harness.write_manifest(state, args=harness.parse_args([]), context={}, service_command=None)

            manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
            self.assertIsNone(manifest["vdbbench_load_metrics"])
            self.assertFalse(manifest["harness"]["construction_decision_diagnostics"])
            self.assertEqual(
                manifest["harness"]["python_executable"], str(Path(sys.executable))
            )
            self.assertEqual(
                manifest["harness"]["python_sha256"],
                harness.sha256_file(Path(sys.executable)),
            )
            self.assertEqual(manifest["harness"]["use_uv"], "auto")
            self.assertNotIn("lifecycle_route_proof", manifest)

    def test_manifest_uses_service_identity_captured_before_launch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = root / "service"
            service.write_bytes(b"original")
            identity = harness.file_identity(service)
            state = harness.HarnessState(root=root, service_binary=identity)
            service.write_bytes(b"replaced")

            harness.write_manifest(
                state,
                args=harness.parse_args([]),
                context={},
                service_command=[str(service)],
            )

            manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["service"]["binary"], identity)

    def test_lifecycle_manifest_and_readme_name_cold_reopen_route_proof(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "lifecycle_route_response.json").write_text("{}\n", encoding="utf-8")
            args = harness.parse_args([])
            args.lifecycle = True
            state = harness.HarnessState(root=root)

            harness.write_readme(state, args)
            harness.write_manifest(state, args=args, context={}, service_command=None)

            manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
            readme = (root / "README.md").read_text(encoding="utf-8")

        self.assertEqual(manifest["harness"]["mode"], "vdbbench+lifecycle")
        self.assertIsNone(manifest["route_proof"])
        self.assertEqual(manifest["lifecycle_route_proof"], "lifecycle_route_response.json")
        self.assertIn("lifecycle_route_response.json", readme)
        self.assertNotIn("route_proof.json", readme)


if __name__ == "__main__":
    unittest.main()
