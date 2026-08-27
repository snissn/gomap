#!/usr/bin/env python3
"""Unit tests for treedb_vectordbbench_artifact.py helpers."""

from __future__ import annotations

import contextlib
import functools
import gzip
import io
import inspect
import json
import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

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
    service_binary = root / "bin" / "treedb-document-service"
    service_binary.parent.mkdir(parents=True)
    service_binary.write_bytes(b"fixture treedb document service\n")
    service_binary.chmod(0o755)
    service_binary_sha256 = harness.sha256_file(service_binary)
    profile = root / "profiles" / "build.cpu.pprof"
    profile.parent.mkdir(parents=True)
    profile.write_bytes(valid_pprof_fixture())
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
                "writes": 50_000 if loaded else 0,
                "checkpoints": 1 if durable else 0,
                "builds": 1 if sequence >= 6 else 0,
            },
        }
        if sequence >= 6:
            state["index"] = {"identity": "index-a", "asset_generation": 7, "status": "ready"}
        if sequence >= 9:
            state["database"] = {"identity": "database-a", "commit_seq": 50_000}
        if stage == "route_verify":
            state["route"] = {
                "name": "exact_hnsw_search_pack_v1",
                "fallback_reason": "none",
                "optimized": True,
                "index_identity": "index-a",
                "index_asset_generation": 7,
                "requested_top_k": 2,
                "result_count": 2,
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
    manifest = {
        "schema_version": harness.ARTIFACT_SCHEMA,
        "context": {
            "gomap": {"commit": "1" * 40, "dirty": False},
            "vectordbbench": {"commit": "2" * 40, "dirty": False},
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
            "profile": "command_wal_durable",
            "data_dir": str(data_dir),
            "command": [
                str(service_binary), "-dir", str(data_dir),
                "-addr", "127.0.0.1:9876", "-profile", "command_wal_durable",
            ],
            "binary": {"path": str(service_binary), "sha256": service_binary_sha256},
        },
        "harness": {
            "case_type": "Performance768D50K",
            "num_per_batch": 500,
            "num_concurrency": "32",
            "m": 16,
            "ef_construction": 128,
        },
        "vdbbench": [],
    }
    manifest["lifecycle"] = {
        "schema_version": harness.LIFECYCLE_SCHEMA,
        "result_status": "completed",
        "file": "lifecycle.jsonl",
        "sha256": harness.sha256_file(lifecycle_path),
        "expected_rows": 50_000,
        "dataset": {"name": "cohere-50k", "sha256": "4" * 64, "dimensions": 768, "vectors": 50_000},
        "identity": {
            "gomap_commit": "1" * 40,
            "vectordbbench_commit": "2" * 40,
            "service_binary_sha256": service_binary_sha256,
            "config_sha256": harness.lifecycle_config_sha256(manifest),
        },
        "raw_artifacts": [{"path": "profiles/build.cpu.pprof", "sha256": harness.sha256_file(profile)}],
        "profiles": [{
            "path": "profiles/build.cpu.pprof",
            "sha256": harness.sha256_file(profile),
            "kind": "cpu",
            "before_sequence": 5,
            "after_sequence": 6,
        }],
    }
    harness.write_json(root / "manifest.json", manifest)
    return manifest, events


def rewrite_lifecycle_fixture(root: Path, manifest: dict, events: list[dict]) -> None:
    lifecycle_path = root / "lifecycle.jsonl"
    lifecycle_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in events), encoding="utf-8")
    manifest["lifecycle"]["sha256"] = harness.sha256_file(lifecycle_path)
    harness.write_json(root / "manifest.json", manifest)


class RouteProofSummaryTest(unittest.TestCase):
    def test_iso_now_is_utc(self) -> None:
        self.assertTrue(harness.iso_now().endswith("Z"))

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

            args = harness.parse_args([*base, "--skip-search-serial"])

        self.assertTrue(args.skip_search_serial)
        self.assertFalse(args.skip_search_concurrent)

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
            "boundary": "optimize_end",
            "boundary_timestamp_ns": 8,
            "sample_timestamp_ns": 9,
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

    def test_manifest_storage_requires_meaningful_identity_and_capacity(self) -> None:
        invalid_storage = (
            {"mount": "unavailable: findmnt missing"},
            {"path": "/tmp", "method": "findmnt", "device": "/dev/x", "filesystem": "xfs", "mount": "/tmp", "capacity_bytes": 0},
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
            task_config = {"case_config": {"custom_case": {"dataset_config": {
                "dir": str(dataset_dir), "size": 50000, "dim": 768,
                "file_count": "1", "use_shuffled": False,
            }}}}
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
            manifest["vdbbench"] = [{"load_metrics": metrics}]
            manifest["lifecycle"]["dataset"]["sha256"] = harness.sha256_file(dataset_file)
            manifest["lifecycle"]["task_config_binding"] = {
                key: metrics[key] for key in ("result_file", "result_sha256", "task_config_sha256")
            }
            manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)
            self.assertTrue(got["complete"], got)

            for malformed_results in (None, {"task_config": task_config}):
                with self.subTest(malformed_results=malformed_results):
                    result.write_text(json.dumps({"results": malformed_results}), encoding="utf-8")
                    metrics["result_sha256"] = harness.sha256_file(result)
                    manifest["lifecycle"]["task_config_binding"]["result_sha256"] = metrics["result_sha256"]
                    harness.write_json(root / "manifest.json", manifest)

                    malformed = harness.validate_lifecycle_artifact(root)

                    self.assertFalse(malformed["analyzable"], malformed)
                    self.assertTrue(any("results must be a list" in error for error in malformed["errors"]), malformed)

            result.write_text(json.dumps({"results": [{"task_config": task_config}]}), encoding="utf-8")
            metrics["result_sha256"] = harness.sha256_file(result)
            manifest["lifecycle"]["task_config_binding"]["result_sha256"] = metrics["result_sha256"]

            task_config["case_config"]["custom_case"]["dataset_config"]["dim"] = 769
            metrics["task_config_sha256"] = harness.canonical_sha256(task_config)
            manifest["lifecycle"]["task_config_binding"]["task_config_sha256"] = metrics["task_config_sha256"]
            result.write_text(json.dumps({"results": [{"task_config": task_config}]}), encoding="utf-8")
            metrics["result_sha256"] = harness.sha256_file(result)
            manifest["lifecycle"]["task_config_binding"]["result_sha256"] = metrics["result_sha256"]
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
                manifest["vdbbench"] = [{"load_metrics": metrics}]
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

        self.assertTrue(got["analyzable"])
        self.assertFalse(got["complete"])
        self.assertEqual(got["t_ready_seconds"], 0.0)
        self.assertIn("T_ready must be strictly positive", got["completion_errors"])

    def test_interrupted_fixture_is_analyzable_but_never_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            manifest["lifecycle"]["result_status"] = "interrupted"
            manifest["lifecycle"]["profiles"] = []
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
            (12, lambda state: state["route"].__setitem__("requested_top_k", "2"), "route.requested_top_k"),
            (12, lambda state: state["route"].__setitem__("result_count", []), "route.result_count"),
        ]
        for field in (
            "name", "fallback_reason", "optimized", "index_identity", "index_asset_generation",
            "requested_top_k", "result_count",
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

    def test_service_pprof_address_matches_loopback_listener_contract(self) -> None:
        valid = ("", "127.0.0.1:6060", "localhost:6060", "[::1]:6060", "127.0.0.1:65535")
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
            ("PerformanceCustomDataset", "task_config dataset shape"),
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
            late["state"]["counters"]["writes"] += 1
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
                    rewrite_lifecycle_fixture(root, manifest, events)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertTrue(got["complete"], got)

    def test_raw_checksum_and_profile_association_are_verified(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lifecycle_fixture(root)
            (root / "profiles" / "build.cpu.pprof").write_bytes(b"corrupt")

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
                    profile = root / "profiles" / "build.cpu.pprof"
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
            profile = root / "profiles" / "build.heap.pprof"
            profile.write_bytes(valid_heap_pprof_fixture())
            checksum = harness.sha256_file(profile)
            manifest["lifecycle"]["raw_artifacts"] = [{"path": "profiles/build.heap.pprof", "sha256": checksum}]
            manifest["lifecycle"]["profiles"] = [{
                "path": "profiles/build.heap.pprof",
                "sha256": checksum,
                "kind": "heap",
                "before_sequence": 5,
                "after_sequence": 6,
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
            manifest["lifecycle"]["raw_artifacts"] = [{"path": "profiles/build.trace.out", "sha256": checksum}]
            manifest["lifecycle"]["profiles"] = [{
                "path": "profiles/build.trace.out",
                "sha256": checksum,
                "kind": "trace",
                "before_sequence": 5,
                "after_sequence": 6,
            }]
            harness.write_json(root / "manifest.json", manifest)

            valid = harness.validate_lifecycle_artifact(root)

            trace.write_bytes(b"go 1.26 trace\x00\x00\x00")
            checksum = harness.sha256_file(trace)
            manifest["lifecycle"]["raw_artifacts"][0]["sha256"] = checksum
            manifest["lifecycle"]["profiles"][0]["sha256"] = checksum
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
                manifest["lifecycle"]["raw_artifacts"] = [{
                    "path": "profiles/build.perf.data", "sha256": checksum,
                }]
                manifest["lifecycle"]["profiles"] = [{
                    "path": "profiles/build.perf.data",
                    "sha256": checksum,
                    "kind": "perf",
                    "before_sequence": 5,
                    "after_sequence": 6,
                }]
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
            (lambda rows: rows[7]["state"]["counters"].__setitem__("builds", 0), "counters.builds decreased"),
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
                    rewrite_lifecycle_fixture(root, manifest, events)

                    got = harness.validate_lifecycle_artifact(root)

                if profile == "no_wal_fast":
                    self.assertTrue(got["complete"], got)
                else:
                    self.assertTrue(got["analyzable"], got)
                    self.assertFalse(got["complete"], got)
                    self.assertTrue(
                        any("requires positive wal.frontier" in item for item in got["completion_errors"]), got,
                    )
                    self.assertTrue(
                        any("requires positive wal.bytes_written_total" in item for item in got["completion_errors"]),
                        got,
                    )

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

        class Sampler:
            samples = [
                {"timestamp_ns": 1, "snapshot": snapshot},
                {
                    "timestamp_ns": 5,
                    "snapshot": snapshot,
                    "boundary": "load_end",
                    "boundary_timestamp_ns": 5,
                },
                {
                    "timestamp_ns": 6,
                    "snapshot": snapshot,
                    "boundary": "optimize_start",
                    "boundary_timestamp_ns": 6,
                },
                {
                    "timestamp_ns": 7,
                    "snapshot": snapshot,
                    "boundary": "optimize_end",
                    "boundary_timestamp_ns": 7,
                },
            ]

            def stop(self):
                return None

            def at(self, timestamp_ns):
                return snapshot

            def sample(self):
                record = {"timestamp_ns": 8, "snapshot": snapshot}
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
        ]
        (root / "adapter-lifecycle.jsonl").write_text(
            "".join(json.dumps(record) + "\n" for record in records), encoding="utf-8"
        )
        harness.write_json(root / "lifecycle-boundary-diagnostics.json", {
            "boundary": "optimize_end",
            "boundary_timestamp_ns": 7,
            "sample_timestamp_ns": 7,
        })
        proc = mock.Mock(returncode=None)
        return args, state, Sampler(), proc

    def test_build_boundaries_use_synchronous_samples_when_periodic_sample_is_stale(self) -> None:
        stale = {"phase": "stale"}
        snapshots = {
            "load_end": {"phase": "loaded"},
            "optimize_start": {"phase": "build-started"},
            "optimize_end": {"phase": "build-finished"},
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
                    harness.LIFECYCLE_DIAGNOSTIC_BOUNDARIES, (4, 6, 8), strict=True
                )
            ]

        for boundary, timestamp_ns in zip(
            harness.LIFECYCLE_DIAGNOSTIC_BOUNDARIES, (4, 6, 8), strict=True
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
            reopened = mock.Mock(returncode=None)
            snapshot = sampler.samples[0]["snapshot"]
            responses = iter([
                snapshot,
                {"count": 50000},
                {"index": {"generation": 7}},
                {
                    "no_documents": True,
                    "results": [{"id": "1"}, {"id": "2"}],
                    "diagnostics": {"route": "exact_hnsw_search_pack_v1", "fallback_reason": "none"},
                },
            ])

            def fetch(_url, path):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"profile")

            with mock.patch.object(harness, "fetch_file", side_effect=fetch), \
                    mock.patch.object(harness, "http_json", side_effect=lambda *_args, **_kwargs: next(responses)), \
                    mock.patch.object(harness, "close_process_group_cleanly"), \
                    mock.patch.object(harness, "start_service", return_value=(reopened, {}, ["service"])):
                harness.complete_lifecycle(state, args, root, root / "service", proc, sampler)

            profile = state.lifecycle["profiles"][0]
            events = [
                json.loads(line)
                for line in (root / "lifecycle.jsonl").read_text(encoding="utf-8").splitlines()
            ]

        self.assertEqual((profile["before_sequence"], profile["after_sequence"]), (8, 9))
        self.assertEqual(events[8]["stage"], "cache_warm")
        self.assertEqual(events[9]["stage"], "graceful_close")

    def test_loaded_route_proof_requires_exact_requested_results(self) -> None:
        valid_response = {
            "no_documents": True,
            "results": [{"id": "1"}, {"id": "2"}],
            "diagnostics": {"route": "exact_hnsw_search_pack_v1", "fallback_reason": "none"},
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args, state, _sampler, _proc = self._complete_fixture(root)
            responses = iter([{"index": {"generation": 7}}, valid_response])
            with mock.patch.object(harness, "http_json", side_effect=lambda *_args, **_kwargs: next(responses)):
                route = harness.run_loaded_route_proof(state, args, "cohere", "cohere:vector_hnsw", 7, 7)

        self.assertEqual(route["requested_top_k"], 2)
        self.assertEqual(route["result_count"], 2)

        for results in ([], [{"id": "1"}], {"0": {"id": "1"}}, ["malformed", {"id": "2"}]):
            with self.subTest(results=results), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                args, state, _sampler, _proc = self._complete_fixture(root)
                response = dict(valid_response, results=results)
                responses = iter([{"index": {"generation": 7}}, response])
                with mock.patch.object(harness, "http_json", side_effect=lambda *_args, **_kwargs: next(responses)), \
                        self.assertRaisesRegex(RuntimeError, "exactly the requested results"):
                    harness.run_loaded_route_proof(state, args, "cohere", "cohere:vector_hnsw", 7, 7)

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
            )
            path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")

            got = harness.read_adapter_lifecycle_sidecar(path)

        self.assertEqual(got["client_sent"], 5)
        self.assertEqual(got["server_accepted"], 5)
        self.assertEqual(got["load_start_ns"], 2)
        self.assertEqual(got["load_end_ns"], 5)
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
        self.assertEqual(stages, ["startup", "reset", "load_start", "load_end", "optimize_start"])

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
        self.assertEqual(state.lifecycle["result_status"], "partial")

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


if __name__ == "__main__":
    unittest.main()
