#!/usr/bin/env python3
"""Tests for summarize_celestia_sync_runs.py."""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "summarize_celestia_sync_runs.py"


def write_run_home(
    root: Path,
    backend: str,
    *,
    sync_seconds: int,
    rss_kb: int,
    app_bytes: int,
    include_end_app_bytes: bool = True,
    dwell_samples: int = 1,
) -> Path:
    home = root / f".celestia-app-mainnet-{backend}-20260626010101"
    sync = home / "sync"
    app_db = home / "data" / "application.db"
    sync.mkdir(parents=True)
    app_db.mkdir(parents=True)
    time_lines = [
        "start_utc=2026-06-26T00:00:00Z",
        "trust_height=1000",
        "trust_hash=ABCDEF",
        f"db_backend={backend}",
        f"app_db_backend={backend}",
        f"sync_duration_seconds={sync_seconds}",
        f"duration_seconds={sync_seconds}",
        f"total_duration_seconds={sync_seconds + 60}",
        "post_sync_dwell_elapsed_seconds=60",
        "final_local_height=1250",
        "final_remote_height=1248",
        f"max_rss_kb={rss_kb}",
        f"max_hwm_kb={rss_kb + 1024}",
        f"end_home_bytes={app_bytes + 4096}",
        f"end_data_bytes={app_bytes + 2048}",
    ]
    if include_end_app_bytes:
        time_lines.append(f"end_app_bytes={app_bytes}")
    (sync / "sync-time.log").write_text("\n".join(time_lines) + "\n", encoding="utf-8")
    (sync / "disk-breakdown.log").write_text(
        "\n".join(
            [
                f"app_db={app_db}",
                "du_bytes:",
                f"{app_bytes} {app_db}",
                f"1024 {app_db / 'maindb'}",
                f"512 {app_db / 'maindb' / 'wal'}",
                "top_files_bytes:",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    dwell = sync / "dwell-stats"
    dwell.mkdir()
    for idx in range(dwell_samples):
        (dwell / f"sample_{idx}.json").write_text(
            json.dumps(
                {
                    "timestamp": f"2026-06-26T00:02:{idx:02d}Z",
                    "home_apparent_bytes": app_bytes + 4096 + idx,
                    "app_db_apparent_bytes": app_bytes + idx,
                    "maindb_apparent_bytes": 1024 + idx,
                    "wal_apparent_bytes": 512 + idx,
                    "vmrss_kb": rss_kb + 2048 + idx,
                    "vmhwm_kb": rss_kb + 4096 + idx,
                }
            )
            + "\n",
            encoding="utf-8",
        )
    (sync / "node.log").write_text("normal line\n", encoding="utf-8")
    return home


def write_treedb_debug_vars(home: Path, *, instance_key: str = "db_1#0xbeef") -> Path:
    app_wal = str(home / "data" / "application.db" / "maindb" / "wal")
    diag = home / "sync" / "diagnostics"
    diag.mkdir(parents=True, exist_ok=True)
    path = diag / "final.debug_vars.json"
    path.write_text(
        json.dumps(
            {
                "treedb": {
                    "instances": {
                        "db_0#0xdead": {
                            "treedb.expvar.wal_dir": "/tmp/other/application.db/maindb/wal",
                            "treedb.cache.vlog_generation.maintenance.attempts": "100",
                        },
                        instance_key: {
                            "treedb.expvar.wal_dir": app_wal,
                            "treedb.process.identity.wal_dir": app_wal,
                            "treedb.cache.vlog_generation.maintenance.attempts": "7",
                            "treedb.cache.vlog_generation.maintenance.acquired": "5",
                            "treedb.cache.vlog_generation.maintenance.collisions": "1",
                            "treedb.cache.vlog_generation.maintenance.passes.with_gc": "2",
                            "treedb.cache.vlog_generation.gc.runs": "2",
                            "treedb.cache.vlog_generation.gc.deleted_bytes": "1024",
                            "treedb.cache.vlog_generation.gc.deleted_segments": "3",
                            "treedb.cache.vlog_generation.gc.last_eligible_bytes": "128",
                            "treedb.cache.vlog_generation.leaf_pack.gc.runs": "3",
                            "treedb.cache.vlog_generation.leaf_pack.gc.deleted_bytes": "1536",
                            "treedb.cache.vlog_generation.leaf_pack.gc.deleted_files": "6",
                            "treedb.cache.vlog_generation.leaf_pack.gc.deleted_generations": "2",
                            "treedb.cache.vlog_generation.leaf_pack.gc.eligible_generations": "5",
                            "treedb.cache.vlog_retained_segments": "4",
                            "treedb.cache.vlog_retained_bytes_estimate": "4096",
                            "treedb.cache.vlog_retained_prune.closed_bytes": "2048",
                            "treedb.cache.vlog_retained_prune.removed_bytes": "512",
                            "treedb.cache.vlog_retained_prune.removed_segments": "1",
                            "treedb.cache.vlog_zombie.bytes": "256",
                            "treedb.cache.vlog_zombie.segments": "2",
                            "treedb.cache.vlog_generation.checkpoint_kick.runs": "9",
                            "treedb.cache.vlog_generation.checkpoint_kick.gc_runs": "2",
                            "treedb.cache.vlog_generation.checkpoint_kick.rewrite_runs": "1",
                            "treedb.command_wal.public_batch.set.calls_total": "11",
                            "treedb.command_wal.public_batch.set_view.calls_total": "22",
                            "treedb.command_wal.public_batch.delete.calls_total": "3",
                            "treedb.command_wal.public_batch.delete_view.calls_total": "4",
                            "treedb.raw.span_native.used_ops_total": "29",
                            "treedb.raw.span_native.used_spans_total": "7",
                            "treedb.raw.span_native.route.point_put.observations_total": "2",
                            "treedb.raw.span_native.route.point_put.candidate_ops_total": "30",
                            "treedb.raw.span_native.route.point_put.eligible_ops_total": "30",
                            "treedb.raw.span_native.route.point_put.used_ops_total": "29",
                            "treedb.raw.span_native.route.point_put.fallbacks_total": "1",
                            "treedb.raw.span_native.route.point_put.fallback.reason.maintenance.ops_total": "1",
                            "treedb.flush_apply.span_native.candidate_ops_total": "40",
                            "treedb.flush_apply.span_native.eligible_ops_total": "39",
                            "treedb.flush_apply.span_native.used_ops_total": "38",
                            "treedb.flush_apply.span_native.fallbacks_total": "2",
                            "treedb.flush_apply.span_native.fallback.reason.maintenance.ops_total": "2",
                            "treedb.cache.flush_apply.span_native": "true",
                            "treedb.publish.ordered_root_delta_group.calls_total": "0",
                            "treedb.publish.ordered_root_delta_group.roots_total": "0",
                            "treedb.publish.ordered_root_delta_group.root_apply_calls_total": "0",
                            "treedb.publish.ordered_root_delta_group.span_native.candidate_ops_total": "0",
                            "treedb.publish.ordered_root_delta_group.span_native.used_ops_total": "0",
                            "treedb.cache.flush_apply.backend_write_ns_total": "123000000000",
                            "treedb.cache.flush_apply.backend_batch_write_ns_total": "120000000000",
                            "treedb.cache.flush_apply.leaf_log_append_ns_total": "23000000000",
                            "treedb.cache.flush_apply.leaf_log_encode_compress_ns_total": "17000000000",
                            "treedb.cache.flush_apply.foreground_assist_wait_ns_total": "5000000000",
                            "treedb.cache.flush_apply.coordinator.in_flight_bytes": "0",
                            "treedb.cache.flush_apply.coordinator.active_workers": "0",
                            "treedb.cache.flush_apply.entries_total": "41",
                            "treedb.cache.batch_arena.retained_bytes_global_estimate": "1024",
                            "treedb.cache.batch_arena.retained_bytes_global_max_estimate": "2048",
                            "treedb.cache.batch_arena.tail_waste_bytes_total": "4096",
                            "treedb.cache.append_only_direct_arena.retained_bytes": "8192",
                            "treedb.vlog.mmap_active_bytes": "16384",
                            "treedb.vlog.mmap_dead_bytes": "0",
                        },
                    },
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def write_stats_json(path: Path, stats: dict[str, str]) -> None:
    path.write_text(json.dumps(stats) + "\n", encoding="utf-8")


class CelestiaSyncSummaryTest(unittest.TestCase):
    def test_summarizes_runs_and_writes_json_and_markdown(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            level = write_run_home(root, "goleveldb", sync_seconds=120, rss_kb=1000, app_bytes=10_000)
            tree = write_run_home(root, "treedb", sync_seconds=150, rss_kb=2000, app_bytes=8_000)
            out = root / "out"

            result = subprocess.run(
                [str(SCRIPT), "--out-dir", str(out), str(level), str(tree)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads((out / "celestia_sync_runs.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["schema"], "celestia_sync_run_summary.v1")
            self.assertEqual([run["db_backend"] for run in payload["runs"]], ["goleveldb", "treedb"])
            self.assertEqual(payload["runs"][0]["blocks_synced"], 250)
            self.assertEqual(payload["runs"][1]["dwell"]["sample_count"], 1)
            self.assertEqual(payload["runs"][0]["disk_breakdown_bytes"]["application.db"], 10_000)
            self.assertEqual(payload["comparison"]["deltas"]["sync_duration_seconds"], 30)
            self.assertEqual(payload["comparison"]["ratios"]["end_app_bytes"], 0.8)
            self.assertTrue(payload["comparison"]["valid"])

            markdown = (out / "celestia_sync_runs.md").read_text(encoding="utf-8")
            self.assertIn("# Celestia Sync Run Summary", markdown)
            self.assertIn("goleveldb", markdown)
            self.assertIn("treedb", markdown)
            self.assertIn("sync_duration_seconds", markdown)

    def test_skips_comparison_for_mismatched_run_windows(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            level = write_run_home(root, "goleveldb", sync_seconds=120, rss_kb=1000, app_bytes=10_000)
            tree = write_run_home(root, "treedb", sync_seconds=150, rss_kb=2000, app_bytes=8_000)
            sync_time = tree / "sync" / "sync-time.log"
            sync_time.write_text(
                sync_time.read_text(encoding="utf-8").replace("trust_hash=ABCDEF", "trust_hash=DIFFERENT"),
                encoding="utf-8",
            )
            out = root / "out"

            result = subprocess.run(
                [str(SCRIPT), "--out-dir", str(out), str(level), str(tree)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads((out / "celestia_sync_runs.json").read_text(encoding="utf-8"))
            comparison = payload["comparison"]
            self.assertFalse(comparison["valid"])
            self.assertEqual(comparison["reason"], "mismatched_run_window")
            self.assertNotIn("ratios", comparison)
            self.assertEqual(comparison["mismatches"][0]["field"], "trust_hash")

            markdown = (out / "celestia_sync_runs.md").read_text(encoding="utf-8")
            self.assertIn("Comparison skipped: mismatched_run_window.", markdown)
            self.assertIn("trust_hash", markdown)

    def test_skips_comparison_when_window_evidence_is_missing(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            level = write_run_home(root, "goleveldb", sync_seconds=120, rss_kb=1000, app_bytes=10_000)
            tree = write_run_home(root, "treedb", sync_seconds=150, rss_kb=2000, app_bytes=8_000)
            for run in [level, tree]:
                sync_time = run / "sync" / "sync-time.log"
                sync_time.write_text(
                    sync_time.read_text(encoding="utf-8").replace("trust_hash=ABCDEF\n", ""),
                    encoding="utf-8",
                )
            out = root / "out"

            result = subprocess.run(
                [str(SCRIPT), "--out-dir", str(out), str(level), str(tree)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads((out / "celestia_sync_runs.json").read_text(encoding="utf-8"))
            comparison = payload["comparison"]
            self.assertFalse(comparison["valid"])
            self.assertEqual(comparison["reason"], "missing_run_window_evidence")
            self.assertEqual(
                comparison["missing_fields"],
                [
                    {"side": "baseline", "field": "trust_hash"},
                    {"side": "candidate", "field": "trust_hash"},
                ],
            )
            self.assertNotIn("ratios", comparison)

            markdown = (out / "celestia_sync_runs.md").read_text(encoding="utf-8")
            self.assertIn("Comparison skipped: missing_run_window_evidence.", markdown)
            self.assertIn("trust_hash", markdown)

    def test_counts_fatal_log_matches(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            run = write_run_home(root, "treedb", sync_seconds=10, rss_kb=1000, app_bytes=1000)
            node_log = run / "sync" / "node.log"
            node_log.write_text("ok\npanic: bad state\nfatal error: crash\n", encoding="utf-8")

            result = subprocess.run(
                [str(SCRIPT), str(run)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("| treedb", result.stdout)
            self.assertIn("| 2", result.stdout)

    def test_disk_fallback_and_numeric_dwell_sample_order(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            run = write_run_home(
                root,
                "treedb",
                sync_seconds=10,
                rss_kb=1000,
                app_bytes=1234,
                include_end_app_bytes=False,
                dwell_samples=11,
            )
            out = root / "out"

            result = subprocess.run(
                [str(SCRIPT), "--out-dir", str(out), str(run)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads((out / "celestia_sync_runs.json").read_text(encoding="utf-8"))
            summary = payload["runs"][0]
            self.assertEqual(summary["end_app_bytes"], 1234)
            self.assertEqual(summary["disk_breakdown_bytes"]["application.db"], 1234)
            self.assertEqual(summary["dwell"]["sample_count"], 11)
            self.assertEqual(summary["dwell"]["last_timestamp"], "2026-06-26T00:02:10Z")
            self.assertEqual(summary["dwell"]["last_app_db_apparent_bytes"], 1244)

    def test_disk_fallback_when_sync_app_bytes_is_zero(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            run = write_run_home(root, "treedb", sync_seconds=10, rss_kb=1000, app_bytes=1234)
            sync_time = run / "sync" / "sync-time.log"
            sync_time.write_text(
                sync_time.read_text(encoding="utf-8").replace("end_app_bytes=1234", "end_app_bytes=0"),
                encoding="utf-8",
            )
            out = root / "out"

            result = subprocess.run(
                [str(SCRIPT), "--out-dir", str(out), str(run)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads((out / "celestia_sync_runs.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["runs"][0]["end_app_bytes"], 1234)

    def test_requires_sync_time_log(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            run = root / ".celestia-app-mainnet-treedb-20260626010101"
            (run / "sync").mkdir(parents=True)

            result = subprocess.run(
                [str(SCRIPT), str(run)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 2)
            self.assertEqual(result.stdout, "")
            self.assertIn("missing required sync-time.log", result.stderr)

    def test_treedb_maintenance_summary_selects_app_instance_by_wal_dir(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            run = write_run_home(root, "treedb", sync_seconds=10, rss_kb=1000, app_bytes=3000, dwell_samples=2)
            write_treedb_debug_vars(run)
            sample_0 = run / "sync" / "dwell-stats" / "sample_0.json"
            sample_1 = run / "sync" / "dwell-stats" / "sample_1.json"
            first = json.loads(sample_0.read_text(encoding="utf-8"))
            last = json.loads(sample_1.read_text(encoding="utf-8"))
            first["app_db_apparent_bytes"] = 5000
            first["app_db_physical_bytes"] = 6000
            last["app_db_apparent_bytes"] = 3000
            last["app_db_physical_bytes"] = 3500
            sample_0.write_text(json.dumps(first) + "\n", encoding="utf-8")
            sample_1.write_text(json.dumps(last) + "\n", encoding="utf-8")
            out = root / "out"

            result = subprocess.run(
                [str(SCRIPT), "--out-dir", str(out), str(run)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads((out / "celestia_sync_runs.json").read_text(encoding="utf-8"))
            summary = payload["runs"][0]
            maintenance = summary["treedb_maintenance"]
            self.assertTrue(maintenance["available"])
            self.assertEqual(maintenance["instance"], "db_1#0xbeef")
            self.assertEqual(maintenance["counters"]["maintenance_attempts"], 7)
            self.assertEqual(maintenance["counters"]["gc_deleted_bytes"], 1024)
            self.assertEqual(maintenance["counters"]["leaf_pack_gc_deleted_bytes"], 1536)
            self.assertEqual(maintenance["counters"]["leaf_pack_gc_deleted_files"], 6)
            self.assertEqual(maintenance["counters"]["retained_prune_closed_bytes"], 2048)
            self.assertEqual(maintenance["counters"]["vlog_retained_bytes_estimate"], 4096)
            self.assertEqual(summary["dwell"]["app_db_apparent_shrink_from_peak_bytes"], 2000)
            self.assertEqual(summary["treedb_disk_reclaim"]["dwell_app_db_physical_shrink_from_peak_bytes"], 2500)
            self.assertEqual(summary["treedb_disk_reclaim"]["leaf_pack_gc_deleted_bytes"], 1536)
            self.assertEqual(summary["treedb_disk_reclaim"]["named_delete_or_remove_bytes"], 3072)
            decision = summary["treedb_decision_tree"]
            self.assertTrue(decision["available"])
            self.assertEqual(decision["public_batch"]["set_view_calls_total"], 22)
            self.assertEqual(decision["raw_span_native"]["used_ops_total"], 29)
            self.assertEqual(decision["raw_span_native"]["routes"]["point_put"]["used_ops_total"], 29)
            self.assertEqual(
                decision["raw_span_native"]["routes"]["point_put"]["fallback_reasons"]["maintenance"]["ops_total"],
                1,
            )
            self.assertTrue(decision["flush_apply_span_native"]["enabled"])
            self.assertEqual(decision["flush_apply_span_native"]["used_ops_total"], 38)
            self.assertEqual(decision["ordered_root"]["calls_total"], 0)
            self.assertEqual(decision["apply_backend"]["backend_write_ns_total"], 123000000000)
            self.assertEqual(decision["memory_residency"]["vlog_mmap_active_bytes"], 16384)

            markdown = (out / "celestia_sync_runs.md").read_text(encoding="utf-8")
            self.assertIn("## TreeDB Maintenance", markdown)
            self.assertIn("leaf-pack deleted", markdown)
            self.assertIn("1.50 KiB", markdown)
            self.assertIn("retained estimate", markdown)
            self.assertIn("db_1#0xbeef", markdown)
            self.assertIn("## TreeDB Decision Counters", markdown)
            self.assertIn("11/22/3/4", markdown)
            self.assertIn("30/30/29/1", markdown)
            self.assertIn("maintenance", markdown)
            self.assertIn("2.00 KiB", markdown)

    def test_treedb_decision_tree_prefers_latest_dwell_app_stats(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            run = write_run_home(root, "treedb", sync_seconds=10, rss_kb=1000, app_bytes=3000, dwell_samples=2)
            write_treedb_debug_vars(run)
            dwell = run / "sync" / "dwell-stats"
            write_stats_json(
                dwell / "treedb_app_0.json",
                {
                    "treedb.cache.vlog_generation.maintenance.attempts": "3",
                    "treedb.command_wal.public_batch.set.calls_total": "1",
                    "treedb.command_wal.public_batch.set_view.calls_total": "2",
                    "treedb.raw.span_native.route.point_put.candidate_ops_total": "3",
                    "treedb.raw.span_native.route.point_put.eligible_ops_total": "3",
                    "treedb.raw.span_native.route.point_put.used_ops_total": "3",
                    "treedb.flush_apply.span_native.candidate_ops_total": "4",
                    "treedb.flush_apply.span_native.eligible_ops_total": "4",
                    "treedb.flush_apply.span_native.used_ops_total": "4",
                    "treedb.flush_admission.flush_apply_span_native": "true",
                    "treedb.publish.ordered_root_delta_group.calls_total": "0",
                },
            )
            write_stats_json(
                dwell / "treedb_app_2.json",
                {
                    "treedb.cache.vlog_generation.maintenance.attempts": "13",
                    "treedb.command_wal.public_batch.set.calls_total": "101",
                    "treedb.command_wal.public_batch.set_view.calls_total": "202",
                    "treedb.raw.span_native.route.point_put.candidate_ops_total": "303",
                    "treedb.raw.span_native.route.point_put.eligible_ops_total": "303",
                    "treedb.raw.span_native.route.point_put.used_ops_total": "303",
                    "treedb.raw.span_native.route.point_put.fallbacks_total": "0",
                    "treedb.flush_apply.span_native.candidate_ops_total": "404",
                    "treedb.flush_apply.span_native.eligible_ops_total": "404",
                    "treedb.flush_apply.span_native.used_ops_total": "404",
                    "treedb.flush_admission.flush_apply_span_native": "true",
                    "treedb.publish.ordered_root_delta_group.calls_total": "0",
                    "treedb.cache.flush_apply.backend_write_ns_total": "5000000000",
                    "treedb.cache.flush_apply.coordinator.in_flight_bytes": "0",
                    "treedb.vlog.mmap_active_bytes": "4096",
                },
            )
            write_stats_json(
                dwell / "treedb_app_10.json",
                {
                    "treedb.cache.vlog_generation.maintenance.attempts": "23",
                    "treedb.command_wal.public_batch.set.calls_total": "9001",
                    "treedb.command_wal.public_batch.set_view.calls_total": "9002",
                    "treedb.raw.span_native.route.point_put.candidate_ops_total": "9003",
                    "treedb.raw.span_native.route.point_put.eligible_ops_total": "9003",
                    "treedb.raw.span_native.route.point_put.used_ops_total": "9003",
                    "treedb.raw.span_native.route.point_put.fallbacks_total": "0",
                    "treedb.flush_apply.span_native.candidate_ops_total": "9004",
                    "treedb.flush_apply.span_native.eligible_ops_total": "9004",
                    "treedb.flush_apply.span_native.used_ops_total": "9004",
                    "treedb.flush_admission.flush_apply_span_native": "true",
                    "treedb.publish.ordered_root_delta_group.calls_total": "0",
                    "treedb.cache.flush_apply.backend_write_ns_total": "5000000000",
                    "treedb.cache.flush_apply.coordinator.in_flight_bytes": "0",
                    "treedb.vlog.mmap_active_bytes": "4096",
                },
            )
            out = root / "out"

            result = subprocess.run(
                [str(SCRIPT), "--out-dir", str(out), str(run)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads((out / "celestia_sync_runs.json").read_text(encoding="utf-8"))
            maintenance = payload["runs"][0]["treedb_maintenance"]
            self.assertTrue(maintenance["source_file"].endswith("final.debug_vars.json"))
            self.assertEqual(maintenance["counters"]["maintenance_attempts"], 7)
            decision = payload["runs"][0]["treedb_decision_tree"]
            self.assertTrue(decision["source_file"].endswith("treedb_app_10.json"))
            self.assertEqual(decision["public_batch"]["set_view_calls_total"], 9002)
            self.assertEqual(decision["raw_span_native"]["routes"]["point_put"]["used_ops_total"], 9003)
            self.assertTrue(decision["flush_apply_span_native"]["enabled"])
            self.assertEqual(decision["flush_apply_span_native"]["used_ops_total"], 9004)

            markdown = (out / "celestia_sync_runs.md").read_text(encoding="utf-8")
            self.assertIn("9001/9002/0/0", markdown)
            self.assertIn("9003/9003/9003/0", markdown)

    def test_treedb_decision_tree_prefers_final_app_vars_over_non_final_debug_vars(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            run = write_run_home(root, "treedb", sync_seconds=10, rss_kb=1000, app_bytes=3000)
            diagnostics = run / "sync" / "diagnostics"
            diagnostics.mkdir(parents=True, exist_ok=True)
            write_stats_json(
                diagnostics / "pprof-heap-max-hwm-123.debug_vars.json",
                {
                    "treedb.command_wal.public_batch.set.calls_total": "1",
                    "treedb.command_wal.public_batch.set_view.calls_total": "2",
                    "treedb.raw.span_native.route.point_put.candidate_ops_total": "3",
                    "treedb.raw.span_native.route.point_put.eligible_ops_total": "3",
                    "treedb.raw.span_native.route.point_put.used_ops_total": "3",
                },
            )
            write_stats_json(
                diagnostics / "final.max-memory-final.treedb_application_vars.json",
                {
                    "treedb.command_wal.public_batch.set.calls_total": "101",
                    "treedb.command_wal.public_batch.set_view.calls_total": "202",
                    "treedb.raw.span_native.route.point_put.candidate_ops_total": "303",
                    "treedb.raw.span_native.route.point_put.eligible_ops_total": "303",
                    "treedb.raw.span_native.route.point_put.used_ops_total": "303",
                    "treedb.flush_apply.span_native.used_ops_total": "404",
                },
            )
            out = root / "out"

            result = subprocess.run(
                [str(SCRIPT), "--out-dir", str(out), str(run)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads((out / "celestia_sync_runs.json").read_text(encoding="utf-8"))
            decision = payload["runs"][0]["treedb_decision_tree"]
            self.assertTrue(
                decision["source_file"].endswith("final.max-memory-final.treedb_application_vars.json")
            )
            self.assertEqual(decision["public_batch"]["set_calls_total"], 101)
            self.assertEqual(decision["public_batch"]["set_view_calls_total"], 202)
            self.assertEqual(decision["raw_span_native"]["routes"]["point_put"]["used_ops_total"], 303)
            self.assertEqual(decision["flush_apply_span_native"]["used_ops_total"], 404)

    def test_treedb_decision_tree_rejects_maintenance_only_stats(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            run = write_run_home(root, "treedb", sync_seconds=10, rss_kb=1000, app_bytes=3000)
            diagnostics = run / "sync" / "diagnostics"
            diagnostics.mkdir(parents=True, exist_ok=True)
            write_stats_json(
                diagnostics / "final.max-memory-final.debug_vars.json",
                {"treedb.cache.vlog_generation.maintenance.attempts": "11"},
            )
            out = root / "out"

            result = subprocess.run(
                [str(SCRIPT), "--out-dir", str(out), str(run)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads((out / "celestia_sync_runs.json").read_text(encoding="utf-8"))
            decision = payload["runs"][0]["treedb_decision_tree"]
            self.assertFalse(decision["available"])
            self.assertEqual(decision["reason"], "treedb_app_stats_not_found")
            self.assertTrue(decision["source_file"].endswith("final.max-memory-final.debug_vars.json"))
            markdown = (out / "celestia_sync_runs.md").read_text(encoding="utf-8")
            self.assertNotIn("## TreeDB Decision Counters", markdown)

    def test_treedb_decision_tree_uses_process_mmap_residency_when_available(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            run = write_run_home(root, "treedb", sync_seconds=10, rss_kb=1000, app_bytes=3000)
            diagnostics = run / "sync" / "diagnostics"
            diagnostics.mkdir(parents=True, exist_ok=True)
            write_stats_json(
                diagnostics / "final.max-memory-final.treedb_application_vars.json",
                {
                    "treedb.command_wal.public_batch.set.calls_total": "1",
                    "treedb.vlog.mmap_active_bytes": "4096",
                    "treedb.cache.vlog_mmap.active_bytes": "8192",
                    "treedb.process.memory.vlog_mmap_active_bytes": "12288",
                    "treedb.vlog.mmap_dead_bytes": "16",
                    "treedb.cache.vlog_mmap.dead_bytes": "32",
                    "treedb.process.memory.vlog_mmap_dead_bytes": "48",
                },
            )
            out = root / "out"

            result = subprocess.run(
                [str(SCRIPT), "--out-dir", str(out), str(run)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads((out / "celestia_sync_runs.json").read_text(encoding="utf-8"))
            decision = payload["runs"][0]["treedb_decision_tree"]
            self.assertTrue(decision["available"])
            self.assertEqual(decision["memory_residency"]["vlog_mmap_active_bytes"], 12288)
            self.assertEqual(decision["memory_residency"]["vlog_mmap_dead_bytes"], 48)

    def test_treedb_maintenance_prefers_diagnostics_debug_vars_over_app_vars_and_quiesce(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            run = write_run_home(root, "treedb", sync_seconds=10, rss_kb=1000, app_bytes=3000)
            diagnostics = run / "sync" / "diagnostics"
            diagnostics.mkdir(parents=True, exist_ok=True)
            maintenance_quiesce = run / "sync" / "maintenance-quiesce"
            maintenance_quiesce.mkdir(parents=True, exist_ok=True)
            write_stats_json(
                diagnostics / "final.max-memory-final.treedb_application_vars.json",
                {"treedb.cache.vlog_generation.maintenance.attempts": "11"},
            )
            write_stats_json(
                diagnostics / "final.max-memory-final.debug_vars.json",
                {"treedb.cache.vlog_generation.maintenance.attempts": "22"},
            )
            write_stats_json(
                maintenance_quiesce / "debug_vars_9.json",
                {"treedb.cache.vlog_generation.maintenance.attempts": "33"},
            )
            out = root / "out"

            result = subprocess.run(
                [str(SCRIPT), "--out-dir", str(out), str(run)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads((out / "celestia_sync_runs.json").read_text(encoding="utf-8"))
            maintenance = payload["runs"][0]["treedb_maintenance"]
            self.assertTrue(maintenance["source_file"].endswith("final.max-memory-final.debug_vars.json"))
            self.assertEqual(maintenance["counters"]["maintenance_attempts"], 22)

    def test_treedb_maintenance_prefers_quiesce_over_non_final_hwm_debug_vars(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            run = write_run_home(root, "treedb", sync_seconds=10, rss_kb=1000, app_bytes=3000)
            diagnostics = run / "sync" / "diagnostics"
            diagnostics.mkdir(parents=True, exist_ok=True)
            maintenance_quiesce = run / "sync" / "maintenance-quiesce"
            maintenance_quiesce.mkdir(parents=True, exist_ok=True)
            write_stats_json(
                diagnostics / "pprof-heap-max-hwm-123.debug_vars.json",
                {"treedb.cache.vlog_generation.maintenance.attempts": "44"},
            )
            write_stats_json(
                diagnostics / "pprof-heap-max-hwm-123.treedb_application_vars.json",
                {"treedb.cache.vlog_generation.maintenance.attempts": "11"},
            )
            write_stats_json(
                maintenance_quiesce / "debug_vars_9.json",
                {"treedb.cache.vlog_generation.maintenance.attempts": "33"},
            )
            out = root / "out"

            result = subprocess.run(
                [str(SCRIPT), "--out-dir", str(out), str(run)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads((out / "celestia_sync_runs.json").read_text(encoding="utf-8"))
            maintenance = payload["runs"][0]["treedb_maintenance"]
            self.assertTrue(maintenance["source_file"].endswith("maintenance-quiesce/debug_vars_9.json"))
            self.assertEqual(maintenance["counters"]["maintenance_attempts"], 33)

    def test_treedb_decision_tree_accepts_flat_non_cache_treedb_stats(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            run = write_run_home(root, "treedb", sync_seconds=10, rss_kb=1000, app_bytes=3000)
            write_treedb_debug_vars(run)
            dwell = run / "sync" / "dwell-stats"
            write_stats_json(
                dwell / "treedb_app_0.json",
                {
                    "treedb.command_wal.public_batch.set.calls_total": "77",
                    "treedb.command_wal.public_batch.set_view.calls_total": "88",
                    "treedb.raw.span_native.public.command_wal_rejections_total": "12",
                    "treedb.raw.span_native.public.route.update.fallback.reason.command_wal_barrier.count_total": "5",
                    "treedb.raw.span_native.public.route.update.fallback.reason.command_wal_barrier.ops_total": "5",
                    "treedb.raw.span_native.public.route.update_sync.fallback.reason.command_wal_barrier.count_total": "7",
                    "treedb.raw.span_native.public.route.update_sync.fallback.reason.command_wal_barrier.ops_total": "7",
                },
            )
            out = root / "out"

            result = subprocess.run(
                [str(SCRIPT), "--out-dir", str(out), str(run)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads((out / "celestia_sync_runs.json").read_text(encoding="utf-8"))
            maintenance = payload["runs"][0]["treedb_maintenance"]
            decision = payload["runs"][0]["treedb_decision_tree"]
            self.assertTrue(maintenance["source_file"].endswith("final.debug_vars.json"))
            self.assertTrue(decision["source_file"].endswith("treedb_app_0.json"))
            self.assertEqual(decision["public_batch"]["set_calls_total"], 77)
            self.assertEqual(decision["public_batch"]["set_view_calls_total"], 88)
            self.assertEqual(decision["raw_span_native"]["public_command_wal_rejections_total"], 12)
            self.assertEqual(
                decision["raw_span_native"]["public_routes"]["update"]["fallback_reasons"]["command_wal_barrier"][
                    "count_total"
                ],
                5,
            )
            self.assertEqual(
                decision["raw_span_native"]["public_routes"]["update_sync"]["fallback_reasons"]["command_wal_barrier"][
                    "ops_total"
                ],
                7,
            )
            markdown = (out / "celestia_sync_runs.md").read_text(encoding="utf-8")
            self.assertIn(
                "12/update:command_wal_barrier:5count/5ops,update_sync:command_wal_barrier:7count/7ops",
                markdown,
            )

    def test_treedb_maintenance_rejects_flat_decision_only_stats(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            run = write_run_home(root, "treedb", sync_seconds=10, rss_kb=1000, app_bytes=3000)
            diagnostics = run / "sync" / "diagnostics"
            diagnostics.mkdir(parents=True, exist_ok=True)
            write_stats_json(
                diagnostics / "final.max-memory-final.treedb_application_vars.json",
                {
                    "treedb.command_wal.public_batch.set.calls_total": "77",
                    "treedb.command_wal.public_batch.set_view.calls_total": "88",
                },
            )
            out = root / "out"

            result = subprocess.run(
                [str(SCRIPT), "--out-dir", str(out), str(run)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads((out / "celestia_sync_runs.json").read_text(encoding="utf-8"))
            maintenance = payload["runs"][0]["treedb_maintenance"]
            decision = payload["runs"][0]["treedb_decision_tree"]
            self.assertFalse(maintenance["available"])
            self.assertEqual(maintenance["reason"], "treedb_app_stats_not_found")
            self.assertTrue(decision["source_file"].endswith("final.max-memory-final.treedb_application_vars.json"))
            self.assertEqual(decision["public_batch"]["set_calls_total"], 77)
            self.assertEqual(decision["public_batch"]["set_view_calls_total"], 88)

    def test_treedb_maintenance_uses_quiesce_without_diagnostics_dir(self) -> None:
        with tempfile.TemporaryDirectory(prefix="celestia_sync_summary_test_") as tmp:
            root = Path(tmp)
            run = write_run_home(root, "treedb", sync_seconds=10, rss_kb=1000, app_bytes=3000)
            maintenance_quiesce = run / "sync" / "maintenance-quiesce"
            maintenance_quiesce.mkdir(parents=True, exist_ok=True)
            write_stats_json(
                maintenance_quiesce / "debug_vars_1.json",
                {"treedb.cache.vlog_generation.maintenance.attempts": "55"},
            )
            out = root / "out"

            result = subprocess.run(
                [str(SCRIPT), "--out-dir", str(out), str(run)],
                cwd=ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            payload = json.loads((out / "celestia_sync_runs.json").read_text(encoding="utf-8"))
            maintenance = payload["runs"][0]["treedb_maintenance"]
            self.assertTrue(maintenance["available"])
            self.assertTrue(maintenance["source_file"].endswith("maintenance-quiesce/debug_vars_1.json"))
            self.assertEqual(maintenance["counters"]["maintenance_attempts"], 55)


if __name__ == "__main__":
    unittest.main()
