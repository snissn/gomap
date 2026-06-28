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
                        },
                    },
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return path


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

            markdown = (out / "celestia_sync_runs.md").read_text(encoding="utf-8")
            self.assertIn("## TreeDB Maintenance", markdown)
            self.assertIn("leaf-pack deleted", markdown)
            self.assertIn("1.50 KiB", markdown)
            self.assertIn("retained estimate", markdown)
            self.assertIn("db_1#0xbeef", markdown)


if __name__ == "__main__":
    unittest.main()
