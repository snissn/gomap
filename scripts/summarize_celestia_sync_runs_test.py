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


def write_run_home(root: Path, backend: str, *, sync_seconds: int, rss_kb: int, app_bytes: int) -> Path:
    home = root / f".celestia-app-mainnet-{backend}-20260626010101"
    sync = home / "sync"
    app_db = home / "data" / "application.db"
    sync.mkdir(parents=True)
    app_db.mkdir(parents=True)
    (sync / "sync-time.log").write_text(
        "\n".join(
            [
                "start_utc=2026-06-26T00:00:00Z",
                "trust_height=1000",
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
                f"end_app_bytes={app_bytes}",
                f"end_home_bytes={app_bytes + 4096}",
                f"end_data_bytes={app_bytes + 2048}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
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
    (dwell / "sample_0.json").write_text(
        json.dumps(
            {
                "timestamp": "2026-06-26T00:02:00Z",
                "home_apparent_bytes": app_bytes + 4096,
                "app_db_apparent_bytes": app_bytes,
                "maindb_apparent_bytes": 1024,
                "wal_apparent_bytes": 512,
                "vmrss_kb": rss_kb + 2048,
                "vmhwm_kb": rss_kb + 4096,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (sync / "node.log").write_text("normal line\n", encoding="utf-8")
    return home


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
            self.assertEqual(payload["comparison"]["deltas"]["sync_duration_seconds"], 30)
            self.assertEqual(payload["comparison"]["ratios"]["end_app_bytes"], 0.8)

            markdown = (out / "celestia_sync_runs.md").read_text(encoding="utf-8")
            self.assertIn("# Celestia Sync Run Summary", markdown)
            self.assertIn("goleveldb", markdown)
            self.assertIn("treedb", markdown)
            self.assertIn("sync_duration_seconds", markdown)

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


if __name__ == "__main__":
    unittest.main()
