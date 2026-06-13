#!/usr/bin/env python3
"""Focused tests for the SQLite FTS5 scoreboard baseline runner."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class SQLiteFTS5BenchTest(unittest.TestCase):
    def test_fresh_out_directory_creates_sidecar_db_parent(self) -> None:
        script = Path(__file__).with_name("sqlite_fts5_bench.py")
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "fresh" / "nested" / "sqlite_fts5.json"
            result = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--docs",
                    "32",
                    "--queries",
                    "2",
                    "--top-k",
                    "5",
                    "--out",
                    str(out),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(out.exists(), "runner should create the fresh output directory")
            payload = json.loads(out.read_text(encoding="utf-8"))
            reason = payload.get("unavailable_reason", "")
            self.assertNotIn("unable to open database file", reason)
            if payload.get("status") == "unavailable":
                self.skipTest(f"local Python sqlite3 lacks usable FTS5: {reason}")
            self.assertEqual(payload.get("status"), "ok")
            self.assertEqual(payload.get("metrics", {}).get("docs_fetched/search"), 0)

    def test_rejects_unsupported_tokenizer_before_sql_ddl(self) -> None:
        script = Path(__file__).with_name("sqlite_fts5_bench.py")
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "sqlite_fts5.json"
            result = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--docs",
                    "32",
                    "--queries",
                    "2",
                    "--top-k",
                    "5",
                    "--tokenize",
                    "unicode61'); DROP TABLE docs_fts; --",
                    "--out",
                    str(out),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("--tokenize must be one of", result.stderr)
            self.assertFalse(
                out.exists(),
                "invalid tokenizer should fail before writing a baseline artifact",
            )


if __name__ == "__main__":
    unittest.main()
