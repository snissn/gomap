#!/usr/bin/env python3
"""Failure controls for the scoped public Stats diagnostic (no benchmarks)."""

import json
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import pool_pressure_stats_diagnostic as diagnostic


class PublicStatsDiagnosticTests(unittest.TestCase):
    def test_command_failure_retains_null_not_acceptance(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence"
            with mock.patch.object(diagnostic.subprocess, "Popen", side_effect=OSError("injected command failure")):
                self.assertEqual(diagnostic.run(ROOT, output, "1" * 40, "2" * 40), 1)
            result = json.loads((output / "summary.json").read_text())
            self.assertEqual(result["verdict"], "NULL")
            self.assertFalse(result["product_guard_available"])
            self.assertEqual(result["retained_samples"], 0)
            self.assertIn("injected command failure", result["error"])

    def sample(self):
        return f"{diagnostic.BENCHMARK} 12345 100 ns/op 200 B/op 3 allocs/op\nPASS\n"

    def rows(self):
        return [dict(pair=pair, revision=revision, **diagnostic.parse_sample(self.sample()))
                for pair in range(3) for revision in
                (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline"))]

    def test_fixed_plan_and_independent_strict_limits(self):
        self.assertTrue(diagnostic.evaluate(self.rows())["measurement_pass"])
        for metric, value, name in (("ns/op", 106, "timing"), ("B/op", 201, "bytes"),
                                    ("allocs/op", 4, "allocations")):
            rows = self.rows()
            for row in rows:
                if row["revision"] == "candidate":
                    row["metrics"][metric] = value
            self.assertFalse(diagnostic.evaluate(rows)["gates"][name])
            self.assertFalse(diagnostic.evaluate(rows)["measurement_pass"])
        # Pairing must preserve chronological AB/BA/AB, not sort results by cost.
        rows = self.rows()
        for invalid in (rows[:-1], rows + rows[:1], list(reversed(rows)), rows[1:] + rows[:1]):
            with self.assertRaises(ValueError):
                diagnostic.evaluate(invalid)
        for row in rows:
            row["metrics"]["ns/op"] = (row["pair"] + 1) * 100 * (1.02 if row["revision"] == "candidate" else 1)
        self.assertAlmostEqual(diagnostic.evaluate(rows)["median_paired_delta_percent"], 2)

    def test_incomplete_malformed_and_failed_samples_refused(self):
        good = self.sample()
        for bad in (good + good, good.replace("12345", "0"), good.replace("PASS", "FAIL"),
                    good.replace("100 ns/op", "nan ns/op"), good.replace("100 ns/op", "0 ns/op"),
                    good.replace("200 B/op", "-1 B/op"), good.replace("3 allocs/op", ""),
                    good.replace("3 allocs/op", "3 allocs/op 3 allocs/op"),
                    good.replace("3 allocs/op", "3 unknown/op"), good + "panic: broken\n"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                diagnostic.parse_sample(bad)

    def test_scope_is_identical_in_workflow_and_excludes_unrelated_changes(self):
        workflow = (ROOT / ".github/workflows/treedb-tests.yml").read_text()
        expression = re.search(r"(?m)^      PRESSURE_STATS_PATH_RE: '([^']+)'$", workflow)[1]
        scope = re.compile(expression)
        included = ["TreeDB/caching/db.go", "TreeDB/caching/pool_pressure_snapshot.go",
                    diagnostic.HARNESS, "TreeDB/caching/pool_pressure_publication_bench_test.go",
                    "scripts/pool_pressure_stats_diagnostic.py", "scripts/mvcc_primary_cost_gate.py",
                    ".github/scripts/test_pool_pressure_stats_diagnostic.py"]
        excluded = ["TreeDB/db/db.go", "TreeDB/mvcc/primary_generation_routing_v6_test.go",
                    "TreeDB/caching/iterator.go", "docs/benchmarks/pool-pressure-publication.md"]
        for path in included + excluded:
            with self.subTest(path=path):
                self.assertEqual(bool(scope.fullmatch(path)), path in included)
                self.assertEqual(bool(diagnostic.SCOPE.fullmatch(path)), path in included)
        start = workflow.index("      - name: Compare public Stats diagnostic")
        step = workflow[start:workflow.index("      - name: Add benchmark summary", start)]
        self.assertIn("steps.paths.outputs.pressure_stats == 'true'", step)
        self.assertNotIn("continue-on-error:", step)
        self.assertIn('"$RUNNER_TEMP/mvcc-raw-path-gate/pressure-stats"', step)
        self.assertIn("scripts/mvcc_primary_cost_gate.py", workflow)

    def test_overlay_fixture_has_only_public_stats_and_preserves_real_samplers(self):
        fixture = (ROOT / diagnostic.HARNESS).read_text()
        self.assertEqual(re.findall(r"func (Benchmark\w+)\(", fixture), [diagnostic.BENCHMARK])
        for call in ("backenddb.Open(", "SetSync(", "cached.Stats()", "b.ReportAllocs()", "b.ResetTimer()", "b.StopTimer()"):
            self.assertIn(call, fixture)
        self.assertGreater(fixture.index("b.StopTimer()"), fixture.index("runtime.KeepAlive(cached.Stats())"))
        for private in ("poolPressureNow", "publishPoolPressureSnapshot", "samplePoolPressureSnapshot"):
            self.assertNotIn(private, fixture)
        self.assertNotIn("func " + diagnostic.BENCHMARK, (ROOT / "TreeDB/caching/pool_pressure_publication_bench_test.go").read_text())


if __name__ == "__main__":
    unittest.main()
