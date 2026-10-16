#!/usr/bin/env python3
"""Failure controls for the fixed PRIMARY cost measurement contract."""

import importlib.util
import os
from pathlib import Path
import unittest

path = Path(__file__).with_name("mvcc_primary_cost_gate.py")
if not path.exists():
    path = Path(__file__).resolve().parents[2] / "scripts/mvcc_primary_cost_gate.py"
spec = importlib.util.spec_from_file_location("primary_gate", path)
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class PrimaryCostGateTests(unittest.TestCase):
    def sample(self, ns=100, size=200, allocs=3, generations=0):
        return f"{gate.BENCHMARK} 128 {ns} ns/op {generations} generation_advances/op {size} B/op {allocs} allocs/op\nPASS\n"

    def rows(self):
        return [{"pair": pair, "revision": revision,
                 "metrics": gate.parse_sample(self.sample(generations=2 if revision == "baseline" else 0))}
                for pair in range(8) for revision in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline"))]

    def test_retains_baseline_generation_churn_and_requires_candidate_activation(self):
        rows = self.rows()
        self.assertTrue(gate.evaluate(rows)["measurement_pass"])
        rows[1]["metrics"]["generation_advances/op"] = 1
        self.assertFalse(gate.evaluate(rows)["gates"]["candidate_primary_activation"])

    def test_refuses_incomplete_duplicate_or_reordered_evidence(self):
        rows = self.rows()
        for invalid in (rows[:-1], rows + rows[:1], list(reversed(rows)), rows[:1] + rows):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    gate.evaluate(invalid)

    def test_timing_and_allocation_regressions_fail_independently(self):
        for metric, value, name in (("ns/op", 106, "timing"), ("B/op", 201, "bytes"), ("allocs/op", 4, "allocations")):
            rows = self.rows()
            for row in rows:
                if row["revision"] == "candidate":
                    row["metrics"][metric] = value
            result = gate.evaluate(rows)
            self.assertFalse(result["measurement_pass"])
            self.assertFalse(result["gates"][name])

    def test_malformed_nonfinite_missing_and_unsuccessful_samples_fail(self):
        good = self.sample()
        invalids = (good.replace("128 ", "127 "), good + good, good.replace("PASS", "FAIL"),
                    good.replace("200 B/op", "nan B/op"), good.replace("200 B/op", "-1 B/op"),
                    good.replace("200 B/op", "200 B/op 200 B/op"), good.replace("3 allocs/op", ""),
                    good.replace("100 ns/op", "0 ns/op"))
        for text in invalids:
            with self.subTest(text=text):
                with self.assertRaises(ValueError):
                    gate.parse_sample(text)

    def test_paired_delta_uses_matching_samples(self):
        rows = self.rows()
        for row in rows:
            baseline = 100 + row["pair"] * 100
            row["metrics"]["ns/op"] = baseline * (1.02 if row["revision"] == "candidate" else 1)
        self.assertAlmostEqual(gate.evaluate(rows)["median_paired_delta_percent"], 2)

    def test_process_cpu_survives_exit_and_pid_reuse(self):
        watch = gate.ProcessObservation.__new__(gate.ProcessObservation)
        watch.previous, watch.totals, watch.first = {}, {}, True
        def record(ticks, start="first", parent=0):
            return {"ticks": ticks, "start": start, "parent": parent, "command": "external"}
        watch.accumulate({-1: record(100)})
        watch.accumulate({-1: record(105)})
        watch.accumulate({})
        watch.accumulate({-1: record(2, "reused")})
        self.assertEqual(watch.totals[(-1, "first")]["ticks"], 5)
        self.assertEqual(watch.totals[(-1, "reused")]["ticks"], 2)

    def test_only_own_descendants_and_ancestors_are_excluded(self):
        own = os.getpid()
        snapshot = {own: {"parent": own+1}, own+1: {"parent": 0},
                    own+2: {"parent": own}, own+3: {"parent": own+2}, own+4: {"parent": own+1}}
        self.assertEqual(gate.allowed_processes(snapshot), {own, own+1, own+2, own+3})


if __name__ == "__main__":
    unittest.main()
