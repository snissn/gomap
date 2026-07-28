#!/usr/bin/env python3
"""Contract tests for bounded unified-suite workflow headroom."""

from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "unified-bench-suites.yml"


class UnifiedBenchSuitesHeadroomContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = WORKFLOW.read_text(encoding="utf-8")
        cls.suites = cls.source.split("  suites:\n", 1)[1].split("\n  profiles:\n", 1)[0]
        cls.profiles = cls.source.split("  profiles:\n", 1)[1]

    def test_suites_job_has_measured_outer_margin(self) -> None:
        self.assertIn("    timeout-minutes: 25", self.suites)

    def test_suite_gates_and_fail_closed_limits_are_preserved(self) -> None:
        self.assertIn(
            "./bin/unified-bench -suite flushthrash -keys 200000 -seed 1 -progress=false",
            self.suites,
        )
        self.assertIn(
            "./bin/unified-bench -suite longmix -keys 100001 -seed 1 -progress=false",
            self.suites,
        )
        self.assertIn(
            "./bin/unified-bench -suite bigkeys_guard -keys 1000000 -seed 1 -progress=false -max-wall 15m -max-rss-mb 4096",
            self.suites,
        )

    def test_profile_gates_remain_enabled(self) -> None:
        self.assertIn("timeout-minutes: 20", self.profiles)
        self.assertIn("profile: [balanced, fast, durable]", self.profiles)


if __name__ == "__main__":
    unittest.main()
