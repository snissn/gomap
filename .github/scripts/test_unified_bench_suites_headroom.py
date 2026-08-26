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
        cls.suites = cls.source.split("  suites:\n", 1)[1].split("\n  bigkeys_guard:\n", 1)[0]
        cls.bigkeys = cls.source.split("  bigkeys_guard:\n", 1)[1].split("\n  profiles:\n", 1)[0]
        cls.profiles = cls.source.split("  profiles:\n", 1)[1]

    def test_bigkeys_has_an_independent_bounded_job(self) -> None:
        self.assertIn("name: bigkeys_guard (linux)", self.bigkeys)
        self.assertRegex(self.bigkeys, r"(?m)^    timeout-minutes: 25$")
        self.assertRegex(self.suites, r"(?m)^    timeout-minutes: 20$")
        self.assertNotIn("Suite: bigkeys_guard", self.suites)
        self.assertNotIn(
            "run: ./bin/unified-bench -suite bigkeys_guard",
            self.suites,
        )

    def test_suite_gates_and_fail_closed_limits_are_preserved(self) -> None:
        self.assertIn(
            "run: ./bin/unified-bench -suite flushthrash -keys 200000 -seed 1 -progress=false",
            self.suites,
        )
        self.assertIn(
            "run: ./bin/unified-bench -suite longmix -keys 100001 -seed 1 -progress=false",
            self.suites,
        )
        self.assertIn(
            "run: ./bin/unified-bench -suite bigkeys_guard -keys 1000000 -seed 1 -progress=false -max-wall 20m -max-rss-mb 4096",
            self.bigkeys,
        )

    def test_profile_gates_remain_enabled(self) -> None:
        self.assertRegex(self.profiles, r"(?m)^    timeout-minutes: 20$")
        self.assertIn("profile: [balanced, fast, durable]", self.profiles)
        self.assertIn(
            "run: ./bin/unified-bench -dbs treedb -profile ${{ matrix.profile }} "
            "-test write_rand,read_rand -keys 20000 -seed 1 -progress=false "
            "-checkpoint-between-tests",
            self.profiles,
        )


if __name__ == "__main__":
    unittest.main()
