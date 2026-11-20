#!/usr/bin/env python3

from collections import Counter
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import mvcc_primary_interleave_diagnostic as diag
import mvcc_primary_cost_gate as gate


class InterleavingTest(unittest.TestCase):
    def test_offered_slots_retain_full_factorial(self):
        self.assertEqual(len(set(diag.BLOCKS)), 12)
        for slot in range(1, 5):
            self.assertEqual(Counter(diag.rotate(slot)), Counter(diag.BLOCKS))

    def test_resident_grants_preserve_role_totals_and_chunk_order(self):
        for first in diag.ROLE:
            for schedule in ("resident-serial", "interleaved"):
                with self.subTest(first=first, schedule=schedule):
                    order = diag.grants(schedule, first)
                    self.assertEqual(order[0], (0, first))
                    for role in diag.ROLE:
                        self.assertEqual([n for n, actual in order if actual == role], list(range(8)))
            self.assertEqual(Counter(role for _, role in diag.grants("interleaved", first)[::2]),
                             dict.fromkeys(diag.ROLE, 4))

    def test_readback_and_timing_inventory_cannot_be_skipped(self):
        row = f"{gate.BENCHMARK} 128 2000000 ns/op 0 generation_advances/op 100 B/op 2 allocs/op\nPASS\n"
        boundaries = "".join(f'PRIMARY_WINDOW_START part={i} dir="/isolated"\n'
                             f"PRIMARY_WINDOW_STOP part={i} total_ns={(i + 1) * 32_000_000}\n"
                             for i in range(8))
        count = "PRIMARY_COUNT normal=128 extra=32\n"
        readback = "PRIMARY_READBACK normal=128 extra=32 value=again\n"
        evidence = boundaries + count + readback
        self.assertEqual(diag.check_sample(row, evidence, "interleaved", True, [], trace=True)["extra"], 32)
        self.assertEqual(diag.check_sample(row, boundaries + count, "interleaved", True, [], trace=False)["extra"], 32)
        for invalid in (boundaries, evidence.replace("extra=32", "extra=0"),
                        evidence.replace("part=4", "part=1"), evidence.replace("256000000", "256001280")):
            with self.assertRaises(ValueError):
                diag.check_sample(row, invalid, "interleaved", True, [], trace=True)


if __name__ == "__main__":
    unittest.main()
