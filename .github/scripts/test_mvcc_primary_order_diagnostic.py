#!/usr/bin/env python3
"""Exercise the raw evidence parsers and fixed control schedule."""

from collections import Counter
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import mvcc_primary_order_diagnostic as diagnostic
import mvcc_primary_cost_gate as gate


class PrimaryOrderDiagnosticTests(unittest.TestCase):
    def test_every_full_block_has_an_opposite_start(self):
        self.assertEqual(len(diagnostic.BLOCKS), 8)
        self.assertEqual(set(diagnostic.BLOCKS),
                         {(protocol, mode, first) for protocol in ("unconditioned", "preconditioned")
                          for mode in ("identical", "slow") for first in ("baseline", "candidate")})
        for _, _, first in diagnostic.BLOCKS:
            schedule = gate.sample_order(first)
            self.assertEqual(schedule[0], (0, first))
            self.assertEqual(Counter(revision for _, revision in schedule), {"baseline": 8, "candidate": 8})
            self.assertEqual(Counter(revision for _, revision in schedule[::2]), {"baseline": 4, "candidate": 4})

    def test_overlay_preserves_public_operation_and_lifecycle(self):
        fixture = (ROOT / gate.HARNESS).read_text()
        overlay = diagnostic.instrument(fixture)
        benchmark = f"func {gate.BENCHMARK}("
        self.assertIn(fixture[fixture.index("func " + gate.LIFECYCLE):fixture.index(benchmark)], overlay)
        self.assertIn("st.CommitAt(100, mutation, CommitDurable)", overlay)
        self.assertIn("d.Close()", overlay)
        self.assertLess(overlay.index("st.CommitAt(100, mutation, CommitDurable)"), overlay.index("if dose != 0"))
        self.assertLess(overlay.index("if dose != 0"), overlay.index("b.StopTimer()"))
        with self.assertRaises(ValueError):
            diagnostic.instrument(fixture.replace("b.ResetTimer()", "b.StartTimer()"))

    def sample(self, injection=0, generations=0):
        return ("PRIMARY_CLOCK n=1 cpu_ns=1200000 injected_ns=" + str(injection) + "\n"
                f"PRIMARY_CLOCK n=128 cpu_ns=143000000 injected_ns={128 * injection}\n"
                f"{gate.BENCHMARK} 128 5000000 ns/op {generations} generation_advances/op 200 B/op 3 allocs/op\nPASS\n")

    def test_requires_observed_slow_dose_on_both_calibration_and_measured_row(self):
        self.assertEqual(diagnostic.sample(self.sample(), slow=False)["injected_cpu_ns_per_op"], 0)
        slow = self.sample(injection=diagnostic.SLOW_CPU_NS)
        self.assertEqual(diagnostic.sample(slow, slow=True)["injected_cpu_ns_per_op"], diagnostic.SLOW_CPU_NS)
        for data, flag in ((self.sample(), True), (slow, False), (self.sample(generations=1), False),
                           (slow.replace("n=1 cpu", "n=2 cpu"), True), (slow + slow, True)):
            with self.subTest(data=data, flag=flag), self.assertRaises(ValueError):
                diagnostic.sample(data, slow=flag)

    def test_trace_requires_exact_count_and_real_primary_path(self):
        ready = '4 10.000001 write(2, "PRIMARY_START n=128\\n", 20) = 20 <0.000001>\n'
        complete = '4 12.000001 write(2, "PRIMARY_STOP n=128\\n", 19) = 19 <0.000001>\n'
        ordinary = '4 10.000001 fdatasync(8</mnt/example/index.db.primary>) = 0 <0.001000>\n'
        split = ('4 11.000001 fdatasync(8</mnt/example/index.db.primary> <unfinished ...>\n'
                 '4 11.001001 <... fdatasync resumed>) = 0 <0.001000>\n')
        traces = ready + ordinary * 127 + split + complete
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "trace"
            path.write_text(traces)
            result = diagnostic.sync_trace(path)
            self.assertEqual(result["calls"], {"fdatasync index.db.primary": 128})
            self.assertAlmostEqual(result["sum_syscall_ns_per_op"], 1_000_000)
            for invalid in (traces.replace("/index.db.primary", "/value-l0.log"),
                            traces.replace(ordinary, "", 1),
                            traces.replace(complete, ""),
                            traces.replace("= 0 <0.001000>", "= -1 EIO <0.001000>", 1)):
                path.write_text(invalid)
                with self.assertRaises(ValueError):
                    diagnostic.sync_trace(path)


if __name__ == "__main__":
    unittest.main()
