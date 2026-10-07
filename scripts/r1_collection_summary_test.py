#!/usr/bin/env python3
"""Reject stable throughput hiding unstable process latency."""
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location('r1_summary', Path(__file__).with_name('r1_collection_summary.py'))
SUMMARY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUMMARY)

class NoiseRuleTests(unittest.TestCase):
    def packet(self, rates, medians):
        cells = []
        for index, (rate, median) in enumerate(zip(rates, medians)):
            phase = {'name': 'point', 'ops_per_sec': rate, 'p50_ns': median,
                     'ns_per_op': median, 'bytes_per_op': 10, 'allocs_per_op': 1}
            cells.append({'engine': 'typed-row', 'repetition': index,
                          'state_transition': {'skipped': 'outside'},
                          'setup': {'skipped': 'outside'}, 'warmup': {'skipped': 'outside'},
                          'phases': [phase]})
        return {'schema': 'test', 'config': {'qualification': 'retained'}, 'source': {},
                'fixture_sha256': 'fixture', 'cells': cells}

    def test_both_limits_required(self):
        for rates, medians in [([100] * 5, [100, 100, 100, 100, 150]),
                               ([80, 100, 100, 100, 120], [100] * 5),
                               ([100] * 5, [0] * 5)]:
            result = SUMMARY.summarize(self.packet(rates, medians))
            self.assertEqual(result['summaries'][0]['noise_status'], 'inconclusive')
            self.assertEqual(result['summaries'][0]['repetitions'], 5)

    def test_stable_group_passes_without_discarding_cells(self):
        result = SUMMARY.summarize(self.packet([99, 100, 100, 100, 101], [99, 100, 100, 100, 101]))
        self.assertEqual(result['summaries'][0]['noise_status'], 'within_limits')
        self.assertEqual(result['summaries'][0]['repetitions'], 5)
        self.assertGreater(result['summaries'][0]['p50_cv'], 0)

if __name__ == '__main__':
    unittest.main()
