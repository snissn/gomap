"""Offline controls for diagnostic isolation, drift and sample handling."""
from contextlib import ExitStack
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import idle_checkpoint_ab as ab


class IdleCheckpointABControls(unittest.TestCase):
    def test_only_twelve_fresh_gomap_slots_with_long_baseline_first(self):
        slots = ab.schedule(['AB', 'BA', 'AB'], 10000, 100000)
        self.assertEqual(len(slots), 12)
        self.assertEqual(slots[0], dict(length='long', operations=100000, repeat=1, arm='A'))
        self.assertEqual(len({(s['length'], s['repeat'], s['arm']) for s in slots}), 12)
        for order, short, long in ((['BA', 'AB', 'BA'], 10000, 100000),
                                   (['AB']*3, 10000, 100000),
                                   (['AB', 'BA', 'AB'], 1000, 100000),
                                   (['AB', 'BA', 'AB'], 10000, 10000)):
            with self.subTest(order=order, short=short, long=long), self.assertRaises(ValueError):
                ab.schedule(order, short, long)

    def test_lane_contention_and_release(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'host.lock'
            with ExitStack() as owner:
                ab.lock_lane(owner, [path])
                with ExitStack() as other, self.assertRaisesRegex(ValueError, 'already owned'):
                    ab.lock_lane(other, [path])
            with ExitStack() as successor:
                ab.lock_lane(successor, [path])

    def test_drift_is_rejected_before_importing_frozen_code(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'recipe').mkdir()
            (root/'recipe/run.py').write_text('raise RuntimeError("must not execute changed recipe")\n')
            (root/'freeze.json').write_text(json.dumps({'recipe_sources': {
                'run.py': '0'*64, 'ycsb_log.py': '0'*64, 'census.go': '0'*64}}))
            with self.assertRaisesRegex(ValueError, 'unexpected frozen manifest'):
                ab.load_arm(root, 'A', '0'*64)
            with self.assertRaisesRegex(ValueError, 'frozen recipe changed'):
                ab.load_arm(root, 'A', ab.recipe.sha(root/'freeze.json'))

    def test_allocation_absence_never_becomes_acceptance(self):
        results = {}
        for length in ('long', 'short'):
            for repeat in (1, 2, 3):
                for arm in ('A', 'B'):
                    results[(length, repeat, arm)] = {'run': {
                        'TOTAL': {'ops_per_sec': 1000},
                        'UPDATE': {'p95_us': 10, 'p99_us': 20}}}
        summary = ab.summarize(results, .95, 1.1)
        self.assertEqual(summary['timing_guardrail'], 'PASS')
        self.assertIn('INCOMPLETE', summary['acceptance'])
        results[('long', 1, 'B')]['run']['TOTAL']['ops_per_sec'] = 800
        results[('long', 2, 'B')]['run']['TOTAL']['ops_per_sec'] = 800
        self.assertEqual(ab.summarize(results, .95, 1.1)['timing_guardrail'], 'FAIL')

    def test_short_long_baseline_stops_before_any_candidate_and_retains_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base, cand = root/'baseline', root/'candidate'
            base.mkdir()
            cand.mkdir()
            out = root/'results'
            launched = []

            def arm(path, name, digest):
                instance = SimpleNamespace(ROOT=path, FREEZE_HASH=digest, verify_freeze=lambda: None,
                                           FREEZE={'product_commit': name*40})
                def run_case(suite, label, target, repeat, count, ops):
                    launched.append((name, ops))
                    case = instance.OUT/(suite+'-'+label+'-p'+str(repeat)+'-'+target)
                    case.mkdir()
                    ab.recipe.save(case/'result.json', {'run': {'TOTAL': {'seconds': 29}}})
                instance.run_case = run_case
                return instance

            argv = ['idle_checkpoint_ab.py', '--baseline-root', str(base), '--candidate-root', str(cand),
                    '--baseline-freeze-sha256', 'a'*64, '--candidate-freeze-sha256', 'b'*64,
                    '--out', str(out), '--lane-lock', str(root/'host.lock'), '--long-ops', '100000']
            with patch.object(ab.sys, 'argv', argv), patch.object(ab, 'load_arm', side_effect=arm), \
                    patch.object(ab, 'compatible'):
                with self.assertRaisesRegex(ValueError, 'shorter than 30s'):
                    ab.main()
                self.assertEqual(launched, [('A', 100000)])
                self.assertEqual(json.loads((out/'failure.json').read_text())['completed_slots'], 1)
                self.assertTrue(next(out.rglob('result.json')).exists())
                with self.assertRaisesRegex(ValueError, 'output already exists'):
                    ab.main()


if __name__ == '__main__':
    unittest.main()
