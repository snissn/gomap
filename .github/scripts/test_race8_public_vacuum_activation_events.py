"""Fixed activation-only controls; original witness remains non-reproduction-only."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

import test_race8_first_refusal_events as original

m = original.m


def rows(value=None, failed=False, before=None, after=None):
    pkg, top = m.PREFIX + 'collections', m.ACTIVATION_TOP
    result = [{'Package': pkg, 'Action': 'start'},
              {'Package': pkg, 'Test': top, 'Action': 'run'},
              {'Package': pkg, 'Test': top, 'Action': 'output',
               'Output': m.ACTIVATION_BEFORE_MARKER + (before or original.scalar(
                   0, Calls=1, Completed=1, Entered=[1] * 8, Finished=[1] * 8)) + '\n'},
              {'Package': pkg, 'Test': top, 'Action': 'output',
               'Output': m.ACTIVATION_AFTER_MARKER + (after or value or original.scalar(0)) + '\n'},
              {'Package': pkg, 'Test': top, 'Action': 'output',
               'Output': m.MARKER + (value or original.scalar(0)) + '\n'}]
    if failed:
        result.append({'Package': pkg, 'Test': top, 'Action': 'output',
                       'Output': 'actual public VacuumIndexOnline refused\n'})
    result.extend([{'Package': pkg, 'Test': top, 'Action': 'fail' if failed else 'pass'},
                   {'Package': pkg, 'Action': 'fail' if failed else 'pass'}])
    return result


class PublicVacuumActivationControls(unittest.TestCase):
    def check(self, events, go_exit=0):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'activation.jsonl'
            path.write_text(''.join(json.dumps(x) + '\n' for x in events))
            return m.validate(path, 'activation', go_exit)

    def test_exact_positive_and_separate_selector(self):
        checked = self.check(rows())
        self.assertEqual(checked['status'], 'PASS')
        self.assertTrue(checked['activation_verified'])
        self.assertEqual(checked['activation_scalar_deltas']['Calls'], 1)
        self.assertEqual(checked['activation_scalar_deltas']['Completed'], 1)
        self.assertFalse(checked['receipts'][m.ACTIVATION_TOP]['borrower_join_proved'])
        self.assertNotIn(m.ACTIVATION_TOP, m.CONTROLS_SELECTOR)
        self.assertNotEqual(m.ACTIVATION_SELECTOR, m.WITNESS_SELECTOR)

    def test_no_op_and_incomplete_stage_samples_refused(self):
        cases = [original.scalar(0, Calls=0), original.scalar(0, Completed=0),
                 original.scalar(0, Missing=True), original.scalar(0, Overflow=True)]
        for index in (0, 1, 7):
            for field in ('Entered', 'Finished'):
                values = [2] * 8
                values[index] = 0
                cases.append(original.scalar(0, **{field: values}))
        for value in cases:
            with self.subTest(value=value):
                checked = self.check(rows(value))
                self.assertEqual(checked['status'], 'INCOMPLETE_ACTIVATION')
                self.assertFalse(checked['accepted'])
                self.assertFalse(checked['activation_verified'])

    def test_refusal_preserved_not_coverage_success(self):
        value = original.scalar()
        checked = self.check(rows(value, failed=True), go_exit=1)
        self.assertEqual(checked['status'], 'CONTROL_FAILURE')
        self.assertEqual(checked['go_exit'], 1)
        self.assertFalse(checked['accepted'])
        self.assertEqual(checked['receipts'][m.ACTIVATION_TOP]['raw'], value)
        self.assertIn('actual public VacuumIndexOnline refused',
                      ''.join(x['Output'] for x in checked['diagnostic_output']))
        self.assertFalse(self.check(rows(value))['accepted'])

    def test_missing_duplicate_and_foreign_receipts_refused(self):
        base = rows()
        for index in (2, 3, 4):
            for changed in (base[:index] + base[index + 1:],
                            base[:index + 1] + [copy.deepcopy(base[index])] + base[index + 1:]):
                self.assertFalse(self.check(changed)['accepted'])
            changed = copy.deepcopy(base)
            changed[index]['Test'] = m.TOP
            self.assertEqual(self.check(changed)['status'], 'INVALID_EVENT_EVIDENCE')

    def test_cumulative_setup_or_close_cannot_qualify_interval(self):
        # Setup and Close can have arbitrarily positive cumulative observations.
        # No work between bracketing samples must remain ineligible.
        cumulative = original.scalar(0, Calls=100, Completed=100,
                                     Entered=[100] * 8, Finished=[100] * 8)
        checked = self.check(rows(value=cumulative, before=cumulative, after=cumulative))
        self.assertEqual(checked['status'], 'INCOMPLETE_ACTIVATION')
        self.assertFalse(checked['activation_verified'])
        # A final sample taken after Close cannot repair a missing interval increment.
        checked = self.check(rows(value=cumulative, after=original.scalar(
            0, Calls=1, Completed=1, Entered=[1] * 8, Finished=[1] * 8)))
        self.assertFalse(checked['accepted'])

    def test_interval_order_flags_and_each_required_delta(self):
        base = rows()
        changed = copy.deepcopy(base)
        changed[2], changed[3] = changed[3], changed[2]
        self.assertFalse(self.check(changed)['accepted'])
        for field in ('Calls', 'Completed'):
            for amount in (0, 1):  # Decrease and zero delta from the before value 1.
                self.assertFalse(self.check(rows(after=original.scalar(0, **{field: amount})))['accepted'])
        for index in (0, 1, 7):
            for field in ('Entered', 'Finished'):
                for amount in (0, 1):
                    values = [2] * 8
                    values[index] = amount
                    self.assertFalse(self.check(rows(after=original.scalar(0, **{field: values})))['accepted'])
        for field in ('Missing', 'Overflow'):
            self.assertFalse(self.check(rows(before=original.scalar(0, **{field: True})))['accepted'])
        self.assertFalse(self.check(rows(before=original.scalar()))['accepted'])

    def test_independent_samples_do_not_require_equal_counts(self):
        checked = self.check(rows(before=original.scalar(
            0, Calls=10, Completed=3, Entered=[7] * 8, Finished=[2] * 8),
            after=original.scalar(0, Calls=11, Completed=9,
                                  Entered=[10] * 8, Finished=[4] * 8)))
        self.assertTrue(checked['accepted'])
        self.assertFalse(checked['receipts'][m.ACTIVATION_TOP]['borrower_join_proved'])

    def test_original_zero_witness_meaning_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'original.jsonl'
            path.write_text(''.join(json.dumps(x) + '\n' for x in original.events(
                value=original.scalar(0, Calls=0, Completed=0,
                                      Entered=[0] * 8, Finished=[0] * 8))))
            checked = m.validate(path, 'witness', 0)
        self.assertEqual(checked['status'], 'PASS')
        self.assertEqual(checked['receipts'][m.TOP]['attribution'], 'UNKNOWN')
        self.assertNotIn('activation_verified', checked)

    def test_partial_skip_race_and_nonzero_are_not_positive(self):
        base = rows()
        self.assertFalse(self.check(base[:-1])['accepted'])
        changed = copy.deepcopy(base)
        changed[-2]['Action'] = 'skip'
        self.assertFalse(self.check(changed)['accepted'])
        changed = copy.deepcopy(base)
        warning = copy.deepcopy(changed[2])
        warning['Output'] = 'WARNING: DATA RACE\n'
        changed.insert(2, warning)
        self.assertEqual(self.check(changed)['status'], 'RACE_DETECTED')
        self.assertFalse(self.check(base, go_exit=1)['accepted'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
