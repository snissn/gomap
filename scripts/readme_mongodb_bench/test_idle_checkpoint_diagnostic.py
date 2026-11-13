"""Offline controls for diagnostic source binding and evidence boundaries."""
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace

import idle_checkpoint_diagnostic as diagnostic


class IdleCheckpointDiagnosticControls(unittest.TestCase):
    def test_source_anchor_changes_fail_closed(self):
        self.assertEqual(diagnostic.replace_once('abc', 'b', 'X'), 'aXc')
        for source in ('ac', 'abbc'):
            with self.assertRaisesRegex(ValueError, 'anchor changed'):
                diagnostic.replace_once(source, 'b', 'X')

    def test_process_allocations_use_total_operations_and_actual_writes(self):
        keys = ('idle5415.total_alloc', 'idle5415.mallocs',
                'ycsb.gateway.write_concern.LogicalWrites',
                'ycsb.gateway.write_concern.VisibleAcknowledgements',
                'treedb.cache.cow.txn.logical_seq', 'treedb.cache.auto_checkpoint.count')
        before = dict.fromkeys(keys, '100')
        after = dict(zip(keys, ('2100', '500', '105', '105', '105', '101')))
        result = diagnostic.allocation_delta(before, after, 100, 5)
        self.assertEqual(result['process_bytes_per_operation'], 20)
        self.assertEqual(result['process_objects_per_operation'], 4)
        self.assertIn('not update-only', result['basis'])
        with self.assertRaisesRegex(ValueError, 'actual logical writes'):
            diagnostic.allocation_delta(before, after, 100, 6)
        after['idle5415.mallocs'] = '99'
        with self.assertRaisesRegex(ValueError, 'counter decreased'):
            diagnostic.allocation_delta(before, after, 100, 5)

    def test_maintenance_pairing_preserves_reason_write_activity_and_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'events.jsonl'
            begin = dict(id=1, pid=123, ordinal=1, stage='begin', reason='idle', unix_nano=1000000,
                         elapsed_nano=0, last_write_unix_nano=950000, failed=False)
            end = dict(begin, stage='end', unix_nano=1200000, elapsed_nano=200000,
                       last_write_unix_nano=1100000)
            def write(events):
                path.write_text(''.join(json.dumps(e)+'\n' for e in events))
            write([begin, end])
            result = diagnostic.maintenance_events(path, 123)
            self.assertEqual(result[0]['begin_quiet_ms'], .05)
            self.assertTrue(result[0]['writes_published_during_pass'])
            for events in ([begin], [end], [begin, dict(end, failed=True)],
                           [dict(begin, pid=999), end], [begin, dict(end, reason='size')]):
                write(events)
                with self.assertRaises(ValueError):
                    diagnostic.maintenance_events(path, 123)

    def test_missing_duplicate_trace_cannot_hide_observed_completions(self):
        allocation = dict(auto_checkpoint_before=0, auto_checkpoint_after=2)
        passes = [dict(end=dict(ordinal=i)) for i in (1, 2)]
        diagnostic.require_event_coverage(passes, allocation)
        for events in ([], passes[:1], passes+passes[:1]):
            with self.assertRaisesRegex(ValueError, 'does not cover'):
                diagnostic.require_event_coverage(events, allocation)
        diagnostic.require_event_coverage([], dict(auto_checkpoint_before=2, auto_checkpoint_after=2))

    def test_fresh_attempt_does_not_reuse_failed_database_label(self):
        with tempfile.TemporaryDirectory() as directory:
            labels = []
            arm = SimpleNamespace(gateway=lambda: None, ycsb_phase=lambda: None)
            def fail(*args):
                labels.append(arm.OUT.name)
                raise ValueError('preserved failed attempt')
            arm.run_case = fail
            for attempt in ('one', 'two'):
                out = Path(directory)/attempt
                out.mkdir()
                with self.assertRaisesRegex(ValueError, 'preserved failed'):
                    diagnostic.collect(arm, Path('/unused'), {}, out/'arm-A', 10000)
            self.assertNotEqual(*labels)


if __name__ == '__main__':
    unittest.main()
