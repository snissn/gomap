"""Matched-work admission tests using synthetic reports; no DB/process collection."""
import copy
import json
import pathlib
import sys
import subprocess
from unittest import mock
import tempfile
import unittest

import unified_bench_quicksilver_capture as capture
import test_unified_bench_quicksilver_capture as legacy

report = legacy.report


def matched_report(mode='fixed-work'):
    r = report()
    r['config']['concurrent_mode'] = mode
    r['registered_cli_flags']['quicksilver-concurrent-mode'] = mode
    p = r['phases'][3]
    p['composition_seconds'] = 2.
    p.update(process_heap_alloc_before=0, process_heap_alloc_after=0, process_gc_pause_ns=0, process_gc_cycles=0)
    cut = dict(process_allocated_bytes=0, process_mallocs=0, process_heap_alloc_before=0,
               process_heap_alloc_after=0, process_gc_pause_ns=0, process_gc_cycles=0)
    progress = dict(elapsed_ns=1000000001, completed_groups=0, completed_mutation_targets=0,
                    successful_commits=0, committed_set_operations=0, committed_delete_operations=0,
                    completed_checkpoints=0, checkpoint_in_flight=False)
    end = dict(progress, elapsed_ns=1500000001, completed_groups=40, completed_mutation_targets=40000,
               successful_commits=160, committed_set_operations=60000, committed_delete_operations=10000,
               completed_checkpoints=4)
    w = dict(schema=1, mode=mode, allocation_scope='process_go_runtime_concurrent_composition',
             cpu_profile_scope='both_join' if mode == 'fixed-work' else 'reader_join',
             allocs_profile_scope='phase_return_including_report_orchestration',
             requested_reads=10000 if mode == 'fixed-work' else 0, requested_mutation_targets=40000,
             requested_read_counts=[2500]*4 if mode == 'fixed-work' else [0]*4,
             completed_read_counts=[2500]*4, completed_reads=10000, reader_joined=True, writer_joined=True, completed=True,
             reader_elapsed_ns=1000000000, both_join_elapsed_ns=1500000000,
             reader_cut=cut, reader_cut_progress=dict(before=progress, after=dict(progress, elapsed_ns=1000000002)),
             after_both_join=end, both_join_bytes_per_read=0., both_join_mallocs_per_read=0.,
             both_join_bytes_per_mutation_target=0., both_join_mallocs_per_mutation_target=0.)
    if mode == 'fixed-work':
        w['both_join'] = dict(cut, process_allocated_bytes=10000, process_mallocs=100)
        w.update(both_join_bytes_per_read=1., both_join_mallocs_per_read=.01,
                 both_join_bytes_per_mutation_target=.25, both_join_mallocs_per_mutation_target=.0025)
    p['concurrent_work'] = w
    return r


class MatchedWork(unittest.TestCase):
    def validate(self, r=None, mode='fixed-work'):
        r = r or matched_report(mode)
        with tempfile.TemporaryDirectory() as tmp:
            directory, metadata = legacy.CaptureRehearsal.validation_fixture(tmp)
            if mode == 'fixed-work':
                metadata['command'] += ['-quicksilver-concurrent-mode', mode]
            cell = dict(engine='treedb', keys=40000, reads=10000, concurrent_mode=mode)
            capture.validate([r], cell, directory, {}, metadata)

    def test_good_complete_composition_and_legacy(self):
        self.validate()
        self.validate(mode='duration')
        with tempfile.TemporaryDirectory() as tmp:
            directory, metadata = legacy.CaptureRehearsal.validation_fixture(tmp)
            capture.validate([report()], dict(engine='treedb',keys=40000,reads=10000), directory, {}, metadata)
        self.assertEqual(capture.concurrent_mode({}), 'duration')
        for bad in ['fixed', None, True, 1]:
            with self.assertRaises(AssertionError):
                capture.concurrent_mode(dict(concurrent_mode=bad))

    def test_hostile_admission(self):
        def work(r): return r['phases'][3]['concurrent_work']
        changes = [lambda r: r['config'].update(concurrent_mode='duration'),
                   lambda r: r['registered_cli_flags'].update(**{'quicksilver-concurrent-mode': 'duration'}),
                   lambda r: r['phases'][3].pop('concurrent_work'),
                   lambda r: work(r).update(schema=True), lambda r: work(r).update(completed=1),
                   lambda r: work(r).update(reader_joined=False), lambda r: work(r).update(writer_joined=False),
                   lambda r: work(r).update(completed_reads=9999), lambda r: work(r).update(error='canceled'),
                   lambda r: work(r).update(requested_reads=True), lambda r: work(r).pop('both_join'),
                   lambda r: work(r).update(extra='alias'),
                   lambda r: work(r).update(completed_read_counts=[2501,2499,2500,2500]),
                   lambda r: work(r).update(requested_read_counts=[True]*4), lambda r: work(r).update(cpu_profile_scope='reader_join'),
                   lambda r: work(r).update(both_join_bytes_per_read=float('nan')),
                   lambda r: work(r).update(both_join_mallocs_per_read=float('inf')),
                   lambda r: work(r).update(both_join_bytes_per_mutation_target=-1),
                   lambda r: work(r)['after_both_join'].update(successful_commits=159),
                   lambda r: work(r)['after_both_join'].update(completed_groups=True),
                   lambda r: work(r)['after_both_join'].update(checkpoint_in_flight=True),
                   lambda r: work(r)['after_both_join'].update(committed_set_operations=40000),
                   lambda r: work(r)['reader_cut'].update(process_allocated_bytes=1),
                   lambda r: work(r)['both_join'].update(process_mallocs=True),
                   lambda r: work(r)['reader_cut_progress']['after'].update(elapsed_ns=1),
                   lambda r: work(r)['reader_cut_progress']['before'].update(completed_groups=2),
                   lambda r: r['phases'][0].update(concurrent_work=copy.deepcopy(work(r)))]
        for i,change in enumerate(changes):
            with self.subTest(case=i):
                r=matched_report();change(r)
                with self.assertRaises((AssertionError,KeyError,TypeError)):
                    self.validate(r)

    def test_cut_progress_can_bracket_actual_completion(self):
        r=matched_report();w=r['phases'][3]['concurrent_work']
        w['reader_cut_progress']['after'].update(completed_groups=1, completed_mutation_targets=1000,
                                                successful_commits=4, committed_set_operations=1500,
                                                committed_delete_operations=250, checkpoint_in_flight=True)
        self.validate(r)
        w['reader_cut_progress']['before'].update(completed_groups=2,completed_mutation_targets=2000)
        with self.assertRaises(AssertionError):self.validate(r)

    def test_actual_mode_argv_is_bound(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory, metadata = legacy.CaptureRehearsal.validation_fixture(tmp)
            cell = dict(engine='treedb',keys=40000,reads=10000,concurrent_mode='fixed-work')
            for flags in ([], ['-quicksilver-concurrent-mode','duration'],
                          ['-quicksilver-concurrent-mode=fixed-work'],
                          ['-quicksilver-concurrent-mode','fixed-work','-quicksilver-concurrent-mode','duration']):
                metadata['command'] = [metadata['command'][0]] + flags
                with self.assertRaises(AssertionError): capture.validate([matched_report()],cell,directory,{},metadata)

    def test_collector_command_and_failed_raw_are_retained(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp); (root/'bin').mkdir()
            binary=root/'bin'/'fake'; binary.write_bytes(b'\x7fELFfake')
            receipt=root/'receipt.json'; receipt.write_text('{}')
            manifest=dict(build_env={'GOWORK':'off'},sources={'test':dict(head='bound',binary='fake',binary_sha256=capture.sha256(binary))},libraries={},
                          receipts={role:dict(path='receipt.json',sha256=capture.sha256(receipt)) for role in ('source','build','native','runner')})
            manifest_path=root/'manifest.json'; manifest_path.write_text(json.dumps(manifest))
            plan_path=root/'plan.json'
            commands=[]
            packet=None
            def run(command, **kwargs):
                commands.append(command)
                kwargs['stdout'].write(json.dumps([packet])); kwargs['stderr'].write('retained raw error')
                return subprocess.CompletedProcess(command, 0)
            for mode, damaged in [('duration',False),('fixed-work',False),('fixed-work',True)]:
                label=mode+('-bad' if damaged else '')
                cell=dict(label=label,source='test',engine='treedb',keys=40000,reads=10000,concurrent_mode=mode)
                plan_path.write_text(json.dumps(dict(output=label,cells=[cell])))
                packet=matched_report(mode)
                if damaged: packet['phases'][3]['concurrent_work']['writer_joined']=False
                with mock.patch.object(sys,'argv',['capture',str(manifest_path),str(plan_path)]), mock.patch.object(capture,'validate_native'), mock.patch.object(capture.subprocess,'run',run):
                    if damaged:
                        with self.assertRaises(AssertionError): capture.main()
                    else: capture.main()
                actual=commands[-1]
                self.assertEqual('-quicksilver-concurrent-mode' in actual,mode=='fixed-work')
                metadata=json.loads((root/label/('1-'+label)/'run.json').read_text())
                self.assertEqual(metadata.get('validated',False),not damaged)
                self.assertEqual(metadata['command'],actual[2:])
                self.assertEqual(metadata['plan_sha256'],capture.sha256(plan_path))
                self.assertEqual(json.loads((root/label/('1-'+label)/'stdout.json').read_text()),[packet])
                self.assertIn('retained raw error',(root/label/('1-'+label)/'stderr.log').read_text())
                if damaged: self.assertIn('error',metadata)

    def test_timed_and_fixed_receipts_cannot_mix(self):
        r=matched_report();r['config']['concurrent_mode']='duration';r['registered_cli_flags']['quicksilver-concurrent-mode']='duration'
        with self.assertRaises(AssertionError):self.validate(r,mode='duration')
        with self.assertRaises(AssertionError):self.validate(matched_report('duration'))

if __name__ == '__main__':
    unittest.main()
