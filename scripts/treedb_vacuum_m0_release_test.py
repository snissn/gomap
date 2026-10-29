#!/usr/bin/env python3
import copy
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
spec = importlib.util.spec_from_file_location('release', HERE / 'treedb_vacuum_m0_release.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
spec_ci = importlib.util.spec_from_file_location('supervisor', HERE / 'treedb_vacuum_m0_ci.py')
ci = importlib.util.module_from_spec(spec_ci)
spec_ci.loader.exec_module(ci)


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.packet = self.root / 'packet'
        self.packet.mkdir()
        self.proc = self.root / 'proc'
        (self.proc / 'sys/kernel/random').mkdir(parents=True)
        (self.proc / 'sys/kernel/random/boot_id').write_text('boot\n')
        (self.proc / 'locks').write_text('')
        (self.proc / 'self/ns').mkdir(parents=True)
        (self.proc / 'self/ns/pid').symlink_to('pid:[123]')
        self.lock = self.root / 'host.lock'
        self.lock.touch()
        stat = self.lock.stat()
        self.device = [os.major(stat.st_dev), os.minor(stat.st_dev), stat.st_ino]
        self.initial = {'phase': 'admission', 'admitted': True,
                        'source_sha': 'a' * 40, 'boot_id': {'value': 'boot\n'},
                        'pid_namespace': 'pid:[123]',
                        'runner': {'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '2'},
                        'configuration': {'M0_CONTROLLED': '1'},
                        'lease': {'path': str(self.lock), 'device_inode': self.device,
                                  'listener': {'pid': 42, 'start_ticks': '100'},
                                  'holder': {'pid': 41, 'start_ticks': '90'}}}
        self.final = copy.deepcopy(self.initial)
        self.final['phase'] = 'post-upload'
        self.write()
        (self.packet / 'ci-budget.json').write_text(json.dumps({
            'source_sha': self.initial['source_sha'], 'run': self.initial['runner'],
            'qualification_profile': 'controlled-performance', 'completed': True,
            'exit_status': 0, 'termination_reason': 'capture-exit'}))

    def write(self):
        for name, data in [('admission', self.initial), ('post-upload', self.final)]:
            (self.packet / f'environment-{name}.json').write_text(json.dumps(data))

    def test_completed_release_is_bound_to_both_receipts_and_exact_run(self):
        receipt = m.observe(self.packet, self.proc)
        self.assertTrue(receipt['listener_exited_observed'])
        self.assertTrue(receipt['lease_unlocked_observed'])
        self.assertEqual(receipt['source_sha'], 'a' * 40)
        self.assertEqual(receipt['run']['GITHUB_RUN_ATTEMPT'], '2')
        self.assertEqual(set(receipt['receipt_sha256']), {'admission', 'post-upload', 'ci-budget'})
        before = {p.name: p.read_bytes() for p in self.packet.iterdir()}
        with mock.patch.object(m, 'observe', return_value=receipt), mock.patch.object(sys, 'argv',
                ['release', '--packet', str(self.packet), '--output', str(self.root / 'release.json')]):
            self.assertEqual(m.main(), 0)
            with self.assertRaises(FileExistsError):
                m.main()
        sealed = json.loads((self.root / 'release.json').read_text())
        self.assertTrue(sealed['release_observation_complete'])
        self.assertFalse(sealed['continuous_custody_claim'])
        self.assertFalse(sealed['performance_acceptance'])
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.packet.iterdir()})

    def test_failed_capture_remains_failed_after_successful_release(self):
        (self.packet / 'ci-budget.json').write_text(json.dumps({
            'source_sha': self.initial['source_sha'], 'run': self.initial['runner'],
            'qualification_profile': 'controlled-performance', 'completed': False,
            'exit_status': 124, 'termination_reason': 'timeout'}))
        self.assertFalse(m.observe(self.packet, self.proc)['capture_outcome']['completed'])
        (self.packet / 'ci-budget.json').unlink()
        with self.assertRaises(FileNotFoundError):
            m.observe(self.packet, self.proc)

    def test_real_successful_foreign_supervisor_receipts_are_rejected(self):
        original = {'source_sha': self.initial['source_sha'], 'run': self.initial['runner']}
        for field in ('matching', 'source', 'run', 'attempt'):
            identity = copy.deepcopy(original)
            if field == 'source':
                identity['source_sha'] = 'b' * 40
            elif field == 'run':
                identity['run']['GITHUB_RUN_ID'] = '124'
            elif field == 'attempt':
                identity['run']['GITHUB_RUN_ATTEMPT'] = '3'
            foreign = self.root / field
            self.assertEqual(ci.supervise([sys.executable, '-c', 'pass'], foreign,
                'controlled-performance', 900, grace=0.1, identity=identity), 0)
            raw = (foreign / 'ci-budget.json').read_bytes()
            self.assertTrue(json.loads(raw)['completed'])
            (self.packet / 'ci-budget.json').write_bytes(raw)
            if field == 'matching':
                self.assertTrue(m.observe(self.packet, self.proc)['capture_outcome']['completed'])
            else:
                with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'supervisor source/run/attempt'):
                    m.observe(self.packet, self.proc)

    def test_unbound_legacy_supervisor_receipt_is_rejected(self):
        original = json.loads((self.packet / 'ci-budget.json').read_text())
        for key in ('source_sha', 'run'):
            value = dict(original)
            del value[key]
            (self.packet / 'ci-budget.json').write_text(json.dumps(value))
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'supervisor source/run/attempt'):
                m.observe(self.packet, self.proc)

    def test_contradictory_supervisor_outcomes_are_rejected(self):
        original = json.loads((self.packet / 'ci-budget.json').read_text())
        for change in ({'exit_status': 124}, {'completed': False},
                       {'termination_reason': 'timeout'}, {'exit_status': -1},
                       {'termination_reason': 'interrupted', 'exit_status': 143, 'completed': False},
                       {'termination_reason': 'cleanup-failure', 'exit_status': 127, 'completed': False},
                       {'interrupted_signal': True}, {'interrupted_signal': 15}):
            with self.subTest(change=change):
                (self.packet / 'ci-budget.json').write_text(json.dumps(dict(original, **change)))
                with self.assertRaisesRegex(ValueError, 'outcome is inconsistent'):
                    m.observe(self.packet, self.proc)

    def test_missing_post_upload_cannot_complete_custody(self):
        (self.packet / 'environment-post-upload.json').unlink()
        with self.assertRaises(FileNotFoundError):
            m.observe(self.packet, self.proc)

    def test_missing_failed_or_changed_identity_is_rejected(self):
        for change in ('admitted', 'source_sha', 'runner', 'lease', 'boot_id', 'configuration', 'pid_namespace'):
            with self.subTest(change=change):
                original = copy.deepcopy(self.final)
                self.final[change] = {'admitted': False, 'source_sha': 'b' * 40,
                    'runner': {'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '3'},
                    'lease': dict(self.final['lease'], path='/wrong.lock'),
                    'boot_id': {'value': 'other-boot'}, 'pid_namespace': 'pid:[456]', 'configuration': {'M0_CONTROLLED': '0'}}[change]
                self.write()
                with self.assertRaises(ValueError):
                    m.observe(self.packet, self.proc)
                self.final = original

    def test_listener_must_exit_even_after_lock_release_and_pid_reuse_is_safe(self):
        with mock.patch.object(m.environment, 'process', return_value={'start_ticks': '100'}):
            with self.assertRaisesRegex(ValueError, 'has not exited'):
                m.observe(self.packet, self.proc)
        with mock.patch.object(m.environment, 'process', return_value={'start_ticks': '101'}):
            self.assertTrue(m.observe(self.packet, self.proc)['listener_exited_observed'])

    def test_relocked_contended_and_malformed_tables_are_rejected(self):
        major, minor, inode = self.device
        for line in (f'1: FLOCK ADVISORY WRITE 41 {major:x}:{minor:x}:{inode} 0 EOF\n',
                     f'1: -> FLOCK ADVISORY WRITE 41 {major:x}:{minor:x}:{inode} 0 EOF\n',
                     'malformed\n'):
            with self.subTest(line=line):
                (self.proc / 'locks').write_text(line)
                with self.assertRaises(ValueError):
                    m.observe(self.packet, self.proc)

    def test_real_kernel_lock_release_is_observed_without_acquiring_it(self):
        self.initial['boot_id'] = {'value': Path('/proc/sys/kernel/random/boot_id').read_text()}
        self.initial['pid_namespace'] = os.readlink('/proc/self/ns/pid')
        self.initial['lease']['listener']['pid'] = 2147483647
        self.final = copy.deepcopy(self.initial)
        self.final['phase'] = 'post-upload'
        self.write()
        with self.lock.open('r') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            with self.assertRaisesRegex(ValueError, 'still locked'):
                m.observe(self.packet)
            fcntl.flock(lock, fcntl.LOCK_UN)
            self.assertTrue(m.observe(self.packet)['lease_unlocked_observed'])

    def test_original_lock_file_cannot_be_replaced_or_symlinked(self):
        self.lock.rename(self.root / 'old.lock')
        self.lock.touch()
        with self.assertRaisesRegex(ValueError, 'identity changed'):
            m.observe(self.packet, self.proc)
        self.lock.unlink()
        self.lock.symlink_to(self.root / 'old.lock')
        with self.assertRaisesRegex(ValueError, 'unavailable'):
            m.observe(self.packet, self.proc)

    def test_failure_receipt_is_retained_and_original_packet_is_never_an_output(self):
        output = self.root / 'failed.json'
        with mock.patch.object(m, 'observe', side_effect=ValueError('still locked')), \
             mock.patch.object(sys, 'argv', ['release', '--packet', str(self.packet), '--output', str(output)]):
            self.assertEqual(m.main(), 2)
            self.assertFalse(json.loads(output.read_text())['release_observation_complete'])
            with self.assertRaises(FileExistsError):
                m.main()
        with mock.patch.object(sys, 'argv', ['release', '--packet', str(self.packet), '--output', str(self.packet / 'release.json')]):
            with self.assertRaisesRegex(ValueError, 'separate'):
                m.main()


if __name__ == '__main__':
    unittest.main()
