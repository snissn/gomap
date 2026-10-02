"""Synthetic fail-closed controls; no fixture collection or runner activity."""
import copy
import itertools
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import capture


class CaptureTests(unittest.TestCase):
    def fixture(self):
        freeze = {k: 'a' * (40 if k in ('runtime_head', 'runtime_tree') else 64)
                  for k in ('runtime_head', 'runtime_tree', 'harness_sha256', 'binary_sha256')}
        packet = dict(schema='algorithm-work-v1', keys=250000, updates=40000, commits=40,
                      ack_batch_ops=1000, permutation_multiplier=7919, key_bytes=32, value_bytes=256,
                      allowed_concurrent_generations=[0, 1], flush_threshold=64 << 20,
                      read_sample_stride=16, validated_all_values_and_misses=True, final_close_checked=True,
                      read_count=32, read_samples=2, interval_ns=100, ack_ns=20, checkpoint_ns=30,
                      read_p99_ns=10, read_p999_ns=10, read_max_ns=10, provenance=dict(freeze, pilot=False),
                      before={'treedb.command_wal.applied_lsn': '1'},
                      after={'treedb.command_wal.applied_lsn': '41', 'treedb.command_wal.live_accepted_max_lsn': '41'})
        packets = [dict(packet, pointer_threshold=p, checkpoints=c, coalescing_wide=w)
                   for p, c, w in itertools.product((1, 16384), (1, 4), (False, True))]
        for key in capture.WORK_COUNTERS:
            packet['before'][key], packet['after'][key] = '1', '2'
        rows = [name + '-2 1 100 ns/op 10 B/op 1 allocs/op'
                for name in sorted(capture.benchmark_names('writes'))]
        return freeze, packets, rows

    def log(self, packets, rows):
        return '\n'.join(['    algorithm_work_bench_test.go:172: ' + json.dumps(p, sort_keys=True)
                          for p in packets] + rows + ['PASS']) + '\n'

    def test_complete_and_rejected_packets(self):
        freeze, packets, rows = self.fixture()
        self.assertEqual(len(capture.validate_log(self.log(packets, rows), 'writes', freeze, False, False)['rows']), 8)
        for field, bad in (('commits', 39), ('updates', 39999), ('read_samples', 1),
                           ('final_close_checked', False), ('checkpoints', 2)):
            changed = copy.deepcopy(packets)
            changed[0][field] = bad
            with self.subTest(field=field), self.assertRaises(ValueError):
                capture.validate_log(self.log(changed, rows), 'writes', freeze, False, False)
        for field in ('runtime_head', 'binary_sha256', 'harness_sha256'):
            changed = copy.deepcopy(packets)
            changed[0]['provenance'][field] = 'b' * len(freeze[field])
            with self.subTest(field=field), self.assertRaises(ValueError):
                capture.validate_log(self.log(changed, rows), 'writes', freeze, False, False)
        changed = copy.deepcopy(packets)
        del changed[0]['after'][capture.WORK_COUNTERS[0]]
        with self.assertRaises(ValueError):
            capture.validate_log(self.log(changed, rows), 'writes', freeze, False, False)
        for broken in (self.log(packets, rows).replace('\nPASS\n', '\n'),
                       self.log(packets[:-1], rows), self.log(packets, rows[:-1]),
                       self.log(packets, rows) + rows[0] + '\n',
                       self.log(packets, rows).replace('100 ns/op', '100 ns/op engine stderr')):
            with self.assertRaises(ValueError):
                capture.validate_log(broken, 'writes', freeze, False, False)

    def test_many_cell_and_operation_counts(self):
        rows = [name + '-2 1000 100 ns/op 64 keys/op 10 B/op 1 allocs/op'
                for name in sorted(capture.benchmark_names('many'))]
        capture.validate_log('\n'.join(rows + ['PASS']), 'many', {}, True, False)
        with self.assertRaises(ValueError):
            capture.validate_log('\n'.join(rows[:-1] + ['PASS']), 'many', {}, True, False)

    def test_internal_counts_independent_of_timing(self):
        rows = [f'    diagnostic: pointer={p} shape={s} view={v} batches=128 keys_per_batch=64 internal_visits=100 potential_union_visits=20 repeated_visits=80'
                for p, s, v in itertools.product(('false', 'true'), ('sorted', 'clustered', 'uniform'), ('false', 'true'))]
        capture.validate_log('\n'.join(rows + ['PASS']), 'internal', {}, True, False)
        for broken in (rows[:-1], rows + [rows[0]], [row.replace('repeated_visits=80', 'repeated_visits=79') for row in rows]):
            with self.assertRaises(ValueError):
                capture.validate_log('\n'.join(broken + ['PASS']), 'internal', {}, True, False)

    def test_raw_capture_hashes_binary_and_incomplete_status(self):
        freeze, packets, rows = self.fixture()
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            freeze['environment'] = capture.normalized_environment({'TMPDIR': '/frozen/database-filesystem', 'PATH': '/frozen/toolchain/bin', 'HOME': '/frozen/home', 'CC': 'clang'})
            (output / 'freeze.json').write_text(json.dumps(freeze))
            (output / 'stdout.log').write_text(self.log(packets, rows))
            (output / 'stderr.log').write_text('independent engine diagnostics\n')
            freeze.update(inputs={'synthetic.go': 'a'*64}, go_environment={'GOARCH': 'arm64'},
                          modules={'synthetic': {}}, capture_sha256='a'*64, overlay_generator_sha256='a'*64,
                          complete=True)
            (output / 'freeze.json').write_text(json.dumps(freeze))
            expected = capture.digest(output / 'freeze.json')
            source = capture.frozen_identity(freeze)
            record = dict(complete=True, returncode=0, source_before=source, source_after=source,
                          prepared=str(output), freeze_sha256=capture.digest(output / 'freeze.json'),
                          binary_sha256=freeze['binary_sha256'], family='writes', pilot=False, small_flush=False,
                          grant='synthetic-only', environment=dict(freeze['environment'],
                              TREEDB_ALGORITHM_RUNTIME_HEAD=freeze['runtime_head'],
                              TREEDB_ALGORITHM_FREEZE_FILE=str(output / 'freeze.json'),
                              TREEDB_ALGORITHM_PILOT='0', TREEDB_ALGORITHM_SMALL_FLUSH='0'),
                          command=capture.execution_command('writes', output / 'algorithm-work.test'),
                          stdout_sha256=capture.digest(output / 'stdout.log'), stderr_sha256=capture.digest(output / 'stderr.log'))
            (output / 'execution.json').write_text(json.dumps(record))
            capture.validate_capture(output, expected)
            for missing_or_wrong in (None, '', 'b'*64):
                with self.subTest(expected=missing_or_wrong), self.assertRaises(ValueError):
                    capture.validate_capture(output, missing_or_wrong)
            for field, bad in (('complete', False), ('returncode', 1), ('binary_sha256', 'b'*64),
                               ('stderr_sha256', 'b'*64), ('stdout_sha256', 'b'*64), ('command', ['wrong']), ('environment', {})):
                changed = dict(record, **{field: bad})
                (output / 'execution.json').write_text(json.dumps(changed))
                with self.subTest(field=field), self.assertRaises(ValueError):
                    capture.validate_capture(output, expected)
            for source in ({}, {k: v for k, v in capture.frozen_identity(freeze).items() if k != 'inputs'}):
                changed = dict(record, source_before=source, source_after=source)
                (output / 'execution.json').write_text(json.dumps(changed))
                with self.assertRaises(ValueError):
                    capture.validate_capture(output, expected)
            for name in ('TMPDIR', 'PATH', 'HOME', 'CC', 'TREEDB_ALGORITHM_FREEZE_FILE'):
                for altered in (dict(record['environment'], **{name: '/changed'}),
                                {k: v for k, v in record['environment'].items() if k != name}):
                    (output / 'execution.json').write_text(json.dumps(dict(record, environment=altered)))
                    with self.subTest(environment=name), self.assertRaises(ValueError):
                        capture.validate_capture(output, expected)
            changed = dict(record, environment=dict(record['environment'], GOGC='off'))
            (output / 'execution.json').write_text(json.dumps(changed))
            with self.assertRaises(ValueError):
                capture.validate_capture(output, expected)
            for field in ('pilot', 'small_flush'):
                for invalid in (None, 'false', 0):
                    (output / 'execution.json').write_text(json.dumps(dict(record, **{field: invalid})))
                    with self.subTest(mode=field, invalid=invalid), self.assertRaises(ValueError):
                        capture.validate_capture(output, expected)
            (output / 'execution.json').write_text(json.dumps(record))
            incomplete = dict(freeze, complete=False)
            (output / 'freeze.json').write_text(json.dumps(incomplete))
            incomplete_sha = capture.digest(output / 'freeze.json')
            (output / 'execution.json').write_text(json.dumps(dict(record, freeze_sha256=incomplete_sha)))
            with self.assertRaisesRegex(ValueError, 'incomplete preparation freeze'):
                capture.validate_capture(output, incomplete_sha)
            replacement = dict(freeze, binary_sha256='b'*64)
            (output / 'freeze.json').write_text(json.dumps(replacement))
            (output / 'execution.json').write_text(json.dumps(dict(record,
                freeze_sha256=capture.digest(output / 'freeze.json'), binary_sha256='b'*64)))
            with self.assertRaisesRegex(ValueError, 'external preparation freeze'):
                capture.validate_capture(output, expected)
            (output / 'freeze.json').write_text(json.dumps(freeze))
            (output / 'execution.json').write_text(json.dumps(record))
            argv = ['capture.py', 'validate', '--source', '/unused/offline-source',
                    '--output', str(output), '--pilot']
            with mock.patch('sys.argv', argv), self.assertRaisesRegex(ValueError, 'external preparation freeze'):
                capture.main()
            with mock.patch('sys.argv', argv + ['--freeze-sha256', expected]):
                capture.main()

    def test_changed_base_environment_rejected_before_process(self):
        freeze, _, _ = self.fixture()
        freeze.update(inputs={'synthetic.go': 'a'*64}, go_environment={'GOARCH': 'arm64'},
                      modules={'synthetic': {}}, capture_sha256='a'*64,
                      overlay_generator_sha256='a'*64, complete=True)
        base = capture.normalized_environment({'GOROOT': '/frozen/compiler',
                    'TMPDIR': '/frozen/database-filesystem', 'GOTMPDIR': '/frozen/build-temp',
                    'PATH': '/frozen/bin', 'HOME': '/frozen/home', 'CC': 'clang'})
        freeze['environment'] = base
        with tempfile.TemporaryDirectory() as temporary:
            prepared = Path(temporary) / 'prepared'
            prepared.mkdir()
            (prepared / 'algorithm-work.test').write_bytes(b'synthetic-not-executable')
            freeze['binary_sha256'] = capture.digest(prepared / 'algorithm-work.test')
            (prepared / 'freeze.json').write_text(json.dumps(freeze))
            for name in ('TMPDIR', 'GOTMPDIR', 'PATH', 'HOME', 'CC'):
                altered = dict(base, **{name: '/changed'})
                argv = ['capture.py', 'capture', '--source', temporary,
                        '--output', str(Path(temporary) / name), '--prepared', str(prepared),
                        '--grant', 'synthetic-only', '--freeze-sha256', capture.digest(prepared / 'freeze.json')]
                with self.subTest(environment=name), mock.patch('sys.argv', argv), \
                     mock.patch.dict(capture.os.environ, altered, clear=True), \
                     mock.patch.object(capture, 'identity', return_value=capture.frozen_identity(freeze)), \
                     mock.patch.object(capture.subprocess, 'run') as process:
                    with self.assertRaisesRegex(ValueError, 'changed since preparation'):
                        capture.main()
                    process.assert_not_called()

    def test_external_preparation_freeze_required_before_process(self):
        freeze, _, _ = self.fixture()
        freeze.update(inputs={'synthetic.go': 'a'*64}, go_environment={'GOARCH': 'arm64'},
                      modules={'synthetic': {}}, capture_sha256='a'*64,
                      overlay_generator_sha256='a'*64, complete=True,
                      environment=capture.normalized_environment({'GOROOT': '/frozen/compiler'}))
        with tempfile.TemporaryDirectory() as temporary:
            prepared = Path(temporary) / 'prepared'
            prepared.mkdir()
            binary = prepared / 'algorithm-work.test'
            binary.write_bytes(b'original-synthetic-binary')
            freeze['binary_sha256'] = capture.digest(binary)
            (prepared / 'freeze.json').write_text(json.dumps(freeze))
            expected = capture.digest(prepared / 'freeze.json')
            for supplied in (None, 'b'*64, expected):
                if supplied == expected:
                    binary.write_bytes(b'replaced-synthetic-binary')
                    freeze['binary_sha256'] = capture.digest(binary)
                    (prepared / 'freeze.json').write_text(json.dumps(freeze))
                argv = ['capture.py', 'capture', '--source', temporary,
                        '--output', str(Path(temporary) / ('missing' if supplied is None else supplied)),
                        '--prepared', str(prepared), '--grant', 'synthetic-only']
                if supplied is not None:
                    argv += ['--freeze-sha256', supplied]
                with self.subTest(expected=supplied), mock.patch('sys.argv', argv), \
                     mock.patch.dict(capture.os.environ, freeze['environment'], clear=True), \
                     mock.patch.object(capture, 'identity', return_value=capture.frozen_identity(freeze)), \
                     mock.patch.object(capture.subprocess, 'run', return_value=mock.Mock(returncode=1)) as process:
                    with self.assertRaisesRegex(ValueError, 'external preparation freeze'):
                        capture.main()
                    process.assert_not_called()
            capture.load_freeze(prepared, None, True)
            with self.assertRaises(ValueError):
                capture.load_freeze(prepared, expected, True)
            current = capture.digest(prepared / 'freeze.json')
            capture.load_freeze(prepared, current, True)
            rows = [name + '-2 1000 100 ns/op 64 keys/op 10 B/op 1 allocs/op'
                    for name in sorted(capture.benchmark_names('many'))]
            for changed_during_process in (False, True):
                output = Path(temporary) / ('changed-during-process' if changed_during_process else 'matching')
                argv = ['capture.py', 'capture', '--source', temporary,
                        '--output', str(output), '--prepared', str(prepared), '--family', 'many',
                        '--grant', 'synthetic-only', '--freeze-sha256', current]
                def synthetic_process(*args, **kwargs):
                    kwargs['stdout'].write('\n'.join(rows + ['PASS']) + '\n')
                    if changed_during_process:
                        (prepared / 'freeze.json').write_text(json.dumps(dict(freeze, complete=False)))
                    return mock.Mock(returncode=0)
                with mock.patch('sys.argv', argv), \
                     mock.patch.dict(capture.os.environ, freeze['environment'], clear=True), \
                     mock.patch.object(capture, 'identity', return_value=capture.frozen_identity(freeze)), \
                     mock.patch.object(capture.subprocess, 'run', side_effect=synthetic_process):
                    if changed_during_process:
                        with self.assertRaisesRegex(ValueError, 'external preparation freeze'):
                            capture.main()
                        self.assertIs(json.loads((output / 'execution.json').read_text())['complete'], False)
                    else:
                        capture.main()
                        capture.validate_capture(output, current)

    def test_runtime_environment_is_normalized_and_architectures_recorded(self):
        normal = capture.normalized_environment({'GOGC': 'off', 'GODEBUG': 'cpu.all=off',
                                                'PRIVATE_TOKEN': 'secret', 'GOARM64': 'v8.1'})
        self.assertEqual(normal['GOGC'], '100')
        self.assertEqual(normal['GODEBUG'], '')
        self.assertNotIn('PRIVATE_TOKEN', normal)
        self.assertEqual(normal['GOARM64'], 'v8.1')
        self.assertIn('GOARM64', capture.ENV_KEYS)
        self.assertIn('GOAMD64', capture.ENV_KEYS)


if __name__ == '__main__':
    unittest.main()
