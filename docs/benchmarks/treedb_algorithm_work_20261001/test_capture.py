"""Synthetic fail-closed controls; no fixture collection or runner activity."""
import copy
import itertools
import json
from pathlib import Path
import tempfile
import unittest

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
            freeze['environment'] = dict(capture.RUNTIME_ENV)
            (output / 'freeze.json').write_text(json.dumps(freeze))
            (output / 'stdout.log').write_text(self.log(packets, rows))
            (output / 'stderr.log').write_text('independent engine diagnostics\n')
            freeze.update(inputs={'synthetic.go': 'a'*64}, go_environment={'GOARCH': 'arm64'},
                          modules={'synthetic': {}}, capture_sha256='a'*64, overlay_generator_sha256='a'*64)
            (output / 'freeze.json').write_text(json.dumps(freeze))
            source = capture.frozen_identity(freeze)
            record = dict(complete=True, returncode=0, source_before=source, source_after=source,
                          prepared=str(output), freeze_sha256=capture.digest(output / 'freeze.json'),
                          binary_sha256=freeze['binary_sha256'], family='writes', pilot=False, small_flush=False,
                          grant='synthetic-only', environment=freeze['environment'],
                          command=capture.execution_command('writes', output / 'algorithm-work.test'),
                          stdout_sha256=capture.digest(output / 'stdout.log'), stderr_sha256=capture.digest(output / 'stderr.log'))
            (output / 'execution.json').write_text(json.dumps(record))
            capture.validate_capture(output, freeze)
            for field, bad in (('complete', False), ('returncode', 1), ('binary_sha256', 'b'*64),
                               ('stderr_sha256', 'b'*64), ('stdout_sha256', 'b'*64), ('command', ['wrong']), ('environment', {})):
                changed = dict(record, **{field: bad})
                (output / 'execution.json').write_text(json.dumps(changed))
                with self.subTest(field=field), self.assertRaises(ValueError):
                    capture.validate_capture(output, freeze)
            for source in ({}, {k: v for k, v in capture.frozen_identity(freeze).items() if k != 'inputs'}):
                changed = dict(record, source_before=source, source_after=source)
                (output / 'execution.json').write_text(json.dumps(changed))
                with self.assertRaises(ValueError):
                    capture.validate_capture(output, freeze)
            changed = dict(record, environment=dict(freeze['environment'], GOGC='off'))
            (output / 'execution.json').write_text(json.dumps(changed))
            with self.assertRaises(ValueError):
                capture.validate_capture(output, freeze)

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
