"""Corrupt a real successful rehearsal, preserving its original raw evidence."""
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest

from r1_lifecycle_validate import decode, manifest_hash, validate


@unittest.skipUnless(os.environ.get('R1_LIFECYCLE_TEST_PACKET'), 'set R1_LIFECYCLE_TEST_PACKET to a real rehearsal packet')
class RealPacketTests(unittest.TestCase):
    def setUp(self):
        source = Path(os.environ['R1_LIFECYCLE_TEST_PACKET'])
        self.original = decode(source.read_text())
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'packet.json'
        os.symlink((source.parent / 'collections.test').resolve(), self.path.parent / 'collections.test')
        for name in ['build.log', *[row['log'] for row in self.original['runs']]]:
            shutil.copyfile(source.parent / name, self.path.parent / name)
        self.packet = copy.deepcopy(self.original)

    def save(self):
        self.path.write_text(json.dumps(self.packet))

    def reject(self):
        self.save()
        with self.assertRaises((ValueError, KeyError, TypeError)):
            validate(self.path)

    def edit_raw(self, transform):
        record = self.packet['runs'][0]
        raw = self.path.parent / record['log']
        raw.write_text(transform(raw.read_text()))
        record['log_sha256'] = hashlib.sha256(raw.read_bytes()).hexdigest()

    def test_real_packet_acceptance_and_exact_freeze(self):
        self.save()
        rows = validate(self.path, self.original['source_before']['runtime_sha256'],
                        self.original['source_before']['harness_sha256'], self.original['source_before']['commit'])
        self.assertEqual(len(rows), self.packet['config']['repetitions'])
        self.assertEqual(rows[0]['result']['total_calls'], self.packet['config']['epochs'] * self.packet['config']['calls_per_epoch'])

    def test_frozen_harness_mismatch(self):
        self.save()
        with self.assertRaisesRegex(ValueError, 'frozen harness_sha256 mismatch'):
            validate(self.path, expected_harness='0' * 64)

    def test_unbound_compiled_test(self):
        for key in ('source_before', 'source_after'):
            source = self.packet[key]
            del source['harness_files'][source['compiled_test_files'][0]]
            source['harness_sha256'] = manifest_hash(source['harness_files'])
        self.reject()

    def test_runtime_manifest_tampering(self):
        self.packet['source_before']['runtime_sha256'] = '0' * 64
        self.reject()

    def test_binary_identity_mismatch(self):
        self.packet['toolchain']['binary_sha256'] = '0' * 64
        self.reject()

    def test_benchmark_tmpdir_metadata_mismatch(self):
        self.packet['toolchain']['process_environment']['TMPDIR'] = '/tmp'
        self.reject()

    def test_benchmark_filesystem_metadata_mismatch(self):
        self.packet['toolchain']['benchmark_filesystem_device'] += 1
        self.reject()

    def test_missing_effective_benchmark_environment(self):
        del self.packet['toolchain']['process_environment']
        self.reject()

    def test_impossible_epoch_timer_with_rebound_raw_hash(self):
        self.edit_raw(lambda text: __import__('re').sub(r'\s+[\d.]+\s+ns/op', ' 1 ns/op', text))
        self.reject()

    def test_failed_process_even_with_valid_output(self):
        self.packet['runs'][0]['exit_code'] = 1
        self.reject()

    def test_removed_metric_with_rebound_raw_hash(self):
        self.edit_raw(lambda text: __import__('re').sub(r'\s+[\d.]+\s+calls/op', '', text))
        self.reject()

    def test_wrong_actual_denominator_with_rebound_raw_hash(self):
        def change(text):
            lines = text.splitlines()
            for index, line in enumerate(lines):
                if 'R1_LIFECYCLE_RESULT ' in line:
                    prefix, encoded = line.split('R1_LIFECYCLE_RESULT ', 1)
                    result = decode(encoded)
                    result['total_calls'] += 1
                    lines[index] = prefix + 'R1_LIFECYCLE_RESULT ' + json.dumps(result)
            return '\n'.join(lines) + '\n'
        self.edit_raw(change)
        self.reject()

    def test_missing_storage_phase_with_rebound_raw_hash(self):
        def change(text):
            lines = text.splitlines()
            for index, line in enumerate(lines):
                if 'R1_LIFECYCLE_RESULT ' in line:
                    prefix, encoded = line.split('R1_LIFECYCLE_RESULT ', 1)
                    result = decode(encoded)
                    result['census'].pop()
                    lines[index] = prefix + 'R1_LIFECYCLE_RESULT ' + json.dumps(result)
            return '\n'.join(lines) + '\n'
        self.edit_raw(change)
        self.reject()

    def test_rehearsal_cannot_be_relabeled_retained(self):
        self.packet['config']['qualification'] = 'retained'
        self.reject()


if __name__ == '__main__':
    unittest.main()
