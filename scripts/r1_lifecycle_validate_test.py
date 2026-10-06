"""Corrupt a real successful rehearsal, preserving its original raw evidence."""
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest

from r1_lifecycle_validate import decode, manifest_hash, validate, working_set


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
        raw = (source.parent / self.original['runs'][0]['log']).read_text()
        result_line = next(line for line in raw.splitlines() if 'R1_LIFECYCLE_RESULT ' in line)
        self.actual_vacuum = decode(result_line.split('R1_LIFECYCLE_RESULT ', 1)[1])['maintenance'][0]['vacuum']
        self.original_source = source

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

    def edit_results(self, transform):
        def change(text):
            lines = text.splitlines()
            for index, line in enumerate(lines):
                if 'R1_LIFECYCLE_RESULT ' in line:
                    prefix, encoded = line.split('R1_LIFECYCLE_RESULT ', 1)
                    result = decode(encoded)
                    transform(result)
                    lines[index] = prefix + 'R1_LIFECYCLE_RESULT ' + json.dumps(result)
            return '\n'.join(lines) + '\n'
        self.edit_raw(change)

    def test_real_packet_acceptance_and_exact_freeze(self):
        self.save()
        rows = validate(self.path, self.original['source_before']['runtime_sha256'],
                        self.original['source_before']['harness_sha256'], self.original['source_before']['commit'])
        self.assertEqual(len(rows), self.packet['config']['repetitions'])
        self.assertEqual(rows[0]['result']['total_calls'], self.packet['config']['epochs'] * self.packet['config']['calls_per_epoch'])
        expected = self.packet['config']['working_set']['distinct_ids_per_epoch']
        self.assertEqual(rows[0]['result']['distinct_ids'], expected)
        if self.packet['config']['epochs'] > 1:
            self.assertEqual(rows[0]['result']['id_coverage'][1]['revisited_ids'], expected)
            self.assertEqual(rows[0]['result']['id_coverage'][1]['new_ids'], 0)

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

    def reject_matching_packet_receipt(self, message):
        # Deliberately bind this malformed fixture's bytes to reach semantics,
        # rather than stopping at the already tested external receipt guard.
        self.save()
        with self.assertRaisesRegex(ValueError, message):
            validate(self.path,
                     expected_binary_sha256=self.original['toolchain']['binary_sha256'],
                     expected_packet_sha256=hashlib.sha256(self.path.read_bytes()).hexdigest())

    def test_invocation_names_original_capture_executable(self):
        for executable in ('', 'collections.test', '/other/collections.test',
                           self.original['invocation'][0] + '.other'):
            with self.subTest(executable=executable):
                self.packet['invocation'][0] = executable
                self.reject_matching_packet_receipt('mislabeled benchmark command')

    def test_build_command_targets_same_capture_executable_and_package(self):
        original = self.original['build_command']
        changes = [(0, ''), (1, 'build'), (2, '-race'), (3, '--output'),
                   (4, '/other/collections.test'), (5, './TreeDB/db')]
        for index, value in changes:
            with self.subTest(index=index, value=value):
                self.packet['build_command'] = original.copy()
                self.packet['build_command'][index] = value
                self.reject_matching_packet_receipt('mislabeled build command')

    def test_command_argv_shapes_fail_closed(self):
        for key in ('invocation', 'build_command'):
            for value in (None, {}, '', [], [1]):
                with self.subTest(key=key, value=value):
                    self.packet[key] = value
                    self.reject_matching_packet_receipt('mislabeled .* command')
            self.packet[key] = copy.deepcopy(self.original[key])

    def test_recorded_capture_directory_is_canonical_absolute(self):
        for directory in ('relative', '/capture/../other', '/capture/./child',
                          '/capture//child', '/capture/child/', None):
            with self.subTest(directory=directory):
                self.packet['toolchain']['capture_directory'] = directory
                self.reject_matching_packet_receipt('invalid recorded capture directory')

    def test_build_info_names_original_executable(self):
        _, separator, rest = self.original['toolchain']['binary_buildinfo'].partition('\n')
        self.packet['toolchain']['binary_buildinfo'] = '/other/collections.test: go1.26.4' + separator + rest
        self.reject_matching_packet_receipt('build info executable mismatch')

    def test_actual_packet_replays_at_relocated_path(self):
        self.assertNotEqual(str(self.path.parent), self.original['toolchain']['capture_directory'])
        receipt = self.trusted_rehearsal_receipt()
        self.assertEqual(len(validate(self.path, **receipt)), len(self.original['runs']))

    def test_recipe_is_the_recorded_deterministic_fixture(self):
        self.packet['config']['recipe'] = 'different generator'
        self.reject_matching_packet_receipt('mislabeled deterministic fixture recipe')

    def test_repetition_numbers_are_strict_integers(self):
        for value in (True, 1.0):
            with self.subTest(value=value):
                self.packet['runs'][0]['repetition'] = value
                self.reject_matching_packet_receipt('duplicate/wrong raw run')

    def test_affinity_observation_has_valid_cpu_ids(self):
        for value in ([], [0, 0], [1, 0], [-1], [True], [1.0], '0'):
            with self.subTest(value=value):
                self.packet['runs'][0]['before']['affinity'] = value
                self.reject_matching_packet_receipt('invalid CPU affinity observation')

    def test_unavailable_affinity_remains_replayable(self):
        for record in self.packet['runs']:
            record['before']['affinity'] = record['after']['affinity'] = None
        receipt = self.trusted_rehearsal_receipt()
        self.assertEqual(len(validate(self.path, **receipt)), len(self.original['runs']))

    def test_utc_observation_is_valid_calendar_time(self):
        for value in ('not a time', '2026-02-30T01:00:00Z', '2026-13-01T01:00:00Z',
                      '2026-10-06T24:00:00Z', '2026-10-06T01:00:00+00:00'):
            with self.subTest(value=value):
                self.packet['runs'][0]['before']['utc'] = value
                self.reject_matching_packet_receipt('invalid UTC observation')

    def test_serial_process_intervals_cannot_overlap(self):
        self.assertGreaterEqual(len(self.packet['runs']), 2)
        self.packet['runs'][1]['before']['monotonic_ns'] = self.packet['runs'][0]['after']['monotonic_ns'] - 1
        self.reject_matching_packet_receipt('overlapping serial process intervals')

    def test_process_interval_encloses_actual_final_timers(self):
        record = self.packet['runs'][0]
        record['after']['monotonic_ns'] = record['before']['monotonic_ns'] + 1
        self.reject_matching_packet_receipt('process interval shorter than final timers')

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

    def test_new_ids_in_later_epoch_with_rebound_raw_hash(self):
        def change(result):
            if result['epochs'] > 1:
                result['id_coverage'][1]['new_ids'] = result['id_coverage'][1]['distinct_ids']
                result['id_coverage'][1]['revisited_ids'] = 0
        self.edit_results(change)
        self.reject()

    def test_changed_id_set_with_same_counts_and_rebound_raw_hash(self):
        def change(result):
            if result['epochs'] > 1:
                result['id_coverage'][1]['ids_sha256'] = '0' * 64
        self.edit_results(change)
        self.reject()

    def test_mislabeled_working_set_config(self):
        self.packet['config']['working_set']['distinct_ids_per_epoch'] *= 2
        self.reject()

    def test_missing_typed_reachability_source_with_rebound_raw_hash(self):
        self.edit_results(lambda result: result['maintenance'][0]['reclaim']['typed_gc']['Plan']['Sources'].pop('RecoveryManifestBytes'))
        self.reject()

    def test_missing_vlog_active_attribution_with_rebound_raw_hash(self):
        self.edit_results(lambda result: result['maintenance'][0]['vlog_gc'].pop('BytesActive'))
        self.reject()

    def test_missing_post_release_gc_with_rebound_raw_hash(self):
        self.edit_results(lambda result: result.pop('after_view_release_gc'))
        self.reject()

    def test_missing_fold_work_with_rebound_raw_hash(self):
        self.edit_results(lambda result: result['maintenance'][0]['fold']['stats'].update(RowsCompacted=1))
        self.reject()

    def test_unfolded_history_with_rebound_raw_hash(self):
        self.edit_results(lambda result: result['maintenance'][0]['fold']['stats'].update(MutationPartsAfter=1))
        self.reject()

    def test_missing_vacuum_completion_with_rebound_raw_hash(self):
        self.edit_results(lambda result: result['maintenance'][0]['vacuum'].update(WorkCompleted=False))
        self.reject()

    def reset_raw_packet(self):
        self.packet = copy.deepcopy(self.original)
        for row in self.original['runs']:
            shutil.copyfile(self.original_source.parent / row['log'], self.path.parent / row['log'])

    def test_every_actual_vacuum_field_required_with_rebound_raw_hash(self):
        # Enumerate the actual emitted Go record, independently of validator constants.
        for field in self.actual_vacuum:
            with self.subTest(field=field):
                self.reset_raw_packet()
                self.edit_results(lambda result: result['maintenance'][0]['vacuum'].pop(field))
                self.reject()

    def test_every_actual_vacuum_scalar_type_with_rebound_raw_hash(self):
        for field, value in self.actual_vacuum.items():
            with self.subTest(field=field):
                self.reset_raw_packet()
                wrong = 0 if type(value) is bool else False
                self.edit_results(lambda result: result['maintenance'][0]['vacuum'].update({field: wrong}))
                self.reject()

    def test_actual_vacuum_counters_nonnegative_integer_with_rebound_raw_hash(self):
        for field, value in self.actual_vacuum.items():
            if type(value) is not int:
                continue
            for wrong in (-1, 0.5):
                with self.subTest(field=field, value=wrong):
                    self.reset_raw_packet()
                    self.edit_results(lambda result: result['maintenance'][0]['vacuum'].update({field: wrong}))
                    self.reject()

    def test_nonfinite_vacuum_counter_with_rebound_raw_hash(self):
        self.edit_results(lambda result: result['maintenance'][0]['vacuum'].update(TotalDuration=float('nan')))
        self.reject()

    def test_unclassified_vacuum_field_with_rebound_raw_hash(self):
        self.edit_results(lambda result: result['maintenance'][0]['vacuum'].update(UnclassifiedCounter=0))
        self.reject()

    def test_protected_rewrite_counted_as_work_with_rebound_raw_hash(self):
        self.edit_results(lambda result: result['maintenance'][0]['reclaim'].update(decision='eligible'))
        self.reject()

    def test_missing_maintenance_timer_with_rebound_raw_hash(self):
        self.edit_results(lambda result: result['maintenance'][0].update(fold_ns=0))
        self.reject()

    def test_mislabeled_cached_wrapper_scope(self):
        self.packet['config']['execution_scope']['cached_wrapper'] = True
        self.reject()

    def test_unbound_actual_concurrency_with_rebound_raw_hash(self):
        self.edit_results(lambda result: result.update(gomaxprocs=1))
        self.reject()

    def test_wrong_actual_runtime_environment(self):
        self.packet['toolchain']['environment']['GOGC'] = 'off'
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

    def retained_declaration(self):
        # Rebind only the declaration on actual recorded evidence. The negative
        # must fail at provenance, before later raw dimension/run checks.
        config = self.packet['config']
        config.update(qualification='retained', repetitions=5, epochs=5,
                      documents=4096, calls_per_epoch=1024,
                      landed_tooling_commit=self.original['source_before']['commit'],
                      review_url='https://github.com/snissn/gomap/pull/5065')
        config['working_set'] = working_set(config)
        for key in ('source_before', 'source_after'):
            self.packet[key]['clean'] = True
        self.save()

    def frozen_bindings(self):
        source = self.original['source_before']
        return {'expected_runtime': source['runtime_sha256'],
                'expected_harness': source['harness_sha256'],
                'expected_commit': source['commit'],
                'expected_landed_tooling_commit': source['commit'],
                'expected_binary_sha256': self.original['toolchain']['binary_sha256'],
                # The intentionally rebound retained declaration is only a
                # negative fixture: matching its bytes reaches provenance guards.
                'expected_packet_sha256': hashlib.sha256(self.path.read_bytes()).hexdigest()}

    def test_relabel_valid_landing_shape_requires_independent_binding(self):
        self.retained_declaration()
        with self.assertRaisesRegex(ValueError, 'missing independently verified landing binding'):
            validate(self.path)

    def test_forged_valid_hex_landing_rejected_against_independent_binding(self):
        self.retained_declaration()
        self.packet['config']['landed_tooling_commit'] = 'f' * 40
        self.save()
        with self.assertRaisesRegex(ValueError, 'verified landing commit mismatch'):
            validate(self.path, **self.frozen_bindings())

    def test_missing_landing_declaration(self):
        self.retained_declaration()
        self.packet['config']['landed_tooling_commit'] = None
        self.save()
        with self.assertRaisesRegex(ValueError, 'missing review/landing declaration'):
            validate(self.path, **self.frozen_bindings())

    def test_retained_requires_all_independent_source_bindings(self):
        self.retained_declaration()
        for key in ('expected_runtime', 'expected_harness', 'expected_commit'):
            with self.subTest(key=key):
                bindings = self.frozen_bindings()
                bindings.pop(key)
                with self.assertRaisesRegex(ValueError, 'retained validation requires independent source bindings'):
                    validate(self.path, **bindings)

    def trusted_rehearsal_receipt(self):
        # Freeze an unchanged copy of actual successful bytes before corruption.
        self.save()
        return {'expected_binary_sha256': self.original['toolchain']['binary_sha256'],
                'expected_packet_sha256': hashlib.sha256(self.path.read_bytes()).hexdigest()}

    def substitute_binary(self):
        # Never alter the original binary behind the temporary symlink.
        binary = self.path.parent / 'collections.test'
        binary.unlink()
        binary.write_bytes(b'sibling executable substitution, never executed\n')
        self.packet['toolchain']['binary_sha256'] = hashlib.sha256(binary.read_bytes()).hexdigest()
        self.save()

    def test_actual_packet_accepts_independent_binary_and_packet_receipt(self):
        receipt = self.trusted_rehearsal_receipt()
        self.assertEqual(len(validate(self.path, **receipt)), self.original['config']['repetitions'])

    def test_binary_substitution_with_rebound_self_hash_rejects_original_receipt(self):
        receipt = self.trusted_rehearsal_receipt()
        self.substitute_binary()
        # Internal consistency alone allows this sibling substitution.
        validate(self.path)
        with self.assertRaisesRegex(ValueError, 'frozen packet hash mismatch'):
            validate(self.path, **receipt)

    def test_binary_binding_rejects_substitution_even_with_matching_packet_fixture(self):
        receipt = self.trusted_rehearsal_receipt()
        self.substitute_binary()
        # Deliberately match only the corrupted packet fixture to reach the
        # independent executable guard; NEVER change the expected binary hash.
        receipt['expected_packet_sha256'] = hashlib.sha256(self.path.read_bytes()).hexdigest()
        with self.assertRaisesRegex(ValueError, 'frozen binary hash mismatch'):
            validate(self.path, **receipt)

    def test_raw_substitution_with_rebound_self_hash_rejects_original_receipt(self):
        receipt = self.trusted_rehearsal_receipt()
        self.edit_raw(lambda text: text + '\n')
        self.save()
        validate(self.path)
        with self.assertRaisesRegex(ValueError, 'frozen packet hash mismatch'):
            validate(self.path, **receipt)

    def test_metadata_substitution_rejects_original_receipt(self):
        receipt = self.trusted_rehearsal_receipt()
        self.packet['runs'][0]['before']['utc'] = '2000-01-01T00:00:00Z'
        self.save()
        validate(self.path)
        with self.assertRaisesRegex(ValueError, 'frozen packet hash mismatch'):
            validate(self.path, **receipt)

    def test_source_relabel_rejects_original_receipt(self):
        receipt = self.trusted_rehearsal_receipt()
        for key in ('source_before', 'source_after'):
            self.packet[key]['commit'] = 'f' * 40
        self.save()
        validate(self.path)
        with self.assertRaisesRegex(ValueError, 'frozen packet hash mismatch'):
            validate(self.path, **receipt)

    def test_build_log_substitution_with_rebound_self_hash_rejects_original_receipt(self):
        receipt = self.trusted_rehearsal_receipt()
        log = self.path.parent / 'build.log'
        log.write_bytes(log.read_bytes() + b'\n')
        self.packet['build_log_sha256'] = hashlib.sha256(log.read_bytes()).hexdigest()
        self.save()
        validate(self.path)
        with self.assertRaisesRegex(ValueError, 'frozen packet hash mismatch'):
            validate(self.path, **receipt)

    def test_exact_packet_bytes_bound_even_with_identical_decoded_metadata(self):
        receipt = self.trusted_rehearsal_receipt()
        self.path.write_bytes(self.path.read_bytes() + b'\n')
        validate(self.path)
        with self.assertRaisesRegex(ValueError, 'frozen packet hash mismatch'):
            validate(self.path, **receipt)

    def test_retained_requires_independent_binary_and_packet_bindings(self):
        self.retained_declaration()
        for key in ('expected_binary_sha256', 'expected_packet_sha256'):
            with self.subTest(key=key):
                bindings = self.frozen_bindings()
                bindings.pop(key)
                with self.assertRaisesRegex(ValueError, 'retained validation requires independent binary and packet bindings'):
                    validate(self.path, **bindings)

    def test_unknown_effective_child_environment_rejected(self):
        self.packet['toolchain']['process_environment']['GOMAP_UNREPORTED_SETTING'] = '1'
        self.reject()

    def test_effective_godebug_rejected(self):
        self.packet['toolchain']['process_environment']['GODEBUG'] = 'gctrace=1'
        self.reject()


if __name__ == '__main__':
    unittest.main()
