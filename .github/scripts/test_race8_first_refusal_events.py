"""Pure fixed-format synthetic controls; no task/Go/helper execution."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('fixed_race8_checker', HERE / 'race8_first_refusal_events.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def scalar(first=None, **changes):
    value = {'Calls': 2, 'Completed': 2,
             'First': (1 | (3 << 25) | (3 << 29) | (6 << 33)) if first is None else first,
             'Overflow': False, 'Missing': False, 'Entered': [2] * 8, 'Finished': [2] * 8}
    value.update(changes)
    boolean = lambda x: 'true' if x else 'false'
    return ('{Calls:%s Completed:%s First:%s Overflow:%s Missing:%s Entered:[%s] Finished:[%s]}' % (
        value['Calls'], value['Completed'], value['First'], boolean(value['Overflow']), boolean(value['Missing']),
        ' '.join(map(str, value['Entered'])), ' '.join(map(str, value['Finished']))))


def events(mode='witness', fail=False, value=None):
    expected = m.WITNESS if mode == 'witness' else m.CONTROLS
    rows = []
    for pkg, names in expected.items():
        rows.append({'Package': pkg, 'Action': 'start'})
        for top in sorted(x for x in names if '/' not in x):
            rows.append({'Package': pkg, 'Test': top, 'Action': 'run'})
            for child in sorted(x for x in names if x.startswith(top + '/')):
                rows.append({'Package': pkg, 'Test': child, 'Action': 'run'})
                rows.append({'Package': pkg, 'Test': child, 'Action': 'pass'})
            if mode == 'witness':
                rows.append({'Package': pkg, 'Test': top, 'Action': 'output',
                    'Output': '    native_ordinary_concurrent_correctness_test.go:72: ' + m.MARKER + (value or scalar()) + '\n'})
            if fail:
                rows.append({'Package': pkg, 'Test': top, 'Action': 'output',
                    'Output': '    native_ordinary_concurrent_correctness_test.go:221: original COW read refused\n'})
            rows.append({'Package': pkg, 'Test': top, 'Action': 'fail' if fail else 'pass'})
        rows.append({'Package': pkg, 'Action': 'fail' if fail else 'pass'})
    return rows


class FixedRace8Controls(unittest.TestCase):
    def check_rows(self, rows, mode='witness', go_exit=0):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'original.jsonl'
            path.write_text(''.join(json.dumps(x) + '\n' for x in rows))
            return m.validate(path, mode, go_exit)

    def test_exact_owner_inventory_and_both_modes(self):
        self.assertEqual(sum(len(x) for x in m.CONTROL_TOPS.values()), 10)
        self.assertEqual(sum(len(x) for x in m.CONTROLS.values()), 37)
        self.assertEqual(sum(not any(y.startswith(x + '/') for y in group) for group in m.CONTROLS.values() for x in group), 33)
        self.assertEqual(m.WITNESS, {m.PREFIX + 'collections': {m.TOP}})
        self.assertNotIn(m.TOP, m.CONTROLS_SELECTOR)
        for mode in ('controls', 'witness'):
            self.assertEqual(self.check_rows(events(mode), mode)['status'], 'PASS')

    def test_atomic_first_tuple_bit_boundaries(self):
        first = 0xffffff | (1 << 24) | (5 << 25) | (1 << 28) | (7 << 29) | (8 << 33) | (1 << 37) | (1 << 38) | (1 << 39) | (255 << 40)
        r = m.receipt(scalar(first))
        self.assertEqual(r['first_tuple'], {'ordinal': 0xffffff, 'frozen': True, 'descriptor_id': 5,
            'descriptor': 'root', 'after': True, 'stage_id': 7, 'stage': 'leaf_read', 'refusal_id': 8,
            'refusal': 'dictionary', 'shape_known': True, 'compressed': True,
            'dictionary_present': True, 'codec_id': 255,
            'internal_metadata': {'compression_known': False, 'compressed': None,
                'dictionary_presence_known': False, 'dictionary_present': None,
                'raw_codec_known': False, 'raw_frame_reserved': None, 'raw_codec_meaning': 'UNKNOWN'}})
        self.assertFalse(r['borrower_join_proved'])
        self.assertFalse(r['zero_first_is_success'])

    def test_unknown_shape_not_invented_and_no_join(self):
        r = m.receipt(scalar(Completed=99, Calls=1, Entered=[0, 99, 0, 3, 0, 1, 0, 0], Finished=[8] * 8))
        self.assertTrue(r['complete'])  # Separate atomic Loads are not a cross-counter cut.
        self.assertTrue(r['independent_scalar_samples'])
        self.assertIsNone(r['first_tuple']['compressed'])
        self.assertIsNone(r['first_tuple']['dictionary_present'])
        self.assertIsNone(r['first_tuple']['codec_id'])
        r = m.receipt(scalar(1 | (7 << 25) | (3 << 29) | (15 << 33)))
        self.assertEqual(r['attribution'], 'UNKNOWN')
        self.assertEqual(r['first_tuple']['descriptor'], 'UNKNOWN')
        self.assertEqual(r['first_tuple']['refusal'], 'UNKNOWN')

    def test_unknown_zero_missing_overflow_never_clears_failure(self):
        for value in (scalar(0), scalar(Calls=0), scalar(Missing=True), scalar(Overflow=True)):
            r = m.receipt(value)
            self.assertEqual(r['attribution'], 'UNKNOWN')
            result = self.check_rows(events(value=value))
            self.assertEqual(result['status'], 'PASS')  # Only literal test non-reproduction.
            self.assertEqual(next(iter(result['receipts'].values()))['attribution'], 'UNKNOWN')
            fail = self.check_rows(events(fail=True, value=value), go_exit=1)
            self.assertEqual(fail['status'], 'FAILED_DIAGNOSTIC_EVIDENCE')
            self.assertFalse(fail['accepted'])
            self.assertIn('original COW read refused', ''.join(x['Output'] for x in fail['diagnostic_output']))

    def test_fixed_scalar_negative_controls(self):
        bad = [scalar(1 << 61), scalar(0xffffff | (15 << 29) | (1 << 33)), scalar(1 << 33),
               scalar(1), scalar((1 | (1 << 33)) | (1 << 38)), scalar(Calls=-1), scalar(Calls=1 << 64),
               scalar(Completed=True), scalar(Entered=[0] * 7), scalar(Finished=[0] * 9), scalar(Entered=[-1] * 8),
               scalar().replace('Overflow:false', 'Overflow:0'), scalar().replace('Missing:false', 'Missing:null'),
               scalar().replace('Calls:2', 'Calls:02'), scalar().replace('Calls:2', 'Calls:2 Calls:2'),
               scalar()[:-1], scalar() + ' trailing', json.dumps({'Calls': 2})]
        for value in bad:
            with self.subTest(value=value):
                self.assertEqual(self.check_rows(events(value=value))['status'], 'INVALID_RECEIPT')

    def test_precision_known_false_differs_from_unknown(self):
        base = 1 | (3 << 29) | (6 << 33)
        unknown = m.receipt(scalar(base))['first_tuple']
        known_false = m.receipt(scalar(base | (7 << 48)))['first_tuple']
        self.assertFalse(unknown['shape_known'])
        self.assertIsNone(unknown['internal_metadata']['compressed'])
        self.assertIsNone(unknown['internal_metadata']['dictionary_present'])
        self.assertIsNone(unknown['internal_metadata']['raw_frame_reserved'])
        self.assertFalse(known_false['shape_known'])
        self.assertIsNone(known_false['compressed'])  # No validated returned shape.
        self.assertTrue(known_false['internal_metadata']['compression_known'])
        self.assertFalse(known_false['internal_metadata']['compressed'])
        self.assertFalse(known_false['internal_metadata']['dictionary_present'])
        self.assertEqual(known_false['internal_metadata']['raw_frame_reserved'], 0)
        subset = m.receipt(scalar(base | (1 << 48)))['first_tuple']['internal_metadata']
        self.assertFalse(subset['compressed'])
        self.assertIsNone(subset['dictionary_present'])
        self.assertIsNone(subset['raw_frame_reserved'])

    def test_original_composite_clauses_and_unknown_raw_enum(self):
        base = 1 | (1 << 24) | (3 << 29) | (6 << 33)
        for compressed, dictionary, codec in ((False, False, 1), (True, True, 1), (True, False, 255)):
            word = base | (7 << 48) | (int(compressed) << 51) | (int(dictionary) << 52) | (codec << 53)
            result = self.check_rows(events(fail=True, value=scalar(word)), go_exit=1)
            self.assertEqual(result['status'], 'FAILED_DIAGNOSTIC_EVIDENCE')
            item = next(iter(result['receipts'].values()))['first_tuple']
            self.assertFalse(item['shape_known'])
            self.assertIsNone(item['codec_id'])
            self.assertEqual(item['internal_metadata']['compressed'], compressed)
            self.assertEqual(item['internal_metadata']['dictionary_present'], dictionary)
            self.assertEqual(item['internal_metadata']['raw_frame_reserved'], codec)
            if codec == 255:
                self.assertEqual(item['internal_metadata']['raw_codec_meaning'], 'UNKNOWN_ENUM')
        # Payload no-frame: compression only, dictionary/raw remain unknown.
        word = 1 | (3 << 29) | (7 << 33) | (1 << 48)
        self.assertIsNone(m.receipt(scalar(word))['first_tuple']['internal_metadata']['raw_frame_reserved'])

    def test_invalid_precision_coupled_known_bits_and_conflation(self):
        base = 1 | (3 << 29) | (6 << 33)
        bad = [base | (1 << bit) for bit in (61, 62, 63, 51, 52, 53, 60)]
        bad += [base | (bits << 48) for bits in (2, 3, 4, 5, 6)]
        bad += [base | (1 << 37) | (bits << 48) for bits in (1, 7)]
        bad += [base | (1 << 48) | (1 << 52), base | (1 << 48) | (255 << 53)]
        bad += [base | (1 << 38), base | (1 << 39), base | (255 << 40)]
        for word in bad:
            with self.subTest(word=word):
                self.assertEqual(self.check_rows(events(value=scalar(word)))['status'], 'INVALID_RECEIPT')

    def test_failed_witness_and_partial_exit_transport(self):
        result = self.check_rows(events(fail=True), go_exit=1)
        self.assertEqual(result['status'], 'FAILED_DIAGNOSTIC_EVIDENCE')
        self.assertEqual(result['go_exit'], 1)
        self.assertFalse(result['accepted'])
        self.assertEqual(self.check_rows(events(fail=True)[:-1], go_exit=1)['status'], 'INCOMPLETE_DIAGNOSTIC_EVIDENCE')
        self.assertEqual(self.check_rows(events(), go_exit=124)['status'], 'TIMEOUT_OR_INTERRUPTED')
        self.assertEqual(self.check_rows(events(), go_exit=1)['status'], 'INCONSISTENT_EXIT_OR_EVENT_EVIDENCE')
        self.assertEqual(self.check_rows(events(fail=True), go_exit=0)['status'], 'INCONSISTENT_EXIT_OR_EVENT_EVIDENCE')
        self.assertEqual(self.check_rows(events('controls', fail=True), 'controls', 1)['status'], 'CONTROL_FAILURE')
        no_reason = [x for x in events(fail=True) if 'original COW read refused' not in x.get('Output', '')]
        self.assertEqual(self.check_rows(no_reason, go_exit=1)['status'], 'INCOMPLETE_DIAGNOSTIC_EVIDENCE')

    def test_receipt_exact_scope_and_cardinality(self):
        base = events()
        observed = next(x for x in base if m.MARKER in x.get('Output', ''))
        self.assertNotEqual(self.check_rows([x for x in base if x is not observed])['status'], 'PASS')
        index = base.index(observed)
        self.assertEqual(self.check_rows(base[:index] + [observed] + base[index:])['status'], 'INVALID_EVENT_EVIDENCE')
        for mode in ('controls', 'witness'):
            rows = events(mode)
            pkg = next(iter(m.CONTROLS if mode == 'controls' else m.WITNESS))
            rows.insert(1, {'Package': pkg, 'Action': 'output', 'Output': m.MARKER + scalar()})
            self.assertEqual(self.check_rows(rows, mode)['status'], 'INVALID_EVENT_EVIDENCE')
        rows = events()
        next(x for x in rows if m.MARKER in x.get('Output', ''))['Output'] += m.MARKER + scalar()
        self.assertEqual(self.check_rows(rows)['status'], 'INVALID_EVENT_EVIDENCE')


    def test_maintained_compiler_transport_and_race_warning(self):
        for mode in ('controls', 'witness'):
            expected = m.CONTROLS if mode == 'controls' else m.WITNESS
            notes = [{'Action': 'build-output', 'ImportPath': pkg + suffix, 'Output': 'ordinary compiler note\n'}
                     for pkg in expected
                     for suffix in (' [' + pkg + '.test]', '_test [' + pkg + '.test]', '.test')]
            self.assertEqual(self.check_rows(notes + events(mode), mode)['status'], 'PASS')
            for mutate in (lambda e: e.update(ImportPath='foreign.test'),
                           lambda e: e.update(Package=m.PREFIX + 'collections'),
                           lambda e: e.update(Output=None), lambda e: e.pop('Output')):
                note = copy.deepcopy(notes[0])
                mutate(note)
                self.assertEqual(self.check_rows([note] + events(mode), mode)['status'], 'INVALID_EVENT_EVIDENCE')
            for output in ('WARNING: DATA RACE\n', 'prefix WARNING: DATA RACE suffix\n'):
                rows = events(mode)
                rows.insert(1, {'Package': rows[0]['Package'], 'Action': 'output', 'Output': output})
                self.assertEqual(self.check_rows(rows, mode)['status'], 'RACE_DETECTED')
                note = copy.deepcopy(notes[0])
                note['Output'] = output
                self.assertEqual(self.check_rows([note] + events(mode), mode)['status'], 'RACE_DETECTED')

    def test_exact_package_start_order_and_action_controls(self):
        for mode in ('controls', 'witness'):
            base = events(mode)
            for pkg in (m.CONTROLS if mode == 'controls' else m.WITNESS):
                without_start = [x for x in base if not (x['Package'] == pkg and x['Action'] == 'start')]
                self.assertNotEqual(self.check_rows(without_start, mode)['status'], 'PASS')
                start = next(x for x in base if x['Package'] == pkg and x['Action'] == 'start')
                self.assertNotEqual(self.check_rows([start] + base, mode)['status'], 'PASS')
                for action in ('run', 'pause', 'cont'):
                    bad = {'Package': pkg, 'Action': action}
                    changed = base[:1] + [bad] + base[1:]
                    self.assertEqual(self.check_rows(changed, mode)['status'], 'INVALID_EVENT_EVIDENCE')
            note = {'Action': 'build-output', 'ImportPath': m.PREFIX + 'collections.test', 'Output': 'ordinary note\n'}
            self.assertEqual(self.check_rows([note] + base, mode)['status'], 'PASS')

    def test_fixed_go_event_keys_and_optional_field_types(self):
        for mode in ('controls', 'witness'):
            base = events(mode)
            for key, values in {
                'Time': (None, 1, {}, []),
                'Elapsed': (None, True, '0', -1, float('inf'), float('nan'), 10 ** 500),
                'FailedBuild': (None, True, 1, {}, []),
                'Output': (None, True, 1, {}, []),
                'Test': (None, True, 1, {}, []),
            }.items():
                for value in values:
                    with self.subTest(mode=mode, key=key, value=repr(value)):
                        rows = copy.deepcopy(base)
                        rows[1][key] = value
                        self.assertEqual(self.check_rows(rows, mode)['status'], 'INVALID_EVENT_EVIDENCE')
            for key in ('ImportPath', 'unknown'):
                rows = copy.deepcopy(base)
                rows[1][key] = 'not a test transport field'
                self.assertEqual(self.check_rows(rows, mode)['status'], 'INVALID_EVENT_EVIDENCE')
            rows = copy.deepcopy(base)
            for row in rows:
                row['Time'] = '2026-11-05T23:20:00.123456789Z'
                row['Elapsed'] = 0.01
            self.assertEqual(self.check_rows(rows, mode)['status'], 'PASS')
            rows[-1]['FailedBuild'] = 'original failed dependency'
            self.assertEqual(self.check_rows(rows, mode)['status'], 'COMPILE_FAILURE')
            for key, value in (('Time', '2026-11-05T23:20:00Z'), ('Elapsed', 0),
                               ('FailedBuild', ''), ('Test', ''), ('unknown', False)):
                note = {'Action': 'build-output', 'ImportPath': m.PREFIX + 'collections.test', 'Output': 'note\n', key: value}
                self.assertEqual(self.check_rows([note] + base, mode)['status'], 'INVALID_EVENT_EVIDENCE')

    def test_late_test_output_pause_continue_refused(self):
        base = events()
        terminal = next(i for i, x in enumerate(base) if x.get('Test') == m.TOP and x['Action'] == 'pass')
        for action in ('output', 'pause', 'cont'):
            row = {'Package': m.PREFIX + 'collections', 'Test': m.TOP, 'Action': action, 'Output': 'original data\n'}
            self.assertEqual(self.check_rows(base[:terminal + 1] + [row] + base[terminal + 1:])['status'], 'INVALID_EVENT_EVIDENCE')
            self.assertEqual(self.check_rows([base[0], row] + base[1:])['status'], 'INVALID_EVENT_EVIDENCE')
            self.assertEqual(self.check_rows(base[:terminal] + [row] + base[terminal:])['status'], 'PASS')

    def test_strict_duplicate_json_and_non_json_transport(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'events.jsonl'
            text = ''.join(json.dumps(x) + '\n' for x in events())
            path.write_text(text.replace('"Action": "start"', '"Action": "fail", "Action": "start"', 1))
            self.assertEqual(m.validate(path, 'witness', 0)['status'], 'INVALID_EVENT_EVIDENCE')
            for bad in ('null\n', '[]\n', '{"Package": null}\n', 'not JSON\n', '{"Elapsed":NaN}\n'):
                path.write_text(bad)
                result = m.validate(path, 'witness', 1)
                self.assertEqual(result['status'], 'INVALID_EVENT_EVIDENCE')
                self.assertEqual(result['non_event_lines'][0]['text'] + '\n', bad)
            self.assertEqual(m.validate(path / 'absent', 'witness', 1)['status'], 'INCOMPLETE_DIAGNOSTIC_EVIDENCE')
            self.assertFalse(m.validate(path, 'unknown', 0)['accepted'])
            self.assertFalse(m.validate(path, 'witness', True)['accepted'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
