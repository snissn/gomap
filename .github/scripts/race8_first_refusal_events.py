#!/usr/bin/env python3
"""Fixed 3db7d8 basis-refusal controls/witness evidence checker; never changes Go status."""
import argparse
import hashlib
import json
import math
import re
from pathlib import Path

PREFIX = 'github.com/snissn/gomap/TreeDB/'
TOP = 'TestNativeOrdinaryConcurrentExactLifecycle'
CONTROL_CHILDREN = {'caching': {'TestCOWBasisRefreshRefusalKeepsOriginalAndOwnsPartialCleanup': ['unsupported_owner', 'owned_partial'], 'TestCOWBasisRefusalExactInvocationAndPartial': ['active', 'frozen'], 'TestCOWBasisRefusalExclusiveCacheBinding': [], 'TestCOWBasisRefusalMissingSeamPreservesOriginal': [], 'TestCOWPhysicalBasisRefreshPreservesAppliedLogicalIdentity': ['cow_btree/frozen=false', 'cow_btree/frozen=true', 'cow_ordered/frozen=false', 'cow_ordered/frozen=true']}, 'collections': {'TestNativeBasisRefusalDescriptorPropagationAndAllocation': [], 'TestNativeNamedDomainReplacement': [], 'TestNativeScalarReplacementDuringCapturedBuild': []}, 'internal/valuelog': {'TestCOWBasisRefusalActualInspectorControls': ['successful_inspector_equal_and_allocation', 'actual_source_refusal', 'actual_frame_uncompressed_refusal', 'actual_frame_dictionary_refusal', 'actual_frame_codec_refusal', 'actual_payload_refusal', 'actual_payload_no_frame_subset'], 'TestCOWBasisRefusalScalarControls': ['owner_and_no_entry', 'first_tuple_and_unknown_shape', 'known_shape_and_read_stage', 'no_error_methods_or_allocation', 'metadata_known_subset', 'readback_while_borrower_active', 'ordinal_overflow_refuses_attribution'], 'TestCOWLeafInspectionWindowMatchesRecordInspector': ['page', 'compact', 'snappy', 'lz4'], 'TestCOWLeafRecordBoundedRawCompactAndCompressed': ['raw', 'compact', 'compressed-compact'], 'TestCOWRecordRawAndGroupedNoAllocation': ['raw', 'grouped']}}
CONTROL_TOPS = {pkg: tuple(tests) for pkg, tests in CONTROL_CHILDREN.items()}
CONTROLS_SELECTOR = '^(' + '|'.join(x for v in CONTROL_TOPS.values() for x in v) + ')$'
WITNESS_SELECTOR = '^' + TOP + '$'
CONTROLS = {PREFIX + pkg: {name for top, children in tests.items()
             for name in [top, *(top + '/' + child for child in children)]}
            for pkg, tests in CONTROL_CHILDREN.items()}
WITNESS = {PREFIX + 'collections': {TOP}}
CHILDREN = {TOP}  # The original witness has one top and no subtests.
MARKER = 'basis-first-refusal: '
MAX_INPUT_BYTES = 32 * 1024 * 1024
TEST_EVENT_KEYS = {'Time', 'Action', 'Package', 'Test', 'Elapsed', 'Output', 'FailedBuild'}
BUILD_EVENT_KEYS = {'Action', 'ImportPath', 'Output'}
U64 = (1 << 64) - 1
STAGES = ('refresh', 'descriptor', 'pin', 'inspect', 'decoder_sizes', 'admission', 'decoder', 'leaf_read')
DESCRIPTORS = ('no_descriptor', 'next_incarnation', 'metadata', 'incarnation', 'overlay', 'root')
REFUSALS = ('UNKNOWN', 'other_error', 'unsupported', 'capacity', 'corrupt', 'source_contract',
            'frame_contract', 'payload_contract', 'dictionary')
# Exactly fmt %+v of immutable 3db7d8 COWBasisRefusalSnapshot. No DNS/body/error parser.
UINT = r'(0|[1-9][0-9]{0,19})'
ARRAY = r'\[' + ' '.join([UINT] * 8) + r'\]'
RECEIPT_PATTERN = re.compile(r'\{Calls:' + UINT + r' Completed:' + UINT + r' First:' + UINT
    + r' Overflow:(true|false) Missing:(true|false) Entered:' + ARRAY + r' Finished:' + ARRAY + r'\}')

def _need(ok, label):
    if not ok:
        raise ValueError(label)


def _object_pairs(pairs):
    value = {}
    for key, item in pairs:
        _need(key not in value, 'duplicate_json_key')
        value[key] = item
    return value


def _json(text):
    return json.loads(text, object_pairs_hook=_object_pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('non_json_number')))


def _integer(value, maximum):
    return type(value) is int and 0 <= value <= maximum


def _array(value, length):
    return type(value) is list and len(value) == length


def receipt(text):
    """Decode the atomic first tuple; other scalars are independent samples, never a join."""
    _need(type(text) is str, 'receipt_text')
    match = RECEIPT_PATTERN.fullmatch(text)
    _need(match is not None, 'fixed_fmt_snapshot_shape')
    groups = match.groups()
    values = [int(x) for x in groups[:3]] + [int(x) for x in groups[5:]]
    _need(all(_integer(x, U64) for x in values), 'snapshot_uint64')
    calls, completed, first = values[:3]
    _need(first < 1 << 61, 'first_tuple_reserved_high_bits')
    snapshot = {'Calls': calls, 'Completed': completed, 'First': first,
                'Overflow': groups[3] == 'true', 'Missing': groups[4] == 'true',
                'Entered': values[3:11], 'Finished': values[11:19]}
    # Do not compare completed/entered/finished to each other or to Calls: each Load
    # samples a different instant, including after failed Close with active borrowers.
    unknown = []
    if calls == 0:
        unknown.append('no_observed_call')
    if snapshot['Overflow']:
        unknown.append('ordinal_overflow')
    if snapshot['Missing']:
        unknown.append('missing_seam')
    decoded = None
    if first == 0:
        unknown.append('no_observed_refusal_not_success')
    else:
        ordinal, descriptor = first & 0xffffff, (first >> 25) & 7
        stage, why = (first >> 29) & 15, (first >> 33) & 15
        known = bool(first & (1 << 37))
        _need(ordinal != 0 and stage < 8 and why != 0, 'impossible_first_tuple')
        shape_bits = (first >> 38) & 0x3ff
        metadata_known = (first >> 48) & 7
        metadata_compressed = bool(first & (1 << 51))
        metadata_dictionary = bool(first & (1 << 52))
        raw_codec = (first >> 53) & 255
        _need(known or shape_bits == 0, 'unknown_shape_has_set_shape_bits')
        _need(metadata_known in (0, 1, 7), 'invalid_internal_known_subset')
        _need(not known or metadata_known == 0, 'validated_shape_internal_metadata_conflation')
        _need(metadata_known & 1 or not metadata_compressed, 'unknown_internal_compression_value')
        _need(metadata_known & 2 or not metadata_dictionary, 'unknown_internal_dictionary_value')
        _need(metadata_known & 4 or raw_codec == 0, 'unknown_internal_raw_codec_value')
        if descriptor >= len(DESCRIPTORS) or why >= len(REFUSALS):
            unknown.append('unknown_descriptor_or_refusal_enum')
        decoded = {'ordinal': ordinal, 'frozen': bool(first & (1 << 24)),
                   'descriptor_id': descriptor,
                   'descriptor': DESCRIPTORS[descriptor] if descriptor < len(DESCRIPTORS) else 'UNKNOWN',
                   'after': bool(first & (1 << 28)), 'stage_id': stage, 'stage': STAGES[stage],
                   'refusal_id': why, 'refusal': REFUSALS[why] if why < len(REFUSALS) else 'UNKNOWN',
                   'shape_known': known,
                   'compressed': bool(first & (1 << 38)) if known else None,
                   'dictionary_present': bool(first & (1 << 39)) if known else None,
                   'codec_id': (first >> 40) & 255 if known else None,
                   'internal_metadata': {
                       'compression_known': bool(metadata_known & 1),
                       'compressed': metadata_compressed if metadata_known & 1 else None,
                       'dictionary_presence_known': bool(metadata_known & 2),
                       'dictionary_present': metadata_dictionary if metadata_known & 2 else None,
                       'raw_codec_known': bool(metadata_known & 4),
                       'raw_frame_reserved': raw_codec if metadata_known & 4 else None,
                       'raw_codec_meaning': 'UNKNOWN_ENUM' if metadata_known & 4 and raw_codec not in (0, 1, 2, 3) else
                                            ('RAW_RESERVED_ONLY' if metadata_known & 4 else 'UNKNOWN')
                   }}
    return {'receipt': snapshot, 'first_tuple': decoded, 'complete': True,
            'incomplete': [], 'unknown': unknown,
            'attribution': 'OBSERVED_FIRST_REFUSAL' if not unknown else 'UNKNOWN',
            'independent_scalar_samples': True, 'borrower_join_proved': False,
            'zero_first_is_success': False}


def validate(path, mode, go_exit):
    """Classify closed original bytes. Every adverse result is returned, not hidden."""
    result = {'mode': mode, 'go_exit': go_exit, 'status': 'INVALID_EVENT_EVIDENCE',
              'accepted': False, 'issues': [], 'events': {}, 'packages': {}, 'receipts': {},
              'diagnostic_output': [], 'non_event_lines': [], 'literal_failures': [],
              'source_scope': '3db7d8-fixed-original-fixture'}
    if mode not in ('controls', 'witness') or type(go_exit) is not int or not -255 <= go_exit <= 255:
        result['issues'].append({'label': 'invalid_fixed_mode_or_go_exit'})
        return result
    expected = CONTROLS if mode == 'controls' else WITNESS
    counts = {pkg: {name: {'run': 0, 'pass': 0, 'fail': 0, 'skip': 0} for name in sorted(names)}
              for pkg, names in expected.items()}
    packages = {pkg: {'start': 0, 'pass': 0, 'fail': 0, 'skip': 0} for pkg in expected}
    result['events'], result['packages'] = counts, packages
    event_errors, receipt_errors, incomplete = [], [], []
    compile_failure = False
    race_warning = False
    compiler_paths = {pkg + suffix for pkg in expected
                      for suffix in (' [' + pkg + '.test]', '_test [' + pkg + '.test]', '.test')}
    timeout = go_exit in (124, 137, 143, -9, -15)
    aborted = False
    try:
        with Path(path).open('rb') as stream:
            raw = stream.read(MAX_INPUT_BYTES + 1)
        result['input_bytes'] = len(raw)
        result['input_sha256'] = hashlib.sha256(raw).hexdigest()
        result['input_hash_scope'] = 'complete_file' if len(raw) <= MAX_INPUT_BYTES else 'bounded_prefix_only'
        if len(raw) > MAX_INPUT_BYTES:
            result['issues'].append({'label': 'input_exceeds_fixed_32MiB_bound'})
            return result
        lines = raw.decode('utf-8').splitlines()
    except (OSError, UnicodeError):
        result['issues'].append({'label': 'missing_or_non_utf8_event_file'})
        result['status'] = 'INCOMPLETE_DIAGNOSTIC_EVIDENCE'
        return result
    for number, line in enumerate(lines, 1):
        try:
            event = _json(line)
            _need(type(event) is dict, 'event_object')
            pkg, action, test = event.get('Package'), event.get('Action'), event.get('Test')
            _need(type(action) is str, 'event_action_type')
            # Maintained 1a14 compiler transport: Go1.26 emits build notes
            # without Package, using only the selected package's test identities.
            if action in ('build-output', 'build-fail'):
                _need(set(event) <= BUILD_EVENT_KEYS
                      and event.get('ImportPath') in compiler_paths
                      and type(event.get('Output', '')) is str, 'foreign_compiler_transport')
                _need(action != 'build-output' or 'Output' in event, 'missing_compiler_output')
                text = event.get('Output', '')
                result['diagnostic_output'].append({'line': number, 'ImportPath': event['ImportPath'],
                                                    'Package': None, 'Test': None, 'Output': text})
                compile_failure = compile_failure or action == 'build-fail' or '[build failed]' in text
                race_warning = race_warning or 'WARNING: DATA RACE' in text
                continue
            _need(set(event) <= TEST_EVENT_KEYS, 'unknown_test_event_key')
            _need('Time' not in event or type(event['Time']) is str, 'event_time_type')
            _need('Elapsed' not in event or (type(event['Elapsed']) in (int, float)
                  and math.isfinite(event['Elapsed']) and event['Elapsed'] >= 0), 'event_elapsed_type')
            _need('FailedBuild' not in event or type(event['FailedBuild']) is str, 'event_failed_build_type')
            _need('Test' not in event or type(event['Test']) is str, 'event_test_type')
            _need(type(pkg) is str and pkg in expected, 'unexpected_exact_package')
            _need(action in ('start', 'run', 'pause', 'cont', 'pass', 'fail', 'skip', 'output'),
                  'unexpected_event_action')
            _need(test is None or type(test) is str and test in counts[pkg], 'unexpected_exact_test')
            _need(packages[pkg]['pass'] + packages[pkg]['fail'] + packages[pkg]['skip'] == 0,
                  'late_event_after_package_terminal')
            if test is None:
                _need(action in ('start', 'output', 'pass', 'fail', 'skip'), 'unexpected_package_action')
                _need(action != 'start' or packages[pkg]['start'] == 0, 'duplicate_package_start')
                _need(action == 'start' or packages[pkg]['start'] == 1, 'package_action_before_start')
            else:
                _need(action != 'start', 'unexpected_test_start')
                _need(packages[pkg]['start'] == 1, 'test_before_package_start')
                if action in ('output', 'pause', 'cont'):
                    count = counts[pkg][test]
                    _need(count['run'] == 1 and count['pass'] + count['fail'] + count['skip'] == 0,
                          'test_output_or_pause_outside_active_run')
            compile_failure = compile_failure or bool(event.get('FailedBuild'))
            output = event.get('Output', '')
            _need(type(output) is str, 'event_output_type')
            if output:
                result['diagnostic_output'].append({'line': number, 'Package': pkg, 'Test': test, 'Output': output})
                timeout = timeout or 'panic: test timed out after ' in output
                aborted = aborted or 'panic:' in output or 'fatal error:' in output
                compile_failure = compile_failure or '[build failed]' in output
                race_warning = race_warning or 'WARNING: DATA RACE' in output
            if action in ('run', 'pass', 'fail', 'skip') and test is not None:
                count = counts[pkg][test]
                if action == 'run':
                    _need(count == {'run': 0, 'pass': 0, 'fail': 0, 'skip': 0}, 'duplicate_or_late_run')
                else:
                    _need(count['run'] == 1 and count['pass'] + count['fail'] + count['skip'] == 0,
                          'terminal_without_unique_run')
                count[action] += 1
            elif test is None and action in packages[pkg]:
                packages[pkg][action] += 1
            if action == 'fail':
                result['literal_failures'].append({'line': number, 'Package': pkg, 'Test': test})
            if MARKER in output:
                _need(mode == 'witness' and pkg == PREFIX + 'collections' and test in CHILDREN and output.count(MARKER) == 1,
                      'receipt_outside_original_witness')
                _need(test not in result['receipts'], 'duplicate_original_receipt')
                text = output.split(MARKER, 1)[1].strip()
                item = {'line': number, 'raw': text, 'valid': False}
                result['receipts'][test] = item
                if text.startswith(('UNKNOWN:', 'INCOMPLETE:')):
                    item['classification'] = 'ORIGINAL_INCOMPLETE_RECEIPT'
                    incomplete.append('original_unknown_or_incomplete_receipt')
                else:
                    try:
                        checked = receipt(text)
                        item.update(checked)
                        item['valid'] = True
                        incomplete.extend(checked['incomplete'])
                    except (ValueError, TypeError, KeyError, RecursionError):
                        receipt_errors.append({'line': number, 'label': 'invalid_fixed_receipt'})
        except (ValueError, TypeError, KeyError, RecursionError, OverflowError):
            event_errors.append({'line': number, 'label': 'invalid_or_unexpected_event'})
            result['non_event_lines'].append({'line': number, 'text': line})
    complete_events = all(v['run'] == 1 and v['pass'] + v['fail'] == 1 and v['skip'] == 0
                          for group in counts.values() for v in group.values())
    complete_packages = all(v['start'] == 1 and v['pass'] + v['fail'] == 1 and v['skip'] == 0 for v in packages.values())
    all_pass = complete_events and complete_packages and all(v['fail'] == 0 for group in counts.values() for v in group.values()) and all(v['pass'] == 1 for v in packages.values())
    if mode == 'witness' and set(result['receipts']) != CHILDREN:
        incomplete.append('missing_original_scalar_receipt')
    has_skips = any(v['skip'] for group in counts.values() for v in group.values()) or any(v['skip'] for v in packages.values())
    if not complete_events or not complete_packages:
        incomplete.append('missing_or_nonterminal_named_events')
    if complete_events and complete_packages:
        for pkg, tests in counts.items():
            has_fail = any(v['fail'] for v in tests.values())
            if bool(packages[pkg]['fail']) != has_fail:
                event_errors.append({'label': 'package_test_terminal_mismatch', 'Package': pkg})
            for name, count in tests.items():
                if '/' in name and count['fail'] and not tests[name.split('/', 1)[0]]['fail']:
                    event_errors.append({'label': 'child_parent_terminal_mismatch', 'Package': pkg, 'Test': name})
    result['complete_named_events'] = complete_events
    result['complete_package_terminals'] = complete_packages
    failed_names = {(x['Package'], x['Test']) for x in result['literal_failures'] if x['Test'] is not None}
    failure_text = [x for x in result['diagnostic_output']
                    if (x['Package'], x['Test']) in failed_names
                    and MARKER not in x['Output']
                    and not x['Output'].lstrip().startswith(('=== RUN', '=== NAME', '--- FAIL', '--- PASS'))]
    result['failure_text_observed'] = bool(failure_text)
    if mode == 'witness' and result['literal_failures'] and not failure_text:
        incomplete.append('missing_original_assertion_or_error_output')
    result['issues'] = event_errors + receipt_errors + [{'label': x} for x in sorted(set(incomplete))]
    if event_errors:
        result['status'] = 'INVALID_EVENT_EVIDENCE'
    elif receipt_errors:
        result['status'] = 'INVALID_RECEIPT'
    elif race_warning:
        result['status'] = 'RACE_DETECTED'
        result['issues'].append({'label': 'literal_warning_data_race'})
    elif timeout or aborted:
        result['status'] = 'TIMEOUT_OR_INTERRUPTED'
    elif compile_failure:
        result['status'] = 'COMPILE_FAILURE'
    elif mode == 'controls' and (go_exit != 0 or not all_pass or has_skips):
        result['status'] = 'CONTROL_FAILURE'
    elif incomplete or has_skips:
        result['status'] = 'INCOMPLETE_DIAGNOSTIC_EVIDENCE'
    elif go_exit == 0 and all_pass:
        positive = True  # Literal test PASS plus valid receipt is non-reproduction only, not refusal/cause proof.
        result['status'] = 'PASS' if positive else 'INCOMPLETE_DIAGNOSTIC_EVIDENCE'
        if not positive:
            result['issues'].append({'label': 'pass_without_fixed_original_receipt'})
    elif mode == 'witness' and go_exit == 1 and result['literal_failures'] and not all_pass:
        result['status'] = 'FAILED_DIAGNOSTIC_EVIDENCE'
    else:
        result['status'] = 'INCONSISTENT_EXIT_OR_EVENT_EVIDENCE'
    result['accepted'] = result['status'] == 'PASS'
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--path', required=True)
    parser.add_argument('--mode', choices=('controls', 'witness'), required=True)
    parser.add_argument('--go-exit', type=int, required=True)
    parser.add_argument('--summary', required=True)
    args = parser.parse_args()
    result = validate(args.path, args.mode, args.go_exit)
    # Closed-byte summary exists even for a failed original witness. A write/close
    # error stays terminal; no success is printed or retry performed.
    with Path(args.summary).open('w', encoding='utf-8') as stream:
        stream.write(json.dumps(result, sort_keys=True, indent=2) + '\n')
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
