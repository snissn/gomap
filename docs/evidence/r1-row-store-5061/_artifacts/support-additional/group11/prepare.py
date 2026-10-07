#!/usr/bin/env python3
"""Copy preselected original text only; private group11, never qualification."""
import collections
import datetime
import hashlib
import json
import os
import pathlib
import stat

BASE = pathlib.Path('/tmp/gomap-r1-execution-20261005')
OUT = BASE / '5061-catalog-tail-support-preparation'
HEAD = 'e4e7a3032405bef141010b1c80eb0f0db1cd6f74'
OLD = '0245d8e0f5835dcba2929e4f1ba8b5d7ea909133'
RUNTIME = 'cb5b0d7b3633666ae16752d8c353eeeb23542c71e5e25773e33359d3ffa79aa9'
HARNESS = '706b71f11508bf9c0075b0dd9c5463cd741a49cba83996feaf753330ace77822'
selected = []

def add(group, classification, paths, source=None):
    for path in paths:
        assert path not in [x['original_relative'] for x in selected]
        selected.append(dict(group=group, classification=classification,
                             sourceSHA=source, original_relative=path))

add('original_failure_control', 'ORIGINAL_0245_CI_FAIL_AND_MAIN_SAME_CASE_PASS_NOT_CAUSAL_PROOF', [
    '0245-linux-race2-failure-rest-original.log',
    '0245-linux-race2-job-original.json',
    'main3cfe-linux-race2-rest-original.log',
    'main3cfe-ci-jobs-original.json', 'main3cfe-ci-1835.json',
    '0245-prepared-vector-ci-diagnosis.md', '0245-prepared-vector-ci-diagnosis.json'])
add('repair_review', 'SOURCE_REPAIR_REVIEW_AND_POSTPUSH_AUTHORITY_NOT_CURRENT_GATE_ACCEPTANCE', [
    '0245-catalog-tail-fixture-repair-review.md', '0245-catalog-tail-fixture-repair-review.json',
    '5071-catalog-tail-fixture-repair-0245/repair.patch',
    '5071-catalog-tail-fixture-repair-0245/handoff.md',
    '5071-catalog-tail-fixture-repair-0245/handoff.json',
    '5071-e4e7-final-source-original.json', '5071-e4e7-root-adoption.json',
    '5071-catalog-tail-repaired-pr-body.md', 'e4e7-postpush-and-body-original.json',
    'e4e7-hosted-review-before-original.json', 'e4e7-hosted-review-request-payload.json',
    'e4e7-hosted-review-request-actual.json',
    'e4e7-test_inventory-uv-original.log', 'e4e7-test_impact-uv-original.log'], HEAD)
for prefix, source, classification in [
    ('0245-nativewire-recovery-focused-validation', OLD, 'ORIGINAL_0245_ISOLATED_DIAGNOSTIC_PASS_DOES_NOT_WAIVE_CI_FAIL'),
    ('e4e7-catalog-tail-fixture-validation', HEAD, 'REPAIRED_E4E7_FOCUSED_CORRECTNESS_PASS_NOT_REQUIRED_CI_OR_PERFORMANCE_ACCEPTANCE')]:
    paths = [str(x.relative_to(BASE)) for x in sorted((BASE / (prefix + '-original')).iterdir())]
    assert len(paths) == 14
    add(prefix, classification, paths + [prefix + '.py', prefix + '-root-review.json'], source)
add('0245_windows_successes', 'SOURCE_BOUND_0245_WINDOWS_FIXTURE_PASS_NOT_E4E7_GATE_OR_QUALIFICATION', [
    '0245-windows-core2-success-rest-original.log', '0245-windows-core2-success-original.json',
    '0245-windows-root-capture-fixture-root-review.json',
    '0245-windows-core5-success-rest-original.log', '0245-windows-postmeta-fixture-root-review.json',
    '0245-windows-caching-rest1-success-rest-original.log', '0245-windows-cow-fixture-root-review.json'], OLD)
add('historical_cancellation', 'SUPERSEDED_0245_NORMAL_CANCEL_REQUEST_TRANSITIONAL_FINAL_CANCEL_PENDING', [
    '0245-superseded-run-before-cancel-original.json',
    '0245-superseded-jobs-before-cancel-original.json',
    '0245-superseded-run-cancel-original.log',
    '0245-superseded-jobs-after-cancel-2029-original.json'], OLD)
add('future_observer_contract', 'ROOT_ADOPTED_DERIVE_ONLY_PREPARATION_NO_ACTUAL_LANDING_CONFIG_OR_CAPTURE', [
    'final-observer-contract-recheck-0245-preparation/derivation-template-handoff.json',
    'final-observer-contract-recheck-0245-preparation/derive-configs.md',
    'final-observer-contract-recheck-0245-preparation/derive-configs.py',
    'final-observer-contract-recheck-0245-preparation/handoff.json',
    'final-observer-contract-recheck-0245-preparation/handoff.md',
    'final-observer-contract-recheck-0245-preparation/root-inputs.template.json',
    'final-observer-contract-root-adoption.json'])
add('descriptive_presentation_helpers', 'ROOT_ADOPTED_PYTHON_PRESENTATION_CHECKS_ONLY_NO_CURRENT_MEASUREMENT_ACCEPTANCE', [
    'final-A-report-preparation/README.md', 'final-A-report-preparation/report-a.py',
    'final-A-report-preparation/check-report.py', 'final-A-report-preparation/checks.json',
    'final-A-report-preparation/handoff.json', 'final-A-report-preparation/handoff.md',
    'final-A-report-preparation/inspected-baseline-r1-source.txt',
    'final-C-report-preparation/README.md', 'final-C-report-preparation/format.py',
    'final-C-report-preparation/handoff.json', 'final-C-report-preparation/validation.txt',
    'final-descriptive-report-helper-root-adoption.json'])

def digest(data):
    return hashlib.sha256(data).hexdigest()

def read_regular(path, text_only=True):
    s = path.lstat()
    assert stat.S_ISREG(s.st_mode) and not path.is_symlink(), str(path)
    b = path.read_bytes()
    assert len(b) == s.st_size
    if text_only:
        b.decode('utf8')
        assert b'\0' not in b, str(path)
    return b, s

def facts(path, text_only=True):
    b, s = read_regular(path, text_only)
    return dict(sha256=digest(b), bytes=len(b), mode=f'{stat.S_IMODE(s.st_mode):04o}',
                nonsymlink=True, device=s.st_dev, inode=s.st_ino,
                mtime_ns=s.st_mtime_ns, ctime_ns=s.st_ctime_ns)

def jsonread(relative):
    return json.loads(read_regular(BASE / relative)[0])

def write_json(name, obj):
    with (OUT / name).open('x') as f:
        json.dump(obj, f, indent=2, sort_keys=True)
        f.write('\n')
    os.chmod(OUT / name, 0o644)

def write_text(name, text):
    with (OUT / name).open('x') as f:
        f.write(text)
    os.chmod(OUT / name, 0o644)

plan = jsonread('5061-support-assembly-complete-preparation/assembly-plan.json')
protected_dirs = [BASE / x['name'] for x in plan['groups']]
protected_dirs += [BASE / '5061-postmeta-and-root-capture-support-preparation',
                   BASE / '5061-support-assembly-preparation',
                   BASE / '5061-support-assembly-complete-preparation']

def protected_snapshot():
    result = {}
    def protected_facts(path):
        s = path.lstat()
        if stat.S_ISLNK(s.st_mode):
            target = os.readlink(path)
            return dict(kind='SYMLINK_NEGATIVE_TEST_FIXTURE', target=target,
                sha256=digest(os.fsencode(target)), bytes=s.st_size,
                mode=f'{stat.S_IMODE(s.st_mode):04o}', device=s.st_dev,
                inode=s.st_ino, mtime_ns=s.st_mtime_ns, ctime_ns=s.st_ctime_ns)
        return facts(path, text_only=False)
    for root in protected_dirs:
        assert root.is_dir() and not root.is_symlink()
        for d, dirs, files in os.walk(root, followlinks=False):
            dirs.sort()
            for sub in dirs:
                path = pathlib.Path(d, sub)
                if path.is_symlink():
                    result[str(path.relative_to(BASE))] = protected_facts(path)
            for name in sorted(files):
                path = pathlib.Path(d, name)
                result[str(path.relative_to(BASE))] = protected_facts(path)
    return result

before = protected_snapshot()
preselected = []
for row in selected:
    path = BASE / row['original_relative']
    item = dict(row, original=str(path), canonical_original=str(path.resolve()),
                copy='originals/' + row['original_relative'],
                before=facts(path))
    preselected.append(item)
write_json('selection-before-copy.json', dict(status='ORIGINALS_ENUMERATED_BEFORE_COPY',
    group_number=11, exact_candidate=HEAD, files=preselected))
print(json.dumps({'chosen_originals_before_copy': [x['original_relative'] for x in preselected],
                  'count': len(preselected), 'bytes': sum(x['before']['bytes'] for x in preselected)}, indent=2))

# Independently verify original hashes named by root records before copying.
pin_checks = []
def check_pin(relative, pin, authority):
    actual = facts(BASE / relative)
    assert actual['sha256'] == pin['sha256'], (authority, relative, 'sha')
    if 'bytes' in pin:
        assert actual['bytes'] == pin['bytes'], (authority, relative, 'size')
    pin_checks.append(dict(original=relative, authority=authority, sha256=actual['sha256'], bytes=actual['bytes']))

adoption = jsonread('5071-e4e7-root-adoption.json')
assert adoption['head'] == HEAD
for relative, pin in adoption['inputs'].items():
    check_pin(relative, pin, '5071-e4e7-root-adoption.json')
for prefix in ['0245-nativewire-recovery-focused-validation', 'e4e7-catalog-tail-fixture-validation']:
    review = jsonread(prefix + '-root-review.json')
    for relative, pin in review['files'].items():
        check_pin(prefix + '-original/' + relative, pin, prefix + '-root-review.json')
for relative, pin in jsonread('final-observer-contract-root-adoption.json')['files'].items():
    check_pin('final-observer-contract-recheck-0245-preparation/' + relative, pin, 'final-observer-contract-root-adoption.json')
excluded = [dict(path='5071-e4e7-catalog-tail-repair.bundle', reason='BINARY_GIT_BUNDLE_EXCLUDED'),
            dict(path='state.json', reason='MUTABLE_COORDINATOR_STATE_EXCLUDED'),
            dict(path='0245-required-ci-watch.log', reason='ACTIVE_WATCH_LOG_EXCLUDED'),
            dict(path='final-A-report-preparation/__pycache__/', reason='COMPILED_PYTHON_BINARY_EXCLUDED')]
selected_paths = {x['original_relative'] for x in selected}
for item in jsonread('final-descriptive-report-helper-root-adoption.json')['items']:
    for name, pin in item['files'].items():
        relative = item['directory'] + '/' + name
        check_pin(relative, pin, 'final-descriptive-report-helper-root-adoption.json')
        if relative not in selected_paths:
            excluded.append(dict(path=relative, reason='SYNTHETIC_OR_HISTORICAL_FORMAT_OUTPUT_EXCLUDED',
                                 original_sha256=pin['sha256'], original_bytes=pin['bytes']))

observations = []
for prefix, source in [('0245-nativewire-recovery-focused-validation', OLD),
                       ('e4e7-catalog-tail-fixture-validation', HEAD)]:
    root = prefix + '-original/'
    original_source = jsonread(root + 'source-before.json')
    assert original_source == jsonread(root + 'source-after.json')
    assert original_source['commit'] == source and original_source['clean'] is True
    for phase in ['normal', 'race', 'vet', 'format']:
        start = jsonread(root + phase + '-start.json')
        end = jsonread(root + phase + '-exit.json')
        log = read_regular(BASE / (root + phase + '.log'))[0]
        assert end['exit'] == 0 and digest(log) == end['log_sha256']
        assert start['source'] == end['source'] == original_source
        assert start['driver_sha256'] == facts(BASE / (prefix + '.py'))['sha256']
        events = [json.loads(x) for x in log.decode().splitlines() if x.startswith('{')]
        passes = [x for x in events if x.get('Action') == 'pass' and 'Test' in x]
        failures = [x for x in events if x.get('Action') == 'fail']
        assert not failures
        observations.append(dict(group=prefix, sourceSHA=source, phase=phase, actual_exit=0,
            log=root + phase + '.log', log_sha256=digest(log),
            test_PASS_rows=len(passes), test_FAIL_rows=0,
            parent_PASS=[{'Test': x['Test'], 'Elapsed': x.get('Elapsed')} for x in passes if '/' not in x['Test']],
            argv=start['argv'], cwd=start['cwd'], environment=start['environment'],
            start_utc=start['utc'], exit_utc=end['utc'],
            driver_sha256=start['driver_sha256'], go_sha256=start['go_sha256']))

source = jsonread('5071-e4e7-final-source-original.json')
assert source['commit'] == HEAD and source['clean'] is True
assert source['runtime_sha256'] == RUNTIME and source['harness_sha256'] == HARNESS
postpush = jsonread('e4e7-postpush-and-body-original.json')
assert postpush['headRefOid'] == HEAD
assert postpush['body'].encode() == read_regular(BASE / '5071-catalog-tail-repaired-pr-body.md')[0]
request = jsonread('e4e7-hosted-review-request-actual.json')
assert request['id'] == 6024804942
assert request['body'] == jsonread('e4e7-hosted-review-request-payload.json')['body']
for review_name, log_name in [
    ('0245-windows-root-capture-fixture-root-review.json', '0245-windows-core2-success-rest-original.log'),
    ('0245-windows-postmeta-fixture-root-review.json', '0245-windows-core5-success-rest-original.log'),
    ('0245-windows-cow-fixture-root-review.json', '0245-windows-caching-rest1-success-rest-original.log')]:
    review = jsonread(review_name)
    assert review['head'] == OLD and review['job_conclusion'] == 'success'
    assert facts(BASE / log_name)['sha256'] == review['log_sha256']
    assert facts(BASE / log_name)['bytes'] == review['bytes']
    observations.append(dict(group='0245_windows_successes', sourceSHA=OLD,
        review=review_name, log=log_name, log_sha256=review['log_sha256'],
        run=review['run'], job=review['job'], original_job_conclusion='success',
        fixture_PASS_rows_count=len(review['fixture_PASS_rows']),
        applicability='Historical exact0245 only; not currente4e7 gate.'))

files = []
for item in preselected:
    relative = item['original_relative']
    original = BASE / relative
    data, _ = read_regular(original)
    assert facts(original) == item['before'], ('original_changed_before_copy', relative)
    target = OUT / item['copy']
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open('xb') as f:
        f.write(data)
    os.chmod(target, 0o644)
    copy = facts(target)
    assert copy['sha256'] == item['before']['sha256'] and copy['bytes'] == item['before']['bytes']
    assert copy['mode'] == '0644'
    assert facts(original) == item['before'], ('original_changed_during_copy', relative)
    files.append(dict(group=item['group'], classification=item['classification'], sourceSHA=item['sourceSHA'],
        original=str(original), canonical_original=item['canonical_original'], original_relative=relative,
        original_sha256=item['before']['sha256'], original_bytes=item['before']['bytes'],
        original_mode=item['before']['mode'], original_nonsymlink=True,
        copy=item['copy'], copy_sha256=copy['sha256'], copy_bytes=copy['bytes'],
        copy_mode='0644', copy_nonsymlink=True, utf8_non_nul_exact=True))

after = protected_snapshot()
assert before == after, 'previous_groups_or_assemblers_changed'
for item in preselected:
    assert facts(BASE / item['original_relative']) == item['before']
protected = dict(files=len(before), bytes=sum(x['bytes'] for x in before.values()),
    snapshot_sha256=digest(json.dumps(before, sort_keys=True, separators=(',', ':')).encode()),
    unchanged=True, directories=[str(x) for x in protected_dirs])
limits = [
    'Private group11 remains UNPROMOTED_PENDING_ROOT_ADOPTION; root source adoption is preserved but does not adopt this group.',
    'No GitHub query/action, CI polling, SSH, Go/build/test/capture, publication, merge, or performance promotion by this task.',
    'Original0245 CI FAIL stays FAIL; main3cfe same-case PASS and isolated0245 PASS do not prove unrelatedness or causal catalog lag.',
    'Exact failed combined predicate component is untraced; shared lower R1 runtime may interact with schedule.',
    'Actual e4e7 requiredCI37526268804 and hostedrequest6024804942 remain pending; postpush records are snapshots, not current green gates.',
    'Normal cancellation request and transitional jobs retained; final0245 cancellation remains pending; no force cancel.',
    'Observer contract is candidate0245 preparation; actual merged source/configs/A-C-D receipts/certificate/replay still required.',
    'A/C Python checks are descriptive presentation only and historical/synthetic inputs are excluded from copied output evidence.',
    'No compiled benchmark/test executables or bundles reconstructed or included; copied Python drivers are inert0644 text.',
    'Earlier source-bound Windows evidence is exact0245, not e4e7 CI acceptance; no waivers.']
inventory = dict(schema_version='gomap-r1-supplemental-support-inventory-v1', group_number=11,
    status='UNPROMOTED_PENDING_ROOT_ADOPTION', utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    exact_candidate=HEAD, runtime_sha256=RUNTIME, A_C_harness_sha256=HARNESS,
    files=files, excluded=excluded, observations=observations, original_pin_checks=pin_checks,
    requested_originals_missing=[], frozen_prior_groups_and_assemblers_unchanged=protected, limits=limits)
write_json('inventory.json', inventory)
checks = dict(status='PASS_BYTE_PRESERVATION_ONLY', originals=len(files),
    all_original_copy_bytes_equal=True, all_UTF8_regular_nonsymlink=True, all_copy_modes='0644',
    all_originals_unchanged=True, pin_checks=len(pin_checks), focused_receipt_observations=8,
    frozen_prior_groups_and_assemblers_unchanged=protected, exact_postpush_body_equal=True,
    actual_hosted_request_id=6024804942, no_current_gate_or_acceptance_claim=True)
write_json('checks.json', checks)
write_json('readiness.json', dict(status='UNPROMOTED_PENDING_ROOT_ADOPTION', ready_for_root_byte_review=True,
    original_files=len(files), original_bytes=sum(x['original_bytes'] for x in files),
    missing=[], publication=False, root_group_adoption=False, actual_landing_observed=False,
    current_required_CI_observed=False, hosted_review_accepted=False, actual_final_cancellation_observed=False,
    preserved_original_FAILURE=True, all_original_copy_bytes_equal=True,
    UTF8_regular_nonsymlink_nonexec=True, prior_files_verified_unchanged=len(before)))
rows = ['| Group | Original files | Bytes |', '|---|---:|---:|']
for group in sorted({x['group'] for x in files}):
    subset = [x for x in files if x['group'] == group]
    rows.append(f"| {group} | {len(subset)} | {sum(x['original_bytes'] for x in subset):,} |")
readme = '''Group11 is private, unpromoted original text for final E review. It is not publication or final qualification.

Exact pushed candidate: `e4e7a3032405bef141010b1c80eb0f0db1cd6f74`; runtime `cb5b0d7b3633666ae16752d8c353eeeb23542c71e5e25773e33359d3ffa79aa9`; A/C harness `706b71f11508bf9c0075b0dd9c5463cd741a49cba83996feaf753330ace77822`. The latest root adoption was copied after its actual postpush/body/request additions; its historical fields and timestamps remain original. The writer handoff is the earlier staged0245 snapshot and is not final-head proof.

Original0245 run37521995513/job112470641732 FAIL remains unchanged. Exact main3cfe's same-case PASS is retained. Source review identifies a missing bounded CATALOG readiness barrier separate from DATA-prefix replay, but the combined failing component was not traced. Neither upper-layer equality, isolated PASS, nor the test repair proves the original failure unrelated to R1. Permanent missing/stale activation continues to fail within the original120-second context; no thresholds or required assertions were waived.

Unchanged0245 focused receipts contain7 normal and7 race PASS rows with no FAIL; e4e7 repaired shared-fixture receipts contain21 normal and21 race PASS rows with no FAIL. All actual normal/race/vet/format exits, command/cwd/environment/start/end/source bytes, driver hashes and raw-log hashes are preserved and independently checked. Empty format/vet logs are genuine exit0 outputs, not missing fetch placeholders. Focused correctness does not replace current required CI.

Exact0245 Windows native-root GC, maintenance lifetime and whole-publication-cut fixtures passed. Core5 retains37 parent/child PASS rows covering36 cuts, with parent218.20s; these are source-bound historic correctness, not currente4e7 CI, capacity or performance acceptance. Overall0245 run cancellation was requested normally; queued/transitional jobs remain original, and final cancellation remains pending.

Future observer derivation helpers and their root adoption are preparation only. They inspect0245 and need actual clean merged source plus independently verified landing before actual configs or captures. Runtime/harness equality is not binary or merged-source equivalence. Descriptive A/C report helpers retain code/docs, actual presentation checks and root adoption. Bulky historical/synthetic formatting outputs and Python bytecode are excluded; historical counts in those preserved check records stay historical.

Current e4e7 requiredCI37526268804 and hostedrequest6024804942 are pending. Root retains all gate, landing, A/C/D capture, certificate and public replay authority. No current PASS, merge, benchmark, publication or group adoption is asserted here.

All copied originals are regular nonsymlink UTF8/nonNUL text, mode0644, exact-byte SHA256/size pairs in inventory.json. Original file modes are recorded. SUPPORT_SHA256SUMS binds this private packet; public authorization is separate. Existing ten groups and both prior assembler directories were verified unchanged. No originals or source checkouts were edited.

'''
write_text('README.md', readme + '\n'.join(rows) + '\n')
write_text('handoff.md', f"Prepared unpromoted group11: {len(files)} exact text originals, {sum(x['original_bytes'] for x in files):,} bytes.\n\n" + readme)
metadata = {}
for name in ['inventory.json', 'checks.json', 'readiness.json', 'README.md', 'handoff.md', 'selection-before-copy.json', 'prepare.py']:
    f = facts(OUT / name)
    metadata[name] = dict(sha256=f['sha256'], bytes=f['bytes'])
write_json('handoff.json', dict(status='UNPROMOTED_PENDING_ROOT_ADOPTION', group_number=11,
    original_files=len(files), original_bytes=sum(x['original_bytes'] for x in files),
    exact_candidate=HEAD, runtime_sha256=RUNTIME, A_C_harness_sha256=HARNESS,
    metadata=metadata, prior_files_verified_unchanged=len(before), requested_originals_missing=[],
    root_source_adoption_included=True, root_group_adoption=False,
    current_required_CI_observed=False, hosted_review_accepted=False, actual_landing_observed=False,
    actual_final_cancellation_observed=False, publication=False, checks_exit=0, worker_released=True))
ledger = []
for f in sorted(OUT.rglob('*')):
    if f.is_file() and f.name != 'SUPPORT_SHA256SUMS':
        ledger.append(f"{facts(f)['sha256']}  {f.relative_to(OUT)}")
write_text('SUPPORT_SHA256SUMS', '\n'.join(ledger) + '\n')
for line in ledger:
    expected, relative = line.split('  ', 1)
    assert facts(OUT / relative)['sha256'] == expected
print(json.dumps(dict(status='UNPROMOTED_PENDING_ROOT_ADOPTION', files=len(files),
    original_bytes=sum(x['original_bytes'] for x in files),
    inventory=facts(OUT / 'inventory.json'), ledger=facts(OUT / 'SUPPORT_SHA256SUMS'),
    prior_files_verified_unchanged=len(before), originals_unchanged=True), indent=2))
