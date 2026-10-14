import datetime, hashlib, json, pathlib, subprocess

ROOT = pathlib.Path('/home/mikers/benchmarks/mvcc-bounded-prune-4878-20261001')
PACKET = ROOT / 'm7-common-protected-vertical-freeze-20261008T0500Z'
DONOR = ROOT / 'cache-publication-architecture-sol61-20261005/integrated-native-publication-owner-sol61-20261006'
EXPECTED_LEDGER = 'e373b60962e0eac52bbf3e6a672f09308d24fa94b79c4f1a57716c4f271d2887'
EXPECTED_MAP = '7f609ebea843c57f8457d80c2eb91e48a945cc18489962ef5adf42e44127bef5'
EXPECTED_DONOR_MAP = '2b455b88f9aae6139bae1a5c09e97a0418f4b07d161f3b004d76de93a50583d6'
OUT = pathlib.Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013/evidence/native-recovery')

def digest(path):
    path = pathlib.Path(path)
    if not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''):
            h.update(b)
    return h.hexdigest()

def check(path, expected, size=None):
    path = pathlib.Path(path)
    actual = digest(path)
    return {'path': str(path), 'expected_sha256': expected, 'actual_sha256': actual,
            'bytes': path.stat().st_size if path.is_file() else None,
            'ok': actual == expected and (size is None or path.stat().st_size == size)}

ledger = json.loads((PACKET / 'ledger-v2.json').read_text())
source = pathlib.Path(ledger['source'])
manifest = json.loads((PACKET / 'source-complete.json').read_text())
out = {'at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
       'classification': 'READ_ONLY_EXACT_SOURCE_AND_PROTECTED_FIXTURE_RECOVERY_AUDIT',
       'host': '192.168.0.111', 'ledger': check(PACKET / 'ledger-v2.json', EXPECTED_LEDGER),
       'source_complete_map': check(PACKET / 'source-complete.json', EXPECTED_MAP),
       'source': str(source), 'source_entries': len(manifest),
       'recorded_git_base': ledger['head'], 'recorded_branch': ledger['branch'],
       'native_fit': False, 'native_rank': False, 'implementation_or_runtime_qualification': False}
bad = []
total = 0
for p, expected in manifest.items():
    row = check(source / p, expected)
    total += row['bytes'] or 0
    if not row['ok']:
        bad.append(row)
out['full_manifest'] = {'verified': len(manifest) - len(bad), 'hashed_bytes': total, 'missing_or_changed': bad}
out['frozen_source_blobs'] = [check(x['blob'], x['sha256'], x['bytes']) for x in ledger['source_bindings']]
out['frozen_source_path_consistency'] = [x['path'] for x in ledger['source_bindings'] if manifest.get(x['path']) != x['sha256']]
out['operation_anchors'] = check(ledger['operation_anchors']['path'], ledger['operation_anchors']['sha256'])
out['baseline_ledger'] = check(ledger['baseline']['path'], ledger['baseline']['sha256'])
dm = json.loads((DONOR / 'baseline-source-map.json').read_text())
out['protected_fixture_donor_map'] = check(DONOR / 'baseline-source-map.json', EXPECTED_DONOR_MAP)
out['protected_fixtures'] = []
for name, expected in [('bounded_prune_test.go', dm['TreeDB/mvcc/bounded_prune_test.go']),
                       ('bounded_prune_whole_current_publication_test.go', ledger['original_fixture_bindings'][0]['sha256'])]:
    row = check(DONOR / 'source/TreeDB/mvcc' / name, expected)
    row['terminal_source_contains_fixture'] = (source / 'TreeDB/mvcc' / name).is_file()
    out['protected_fixtures'].append(row)
out['original_fixture_blob'] = [check(x['blob'], x['sha256'], x['bytes']) for x in ledger['original_fixture_bindings']]
out['components'] = []
for c in ledger['component_evidence']:
    p = pathlib.Path(c['output'])
    rows = [check(p / 'result.json', c['result_sha256']),
            check(p / 'raw.log', c['result']['raw_sha256']),
            check(p / 'source-before.json', c['source_before_sha256']),
            check(p / 'source-after.json', c['source_after_sha256'])]
    actual = json.loads((p / 'result.json').read_text())
    out['components'].append({'path': str(p), 'artifact_bindings': rows,
                              'result_matches_ledger': actual == c['result'],
                              'command': actual['command'], 'cwd': actual['cwd'],
                              'environment': actual['environment'], 'exit_code': actual['exit_code'],
                              'tested_map_equals_terminal_map': c['source_after_sha256'] == EXPECTED_MAP,
                              'source_drift_paths_recorded': len(c['current_go_different_paths'])})
out['live_git_head'] = subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip()
out['live_git_branch'] = subprocess.check_output(['git', '-C', str(source), 'branch', '--show-current'], text=True).strip()
tracked = set(subprocess.check_output(['git', '-C', str(source), 'ls-files', '-z']).decode().split('\0'))
out['terminal_bound_paths_tracked_in_current_index'] = sum(x['path'] in tracked for x in ledger['source_bindings'])
out['terminal_bound_paths_outside_current_index'] = [x['path'] for x in ledger['source_bindings'] if x['path'] not in tracked]
out['historical_accepted_verification'] = {'expected_sha256': 'ff4a5e4f0552b4f009dd1e8f50e8ad8a90f7abdce456987dd09c87dd36a31717',
    'recorded_by': '/home/mikers/benchmarks/mvcc-architecture-4871-root-state-20261001/checkpoints/graph-state.json',
    'original_bytes_recovered': False,
    'note': 'Exact historical root-freeze-verification.json bytes not present in bounded metadata roots. This audit independently verifies actual ledger, all source entries and frozen blobs; it does not claim recovery of the prior attestation.'}
out['source_and_fixture_recovery_complete'] = all([out['ledger']['ok'], out['source_complete_map']['ok'], not bad,
    not out['frozen_source_path_consistency'], all(x['ok'] for x in out['frozen_source_blobs']),
    out['protected_fixture_donor_map']['ok'], all(x['ok'] for x in out['protected_fixtures']),
    all(x['ok'] for x in out['original_fixture_blob'])])
out['provenance_limits'] = ['Git HEAD is the base only; exact derivative identity is the complete SHA256 file map and frozen blob set.',
    'Both original fixtures are separately retained donor files and are absent from the terminal derivative. No copying or synthesis occurred.',
    'Earlier component packets retain differing tested source maps; only unchanged closures may reuse their evidence.',
    'Recovered negative source does not qualify 32R/1MiB fit, SAME Request rank, Accepted/onceACK/full Finish or native performance.']
OUT.mkdir(parents=True, exist_ok=True)
p = OUT / 'audit.json'
p.write_text(json.dumps(out, indent=2) + '\n')
print(json.dumps({'audit_path': str(p), 'audit_sha256': digest(p),
    'source_and_fixture_recovery_complete': out['source_and_fixture_recovery_complete'],
    'full_manifest': out['full_manifest'], 'frozen_blob_count': len(out['frozen_source_blobs']),
    'protected_fixtures': out['protected_fixtures'],
    'component_count': len(out['components']),
    'bad_component_bindings': [c['path'] for c in out['components'] if not c['result_matches_ledger'] or not all(x['ok'] for x in c['artifact_bindings'])]}))
