from pathlib import Path
import datetime, hashlib, json, os, re, sys, tarfile

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
W = D / 'native-integrated'
packet, grant_hash, archive_hash, expected_outcome, receipt_name = sys.argv[1:]
O = D / 'native-main-integration' / packet
assert expected_outcome in {'SETUP_STOP', 'PASS_LEDGER_INCOMPLETE'}
def h(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()
def j(p):
    return json.loads(p.read_text())
def archive_payload(p):
    result = {}
    with tarfile.open(p, 'r:gz') as tf:
        for m in tf:
            if m.isfile():
                assert m.name not in result
                result[m.name] = hashlib.sha256(tf.extractfile(m).read()).hexdigest()
    return result

manifest = j(O / 'artifact-manifest.json')
for name, digest in manifest.items():
    assert h(O / name) == digest, name
assert h(O / 'evidence.tar.gz') == archive_hash
payload = archive_payload(O / 'evidence.tar.gz')
assert payload == dict(manifest, **{'artifact-manifest.json': h(O / 'artifact-manifest.json')})
assert h(O / 'grant-original.json') == grant_hash
grant = j(O / 'grant-original.json')
expected = grant['command']
assert grant['maximum_commands'] == 1 and grant['retries'] == 0
assert not grant['publication_runtime_authorized'] and not grant['benchmark_authorized']
source_path, source_archive = Path(expected['source_manifest']), Path(expected['source_archive'])
assert h(source_path) == expected['source_manifest_sha256']
assert h(source_archive) == expected['source_archive_sha256']
frozen = j(source_path)
assert archive_payload(source_archive) == frozen and len(frozen) == 10389
live = {}
for base, dirs, files in os.walk(W):
    dirs[:] = [d for d in dirs if d != '.git']
    for name in files:
        p = Path(base, name)
        rel = str(p.relative_to(W))
        if rel != '.git' and p.is_file():
            live[rel] = h(p)
assert live == frozen
call = O / expected['name']
actual, result, proc = j(call / 'command.json'), j(call / 'result.json'), j(call / 'process.json')
raw = (call / 'run.log').read_text()
assert actual['argv'] == expected['argv'] and actual['env'] == expected['env'] and actual['cwd'] == str(W)
assert actual['outer_timeout_seconds'] == expected['outer_timeout_seconds'] == 240
assert actual['executed'] is True and expected['executed'] is False
assert actual['actual_pid'] == result['pid'] == proc['pid']
assert actual['actual_pgid'] == result['pgid'] == proc['pgid']
assert h(call / 'source-before.json') == h(call / 'source-after.json') == h(source_path)
for label, pattern in [('RUN', r'^=== RUN\s+(\S+)'), ('PASS', r'^\s*--- PASS: (\S+)'), ('FAIL', r'^\s*--- FAIL: (\S+)'), ('SKIP', r'^\s*--- SKIP: (\S+)')]:
    assert re.findall(pattern, raw, re.M) == result['test_' + label + '_names']
assert not result['test_SKIP_names'] and not result['test_FAIL_names']
assert result['leader_joined'] and result['group_and_children_empty'] and result['subreaper_verified']
assert not result['signals'] and not result['timeout']
assert not result['remaining_group_members'] and not result['remaining_direct_or_adopted_children']
assert result['residual_before_cleanup'] == {'children': [], 'group': []}
assert result['raw_log_sha256'] == h(call / 'run.log')
identity, index, after = j(O / 'identity.json'), j(O / 'index-binding.json'), j(O / 'index-after.json')
assert identity['runner_pid'] == proc['runner_pid']
assert after['unchanged'] and after['index_sha256_after'] == index['index_sha256_before'] == h(Path(index['index_path']))
assert index['root_recorded_tree'] == after['root_recorded_tree'] == expected['source_tree']
assert identity['runner_sha256'] == h(O / 'run-observation.py')
assert identity['go_sha256'] == h(Path(identity['go_binary'])) == 'd68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65'
assert identity['compiler_sha256'] == h(D / 'toolchains/go1.26.3/pkg/tool/linux_amd64/compile') == '3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1'
assert not Path('/proc', str(identity['runner_pid'])).exists()
for p in Path('/proc').iterdir():
    if not p.name.isdigit():
        continue
    try:
        stat = (p / 'stat').read_text()
        fields = stat[stat.rfind(')') + 2:].split()
    except (OSError, ValueError):
        continue
    assert int(fields[2]) != result['pgid'] and int(fields[1]) != identity['runner_pid']
summary = j(O / 'summary.json')
assert summary['commands_executed'] == 1 and summary['actual33_executed'] and not summary['native_publication_executed']
assert summary['source_still_frozen'] and summary['all_groups_empty']
if expected_outcome == 'SETUP_STOP':
    assert result['exit_code'] == 1 and result['status'] == 'STOP' and not summary['all_authorized_pass']
    assert result['test_RUN_names'] == result['test_PASS_names'] == []
    assert raw.strip() == 'go: creating work dir: stat ' + expected['env']['TMPDIR'] + ': no such file or directory'
    semantic = 'Setup stopped before test bodies; original missing TMPDIR evidence preserved.'
else:
    assert result['exit_code'] == 0 and result['status'] == 'PASS' and summary['all_authorized_pass']
    assert result['top_level_pass_count'] == 1 and not result['stop_reasons']
    assert result['test_RUN_names'] == result['test_PASS_names'] == ['TestNativeMVCCRealBackendRouteRefusalPreservesCustodyV6']
    preflight = j(call / 'tmpdir-preflight.json')
    assert preflight['path'] == expected['env']['TMPDIR'] and preflight['exists_writable'] and preflight['write_read_unlink_verified'] and preflight['immediately_before_launch']
    assert 'selected constructor ledger incomplete: cursor observer refusal' in raw
    assert not re.search(r'domain=selected-empty-constructor-admission-envelope P=', raw)
    for group in range(1, 6):
        assert f'native-observer group={group} ' in raw and f'basisAfter=group{group} ready=true same=true' in raw
    assert 'computedRecords=367 computedBytes=882745 applicableComplete=true observationsAreNotAtomic=true noAllowanceChosen=true' in raw
    assert raw.count('SameRetainedCapture:true') == 2
    assert 'retainedUnionComplete=false' in raw and 'futureFinish=FutureGeometryUnobserved' in raw
    observation = j(O / 'observations.json')
    assert not observation['selected_constructor_ledger_complete'] and not observation['numeric_P_emitted']
    assert not observation['allowance_chosen'] and not observation['whole_retained_bound_claimed']
    semantic = 'Test PASS but accounting goal FAILED: early cursor observer refusal prevents numeric selected P; no native publication or allowance.'
receipt = {
    'verified_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'verifier_sha256': h(Path(__file__)), 'source_files': len(frozen),
    'source_manifest_sha256': h(source_path), 'source_archive_sha256': h(source_archive),
    'evidence_archive_sha256': h(O / 'evidence.tar.gz'), 'evidence_archive_members': len(payload),
    'evidence_manifest_sha256': h(O / 'artifact-manifest.json'),
    'execution_result': expected_outcome, 'semantic_result': semantic,
    'command_metadata_note': 'Grant executed:false is a template; actual receipt records executed:true and PID/PGID.',
    'runtime_repeated': False, 'source_and_index_unchanged': True,
    'processes_joined_and_absent': True, 'public_status': 'Q32 RED/ACK0; no native publication or allowance'
}
(D / 'native-main-integration' / receipt_name).write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt, indent=2))
