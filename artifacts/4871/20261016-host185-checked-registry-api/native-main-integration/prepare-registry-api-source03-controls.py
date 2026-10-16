import datetime
import hashlib
import json
from pathlib import Path
import re

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
P = D / 'native-main-integration/native-registry-api-source02'
B = D / 'native-main-integration/native-registry-api-source03'
W = D / 'native-integrated'
F = D / 'native-decision'
changed_name = 'TreeDB/db/reader_registry_header_failure_v6_test.go'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_text())


def put(path, value):
    with path.open('x') as out:
        json.dump(value, out, indent=2, sort_keys=True)
        out.write('\n')


old, new = read(P / 'source-frozen.json'), read(B / 'source-frozen.json')
assert set(old) == set(new)
assert [name for name in new if new[name] != old[name]] == [changed_name]
assert new[changed_name] == sha(W / changed_name)
prior = read(P / 'proposed-commands.json')
assert read(P / 'compile-online02/result.json')['status'] == 'PASS'
assert read(F / 'root-native-registry-api-compile-source02-verification.json')['accepted']
assert read(F / 'root-native-registry-api-source02-focused-verification.json')['accepted'] is False
reuse_names = ['normal-retainedalloc', 'normal-lifecycle']
checks = []
for command in prior['commands'][:2]:
    assert command['name'] in reuse_names
    O = P / 'controls' / command['name']
    result = read(O / 'result.json')
    raw = (O / 'run.log').read_text()
    runs = re.findall(r'^=== RUN\s+(\S+)', raw, re.M)
    passes = re.findall(r'^\s*--- PASS: (\S+)', raw, re.M)
    expected = command['required_top_level_tests'] + [parent + '/' + child
        for parent, children in command['required_child_tests'].items() for child in children]
    assert result['status'] == 'PASS' and result['exit_code'] == 0
    assert sorted(runs) == sorted(passes) == sorted(expected)
    assert len(runs) == len(set(runs))
    assert result['raw_log_sha256'] == sha(O / 'run.log')
    assert read(O / 'source-before.json') == read(O / 'source-after.json') == old
    assert result['group_and_children_empty'] and not result['signals']
    assert not result['remaining_group_members'] and not result['remaining_direct_or_adopted_children']
    package = next(v[2:] for v in command['argv'] if v.startswith('./'))
    package_tests = list((W / package).glob('*_test.go'))
    # Go never includes a dependency package's _test.go files in an import.
    # The only changed input is a DB test file outside either reused package;
    # all their own tests, dependency production, assets and modules are equal.
    assert changed_name.endswith('_test.go') and not changed_name.startswith(package + '/')
    assert all(new[p.relative_to(W).as_posix()] == old[p.relative_to(W).as_posix()] for p in package_tests)
    checks.append({'name': command['name'], 'raw_sha256': sha(O / 'run.log'),
        'result_sha256': sha(O / 'result.json'), 'top': len(command['required_top_level_tests']),
        'child': command['expected_child_count'], 'groups_joined': True,
        'same_package_inputs_and_all_non_db_fixture_files_unchanged': True})
reuse = {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'status': 'ACCEPTED_UNCHANGED_INPUT_REUSE',
    'parent_manifest_sha256': sha(P / 'source-frozen.json'),
    'successor_manifest_sha256': sha(B / 'source-frozen.json'),
    'only_changed_path': changed_name, 'checks': checks,
    'historical_normal_counts': {'top': sum(c['top'] for c in checks), 'child': sum(c['child'] for c in checks)},
    'parent_compile_result': str(P / 'compile-online02/result.json'),
    'parent_compile_result_sha256': sha(P / 'compile-online02/result.json'),
    'compile_scope': 'All production and unchanged test/module inputs retain source02 root-module compile/default-vet evidence. Changed DB fixture compiles in fresh normal-db; no separate whole-source03 compile claim.',
    'all_affected_test_bodies_pending': True,
    'native_or_performance_qualification': False}
put(F / 'root-native-registry-api-source03-reuse.json', reuse)
plan = dict(prior)
plan['commands'] = [dict(c) for c in prior['commands'] if c['name'] not in reuse_names]
assert len(plan['commands']) == 8
for command in plan['commands']:
    command['env'] = dict(command['env'], TMPDIR=str(B / 'controls' / command['name'] / 'tmp'))
plan['scope'] = 'source03 eight-command focused continuation with explicit unchanged normal retainedalloc/lifecycle evidence reuse'
plan['source_manifest_sha256'] = sha(B / 'source-frozen.json')
plan['test_file_bindings'] = {name: new[name] for name in prior['test_file_bindings']}
plan['historical_reuse'] = str(F / 'root-native-registry-api-source03-reuse.json')
plan['historical_reuse_sha256'] = sha(F / 'root-native-registry-api-source03-reuse.json')
put(B / 'proposed-commands.json', plan)
print(json.dumps({'plan_sha256': sha(B / 'proposed-commands.json'), 'reuse_sha256': plan['historical_reuse_sha256'],
    'historical_normal': reuse['historical_normal_counts'], 'fresh_command_names': [c['name'] for c in plan['commands']]}, indent=2))
