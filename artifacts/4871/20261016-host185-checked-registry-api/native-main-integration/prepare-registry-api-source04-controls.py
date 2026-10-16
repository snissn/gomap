import datetime
import hashlib
import json
from pathlib import Path
import re

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
P = D / 'native-main-integration/native-registry-api-source02'
B = D / 'native-main-integration/native-registry-api-source04'
W = D / 'native-integrated'
F = D / 'native-decision'
changed_names = {'TreeDB/db/reader_registry_header_failure_v6_test.go', 'TreeDB/db/index_gen.go'}


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
assert {name for name in new if new[name] != old[name]} == changed_names
assert all(new[name] == sha(W / name) for name in changed_names)
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
    # Conservative local-package dependency closure, including the selected
    # package's tests and all platform Go files; dependency tests are excluded.
    pending = [(package, True)]
    closure = set()
    while pending:
        directory, include_tests = pending.pop()
        if directory in closure:
            continue
        closure.add(directory)
        files = list((W / directory).glob('*.go'))
        for file in files:
            if file.name.endswith('_test.go') and not include_tests:
                continue
            assert new[file.relative_to(W).as_posix()] == old[file.relative_to(W).as_posix()]
            text = file.read_text()
            for match in re.finditer(r'(?m)^import\s+(\([\s\S]*?^\)|[^\n]+)', text):
                for imported in re.findall(r'"([^"\n]+)"', match.group(1)):
                    prefix = 'github.com/snissn/gomap/'
                    if imported.startswith(prefix):
                        dependency = imported[len(prefix):]
                        assert (W / dependency).is_dir(), dependency
                        pending.append((dependency, False))
    assert 'TreeDB/db' not in closure
    assert all(str(Path(name).parent) not in closure for name in changed_names)
    checks.append({'name': command['name'], 'raw_sha256': sha(O / 'run.log'),
        'result_sha256': sha(O / 'result.json'), 'top': len(command['required_top_level_tests']),
        'child': command['expected_child_count'], 'groups_joined': True,
        'same_package_and_dependency_inputs': True, 'local_dependency_closure': sorted(closure)})
reuse = {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'status': 'ACCEPTED_UNCHANGED_INPUT_REUSE',
    'parent_manifest_sha256': sha(P / 'source-frozen.json'),
    'successor_manifest_sha256': sha(B / 'source-frozen.json'),
    'changed_paths_since_source02': sorted(changed_names), 'checks': checks,
    'historical_normal_counts': {'top': sum(c['top'] for c in checks), 'child': sum(c['child'] for c in checks)},
    'parent_compile_result': str(P / 'compile-online02/result.json'),
    'parent_compile_result_sha256': sha(P / 'compile-online02/result.json'),
    'compile_scope': 'Source02 root-module compile/default-vet evidence is retained only for unchanged inputs. Changed DB production and fixture are compiled and vetted by fresh selected DB/caching/public tests. No separate whole-source04 compile claim.',
    'all_affected_test_bodies_pending': True,
    'native_or_performance_qualification': False}
put(F / 'root-native-registry-api-source04-reuse.json', reuse)
plan = dict(prior)
plan['commands'] = [dict(c) for c in prior['commands'] if c['name'] not in reuse_names]
assert len(plan['commands']) == 8
for command in plan['commands']:
    command['env'] = dict(command['env'], TMPDIR=str(B / 'controls' / command['name'] / 'tmp'))
plan['scope'] = 'source04 eight-command focused continuation with explicit unchanged normal retainedalloc/lifecycle evidence reuse'
plan['source_manifest_sha256'] = sha(B / 'source-frozen.json')
plan['test_file_bindings'] = {name: new[name] for name in prior['test_file_bindings']}
plan['historical_reuse'] = str(F / 'root-native-registry-api-source04-reuse.json')
plan['historical_reuse_sha256'] = sha(F / 'root-native-registry-api-source04-reuse.json')
put(B / 'proposed-commands.json', plan)
print(json.dumps({'plan_sha256': sha(B / 'proposed-commands.json'), 'reuse_sha256': plan['historical_reuse_sha256'],
    'historical_normal': reuse['historical_normal_counts'], 'fresh_command_names': [c['name'] for c in plan['commands']]}, indent=2))
