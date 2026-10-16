import argparse
import datetime
import hashlib
import json
from pathlib import Path
import re

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
p = argparse.ArgumentParser()
p.add_argument('--packet', type=Path, required=True)
p.add_argument('--receipt', type=Path, required=True)
a = p.parse_args()
B = a.packet.resolve()
O = B / 'controls'
assert B.is_relative_to(D / 'native-main-integration')
assert a.receipt.resolve().is_relative_to(D / 'native-decision')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_text())


summary = read(O / 'summary.json')
manifest = read(B / 'source-frozen.json')
plan = read(B / 'proposed-commands.json')
commands = plan['commands']
n = summary['commands_executed']
assert [c['name'] for c in commands[:n]] == [r['name'] for r in summary['results']]
assert [c['name'] for c in commands[n:]] == summary['commands_not_executed']
assert summary['all_groups_empty'] and summary['source_still_frozen'] and summary['git_identity_still_frozen']
checks = []
for command in commands[:n]:
    path = O / command['name']
    result = read(path / 'result.json')
    raw = (path / 'run.log').read_text()
    runs = re.findall(r'^=== RUN\s+(\S+)', raw, re.M)
    passes = re.findall(r'^\s*--- PASS: (\S+)', raw, re.M)
    fails = re.findall(r'^\s*--- FAIL: (\S+)', raw, re.M)
    skips = re.findall(r'^\s*--- SKIP: (\S+)', raw, re.M)
    expected = command['required_top_level_tests'] + [parent + '/' + child
        for parent, children in command['required_child_tests'].items() for child in children]
    assert len(set(runs)) == len(runs)
    assert runs == result['test_RUN_names'] and passes == result['test_PASS_names']
    assert fails == result['test_FAIL_names'] and skips == result['test_SKIP_names']
    assert result['raw_log_sha256'] == sha(path / 'run.log')
    assert read(path / 'source-before.json') == read(path / 'source-after.json') == manifest
    assert result['leader_joined'] and result['group_and_children_empty']
    assert not result['remaining_group_members'] and not result['remaining_direct_or_adopted_children']
    assert not result['residual_observed_before_cleanup'] and not result['signals']
    accepted = (result['exit_code'] == 0 and sorted(runs) == sorted(passes) == sorted(expected)
        and not fails and not skips and not result['timeout']
        and not any(token in raw for token in ('WARNING: DATA RACE', 'panic:', 'goleak:')))
    assert accepted == (result['status'] == 'PASS')
    assert accepted or command['name'] == commands[n - 1]['name']
    if accepted:
        assert not result['stop_reasons']
    checks.append({'name': command['name'], 'accepted': accepted,
        'raw_sha256': sha(path / 'run.log'), 'result_sha256': sha(path / 'result.json'),
        'top_pass': len([name for name in passes if '/' not in name]),
        'child_pass': len([name for name in passes if '/' in name]),
        'missing_RUN': sorted(set(expected) - set(runs)), 'unexpected_RUN': sorted(set(runs) - set(expected)),
        'FAIL': fails, 'SKIP': skips, 'groups_empty': True, 'source_equal': True})
accepted = n == len(commands) and all(row['accepted'] for row in checks)
assert summary['all_authorized_pass'] == accepted
counts = {mode: {'top': sum(row['top_pass'] for row in checks if row['name'].startswith(mode + '-')),
    'child': sum(row['child_pass'] for row in checks if row['name'].startswith(mode + '-'))}
    for mode in ('normal', 'race')}
receipt = {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'accepted': accepted, 'scope': 'frozen checked registry API focused normal/race raw recomputation',
    'manifest_sha256': sha(B / 'source-frozen.json'), 'plan_sha256': sha(B / 'proposed-commands.json'),
    'summary_sha256': sha(O / 'summary.json'), 'checks': checks, 'counts': counts,
    'commands_not_executed': summary['commands_not_executed'],
    'all_process_groups_joined': True, 'all_source_snapshots_equal': True,
    'native_publication_qualified': False, 'Q32': 'original gate remains open',
    'performance_and_graph_qualification': 'OPEN'}
with a.receipt.open('x') as out:
    json.dump(receipt, out, indent=2, sort_keys=True)
    out.write('\n')
print(json.dumps({'receipt': str(a.receipt), 'sha256': sha(a.receipt),
    'accepted': accepted, 'counts': counts}, indent=2))
