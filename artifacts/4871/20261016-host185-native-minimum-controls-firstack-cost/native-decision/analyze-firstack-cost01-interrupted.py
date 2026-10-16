import hashlib
import json
import re
from pathlib import Path

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
B = D / 'native-main-integration/native-firstack-cost-source01'
P = B / 'samples-frozen01/firstack-sample01'

def read(p):
    return json.loads(p.read_text())

def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

raw = (P / 'run.log').read_text()
result = read(P / 'result.json')
summary = read(B / 'runtime-stages/samples-frozen01/summary.json')
assert result['executed'] and result['exit_code'] == 0
assert result['status'] == 'STOP'
assert result['reasons'] == ['competing CPU during sample; retain contaminated sample']
assert result['source_before_after_frozen'] and result['leader_joined']
assert not result['interventions'] and not result['remaining_group_or_adopted_children']
assert result['raw_sha256'] == sha(P / 'run.log')
assert summary['unexecuted_commands'] == ['firstack-sample02', 'firstack-sample03']
expected = read(B / 'source-frozen.json')
assert read(P / 'source-before.json') == read(P / 'source-after.json') == expected
totals = re.findall(r'first-ACK wall-only totals publicCallNanos=(\d+) wholeBirthToACKNanos=(\d+) maxCallNanos=(\d+) publicCalls=(\d+) workRecords=(\d+) workBytes=(\d+) exactACKReturns=(\d+)', raw)
assert len(totals) == 1
total = dict(zip(('public_call_nanos', 'whole_birth_to_ack_nanos', 'max_call_nanos', 'public_calls', 'work_records', 'work_bytes', 'exact_ack_returns'), map(int, totals[0])))
rows = re.findall(r'first-ACK call=(\d+) (start=cursor-birth|start-phase=\d+) wallNanos=(\d+) workRecords=(\d+) workBytes=(\d+)', raw)
calls = [dict(index=int(i), starting_label=label, wall_nanos=int(ns), work_records=int(r), work_bytes=int(b)) for i, label, ns, r, b in rows]
assert [c['index'] for c in calls] == list(range(total['public_calls']))
assert sum(c['wall_nanos'] for c in calls) == total['public_call_nanos']
assert max(c['wall_nanos'] for c in calls) == total['max_call_nanos']
assert sum(c['work_records'] for c in calls) == total['work_records']
assert sum(c['work_bytes'] for c in calls) == total['work_bytes']
assert total['exact_ack_returns'] == 1 and total['whole_birth_to_ack_nanos'] >= total['public_call_nanos']
assert re.findall(r'^BenchmarkNativeSelectedOriginalFirstACKCostV6-\d+\s+1\s+(\d+) ns/op', raw, re.M) == [str(total['public_call_nanos'])]
out = {
    'disposition': 'INTERRUPTED_CONTAMINATED_SAMPLE_RETAINED_NO_QUIET_BASELINE',
    'planned_samples': 3, 'samples_executed': 1, 'samples_unexecuted': [2, 3],
    'automatic_retries': 0, 'replacement_samples': 0,
    'manifest_sha256': sha(B / 'source-frozen.json'), 'source_files': len(expected),
    'elf_sha256': result['elf_sha256'], 'raw_sha256': sha(P / 'run.log'),
    'all_source_bytes_before_after_verified': True,
    'raw_totals_independently_recomputed': total, 'calls': calls,
    'intercall_and_observer_nanos': total['whole_birth_to_ack_nanos'] - total['public_call_nanos'],
    'host_activity': read(P / 'host-window.json')['activity'],
    'limits': [
        'Whole process CPU competitor threshold exceeded after both preflight windows passed.',
        'This result is a contaminated diagnostic observation, not an accepted latency baseline or comparison.',
        'Labels are starting phases; a call can span phases, so timings are not standalone phase costs.',
        'Meter bookkeeping between public calls influences later state; this is not an uninstrumented benchmark.',
        'No allocation, full prune, continuing writer, Q32, or product qualification claim.',
        'Unexecuted samples 2 and 3 require explicit revised collection plan; sample 1 will not be discarded or replaced.',
    ],
    'Q32': 'original RED/ACK0 unchanged',
}
target = D / 'native-decision/root-native-firstack-cost01-interrupted-analysis.json'
assert not target.exists()
target.write_text(json.dumps(out, indent=2) + '\n')
print(json.dumps({'path': str(target), 'sha256': sha(target), 'totals': total}))
