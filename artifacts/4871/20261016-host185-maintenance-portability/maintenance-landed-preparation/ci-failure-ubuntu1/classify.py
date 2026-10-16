import datetime, hashlib, json, subprocess
from pathlib import Path

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
W = D / 'maintenance-landed-harness'
A = D / 'maintenance-landed-preparation/ci-failure-ubuntu1'
BASE = 'eae34350200668e1aae35d56f7f4c8ddfc2acb47'
HEAD = 'a1db58c790e056a02340c98e7dbae11f40c8ed48'

def git(*args):
    return subprocess.check_output(['git', *args], cwd=W)

def tree(ref):
    rows = git('ls-tree', '-r', '-z', ref).split(b'\0')
    return {p.decode(): meta.decode() for row in rows if row for meta, p in [row.split(b'\t', 1)]}

assert git('rev-parse', 'HEAD').decode().strip() == HEAD
assert not git('status', '--porcelain').strip()
b, h = tree(BASE), tree(HEAD)
changed = sorted(p for p in b.keys() | h.keys() if b.get(p) != h.get(p))
review = json.loads((D / 'maintenance-landed-preparation/review.json').read_text())
assert changed == sorted(review['exact_staged_paths'])
assert len(changed) == 9 and not any(p.endswith('.go') for p in changed)
unchanged_go = sorted(p for p in b if p.endswith('.go'))
assert all(b[p] == h[p] for p in unchanged_go)
old_workflows = sorted(p for p in b if p.startswith('.github/workflows/'))
assert all(b[p] == h[p] for p in old_workflows)
assert b['go.mod'] == h['go.mod'] and b['go.sum'] == h['go.sum']
prior = json.loads((D / 'm0-observation-preparation/final-merge-gate/checks.json').read_text())
assert len(prior) == 50 and all(x['state'] == 'SUCCESS' for x in prior)
prior_head = '939b10c22e54caf56dc84553b65e2435862052fb'
assert git('rev-parse', prior_head + '^{tree}') == git('rev-parse', BASE + '^{tree}')
events = []
for line in (A / 'job.log').read_text().splitlines():
    start = line.find('{"Time"')
    if start < 0:
        continue
    try:
        e = json.loads(line[start:])
    except json.JSONDecodeError:
        continue
    if e.get('Action') == 'fail':
        events.append(e)
tests = [e for e in events if e.get('Test')]
assert len(tests) == 1 and tests[0]['Test'] == 'TestImmutableOwnerReplacementPrivateANNQualificationV1'
result = {
    'at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'head': HEAD, 'base': BASE, 'changed_paths': changed,
    'unchanged_go_files': len(unchanged_go), 'unchanged_existing_workflows': old_workflows,
    'unchanged_go_mod_sum': True, 'prior_identical_base_tree': prior_head,
    'prior_check_count_success': len(prior),
    'failed_test_events': tests, 'failed_package_events': [e for e in events if not e.get('Test')],
    'log_sha256': hashlib.sha256((A / 'job.log').read_bytes()).hexdigest(),
    'classification': 'Unrelated existing nativewire restart transport failure; exact EOF mechanism unproven. No evidence of harness-caused product regression.',
    'source_evidence': [
        'All cold/cached warm and four ANN samples completed before failure.',
        'Qualification fixture closes original quorum runtime, reopens it, then closes/reopens target; the later replacement-prepare at source_v1_test.go:438 uses the existing client.',
        'The failure URL is replacement-prepare, after the logged samples. Initial preparation precedes all samples and succeeded.',
        'Client call sends one HTTPS POST through its retained http client; EOF is returned directly. Server Close shuts down listeners and HTTP server. The log does not establish why this specific connection ended.',
        'The new diagnostic workflow is manual main-only and cannot execute in this PR CI event.',
        'Only CI inventory/test cardinality/docs plus six new manual harness paths differ; all Go inputs and existing workflow bytes equal the earlier 50-pass tree.'
    ],
    'decision': 'After current run completes and all failures are inventoried, allow one retry of the failed job plus dependent required aggregate on the same head. Preserve attempt1. Do not skip the gate, weaken tests, change product, rerun performance or infer success before exact-head checks pass.',
    'retry_executed': False
}
(A / 'classification.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({'head': HEAD, 'changed_paths': len(changed), 'unchanged_go_files': len(unchanged_go), 'prior_checks': len(prior), 'failures': len(tests)}))
