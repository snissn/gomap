import datetime, hashlib, json, subprocess
from pathlib import Path

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
A = D / 'ordinary-main-integration'
W = D / 'ordinary-repair'
H = '6d3fc3a6e3582810d7c80fc46b784a9b30fada80'
O = A / 'final-gate'
O.mkdir(exist_ok=False)

def run(args):
    return subprocess.check_output(args, cwd=W, text=True)

def save(name, value):
    (O / name).write_text(json.dumps(value, indent=2) + '\n')
    return value

def api(endpoint):
    return json.loads(run(['gh', 'api', endpoint]))

before = save('pr-before.json', api('repos/snissn/gomap/pulls/5127'))
assert before['head']['sha'] == H and before['state'] == 'open'
checks_run = subprocess.run(['gh', 'pr', 'checks', '5127', '--repo', 'snissn/gomap', '--json', 'name,state,link,bucket,workflow'], capture_output=True, text=True)
checks = save('checks.json', json.loads(checks_run.stdout))
assert checks_run.returncode == 0 and len(checks) == 56 and all(c['state'] == 'SUCCESS' for c in checks)
assert run(['git', 'rev-parse', 'HEAD']).strip() == H
assert not run(['git', 'status', '--porcelain']).strip()
assert run(['git', 'ls-remote', 'origin', 'refs/heads/codex/5111-ordinary-data-primary-publication']).split()[0] == H
manifest_path = A / 'source-final.json'
manifest = json.loads(manifest_path.read_text())
assert len(manifest) == 10258
for path, digest in manifest.items():
    assert hashlib.sha256((W / path).read_bytes()).hexdigest() == digest, path
review = json.loads((A / 'final-current-head-review.json').read_text())
assert review['disposition'] == 'ACCEPT' and not review['blocking_findings']
assert review['head'] == H and review['actual_model'] == 'gpt-6.1-sol'
m0 = json.loads((A / 'm0-current/verification.json').read_text())
assert m0['head'] == H and m0['attempt'] == 1 and m0['all_raw_metric_summaries_recomputed'] and all(m0['gates'].values())
performance = json.loads((A / 'raw-primary-current/verification.json').read_text())
assert performance['accepted'] and performance['head'] == H and performance['base'] == before['base']['sha']
cost = json.loads((D / 'maintenance-portability-runtime/root-disposition.json').read_text())
assert cost['candidate'] == 'a17febafb5353897c59a4e17f992e34c47a2585b' and cost['disposition'].startswith('ACCEPT ')
assert hashlib.sha256((D / 'maintenance-portability-runtime/root-disposition.md').read_bytes()).hexdigest() == cost['root_disposition_sha256']
classifier = subprocess.run(['python3', str(D / 'skills/github-pr-mergeable/scripts/codex_review_gate.py'), '--repo', 'snissn/gomap', '--pr', '5127', '--check'], capture_output=True, text=True)
(O / 'classifier-stderr.txt').write_text(classifier.stderr)
classification = save('classifier.json', json.loads(classifier.stdout))
assert classifier.returncode in (0, 2) and classification['head'] == H
assert classification['state'] in ('not_requested', 'clean') and not classification['unresolved_codex_threads']
query = 'query { repository(owner:"snissn",name:"gomap") { pullRequest(number:5127) { headRefOid reviewDecision mergeable mergeStateStatus reviewThreads(first:100) { pageInfo { hasNextPage } nodes { id isResolved path comments(first:20) { nodes { author { login } body url } } } } reviews(first:100) { pageInfo { hasNextPage } nodes { state body commit { oid } author { login } } } } } }'
gql = save('reviews-and-threads.json', json.loads(run(['gh', 'api', 'graphql', '-f', 'query=' + query])))
pr = gql['data']['repository']['pullRequest']
assert pr['headRefOid'] == H and pr['mergeable'] == 'MERGEABLE'
assert not pr['reviewThreads']['pageInfo']['hasNextPage'] and not pr['reviews']['pageInfo']['hasNextPage']
assert all(t['isResolved'] for t in pr['reviewThreads']['nodes'])
assert not any(r['state'] == 'CHANGES_REQUESTED' for r in pr['reviews']['nodes'])
rules = save('main-rules.json', api('repos/snissn/gomap/rules/branches/main'))
after = save('pr-after.json', api('repos/snissn/gomap/pulls/5127'))
assert after['head']['sha'] == H and after['base']['sha'] == before['base']['sha']
save('acceptance.json', {
    'at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'head': H, 'tree': run(['git', 'rev-parse', 'HEAD^{tree}']).strip(),
    'base': after['base']['sha'], 'checks_success': len(checks),
    'unresolved_threads': 0, 'review_mode': 'INTERNAL_SUBAGENT_REVIEW',
    'review_model': 'gpt-6.1-sol', 'review_disposition': 'ACCEPT',
    'hosted_unavailability': review['hosted_review_unavailability'],
    'review_json_sha256': hashlib.sha256((A / 'final-current-head-review.json').read_bytes()).hexdigest(),
    'source_manifest_sha256': hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
    'source_files': len(manifest), 'runtime_calls_in_final_gate': 0,
    'original_m0_verification_sha256': hashlib.sha256((A / 'm0-current/verification.json').read_bytes()).hexdigest(),
    'raw_primary_verification_sha256': hashlib.sha256((A / 'raw-primary-current/verification.json').read_bytes()).hexdigest(),
    'maintenance_cost_disposition_sha256': hashlib.sha256((D / 'maintenance-portability-runtime/root-disposition.json').read_bytes()).hexdigest(),
    'authorization': 'User explicitly authorized sole-coordinator execute-and-merge for graph 4871; no local human approval restriction.'
})
print(json.dumps({'head': H, 'checks': len(checks), 'review': 'ACCEPT', 'unresolved_threads': 0}))
