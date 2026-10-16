"""Root-only dispatch; requires the separately completed merge and final gate."""
import datetime
import hashlib
import json
import subprocess
from pathlib import Path

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
A = D / 'maintenance-python-portability'
W = D / 'maintenance-landed-harness'
R = D / 'maintenance-portability-runtime'
H = '8db93fec7176d23bad95168954884da807ee1da4'
BASE = '5bca87ffe6e1bfba2d693264d2dc82936adc234f'
CANDIDATE = 'a17febafb5353897c59a4e17f992e34c47a2585b'
WORKFLOW = 'treedb-mvcc-maintenance-diagnostic.yml'

def run(argv, cwd=W):
    return subprocess.check_output(argv, cwd=cwd, text=True)

def api(endpoint):
    return json.loads(run(['gh', 'api', endpoint]))

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def save(name, value):
    (R / name).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    return value

# No request is permitted without the final exact-head acceptance and merge.
gate = json.loads((A / 'final-gate/acceptance.json').read_text())
assert gate['head'] == H and gate['checks_success'] == 50
assert gate['review_disposition'] == 'ACCEPT' and gate['unresolved_threads'] == 0
pr = api('repos/snissn/gomap/pulls/5138')
assert pr['merged'] and pr['state'] == 'closed' and pr['head']['sha'] == H
merged = pr['merge_commit_sha']
assert api('repos/snissn/gomap/git/ref/heads/main')['object']['sha'] == merged
commit = api('repos/snissn/gomap/git/commits/' + merged)
assert [p['sha'] for p in commit['parents']] == [gate['base'], H]
assert commit['tree']['sha'] == gate['tree']
assert not R.exists(), 'A prior grant/attempt must never be overwritten or retried.'
R.mkdir()
save('merged-pr.json', pr)
save('merged-commit.json', commit)
save('acceptance.json', gate)

sources = {}
for label, root, head in [('harness', W, H), ('baseline', D / 'coordinator', BASE),
                           ('candidate', D / 'ordinary-repair', CANDIDATE)]:
    assert run(['git', 'rev-parse', 'HEAD'], root).strip() == head
    assert not run(['git', 'status', '--porcelain'], root).strip()
    files = run(['git', 'ls-files', '-z'], root).split('\0')
    manifest = {f: digest(root / f) for f in files if f}
    save(label + '-source-manifest.json', manifest)
    tree_raw = run(['git', 'ls-tree', '-r', '--full-tree', head], root)
    (R / (label + '-git-tree.txt')).write_text(tree_raw)
    sources[label] = {'source_head': head, 'tree': run(['git', 'rev-parse', head + '^{tree}'], root).strip(),
                      'files': len(manifest), 'manifest_sha256': digest(R / (label + '-source-manifest.json')),
                      'git_tree_sha256': digest(R / (label + '-git-tree.txt'))}
assert sources['harness']['tree'] == commit['tree']['sha']
assert json.loads((R / 'harness-source-manifest.json').read_text()) == json.loads((A / 'source-final.json').read_text())
candidate_binding = json.loads((D / 'ordinary-diagnostic-packing/commit-binding.json').read_text())
assert sources['candidate']['tree'] == candidate_binding['tree']
assert sources['candidate']['files'] == candidate_binding['all_source_manifest_files_unchanged']
assert digest(D / 'ordinary-diagnostic-packing/source-before.json') == candidate_binding['source_manifest_sha256']
assert json.loads((R / 'candidate-source-manifest.json').read_text()) == json.loads((D / 'ordinary-diagnostic-packing/source-before.json').read_text())
review = json.loads((D / 'ordinary-diagnostic-packing/review.json').read_text())
assert review['disposition'] == 'ACCEPT'
source_remote = run(['git', 'ls-remote', 'origin', 'refs/heads/codex/5111-diagnostic-packing-185-20261015'], D / 'ordinary-repair').split()[0]
assert source_remote == CANDIDATE
workflow = save('workflow.json', api('repos/snissn/gomap/actions/workflows/' + WORKFLOW))
assert workflow['state'] == 'active'
before = save('runs-before.json', api('repos/snissn/gomap/actions/workflows/' + WORKFLOW + '/runs?event=workflow_dispatch&per_page=100'))
assert before['total_count'] == 1 and len(before['workflow_runs']) == 1
prior = before['workflow_runs'][0]
assert prior['id'] == 38830037922 and prior['run_attempt'] == 1 and prior['conclusion'] == 'failure'
assert prior['head_sha'] == '9165c53e4d82d12231af8404b0b7c5aea13530a6'
prior_receipt = json.loads((D / 'maintenance-landed-runtime/download-receipt.json').read_text())
prior_classification = json.loads((A / 'failure-classification.json').read_text())
assert prior_receipt['archive_digest_verified']
assert prior_receipt['sha256'] == 'cf87d6565165ef26d31a67a1c8f59b16ed962422e1dbac20e606d96af8139018'
save('prior-failed-dispatch.json', {'run': prior, 'classification': prior_classification,
                                  'download_receipt': prior_receipt,
                                  'disposition': 'Original single attempt consumed; zero builds/benchmark calls. Fresh grant follows exact one-file source repair and reviewed merge; not an unchanged retry.'})
save('grant.json', {
    'granted_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'authority': 'Original user explicitly authorized sole-coordinator execution and merge of graph 4871; root grants this one frozen bounded diagnostic.',
    'runtime_authorized': True, 'maximum_dispatches': 1, 'maximum_attempts': 1,
    'harness_head': merged, 'reviewed_harness_parent': H, 'harness_tree': commit['tree']['sha'],
    'sources': sources, 'baseline': BASE, 'candidate': CANDIDATE,
    'workflow': WORKFLOW, 'ref': 'main', 'maximum_benchmark_processes': 14,
    'schedule': 'Checkpoint16x three AB/BA pairs; production Vacuum1x three AB/BA pairs; candidate cohort8/16 each128x. Existing unchanged benchmark bodies, count1, run-empty, no overlays.',
    'environment': {'go': '1.26.3', 'go_sha256': 'd68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65',
                    'affinity': [0, 1], 'GOMAXPROCS': 2, 'GOMEMLIMIT': '8GiB', 'GOGC': 100},
    'limits': {'job_minutes': 120, 'build_seconds_each': 600, 'go_child_seconds': 120,
               'outer_benchmark_seconds': 150, 'cleanup_only_seconds': 2,
               'quiet_preflight_seconds': 10, 'external_process_percent': 2, 'sample_ms': 250},
    'failure_policy': 'Busy preflight refuses zero benchmark calls; busy measurement is contaminated. Preserve all attempted raws, failures, source/SDK/ELF archives and group-join receipts. No rerun or selective sample replacement.',
    'infrastructure': [
        'INFRASTRUCTURE_UNAVAILABLE: runner: no dedicated quiet persistent runner is available for this scope; use ephemeral ubuntu-24.04 and retain actual host observations.',
        'INFRASTRUCTURE_UNAVAILABLE: cache: no persistent build cache; fresh task caches, possible hosted setup-go toolcache, exact SDK hash mandatory.',
        'INFRASTRUCTURE_UNAVAILABLE: storage: hosted artifacts expire after14days; root will retain the complete downloaded packet and publish durable evidence bindings.'],
    'prior_failed_attempt': {'run_id': 38830037922, 'head': '9165c53e4d82d12231af8404b0b7c5aea13530a6', 'benchmark_calls': 0, 'repair_pr': 5138},
    'qualification': 'Diagnostic only. This grant does not accept ordinary cost, failed original M0, native Q32/ACK0, sustained performance or graph completion.',
    'gate_sha256': digest(A / 'final-gate/acceptance.json'),
    'harness_review_sha256': digest(A / 'independent-review.json'),
    'product_review_sha256': digest(D / 'ordinary-diagnostic-packing/review.json'),
})
# Check main immediately before the one request; returned run must match it.
assert api('repos/snissn/gomap/git/ref/heads/main')['object']['sha'] == merged
request = save('dispatch-request.json', {'ref': 'main'})
started = datetime.datetime.now(datetime.timezone.utc).isoformat()
result = subprocess.run(['gh', 'api', '-X', 'POST', 'repos/snissn/gomap/actions/workflows/' + WORKFLOW + '/dispatches',
                         '--input', str(R / 'dispatch-request.json')], cwd=W, capture_output=True, text=True)
save('dispatch-receipt.json', {'started_utc': started, 'completed_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                             'exit': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr,
                             'grant_sha256': digest(R / 'grant.json'), 'request_count': 1, 'retry_allowed': False})
assert result.returncode == 0, 'Dispatch result uncertain/failed: inspect; never repeat automatically.'
print(json.dumps({'dispatched_once': True, 'harness_head': merged, 'candidate': CANDIDATE, 'grant_sha256': digest(R / 'grant.json')}))
