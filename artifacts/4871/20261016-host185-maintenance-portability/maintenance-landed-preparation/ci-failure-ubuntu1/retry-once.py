import datetime, json, subprocess
from pathlib import Path

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
A = D / 'maintenance-landed-preparation/ci-failure-ubuntu1'
H = 'a1db58c790e056a02340c98e7dbae11f40c8ed48'
assert (A / 'classification.json').is_file()
assert not (A / 'retry-request.json').exists()

def api(ep):
    return json.loads(subprocess.check_output(['gh', 'api', ep], text=True))

def save(name, obj):
    (A / name).write_text(json.dumps(obj, indent=2) + '\n')
    return obj

pr = save('retry-pr-before.json', api('repos/snissn/gomap/pulls/5137'))
run = save('attempt1-run-final.json', api('repos/snissn/gomap/actions/runs/38825099216'))
jobs = save('attempt1-jobs-final.json', api('repos/snissn/gomap/actions/runs/38825099216/jobs?per_page=100'))
assert pr['head']['sha'] == H and run['head_sha'] == H
assert run['run_attempt'] == 1 and run['status'] == 'completed' and run['conclusion'] == 'failure'
assert jobs['total_count'] == len(jobs['jobs'])
failed = [j for j in jobs['jobs'] if j['conclusion'] != 'success']
assert {(j['name'], j['conclusion']) for j in failed} == {('ubuntu-latest-1', 'failure'), ('TreeDB required gate', 'failure')}
job = next(j for j in failed if j['name'] == 'ubuntu-latest-1')
assert job['id'] == 116730604187
checks = subprocess.run(['gh', 'pr', 'checks', '5137', '--repo', 'snissn/gomap', '--json', 'name,state,link,bucket,workflow'], capture_output=True, text=True)
inventory = save('attempt1-checks-final.json', json.loads(checks.stdout))
assert checks.returncode in (0, 1) and len(inventory) == 50
assert sum(x['state'] == 'SUCCESS' for x in inventory) == 48
assert {x['name'] for x in inventory if x['state'] != 'SUCCESS'} == {'ubuntu-latest-1', 'TreeDB required gate'}
command = ['gh', 'run', 'rerun', '38825099216', '--repo', 'snissn/gomap', '--job', str(job['id'])]
save('retry-request.json', {'at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                          'head': H, 'command': command, 'maximum_retries': 1,
                          'scope': 'one failed CI shard and dependent required gate on unchanged reviewed harness head',
                          'performance_rerun': False, 'classification': 'classification.json'})
r = subprocess.run(command, capture_output=True, text=True)
save('retry-command-result.json', {'exit_code': r.returncode, 'stdout': r.stdout, 'stderr': r.stderr})
assert r.returncode == 0, r.stderr
after = save('retry-run-after-request.json', api('repos/snissn/gomap/actions/runs/38825099216'))
assert after['head_sha'] == H and after['run_attempt'] in (1, 2)
print(json.dumps({'head': H, 'run': after['id'], 'attempt': after['run_attempt'], 'status': after['status'], 'failed_job': job['id'], 'retry_requests': 1}))
