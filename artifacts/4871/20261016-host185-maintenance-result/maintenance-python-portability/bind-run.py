import datetime,json,subprocess
from pathlib import Path
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');R=D/'maintenance-portability-runtime'
def read(n):return json.loads((R/n).read_text())
def save(n,x):(R/n).write_text(json.dumps(x,indent=2)+'\n');return x
def api(e):return json.loads(subprocess.check_output(['gh','api',e],text=True))
assert not (R/'run-binding.json').exists()
grant,receipt,before=read('grant.json'),read('dispatch-receipt.json'),read('runs-before.json')
assert receipt['exit']==0 and receipt['request_count']==1 and not receipt['retry_allowed']
runs=save('runs-after-dispatch.json',api('repos/snissn/gomap/actions/workflows/'+grant['workflow']+'/runs?event=workflow_dispatch&per_page=100'))
oldids={r['id']for r in before['workflow_runs']};new=[r for r in runs['workflow_runs']if r['id']not in oldids]
assert len(new)==1,'Inspect new-run inventory; never repeat dispatch.'
r=new[0];assert r['head_sha']==grant['harness_head'] and r['run_attempt']==1 and r['event']=='workflow_dispatch' and r['head_branch']=='main'
x={'at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'run_id':r['id'],'url':r['html_url'],'head':r['head_sha'],'ref':r['head_branch'],'attempt':r['run_attempt'],'event':r['event'],'created_at':r['created_at'],'status':r['status'],'one_new_dispatch':True,'prior_failed_run':38830037922,'retries_allowed':False}
save('run-binding.json',x);save('run-01.json',api(f"repos/snissn/gomap/actions/runs/{r['id']}"));save('jobs-01.json',api(f"repos/snissn/gomap/actions/runs/{r['id']}/jobs?per_page=100"));print(json.dumps(x))
