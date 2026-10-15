import hashlib,json,subprocess,tarfile,datetime
from pathlib import Path
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');R=D/'m0-observation-runtime';A=R/'download/m0-observation-2588c-38824531924-1'
def digest(p):
 h=hashlib.sha256()
 with p.open('rb')as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
def bsha(b):return hashlib.sha256(b).hexdigest()
def j(n):return json.loads((A/n).read_text())
m=j('artifact-hashes.json');total=0
for p,h in m.items():
 path=A/p;assert path.is_file()and digest(path)==h,p;total+=path.stat().st_size
extras={str(p.relative_to(A))for p in A.rglob('*')if p.is_file()}-set(m)
assert extras=={'artifact-hashes.json','workflow-terminal.json'},extras
binding=j('bindings.json');assert binding['candidate']=='2588cd26780fe9ffd8c33797fc6a2bfdf2c00d49'and binding['harness_head']=='eae34350200668e1aae35d56f7f4c8ddfc2acb47'and binding['diagnostic_only']
source=j('source-pristine.json');assert source==j('source-before.json')==j('source-after.json')and j('source-equality.json')=={'equal':True}
def verify_git(w,head,expected):
 names=subprocess.check_output(['git','ls-tree','-r','--name-only',head],cwd=w,text=True).splitlines();assert set(names)==set(expected)
 p=subprocess.Popen(['git','cat-file','--batch'],cwd=w,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
 for name in names:
  p.stdin.write((head+':'+name+'\n').encode());p.stdin.flush();n=int(p.stdout.readline().split()[2]);b=p.stdout.read(n);assert p.stdout.read(1)==b'\n';assert bsha(b)==expected[name],name
 p.stdin.close();p.wait();assert p.returncode==0
 return len(names)
product_n=verify_git(D/'ordinary-repair',binding['candidate'],source);harness_n=verify_git(D/'maintenance-landed-harness',binding['harness_head'],j('harness-source.json'))
for archive,expected,prefix in [('pristine-source.tar.gz',source,''),('go1.26.9.linux-amd64.tar.gz',j('sdk-manifest.json'),'go/')]:
 actual={}
 with tarfile.open(A/archive,'r:gz')as t:
  for item in t:
   if item.isfile():
    assert item.name.startswith(prefix);name=item.name[len(prefix):];assert name not in actual;actual[name]=bsha(t.extractfile(item).read())
 assert actual==expected,(archive,set(actual)^set(expected));print('archive verified',archive,len(actual),flush=True)
assert digest(A/'go1.26.9.linux-amd64.tar.gz')==binding['go_archive_sha256']
sdk=j('sdk-manifest.json')
for source_name,bind_name in [('bin/go','go_sha256'),('pkg/tool/linux_amd64/compile','compiler_sha256'),('pkg/tool/linux_amd64/link','linker_sha256')]:assert sdk[source_name]==binding[bind_name]
assert source['TreeDB/db/vacuum_collection_bench_test.go']==binding['overlay_preimage_sha256'];assert source['scripts/mvcc_primary_cost_gate.py']==binding['observer_source_sha256']
assert digest(A/'vacuum_collection_bench_test.go')==binding['overlay_source_sha256']
assert digest(A/'harness/mvcc_m0_observation_gc.go.txt')==binding['overlay_helper_sha256'];assert digest(A/'harness/mvcc_m0_observation_exec.py')==binding['exec_wrapper_sha256']
commands=j('commands.json');assert len(commands)==37 and len({x['label']for x in commands})==37
for c in commands:
 assert c['exit']==0 and c['joined']and not c['group_killed']and c['pgid']==c['pid'];assert digest(A/c['output'])==c['output_sha256'];assert c['elapsed_s']<c['timeout_s']
schedule=binding['schedule'];labels=[label for label,_ in schedule];actual=[c for c in commands if c['label']in labels];assert [c['label']for c in actual]==labels and len(actual)==22
exec_hashes={}
for c,(label,args)in zip(actual,schedule):
 assert c['argv'][-len(args):]==args and c['argv'][:4]==['taskset','-c','0,1',c['argv'][3]]
 assert c['argv'][4:7]==['test','-overlay',c['argv'][6]]and '-exec'in c['argv']
 e=j('executables/'+label+'/exec.json');h=digest(A/'executables'/label/'actual.test');assert e['sha256']==h and e['wrapper_sha256']==binding['exec_wrapper_sha256']and e['pgid']==c['pgid'];exec_hashes[label]=h
 ops=A/(label+'-operations.json')
 if label.startswith('legacy-'):
  x=json.loads(ops.read_text());assert len(x['Operations'])==4096 and x['WallEnd']>x['WallStart']
  for i,o in enumerate(x['Operations']):assert o['ordinal']==i and o['type']==('range'if i%2 else'point')and isinstance(o['duration_ns'],int)and o['duration_ns']>=0
results=j('results.json');assert all(results['gates'].values())and results['public_status']=='production-index-vacuum-available'
assert not j('quiet-preflight.json')['busy']and not j('observations-summary.json')['busy']
run=json.loads((R/'run-final.json').read_text());assert run['conclusion']=='success'and run['run_attempt']==1 and run['head_sha']==binding['harness_head']
receipt={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'run':38824531924,'head':binding['harness_head'],'candidate':binding['candidate'],'verified_artifact_hashes':len(m),'verified_artifact_bytes':total,'extra_files_bound':{n:digest(A/n)for n in sorted(extras)},'product_git_and_archive_files':product_n,'harness_git_files':harness_n,'official_sdk_files':len(sdk),'all_37_command_output_hashes_and_success_exits':True,'original_22_fixture_and_benchmark_invocations':True,'benchmark_calls':20,'legacy_operations_each':4096,'actual_exec_hashes':exec_hashes,'unique_actual_exec_hashes':len(set(exec_hashes.values())),'source_equality':True,'quiet_preflight_and_observation_busy_empty':True,'summary_gates':results['gates'],'diagnostic_only':True,'original_M0_CI_gate_replaced':False,'normal_exit_join_limit':'All37 command leaders waited and22 exec wrappers exec same process. Normal-return harness does not independently enumerate/reap adopted descendants or record group-emptiness; do not claim that stronger evidence. Timeout cleanup is unused.','github_archive_digest':'Metadata retained, ZIP digest not independently verified because gh downloaded and extracted; every124 internal artifact hashes independently verified.'}
(R/'packet-verification.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps({k:v for k,v in receipt.items()if k!='actual_exec_hashes'},indent=2))
