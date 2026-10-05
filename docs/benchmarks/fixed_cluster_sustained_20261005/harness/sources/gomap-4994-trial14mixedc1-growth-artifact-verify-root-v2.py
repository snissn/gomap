import base64,collections,copy,hashlib,json,math,struct,tarfile
from pathlib import Path
V=Path('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1');T=Path('/tmp');P=Path('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/growth-query-root-v2');L=Path('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/growth-lifecycle-root-v2');B=Path('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/bootstrap-root-v1');I=Path('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/pre-inputs-root-v1')
def sha(x):return hashlib.sha256(x).hexdigest()
def load(p):return json.loads(p.read_bytes())
# Reuse the already reviewed exact-plan assertions without its output mutation.
assert sha((T/'gomap-4994-trial14mixedc1-growth-exact-validate-root-v2.py').read_bytes())=='d9cd3dfbbbc76badf18e77740fb7d0763d66be03b1324402b56f24c4ea136af6'
code=(T/'gomap-4994-trial14mixedc1-growth-exact-validate-root-v2.py').read_text().split("proof={'status'")[0].replace("gomap-4994-rf4trial14mixedc1-growth-query-root-v2","gomap-4994-rf4trial14mixedc1-growth-query-root-v2")
exec(compile(code,'retained-exact-plan-assertions','exec'))
assert r['RunID']==plan['RunID']=='rf4trial14mixedc1query01'
assert r['Generation']==dict(Index='embedding_graph',Generation=1)
assert max(o['EndNS'] for o in ops)<=r['Timeout']
for old,new in zip(plan['Operations'],ops):
 req=old['InsertRequest'] or old['SearchRequest'];assert req['Deadline']=='0001-01-01T00:00:00Z'
 assert sha(json.dumps(req,separators=(',',':'),ensure_ascii=False).encode())==old['RequestSHA256']
 assert (new['InsertRequest'] or new['SearchRequest'])['Deadline']!=req['Deadline']
for i,o in enumerate(inserts[:-1]):
 req=o['InsertRequest'];assert base64.b64decode(req['ID']).decode()==o['ExpectedID']==f'rf4trial14mixedc1query01-doc-{i:02d}'
 assert base64.b64decode(req['IdempotencyKey']).decode()==f'rf4trial14mixedc1query01/insert/{i:02d}'
 v=req['Vector'];angle=math.pi/2+(i+1)*math.pi/(2*2)
 assert struct.pack('<128f',*v)==struct.pack('<128f',math.cos(angle),math.sin(angle),*([0]*126))
 doc=json.loads(base64.b64decode(req['Document']));assert doc==dict(embedding=v,kind='query-under-write')
 assert req['Version']==1 and req['Generation']==r['Generation']
 assert o['InsertResponse']['PartitionID']==0
for o in searches:
 req=o['SearchRequest'];z=o['SearchResponse'];c=z['Counters']
 assert req['VisibilityToken'] is None and req['Version']==1 and req['Generation']==r['Generation'] and req['Metric']=='cosine' and req['TopK']==1 and req['Probes']==1 and req['EfSearch']==128
 assert req['Consistency']=='linearizable_generation_snapshot' and req['Limits']==dict(RequestBytes=1048576,CandidateBytes=8388608,ResponseBytes=1048576,MergeEntries=8)
 expected=next((x['InsertRequest']['Vector'] for x in inserts if x['ExpectedID']==o['ExpectedID']),[1]+[0]*127)
 assert struct.pack('<128f',*req['Query'])==struct.pack('<128f',*expected)
 assert math.isfinite(z['Neighbors'][0]['Score'])
 assert c['SelectedPartitions']==c['HNSWServedPartitions']==c['ReadProofs']==c['RPCs']==c['GenerationPins']==1 and c['ExactScanPartitions']==c['Retries']==c['Redirects']==0
for k in ['plan','bootstrap-qualify','node-c-config']:
 key={'plan':'plan_sha256','bootstrap-qualify':'bootstrap_sha256','node-c-config':'config_sha256'}[k]
 assert sha((P/(k+'.json')).read_bytes())==a[key]
assert (P/'plan.json').read_bytes()==(B/'plan.json').read_bytes()
assert sha((T/'gomap-4994-trial14mixedc1-growth-query-root-v2.py').read_bytes())=='9cf5cfd4012da653780c32ec8eacde075a6979ffd14b31c6f8df4db4590717f4'
assert sha((T/'gomap-4994-trial14mixedc1-growth-lifecycle-root-v2.py').read_bytes())=='36deaea97ae5f61a01709cb05c43c99bceded843cda23cb3adb1e50a6cb84440'
import runpy
_gate=runpy.run_path('/tmp/gomap-4994-trial14mixedc1-growth-admission-root-v2.py')
APPROVED=_gate['admit']()
assert a==APPROVED
invraw=(I/'input-inventory.json').read_bytes()
# All retained SSH receipts are successful; identify the single driver launch/log stream.
raw=(P/'stdout.jsonl').read_bytes();receipt_hashes={};sshcounts={}
for root in [P,L]:
 count=0
 for f in root.glob('*.json'):
  x=load(f)
  if isinstance(x,dict) and 'argv' in x:
   assert x.get('exit_code',x.get('exit'))==0,(f,x.get('exit_code',x.get('exit')))
   count+=1;receipt_hashes[str(f.relative_to(V))]=sha(f.read_bytes())
 sshcounts[root.name]=count
launches=[load(f) for f in P.glob('*.json') if isinstance(load(f),dict) and 'argv' in load(f) and 'docker run ' in ' '.join(load(f)['argv']) and 'treedb-4250-rf4trial14mixedc1-query-write' in ' '.join(load(f)['argv'])]
assert len(launches)==1
assert load(next(P.glob('*-logs.json')))['stdout'].encode()==raw and load(next(P.glob('*-logs.json')))['stderr']==(P/'stderr.log').read_text()
assert load(L/'result.json')==dict(errors=[],state='PASS_STOPPED',stores_preserved=True)
voters=[]
for n in load(P/'plan.json')['nodes']:
 node=n['node'];old=json.loads(load(B/({'node-a':'38','node-b':'40','node-c':'42','node-d':'44'}[node]+'-final-state-'+node+'.json'))['stdout'])[0]
 inspections=sorted(f for f in L.glob('*inspect-'+node+'.json') if '-driver-' not in f.name);assert len(inspections)==5
 initial=json.loads(load(inspections[0])['stdout'])[0];started=json.loads(load(inspections[2])['stdout'])[0];final=json.loads(load(inspections[-1])['stdout'])[0]
 for x in [initial,started,final]:
  assert x['Id']==old['Id'] and x['Image']==n['image'] and x['Name']=='/'+n['name'] and x['Config']['Labels']['treedb.fixed-cluster.run']=='rf4trial14mixedc1'
  hc=x['HostConfig'];assert hc['Memory']==hc['MemorySwap']==2147483648 and hc['NanoCpus']==2000000000 and hc['RestartPolicy']['Name']=='no' and hc['NetworkMode']=='host'
  binds={n['root']+'/config.json:/config.json:ro',n['root']+'/credentials:/credentials:ro',n['root']+'/data:/data',n['root']+'/raft:/raft'}
  if node=='node-c':binds.add(n['root']+'/dataset:/dataset:ro')
  assert set(hc['Binds'])==binds==set(old['HostConfig']['Binds']) and not x['State']['OOMKilled']
 assert not initial['State']['Running'] and initial['State']['ExitCode']==0 and started['State']['Running'] and not final['State']['Running'] and final['State']['ExitCode']==0
 assert load(next(L.glob('*start-'+node+'.json')))['stdout'].strip() in [old['Id'],n['name']]
 voters.append(dict(node=node,cid=old['Id'],exit_code=0,oom_killed=False))
driver=load(P/'inspect.json');assert driver['Id']==launches[0]['stdout'].strip() and driver['Image']==a['query_image'] and driver['Name']=='/treedb-4250-rf4trial14mixedc1-query-write'
assert not driver['State']['Running'] and driver['State']['ExitCode']==0 and not driver['State']['OOMKilled']
assert driver['HostConfig']['RestartPolicy']['Name']=='no' and driver['HostConfig']['NetworkMode']=='host'
assert driver['Config']['Labels']['treedb.fixed-cluster.run']=='rf4trial14mixedc1' and driver['HostConfig']['Memory']==driver['HostConfig']['MemorySwap']==2147483648 and driver['HostConfig']['NanoCpus']==2000000000
assert set(driver['HostConfig']['Binds'])=={a['remote_root']+'/bootstrap-qualify.json:/bootstrap.json:ro',a['remote_root']+'/node-c/config.json:/config.json:ro',a['remote_root']+'/node-c/credentials:/credentials:ro'}
assert json.loads(load(next(L.glob('*-driver-fallback-final-node-c.json')))['stdout'])[0]['Id']==driver['Id']
assert load(next(P.glob('*-driver-hash.json')))['stdout'].split()[0]==a['driver_sha256']
latest={s['RequestedNode']:s['State'] for s in r['Readiness']}
assert all(s['Groups'][0]['LocalAppliedIndex']>=s['Groups'][0]['RequiredAppliedIndex']>=last_commit for s in latest.values())
# Independently reconstruct the post-write physical FP32 population digest.
boot=load(I/'bootstrap.json');vectors={f'doc-{i:06d}':v for i,v in enumerate(struct.iter_unpack('<128f',(I/'dataset/documents.f32').read_bytes()))}
for id,x,y in [('seed-x',1,0),('seed-minus-x',-1,0),('seed-minus-y',0,-1),(boot['Insert']['VisibleID'],0,1)]:vectors[id]=[x,y]+[0]*126
for o in inserts[:-1]:assert o['ExpectedID'] not in vectors;vectors[o['ExpectedID']]=o['InsertRequest']['Vector']
assert len(vectors)==10005
h=hashlib.sha256()
for id in sorted(vectors):h.update(struct.pack('<I',len(id.encode()))+id.encode()+struct.pack('<128f',*vectors[id]))
samples=load(P/'resource-samples.json');peaks=[]
for s in samples:
 assert s['inspect']['Id']==driver['Id'];fields=s['fields']
 assert all(k in fields for k in ['memory.current','memory.peak','memory.max','memory.events','memory.swap.current','memory.swap.max','memory.swap.events','cpu.max','cpu.stat','io.stat'])
 for k,v in fields.items():assert v['status'] in ['available','missing'] and (v['status']!='missing' or v['reason'])
 if fields['memory.peak']['status']=='available':peaks.append(int(fields['memory.peak']['raw']))
 if fields['memory.events']['status']=='available':assert all(int(x.split()[1])==0 for x in fields['memory.events']['raw'].splitlines() if x.split()[0] in ['oom','oom_kill','oom_group_kill'])
 if fields['memory.swap.current']['status']=='available':assert int(fields['memory.swap.current']['raw'])==0
receipt=dict(decision='PASS_ROOT_ASSERTIONS_PENDING_INDEPENDENT_REVIEW',status='PASS_ROOT_ASSERTIONS',findings=[],scope='Actual bounded 70-operation native write/search client-call overlap on the fresh trial14mixedc1 sealed bootstrap corpus; no pre-recall claim; post recall and sustained qualification pending.',stdout_sha256=sha(raw),identities=a,source_script_sha256={'collector':sha((T/'gomap-4994-trial14mixedc1-growth-query-root-v2.py').read_bytes()),'lifecycle':sha((T/'gomap-4994-trial14mixedc1-growth-lifecycle-root-v2.py').read_bytes())},bootstrap_review_sha256=_gate['BOOTSTRAP_REVIEW_SHA256'],admission_sha256=_gate['ADMISSION_SHA256'],pre_input_inventory_sha256=sha(invraw),run_id=r['RunID'],counts=r['Counts'],ordinary_fresh_ids=1,mutation_attempts=2,native_searches=68,ordinary_commit_indices=[o['InsertResponse']['CommitIndex'] for o in inserts[:-1]],retry_commit_index=inserts[-1]['InsertResponse']['CommitIndex'],highest_commit_index=last_commit,post_population_rows=len(vectors),post_population_sha256=h.hexdigest(),verification='Root checked exact70 operation layout, all compact JSON request hashes, planned/result identity exceptDeadline, all1 makePlan FP32 angular vectors/IDs/idempotency/document bytes, declared retry request identical and revision unchanged/newcommit, production commit-applied proofs, strict native exactID+finite scores and no exactscan/retry/redirect; root recomputed all reported call overlaps and final all4 required/local bound through acknowledged prefix.',cleanup=dict(state='PASS_STOPPED',stores_preserved=True,voters=voters,driver_cid=driver['Id'],exit_code=0,oom_killed=False,ssh_receipts_verified=sshcounts,raw_receipt_hashes=receipt_hashes),resources=dict(samples=len(samples),observed_driver_memory_peak_bytes=max(peaks) if peaks else None,final_memory_peak_missing=samples[-1]['fields']['memory.peak']['status']=='missing'),cost=dict(stream_last_ordinary_end_seconds=inserts[-2]['EndNS']/1e9,total_last_operation_end_seconds=max(o['EndNS'] for o in ops)/1e9,ordinary_write_duration_seconds=[(o['EndNS']-o['StartNS'])/1e9 for o in inserts[:-1]]),limits=['Client call overlap is not graph critical-section overlap or concurrent recall.','Observed sampled driver peak is not a whole-lifetime driver/voter peak; missing final cgroup markers retained.','No sustained QPS/p99/capacity/general mutation qualification; RF4/twohosts cannot survive loss of a two-voter host.','Image/ELF/source binding uses approved manifests and raw ELF hash receipts, not an independent rebuild. Stores retained by exact mounted roots and cleanup; no reviewer disk scan/reopen/runtime performed.'])
O=V/'growth-artifact-root-proof-v2.json';assert not O.exists();O.write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps({k:receipt[k] for k in ['decision','highest_commit_index','ordinary_commit_indices','post_population_sha256','resources','stdout_sha256']}));print('review_sha256',sha(O.read_bytes()))
