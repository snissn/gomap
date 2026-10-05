import base64,collections,copy,hashlib,json
from pathlib import Path
p=Path('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/growth-query-root-v2')
a=json.loads((p/'approved-inputs.json').read_text())
events=[json.loads(line) for line in (p/'stdout.jsonl').read_text().splitlines()]
plans=[e['Report'] for e in events if e['Event']=='planned'];results=[e['Report'] for e in events if e['Event']=='result']
assert len(plans)==len(results)==1
plan,r=plans[0],results[0];ops=r['Operations'];assert len(ops)==len(plan['Operations'])==70
for k in ['RunID','Generation','ConfigSHA256','BootstrapSHA256','BinarySHA256','Timeout','RPCTimeout']:assert plan[k]==r[k]
assert r['BinarySHA256']==a['driver_sha256'] and r['ConfigSHA256']==a['config_sha256'] and r['BootstrapSHA256']==a['bootstrap_sha256']
for old,new in zip(plan['Operations'],ops):
 assert old['Outcome']=='unissued'
 for k in ['Ordinal','Phase','Kind','ExpectedID','RequestSHA256']:assert old[k]==new[k]
 for k in ['SearchRequest','InsertRequest']:
  x,y=copy.deepcopy(old[k]),copy.deepcopy(new[k])
  if x is not None:x.pop('Deadline');y.pop('Deadline')
  assert x==y
assert [o['Ordinal'] for o in ops]==list(range(70))
expected=[('preflight-writer','search'),('preflight-reader','search')]+[('concurrent','insert')]*1+[('concurrent','search')]*64+[('explicit-retry','insert')]+[('post-self','search')]*1+[('final-anchor','search')]
assert [(o['Phase'],o['Kind']) for o in ops]==expected
assert all(not o['Error'] and not o['ErrorCode'] and 0<o['StartNS']<o['EndNS'] for o in ops)
counts=dict(collections.Counter(o['Outcome'] for o in ops));assert counts=={'succeeded':70}
assert r['Counts']=={'Planned':70,'Attempted':70,'Succeeded':70,'Failed':0,'Canceled':0,'Unknown':0,'Unissued':0}
assert r['Verdict']=='ACCEPT_BOUNDED_FUNCTIONAL_CLIENT_OVERLAP' and not r['Error']
assert r['Timeout']==600000000000 and r['RPCTimeout']==60000000000
inserts=[o for o in ops if o['Kind']=='insert'];searches=[o for o in ops if o['Kind']=='search'];assert len(inserts)==2 and len(searches)==68
assert len({o['InsertRequest']['ID'] for o in inserts})==1
last_commit=json.loads((p/'bootstrap-qualify.json').read_text())['Retry']['CommitIndex']
for o in inserts:
 z=o['InsertResponse'];assert z['Generation']==r['Generation']==z['VisibilityGeneration'] and z['OwnerGroup']=='group-a' and z['VisibleID']==o['ExpectedID']
 assert z['CommitIndex']>last_commit and z['AppliedIndex']>=z['CommitIndex'] and z['CommitTerm']>0 and z['ProductionConsensus'] and z['VisibilityToken'] is None
 assert z['VisibleID']==base64.b64decode(o['InsertRequest']['ID'],validate=True).decode()
 c=z['Counters'];assert all(c[k]==1 for k in ['Routes','Commits','Replications','Applies','VisibilityProofs']) and 0<=c['Forwards']<=1
 last_commit=z['CommitIndex']
assert [o['InsertResponse']['LiveRevision'] for o in inserts[:-1]]==list(range(2,3))
assert inserts[-1]['Phase']=='explicit-retry'
x,y=copy.deepcopy(inserts[-2]['InsertRequest']),copy.deepcopy(inserts[-1]['InsertRequest']);x.pop('Deadline');y.pop('Deadline');assert x==y
assert inserts[-2]['RequestSHA256']==inserts[-1]['RequestSHA256']
x,y=inserts[-2]['InsertResponse'],inserts[-1]['InsertResponse']
for k in ['VisibleID','OwnerGroup','PartitionID','LiveRevision']:assert x[k]==y[k]
assert r['HighestCommitIndex']==last_commit
for o in searches:
 z=o['SearchResponse'];c=z['Counters'];assert len(z['Neighbors'])==1 and z['Generation']==r['Generation'] and z['Neighbors'][0]['ID']==o['ExpectedID'] and abs(z['Neighbors'][0]['Score']-1)<=1e-5
 assert c['SelectedPartitions']>0 and c['HNSWServedPartitions']==c['SelectedPartitions'] and c['ExactScanPartitions']==0 and c['ReadProofs']>0
overlaps=[{'SearchOrdinal':q['Ordinal'],'InsertOrdinal':w['Ordinal'],'SearchFinishedBeforeInsert':q['EndNS']<w['EndNS']} for q in searches if q['Phase']=='concurrent' for w in inserts if w['Phase']=='concurrent' and q['StartNS']<w['EndNS'] and w['StartNS']<q['EndNS']]
assert r['Overlaps']==overlaps and any(o['SearchFinishedBeforeInsert'] for o in overlaps)
latest={z['RequestedNode']:z for z in r['Readiness']};assert set(latest)=={'node-a','node-b','node-c','node-d'}
for node,z in latest.items():
 s=z['State'];assert not z['Error'] and s['NodeID']==node and s['VectorPhase']=='active' and s['Live'] and s['Ready'] and not s['Draining']
 assert any(g['GroupID']=='group-a' and g['Ready'] and g['LocalAppliedIndex']>=last_commit for g in s['Groups'])
inspect=json.loads((p/'inspect.json').read_text());assert inspect['State']['ExitCode']==0 and not inspect['State']['OOMKilled']
proof={'status':'PASS_EXACT_70_OPERATION_PLAN_NATIVE_PROOFS_FINAL_ALL4_PREFIX','head':a['source_head'],'driver_sha256':a['driver_sha256'],'planned':70,'ordinary_fresh_ids':1,'mutation_attempts':2,'native_searches':68,'highest_acknowledged_commit':last_commit,'voters':sorted(latest),'limits':'Bounded functional client-call overlap only; no sustained throughput, general mutation support, host-loss tolerance, or whole-lifetime peak claim.'}
(p/'root-exact-proof-v2.json').write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps(proof))
