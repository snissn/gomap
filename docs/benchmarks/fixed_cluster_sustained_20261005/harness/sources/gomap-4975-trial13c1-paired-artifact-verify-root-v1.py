"""Read retained artifacts and recompute canonical FP32 truth; never runs runtime."""
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import base64,copy,hashlib,json,math,struct
from pathlib import Path
I=Path('/tmp/gomap-4975-rf4trial13c1-post-inputs-root-v1')
B=Path('/tmp/gomap-4975-rf4trial13c1-bootstrap-root-v1')
C=Path('/tmp/gomap-4975-rf4trial13c1-recall-post-root-v1')
L=Path('/tmp/gomap-4975-rf4trial13c1-post-lifecycle-root-v1')
O=Path('/tmp/gomap-4975-trial13c1-paired-artifact-root-proof-v1.json')
assert not O.exists()
def sha(b):return hashlib.sha256(b).hexdigest()
def load(p):return json.loads(p.read_bytes())
def f32(x):return struct.unpack('<f',struct.pack('<f',x))[0]
def bits(x):return struct.pack('<f',x)
def vecbits(v):return b''.join(bits(x) for x in v)
def rows(raw,n):
 assert len(raw)==n*512
 return [list(x) for x in struct.iter_unpack('<128f',raw)]
def normalized(v):
 s=0.
 for x in v:s+=float(x)*float(x)
 assert math.isfinite(s) and s>0
 inv=f32(1/math.sqrt(s))
 return [f32(x*inv) for x in v]
def score(q,v):
 s=0.
 for a,b in zip(q,v):s+=float(a)*float(b)
 return f32(s)
def result_receipt(p):
 j=load(p);assert j.get('exit_code',j.get('exit'))==0 and not j.get('timed_out');return j
iraw=(I/'input-inventory.json').read_bytes();inv=json.loads(iraw)
assert sha(iraw)=='0256209501c44c75a8374bd4acdf77c3b7f07be39443982f9cc92a1c5918ccc6'
assert len(inv)==116 and all(sha((I/f).read_bytes())==h for f,h in inv.items())
assert (C/'input-inventory.json').read_bytes()==iraw
manifest=load(I/'dataset/manifest.json')
assert manifest['docs']==10000 and manifest['dimensions']==128 and manifest['queries']==16 and manifest['top_k']==10
for f,v in manifest['files'].items():
 raw=(I/'dataset'/f).read_bytes();assert len(raw)==v['bytes'] and sha(raw)==v['sha256']
config=load(I/'config.json');boot=load(I/'bootstrap.json');prov=load(I/'provenance.json');approved=load(C/'approved-inputs.json')
for key,f in [('config_sha256','config.json'),('bootstrap_sha256','bootstrap.json')]:assert approved[key]==sha((I/f).read_bytes())
assert (C/'node-c-config.json').read_bytes()==(I/'config.json').read_bytes()
assert (C/'bootstrap-qualify.json').read_bytes()==(I/'bootstrap.json').read_bytes()
assert prov['Phase']=='post-only' and prov['CampaignID']==prov['BootstrapRequestID']=='rf4trial13c1'
assert all(prov[k] for k in ['RootAccepted','InitializationSucceeded','CleanReopenSucceeded','ExclusiveWriterStopped'])
assert prov['ProbeSHA256']=='47c1f3c30ad8cc8dce1e94fd9c40b9dca77d6713780246158e4eba8c863c7868' and prov['ProbeBinarySHA256']=='962536ad01cd393d2415efb8f3fdfd199b674badf461cc3d3ad80a48c0b68ffa' and prov['ProbeRunID']=='rf4trial13c1query01'
# Reuse artifact-only exact write assertions without its receipt write.
assert sha(Path('/tmp/gomap-4975-trial13c1-artifact-verify-root-v1.py').read_bytes())=='f09b92fd76bd9ca66c7b2ed4397b92d39d7b5d85e28dee604c2653c779c592c1'
wns={};writecode=Path('/tmp/gomap-4975-trial13c1-artifact-verify-root-v1.py').read_text().split('receipt=dict(')[0];exec(compile(writecode,'independent-write-artifact-checks','exec'),wns)
assert wns['sha']((I/'probe.jsonl').read_bytes())==prov['ProbeSHA256'] and (I/'probe.jsonl').read_bytes()==(wns['P']/'stdout.jsonl').read_bytes()
assert wns['last_commit']==157 and len(wns['inserts'])==66
assert approved['driver_sha256']=='962536ad01cd393d2415efb8f3fdfd199b674badf461cc3d3ad80a48c0b68ffa'
assert approved['server_sha256']==prov['ServerBinarySHA256']=='bb3303ab0452e19a7e9d02d6d3b617ff34859edfc93400f04b06082128d8594c'
assert approved['source_head']=='e1e965fa5c95f78a6b17e7dcf6174cd98265ad40' and prov['RuntimeSourceHead']=='e1e965fa5c95f78a6b17e7dcf6174cd98265ad40'
assert prov['QualificationSourceSHA256']=='750dde6bc3866287e3201e62be9b88dc6f5e0d7b2a947d16da0e5efba074e769'
corpus=rows((I/'dataset/documents.f32').read_bytes(),10000);queries=rows((I/'dataset/queries.f32').read_bytes(),16)
for v in corpus:
 assert all(math.isfinite(x) for x in v)
 s=0.
 for x in v:s+=x*x
 assert abs(math.sqrt(s)-1)<=.001 and (v[0]*v[0]+v[1]*v[1])/s<=.9
vectors={f'doc-{i:06d}':v for i,v in enumerate(corpus)}
fresh='rf4trial13c1/dataset-'+sha((I/'dataset/manifest.json').read_bytes())+'-fresh-y'
assert boot['Insert']['VisibleID']==boot['Retry']['VisibleID']==fresh
for name,x,y in [('seed-x',1,0),('seed-minus-x',-1,0),('seed-minus-y',0,-1),(fresh,0,1)]:vectors[name]=[float(x),float(y)]+[0.]*126
for op in wns['inserts'][:-1]:
 req=op['InsertRequest'];id=base64.b64decode(req['ID'],validate=True).decode();assert id not in vectors;vectors[id]=req['Vector']
assert wns['inserts'][-1]['Phase']=='explicit-retry' and wns['inserts'][-1]['InsertResponse']['LiveRevision']==wns['inserts'][-2]['InsertResponse']['LiveRevision']==66
h=hashlib.sha256()
for id in sorted(vectors):h.update(struct.pack('<I',len(id.encode()))+id.encode()+vecbits(vectors[id]))
population=h.hexdigest();assert len(vectors)==10069 and population=='c152d622b5f6ab1c5503b60f288ef2eec74cf1cdd0a8eec694fac5288506c117'
# Full retained bootstrap chain and exact encoded dataset payloads.
breceipts=sorted(B.glob('[0-9][0-9]-*.json'));assert len(breceipts)==44
for p in breceipts:
 result_receipt(p);assert p.read_bytes()==(I/'historical-chain'/p.name).read_bytes()
initrecord=load(B/'19-initialize.json');init=json.loads(initrecord['stdout'])
assert sha((B/'19-initialize.json').read_bytes())==prov['InitializationReceiptSHA256']
assert sha((B/'result.json').read_bytes())==prov['CleanReopenReceiptSHA256'] and load(B/'result.json')['status']=='PASS'
assert json.loads(load(B/'36-qualify.json')['stdout'])==boot
assert init['Stage']=='prepared-restart-required' and init['Prepare']==boot['Prepare'] and init['Dataset']==boot['Dataset']
assert init['Dataset']['Rows']==10000 and init['Dataset']['SourceRows']==10003 and init['Dataset']['Dimensions']==128 and init['Dataset']['InputBytes']==5221566
assert init['Dataset']['ManifestSHA256']==prov['ManifestSHA256']==sha((I/'dataset/manifest.json').read_bytes())
def checked_submit(r):
 assert r['ActualAck']==4 and r['CommittedApplied'] and r['CommittedRecoverable']
 e=r['Evidence'];entry=r['CommittedEntry'];assert e['Kind']=='production-consensus-v1' and e['ProductionConsensus'] and e['Committed'] and e['GroupID']=='group-a'
 assert e['Index']==entry['Index'] and e['Term']==entry['Term'] and e['NodeID']==e['LeaderID']
 assert r['ApplyResult']['Status'] in ['applied','already-applied'] and not r['ApplyResult']['DeterministicErrorCode']
 raw=base64.b64decode(entry['Bytes']);assert raw==base64.b64decode(r['DecodedEntry']['Bytes']) and r['ApplyResult']['CommandDigest']==r['DecodedEntry']['Digest']
 return entry['Index'],raw
prior,_=checked_submit(init['Create']);seedindex,_=checked_submit(init['Seed']);assert seedindex>prior;prior=seedindex
# Byte-list sections: unsigned varint count, lengths, concatenated byte strings.
def varint(raw,pos):
 v=0;shift=0
 while True:
  b=raw[pos];pos+=1;v|=(b&127)<<shift
  if b<128:return v,pos
  shift+=7;assert shift<=63

def byte_list(raw):
 n,pos=varint(raw,0);lens=[]
 for _ in range(n):v,pos=varint(raw,pos);lens.append(v)
 out=[]
 for n in lens:out.append(raw[pos:pos+n]);pos+=n
 assert pos==len(raw);return out
assert len(init['Chunks'])==79
for k,ch in enumerate(init['Chunks']):
 assert ch['Ordinal']==k and ch['FirstRow']==k*128 and ch['Rows']==min(128,10000-k*128) and ch['Outcome']=='committed-applied' and not ch['Error']
 assert ch['RequestID']=='rf4trial13c1/dataset-'+prov['ManifestSHA256']+f'/dataset/{k:06d}'
 index,raw=checked_submit(ch['Result']);assert index>prior and sha(raw)==ch['EntrySHA256'];prior=index
 sections={s['ID']:base64.b64decode(s['Bytes']) for s in ch['Result']['DecodedEntry']['Decoded']['Sections']}
 assert sections[8].decode()==ch['RequestID'];ids=byte_list(sections[102]);docs=byte_list(sections[103]);assert len(ids)==len(docs)==ch['Rows']
 assert ch['Result']['ApplyResult']['AffectedCount']==ch['Rows']
 for j,(id,docraw) in enumerate(zip(ids,docs)):
  doc=json.loads(docraw);assert id.decode()==f'doc-{k*128+j:06d}' and doc['kind']=='dataset' and vecbits(doc['embedding'])==vecbits(corpus[k*128+j])
prepare=boot['Prepare']['Command'];assert prepare['SourceRowCount']==prepare['MaxSourceRows']==10003 and prepare['IndexPosition']>prior and prepare['Operation']=='prepare'
assert boot['Insert']['CommitIndex']==87 and boot['Retry']['CommitIndex']==88 and boot['Insert']['LiveRevision']==boot['Retry']['LiveRevision']==1
for key in ['Insert','Retry']:
 r=boot[key];assert r['PartitionID']==0 and r['OwnerGroup']=='group-a' and r['ProductionConsensus'] and r['AppliedIndex']>=r['CommitIndex'] and not r['VisibilityToken']
# Report identity, immutable request/accounting and canonical oracle.
raw=(C/'stdout.jsonl').read_bytes();events=[json.loads(l) for l in raw.splitlines()];assert [e['Event'] for e in events]==['planned','result']
p,r=[e['Report'] for e in events]
assert r['Verdict']=='ACCEPT_QUIESCENT_RECALL_OBSERVATION' and r['Error']==''
for report in [p,r]:
 assert report['Version']==1 and report['Kind']=='fixed_cluster_quiescent_recall_v1' and report['Phase']=='post-only' and report['RunID']=='rf4trial13c1post01'
 assert report['Generation']=={'Index':'embedding_graph','Generation':1} and report['PopulationRows']==10069 and report['PopulationSHA256']==population and report['HighestCommitIndex']==157
 assert report['ConfigSHA256']==sha((I/'config.json').read_bytes()) and report['BootstrapSHA256']==sha((I/'bootstrap.json').read_bytes()) and report['ManifestSHA256']==prov['ManifestSHA256']
 assert report['ProvenanceSHA256']==sha((I/'provenance.json').read_bytes()) and report['Provenance']==prov and report['ProbeSHA256']==prov['ProbeSHA256'] and report['BinarySHA256']==approved['driver_sha256']
 assert report['Manifest']==manifest and report['Timeout']==120000000000 and report['RPCTimeout']==10000000000 and len(report['Queries'])==16
assert r['Counts']==dict(Planned=16,Attempted=16,Succeeded=16,Failed=0,Canceled=0,Unknown=0,Unissued=0) and r['MeanRecallAt10']==1
norm={id:normalized(v) for id,v in vectors.items()};export=[json.loads(l) for l in (I/'dataset/exact_truth.jsonl').read_text().splitlines()];details=[];previous_end=0
for n,(pq,rq,q) in enumerate(zip(p['Queries'],r['Queries'],queries)):
 assert pq['QueryID']==rq['QueryID']==f'query-{n:06d}' and rq['Outcome']=='succeeded' and rq['ErrorCode']==rq['Error']=='' and pq['Outcome']=='unissued'
 request=pq['Request'];actual=rq['Request'];assert vecbits(request['Query'])==vecbits(q)==vecbits(actual['Query'])
 assert request['VisibilityToken'] is None and request['Version']==1 and request['Metric']=='cosine' and request['TopK']==10 and request['Probes']==1 and request['EfSearch']==128
 assert request['Consistency']=='linearizable_generation_snapshot' and request['Generation']==r['Generation'] and request['Limits']==dict(RequestBytes=1048576,CandidateBytes=8388608,ResponseBytes=1048576,MergeEntries=32)
 stripped=copy.deepcopy(actual);stripped['Deadline']=request['Deadline'];assert stripped==request and request['Deadline']=='0001-01-01T00:00:00Z' and actual['Deadline']!=request['Deadline']
 digest=sha(json.dumps(request,separators=(',',':'),ensure_ascii=False).encode());assert digest==pq['RequestSHA256']==rq['RequestSHA256']
 assert rq['StartNS']>=previous_end and rq['EndNS']>rq['StartNS'];previous_end=rq['EndNS']
 nq=normalized(q);scored=sorted(((score(nq,v),id) for id,v in norm.items()),key=lambda x:(-x[0],x[1]));aug=scored[:10];cor=[x for x in scored if x[1].startswith('doc-')][:10]
 assert [x[1] for x in cor]==export[n]['document_ids'] and export[n]['query_id']==pq['QueryID']
 for expect,key in [(cor,'CorpusTruth'),(aug,'Truth')]:
  for actualtruth in [pq[key],rq[key]]:assert len(actualtruth)==10 and all(z['ID']==id and bits(z['Score'])==bits(s) for z,(s,id) in zip(actualtruth,expect))
 response=rq['Response'];assert response['Generation']==r['Generation'] and len(response['Neighbors'])==10
 assert all(z['ID']==id and bits(z['Score'])==bits(s) for z,(s,id) in zip(response['Neighbors'],aug)) and rq['RecallAt10']==1
 ctr=response['Counters'];assert ctr['SelectedPartitions']==ctr['HNSWServedPartitions']==ctr['ReadProofs']==ctr['RPCs']==ctr['GenerationPins']==1 and ctr['ExactScanPartitions']==ctr['Retries']==ctr['Redirects']==0 and ctr['Candidates']>0 and ctr['Edges']>0
 details.append(dict(query_id=rq['QueryID'],recall_at_10=rq['RecallAt10'],candidate_count=ctr['Candidates'],edges=ctr['Edges'],client_duration_ns=rq['EndNS']-rq['StartNS']))
for states,target in [(boot['Readiness'],88)]+[([x['State'] for x in r[k]],157) for k in ['ReadinessBefore','ReadinessAfter']]:
 assert len(states)==4 and {s['NodeID'] for s in states}=={'node-a','node-b','node-c','node-d'}
 for s in states:
  assert s['Ready'] and s['Live'] and not s['Draining'] and s['VectorPhase']=='active' and len(s['Groups'])==1
  g=s['Groups'][0];assert g['GroupID']=='group-a' and g['Ready'] and g['RequiredAppliedIndex']>=target and g['LocalAppliedIndex']>=g['RequiredAppliedIndex']
for k in ['ReadinessBefore','ReadinessAfter']:assert all(x['Error']==x['ErrorCode']=='' and x['RequestedNode']==x['State']['NodeID'] for x in r[k])
# SSH/log/CID/mount/resource evidence, including original clean close/reopen.
for root in [C,L]:
 for f in root.glob('*.json'):
  j=load(f)
  if isinstance(j,dict) and 'argv' in j:result_receipt(f)
assert load(L/'result.json')==dict(errors=[],state='PASS_STOPPED',stores_preserved=True)
assert load(C/'exit.json')['status']=='PASS_PENDING_ROOT_RECALL_VALIDATION'
plan=load(C/'plan.json');assert (C/'plan.json').read_bytes()==(B/'plan.json').read_bytes() and sha((C/'plan.json').read_bytes())==approved['plan_sha256']
voters=[]
for n in plan['nodes']:
 node=n['node'];original=load(B/({'node-a':'38','node-b':'40','node-c':'42','node-d':'44'}[node]+'-final-state-'+node+'.json'));old=json.loads(original['stdout'])[0]
 current=[f for f in sorted(L.glob('*inspect-'+node+'.json')) if '-driver-' not in f.name];assert len(current)==5;before=json.loads(load(current[0])['stdout'])[0];started=json.loads(load(current[2])['stdout'])[0];assert started['State']['Running'];final=json.loads(load(current[-1])['stdout'])[0]
 for x in [old,before,started,final]:
  assert x['Id']==old['Id'] and x['Image']==n['image'] and x['Name']=='/'+n['name'] and x['Config']['Labels']['treedb.fixed-cluster.run']=='rf4trial13c1'
  hc=x['HostConfig'];assert hc['Memory']==hc['MemorySwap']==2147483648 and hc['NanoCpus']==2000000000 and hc['RestartPolicy']['Name']=='no'
  expected={n['root']+'/config.json:/config.json:ro',n['root']+'/credentials:/credentials:ro',n['root']+'/data:/data',n['root']+'/raft:/raft'}
  if node=='node-c':expected.add(n['root']+'/dataset:/dataset:ro')
  assert set(hc['Binds'])==expected and set(hc['Binds'])==set(old['HostConfig']['Binds']) and not x['State']['OOMKilled']
 assert old['State']['Running'] and not before['State']['Running'] and before['State']['ExitCode']==0 and not final['State']['Running'] and final['State']['ExitCode']==0
 assert old['Id']==load(B/({'node-a':'15','node-b':'16','node-c':'17','node-d':'18'}[node]+'-serve-'+node+'.json'))['stdout'].strip()
 for stage in ['close-exit','restart']:
  rr=load(next(B.glob('*-'+stage+'-'+node+'.json')));assert rr['stdout'].strip()==('0' if stage=='close-exit' else old['Id'])
 voters.append(dict(node=node,cid=old['Id'],final_receipt_sha256=sha(current[-1].read_bytes())))
driver=load(C/'inspect.json');state=driver['State'];assert not state['Running'] and not state['OOMKilled'] and state['ExitCode']==0
assert driver['Image']==approved['query_image'] and driver['Name']=='/treedb-4250-rf4trial13c1-recall-post-v1' and driver['Config']['Labels']['treedb.fixed-cluster.run']=='rf4trial13c1'
assert driver['HostConfig']['Memory']==driver['HostConfig']['MemorySwap']==2147483648 and driver['HostConfig']['NanoCpus']==2000000000
assert json.loads(load(next(L.glob('*-driver-fallback-final-node-c.json')))['stdout'])[0]['Id']==driver['Id']
assert load(next(C.glob('*-driver-hash.json')))['stdout'].split()[0]==approved['driver_sha256']
logs=load(next(C.glob('*-logs.json')));assert logs['stdout'].encode()==raw and logs['stderr']==(C/'stderr.log').read_text()
samples=load(C/'resource-samples.json');peaks=[]
for s in samples:
 assert s['inspect']['Id']==driver['Id']
 fields=s['fields'];assert all(x in fields for x in ['memory.current','memory.peak','memory.max','memory.events','memory.swap.current','memory.swap.max','memory.swap.events','cpu.max','cpu.stat','io.stat'])
 for name,v in fields.items():
  assert v['status'] in ['available','missing']
  if v['status']=='missing':assert v['reason']
 if fields['memory.peak']['status']=='available':peaks.append(int(fields['memory.peak']['raw']))
 if fields['memory.events']['status']=='available':assert all(int(line.split()[1])==0 for line in fields['memory.events']['raw'].splitlines() if line.split()[0] in ['oom','oom_kill','oom_group_kill'])
 if fields['memory.swap.current']['status']=='available':assert int(fields['memory.swap.current']['raw'])==0
receipt=dict(decision='PASS_ROOT_ASSERTIONS_PENDING_INDEPENDENT_REVIEW',status='PASS_ROOT_ASSERTIONS',scope='Actual samecampaign trial13c1 pre/write/post bounded recall comparison: sixteen serial canonical queries per phase around65 ordinary inserts plus declared retry; no concurrent/sustained qualification.',findings=[],stdout_sha256=sha(raw),input_inventory_sha256=sha(iraw),input_files_verified=len(inv),identities=approved,run_id=r['RunID'],population_rows=10069,population_sha256=population,highest_commit_index=157,counts=r['Counts'],mean_recall_at_10=1,
 oracle_verification='Independent Python reconstruction of all10000 original little-endian FP32 rows plus3anchors+qualifiedfreshY+65 actual ordinary insert IDs/vectors, with declared retry deduplicated. All79 encoded dataset ID/document payloads independently match original corpus FP32 bytes. Sorted length-prefixed ID/FP32 digest matchesc152d622b5f6ab1c5503b60f288ef2eec74cf1cdd0a8eec694fac5288506c117. FP32 inverse norm/component rounding, left-to-right binary64 dot, final FP32 score, score DESC/ID ASC reproduce all16 corpus and augmented top10 IDs/scorebits and returned neighbors; unchanged exporter truth IDs match.',
 request_verification='All16 original query FP32 bits, exact identity/limits/native requests and SHA256 hashes reproduced independently; planned/result differ only Deadline. Sequential timings/counts, phase post-only/frozen accepted complete probe identity, generation/source/config/bootstrap/provenance hash bindings verified.',
 native_verification='All16 selected/native partitions1, readproofs/RPCs/genpins1, exactscans/retries/redirects0; all4 bootstrap and before/after active/ready/live required/local applied at least157.',
 bootstrap_verification='All44 retained raw receipts exit0 and byte-identical to frozen historical chain; fresh CID creation, original clean exit0/restart sameCID, prepare10003/79 committed-applied chunks with actual ID+FP32 payloads, qualify original87/retry88 revision1; no reviewer runtime operations.',
 cleanup=dict(voters=voters,driver_cid=driver['Id'],driver_exit=0,driver_oom_killed=False,state='PASS_STOPPED',stores_preserved=True),
 resources=dict(samples=len(samples),observed_driver_memory_peak_bytes=max(peaks) if peaks else None,final_peak_missing=samples[-1]['fields']['memory.peak']['status']=='missing',scope='Driver-only observed samples; final cgroup may disappear. Whole-lifetime driver peak and voter peaks unproved.'),queries=details,
 limits=['Root-attested operational quiescence; actual samecampaign pre/write/post external pairing. No runtime query watermark or concurrent recall proof.','Sixteen serial recall1 observations do not qualify sustained QPS/p99/capacity or host loss. RF4/twohosts cannot survive loss of a two-voter host.','ELF/source binding uses approved image/manifest and copied ELF receipts, not independent rebuild. No Go/runtime/Git/GH run by reviewer.','Stores preserved means retained mounted roots and no deletion in lifecycle; this review did not reopen or independently scan disk.'])

# Bind scripts/full CLI and pair actual retained pre/post identities.
T=Path('/tmp');preC=T/'gomap-4975-rf4trial13c1-recall-pre-root-v1';preReview=load(T/'gomap-4975-trial13c1-pre-artifact-review-root-v1.json');writeReview=load(T/'gomap-4975-trial13c1-growth-artifact-review-root-v1.json')
assert preReview['decision']==writeReview['decision']=='ACCEPT'
assert sha((preC/'stdout.jsonl').read_bytes())==preReview['stdout_sha256']
assert sha((I/'probe.jsonl').read_bytes())==writeReview['stdout_sha256'] and (I/'write-artifact-review.json').read_bytes()==(T/'gomap-4975-trial13c1-growth-artifact-review-root-v1.json').read_bytes()
assert sha((I/'pre-input-inventory.json').read_bytes())==preReview['input_inventory_sha256']
for filename,expected in [('gomap-4975-rf4trial13c1-recall-post-root-v1.py','ecb631513697471efb5be19dc3778c5b2e1ae5fd50444281b48115911b05975f'),('gomap-4975-rf4trial13c1-post-lifecycle-root-v1.py','c30fe8363d2dc38ce7feccf71e2b26a278b8fa6cd89e3db22d55f25613e4e699')]:assert sha((T/filename).read_bytes())==expected
command=load(C/'command.json');assert command==['docker','run','--pull=never','-d','--restart=no','--name','treedb-4250-rf4trial13c1-recall-post-v1','--label','treedb.fixed-cluster.run=rf4trial13c1','--network=host','--memory=2g','--memory-swap=2g','--cpus=2','--entrypoint','/treedb-query-under-write','-v',approved['remote_root']+'/node-c/config.json:/config.json:ro','-v',approved['remote_root']+'/node-c/credentials:/credentials:ro','-v',approved['remote_root']+'/bootstrap-qualify.json:/bootstrap.json:ro','-v','/home/mikers/gomap-4975-rf4trial13c1-post-inputs-root-v1:/recall:ro',approved['query_image'],'-config','/config.json','-bootstrap-receipt','/bootstrap.json','-mode','quiescent-recall','-phase','post-only','-probe-receipt','/recall/probe.jsonl','-dataset','/recall/dataset','-provenance','/recall/provenance.json','-run-id','rf4trial13c1post01','-timeout','120s','-rpc-timeout','10s']
assert driver['Args']==command[command.index(approved['query_image'])+1:] and driver['Id']==load(next(C.glob('*-launch.json')))['stdout'].strip()
assert set(driver['HostConfig']['Binds'])=={approved['remote_root']+'/node-c/config.json:/config.json:ro',approved['remote_root']+'/node-c/credentials:/credentials:ro',approved['remote_root']+'/bootstrap-qualify.json:/bootstrap.json:ro','/home/mikers/gomap-4975-rf4trial13c1-post-inputs-root-v1:/recall:ro'}
pr=[json.loads(x)['Report'] for x in (preC/'stdout.jsonl').read_bytes().splitlines()][-1]
assert pr['Phase']=='pre' and pr['RunID']=='rf4trial13c1pre01' and pr['PopulationRows']==10004 and pr['HighestCommitIndex']==88 and pr['MeanRecallAt10']==1
for k in ['ConfigSHA256','BootstrapSHA256','ManifestSHA256','BinarySHA256','Generation','ScoreContract','ConfigIdentity','Manifest','Timeout','RPCTimeout']:assert pr[k]==r[k]
paired=[]
for preq,postq in zip(pr['Queries'],r['Queries']):
 assert preq['QueryID']==postq['QueryID'] and preq['RequestSHA256']==postq['RequestSHA256']
 a,b=copy.deepcopy(preq['Request']),copy.deepcopy(postq['Request']);a.pop('Deadline');b.pop('Deadline');assert a==b
 assert vecbits(preq['Request']['Query'])==vecbits(postq['Request']['Query'])
 assert preq['CorpusTruth']==postq['CorpusTruth'] and preq['Truth']==postq['Truth']
 paired.append(dict(query_id=postq['QueryID'],request_sha256=postq['RequestSHA256'],pre_recall_at_10=preq['RecallAt10'],post_recall_at_10=postq['RecallAt10'],pre_population=10004,post_population=10069))
receipt['matching_query_pairs']=paired
receipt['pairing']=dict(campaign='rf4trial13c1',pre_run_id=pr['RunID'],post_run_id=r['RunID'],pre_stdout_sha256=preReview['stdout_sha256'],post_stdout_sha256=sha(raw),pre_population_rows=10004,post_population_rows=10069,pre_population_sha256=pr['PopulationSHA256'],post_population_sha256=population,pre_prefix=88,post_acknowledged_prefix=157,pre_mean_recall_at_10=1,post_mean_recall_at_10=1,qualification='Actual serial operationally quiescent samecampaign observations surrounding accepted actual ordinary writes; pairing is external artifact analysis, not retrospective fabricated pre state or concurrent recall.')
receipt['bindings']=dict(collector_sha256=sha((T/'gomap-4975-rf4trial13c1-recall-post-root-v1.py').read_bytes()),lifecycle_sha256=sha((T/'gomap-4975-rf4trial13c1-post-lifecycle-root-v1.py').read_bytes()),pre_review_sha256=sha((T/'gomap-4975-trial13c1-pre-artifact-review-root-v1.json').read_bytes()),write_review_sha256=sha((T/'gomap-4975-trial13c1-growth-artifact-review-root-v1.json').read_bytes()),source_review_sha256=sha((T/'gomap-4975-trial13c1-post-source-review-root-v1.json').read_bytes()),input_archive_sha256=sha((T/'gomap-4975-rf4trial13c1-post-inputs-root-v1.tar.gz').read_bytes()))
receipt['artifact_inventory']={str(f.relative_to(T)):sha(f.read_bytes()) for root in [C,L] for f in sorted(root.rglob('*')) if f.is_file()}
receipt['readiness_before_after']={k:[dict(node=x['RequestedNode'],required=x['State']['Groups'][0]['RequiredAppliedIndex'],local=x['State']['Groups'][0]['LocalAppliedIndex']) for x in r[k]] for k in ['ReadinessBefore','ReadinessAfter']}

O.write_text(json.dumps(receipt,indent=2)+'\n');print(O);print(sha(O.read_bytes()));print('PASS_ROOT_FP32_AND_ARTIFACT_ASSERTIONS_PENDING_INDEPENDENT_REVIEW')
