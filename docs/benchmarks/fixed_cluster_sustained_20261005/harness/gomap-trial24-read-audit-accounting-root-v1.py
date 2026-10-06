if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import sys
sys.dont_write_bytecode=True
from source_paths import source_path, W
"""Pure LOCAL Trial24 C1 read/audit accounting. No runtime or campaign acceptance.
Native Go prefix tables determine rank; scalar FP32 recomputation checks returned
scores only. Original Go JSON pieces, including -0, are never reserialized for
wire/retention accounting. All imported sources are inert and hash-pinned.
"""
import argparse, ast, hashlib, importlib.util, json, math, struct, sys
from pathlib import Path
READ=source_path('/tmp/gomap-5021-sustained-consumers-provisional-root-v1/shared_read.py')
READ_SHA='6f4530fa70a6bacd21cbd4ebbfd6268dff585b41e340712e9c9298e697aed28b'
AUDIT=source_path('/tmp/gomap-5021-sustained-consumers-provisional-root-v1/shared_audit.py')
AUDIT_SHA='8199998cdee169b849755ae35bfb8a204ef86804151ccae54d62925c85d07240'
COLLECTOR=source_path('/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24mixedchangingc1-collector-root-v1.py')
COLLECTOR_SHA='7ba177dd3b64ee37ecbaf6f92f80d76efe4c931ac8cf9b5123dbbb3910d361df'
HEAD='__ROOT_FROZEN_HEAD__'
TREE='__ROOT_FROZEN_TREE__'
RUN=W.campaign
QUERY_RUN=W.query_run
PINS={READ:READ_SHA,AUDIT:AUDIT_SHA,COLLECTOR:COLLECTOR_SHA}
def sha(raw): return hashlib.sha256(raw).hexdigest()
def need(ok,label):
 if not ok: raise AssertionError(label)
def bounded(path,cap=128<<20):
 p=Path(path);need(p.is_absolute() and p.is_file() and not p.is_symlink() and p.stat().st_size<=cap,'bounded regular absolute input')
 with p.open('rb') as f: raw=f.read(cap+1)
 need(len(raw)<=cap,'bounded input bytes');return raw
def load(path,digest,name):
 raw=bounded(path,1<<20);need(sha(raw)==digest,'frozen source '+path)
 spec=importlib.util.spec_from_file_location(name,path)
 module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module,raw.decode()
rd,read_source=load(READ,READ_SHA,'trial24_retained_read')
au,audit_source=load(AUDIT,AUDIT_SHA,'trial24_retained_audit')
cc,collector_source=load(COLLECTOR,COLLECTOR_SHA,'trial24_collector_local')
core=rd.c
core.RUN=au.c.RUN=RUN
core.QUERY_RUN=au.c.QUERY_RUN=QUERY_RUN
au.RUN=QUERY_RUN
def prohibited(*args,**kwargs): raise RuntimeError('local accounting cannot invoke transport/runtime')
cc.call=prohibited
LIMITS=[
 'Read/audit accounting only; no campaign acceptance, runtime execution, transport authentication, lifecycle/resource or whole-population acquisition authority.',
 'Post CorpusTruth maximal ranking remains producer-computed; independently frozen full native prefix truth governs all accepted recall. Native Go pending oracle bytes and root-promoted frozen receipt must agree. Root owns native execution/provenance acceptance; this helper never ranks a Python population.',
 'Returned score bits are recomputed under the traced native FP32 normalization/binary64 accumulation contract, separately from native rank authority.',
 'Audit attachments attest prepared-owner source/membership, current-FSM fences and WAL; only retained outcome/token/chain/physical scalar consistency is independently recomputed.',
 'Warmup/paired monotonic origins and measured dispatch reservation are not serialized. Deadline alignment limits and observed overshoot remain explicit.',
 'Read QPS includes validation, retention and drain; excludes setup, oracle derivation, warmup, postjoin recheck, retry, post-recall and audits. Client overlap is not server overlap or sustained capacity.'
]
rd.LIMITATIONS=LIMITS
rd.DURATION=W.duration_ns
rd.SPACING=W.spacing_ns
rd.CONCURRENCY=W.concurrency
DERIVATION={}
# Reuse the complete reviewed verifier, changing only invariant recall assumptions.
node=next(n for n in ast.parse(read_source).body if isinstance(n,ast.FunctionDef) and n.name=='verifier')
original=ast.get_source_segment(read_source,node)
adapted=original
replacements=[
 ('ACCEPT_MIXED_INVARIANT_RECALL_WINDOW_OBSERVATION_PENDING_ROOT_SHUTDOWN_VERIFICATION','ACCEPT_MIXED_CHANGING_TOP10_RECALL_WINDOW_OBSERVATION_PENDING_ROOT_SHUTDOWN_VERIFICATION'),
 ("matched,value=c.one_prefix(x['Response'],q['Request'],states,truth[ordinal%16],lo,hi)","mask,value=changing_response(x['Response'],q['Request'],states,p['Prefixes'],ordinal%16,lo,hi)\n   matched=min(i for i in range(len(states)) if mask&(1<<i))"),
 ("'independent single-prefix exact FP32/recall'","'all compatible prefixes conservative minimum recall'"),
 ("dict(Ordinal=ordinal,Lower=lo,Upper=hi,Matched=matched)","dict(Ordinal=ordinal,Lower=lo,Upper=hi,Matched=matched,CompatibleMask=mask,RecallAt10=value)"),
 ("'every actual final single-prefix causal claim'","'every actual final compatible mask/minimum recall claim'")
]
for before,after in replacements:
 need(adapted.count(before)==1,'exact retained verifier adaptation '+before)
 adapted=adapted.replace(before,after,1)
DERIVATION['read_verifier']={'original_sha256':sha(original.encode()),'adapted_sha256':sha(adapted.encode()),'exact_replacements':len(replacements)}

core_raw=bounded(rd.CORE_PATH,1<<20);need(sha(core_raw)==rd.CORE_SHA,'frozen mutation core source')
PINS[rd.CORE_PATH]=rd.CORE_SHA
PINS.update(core.PINS)
mutation_source=core_raw.decode()
mutation_node=next(n for n in ast.parse(mutation_source).body if isinstance(n,ast.FunctionDef) and n.name=='mutation')
mutation_original=ast.get_source_segment(mutation_source,mutation_node)
need(mutation_original.count('else (0,0,1)')==1,'exact current delete matched-count adaptation')
assert mutation_original.count("10000000000")==1
mutation_adapted=mutation_original.replace("10000000000","3000000000")
exec(compile(mutation_adapted,'trial24-current-delete-outcome','exec'),core.__dict__)
DERIVATION['mutation']={'original_sha256':sha(mutation_original.encode()),'adapted_sha256':sha(mutation_adapted.encode()),'delta':'reviewed Delete Matched=0, Modified=0, Deleted=1; explicit RPC3s mutation deadline bound'}

# No copied scorer or alternate ranking: use retained arithmetic only.
def scalar_response(response,request,state,truth):
 need(isinstance(response,dict) and response['Generation']==request['Generation'] and len(response['Neighbors'])==10,'generation/top10')
 counters=response['Counters'];need(all(type(v)is int and v>=0 for v in counters.values()),'integer native counters')
 need(counters['SelectedPartitions']==counters['HNSWServedPartitions']==counters['ReadProofs']==counters['RPCs']==counters['GenerationPins']==1 and counters['ExactScanPartitions']==counters['Retries']==counters['Redirects']==0 and counters['Candidates']>0 and counters['Edges']>0,'strict C1 native proof counters')
 nq=core.normalized(core.vectors32(request['Query']));seen=set();previous=None
 for neighbor in response['Neighbors']:
  ident,score=neighbor['ID'],neighbor['Score']
  need(isinstance(ident,str) and ident in state and ident not in seen and type(score) in (int,float) and math.isfinite(score),'unique known present neighbor')
  need(core.bits(core.score(nq,core.normalized(state[ident])))==core.bits(score),'returned canonical FP32 score bits')
  order=(-score,ident.encode('utf-8'));need(previous is None or previous<order,'native score/stable UTF8 ID ordering');previous=order;seen.add(ident)
 return len(seen & {n['ID'] for n in truth})/10
core.native=scalar_response
def changing_response(response,request,states,prefixes,qi,lo,hi):
 # Check unchanged scores once; changed scores/presence use native frozen bits.
 changed={row['ID'] for row in prefixes[0]['Changed'][qi]}
 need(len(changed)==4 and all([v['ID'] for v in p['Changed'][qi]]==[v['ID'] for v in prefixes[0]['Changed'][qi]] for p in prefixes),'stable changed-ID native row order')
 counters=response['Counters']
 need(response['Generation']==request['Generation'] and all(type(v)is int and v>=0 for v in counters.values()),'strict generation/integer native counters')
 need(counters['SelectedPartitions']==counters['HNSWServedPartitions']==counters['ReadProofs']==counters['RPCs']==counters['GenerationPins']==1 and counters['ExactScanPartitions']==counters['Retries']==counters['Redirects']==0 and counters['Candidates']>0 and counters['Edges']>0,'strict C1 native bounded counters')
 nq=core.normalized(core.vectors32(request['Query']))
 for n in response['Neighbors']:
  if n['ID'] not in changed:
   need(n['ID'] in states[0],'unknown unchanged neighbor')
   need(core.bits(core.score(nq,core.normalized(states[0][n['ID']])))==core.bits(n['Score']),'unchanged neighbor canonical scalar score')
 return cc.compatible_prefixes(response,prefixes,qi,lo,hi)
rd.changing_response=changing_response
def current_corpus_truth(truth,request,state):
 need(isinstance(truth,list) and len(truth)==10,'ten producer-computed corpus neighbors')
 nq=core.normalized(core.vectors32(request['Query']));seen=set();previous=None
 for n in truth:
  ident,score=n['ID'],n['Score']
  need(isinstance(ident,str) and ident.startswith('doc-') and ident in state and ident not in seen and type(score) in (int,float) and math.isfinite(score),'unique present corpus truth ID/finite score')
  need(core.bits(core.score(nq,core.normalized(state[ident])))==core.bits(score),'current corpus truth exact scalar score bits')
  order=(-score,ident.encode('utf-8'));need(previous is None or previous<order,'producer corpus score/stable ID ordering');previous=order;seen.add(ident)
 # Corpus maximal ranking is producer-computed; full native prefixtruth governs recall.
 return True
paired_node=next(n for n in ast.parse(read_source).body if isinstance(n,ast.FunctionDef) and n.name=='paired')
paired_original=ast.get_source_segment(read_source,paired_node)
needle="q['CorpusTruth']==baseline['CorpusTruth']"
need(paired_original.count(needle)==1,'one retained paired corpus invariant')
paired_adapted=paired_original.replace(needle,"(core.same_truth(q['CorpusTruth'],baseline['CorpusTruth']) if phase=='quiescent-before-mixed' else current_corpus_truth(q['CorpusTruth'],q['Request'],state))",1)
rd.core=core;rd.current_corpus_truth=current_corpus_truth
exec(compile(paired_adapted,READ+'::trial24-current-corpus-paired','exec'),rd.__dict__)
DERIVATION['paired']={'original_sha256':sha(paired_original.encode()),'adapted_sha256':sha(paired_adapted.encode()),'delta':'preserve pre corpus equality; post corpus scalar membership/order only; full native canonical prefix truth governs recall'}
exec(compile(adapted,READ+'::trial24-adapted-verifier','exec'),rd.__dict__)
def compare_prefixes(actual,expected):
 need(len(actual)==len(expected)==W.prefixes,'49 native prefixes')
 for p,want in zip(actual,expected):
  need(set(p)==set(want) and p==want,'native prefix structural/population/changed fields')
  need(type(p['Prefix'])is int and type(p['PopulationRows'])is int,'integer prefix scalars')
  for got,truth in zip(p['Truth'],want['Truth']):
   need(core.same_truth(got,truth),'native oracle truth ID/FP32 score bits')
  for rows in p['Changed']:
   for row in rows:
    need(type(row['Present'])is bool and type(row['ScoreBits'])is int and 0<=row['ScoreBits']<1<<32,'native changed presence/score bits')
def raw_originals(report,raw):
 return [core.field(piece,cc.operation_request(w)[0].capitalize())[1] for w,piece in core.pieces(raw,'Writes')]
def native_binding(native,raw,oracle,planned,praw,report,rraw,a,initial):
 cc.validate_prefix_oracles(oracle,a,initial)
 need(native['state']=='NATIVE_CANONICAL_PREFIX_ORACLES_GENERATED_PENDING_ROOT_VALIDATION','actual native pending packet')
 for k in ('RunID','Profile','source_head','source_tree','InitialPopulationSHA256'):
  need(native[k]==oracle[k],'native promoted binding '+k)
 need(native['InputInventorySHA256']==a['input_inventory_sha256'],'native exact input inventory')
 compare_prefixes(native['Prefixes'],oracle['Prefixes'])
 compare_prefixes(planned['Prefixes'],oracle['Prefixes']);compare_prefixes(report['Prefixes'],oracle['Prefixes'])
 for raw_report in (raw,praw,rraw):
  for prefix,piece in core.pieces(raw_report,'Prefixes'):
   need(sha(core.field(piece,'Truth')[1].encode())==prefix['Top10SHA256'],'actual native serialized truth digest')
 native_originals=core.pieces(raw,'OriginalRequests')
 need(len(native_originals)==W.originals,'48 native original requests')
 expected=[core.field(piece,cc.operation_request(w)[0].capitalize())[1] for w,piece in native_originals]
 need(raw_originals(planned,praw)==raw_originals(report,rraw)==expected,'native/planned/actual original request exact raw FP32 including signed zero')
 need(cc.encode_go_json(cc.original_requests(report['Writes']))==cc.encode_go_json(oracle['OriginalRequests']),'promoted originals canonical omitted-pointer/signed-zero binding')
 need(native['OriginalRequests']==oracle['OriginalRequests'],'native promoted original requests')
 for left,right in zip(native['OriginalRequests'],oracle['OriginalRequests']):
  kind=cc.operation_request(left)[0].capitalize()
  if kind=='Replace':need(core.vecbits(left[kind]['Vector'])==core.vecbits(right[kind]['Vector']),'promoted original vector signed-zero bits')

def validate_mutations(p,r,ptext,text):
 pairs=core.pieces(ptext,'Writes');origin=core.stamp(r['MeasuredOriginUTC'])
 for (w,raw),(planned,praw) in zip(core.pieces(text,'Writes'),pairs):
  core.mutation(w,raw,planned,praw,r['Admission']['Generation'],origin)
 retryorigin=core.stamp(r['RetryOriginUTC'])
 for w,raw in core.pieces(text,'Retries'):
  planned,praw=pairs[w['Ordinal']]
  core.mutation(w,raw,planned,praw,r['Admission']['Generation'],retryorigin,w['Ordinal'])
def population_accounting(C,a,pinned,nodes,states,r):
 cc.APPROVED=a
 results={}
 for phase,idx,floor in (('initial',0,a['baseline']['HighestCommitIndex']),('final',W.originals,r['RequiredAppliedIndex'])):
  expected=cc.population_plan(states[idx],floor,None if idx==0 else r['AuditPlan'])
  planraw=bounded(C/(phase+'-population-plan.json'),512<<10)
  need(planraw==cc.encode_go_json(expected)+b'\n','actual full-population raw plan including original FP32 bytes')
  packet=core.strict(bounded(C/(phase+'-population-audits.json')))
  identity=cc.population_identity(states[idx],128)
  need(packet['state']=='ALL4_SOURCE_POPULATION_OBSERVED_PENDING_ROOT_ACCEPTANCE' and packet['oracle']==identity,'actual complete population packet')
  observations=packet['observations'];need(len(observations)==4,'all four population observations')
  hashes=[]
  for observation,n in zip(observations,nodes):
   label=phase+'-population-'+n['node']
   files=list(C.glob('[0-9][0-9][0-9][0-9]-'+label+'.json'));need(len(files)==1,'one original population diagnostic, no replay')
   path=files[0];rec=core.strict(bounded(path,16<<20));stdout=bounded(path.with_suffix('.stdout'),8<<20);stderr=bounded(path.with_suffix('.stderr'),8<<20)
   need(rec['exit_code']==0 and not rec.get('timed_out',False) and rec['started_unix']<=rec['finished_unix'],'actual population command completed once')
   need(rec['stdout'].encode('utf-8','surrogateescape')==stdout and rec['stderr'].encode('utf-8','surrogateescape')==stderr,'raw population stdout/stderr')
   import shlex
   argv=['docker','exec','-i',n['cid'],a['server_cli_path'],'-mode','diagnostics','-config','/config.json','-expected-binary-sha256',n['server_sha256'],'-colocated-audit-plan','/dev/stdin']
   need(rec['argv']==['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','mikers@'+n['host'],shlex.join(argv)],'exact actual same-CID population diagnostic argv')
   need(rec['stdin_sha256']==sha(planraw) and rec['stdin_bytes']==len(planraw),'actual bounded raw population plan stdin')
   decoded=cc.validate_population_diagnostic(stdout,n,expected,identity)
   need(observation['node']==n['node'] and observation['cid']==n['cid'] and observation['diagnostics']==decoded,'actual raw diagnostic/observation binding')
   # Resource helper separately binds the sample commands and stop ordering.
   for key in ('before_samples','after_samples'):
    batches=observation[key];need(len(batches)==2 and {s['host'] for s in batches}=={'192.168.0.111','192.168.0.185'},'both retained population liveness brackets')
    targets=[t for batch in batches for t in batch['targets']]
    voters=[t for t in targets if t['role']=='voter'];need(len(voters)==4 and {t['node'] for t in voters}==set(cc.HOSTS),'all four population liveness voters')
    for t in voters:
     owner=next(v for v in nodes if v['node']==t['node']);cc.validate_voter(t['inspect'],owner)
     st=t['inspect']['State'];need(st['Running'] is True and st['Pid']>0 and not st['OOMKilled'],'all population voters LIVE/noOOM')
   hashes.append(sha(stdout))
  results[phase]={'rows':identity['Rows'],'population_sha256':identity['SHA256'],'plan_sha256':sha(planraw),'diagnostic_stdout_sha256':hashes,'scope':'raw same-CID authenticated diagnostics argv/attachment accounting; SSH execution authenticity and brackets/stop timing remain outer resource/lifecycle authority'}
 return results

def verify(manifest,manifest_sha,artifacts,inputs,native_path,native_sha,stdout_sha,mean_recall_floor):
 manifest_raw=bounded(manifest,1<<20);need(core.digest(manifest_sha) and sha(manifest_raw)==manifest_sha,'current manifest raw SHA')
 a=cc.validate_manifest(cc.strict_json(manifest_raw),COLLECTOR_SHA)
 need((a['source_head'],a['source_tree'])==(HEAD,TREE),'exact current reviewed source identity')
 C=Path(artifacts);I=Path(inputs)
 need(C.is_absolute() and C.is_dir() and not C.is_symlink() and I.is_absolute() and I.is_dir() and not I.is_symlink(),'actual artifact/input directories')
 need(bounded(C/'approved-inputs.json',1<<20)==manifest_raw,'exact retained manifest bytes')
 need(bounded(C/'collector-source.py',1<<20)==COLLECTOR_SHA_BYTES,'actual retained collector bytes')
 retained=cc.strict_json(bounded(C/'retained-input-pins.json',1<<20))
 need(set(retained)==set(a['local_pins']),'all retained approved pins, no missing/extra')
 total=0
 def retained_read(path,cap=64<<20):
  nonlocal total
  path=str(path)
  if path in a['local_pins']:
   row=retained[path];need(set(row)=={'sha256','retained_path'} and row['sha256']==a['local_pins'][path],'retained approved pin identity')
   rel=cc.input_name(row['retained_path']);raw=bounded(C/rel,cap);need(sha(raw)==row['sha256'],'actual retained pin bytes')
  else:
   prefix=cc.LOCAL_INPUT_ROOT+'/'
   need(path.startswith(prefix),'only exact staged input paths')
   raw=bounded(I/cc.input_name(path[len(prefix):]),cap)
  total+=len(raw);need(total<=512<<20,'bounded complete input reads');return raw
 saved_read=cc.read_bounded
 cc.read_bounded=retained_read
 try: pinned,inventory,nodes,initial=cc.prepare_local(a)
 finally:cc.read_bounded=saved_read
 for name,digest in inventory.items():
  need(sha(bounded(I/cc.input_name(name),64<<20))==digest,'actual complete sealed input bytes '+name)
 need(bounded(I/'input-inventory.json',1<<20)==pinned[cc.LOCAL_INPUT_ROOT+'/input-inventory.json'],'actual staged inventory bytes')
 need(bounded(C/'input-inventory.json',1<<20)==pinned[cc.LOCAL_INPUT_ROOT+'/input-inventory.json'],'actual retained inventory bytes')
 states=[dict(initial)]+[cc.apply_prefix(initial,cc.strict_json(pinned[a['receipts']['prefix_oracles']])['OriginalRequests'][:i]) for i in range(1,W.prefixes)]
 need(len(initial)==10005 and len(states[-1])==10002,'native changed population 10005 to10002')
 stdout=bounded(C/'stdout.jsonl');need(core.digest(stdout_sha) and sha(stdout)==stdout_sha and stdout.endswith(b'\n'),'root-pinned complete raw stdout')
 lines=stdout.splitlines();need(len(lines)==2,'exact two original JSONL events')
 events=[core.strict(line) for line in lines]
 need(all(set(e)=={'Event','Report'} for e in events) and [e['Event'] for e in events]==['planned','result'],'exact planned/result event order')
 p,r=[e['Report'] for e in events];need(cc.original_count(p)==cc.original_count(r)==W.originals,'exact campaign declared count');cc.sustained_shape(p,True);cc.sustained_shape(r);ptext=core.field(lines[0].decode(),'Report')[1];text=core.field(lines[1].decode(),'Report')[1]
 need(stdout==('{"Event":"planned","Report":'+ptext+'}\n{"Event":"result","Report":'+text+'}\n').encode(),'exact complete canonical producer wrappers')
 need(p['Profile']==r['Profile']=='changing-top10' and r['ResourceGateDir']==cc.MOUNT_GATE,'current profile/gate')
 oracle=cc.strict_json(pinned[a['receipts']['prefix_oracles']])
 native_raw=bounded(native_path,1<<20);need(core.digest(native_sha) and sha(native_raw)==native_sha,'actual native oracle raw pin')
 native_binding(core.strict(native_raw),native_raw.decode(),oracle,p,ptext,r,text,a,initial)
 validate_mutations(p,r,ptext,text)
 cc.validate_causal_report(r,initial)
 need(cc.apply_originals(initial,r['Writes'])==states[-1],'six actual ACK outcome/population effects')
 query_raw=bounded(I/'dataset/queries.f32',1<<20)
 need(len(query_raw)==16*128*4 and sha(query_raw)==inventory['dataset/queries.f32'],'actual sixteen FP32 query input bytes')
 config=core.strict(pinned[a['receipts']['config']]);init=config['VectorInitialization']
 need(admission_generation := r['Admission']['Generation'], 'actual generation present')
 need(admission_generation=={'Index':init['IndexDefinition']['name'],'Generation':init['Generation']},'actual configured generation')
 for idx,(vector,q) in enumerate(zip(struct.iter_unpack('<128f',query_raw),r['Admission']['Queries'])):
  req=q['Request']
  for key,value in (('Version',1),('Generation',admission_generation),('Metric','cosine'),('TopK',10),('Probes',1),('EfSearch',init['IndexDefinition']['ef_search']),('Consistency','linearizable_generation_snapshot'),('Limits',{'RequestBytes':1<<20,'CandidateBytes':8<<20,'ResponseBytes':1<<20,'MergeEntries':32})):
   need(req[key]==value,'actual native query contract '+key)
  need(core.vecbits(vector)==core.vecbits(q['Request']['Query']),'actual query FP32 bits including signed zero')
  need(core.same_truth(q['Truth'],oracle['Prefixes'][0]['Truth'][idx]),'admitted native prefix0 truth bits')
 admission=r['Admission']
 for k,expected in (('BinarySHA256',a['driver_sha256']),('ConfigSHA256',a['config_sha256']),('BootstrapSHA256',a['bootstrap_sha256']),('HighestCommitIndex',a['baseline']['HighestCommitIndex']),('ProbeSHA256',inventory['probe.jsonl']),('ProvenanceSHA256',inventory['provenance.json']),('ManifestSHA256',inventory['dataset/manifest.json'])):
  need(admission[k]==expected,'actual frozen admission binding '+k)
 need(admission['Verdict']=='ACCEPTED_INPUTS_CHANGING_TOP10_PENDING_RUNTIME' and admission['ScoreContract']=='fp32_normalized_cosine_binary64_accum_score_desc_stable_id_asc_best_duplicate_v1','native admission contract')
 # Route the generic config reads through actual retained bytes, not stale paths.
 saved_rd,saved_au=core.bounded,au.c.bounded
 core.bounded=au.c.bounded=retained_read
 try:
  read=rd.verifier(a,p,r,ptext,text,states,mean_recall_floor)
  audit=au.verifier(a,p,r,ptext,text,states)
 finally:core.bounded=saved_rd;au.c.bounded=saved_au
 population=population_accounting(C,a,pinned,nodes,states,r)
 return {'population':population,'disposition':'TRIAL24_READ_AUDIT_ACCOUNTING_ONLY','campaign_acceptance':False,'manifest_sha256':manifest_sha,'stdout_sha256':stdout_sha,'native_oracle_sha256':native_sha,'promoted_oracle_sha256':a['local_pins'][a['receipts']['prefix_oracles']],'source_head':HEAD,'source_tree':TREE,'source_pins':PINS,'read':read,'audit':audit,'derivation':DERIVATION,'limits':LIMITS}
COLLECTOR_SHA_BYTES=collector_source.encode()
need(sha(COLLECTOR_SHA_BYTES)==COLLECTOR_SHA,'authenticated retained collector source bytes')
def self_check():
 # Synthetic counterfactual native tables only; no full structural campaign claim.
 names=['n%02d'%i for i in range(10)]
 response={'Neighbors':[{'ID':x,'Score':1.0} for x in names],'Counters':{'SelectedPartitions':1,'HNSWServedPartitions':1,'ReadProofs':1,'ExactScanPartitions':0,'Retries':0,'Redirects':0}}
 changed=[{'ID':'changed%d'%i,'Present':True,'ScoreBits':1065353216} for i in range(4)]
 prefixes=[{'Prefix':i,'Changed':[changed],'Truth':[[{'ID':x,'Score':1.0} for x in (names if i==0 else names[:4]+['x%d'%j for j in range(6)])]]} for i in range(7)]
 need(cc.compatible_prefixes(response,prefixes,0,0,6)==(127,.4),'all seven mask/min recall; never best prefix')
 need(cc.compatible_prefixes(response,prefixes,0,0,0)==(1,1.0),'strict prefix0 warmup')
 bad=json.loads(json.dumps(prefixes));bad[2]['Changed'][0][0]={'ID':names[0],'Present':False,'ScoreBits':0}
 need(cc.compatible_prefixes(response,bad,0,0,6)==(123,.4),'one stale changed-ID prefix removed')
 for label,fn in [
  ('future outside range',lambda:cc.compatible_prefixes(response,bad,0,2,2)),
  ('native truth score signedzero',lambda:compare_prefixes([dict(p,Truth=[[{'ID':x,'Score':-0.0} for x in names]]) for p in prefixes],[dict(p,Truth=[[{'ID':x,'Score':0.0} for x in names]]) for p in prefixes]))]:
  try:fn()
  except (AssertionError,ValueError,KeyError,TypeError):pass
  else:raise AssertionError('synthetic refusal missed: '+label)
 need(core.bits(core.strict('[-0]')[0])==struct.pack('<I',0x80000000),'exact raw signedzero parser')
 vector=[1.0]+[0.0]*127;state={name:vector for name in names}
 response['Generation']={'Index':'embedding','Generation':1}
 response['Counters'].update(RPCs=1,GenerationPins=1,Candidates=10,Edges=10)
 request={'Generation':response['Generation'],'Query':vector}
 need(changing_response(response,request,[state]*7,prefixes,0,0,6)==(127,.4),'actual adapted response scorer/mask/min recall path')
 corpus_state={'doc-%06d'%i:vector for i in range(10)};corpus_truth=[{'ID':name,'Score':1.0} for name in corpus_state]
 need(current_corpus_truth(corpus_truth,request,corpus_state),'current present corpus scalar truth')
 corrupt=json.loads(json.dumps(corpus_truth));corrupt[0]['ID']='doc-999999'
 try:current_corpus_truth(corrupt,request,corpus_state)
 except (AssertionError,ValueError):pass
 else:raise AssertionError('deleted/unknown corpus truth ID admitted')

 wrong=json.loads(json.dumps(response));wrong['Neighbors'][0]['Score']=.5
 try:changing_response(wrong,request,[state]*7,prefixes,0,0,6)
 except (AssertionError,ValueError):pass
 else:raise AssertionError('wrong unchanged scalar score admitted')
 return {'state':'PURE_SYNTHETIC_SELF_CHECK_PASS_NOT_RUNTIME','checks':10,'native_ranking_executed':False,'campaign_acceptance':False,'derivation':DERIVATION}
def main():
 parser=argparse.ArgumentParser(description=__doc__)
 parser.add_argument('--self-check',action='store_true')
 for name in ('manifest','manifest-sha256','artifacts','inputs','native-oracle','native-oracle-sha256','stdout-sha256'):parser.add_argument('--'+name)
 parser.add_argument('--mean-recall-floor',type=float)
 args=parser.parse_args()
 values=[args.manifest,args.manifest_sha256,args.artifacts,args.inputs,args.native_oracle,args.native_oracle_sha256,args.stdout_sha256,args.mean_recall_floor]
 if args.self_check:
  need(all(v is None for v in values),'self-check cannot consume actual inputs');answer=self_check()
 else:
  need(all(v is not None for v in values),'all current final manifest/artifact/input/native/stdout pins and recall floor required')
  answer=verify(*values)
 print(json.dumps(answer,indent=2,allow_nan=False))
if __name__=='__main__':main()
