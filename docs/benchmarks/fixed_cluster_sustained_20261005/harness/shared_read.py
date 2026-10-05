from source_paths import source_path
"""Pure retained-read/accounting gate only; never campaign acceptance.
Input ptext/text are exact Report JSON strings, states are seven populations
already authenticated/reconstructed by the independent full-oracle gate.
No subprocess/network/runtime or old campaign top-level is executed.
"""
import argparse, copy, hashlib, importlib.util, json, math, sys
from pathlib import Path
CORE_PATH=source_path('/tmp/gomap-4994-mixed-window-artifact-verify-root-v7.py')
CORE_SHA='0182750a8662e3beed1748527f1c24549470bfa275852877f7408233883412a6'
sys.dont_write_bytecode=True
if not __debug__: raise RuntimeError('ordinary Python required')
if hashlib.sha256(Path(CORE_PATH).read_bytes()).hexdigest()!=CORE_SHA:
 raise ValueError('frozen core pin mismatch')
spec=importlib.util.spec_from_file_location('mixed_frozen_core',CORE_PATH)
c=importlib.util.module_from_spec(spec);spec.loader.exec_module(c)
need=c.need
ZERO='0001-01-01T00:00:00Z'
NODES=('node-a','node-b','node-c','node-d')
RPC=3_000_000_000
DURATION=300_000_000_000
LIMIT=65536
CAP=128<<20
LIMITATIONS=[
 'Read/accounting gate only: no transport, physical WAL, local audit, source authority, resource, lifecycle or whole-campaign acceptance.',
 'Caller must independently authenticate source/build/manifest/stdout and reconstruct all seven full canonical populations/truths; supplied states are not network source observations.',
 'Successful window responses and paired recall have no producer response-SHA field. Exact retained JSON is checked and independently hashed; no missing producer hash is invented.',
 'Warmup and paired recall monotonic origins are not serialized. Absolute phase alignment/deadline-origin binding is unproved; actual serialized deadlines and relative RPC durations are checked.',
 'Measured dispatch reservation timestamp is not serialized. Observed dispatch overshoot is reported, not hidden or retried.',
 'Read QPS includes validation/retention/drain and writer join; C1 client-call overlap proves neither server critical-section overlap nor sustained capacity.'
]

def numeric(v,label):
 need(type(v) in (int,float) and math.isfinite(v),label+' finite number');return v

def interval(x,label,limit=420_000_000_000):
 s,e=x['StartNS'],x['EndNS']
 need(type(s)is int and type(e)is int and 0<=s<e<=limit and e-s<=RPC,label+' actual bounded call interval')
 return s,e

def successful(x,label):
 need(x['Outcome']=='succeeded' and x['Error']==x['ErrorCode']=='',label+' complete success; no UNKNOWN/failure discard')

def raw_array(raw,key,values):
 pairs=c.pieces(raw,key);need([v for v,_ in pairs]==values,'exact retained '+key+' array');return pairs

def raw_field(raw,key,value):
 v,s=c.field(raw,key);need(v==value,'exact retained '+key);return s

def logical(raw,req,expected_raw,expected,label):
 need(c.request_zero(req)==expected,label+' immutable request parameters/token')
 wire=c.request_wire(expected_raw,req['Deadline'])
 need(raw==wire,label+' exact serialized request preserves FP32 decimals')
 return c.sha(raw.encode())

def timing(response):
 need(all(type(v)is int and v>=0 for v in response['Timing'].values()),'nonnegative integer server timings')

def readiness(history,prefix,epoch,maxrounds,label):
 need(isinstance(history,list) and 4<=len(history)<=4*maxrounds and len(history)%4==0,label+' complete retained readiness rounds')
 order=None
 for offset in range(0,len(history),4):
  group=history[offset:offset+4];ids=[v['RequestedNode'] for v in group]
  need(set(ids)==set(NODES) and len(set(ids))==4,label+' four unique requested nodes per round')
  if order is None:order=ids
  need(ids==order and all(type(v['Round'])is int and v['Round']==offset//4 for v in group),label+' exact serial contiguous readiness rounds')
 final=history[-4:]
 for v in final:
  s=v['State'];need(v['Error']==v['ErrorCode']==s.get('Error','')=='' and s['NodeID']==v['RequestedNode'] and s['Live'] is True and s['Ready'] is True and s['Draining'] is False and s['VectorPhase']=='active' and s['CatalogEpoch']==epoch,label+' final active catalog identity')
  need(len(s['Groups'])==1,'one supported readiness group');g=s['Groups'][0]
  need(g['GroupID']=='group-a' and g['Ready'] is True and g.get('Error','')=='' and type(g['LocalAppliedIndex'])is int and g['LocalAppliedIndex']>=prefix,label+' final actual applied floor')
 return {'rounds':len(history)//4,'earlier_rounds_retained':len(history)//4-1,'history_sha256':c.sha(json.dumps(history,separators=(',',':')).encode()),'required_applied_index':prefix}

def threshold(v):
 numeric(v,'root external mean recall floor')
 need(0<=v<=1,'configured recall threshold range');return v

def check_queries(planned,actual,praw,araw,label):
 need(len(planned)==len(actual)==16,label+' sixteen queries')
 pq=raw_array(praw,'Queries',planned);aq=raw_array(araw,'Queries',actual)
 for i,((q,raw),(z,zraw)) in enumerate(zip(pq,aq)):
  need(q['QueryID']==z['QueryID']=='query-%06d'%i,label+' ordered query IDs')
  reqraw=raw_field(raw,'Request',q['Request']);actualraw=raw_field(zraw,'Request',z['Request'])
  need(q['Request']==z['Request'] and reqraw==actualraw and q['Request']['Deadline']==ZERO and q['Request']['VisibilityToken'] in (None,''),label+' immutable zero-deadline token-free request')
  need(q['RequestSHA256']==z['RequestSHA256']==c.sha(reqraw.encode()),label+' logical serialized request hash')
  need(q['Outcome']==z['Outcome']=='unissued' and q['Response'] is z['Response'] is None and q['StartNS']==q['EndNS']==z['StartNS']==z['EndNS']==0 and q['RecallAt10'] is z['RecallAt10'] is None,label+' admission remains unissued')
  need(q['Error']==q['ErrorCode']==z['Error']==z['ErrorCode']=='',label+' admission errors empty')
 return pq

def paired(rec,raw,base,baseq,state,prefixtruth,phase,index,minrec):
 need(rec['Phase']==phase and len(rec['Queries'])==16 and rec['Error']=='','paired phase complete')
 for key in ('Version','Kind','RunID','ConfigSHA256','BootstrapSHA256','ManifestSHA256','ProvenanceSHA256','ProbeSHA256','BinarySHA256','ConfigIdentity','Generation','Manifest','Provenance','ScoreContract','Timeout','RPCTimeout'):
  need(rec[key]==base[key],'paired immutable admission '+key)
 need(rec['PopulationRows']==len(state) and rec['PopulationSHA256']==c.population(state) and rec['HighestCommitIndex']==index,'paired actual acknowledged population/position')
 need(rec['Counts']==c.account(rec['Queries']) and rec['Counts']['Succeeded']==16,'paired full attempt accounting')
 previous=0;values=[];hashes=[]
 for i,(q,s) in enumerate(raw_array(raw,'Queries',rec['Queries'])):
  successful(q,'paired query');start,end=interval(q,'paired query');need(start>=previous,'serial paired queries');previous=end
  baseline,braw=baseq[i];reqraw=raw_field(s,'Request',q['Request']);expectedraw=raw_field(braw,'Request',baseline['Request'])
  need(q['QueryID']==baseline['QueryID'] and q['RequestSHA256']==baseline['RequestSHA256'] and q['CorpusTruth']==baseline['CorpusTruth'],'paired logical identity')
  wirehash=logical(reqraw,q['Request'],expectedraw,baseline['Request'],'paired query');c.stamp(q['Request']['Deadline'])
  response_raw=raw_field(s,'Response',q['Response']);need(len(response_raw.encode())<=32768,'paired response retention bound');timing(q['Response'])
  need(c.same_truth(q['Truth'],prefixtruth[i]),'paired full canonical truth bits')
  value=c.native(q['Response'],q['Request'],state,prefixtruth[i]);numeric(q['RecallAt10'],'paired recall');need(q['RecallAt10']==value,'paired exact recall');values.append(value)
  hashes.append((wirehash,c.sha(response_raw.encode())))
 mean=sum(values)/16;numeric(rec['MeanRecallAt10'],'paired mean recall');need(rec['MeanRecallAt10']==mean and mean>=minrec,'paired configured mean recall threshold')
 return {'mean_recall_at_10':mean,'raw_query_ledger_sha256':c.sha(raw.encode()),'wire_response_hash_pairs':hashes}

def config_epoch(raw,a):
 need(c.sha(raw)==a['config_sha256']==a['local_pins'][a['receipts']['config']],'actual approved config bytes/hash')
 config=c.strict(raw);epoch=config['VectorInitialization']['CatalogEpoch']
 need(type(epoch)is int and epoch>0,'actual admitted catalog epoch')
 need({v['ID'] for v in config['Nodes']}==set(NODES) and len(config['Nodes'])==4 and len(config['Groups'])==1 and config['Groups'][0]['ID']=='group-a','actual configured four-voter one-group scope')
 return epoch

def causal_range(writes,start,end):
 need(0<=start<end,'search monotonic interval');lower=upper=0
 for i,w in enumerate(writes):
  if w['Outcome']=='succeeded' and w['EndNS']<=start:lower=i+1
  if w['Invoked'] and w['StartNS']<=end:upper=i+1
 need(0<=lower<=upper<=len(writes)<=63,'causal interval');return lower,upper

def original_count(report):
 n=report.get('Originals',0)
 need(type(n) is int and (n==0 or 6<=n<=63),'declared original count')
 return n or 6

def verifier(a,p,r,ptext,text,states,mean_recall_floor=None):
 """Return this gate's proof only; raise on missing/malformed/inconsistent evidence."""
 need(c.strict(ptext)==p and c.strict(text)==r,'exact raw Report object bindings')
 count=original_count(r);need(original_count(p)==count,'planned/actual original count');need(len(states)==count+1 and all(isinstance(s,dict) and s for s in states),'seven caller-authenticated populations')
 minimum=threshold(mean_recall_floor)
 fixed={'Version':1,'Kind':'fixed_cluster_mixed_window_v1','Concurrency':1,'WarmupPlanned':64,'MaxAttempts':LIMIT,'OutputBytes':CAP,'RequestedDuration':DURATION,'PaceInterval':5_000_000_000}
 for k,v in fixed.items():need(p[k]==r[k]==v,'fixed campaign '+k)
 need(r['Verdict']=='ACCEPT_MIXED_INVARIANT_RECALL_WINDOW_OBSERVATION_PENDING_ROOT_SHUTDOWN_VERIFICATION' and r['Error']=='' and r['Truncated'] is False and r['StopReason']=='window_elapsed','complete observation required; no failure erased')
 duration=r['ActualDurationNS'];need(type(duration)is int and DURATION<=duration<=420_000_000_000,'actual window+drain bound')
 origin=c.stamp(r['MeasuredOriginUTC']);base=r['Admission'];pb=p['Admission']
 need(base['RunID']==pb['RunID']==c.QUERY_RUN and base['Phase']==pb['Phase']=='post-only' and base['Timeout']==pb['Timeout']==420_000_000_000 and base['RPCTimeout']==pb['RPCTimeout']==RPC,'fixed fresh admission')
 for k in ('PopulationRows','PopulationSHA256','HighestCommitIndex','Generation','ConfigIdentity','ScoreContract','Manifest','Provenance','ConfigSHA256','BootstrapSHA256','BinarySHA256','ProbeSHA256','ManifestSHA256','ProvenanceSHA256'):
  need(base[k]==pb[k],'immutable admission '+k)
 need(base['PopulationRows']==len(states[0]) and base['PopulationSHA256']==c.population(states[0]),'supplied baseline population binding')
 pra=raw_field(ptext,'Admission',pb);ara=raw_field(text,'Admission',base)
 queries=check_queries(pb['Queries'],base['Queries'],pra,ara,'admission')
 for report in (p,r):
  for key in ('Counts','WarmupCounts','WriteCounts','RetryCounts'):need(all(type(v)is int and v>=0 for v in report[key].values()),'exact integer count scalars '+key)
 need(p['Attempts'] in (None,[]) and p['ReadPrefixes'] in (None,[]) and p['Visibility'] in (None,[]) and p['VisibilityEvidence'] in (None,[]) and p['Retries'] in (None,[]),'planned event contains no invented calls')
 for key,limit in (('Counts',LIMIT),('WarmupCounts',64),('WriteCounts',count),('RetryCounts',2)):
  need(p[key]==c.account([],limit),'planned full unissued budget '+key)
 need(len(p['Writes'])==len(r['Writes'])==count and len(r['Retries'])==2 and all(w['Invoked'] is True for w in r['Writes']+r['Retries']),'complete mutation invocation evidence for causal ledger')
 for i,w in enumerate(p['Writes']):
  need(w['Ordinal']==i and w['Outcome']=='unissued' and w['Invoked'] is False and w['StartNS']==w['EndNS']==0 and w.get('Response') is None,'planned original mutation remains unissued')
 for i,w in enumerate(r['Writes']):
  need(w['Ordinal']==i and w['StartNS']>=i*5_000_000_000 and w['EndNS']<=duration and (i==0 or w['StartNS']>=r['Writes'][i-1]['EndNS'] and w['StartNS']>=r['Writes'][i-1]['StartNS']+5_000_000_000),'actual serial original invocation boundaries')
 need([w['Ordinal'] for w in r['Retries']]==[0,2] and r['Retries'][1]['StartNS']>=r['Retries'][0]['EndNS'],'original slot0/slot2 serial retry accounting')
 for w in r['Writes']+r['Retries']:successful(w,'mutation ledger');interval(w,'mutation ledger')
 need(r['WriteCounts']==c.account(r['Writes'],count) and r['RetryCounts']==c.account(r['Retries'],2),'complete mutation attempt accounting')
 floor=max(w['Response']['AppliedIndex'] for w in r['Writes']+r['Retries'])
 need(r['RequiredAppliedIndex']==floor and r['HighestNewCommitIndex']==max(w['Response']['CommitIndex'] for w in r['Writes']),'ACK-only highest original and observed applied floor')
 retryorigin=c.stamp(r['RetryOriginUTC']);need(retryorigin>=origin+duration,'after-drain retry origin')
 bounds=r['ResourceBoundaries'];need(len(bounds)==2,'retained phase boundary observations')
 ready,done=bounds;need(ready['Phase']=='ready' and done['Phase']=='done' and ready['RunID']==done['RunID']==c.QUERY_RUN and ready['Nonce']==done['Nonce'] and c.digest(ready['Nonce'],32),'phase observation identity')
 need(ready['MeasuredOriginUTC']==ZERO and ready['ActualDurationNS']==0 and ready['StopReason']=='' and done['MeasuredOriginUTC']==r['MeasuredOriginUTC'] and done['ActualDurationNS']==duration and done['StopReason']==r['StopReason'],'phase observation values')
 need(c.stamp(ready['PublishedUTC'])<=c.stamp(ready['AcknowledgedUTC'])<=origin and origin+duration<=c.stamp(done['PublishedUTC'])<=c.stamp(done['AcknowledgedUTC'])<=retryorigin,'observed measurement/drain/gate/retry ordering')
 for b in bounds:need(type(b['WaitNS'])is int and b['WaitNS']>=0,'bounded phase wait scalar')
 truth=p['Prefixes'][0]['Truth'];need(len(p['Prefixes'])==count+1 and all(len(x['Truth'])==16 for x in p['Prefixes']),'sixteen seven-prefix oracle rows')
 need(p['Prefixes']==r['Prefixes'],'immutable prefix proof ledger')
 attempts=raw_array(text,'Attempts',r['Attempts']);phases={key:[(v,s) for v,s in attempts if v['Phase']==key] for key in ('warmup','measured')}
 need(len(attempts)==sum(map(len,phases.values())) and len(phases['warmup'])==64 and 0<len(phases['measured'])<=LIMIT,'all retained warmup and measured calls; unissued suffix')
 need([v['Phase'] for v,_ in attempts]==['warmup']*64+['measured']*len(phases['measured']),'canonical final ledger phase order')
 coverage=[0]*16;latency=[];recalls=[];retained=0;hashchain=hashlib.sha256();overlap=completed=0;maxovershoot=0;claims=[]
 for phase,pairs in phases.items():
  previous=0;deadline_previous=0
  for ordinal,(x,raw) in enumerate(pairs):
   need(type(x['Ordinal'])is int and type(x['Worker'])is int and x['Ordinal']==ordinal and x['Worker']==0,'contiguous C1 issue ownership');successful(x,'window call')
   start,end=interval(x,'window call',duration if phase=='measured' else 420_000_000_000)
   need(start>=previous,'one outstanding C1 API call');previous=end
   q,qraw=queries[ordinal%16];need(x['QueryID']==q['QueryID'] and x['RequestSHA256']==q['RequestSHA256'],'cyclic exact logical query hash')
   request_raw=raw_field(qraw,'Request',q['Request']);wire=c.request_wire(request_raw,x['Deadline']);deadline=c.stamp(x['Deadline'])
   need(deadline>=deadline_previous,'serial actual deadline order');deadline_previous=deadline
   if phase=='measured':need(origin+end<=deadline<=origin+start+RPC,'actual measured deadline binds call/origin/RPC')
   else:need(deadline<=origin+RPC,'warmup deadline cannot follow measured origin by more than its RPC budget')
   response_raw=raw_field(raw,'Response',x['Response']);need(x['ResponseSHA256']=='' and x['ResponseBytes']==len(response_raw.encode())<=32768,'actual successful window response bytes; no producer hash field')
   timing(x['Response']);lo,hi=(0,0) if phase=='warmup' else causal_range(r['Writes'],start,end)
   matched,value=c.one_prefix(x['Response'],q['Request'],states,truth[ordinal%16],lo,hi)
   numeric(x['RecallAt10'],'window recall');need(x['RecallAt10']==value,'independent single-prefix exact FP32/recall')
   hashchain.update((c.sha(wire.encode())+c.sha(response_raw.encode())+'\n').encode());retained+=len(raw.encode())+1
   if phase=='measured':
    claims.append(dict(Ordinal=ordinal,Lower=lo,Upper=hi,Matched=matched));coverage[ordinal%16]+=1;latency.append(end-start);recalls.append(value);maxovershoot=max(maxovershoot,start-DURATION)
    overlap+=any(start<w['EndNS'] and end>w['StartNS'] for w in r['Writes'])
    completed+=any(start>=w['StartNS'] and end<w['EndNS'] for w in r['Writes'])
 actual_claims=r['ReadPrefixes'];need(all(type(v['Ordinal']) is int for v in actual_claims),'integer claim ordinal');need(all(type(v.get('CompatibleMask')) is int and 0<v['CompatibleMask']<1<<64 for v in actual_claims),'exact uint64 compatible mask');by_ordinal={v['Ordinal']:v for v in actual_claims};need(len(by_ordinal)==len(actual_claims)==len(claims) and all(by_ordinal.get(v['Ordinal'])==v for v in claims),'every actual final single-prefix causal claim')
 for phase,key,limit,completions in (('warmup','WarmupCounts',64,'WarmupCompletions'),('measured','Counts',LIMIT,'Completions')):
  vals=[x for x,_ in phases[phase]];need(r[key]==c.account(vals,limit) and r[completions]==len(vals),'exact attempted/completed/unissued accounting '+phase)
 need(r['RetainedAttemptBytes']==retained and r['PlannedEventBytes']==len(('{"Event":"planned","Report":'+ptext+'}\n').encode()),'exact retained/planned byte accounting')
 need(len(('{"Event":"planned","Report":'+ptext+'}\n{"Event":"result","Report":'+text+'}\n').encode())<=CAP,'complete pair output byte bound')
 need(all(coverage) and coverage==r['MeasuredQueryAttempts']==r['MeasuredQuerySucceeded'],'sixteen measured query coverage')
 need(r['SuccessLatency']==c.percent(latency) and r['FailureLatency']==c.percent([]),'exact nearest-rank measured latency populations')
 qps=len(latency)/(duration/1e9)
 need(math.isclose(numeric(r['AttemptsQPS'],'attempt QPS'),qps,rel_tol=1e-14) and math.isclose(numeric(r['SuccessfulQPS'],'successful QPS'),qps,rel_tol=1e-14),'attempt/success QPS includes actual drain')
 mean=sum(recalls)/len(recalls);numeric(r['MeanRecallAt10'],'measured mean recall');need(r['MeanRecallAt10']==mean and mean>=minimum,'configured measured mean recall threshold')
 need(r['OverlappingSearches']==overlap and r['CompletedSearchesDuringMutation']==completed and completed>0,'exact client call overlap/contained completed search accounting')
 need(r['WriterLatencyNs']==[w['EndNS']-w['StartNS'] for w in r['Writes']],'six actual writer latencies')
 need(len(r['Visibility'])==len(r['VisibilityEvidence'])==len(r['VisibilityOriginUTC'])==count,'six post-ACK exact-token read proofs retained')
 visibility_hashes=[];previous_end=origin
 for i,(op,raw) in enumerate(raw_array(text,'Visibility',r['Visibility'])):
  successful(op,'visibility');start,end=interval(op,'visibility');v_origin=c.stamp(r['VisibilityOriginUTC'][i]);w=r['Writes'][i]
  need(v_origin>=origin+w['EndNS'] and v_origin>=previous_end and v_origin+end<=origin+duration and (i==5 or v_origin+end<=origin+r['Writes'][i+1]['StartNS']),'post-ACK probe completes before next mutation/drain');previous_end=v_origin+end
  need(op['Ordinal']==i and op['Kind']=='search' and op['Phase']=='post-ack-current-prefix' and op['InsertRequest'] is None,'declared exact visibility slot')
  q,qraw=queries[i%16];expected=copy.deepcopy(q['Request']);expected['VisibilityToken']=w['Response']['VisibilityToken'];need(expected['VisibilityToken'] and op['SearchRequest']['VisibilityToken']==expected['VisibilityToken'],'actual original ACK token floor')
  base_raw=raw_field(qraw,'Request',q['Request']);_,tokenraw=c.field(raw_array(text,'Writes',r['Writes'])[i][1],'Response');_,token=c.field(tokenraw,'VisibilityToken')
  marker='"VisibilityToken":null' if q['Request']['VisibilityToken'] is None else '"VisibilityToken":""';need(base_raw.count(marker)==1,'one raw token field');expected_raw=base_raw.replace(marker,'"VisibilityToken":'+token,1)
  reqraw=raw_field(raw,'SearchRequest',op['SearchRequest']);wirehash=logical(reqraw,op['SearchRequest'],expected_raw,expected,'visibility')
  need(op['RequestSHA256']==wirehash and v_origin+end<=c.stamp(op['SearchRequest']['Deadline'])<=v_origin+start+RPC,'exact visibility request hash/deadline')
  rr=raw_field(raw,'SearchResponse',op['SearchResponse']);ev=r['VisibilityEvidence'][i];need(ev['Bytes']==len(rr.encode())<=32768 and ev['SHA256']==c.sha(rr.encode()),'actual visibility response bytes/hash')
  c.native(op['SearchResponse'],op['SearchRequest'],states[i+1],p['Prefixes'][i+1]['Truth'][i%16]);timing(op['SearchResponse']);visibility_hashes.append((wirehash,ev['SHA256']))
 pairedproof={}
 for name,idx,phase,commit in (('PreRecall',0,'quiescent-before-mixed',base['HighestCommitIndex']),('PostRecall',count,'quiescent-after-mixed',r['HighestNewCommitIndex'])):
  raw=raw_field(text,name,r[name]);pairedproof[name]=paired(r[name],raw,base,queries,states[idx],p['Prefixes'][idx]['Truth'],phase,commit,minimum)
 config_path=a['receipts']['config'];config_raw=c.bounded(config_path,1<<20)
 epoch=config_epoch(config_raw,a)
 readproof={'initial':readiness(base['ReadinessBefore'],base['HighestCommitIndex'],epoch,1,'initial'),
  'after_retry':readiness(base['ReadinessAfter'],floor,epoch,64,'after retry'),
  'after_post_recall':readiness(r['PostRecall']['ReadinessAfter'],floor,epoch,64,'after post recall')}
 return {'verdict':'READ_ACCOUNTING_GATE_PASS_ONLY','campaign_acceptance':False,'counts':r['Counts'],'warmup_counts':r['WarmupCounts'],'readiness':readproof,'attempt_response_hash_chain_sha256':hashchain.hexdigest(),'visibility_wire_response_hash_pairs':visibility_hashes,'paired':pairedproof,'root_external_mean_recall_floor':minimum,'mean_recall_at_10':mean,'successful_qps':qps,'success_latency':r['SuccessLatency'],'query_coverage':coverage,'overlapping_searches':overlap,'completed_searches_during_mutation':completed,'max_measured_dispatch_overshoot_ns':maxovershoot,'retained_attempt_bytes':retained,'raw_planned_report_sha256':c.sha(ptext.encode()),'raw_result_report_sha256':c.sha(text.encode()),'limitations':LIMITATIONS}

def self_check():
 """In-memory producer-shaped fixture; only approved-config read is substituted.
 Calls actual verifier and gate helpers. No network/subprocess/file creation.
 This is test evidence, never evidence of an actual RF4 campaign.
 """
 def encode(v):return json.dumps(v,separators=(',',':'))
 def utc(offset):
  seconds,ns=divmod(offset,1_000_000_000)
  dt=c.datetime.datetime(2026,10,4)+c.datetime.timedelta(seconds=seconds)
  return dt.strftime('%Y-%m-%dT%H:%M:%S')+'.%09dZ'%ns
 vector=[1.0]+[0.0]*127;state={'doc%02d'%i:vector for i in range(14)};states=[copy.deepcopy(state) for _ in range(7)]
 generation={'Index':'embedding_graph','Generation':1};truth=c.top(state,vector)
 counters={k:1 for k in ('SelectedPartitions','HNSWServedPartitions','ReadProofs','RPCs','GenerationPins','Candidates','Edges')};counters.update(ExactScanPartitions=0,Retries=0,Redirects=0)
 response={'Generation':generation,'Neighbors':truth,'Counters':counters,'Timing':{'SearchNS':1}}
 request={'VisibilityToken':None,'Version':1,'Generation':generation,'Query':vector,'Metric':'cosine','TopK':10,'Probes':1,'EfSearch':32,'Consistency':'linearizable_generation_snapshot','Limits':{'RequestBytes':1048576,'CandidateBytes':8388608,'ResponseBytes':1048576,'MergeEntries':32},'Deadline':ZERO}
 queries=[]
 for i in range(16):queries.append(dict(QueryID='query-%06d'%i,RequestSHA256=c.sha(encode(request).encode()),Outcome='unissued',ErrorCode='',Error='',Request=copy.deepcopy(request),StartNS=0,EndNS=0,Response=None,CorpusTruth=truth,Truth=truth,RecallAt10=None))
 base=dict(Version=1,Kind='fixed_cluster_mixed_window_admission_v1',RunID=c.QUERY_RUN,Phase='post-only',ConfigSHA256='config',BootstrapSHA256='boot',ManifestSHA256='manifest',ProvenanceSHA256='provenance',ProbeSHA256='probe',BinarySHA256='binary',ConfigIdentity={'Authenticated':True},Generation=generation,Manifest={},Provenance={},ScoreContract='canonical',PopulationRows=len(state),PopulationSHA256=c.population(state),HighestCommitIndex=10,Timeout=420_000_000_000,RPCTimeout=RPC,Queries=queries)
 def ready(round,floor):
  return [dict(Round=round,RequestedNode=n,ErrorCode='',Error='',State=dict(NodeID=n,Live=True,Ready=True,Draining=False,VectorPhase='active',CatalogEpoch=7,Groups=[dict(GroupID='group-a',Ready=True,LocalAppliedIndex=floor)])) for n in NODES]
 base.update(ReadinessBefore=ready(0,10),ReadinessAfter=ready(0,16))
 cfg=encode({'VectorInitialization':{'CatalogEpoch':7},'Nodes':[{'ID':n} for n in NODES],'Groups':[{'ID':'group-a'}]}).encode()
 configpath='/pure-self-check/config.json';a=dict(receipts={'config':configpath},local_pins={configpath:c.sha(cfg)},config_sha256=c.sha(cfg))
 p=dict(Version=1,Kind='fixed_cluster_mixed_window_v1',Concurrency=1,WarmupPlanned=64,MaxAttempts=LIMIT,OutputBytes=CAP,RequestedDuration=DURATION,PaceInterval=5_000_000_000,Admission=copy.deepcopy(base),Attempts=[],ReadPrefixes=[],Visibility=[],VisibilityEvidence=[],Retries=[],Counts=c.account([],LIMIT),WarmupCounts=c.account([],64),WriteCounts=c.account([],6),RetryCounts=c.account([],2),Prefixes=[dict(Truth=[truth]*16) for _ in range(7)])
 r=copy.deepcopy(p);r.update(Verdict='ACCEPT_MIXED_INVARIANT_RECALL_WINDOW_OBSERVATION_PENDING_ROOT_SHUTDOWN_VERIFICATION',Error='',Truncated=False,StopReason='window_elapsed',ActualDurationNS=DURATION,MeasuredOriginUTC=utc(0),HighestNewCommitIndex=16,RequiredAppliedIndex=16,RetryOriginUTC=utc(61_000_000_000))
 r['Writes']=[dict(Ordinal=i,Kind='replace',Invoked=True,Outcome='succeeded',Error='',ErrorCode='',StartNS=i*5_000_000_000,EndNS=i*5_000_000_000+20_000_000,Response=dict(AppliedIndex=11+i,CommitIndex=11+i,VisibilityToken='dG9rZW4=')) for i in range(6)]
 r['Retries']=[dict(Ordinal=i,Kind='replace' if i==0 else 'delete',Invoked=True,Outcome='succeeded',Error='',ErrorCode='',StartNS=j*2_000_000,EndNS=j*2_000_000+1_000_000,Response=dict(AppliedIndex=16,CommitIndex=11+i)) for j,i in enumerate((0,2))]
 r.update(WriteCounts=c.account(r['Writes'],6),RetryCounts=c.account(r['Retries'],2),WriterLatencyNs=[20_000_000]*6)
 r['ResourceBoundaries']=[dict(Phase='ready',RunID=c.QUERY_RUN,Nonce='1'*32,MeasuredOriginUTC=ZERO,ActualDurationNS=0,StopReason='',PublishedUTC=utc(-2_000_000),AcknowledgedUTC=utc(-1_000_000),WaitNS=1),dict(Phase='done',RunID=c.QUERY_RUN,Nonce='1'*32,MeasuredOriginUTC=utc(0),ActualDurationNS=DURATION,StopReason='window_elapsed',PublishedUTC=utc(DURATION+1),AcknowledgedUTC=utc(DURATION+2),WaitNS=1)]
 # Warmup UTC timestamps intentionally need no invented monotonic origin.
 r['Attempts']=[]
 for phase,count in (('warmup',64),('measured',16)):
  for i in range(count):
   start=i*2_000_000 if phase=='warmup' else (5_000_000 if i==0 else i*3_000_000_000+25_000_000);end=start+1_000_000
   x=dict(Ordinal=i,Worker=0,Phase=phase,QueryID=queries[i%16]['QueryID'],RequestSHA256=queries[i%16]['RequestSHA256'],Outcome='succeeded',Error='',ErrorCode='',Deadline=utc(start+RPC if phase=='measured' else -2_000_000_000+i*2_000_000),StartNS=start,EndNS=end,Response=copy.deepcopy(response),ResponseSHA256='',ResponseBytes=len(encode(response).encode()),RecallAt10=1.0)
   r['Attempts'].append(x)
   if phase=='measured':
    lo,hi=c.causal_range(r['Writes'],start,end);r['ReadPrefixes'].append(dict(Ordinal=i,Lower=lo,Upper=hi,Matched=lo))
 r.update(Counts=c.account(r['Attempts'][64:],LIMIT),WarmupCounts=c.account(r['Attempts'][:64],64),Completions=16,WarmupCompletions=64,MeasuredQueryAttempts=[1]*16,MeasuredQuerySucceeded=[1]*16,SuccessLatency=c.percent([1_000_000]*16),FailureLatency=c.percent([]),AttemptsQPS=16/60,SuccessfulQPS=16/60,MeanRecallAt10=1.0,OverlappingSearches=1,CompletedSearchesDuringMutation=1,RetainedAttemptBytes=sum(len(encode(x).encode())+1 for x in r['Attempts']))
 r['VisibilityOriginUTC']=[]
 for i,w in enumerate(r['Writes']):
  req=copy.deepcopy(request);req['VisibilityToken']=w['Response']['VisibilityToken'];vo=w['EndNS']+1_000_000;req['Deadline']=utc(vo+RPC)
  r['VisibilityOriginUTC'].append(utc(vo))
  r['Visibility'].append(dict(Ordinal=i,Kind='search',Phase='post-ack-current-prefix',InsertRequest=None,SearchRequest=req,RequestSHA256=c.sha(encode(req).encode()),Outcome='succeeded',Error='',ErrorCode='',StartNS=100,EndNS=1_000_000,SearchResponse=copy.deepcopy(response)))
  r['VisibilityEvidence'].append(dict(Bytes=len(encode(response).encode()),SHA256=c.sha(encode(response).encode())))
 for name,phase,index in (('PreRecall','quiescent-before-mixed',10),('PostRecall','quiescent-after-mixed',16)):
  rec=copy.deepcopy(base);rec['Phase']=phase;rec['HighestCommitIndex']=index;rec['Error']=''
  for i,q in enumerate(rec['Queries']):q.update(Outcome='succeeded',StartNS=i*2_000_000,EndNS=i*2_000_000+1_000_000,Response=copy.deepcopy(response),RecallAt10=1.0);q['Request']['Deadline']=utc((62_000_000_000 if name=='PostRecall' else -3_000_000_000)+i*2_000_000)
  rec.update(Counts=c.account(rec['Queries']),MeanRecallAt10=1.0,ReadinessAfter=ready(0,16) if name=='PostRecall' else None);r[name]=rec
 p['Writes']=[dict(Ordinal=i,Outcome='unissued',Invoked=False,StartNS=0,EndNS=0) for i in range(6)]
 ptext=encode(p);r['PlannedEventBytes']=len(('{"Event":"planned","Report":'+ptext+'}\n').encode())
 tests=[];old=c.bounded
 def bounded_stub(path,cap=CAP):need(path==configpath,'only fixture approved config read');return cfg
 c.bounded=bounded_stub
 def run(candidate=r,floor=.9):return verifier(a,p,candidate,ptext,encode(candidate),states,floor)
 def ok(name,fn):fn();tests.append(dict(name=name,outcome='PASS'))
 def no(name,fn):
  try:fn()
  except (AssertionError,ValueError,KeyError,TypeError):tests.append(dict(name=name,outcome='EXPECTED_REJECTION'));return
  raise AssertionError('tamper accepted '+name)
 try:
  ok('complete producer-shaped read accounting fixture with omitted planned Response',run)
  def planned_response(value):
   pp=copy.deepcopy(p);pp['Writes'][0]['Response']=value;pt=encode(pp);rr=copy.deepcopy(r)
   rr['PlannedEventBytes']=len(('{"Event":"planned","Report":'+pt+'}\n').encode())
   return verifier(a,pp,rr,pt,encode(rr),states,.9)
  ok('accept explicit null planned Response',lambda:planned_response(None))
  no('reject non-null planned Response',lambda:planned_response({}))
  ok('actual raw array decoder preserves Go negative-zero score',lambda:need(c.bits(c.pieces('{"Neighbors":[{"Score":-0}]}','Neighbors')[0][0]['Score'])==c.bits(-0.),'raw signed-zero array'))
  ok('actual raw field decoder preserves Go negative-zero score',lambda:need(c.bits(c.field('{"Score":-0}','Score')[0])==c.bits(-0.),'raw signed-zero field'))
  for key,value in (('Counts',{}),('WarmupCompletions',63),('RetainedAttemptBytes',1),('PlannedEventBytes',1),('AttemptsQPS',1),('SuccessLatency',{}),('MeasuredQuerySucceeded',[0]*16),('OverlappingSearches',0),('CompletedSearchesDuringMutation',0),('WriterLatencyNs',[0]*6),('RequiredAppliedIndex',15),('MeanRecallAt10',.8),('ReadPrefixes',[]),('StopReason','attempt_cap')):
   z=copy.deepcopy(r);z[key]=value;no('reject altered '+key,lambda z=z:run(z))
  for key,value in (('Outcome','unknown'),('ResponseSHA256','invented'),('ResponseBytes',1),('Ordinal',1),('RequestSHA256','wrong'),('Deadline',ZERO),('RecallAt10',.8),('Worker',1),('RecallAt10',True)):
   z=copy.deepcopy(r);z['Attempts'][64][key]=value;no('reject measured '+key,lambda z=z:run(z))
  z=copy.deepcopy(r);z['Visibility'][0]['SearchRequest']['VisibilityToken']='Zm9yZ2Vk';no('reject wrong original ACK token',lambda:run(z))
  z=copy.deepcopy(r);z['VisibilityEvidence'][0]['SHA256']='0'*64;no('reject visibility response hash',lambda:run(z))
  z=copy.deepcopy(r);z['PostRecall']['Queries'][0]['RequestSHA256']='wrong';no('reject paired logical hash',lambda:run(z))
  z=copy.deepcopy(r);z['PostRecall']['Queries'][0]['Truth'][0]['Score']=.5;no('reject paired canonical truth',lambda:run(z))
  z=copy.deepcopy(r);z['Admission']['ReadinessAfter']=ready(0,0)+ready(1,16);ok('retain earlier lag then exact final successful readiness round',lambda:run(z))
  z=copy.deepcopy(r);z['Admission']['ReadinessAfter']=ready(0,16)+ready(1,0);no('reject final lag despite earlier passing round',lambda:run(z))
  z=copy.deepcopy(r);z['Admission']['ReadinessAfter'][0]['State']['NodeID']='node-b';no('reject wrong final node identity',lambda:run(z))
  z=copy.deepcopy(r);z['PostRecall']['ReadinessAfter'][0]['State']['CatalogEpoch']=8;no('reject wrong catalog epoch',lambda:run(z))
  for v in (None,float('nan'),float('inf'),-1,1.1,True):no('reject invalid external floor '+str(v),lambda v=v:run(floor=v))
  no('reject altered pinned config bytes',lambda:config_epoch(cfg+b' ',a))
  z=copy.deepcopy(r);z['Retries'][1]['Ordinal']=1;no('reject retry reindex instead of original slot2',lambda:run(z))
  z=copy.deepcopy(r);z['Counts']['Unissued']-=1;no('reject silently consumed unissued suffix',lambda:run(z))
 finally:c.bounded=old
 return dict(verdict='PURE_SELF_CHECK_PASS_NOT_CAMPAIGN_ACCEPTANCE',check_count=len(tests),checks=tests,module_sha256=c.sha(Path(__file__).read_bytes()),core_sha256=CORE_SHA,external_calls=0,limitations=LIMITATIONS)

if __name__=='__main__':
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--self-check',action='store_true');args=parser.parse_args()
 if not args.self_check:parser.error('module is callable; standalone CLI supports only pure --self-check')
 print(json.dumps(self_check(),indent=2))
