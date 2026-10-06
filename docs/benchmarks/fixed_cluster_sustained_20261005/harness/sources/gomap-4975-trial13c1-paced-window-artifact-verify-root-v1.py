"""Artifact-only stdlib validator. Run only after root confirms collection and stop."""
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import argparse, ast, base64, shlex, copy, datetime, calendar, hashlib, json, math, re, struct
from pathlib import Path
T=Path('/tmp')
RUN='rf4trial13c1pacedc1v1'
COLLECTOR=T/'gomap-4975-trial13c1-paced-window-collector-root-v1.py'
PIN_PLAN=Path('/tmp/gomap-4975-trial13c1-paced-window-preparation-v1.json')
def sha(x):return hashlib.sha256(x).hexdigest()
def load(p):return json.loads(p.read_bytes())
def need(v,msg):
 if not v:raise AssertionError(msg)
def stamp(s):
 m=re.fullmatch(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z',s);need(m,'RFC3339 timestamp')
 d=datetime.datetime.strptime(m[1],'%Y-%m-%dT%H:%M:%S');return calendar.timegm(d.timetuple())*10**9+int((m[2] or '').ljust(9,'0'))
def pieces(text,key):
 pos=text.index('"'+key+'":')+len(key)+3
 decoder=json.JSONDecoder();need(text[pos]=='[','array '+key);pos+=1;out=[]
 while text[pos]!=']':
  obj,end=decoder.raw_decode(text,pos);out.append((obj,text[pos:end]));pos=end
  if text[pos]==',':pos+=1
 return out

PREPARATION_HELPER=Path('/tmp/gomap-4975-trial13c1-paced-window-preparation-root-v1.py')
need(sha(PREPARATION_HELPER.read_bytes())=='53e4e0273e005c0fbe017c0f9f16fbda4f5d40a45cb0c8626f37f817e712a242','immutable preparation helper')
prep={};exec(compile(PREPARATION_HELPER.read_text(),str(PREPARATION_HELPER),'exec'),prep)

def oracle():
 p=T/'gomap-4975-trial13c1-paired-artifact-verify-root-v1.py'
 need(sha(p.read_bytes())=='13116decf94494eab68139735d96b4eecb3255ff732c8e52fa75a7301e6e65dd','accepted oracle helper pin')
 need(sha((T/'gomap-4975-trial13c1-artifact-verify-root-v1.py').read_bytes())=='f09b92fd76bd9ca66c7b2ed4397b92d39d7b5d85e28dee604c2653c779c592c1','accepted write verifier pin')
 need(sha((T/'gomap-4975-trial13c1-exact-validate-root-v1.py').read_bytes())=='d370d16a86f199e4ef0722afb64bada80c50e48cc8b914697db614035bdf4f1e','accepted transitive executable exact verifier pin')
 code=p.read_text().split('# SSH/log/CID/mount/resource evidence')[0].replace('assert not O.exists()\n','')
 ns={};exec(compile(code,'retained-independent-FP32-reconstruction','exec'),ns)
 return ns

def field(text,key):
 pos=text.index('"'+key+'":')+len(key)+3
 value,end=json.JSONDecoder().raw_decode(text,pos)
 return value,text[pos:end]

def percent(a):
 a=sorted(a)
 return dict(Samples=len(a),P50NS=a[math.ceil(.5*len(a))-1] if a else 0,P95NS=a[math.ceil(.95*len(a))-1] if a else 0,P99NS=a[math.ceil(.99*len(a))-1] if a else 0)

def account(ops,limit=None):
 outcomes=['succeeded','failed','canceled','unknown','unissued'];need(all(x['Outcome'] in outcomes for x in ops),'declared outcome only')
 n={k:sum(x['Outcome']==k for x in ops) for k in outcomes};planned=len(ops) if limit is None else limit
 return dict(Planned=planned,Attempted=len(ops)-n['unissued'],Succeeded=n['succeeded'],Failed=n['failed'],Canceled=n['canceled'],Unknown=n['unknown'],Unissued=n['unissued']+planned-len(ops))

def native(response,request,norm,ns,truth=None):
 need(response is not None and response['Generation']==ns['r']['Generation'] and len(response['Neighbors'])==request['TopK'],'native generation/topK')
 need(all(type(v) is int and v>=0 for v in response['Counters'].values()),'complete nonnegative integer native counters')
 c=response['Counters'];need(c['SelectedPartitions']==c['HNSWServedPartitions']==c['ReadProofs']==c['RPCs']==c['GenerationPins']==1 and c['ExactScanPartitions']==c['Retries']==c['Redirects']==0 and c['Candidates']>0 and c['Edges']>0,'native proof/no exactscan/retry/redirect')
 need(all(type(v) is int and v>=0 for v in response['Timing'].values()),'actual nonnegative server timing')
 seen=set();previous=None;nq=ns['normalized'](request['Query'])
 for z in response['Neighbors']:
  id=z['ID'];need(id in norm and id not in seen,'known unique native ID');seen.add(id);s=ns['score'](nq,norm[id]);need(ns['bits'](s)==ns['bits'](z['Score']),'independent native FP32 score')
  if previous is not None:need(previous[0]>s or (ns['bits'](previous[0])==ns['bits'](s) and previous[1]<id),'canonical score DESC ID ASC including ties')
  previous=(s,id)
 return sum(z['ID'] in {v['ID'] for v in truth} for z in response['Neighbors'])/request['TopK'] if truth is not None else None

def readiness(states,prefix):
 need(len(states)==4 and {x['RequestedNode'] for x in states}=={'node-a','node-b','node-c','node-d'},'all four actual voter observations')
 for x in states:
  s=x['State'];need(x['Error']==x['ErrorCode']==s.get('Error','')=='' and s['NodeID']==x['RequestedNode'] and s['Ready'] and s['Live'] and not s['Draining'] and s['VectorPhase']=='active' and len(s['Groups'])==1,'authenticated ready/live voter')
  g=s['Groups'][0];need(g['GroupID']=='group-a' and g['Ready'] and g.get('Error','')=='' and g['LocalAppliedIndex']>=g['RequiredAppliedIndex']>=prefix,'required actual applied prefix')

def insert_receipt(w,encoded,baseline,revision,previous):
 op=w['Operation'];z=op['InsertResponse'];need(op['Outcome']=='succeeded' and op['Error']==op['ErrorCode']==w['ResponseSHA256']=='' and z is not None,'complete acknowledged insert only')
 _,oe=field(encoded,'Operation');value,raw=field(oe,'InsertResponse');need(value==z and w['ResponseBytes']==len(raw.encode())<=32768,'exact public response bytes')
 need(z['Generation']==z['VisibilityGeneration']==baseline['Generation'] and z['VisibleID']==op['ExpectedID']==base64.b64decode(op['InsertRequest']['ID'],validate=True).decode(),'insert identity/generation visibility fence')
 need(z['OwnerGroup']==baseline['OwnerGroup']=='group-a' and z['PartitionID']==baseline['PartitionID']==0 and z['LiveRevision']==revision and z['CommitIndex']>previous and z['AppliedIndex']>=z['CommitIndex'] and z['CommitTerm']>0 and z['ProductionConsensus'] is True and z['VisibilityToken'] is None,'public production/applied/revision/partition receipt; no invented bootstrap ack')
 c=z['Counters'];need(all(type(v) is int and v>=0 for v in c.values()) and all(c[k]==1 for k in ['Routes','Commits','Replications','Applies','VisibilityProofs']) and 0<=c['Forwards']<=1,'full public insert counters')
 return z['CommitIndex']

def planned_admission(actual,offline):
 actual,offline=copy.deepcopy(actual),copy.deepcopy(offline)
 need(actual.pop('RPCTimeout')==10000000000 and offline.pop('RPCTimeout')==1000000000,'explicit runtime10s/offline1s RPC budgets')
 need(actual==offline,'exact immutable admission apart from separately admitted RPC budgets')

def verify_paced(p,r,ptext,text,ns,a):
 offline_raw=Path(a['offline_admission_path']).read_bytes();need(sha(offline_raw)==a['offline_admission_sha256'] and offline_raw.endswith(b'\n'),'root bound offline bytes')
 offevents=[json.loads(x) for x in offline_raw.splitlines()];need(offevents[0]['Event']=='planned','offline planned admission');off=offevents[0]['Report']
 need(p['Writes']==off['Writes'] and p['PrefixProofs']==off['PrefixProofs'],'exact independently admitted writes and prefix proofs')
 planned_admission(p['Admission'],off['Admission'])
 for x in [p,r]:need(x['BaselineLiveRevision']==66 and x['BaselinePopulationRows']==10069 and x['BaselinePopulationSHA256']==ns['population'] and x['PaceInterval']==5000000000 and len(x['Writes'])==6 and len(x['PrefixProofs'])==7 and len(x['Visibility'])==len(x['VisibilityEvidence'])==6,'initial baseline/complete paced plan')
 need(p['FinalLiveRevision']==66 and p['FinalCommitIndex']==157,'original admission prefix/revision')
 need(p['WriteCounts']==account([w['Operation'] for w in p['Writes']]) and p['MutationCounts']==account([w['Operation'] for w in p['Writes']]+[p['Retry']['Operation']]) and p['VisibilityCounts']==account(p['Visibility']) and p['MutationCounts']['Attempted']==p['VisibilityCounts']['Attempted']==0,'planned complete unissued mutation/visibility ledger')
 _,adtext=field(text,'Admission');qs=pieces(adtext,'Queries');truth_raw=[field(s,'Truth')[1] for q,s in qs];top_sha=sha(('['+','.join(truth_raw)+']').encode())
 need(p['PrefixProofs'][0]['DistinctPrefix']==0 and all(ns['bits'](x)==ns['bits'](0) for x in p['PrefixProofs'][0]['CandidateScores']),'prefix0 actual zero scores')
 norm=dict(ns['norm']);candidates=[];praw=pieces(ptext,'Writes');wraw=pieces(text,'Writes')
 for i,(pw,encoded) in enumerate(praw):
  op=pw['Operation'];req=op['InsertRequest'];id=base64.b64decode(req['ID'],validate=True).decode();need(id==RUN+f'-doc-{i:02d}' and id==op['ExpectedID'] and id not in norm,'fresh exact candidate ID')
  need(base64.b64decode(req['IdempotencyKey'],validate=True).decode()==RUN+f'/insert/{i:02d}' and req['Version']==1 and req['Generation']==ns['r']['Generation'] and req['Deadline']=='0001-01-01T00:00:00Z','exact immutable insert identity')
  angle=math.pi/2+(i+1)*math.pi/14;expected=[ns['f32'](math.cos(angle)),ns['f32'](math.sin(angle))]+[0.]*126;need(ns['vecbits'](req['Vector'])==ns['vecbits'](expected),'six exact angular FP32 fixtures')
  docraw=base64.b64decode(req['Document'],validate=True);doc=json.loads(docraw);need(set(doc)=={'embedding','kind'} and doc['kind']=='query-under-write' and ns['vecbits'](doc['embedding'])==ns['vecbits'](expected),'retained actual document bytes/vector')
  _,oe=field(encoded,'Operation');_,rr=field(oe,'InsertRequest');need(sha(rr.encode())==op['RequestSHA256'],'exact encoded immutable request digest')
  need(op['Ordinal']==i and op['Phase']=='paced-measured' and op['Kind']=='insert' and op['Outcome']=='unissued' and op['StartNS']==op['EndNS']==pw['InvocationStartNS']==pw['ResponseBytes']==0 and not pw['Invoked'] and pw['IntendedOffsetNS']==i*5000000000 and pw['SkipReason']==pw['ResponseSHA256']=='','unissued planned invocation')
  norm[id]=ns['normalized'](req['Vector']);candidates.append(id)
  proof=p['PrefixProofs'][i+1];need(proof['DistinctPrefix']==i+1 and len(proof['CandidateScores'])==16,'complete prefix proof')
  for j,q in enumerate(p['Admission']['Queries']):
   nq=ns['normalized'](q['Request']['Query']);s=ns['score'](nq,norm[id]);need(ns['bits'](s)==ns['bits'](proof['CandidateScores'][j]),'independent prefix candidate FP32 bits')
   scores=[(z['Score'],z['ID']) for z in q['Truth']]+[(ns['score'](nq,norm[k]),k) for k in candidates];top=sorted(scores,key=lambda z:(-z[0],z[1]))[:10]
   need([(ns['bits'](v),k) for v,k in top]==[(ns['bits'](z['Score']),z['ID']) for z in q['Truth']],'entire accepted baseline plus every planned prefix invariant including ties')
 for proof in p['PrefixProofs']:need(proof['Top10SHA256']==top_sha,'actual encoded sixteen-prefix top10 digest')
 need(r['PrefixProofs']==p['PrefixProofs'],'frozen prefix proofs')
 for id in candidates:
  nq=ns['normalized'](p['Writes'][candidates.index(id)]['Operation']['InsertRequest']['Vector']);winner=min(((ns['score'](nq,v),k) for k,v in norm.items()),key=lambda z:(-z[0],z[1]));need(winner[1]==id,'complete possible population canonical self winner')
 writes=r['Writes'];ack=[];previous=157;prior_end=prior_start=0;ended=False;origin=stamp(r['MeasuredOriginUTC'])
 for i,(w,encoded) in enumerate(wraw):
  op=w['Operation'];pw=p['Writes'][i];req=copy.deepcopy(op['InsertRequest']);req['Deadline']='0001-01-01T00:00:00Z'
  need(req==pw['Operation']['InsertRequest'] and all(op[k]==pw['Operation'][k] for k in ['Ordinal','Phase','Kind','ExpectedID','RequestSHA256']) and w['IntendedOffsetNS']==pw['IntendedOffsetNS'],'unchanged planned write except actual deadline/evidence')
  if not w['Invoked']:
   need(op['Outcome']=='unissued' and op['StartNS']==op['EndNS']==w['InvocationStartNS']==w['ResponseBytes']==0 and op['InsertResponse'] is None and w['ResponseSHA256']=='','uninvoked is unissued, never absent/ack');ended=True;continue
  need(not ended and w['SkipReason']=='' and 0<=op['StartNS']<=w['InvocationStartNS']<op['EndNS']<=r['ActualDurationNS'] and op['StartNS']>=prior_end,'contiguous serial actual invocations')
  need(w['InvocationStartNS']>=i*5000000000 and (i==0 or w['InvocationStartNS']-prior_start>=5000000000) and w['InvocationStartNS']+10000000000<=60000000000,'actual full RPC fits/minimum spacing/no catchup')
  deadline=stamp(op['InsertRequest']['Deadline']);need(origin+op['EndNS']<=deadline<=origin+60000000000 and 0<deadline-origin-op['StartNS']<=10000000000,'actual insertion deadline')
  previous=insert_receipt(w,encoded,ns['boot']['Insert'],66+len(ack)+1,previous);ack.append(w);prior_end=op['EndNS'];prior_start=w['InvocationStartNS']
 need(ack and r['WriteCounts']==account([w['Operation'] for w in writes]) and r['WriteCounts']['Attempted']==len(ack),'acknowledged distinct prefix only')
 need(r['WriteSuccessLatency']==percent([w['Operation']['EndNS']-w['Operation']['StartNS'] for w in ack]) and r['WriteFailureLatency']==percent([]),'independent write latency')
 retry=r['Retry'];op=retry['Operation'];_,encoded=field(text,'Retry');req=copy.deepcopy(op['InsertRequest']);req['Deadline']='0001-01-01T00:00:00Z';last=copy.deepcopy(ack[-1]['Operation']['InsertRequest']);last['Deadline']='0001-01-01T00:00:00Z'
 need(req==last and op['RequestSHA256']==ack[-1]['Operation']['RequestSHA256'] and op['Phase']=='explicit-retry' and op['Kind']=='insert' and retry['Invoked'] and retry['SkipReason']=='' and retry['InvocationStartNS']==op['StartNS'] and 0<=op['StartNS']<op['EndNS'],'identical declared retry actual invocation')
 retry_origin=stamp(r['RetryOriginUTC']);need(retry_origin>=origin+r['ActualDurationNS'] and retry_origin+op['EndNS']<=stamp(op['InsertRequest']['Deadline']) and 0<stamp(op['InsertRequest']['Deadline'])-retry_origin-op['StartNS']<=10000000000,'retry after fixed measured drain')
 previous=insert_receipt(retry,encoded,ns['boot']['Insert'],66+len(ack),previous)
 need(r['FinalCommitIndex']==previous and r['FinalLiveRevision']==66+len(ack) and r['MutationCounts']==account([w['Operation'] for w in writes]+[op]),'retry advances prefix/no new row or revision')
 final=dict(ns['vectors'])
 for w in ack:final[w['Operation']['ExpectedID']]=w['Operation']['InsertRequest']['Vector']
 h=hashlib.sha256()
 for id in sorted(final):h.update(struct.pack('<I',len(id.encode()))+id.encode()+ns['vecbits'](final[id]))
 final_norm={k:ns['normalized'](v) for k,v in final.items()}
 for name,pop,rows,digest,prefix in [('PreRecall',ns['norm'],10069,ns['population'],157),('PostRecall',final_norm,len(final),h.hexdigest(),previous)]:
  rec=r[name];need(rec['Version']==1 and rec['Kind']==r['Admission']['Kind'] and rec['RunID']==RUN and rec['Phase']==('quiescent-before-paced' if name=='PreRecall' else 'quiescent-after-paced') and rec['PopulationRows']==rows and rec['PopulationSHA256']==digest and rec['HighestCommitIndex']==prefix and rec['Counts']==account(rec['Queries']) and len(rec['Queries'])==16 and rec['Counts']['Succeeded']==16,'complete paired acknowledged-population recall')
  for key in ['ConfigSHA256','BootstrapSHA256','ManifestSHA256','ProvenanceSHA256','ProbeSHA256','Provenance','Manifest','Generation','ScoreContract','ConfigIdentity','BinarySHA256','Timeout','RPCTimeout']:need(rec[key]==r['Admission'][key],'paired immutable admission '+key)
  _,re=field(text,name);qraw=pieces(re,'Queries');need([q for q,s in qraw]==rec['Queries'] and rec['Error']=='','exact retained paired query ledger')
  last_end=0
  for i,(q,qe) in enumerate(qraw):
   canonical=r['Admission']['Queries'][i];request=copy.deepcopy(q['Request']);request['Deadline']='0001-01-01T00:00:00Z';need(request==canonical['Request'] and q['QueryID']==canonical['QueryID'] and q['RequestSHA256']==canonical['RequestSHA256'] and q['CorpusTruth']==canonical['CorpusTruth'] and q['Error']==q['ErrorCode']=='' and q['StartNS']>=last_end and q['EndNS']>q['StartNS'] and q['EndNS']<=120000000000,'complete serial paired query/request/deadline');stamp(q['Request']['Deadline']);last_end=q['EndNS']
   _,rb=field(qe,'Response');need(len(rb.encode())<=32768 and q['EndNS']-q['StartNS']<=10000000000,'paired response/call retention bounds')
   nq=ns['normalized'](request['Query']);scored=sorted(((ns['score'](nq,v),k) for k,v in pop.items()),key=lambda z:(-z[0],z[1]))[:10]
   need([(ns['bits'](z['Score']),z['ID']) for z in q['Truth']]==[(ns['bits'](v),k) for v,k in scored] and q['RecallAt10']==native(q['Response'],q['Request'],pop,ns,q['Truth']),'full paired population FP32 oracle/native recall')
  need(rec['MeanRecallAt10']==sum(q['RecallAt10'] for q in rec['Queries'])/16,'paired mean recall')
 readiness(r['PostRecall']['ReadinessAfter'],previous)
 vis_origin=stamp(r['VisibilityOriginUTC']);need(vis_origin>=retry_origin+r['Retry']['Operation']['EndNS'],'visibility after acknowledged retry')
 vraw=pieces(text,'Visibility');last_end=0
 for i,(op,encoded) in enumerate(vraw):
  planned=p['Visibility'][i];need(op['Ordinal']==i and op['Phase']=='post-ack-visibility' and op['Kind']=='search' and op['ExpectedID']==candidates[i],'all planned visibility identities')
  if i>=len(ack):need(op==planned and r['VisibilityEvidence'][i]==dict(Bytes=0,SHA256=''),'unissued visibility suffix never projected absent');continue
  req=copy.deepcopy(op['SearchRequest']);req['Deadline']='0001-01-01T00:00:00Z';need(req==planned['SearchRequest'] and op['RequestSHA256']==planned['RequestSHA256'] and op['Outcome']=='succeeded' and op['Error']==op['ErrorCode']=='' and op['StartNS']>=last_end and op['EndNS']>op['StartNS'],'actual acknowledged-only serial visibility');last_end=op['EndNS']
  expected=copy.deepcopy(r['Admission']['Queries'][0]['Request']);expected['Query']=ack[i]['Operation']['InsertRequest']['Vector'];expected['TopK']=1;need(req==expected,'canonical unchanged self query limits/generation')
  _,pe=pieces(ptext,'Visibility')[i];_,rr=field(pe,'SearchRequest');need(sha(rr.encode())==op['RequestSHA256'],'actual visibility immutable request bytes')
  _,raw=field(encoded,'SearchResponse');ev=r['VisibilityEvidence'][i];need(ev['Bytes']==len(raw.encode())<=32768 and ev['SHA256']=='','complete bounded visibility response bytes')
  need(vis_origin+op['EndNS']<=stamp(op['SearchRequest']['Deadline']) and 0<stamp(op['SearchRequest']['Deadline'])-vis_origin-op['StartNS']<=10000000000,'actual visibility deadline')
  native(op['SearchResponse'],op['SearchRequest'],final_norm,ns);need(op['SearchResponse']['Neighbors'][0]['ID']==op['ExpectedID'],'actual acknowledged native self winner')
 need(r['VisibilityCounts']==account(r['Visibility']) and r['VisibilityCounts']['Succeeded']==len(ack),'complete acknowledged visibility accounting')
 overlap=completed=0
 for x in r['Attempts']:
  if x['Phase']!='measured':continue
  for z in (x['Response'] or {}).get('Neighbors',[]):
   if z['ID'] in candidates:
    w=writes[candidates.index(z['ID'])];need(w['Invoked'] and w['InvocationStartNS']<=x['EndNS'],'never-invoked/future IDs rejected')
  if x['Outcome']=='succeeded':
   overlap+=any(x['StartNS']<w['Operation']['EndNS'] and x['EndNS']>w['Operation']['StartNS'] for w in ack)
   completed+=any(x['StartNS']>=w['Operation']['StartNS'] and x['EndNS']<w['Operation']['EndNS'] for w in ack)
 need(r['OverlappingSearches']==overlap and r['CompletedSearchesDuringInsert']==completed and completed>0,'independent client API interval overlap, not server critical section')
 return dict(union_norm=norm,ordinary_acknowledged=len(ack),highest_acknowledged_prefix=previous,live_revision=r['FinalLiveRevision'],final_rows=len(final),final_population_sha256=h.hexdigest(),overlapping_searches=overlap,completed_searches_during_insert=completed)

def verify(n,approved_path):
 C=T/f'gomap-4975-trial13c1-paced-window-c{n}-root-v1';L=T/f'gomap-4975-trial13c1-paced-window-lifecycle-c{n}-root-v1'
 pins=load(PIN_PLAN);a=load(approved_path);prep['source_gate'](a)
 need(sha(Path(__file__).read_bytes())==pins['validator_sha256']==a['validator_sha256'],'frozen validator source')
 for key in ['approved_sha256','query_image','offline_admission_sha256','no_network_preflight_sha256']:
  need(isinstance(pins[key],str) and pins[key], 'root must bind unset prerequisite '+key)
 need(sha(approved_path.read_bytes())==pins['approved_sha256'] and (C/'approved-inputs.json').read_bytes()==approved_path.read_bytes()==(L/'approved-inputs.json').read_bytes(),'exact approved receipt bytes')
 for key in ['RootAccepted','ReadWindowModeValidated','ResourceGateModeValidated','DriverUserReadAccessValidated','PacedModeValidated','OfflineAdmissionValidated','NoNetworkNoStorePreflightValidated','InitialBaselineStillExclusive']:need(a[key] is True,'root prerequisite '+key)
 for key in ['driver_sha256','source_head','source_inventory_sha256','query_image','server_sha256','offline_admission_sha256','no_network_preflight_sha256']:need(a[key]==pins[key],'root accepted pin '+key)
 need(re.fullmatch('sha256:[0-9a-f]{64}',a['query_image']) and a['driver_uid_gid']=='1000:1000','actual image/non-root user')
 need(sha(COLLECTOR.read_bytes())==pins['collector_sha256']==sha((C/'collector-source.py').read_bytes())==sha((L/'lifecycle-source.py').read_bytes()),'one pinned collector/lifecycle source')
 gate=f'/home/mikers/gomap-4975-trial13c1-paced-window-resource-c{n}-root-v1/gate'
 ns=oracle();prep['baseline'](ns['r']);I=ns['I'];post=ns['r'];bits=ns['bits'];norm=ns['norm'];score=ns['score'];normalized=ns['normalized'];vecbits=ns['vecbits']
 need((C/'input-inventory.json').read_bytes()==(I/'input-inventory.json').read_bytes(),'116 input inventory binding')
 for f,k in [('node-c-config.json','config_sha256'),('bootstrap-qualify.json','bootstrap_sha256'),('plan.json','plan_sha256')]:need(sha((C/f).read_bytes())==a[k],'snapshot '+f)
 raw=(C/'stdout.jsonl').read_bytes();need(raw.endswith(b'\n') and len(raw)<=134217728,'encoded pair bound/framing')
 lines=raw.splitlines(keepends=True);need(len(lines)==2,'exact planned/result pair')
 events=[json.loads(x) for x in lines];need([x['Event'] for x in events]==['planned','result'],'event order');p,r=[x['Report'] for x in events]
 need(r['Verdict']=='ACCEPT_PACED_INVARIANT_RECALL_WINDOW_OBSERVATION' and r['Error']=='' and not r['Truncated'] and r['StopReason']=='window_elapsed','actual native success')
 for report in [p,r]:
  need(report['Version']==1 and report['Kind']=='fixed_cluster_paced_window_v1' and report['Concurrency']==n and report['WarmupPlanned']==64 and report['MaxAttempts']==65536 and report['OutputBytes']==134217728 and report['RequestedDuration']==60000000000,'window contract')
  ad=report['Admission'];need(ad['RunID']==RUN and ad['Phase']=='post-only' and ad['Kind']=='fixed_cluster_paced_window_admission_v1','run/phase identity')
  need(ad['BinarySHA256']==a['driver_sha256'] and ad['Timeout']==120000000000 and ad['RPCTimeout']==10000000000,'binary/deadline contract')
  for key in ['ConfigSHA256','BootstrapSHA256','ManifestSHA256','ProvenanceSHA256','ProbeSHA256','Provenance','Manifest','Generation','PopulationRows','PopulationSHA256','HighestCommitIndex','ScoreContract','ConfigIdentity']:need(ad[key]==post[key],'accepted post admission '+key)
  need(len(ad['Queries'])==16,'all16 admitted queries')
  for q,z in zip(ad['Queries'],post['Queries']):
   need(q['QueryID']==z['QueryID'] and q['RequestSHA256']==z['RequestSHA256'] and q['CorpusTruth']==z['CorpusTruth'] and q['Truth']==z['Truth'],'canonical oracle identity')
   req=copy.deepcopy(z['Request']);req['Deadline']='0001-01-01T00:00:00Z';need(q['Request']==req and q['Outcome']=='unissued','immutable canonical request')
   need(sha(json.dumps(req,separators=(',',':'),ensure_ascii=False).encode())==q['RequestSHA256'] and vecbits(req['Query'])==vecbits(ns['queries'][int(q['QueryID'][-6:])]),'request SHA/FP32 bits')
 need(p['Attempts'] is None and p['Counts']['Attempted']==p['WarmupCounts']['Attempted']==0 and r['PlannedEventBytes']==len(lines[0]),'planned admission contains no calls')
 need(p['Admission']['Queries']==r['Admission']['Queries'],'planned/result frozen queries')
 paced=verify_paced(p,r,lines[0].decode(),lines[1].decode(),ns,a)
 norm=paced.pop('union_norm')
 duration=r['ActualDurationNS'];need(60000000000<=duration<=120000000000,'measured admission+drain duration')
 origin=stamp(r['MeasuredOriginUTC']);ledger=pieces(lines[1].decode(),'Attempts');need([x for x,_ in ledger]==r['Attempts'],'exact raw ledger')
 phases={x:[(v,s) for v,s in ledger if v['Phase']==x] for x in ['warmup','measured']};need(sum(map(len,phases.values()))==len(ledger),'only declared phases')
 coverage=[0]*16;success=[];recalls=[];worker_ends={};retained=0;max_dispatch_overshoot=0
 for phase,pop in phases.items():
  need([x['Ordinal'] for x,_ in pop]==list(range(len(pop))),phase+' contiguous issued ordinals')
  need(len(pop)==(64 if phase=='warmup' else r['Counts']['Attempted']),phase+' full retained population')
  for x,encoded in pop:
   ordinal=x['Ordinal'];worker=x['Worker'];need(0<=worker<n and x['StartNS']>=0 and x['EndNS']>x['StartNS'],'worker/time bounds')
   need(x['StartNS']>=worker_ends.get((phase,worker),0),'one outstanding API call per worker');worker_ends[phase,worker]=x['EndNS']
   if phase=='warmup':need(worker==ordinal%n,'warmup worker ownership')
   q=r['Admission']['Queries'][ordinal%16];need(x['QueryID']==q['QueryID'] and x['RequestSHA256']==q['RequestSHA256'],'cyclic request ledger')
   need(x['Outcome']=='succeeded' and x['Error']==x['ErrorCode']==x['ResponseSHA256']=='' and x['Response'] is not None,'successful complete response')
   d=stamp(x['Deadline']);need(d>0,'actual deadline')
   response=x['Response'];need(response['Generation']==post['Generation'] and len(response['Neighbors'])==10,'native generation/top10')
   native(response,q['Request'],ns['norm'] if phase=='warmup' else norm,ns,q['Truth'])
   seen=set();prior=None;hits=0;nq=normalized(q['Request']['Query']);truth={z['ID'] for z in q['Truth']}
   for z in response['Neighbors']:
    id=z['ID'];active_norm=ns['norm'] if phase=='warmup' else norm;need(id in active_norm and id not in seen,'known unique actual ID');seen.add(id);value=score(nq,active_norm[id]);need(bits(z['Score'])==bits(value),'canonical FP32 scorebits')
    if prior is not None:need(prior[0]>value or (bits(prior[0])==bits(value) and prior[1]<id),'score DESC / ID ASC tie order')
    prior=(value,id);hits+=id in truth
   recall=hits/10;need(x['RecallAt10']==recall,'independent per-call recall')
   ctr=response['Counters'];need(ctr['SelectedPartitions']==ctr['HNSWServedPartitions']==ctr['ReadProofs']==ctr['RPCs']==ctr['GenerationPins']==1 and ctr['ExactScanPartitions']==ctr['Retries']==ctr['Redirects']==0 and ctr['Candidates']>0 and ctr['Edges']>0,'strict native proof/no fallback/retry/redirect')
   need(all(isinstance(v,int) and v>=0 for v in response['Timing'].values()),'nonnegative server timing sums')
   text=json.dumps(response,separators=(',',':'));pos=encoded.index('"Response":')+len('"Response":');_,end=json.JSONDecoder().raw_decode(encoded,pos)
   need(x['ResponseBytes']==len(encoded[pos:end].encode())<=32768,'actual encoded response retention bound')
   retained+=len(encoded.encode())+1
   if phase=='measured':
    need(x['EndNS']<=duration and 0<d-origin-x['StartNS']<=10000000000 and origin+x['EndNS']<=d,'measured monotonic/deadline/drain bounds')
    max_dispatch_overshoot=max(max_dispatch_overshoot,x['StartNS']-60000000000);coverage[ordinal%16]+=1;success.append(x['EndNS']-x['StartNS']);recalls.append(recall)
 need(r['RetainedAttemptBytes']==retained,'actual encoded retained attempt bytes')
 for phase,key,limit,comp in [('warmup','WarmupCounts',64,'WarmupCompletions'),('measured','Counts',65536,'Completions')]:
  count=len(phases[phase]);need(r[key]==dict(Planned=limit,Attempted=count,Succeeded=count,Failed=0,Canceled=0,Unknown=0,Unissued=limit-count) and r[comp]==count,'exact counts/unissued budget suffix '+phase)
 need(all(coverage) and coverage==r['MeasuredQueryAttempts']==r['MeasuredQuerySucceeded'],'16-query coverage')
 success.sort();percent=lambda a:dict(Samples=len(a),P50NS=a[math.ceil(.5*len(a))-1],P95NS=a[math.ceil(.95*len(a))-1],P99NS=a[math.ceil(.99*len(a))-1])
 need(r['SuccessLatency']==percent(success) and r['FailureLatency']==dict(Samples=0,P50NS=0,P95NS=0,P99NS=0),'nearest-rank latency populations')
 qps=len(success)/(duration/1e9);need(math.isclose(r['AttemptsQPS'],qps,rel_tol=1e-14) and math.isclose(r['SuccessfulQPS'],qps,rel_tol=1e-14) and r['MeanRecallAt10']==sum(recalls)/len(recalls),'QPS elapsed+drain/mean recall')
 for key in ['ReadinessBefore','ReadinessAfter']:
  states=r['Admission'][key];need(len(states)==4 and {x['RequestedNode'] for x in states}=={'node-a','node-b','node-c','node-d'},'all4 control observations')
  for x in states:
   s=x['State'];need(x['Error']==x['ErrorCode']==s.get('Error','')=='' and s['NodeID']==x['RequestedNode'] and s['Ready'] and s['Live'] and not s['Draining'] and s['VectorPhase']=='active' and len(s['Groups'])==1,'authenticated active voter')
   g=s['Groups'][0];need(g['GroupID']=='group-a' and g['Ready'] and g.get('Error','')=='' and g['LocalAppliedIndex']>=g['RequiredAppliedIndex']>=(157 if key=='ReadinessBefore' else r['FinalCommitIndex']),'actual initial/final acknowledged prefix')
 # Retained command/image/driver and all four original stopped roots.
 command=load(C/'command.json');name=f'treedb-4250-rf4trial13c1-paced-window-c{n}-v1'
 need(command[command.index(a['query_image'])+1:]==['-config','/config.json','-bootstrap-receipt','/bootstrap.json','-mode','paced-window','-paced-inserts','6','-paced-interval','5s','-phase','post-only','-probe-receipt','/recall/probe.jsonl','-dataset','/recall/dataset','-provenance','/recall/provenance.json','-run-id',RUN,'-read-resource-gate-dir','/run-resource-gate','-timeout','120s','-rpc-timeout','10s','-read-concurrency',str(n),'-read-window','60s','-read-warmup','64','-read-max-attempts','65536','-read-output-bytes','134217728'],'full actual CLI')
 recs=[(f,load(f)) for f in C.glob('*.json')];ssh=[(f,x) for f,x in recs if isinstance(x,dict) and 'argv' in x]
 for root in [C,L]:
  for f in root.glob('*.json'):
   x=load(f)
   if isinstance(x,dict) and 'argv' in x:need(x.get('exit_code',x.get('exit'))==0 and not x.get('timed_out'),'successful actual SSH receipt '+f.name)
 need(load(L/'result.json')==dict(errors=[],state='PASS_STOPPED',stores_preserved=True),'independent lifecycle disposition')
 launches=[x for f,x in ssh if re.search(r'(^|\s)docker run ',x['argv'][-1])];need(len(launches)==1,'one driver launch/no hidden workload retry')
 driver=load(C/'inspect.json');need(driver['Id']==launches[0]['stdout'].strip() and driver['Name']=='/'+name and driver['Image']==a['query_image'] and driver['Config']['Labels']['treedb.fixed-cluster.run']=='rf4trial13c1','actual client identity')
 def caps(x):need(x['HostConfig']['Memory']==x['HostConfig']['MemorySwap']==2147483648 and x['HostConfig']['NanoCpus']==2000000000 and x['HostConfig']['RestartPolicy']['Name']=='no','actual caps/no extra swap/no restart')
 need(driver['Config']['User']==a['driver_uid_gid'] and driver['HostConfig']['NetworkMode']=='host','actual approved non-root user/network');caps(driver);need(not driver['State']['Running'] and driver['State']['ExitCode']==0 and not driver['State']['OOMKilled'],'native driver clean exit0/noOOM')
 need(driver['Args']==command[command.index(a['query_image'])+1:] and set(driver['HostConfig']['Binds'])=={a['remote_root']+'/node-c/config.json:/config.json:ro',a['remote_root']+'/node-c/credentials:/credentials:ro',a['remote_root']+'/bootstrap-qualify.json:/bootstrap.json:ro','/home/mikers/gomap-4975-rf4trial13c1-post-inputs-root-v1:/recall:ro',gate+':/run-resource-gate:rw'},'all client mounts read-only/exact')
 hashes=[x for f,x in ssh if f.name.endswith('-driver-hash.json')];need(len(hashes)==1 and hashes[0]['stdout'].split()[0]==a['driver_sha256'],'image-copied exact ELF')
 logs=[x for f,x in ssh if f.name.endswith('-logs.json')];need(len(logs)==1 and logs[0]['stdout'].encode('utf-8',errors='surrogateescape')==raw and logs[0]['stderr'].encode('utf-8',errors='surrogateescape')==(C/'stderr.log').read_bytes(),'raw logs/stdout/stderr binding')
 voters=[]
 for node in load(C/'plan.json')['nodes']:
  name0=node['node'];old=json.loads(load(ns['B']/({'node-a':'38','node-b':'40','node-c':'42','node-d':'44'}[name0]+'-final-state-'+name0+'.json'))['stdout'])[0]
  fs=sorted(f for f in L.glob('*inspect-'+name0+'.json') if '-driver-' not in f.name);need(len(fs)>=4,'full lifecycle inspections')
  for f in fs:
   rec=load(f);need(rec.get('exit',rec.get('exit_code'))==0 and not rec.get('timed_out'),'voter inspection receipt');x=json.loads(rec['stdout'])[0];caps(x)
   need(x['Id']==old['Id'] and x['Image']==node['image'] and x['Name']=='/'+node['name'] and x['Config']['Labels']['treedb.fixed-cluster.run']=='rf4trial13c1' and set(x['HostConfig']['Binds'])==set(old['HostConfig']['Binds']) and not x['State']['OOMKilled'],'original voter mounts/CID/image')
  first=json.loads(load(fs[0])['stdout'])[0];last=json.loads(load(fs[-1])['stdout'])[0];need(not first['State']['Running'] and first['State']['ExitCode']==0 and not last['State']['Running'] and last['State']['ExitCode']==0,'original four pre/post stopped0')
  voters.append(dict(node=name0,cid=old['Id'],final_receipt_sha256=sha(fs[-1].read_bytes())))
 finals=list(L.glob('*driver-fallback-final-node-c.json'));need(len(finals)==1,'independent client fallback final');fx=json.loads(load(finals[0])['stdout'])[0];need(fx['Id']==driver['Id'] and not fx['State']['Running'] and fx['State']['ExitCode']==0 and not fx['State']['OOMKilled'],'actual fallback clean client')
 # Independently reconstruct field-level resource bracket selection, not imported collector logic.
 samples=load(C/'resource-samples.json');resource=load(C/'measurement-resource-brackets.json');start=datetime.datetime.fromisoformat(r['MeasuredOriginUTC'].replace('Z','+00:00')).timestamp();end=start+duration/1e9
 need(resource['measured_origin_unix']==start and resource['measured_end_unix']==end,'resource measurement boundaries')
 fields=['memory.current','memory.peak','memory.max','memory.events','memory.swap.current','memory.swap.max','memory.swap.events','cpu.max','cpu.stat','io.stat','process.status'];dlo=min(s['clock_offset_low'] for s in samples if s['host']=='192.168.0.185');dhi=max(s['clock_offset_high'] for s in samples if s['host']=='192.168.0.185');missing={};peak={}
 for s in samples:
  need(s['clock_offset_low']==s['started_unix']-s['local_finished_unix'] and s['clock_offset_high']==s['finished_unix']-s['local_started_unix'],'raw clock uncertainty formula')
  matches=[x for f,x in ssh if x['started_unix']==s['local_started_unix'] and x['finished_unix']==s['local_finished_unix']];need(len(matches)==1,'raw sample SSH binding');remote=json.loads(matches[0]['stdout']);need(remote['targets']==s['targets'] and remote['started_unix']==s['started_unix'] and remote['finished_unix']==s['finished_unix'],'raw sampler values')
  for target in s['targets']:
   x=target['inspect'];caps(x);role=target['node'];need(target['network']['status']=='unsupported','no invented host-network byte counts')
   expected=driver['Id'] if role=='client' else next(v['cid'] for v in voters if v['node']==role);need(x['Id']==expected and not x['State']['OOMKilled'],'sampled owned identity')
   for field in fields:
    v=target['fields'][field];need(v['status'] in ['available','missing'] and s['started_unix']<=v['started_unix']<=v['finished_unix']<=s['finished_unix'],'per-field capture bounds')
    if v['status']=='missing':need(bool(v['reason']),'missing reason');continue
    need('raw' in v,'actual raw available field')
    if field=='process.status':need('VmRSS:' in v['raw'] and 'VmHWM:' in v['raw'],'process RSS/HWM raw')
    if field=='memory.peak':peak[role]=max(peak.get(role,0),int(v['raw']))
    if field=='memory.max':need(int(v['raw'])==2147483648,'cgroup2GiB actual cap')
    if field in ['memory.swap.current','memory.swap.max']:need(int(v['raw'])==0,'no actual extra swap')
    if field=='memory.events':need(all(int(line.split()[1])==0 for line in v['raw'].splitlines() if line.split()[0] in ['oom','oom_kill','oom_group_kill']),'sampled noOOM events')
 for role in ['node-a','node-b','node-c','node-d','client']:
  missing[role]=[];reported=resource['brackets'][role];need(reported['network']['status']=='unsupported','resource unsupported network')
  for field in fields:
   entries=[]
   for index,s in enumerate(samples):
    for target in s['targets']:
     if target['node']!=role:continue
     v=target['fields'][field]
     if v['status']!='available':continue
     lo,hi=(0,0) if s['host']=='192.168.0.185' else (dlo-s['clock_offset_high'],dhi-s['clock_offset_low'])
     entries.append((v['started_unix']+lo,v['finished_unix']+hi,index))
   before=[v for v in entries if v[1]<=start];after=[v for v in entries if v[0]>=end];claim=reported['fields'][field]
   if not before or not after:need(claim['status']=='unavailable','missing bracket not labeled available');missing[role].append(field);continue
   left=max(before,key=lambda v:v[1]);right=min(after,key=lambda v:v[0]);need(claim['status']=='available' and claim['before_sample_index']==left[2] and claim['after_sample_index']==right[2],'actual enclosing resource field')
   for key,value in [('before_started_driver_clock_lower',left[0]),('before_finished_driver_clock_upper',left[1]),('after_started_driver_clock_lower',right[0]),('after_finished_driver_clock_upper',right[1])]:need(claim[key]==value,'actual field clock bound')
  need(reported['status']==('incomplete' if missing[role] else 'available'),'role resource completeness')
 complete=not any(missing.values());exit=load(C/'exit.json');need(exit['native_window_passed_pending_root'] is True and exit['resource_brackets_complete']==complete and exit['status']==('PASS_PENDING_ROOT_PACED_AND_RESOURCE_VALIDATION' if complete else 'NATIVE_WINDOW_PASSED_RESOURCE_INCOMPLETE'),'honest native/resource gate split')
 need(int((L/'collector.exit').read_text())==(0 if complete else 1),'actual resource gate process disposition')
 need(complete,'V3 actual complete eleven-field enclosing brackets required');gate_proof=verify_gate(C,r,samples,ssh,driver,gate,a)
 return dict(gate_proof=gate_proof,paced=paced,verdict='ACCEPT',status='ACCEPT',scope='Bounded C1 paced native observation and eleven-field enclosing resource intervals; no capacity qualification.',concurrency=n,run_id=RUN,identities=a,stdout_sha256=sha(raw),input_inventory_sha256=sha((I/'input-inventory.json').read_bytes()),input_files_verified=116,counts=r['Counts'],warmup_counts=r['WarmupCounts'],write_counts=r['WriteCounts'],mutation_counts=r['MutationCounts'],visibility_counts=r['VisibilityCounts'],actual_duration_ns=duration,success_qps=qps,latency=r['SuccessLatency'],mean_recall_at_10=r['MeanRecallAt10'],coverage=coverage,highest_acknowledged_prefix=r['FinalCommitIndex'],driver=dict(cid=driver['Id'],exit=0,oom=False),voters=voters,resource_fields_enclosing_complete=complete,resource_missing_fields=missing,observed_memory_peak_bytes=peak,full_resource_qualification=False,max_measured_dispatch_overshoot_ns=max_dispatch_overshoot,findings=[],limits=['Exclusive ownership is root attestation; readiness is not writer exclusion.','Paced C1 call overlap is not server critical-section overlap, saturation, capacity or host-loss qualification.','Resource intervals include overscan; memory/process peaks include setup/warmup/history; exact measured-only and whole-lifetime peaks unproved.','Host-network per-container bytes unsupported; wall-clock translation assumes no clock step.','Measured read reservation timestamps, warmup and paired-query monotonic origins are not serialized; absolute phase alignment cannot be independently reconstructed.','Root binds source/build/image; no independent rebuild or runtime performed.'],artifact_inventory={str(f.relative_to(T)):sha(f.read_bytes()) for root in [C,L] for f in sorted(root.rglob('*')) if f.is_file()})


def verify_gate(C,r,samples,ssh,driver,gate,a):
 keys={'Version','RunID','Phase','Nonce','PublishedUTC','MeasuredOriginUTC','ActualDurationNS','StopReason','AcknowledgedUTC','WaitNS'}
 def unique(pairs):
  out={}
  for k,v in pairs:need(k not in out,'no duplicate gate key');out[k]=v
  return out
 tokens={};nonce=None
 snapshot=load(C/'gate-final-snapshot.json');need(snapshot['path']==gate and set(snapshot['tokens'])==set(snapshot['entries'])=={'claim.json','ready.json','ready.ack','done.json','done.ack'},'complete actual five gate tokens')
 for name in snapshot['tokens']:
  raw=(C/('gate-'+name)).read_bytes();v=snapshot['tokens'][name]
  need(0<len(raw)<=2048 and raw.endswith(b'\n') and v['complete'] and v['size']==len(raw) and base64.b64decode(v['raw_b64'],validate=True)==raw==(C/('gate-final-'+name)).read_bytes(),'actual complete bounded immutable token '+name);tokens[name]=raw
 for phase in ['claim','ready','done']:
  v=json.loads(tokens[phase+'.json'],object_pairs_hook=unique);need(set(v)==keys and type(v['Version']) is int and v['Version']==1 and v['RunID']==r['Admission']['RunID'] and v['Phase']==phase,'gate identity/schema')
  need(isinstance(v['Nonce'],str) and re.fullmatch('[0-9a-f]{32}',v['Nonce']) and (nonce is None or nonce==v['Nonce']),'fresh nonce binding');nonce=v['Nonce'];need(v['AcknowledgedUTC']=='0001-01-01T00:00:00Z' and v['WaitNS']==0 and type(v['ActualDurationNS']) is int,'immutable receipt initial ack fields');stamp(v['PublishedUTC'])
  if phase!='done':need(v['MeasuredOriginUTC']=='0001-01-01T00:00:00Z' and v['ActualDurationNS']==0 and v['StopReason']=='','pre-origin claim/ready')
  else:need(v['MeasuredOriginUTC']==r['MeasuredOriginUTC'] and v['ActualDurationNS']==r['ActualDurationNS'] and v['StopReason']==r['StopReason'] and stamp(v['PublishedUTC'])>=stamp(v['MeasuredOriginUTC'])+v['ActualDurationNS'],'fixed drain/origin/duration before done')
  if phase!='claim':need(tokens[phase+'.ack']==tokens[phase+'.json'],'exact byte ack '+phase)
 need(r['ResourceGateDir']=='/run-resource-gate' and len(r['ResourceBoundaries'])==2,'actual bound driver gate report')
 boundaries=load(C/'resource-boundaries.json');need([b['phase'] for b in boundaries]==['ready','done'],'one ready and done collection')
 creates=[x for f,x in ssh if f.name.endswith('-gate-create-exclusive.json')];need(len(creates)==1 and creates[0]['exit_code']==0,'one exclusive gate creation');creation=json.loads(creates[0]['stdout']);need(creation['path']==gate,'owned gate creation');identity=creation['directory_identity']
 constants={}
 collector_constants={}
 for node in ast.parse(COLLECTOR.read_text()).body:
  if isinstance(node,ast.Assign) and isinstance(node.value,ast.Constant):
   for k in node.targets:
    if isinstance(k,ast.Name):collector_constants[k.id]=node.value.value
 for node in ast.parse(collector_constants['COLLECTOR_SOURCE']).body:
  if isinstance(node,ast.Assign) and isinstance(node.value,ast.Constant):
   for k in node.targets:
    if isinstance(k,ast.Name):constants[k.id]=node.value.value
 prior_serial=0;gate_details=[]
 for phase,boundary,db in zip(['ready','done'],boundaries,r['ResourceBoundaries']):
  receipt=json.loads(tokens[phase+'.json']);need(boundary['receipt']==receipt and all(db[k]==receipt[k] for k in keys-{'AcknowledgedUTC','WaitNS'}),'driver/report/file boundary identity');need(db['WaitNS']>0 and stamp(db['AcknowledgedUTC'])>=stamp(db['PublishedUTC']),'positive actual wait and ack time')
  ar=[(f,x) for f,x in ssh if f.name.endswith('-gate-ack-'+phase+'.json')];need(len(ar)==1,'one acknowledgment no retry '+phase);f,record=ar[0];need(record['exit_code']==0 and not record.get('timed_out') and int(f.name.split('-')[0])>prior_serial,'successful ordered single ack');prior_serial=int(f.name.split('-')[0]);ack=json.loads(record['stdout']);need(ack==boundary['ack'] and ack['directory_identity']==identity and ack['phase']==phase and ack['bytes']==len(tokens[phase+'.json']) and ack['sha256']==sha(tokens[phase+'.json']),'raw ack metadata/hash')
  actual=shlex.split(record['argv'][-1]);need(actual==['python3','-c',constants['GATE_ACK'],gate,phase,base64.b64encode(tokens[phase+'.json']).decode(),json.dumps(identity)],'actual exact-byte atomic ack command');need(record['started_unix']==boundary['ack_local_started_unix'] and record['finished_unix']==boundary['ack_local_finished_unix'],'raw ack wallclock interval')
  indices=boundary['sample_indices'];need(len(indices)==2 and indices[1]==indices[0]+1,'full two-host boundary sample indices');batch=[samples[i] for i in indices];need([s['host'] for s in batch]==['192.168.0.111','192.168.0.185'],'two actual hosts');roles=[]
  for s in batch:
   need(s['boundary']==phase and s['local_finished_unix']<=record['started_unix'],'actual samples completed before ack dispatch')
   for target in s['targets']:
    roles.append(target['node']);x=target['inspect'];need(x['State']['Running'] and x['State']['Pid']>0 and not x['State']['OOMKilled'],'five roles actually alive at boundary')
    if target['node']=='client':need(x['Id']==driver['Id'] and x['Config']['User']==a['driver_uid_gid'],'same live non-root client')
    for field,v in target['fields'].items():
     need(v['status']=='available' and isinstance(v['raw'],str),'actual available boundary value '+field)
     if s['host']=='192.168.0.185':need(stamp(receipt['PublishedUTC'])/1e9<=v['started_unix']<=v['finished_unix']<=ack['installed_unix'],'driver-host actual boundary sample precedes installation')
  need(len(roles)==5 and set(roles)=={'node-a','node-b','node-c','node-d','client'},'five distinct roles each boundary')
  polls=[x for pf,x in ssh if pf.name.endswith('-gate-poll-'+phase+'.json') and record['started_unix']>=x['finished_unix']>=max(s['local_finished_unix'] for s in batch)]
  need(polls,'post-sample receipt/liveness recheck before ack');proof=json.loads(polls[-1]['stdout']);need(proof['directory_identity']==identity and proof['inspect']['Id']==driver['Id'] and proof['inspect']['State']['Running'] and proof['inspect']['State']['Pid']>0 and not proof['inspect']['State']['OOMKilled'],'actual live post-sample gate proof');need(base64.b64decode(proof['files'][phase+'.json'],validate=True)==tokens[phase+'.json'],'unchanged boundary after reads');need(ack['installed_unix']<=stamp(db['AcknowledgedUTC'])/1e9,'Go acknowledged after installation')
  if phase=='ready':need(stamp(db['AcknowledgedUTC'])<=stamp(r['MeasuredOriginUTC']),'ready ack before measured origin')
  else:need(stamp(receipt['PublishedUTC'])>=stamp(r['MeasuredOriginUTC'])+r['ActualDurationNS'],'measured duration fixed before done ack')
  gate_details.append(dict(phase=phase,sample_indices=indices,ack_sha256=sha(tokens[phase+'.ack']),wait_ns=db['WaitNS'],acknowledged_utc=db['AcknowledgedUTC']))
 permission=Path(a['no_network_preflight_path']);pr=load(permission);need(sha(permission.read_bytes())==a['no_network_preflight_sha256'] and pr['NetworkMode']=='none' and pr['DBMounts']==[] and pr['CredentialsReadable'] is True and pr['DriverSHA256']==a['driver_sha256'] and pr['Image']==a['query_image'] and pr['User']==a['driver_uid_gid'] and pr['RunID']==RUN and pr['PlannedSHA256']==a['offline_admission_sha256'] and pr['MutationInvocations']==0,'root isolated credential/ELF/admission preflight')
 need(isinstance(pr.get('raw_evidence'),dict) and pr['raw_evidence'],'retained preflight raw evidence')
 for path,digest in pr['raw_evidence'].items():prep['pinned'](path,digest)
 return dict(directory_identity=identity,nonce=nonce,tokens_sha256={k:sha(v) for k,v in tokens.items()},boundaries=gate_details,permission_proof_sha256=sha(permission.read_bytes()),permission_source='root retained isolated constructor/readiness refusal proof; Inspect alone does not load credentials')

if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--approved',type=Path,required=True);parser.add_argument('--concurrency',type=int,choices=[1],required=True);parser.add_argument('--root-confirmed-stopped',action='store_true',required=True);args=parser.parse_args()
 try:receipt=verify(args.concurrency,args.approved)
 except Exception as e:receipt=dict(verdict='REJECT',status='FAILED_OR_INCOMPLETE_EVIDENCE',scope='No population, absence, acknowledgment or performance inference from failed evidence',concurrency=args.concurrency,findings=[dict(error_type=type(e).__name__,detail=str(e))])
 receipt['validator_sha256']=sha(Path(__file__).read_bytes());print(json.dumps(receipt));raise SystemExit(receipt['verdict']!='ACCEPT')
