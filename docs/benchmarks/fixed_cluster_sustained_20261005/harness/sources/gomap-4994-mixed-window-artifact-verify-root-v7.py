"""Artifact-only mixed-window core verifier. INCOMPLETE is never acceptance.
Pinned AST function extraction executes no old campaign/module top-level.
This bounded packet deliberately fails closed on outstanding independent
transport/WAL/audit/resource/lifecycle verification. Root owns completion.
"""
import argparse, ast, base64, calendar, copy, datetime, hashlib, json, math, re, struct
from pathlib import Path
from source_paths import source_path
if not __debug__: raise RuntimeError('ordinary Python required')
RUN='rf4trial14mixedc1'
QUERY_RUN=RUN+'mixedc1v1'
COLLECTION=Path('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/mixed-window-root-v1')
INPUT=Path('/Volumes/FlashDrive/gomap-4994-rf4trial14mixedc1/inputs-root-v1')
NODES={'node-a','node-b','node-c','node-d'}
PINS={
 '/tmp/gomap-4975-trial13c1-paced-window-artifact-verify-root-v1.py':'fac3df4155d560cf8e1ae283195f84b58c6f479b984fb7777302a97c4c8fee9d',
 '/tmp/gomap-4975-trial13c1-paired-artifact-verify-root-v1.py':'1d4dcb61e8bbd08bc668ebe78c732433bdb05856051ffdde6bb142d8435191b0'}
HELPERS={}
def parse_json_int(x):return -0.0 if x=='-0' else int(x)
def extract_pure():
 groups=[(list(PINS)[0],('sha','need','stamp','pieces','field','percent','account')),
         (list(PINS)[1],('f32','bits','vecbits','normalized','score','rows'))]
 namespace={'hashlib':hashlib,'datetime':datetime,'calendar':calendar,'re':re,'json':json,'math':math,'struct':struct,'parse_json_int':parse_json_int}
 for name,allow in groups:
  raw=Path(source_path(name)).read_bytes()
  if hashlib.sha256(raw).hexdigest()!=PINS[name]: raise ValueError('pure helper pin '+name)
  source=raw.decode();tree=ast.parse(source);byname={n.name:n for n in tree.body if isinstance(n,ast.FunctionDef)}
  selected=[]
  for key in allow:
   n=byname[key]
   if n.decorator_list: raise ValueError('unexpected helper decorator')
   original=ast.get_source_segment(source,n)
   actual=original.replace('json.JSONDecoder()', 'json.JSONDecoder(parse_int=parse_json_int)') if key in ('pieces','field') else original
   n=ast.parse(actual).body[0];selected.append(n)
   HELPERS[key]={'file':name,'source_sha256':hashlib.sha256(original.encode()).hexdigest(),'executed_source_sha256':hashlib.sha256(actual.encode()).hexdigest(),'signed_zero_decoder_repair':actual!=original}
  exec(compile(ast.Module(body=selected,type_ignores=[]),'pinned-pure-functions','exec'),namespace)
 globals().update({k:namespace[k] for k in HELPERS})
extract_pure()

def field(text,key):
 # Bind only a top-level member; nested counters can use the same field name.
 obj=strict(text);need(isinstance(obj,dict),'raw object')
 decoder=json.JSONDecoder(parse_int=parse_json_int)
 pos=text.index('{')+1
 for _ in obj:
  while text[pos].isspace():pos+=1
  name,pos=decoder.raw_decode(text,pos)
  while text[pos].isspace():pos+=1
  need(text[pos]==':','raw member colon');pos+=1
  while text[pos].isspace():pos+=1
  start=pos;value,pos=decoder.raw_decode(text,pos)
  if name==key:return value,text[start:pos]
  while text[pos].isspace():pos+=1
  if text[pos]==',':pos+=1
 raise AssertionError('missing top-level field '+key)

def pieces(text,key):
 value,raw=field(text,key);need(isinstance(value,list),'array '+key)
 decoder=json.JSONDecoder(parse_int=parse_json_int);pos=1;out=[]
 for _ in value:
  while raw[pos].isspace():pos+=1
  start=pos;item,pos=decoder.raw_decode(raw,pos);out.append((item,raw[start:pos]))
  while raw[pos].isspace():pos+=1
  if raw[pos]==',':pos+=1
 while raw[pos].isspace():pos+=1
 need(raw[pos]==']','raw array end');return out

def strict(raw):
 def unique(pairs):
  out={}
  for k,v in pairs:need(k not in out,'duplicate JSON key');out[k]=v
  return out
 return json.loads(raw,object_pairs_hook=unique,parse_int=parse_json_int,parse_constant=lambda x:(_ for _ in ()).throw(ValueError('nonfinite JSON '+x)))
def bounded(p,cap=128<<20):
 p=Path(p);need(p.is_file() and not p.is_symlink() and p.stat().st_size<=cap,'bounded regular artifact '+str(p))
 with p.open('rb') as f:raw=f.read(cap+1)
 need(len(raw)<=cap,'growing oversized artifact');return raw

def digest(x,n=64):return isinstance(x,str) and re.fullmatch('[0-9a-f]{%d}'%n,x) is not None and x!='0'*n

def path_safe(p):
 need(isinstance(p,str) and p and '\\' not in p,'relative path type')
 q=Path(p);need(not q.is_absolute() and q.as_posix()==p and all(x not in ('.','..') for x in q.parts),'safe relative path');return p

def vectors32(v):
 need(len(v)==128 and all(isinstance(x,(int,float)) and math.isfinite(x) for x in v),'finite128D vector')
 return [f32(x) for x in v]

def population(vectors):
 h=hashlib.sha256()
 for id in sorted(vectors):
  b=id.encode();h.update(struct.pack('<I',len(b))+b+vecbits(vectors[id]))
 return h.hexdigest()

def top(vectors,query):
 nq=normalized(vectors32(query));return [{'ID':id,'Score':s} for s,id in sorted(((score(nq,normalized(v)),id) for id,v in vectors.items()),key=lambda x:(-x[0],x[1]))[:10]]

def same_truth(a,b):return [(v['ID'],bits(v['Score'])) for v in a]==[(v['ID'],bits(v['Score'])) for v in b]

def b64id(v):
 raw=base64.b64decode(v,validate=True);need(raw and len(raw)<=2048,'bounded ID');return raw.decode()

def request_zero(req):
 out=copy.deepcopy(req);out['Deadline']='0001-01-01T00:00:00Z';return out

def outcome_same(a,b):
 for key in ('Generation','OwnerGroup','CommitTerm','CommitIndex','Coverage','LiveRevision','Matched','Modified','Deleted','VisibilityToken'):
  need(a[key]==b[key],'original outcome changed '+key)
 need(b['ProductionConsensus'] is True and b['AppliedIndex']>=a['CommitIndex'],'retry actual consensus/applied')
 mutation_counters(a);mutation_counters(b)

def mutation_counters(z):
 c=z['Counters'];need(all(type(v)is int and v>=0 for v in c.values()),'integer mutation counters')
 need(all(c[k]==1 for k in ('Routes','Commits','Replications','Applies','VisibilityProofs')) and 0<=c['Forwards']<=1,'per-call route and durable outcome counters')


def request_wire(logical_raw,deadline):
 # mixedCall retains zero-deadline logical request and hashes a temporary wire
 # copy. Replace the one raw JSON time field without reencoding FP32 decimals.
 zero='"Deadline":"0001-01-01T00:00:00Z"'
 need(logical_raw.count(zero)==1,'one immutable request deadline')
 stamp(deadline)
 return logical_raw.replace(zero,'"Deadline":'+json.dumps(deadline),1)

def audit_shape(audits,required):
 need(len(audits)==4 and {d['ColocatedAudit']['NodeID'] for d in audits}==NODES,'all four distinct local audit attachments')
 chains=[]
 for d in audits:
  a=d['ColocatedAudit'];need(a['RunID']==QUERY_RUN and a['AppliedIndex']>=required and a['RetainedCount']==len(a['Witnesses'])==6 and len(a['Final'])==4,'bounded audit count/applied attachment')
  need(digest(a['PlanSHA256']) and digest(a['RetainedChain']) and a['RetainedBytes']>0 and a['CommandWALNextLSN']>0,'actual bounded audit chain/plan/WAL fields')
  chains.append((a['RetainedCount'],a['RetainedBytes'],a['RetainedChain']))
 need(len(set(chains))==1,'matching four-voter retained summary')

def ownership_shape(x,node,image,cid,nonce=None):
 need(x['Id']==cid and x['Image']==image and x['Name']=='/treedb-4250-'+RUN+('-mixed-window-c1-v1' if node=='client' else '-'+node),'exact observed name/CID/image')
 need(x['Config']['Labels']['treedb.fixed-cluster.run']==RUN,'observed run label')
 if node=='client':need(digest(nonce,32) and x['Config']['Labels']['treedb.fixed-cluster.invocation']==nonce,'exact invocation nonce')
 h=x['HostConfig'];need(h['Memory']==h['MemorySwap']==2147483648 and h['NanoCpus']==2000000000 and h['NetworkMode']=='host' and h['RestartPolicy']['Name']=='no','observed cgroup caps/network/restart')
 need(not x['State']['Running'] and x['State']['ExitCode']==0 and not x['State']['OOMKilled'],'actual clean stopped0/noOOM observation')

def resource_shape(batch,phase):
 roles=[];fields=('memory.current','memory.peak','memory.max','memory.events','memory.swap.current','memory.swap.max','memory.swap.events','cpu.max','cpu.stat','io.stat','process.status')
 need([s['host'] for s in batch]==['192.168.0.111','192.168.0.185'],'both actual host samples')
 for s in batch:
  need(s['boundary']==phase and s['started_unix']<=s['finished_unix'],'actual phase/sample clock order')
  for t in s['targets']:
   roles.append(t['node']);need(t['inspect']['State']['Running'] and t['inspect']['State']['Pid']>0 and not t['inspect']['State']['OOMKilled'],'actual alive boundary role')
   need(t['network']['status']=='unsupported','host-network unsupported byte scope')
   for f in fields:
    v=t['fields'][f];need(v['status']=='available' and isinstance(v['raw'],str) and s['started_unix']<=v['started_unix']<=v['finished_unix']<=s['finished_unix'],'actual enclosing available field '+f)
 need(len(roles)==5 and set(roles)==NODES|{'client'},'five unique boundary roles')

def mutation(w,encoded,planned,planned_encoded,generation,origin,ordinal=None):
 need(w['Invoked'] and w['Outcome']=='succeeded' and w['Error']==w['ErrorCode']==w['SkipReason']=='','successful actual mutation')
 need(w['Kind']==planned['Kind'] and w['Kind'] in ('replace','delete') and w['Ordinal']==(planned['Ordinal'] if ordinal is None else ordinal),'mutation kind/ordinal')
 key='Replace' if w['Kind']=='replace' else 'Delete';req=w[key];zero=request_zero(req)
 need(zero==planned[key] and w['LogicalSHA256']==planned['LogicalSHA256'],'original request logical bytes')
 _,plannedraw=field(planned_encoded,key);need(sha(plannedraw.encode())==planned['LogicalSHA256'],'actual planned logical JSON hash')
 _,requestraw=field(encoded,key);need(requestraw==plannedraw,'retained immutable logical request bytes')
 wire=request_wire(requestraw,w['Deadline']);need(sha(wire.encode())==w['RequestSHA256'],'actual serialized wire request hash with separate retained deadline')
 _,resraw=field(encoded,'Response');need(sha(resraw.encode())==w['ResponseSHA256'] and len(resraw.encode())==w['ResponseBytes']<=32768,'actual response bytes/hash')
 need(req['Deadline']=='0001-01-01T00:00:00Z' and origin+w['EndNS']<=stamp(w['Deadline'])<=origin+w['StartNS']+10000000000,'actual mutation deadline')
 z=w['Response'];mutation_counters(z)
 need(z['Generation']==generation and z['ProductionConsensus'] is True and z['OwnerGroup']=='group-a' and z['CommitTerm']>0 and z['CommitIndex']>0 and z['AppliedIndex']>=z['CommitIndex'],'actual original production position')
 need(z['Coverage']>0 and z['LiveRevision']>0 and base64.b64decode(z['VisibilityToken'],validate=True),'actual scoped visibility authority token')
 need((z['Matched'],z['Modified'],z['Deleted'])==((1,1,0) if key=='Replace' else (0,0,1)),'expected changed mutation counts')
 return z

def native(response,request,state,truth):
 need(response is not None and response['Generation']==request['Generation'] and len(response['Neighbors'])==10,'strict response generation/top10')
 c=response['Counters'];need(all(type(v)is int and v>=0 for v in c.values()),'integer search counters')
 need(c['SelectedPartitions']==c['HNSWServedPartitions']==c['ReadProofs']==c['RPCs']==c['GenerationPins']==1 and c['ExactScanPartitions']==c['Retries']==c['Redirects']==0 and c['Candidates']>0 and c['Edges']>0,'strict native bounded proof counters')
 nq=normalized(vectors32(request['Query']));seen=set();previous=None
 for n in response['Neighbors']:
  id=n['ID'];s=n['Score'];need(id in state and id not in seen and math.isfinite(s),'unique known present neighbor');seen.add(id)
  need(bits(score(nq,normalized(state[id])))==bits(s),'exact canonical FP32 response score')
  if previous is not None:need(previous[0]>s or (bits(previous[0])==bits(s) and previous[1]<id),'canonical score order/stable ties')
  previous=(s,id)
 return len(seen & {n['ID'] for n in truth})/10

def causal_range(writes,start,end):
 need(0<=start<end,'search monotonic interval');lower=upper=0
 for i,w in enumerate(writes):
  if w['Outcome']=='succeeded' and w['EndNS']<=start:lower=i+1
  if w['Invoked'] and w['StartNS']<=end:upper=i+1
 need(0<=lower<=upper<=6,'causal interval');return lower,upper

def one_prefix(response,request,states,truth,lower,upper):
 errors=[]
 for i in range(lower,upper+1):
  try:return i,native(response,request,states[i],truth)
  except (AssertionError,KeyError,ValueError) as e:errors.append(str(e))
 raise AssertionError('no ONE causally permitted full response prefix: '+repr(errors))

def raw_report(path):
 raw=bounded(path);need(raw.endswith(b'\n'),'complete JSONL')
 lines=[line.decode() for line in raw.splitlines() if line.strip()];events=[strict(line) for line in lines]
 p=[(e['Report'],line) for e,line in zip(events,lines) if e.get('Event')=='planned'];r=[(e['Report'],line) for e,line in zip(events,lines) if e.get('Event')=='result']
 need(len(p)==len(r)==1,'one original planned/result event, no replacement');return p[0][0],r[0][0],field(p[0][1],'Report')[1],field(r[0][1],'Report')[1],sha(raw)

def source_links(a):
 need(a['Version']==1 and a['RunID']==RUN and a['remote_root']=='/home/mikers/gomap-4250-twohost-'+RUN,'fresh manifest identity')
 need(all(a[k] is True for k in ('RootAccepted','SourcePrereviewAccepted','MixedWindowModeValidated','ResourceGateModeValidated','DriverUserReadAccessValidated')),'root admission flags')
 need(digest(a['source_head'],40) and digest(a['source_tree'],40),'source identities')
 pinned={}
 for path,h in a['local_pins'].items():
  raw=bounded(path,64<<20);need(digest(h) and sha(raw)==h,'actual manifest local pin');pinned[path]=raw
 need(sum(map(len,pinned.values()))<=256<<20,'bounded local pins')
 rec=a['receipts'];s=strict(pinned[rec['source_inventory']]);review=strict(pinned[rec['source_prereview']]);build=strict(pinned[rec['build']])
 need(s['head']==review['candidate_head']==build['head']==a['source_head'] and s['tree']==review['candidate_tree']==build['tree']==a['source_tree'],'source/build/prereview links')
 need(review['outcome']=='ACCEPT' and build['state']=='VERIFIED_BUILD_FROM_FROZEN_SOURCE' and build['source_verified_before_after'] is True,'accepted reviewed frozen build')
 need(sha(pinned[rec['source_inventory']])==a['source_inventory_sha256']==build['source_inventory_sha256'],'actual full inventory hash')
 seen=set()
 for row in s['rows']:
  path=path_safe(row['path']);need(path not in seen and digest(row['git_blob'],40) and row['mode'] in ('100644','100755'),'source inventory unique paths/blobs');seen.add(path)
 need(s['rows'] and all(path_safe(k) in seen and digest(v) for k,v in s['overlays'].items()),'source inventory overlays')
 need(build['driver_sha256']==a['driver_sha256'] and build['driver_image']==a['query_image'],'driver build bindings')
 need(len(a['nodes'])==4 and {n['node'] for n in a['nodes']}==NODES and set(build['server_images'])==NODES,'four voter build bindings')
 for n in a['nodes']:need(n['server_sha256']==build['server_sha256'] and n['image']==build['server_images'][n['node']] and digest(n['cid']),'server actual ELF/image binding')
 need(len({n['cid'] for n in a['nodes']})==4,'four unique voter CIDs')
 need(sha(pinned[rec['bootstrap']])==a['bootstrap_sha256'] and sha(pinned[rec['config']])==a['config_sha256'] and sha(pinned[rec['plan']])==a['plan_sha256'],'actual config/bootstrap/plan links')
 need(strict(pinned[rec['baseline']])==a['baseline'],'measured baseline receipt')
 need(sha(bounded(COLLECTION/'collector-source.py',1<<20))==a['collector_sha256'],'actual collector source pin')
 need(bounded(COLLECTION/'approved-inputs.json',1<<20)==approved_raw,'actual collection exact approved input bytes')
 return pinned

def bootstrap_prefix(boot,fresh):
 a,b=boot['Insert'],boot['Retry']
 need(a['VisibleID']==b['VisibleID']==fresh and 0<a['CommitIndex']<b['CommitIndex'],'fresh ordinary insert bootstrap retry advances consensus position')
 need(a['ProductionConsensus'] is True and b['ProductionConsensus'] is True and a['AppliedIndex']>=a['CommitIndex'] and b['AppliedIndex']>=b['CommitIndex'],'actual bootstrap consensus/applied coverage')
 return b['CommitIndex']

def anchor_json_digest(vector,serialized):
 need(isinstance(serialized,str) and vecbits(vectors32(strict(serialized)))==vecbits(vector),'actual Go-serialized anchor vector preserves exact FP32 bits')
 return sha(serialized.encode())

def admission_truth(vectors,admission):
 corpus={id:v for id,v in vectors.items() if re.fullmatch(r'doc-[0-9]{6}',id)}
 excluded=set()
 for q in admission['Queries']:
  full=top(vectors,q['Request']['Query']);pure=top(corpus,q['Request']['Query'])
  need(same_truth(q['Truth'],full) and same_truth(q['CorpusTruth'],pure),'independent admission full/corpus canonical truths')
  excluded.update(n['ID'] for n in full+pure)
 return excluded

def reconstruct(a,pinned):
 invraw=bounded(INPUT/'input-inventory.json',1<<20);need(sha(invraw)==a['input_inventory_sha256'],'fresh input inventory pin');inv=strict(invraw)
 need(0<len(inv)<=512 and {'probe.jsonl','provenance.json','dataset/manifest.json'}<=set(inv),'fresh required input bundle')
 for name,h in inv.items():need(path_safe(name) and sha(bounded(INPUT/name,64<<20))==h,'actual input bundle pin')
 manifest=strict(bounded(INPUT/'dataset/manifest.json',1<<20));need((manifest['docs'],manifest['dimensions'],manifest['queries'],manifest['top_k'])==(10000,128,16,10),'10K128D16query corpus')
 for name,meta in manifest['files'].items():
  raw=bounded(INPUT/'dataset'/path_safe(name),64<<20);need(len(raw)==meta['bytes'] and sha(raw)==meta['sha256'],'dataset payload pin')
 corpus=rows(bounded(INPUT/'dataset/documents.f32'),10000);queries=rows(bounded(INPUT/'dataset/queries.f32'),16)
 vectors={'doc-%06d'%i:vectors32(v) for i,v in enumerate(corpus)}
 boot=strict(pinned[a['receipts']['bootstrap']]);fresh=RUN+'/dataset-'+sha(bounded(INPUT/'dataset/manifest.json'))+'-fresh-y'
 highest=bootstrap_prefix(boot,fresh)
 anchor_hashes={}
 for id,x,y in [('seed-x',1,0),('seed-minus-x',-1,0),('seed-minus-y',0,-1),(fresh,0,1)]:
  vectors[id]=[float(x),float(y)]+[0.]*126
  anchor_hashes[id]=anchor_json_digest(vectors[id],json.dumps([x,y]+[0]*126,separators=(',',':')))
 pp,probe,_,probetext,probehash=raw_report(INPUT/'probe.jsonl');prov=strict(bounded(INPUT/'provenance.json',1<<20))
 need(prov['Phase']=='post-only' and prov['CampaignID']==RUN and prov['BootstrapRequestID']==RUN and prov['ProbeSHA256']==probehash,'fresh actual bounded probe provenance')
 need(all(prov[k] is True for k in ('RootAccepted','InitializationSucceeded','CleanReopenSucceeded','ExclusiveWriterStopped')),'fresh probe initialization/admission authority declarations')
 need(prov['RuntimeSourceHead']==a['source_head'] and prov['ServerBinarySHA256']==a['nodes'][0]['server_sha256'],'fresh source runtime provenance')
 need(probe['ConfigSHA256']==a['config_sha256'] and probe['BootstrapSHA256']==a['bootstrap_sha256'] and probe['BinarySHA256']==prov['ProbeBinarySHA256'],'probe exact input/ELF provenance')
 ops=probe['Operations'];need(len(ops)==len(pp['Operations'])==70 and probe['Counts']==account(ops) and probe['Counts']['Succeeded']==70,'fresh-inserts1 actual70 original operations all retained')
 originals=[];retries=[]
 op_raw=pieces(probetext,'Operations');need([x for x,_ in op_raw]==ops,'actual complete probe operations serialization')
 for i,(op,encoded) in enumerate(op_raw):
  need(op['Ordinal']==i and op['Outcome']=='succeeded' and op['Error']==op['ErrorCode']=='','complete successful bounded probe ledger')
  if op['Kind']!='insert':continue
  z=op['InsertResponse'];req=op['InsertRequest'];id=b64id(req['ID']);need(z['VisibleID']==id and z['ProductionConsensus'] and z['AppliedIndex']>=z['CommitIndex'],'actual probe insert identity/consensus')
  highest=max(highest,z['CommitIndex'])
  if op['Phase']=='explicit-retry':retries.append(op);continue
  need(id not in vectors,'distinct original probe ID');vectors[id]=vectors32(req['Vector']);originals.append(op)
  _,request_raw=field(encoded,'InsertRequest');_,vector_raw=field(request_raw,'Vector');anchor_hashes[id]=anchor_json_digest(vectors[id],vector_raw)
 need(len(originals)==len(retries)==1 and request_zero(originals[0]['InsertRequest'])==request_zero(retries[0]['InsertRequest']),'probe exact original attempt retry, no added retry row')
 z,y=originals[0]['InsertResponse'],retries[0]['InsertResponse'];need(all(z[k]==y[k] for k in ('Generation','OwnerGroup','PartitionID','VisibleID','LiveRevision')) and y['CommitIndex']>z['CommitIndex'],'ordinary probe retained identity with actual new consensus retry')
 b=a['baseline'];need(b['CorpusRows']==10000 and b['AnchorRows']==len(vectors)-10000 and b['PopulationRows']==len(vectors) and b['PopulationSHA256']==population(vectors) and b['HighestCommitIndex']==highest,'measured full frozen baseline, all actual anchors and fresh highest prefix')
 need(probe['HighestCommitIndex']==highest,'complete functional probe declared highest consensus prefix')
 return vectors,queries,probehash,anchor_hashes

def prefix_states(vectors,p,ptext,anchor_hashes):
 need(len(p['Writes'])==6 and len(p['Prefixes'])==7 and len(p['Admission']['Queries'])==16,'complete six-slot seven-prefix plan')
 states=[copy.deepcopy(vectors)];ids=[]
 for w in p['Writes']:
  req=w.get('Replace',w.get('Delete'));ids.append(b64id(req['ID']))
 ids=list(dict.fromkeys(ids));need(len(ids)==4,'four existing changed IDs')
 excluded=admission_truth(vectors,p['Admission'])
 expected=[id for id in sorted(vectors) if id.startswith('doc-') and id not in excluded][:4]
 need(ids==expected,'four deterministic corpus IDs outside every baseline/corpus top10')
 need([w['Kind'] for w in p['Writes']]==['replace','replace','delete','replace','delete','delete'],'six exact mutation sequence')
 need([b64id(w.get('Replace',w.get('Delete'))['ID']) for w in p['Writes']]==[ids[0],ids[0],ids[1],ids[2],ids[2],ids[3]],'supersession/delete targets')
 for i,w in enumerate(p['Writes']):
  req=w.get('Replace',w.get('Delete'));id=b64id(req['ID']);state=copy.deepcopy(states[-1]);need(id in state,'replace/delete existing original prefix')
  need(req['Version']==1 and req['Generation']==p['Admission']['Generation'] and b64id(req['IdempotencyKey'])==QUERY_RUN+'-mixed-'+str(i),'exact singleton attempt identity')
  if w['Kind']=='replace':
   expected=vectors[ids[1] if i==0 else ids[3]];need(vecbits(vectors32(req['Vector']))==vecbits(expected),'exact planned replacement vector')
   doc=strict(base64.b64decode(req['Document'],validate=True));need(set(doc)=={'embedding','kind'} and vecbits(vectors32(doc['embedding']))==vecbits(expected) and doc['kind']=='mixed-'+QUERY_RUN+'-'+str(i),'planned canonical replacement document')
   state[id]=vectors32(req['Vector'])
  else:del state[id]
  states.append(state)
 anchor_ids=set(vectors)-{'doc-%06d'%i for i in range(10000)}
 need(set(anchor_hashes)==anchor_ids,'all actual anchor serialized vector sources')
 anchors=[{'ID':id,'VectorSHA256':anchor_hashes[id]} for id in sorted(anchor_ids)]
 need(p['Anchors']==anchors,'every non-corpus anchor exact vector bits')
 praw=pieces(ptext,'Prefixes');base=None
 for i,(proof,raw) in enumerate(praw):
  need(proof==p['Prefixes'][i] and proof['Prefix']==i and proof['PopulationRows']==len(states[i]) and proof['PopulationSHA256']==population(states[i]),'full canonical prefix population digest')
  truths=[];changed=[]
  for q in p['Admission']['Queries']:
   truth=top(states[i],q['Request']['Query']);truths.append(truth);row=[];nq=normalized(vectors32(q['Request']['Query']))
   for id in ids:row.append(dict(ID=id,Present=id in states[i],ScoreBits=struct.unpack('<I',bits(score(nq,normalized(states[i][id]))))[0] if id in states[i] else 0))
   changed.append(row)
  need(len(proof['Truth'])==16 and all(same_truth(a,b) for a,b in zip(proof['Truth'],truths)) and proof['Changed']==changed,'independent full128D FP32 truths and changed presence bits')
  _,truthraw=field(raw,'Truth');need(proof['Top10SHA256']==sha(truthraw.encode()),'actual serialized top10 digest')
  if base is None:base=truths
  else:need(all(same_truth(a,b) for a,b in zip(base,truths)),'invariant full-population top10 across all seven prefixes')
 return states,ids

OUTSTANDING=[
 'Complete original/retained functional probe request/deadline/raw response hashes, readiness and bootstrap raw initialization/reopen/transport chain; current core reconstructs acknowledged baseline, not that whole prerequisite proof.',
 'Exact measured/read/visibility/pre-post recall serialized request/response hashes, deadlines, token floors and complete accounting/QPS/latency/overlap/query-coverage acceptance threshold.',
 'Authenticate every audit attachment to raw DiagnosticsWithColocatedAudit transport; independently decode exact plan/witness attempt and scope digests, covered physical WAL/current-FSM/root/applied fencing, six retained ordered outcome-chain and four local source+live membership/content proofs.',
 'Raw serial SSH/Docker commands and returned CID/image/ELF/mount/caps/nonce ownership, all five exact gate tokens/ACK installation/presample/postsample directory+liveness proofs, conservative two-host clock/per-field eleven-field resource brackets and exact all-voter stop0/noOOM ordering after audits.'
]

def verify(approved):
 global approved_raw
 approved_raw=bounded(approved,1<<20);a=strict(approved_raw);pinned=source_links(a)
 p,r,ptext,text,stdoutsha=raw_report(COLLECTION/'stdout.jsonl');vectors,queries,probehash,anchor_hashes=reconstruct(a,pinned)
 need(p['Admission']['RunID']==r['Admission']['RunID']==QUERY_RUN and p['Admission']['Phase']==r['Admission']['Phase']=='post-only','fresh mixed post-only identity')
 for ad in (p['Admission'],r['Admission']):
  b=a['baseline'];need(ad['PopulationRows']==len(vectors) and ad['PopulationSHA256']==population(vectors) and ad['HighestCommitIndex']==b['HighestCommitIndex'],'actual admission baseline')
  need(ad['BinarySHA256']==a['driver_sha256'] and ad['ConfigSHA256']==a['config_sha256'] and ad['BootstrapSHA256']==a['bootstrap_sha256'],'actual admission binary/input')
  need(len(ad['Queries'])==16 and all(vecbits(vectors32(q['Request']['Query']))==vecbits(v) for q,v in zip(ad['Queries'],queries)),'actual corpus query FP32 bits')
  admission_truth(vectors,ad)
 states,ids=prefix_states(vectors,p,ptext,anchor_hashes);need(r['Prefixes']==p['Prefixes'] and r['Anchors']==p['Anchors'],'immutable full truth/anchors')
 need(len(r['Writes'])==6 and len(r['Retries'])==2,'all original and after-drain retry attempts retained')
 wraw=pieces(text,'Writes');pwraw=pieces(ptext,'Writes');origin=stamp(r['MeasuredOriginUTC']);previous=a['baseline']['HighestCommitIndex'];lastend=laststart=0
 for i,((w,raw),(pw,praw)) in enumerate(zip(wraw,pwraw)):
  need(w['Phase']=='mixed-measured' and w['IntendedOffsetNS']==i*5000000000 and w['StartNS']>=max(lastend,i*5000000000) and (i==0 or w['StartNS']-laststart>=5000000000),'serial paced original boundary')
  need(w['StartNS']<w['EndNS']<=r['ActualDurationNS'] and w['StartNS']+20000000000<=60000000000,'complete write+proof budgets admitted')
  z=mutation(w,raw,pw,praw,r['Admission']['Generation'],origin);need(z['CommitIndex']>previous,'new original strictly increasing consensus position');previous=z['CommitIndex'];lastend=w['EndNS'];laststart=w['StartNS']
 need(r['HighestNewCommitIndex']==previous,'highest original only prefix')
 retryorigin=stamp(r['RetryOriginUTC']);need(retryorigin>=origin+r['ActualDurationNS'],'explicit retries after measured drain')
 for i,(w,raw) in enumerate(pieces(text,'Retries')):
  original=r['Writes'][[0,2][i]];pw,praw=pwraw[[0,2][i]]
  need(w['Phase']=='after-drain-original-retry' and request_zero(w.get('Replace',w.get('Delete')))==request_zero(original.get('Replace',original.get('Delete'))),'retry exact original attempt after supersession/deletion')
  mutation(w,raw,pw,praw,r['Admission']['Generation'],retryorigin);outcome_same(original['Response'],w['Response'])
 need(r['RequiredAppliedIndex']==max(w['Response']['AppliedIndex'] for w in r['Writes']+r['Retries']),'observed ACK/retry required applied floor')
 measured=[a for a in r['Attempts'] if a['Phase']=='measured'];proofs=r['ReadPrefixes'];need(len(measured)==len(proofs),'every measured response causal proof')
 for attempt,claim in zip(measured,proofs):
  need(attempt['Outcome']=='succeeded' and attempt['Error']==attempt['ErrorCode']=='','successful measured response, no UNKNOWN discard')
  qi=attempt['Ordinal']%16;q=r['Admission']['Queries'][qi];lo,hi=causal_range(r['Writes'],attempt['StartNS'],attempt['EndNS']);matched,recall=one_prefix(attempt['Response'],q['Request'],states,q['Truth'],lo,hi)
  need(claim==dict(Ordinal=attempt['Ordinal'],Lower=lo,Upper=hi,Matched=matched) and attempt['RecallAt10']==recall,'independent ONE-prefix causal score/recall proof')
 audit_shape(r['Audits'],r['RequiredAppliedIndex'])
 return dict(verdict='INCOMPLETE_NON_ACCEPTING_VALIDATION',status='CORE_CHECKS_PASS_REMAINING_GATES_OPEN',source_head=a['source_head'],source_tree=a['source_tree'],baseline_rows=len(vectors),baseline_population_sha256=population(vectors),baseline_anchor_rows=len(vectors)-10000,highest_original_commit=previous,stdout_sha256=stdoutsha,probe_sha256=probehash,verified_core=['actual pinned source/build/prereview/input bindings','fresh10K+seed/bootstrap/original probe baseline','seven full canonical FP32 population/top10/changed-bits truths','six original raw request/response hashes/deadlines/counts','two exact original durable retries with independently variable Forwards','single causal full-response prefix score/recall checks','four local audit structural attachments only'],outstanding=OUTSTANDING,scope='NO acceptance/resource/performance/whole-population current-source authority; root must finish all remaining proof gates')

def self_check():
 tests=[]
 def yes(name,fn):fn();tests.append(dict(name=name,outcome='PASS'))
 def no(name,fn):
  try:fn()
  except (AssertionError,ValueError,KeyError,TypeError):tests.append(dict(name=name,outcome='PASS_REJECTED'));return
  raise AssertionError('tamper admitted '+name)
 def ok(v):need(v,'synthetic condition')
 no('duplicate JSON keys',lambda:strict('{"x":1,"x":2}'));no('traversal anchor input',lambda:path_safe('../escape'))
 yes('top-level array ignores preceding nested counter',lambda:ok(pieces('{"Counters":{"Retries":0},"Retries":[{"Score":-0}]}','Retries')[0][1]=='{"Score":-0}'))
 yes('top-level field ignores nested and encoded-looking text',lambda:ok(field('{"Nested":{"Response":0},"Text":"Response\\\":0","Response":{"x":1}}','Response')[1]=='{"x":1}'))
 yes('raw selector preserves whitespace and escaped member key',lambda:ok(field(' { "\\u0052etries" : [ {"Score":-0} ] } ','Retries')[1]=='[ {"Score":-0} ]'))
 yes('raw array retains nested exact bytes and signed zero',lambda:ok(bits(pieces('{"Retries": [ {"Score":-0}, {"Score":1} ]}','Retries')[0][0]['Score'])==bits(-0.)))
 no('nested field cannot stand in for absent top-level',lambda:field('{"Nested":{"Retries":[]}}','Retries'))
 no('raw selector rejects duplicate top-level keys',lambda:field('{"Retries":[],"Retries":[]}','Retries'))
 no('raw selector rejects nested duplicate keys',lambda:pieces('{"Retries":[{"x":1,"x":2}]}','Retries'))
 no('raw selector rejects malformed trailing data',lambda:field('{"Retries":[]} trailing','Retries'))
 no('raw selector rejects nonfinite nested values',lambda:pieces('{"Retries":[NaN]}','Retries'))
 no('raw array selector rejects numeric counter',lambda:pieces('{"Retries":0}','Retries'))
 v=[1.,0.]+[0.]*126;yes('FP32 oracle exact scalar order',lambda:ok(bits(score(normalized(v),normalized(v)))==bits(1.)))
 yes('FP32 signed zero bit distinction retained',lambda:ok(bits(-0.)!=bits(0.)))
 yes('raw array decoder preserves Go numeric signed zero',lambda:ok(bits(pieces('{"a":[{"Score":-0}]}','a')[0][0]['Score'])==bits(-0.)))
 yes('raw field decoder preserves Go numeric signed zero',lambda:ok(bits(field('{"Score":-0}','Score')[0])==bits(-0.)))
 b={'x':v,'y':[-1.,0.]+[0.]*126};yes('full population digest changes with anchor',lambda:ok(population(b)!=population({'x':v})))
 writes=[dict(Outcome='succeeded',Invoked=True,StartNS=10,EndNS=20),dict(Outcome='succeeded',Invoked=True,StartNS=30,EndNS=40)]
 yes('ACK floor and overlap range',lambda:ok(causal_range(writes,25,35)==(1,2)));yes('after ACK forbids old prefix',lambda:ok(causal_range(writes,41,42)==(2,2)))
 req=dict(Generation='g',Query=v);counter=dict(SelectedPartitions=1,HNSWServedPartitions=1,ReadProofs=1,RPCs=1,GenerationPins=1,ExactScanPartitions=0,Retries=0,Redirects=0,Candidates=10,Edges=10)
 state={'id%02d'%i:[1.,float(i)/20]+[0.]*126 for i in range(10)};truth=top(state,v);response=dict(Generation='g',Neighbors=copy.deepcopy(truth),Counters=counter)
 yes('single entire response prefix',lambda:ok(one_prefix(response,req,[state],truth,0,0)==(0,1.)))
 altered=copy.deepcopy(response);altered['Neighbors'][0]['Score']=.25;no('canonical bit tamper',lambda:one_prefix(altered,req,[state],truth,0,0))
 altered=copy.deepcopy(response);altered['Neighbors'][1]['ID']=altered['Neighbors'][0]['ID'];no('duplicate neighbor',lambda:one_prefix(altered,req,[state],truth,0,0))
 newer=copy.deepcopy(state);del newer[truth[0]['ID']];no('old present row after delete ACK',lambda:one_prefix(response,req,[state,newer],truth,1,1))
 # Two changed scores drawn from different states cannot be combined.
 other=copy.deepcopy(state);other['id00']=[1.,.01]+[0.]*126;other['id01']=[1.,.1]+[0.]*126
 mixed=copy.deepcopy(response);mixed['Neighbors'][1]['Score']=score(normalized(v),normalized(other[mixed['Neighbors'][1]['ID']]))
 no('no per-neighbor prefix mixing',lambda:one_prefix(mixed,req,[state,other],truth,0,1))
 c=dict(Routes=1,Commits=1,Replications=1,Applies=1,VisibilityProofs=1,Forwards=0)
 out=dict(Generation='g',OwnerGroup='group-a',CommitTerm=1,CommitIndex=2,Coverage=3,LiveRevision=4,Matched=1,Modified=1,Deleted=0,VisibilityToken='token',ProductionConsensus=True,AppliedIndex=2,Counters=c)
 retry=copy.deepcopy(out);retry['Counters']['Forwards']=1;retry['AppliedIndex']=7;yes('retry allows legitimate per-call forward difference',lambda:outcome_same(out,retry))
 for key in ('CommitTerm','CommitIndex','Coverage','LiveRevision','Matched','Modified','Deleted'):
  tamper=copy.deepcopy(retry);tamper[key]+=1;no('original outcome '+key,lambda t=tamper:outcome_same(out,t))
 tamper=copy.deepcopy(retry);tamper['Counters']['Forwards']=2;no('invalid per-call forward',lambda:outcome_same(out,tamper))

 logical='{"Version":1,"Deadline":"0001-01-01T00:00:00Z","Vector":[0.12345679]}'
 deadline='2026-10-05T00:00:00.123456789Z'
 yes('wire deadline hashing preserves exact FP32 decimal spelling',lambda:ok(request_wire(logical,deadline)=='{"Version":1,"Deadline":"'+deadline+'","Vector":[0.12345679]}'))
 reqlogical=dict(Version=1,Generation='g',ID=base64.b64encode(b'x').decode(),IdempotencyKey=base64.b64encode(b'k').decode(),Deadline='0001-01-01T00:00:00Z')
 request_raw=json.dumps(reqlogical,separators=(',',':'));z=copy.deepcopy(out);z.update(Matched=0,Modified=0,Deleted=1,VisibilityToken=base64.b64encode(b'token').decode())
 response_raw=json.dumps(z,separators=(',',':'));planned=dict(Ordinal=2,Kind='delete',Delete=reqlogical,LogicalSHA256=sha(request_raw.encode()))
 attempt=dict(Ordinal=2,Kind='delete',Delete=reqlogical,Invoked=True,Outcome='succeeded',Error='',ErrorCode='',SkipReason='',LogicalSHA256=planned['LogicalSHA256'],RequestSHA256=sha(request_wire(request_raw,deadline).encode()),ResponseSHA256=sha(response_raw.encode()),Response=z,ResponseBytes=len(response_raw.encode()),Deadline=deadline,StartNS=1,EndNS=2)
 encoded=json.dumps(attempt,separators=(',',':'));pencoded=json.dumps(planned,separators=(',',':'));origin=stamp(deadline)-1000000000
 yes('retry original ordinal2 retains delete2 logical bytes and new wire deadline',lambda:mutation(attempt,encoded,planned,pencoded,'g',origin))
 tamper=copy.deepcopy(attempt);tamper['Ordinal']=1;no('delete retry cannot use enumerate ordinal1',lambda:mutation(tamper,json.dumps(tamper,separators=(',',':')),planned,pencoded,'g',origin))

 no('missing raw logical deadline',lambda:request_wire('{}',deadline))
 audits=[dict(ColocatedAudit=dict(NodeID=n,RunID=QUERY_RUN,AppliedIndex=9,RetainedCount=6,RetainedBytes=100,RetainedChain='a'*64,PlanSHA256='b'*64,CommandWALNextLSN=2,Witnesses=[{}]*6,Final=[{}]*4)) for n in sorted(NODES)]
 yes('four-voter audit summary shape',lambda:audit_shape(audits,9))
 no('missing follower audit',lambda:audit_shape(audits[:-1],9))
 altered=copy.deepcopy(audits);altered[0]['ColocatedAudit']['AppliedIndex']=8;no('uncovered local audit',lambda:audit_shape(altered,9))
 altered=copy.deepcopy(audits);altered[0]['ColocatedAudit']['RetainedChain']='c'*64;no('mismatched retained chain',lambda:audit_shape(altered,9))
 x=dict(Id='d'*64,Image='sha256:'+'e'*64,Name='/treedb-4250-'+RUN+'-mixed-window-c1-v1',Config=dict(Labels={'treedb.fixed-cluster.run':RUN,'treedb.fixed-cluster.invocation':'f'*32}),HostConfig=dict(Memory=2147483648,MemorySwap=2147483648,NanoCpus=2000000000,NetworkMode='host',RestartPolicy=dict(Name='no')),State=dict(Running=False,ExitCode=0,OOMKilled=False))
 yes('observed driver exact nonce/identity/clean-stop shape',lambda:ownership_shape(x,'client',x['Image'],x['Id'],'f'*32))
 no('foreign driver nonce',lambda:ownership_shape(x,'client',x['Image'],x['Id'],'a'*32))
 altered=copy.deepcopy(x);altered['State']['OOMKilled']=True;no('OOM cannot become clean-stop',lambda:ownership_shape(altered,'client',x['Image'],x['Id'],'f'*32))
 fields=('memory.current','memory.peak','memory.max','memory.events','memory.swap.current','memory.swap.max','memory.swap.events','cpu.max','cpu.stat','io.stat','process.status')
 batch=[]
 for host,roles in [('192.168.0.111',['node-a','node-b']),('192.168.0.185',['node-c','node-d','client'])]:
  batch.append(dict(host=host,boundary='ready',started_unix=0,finished_unix=1,targets=[dict(node=n,inspect=dict(State=dict(Running=True,Pid=1,OOMKilled=False)),network=dict(status='unsupported'),fields={f:dict(status='available',raw='',started_unix=.1,finished_unix=.2) for f in fields}) for n in roles]))
 yes('all five actual roles/eleven fields incl empty io.stat',lambda:resource_shape(batch,'ready'))
 altered=copy.deepcopy(batch);altered[0]['targets'][0]['fields']['memory.current']['status']='missing';no('gate cannot replace missing field read',lambda:resource_shape(altered,'ready'))
 altered=copy.deepcopy(batch);altered[1]['targets'].pop();no('missing client boundary observation',lambda:resource_shape(altered,'ready'))
 no('nonfinite JSON',lambda:strict('{"x":NaN}'))
 boot=dict(Insert=dict(VisibleID='fresh',CommitIndex=87,AppliedIndex=87,ProductionConsensus=True),Retry=dict(VisibleID='fresh',CommitIndex=88,AppliedIndex=90,ProductionConsensus=True))
 yes('ordinary bootstrap retry later position retained',lambda:ok(bootstrap_prefix(boot,'fresh')==88))
 for n in (86,87):
  altered=copy.deepcopy(boot);altered['Retry']['CommitIndex']=n;no('bootstrap retry refuses equal/decreasing '+str(n),lambda t=altered:bootstrap_prefix(t,'fresh'))
 rawvector='[1,0'+',0'*126+']'
 yes('Go JSON anchor hash differs from binary population vector hash',lambda:ok(anchor_json_digest(v,rawvector)==sha(rawvector.encode()) and anchor_json_digest(v,rawvector)!=sha(vecbits(v))))
 signed=[-0.,.12345679]+[0.]*126;rawsigned='[-0,0.12345679'+',0'*126+']'
 yes('actual probe shortest FP32 spelling and signed zero retained',lambda:ok(anchor_json_digest(vectors32(signed),rawsigned)==sha(rawsigned.encode())))
 no('anchor vector bit tamper',lambda:anchor_json_digest(v,rawsigned))
 corpus={'doc-%06d'%i:[1.,i/20]+[0.]*126 for i in range(10)};augmented=dict(corpus,anchor=[0.,1.]+[0.]*126)
 admission=dict(Queries=[dict(Request=dict(Query=v),Truth=top(augmented,v),CorpusTruth=top(corpus,v))])
 yes('independent full and corpus admission truths',lambda:ok(len(admission_truth(augmented,admission))==10))
 for key in ('Truth','CorpusTruth'):
  altered=copy.deepcopy(admission);altered['Queries'][0][key][0]['Score']=.25;no('tampered independent admission '+key,lambda t=altered:admission_truth(augmented,t))
 return dict(verdict='SELF_CHECK_PASS_NOT_ARTIFACT_ACCEPTANCE',checks=tests,check_count=len(tests),runtime_or_network_invocations=0,helper_files=PINS,helper_functions=HELPERS,outstanding=OUTSTANDING,limitations='Pure checks exercise oracle/causal/retry/anchor primitives. Audit/ownership/resource SHAPE tamper checks are exercised, but authenticated raw transport/identity/mount/clock/ordering integration is explicitly outstanding, not accepted proof.')

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--approved',type=Path);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--self-check',action='store_true');args=parser.parse_args()
 need(not args.output.exists(),'proof output must not overwrite evidence')
 try:
  if args.self_check:need(args.approved is None,'self-check has no campaign inputs');proof=self_check()
  else:need(args.approved is not None,'actual approved inputs required');proof=verify(args.approved)
 except Exception as e:proof=dict(verdict='REJECT',status='FAILED_OR_MISSING_EVIDENCE',error_type=type(e).__name__,error=str(e),outstanding=OUTSTANDING,scope='no acceptance inference from incomplete or failed evidence')
 proof['validator_sha256']=sha(Path(__file__).read_bytes());proof['helper_functions']=HELPERS
 with args.output.open('x') as f:json.dump(proof,f,indent=2);f.write('\n')
 print(json.dumps(dict(verdict=proof['verdict'],output=str(args.output),validator_sha256=proof['validator_sha256'])))
 return 0 if proof['verdict']=='SELF_CHECK_PASS_NOT_ARTIFACT_ACCEPTANCE' else (2 if proof['verdict']=='INCOMPLETE_NON_ACCEPTING_VALIDATION' else 1)
if __name__=='__main__':raise SystemExit(main())
