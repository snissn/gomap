if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import sys
sys.dont_write_bytecode=True
from source_paths import source_path
"""Pure four-voter audit attachment accounting; NEVER campaign acceptance.
Exact Report JSON text, authenticated config and seven reconstructed populations
are caller inputs. No subprocess/network/Go or historical campaign execution.
"""
import argparse, base64, copy, hashlib, importlib.util, json, struct, sys
from pathlib import Path
CORE=source_path('/tmp/gomap-4994-mixed-window-artifact-verify-root-v7.py')
CORE_SHA='2af4fdcf0a78a1607ddaa199ef18e26d6fa69b04c4b10fa0917496cb1c2083f5'
if hashlib.sha256(Path(CORE).read_bytes()).hexdigest()!=CORE_SHA: raise ValueError('frozen core pin')
spec=importlib.util.spec_from_file_location('audit_frozen_core',CORE)
c=importlib.util.module_from_spec(spec);spec.loader.exec_module(c)
need=c.need
NODES=('node-a','node-b','node-c','node-d')
RUN='rf4trial14mixedc1mixedc1v1'
SCOPE_LABEL='six retained original outcomes and known-ID canonical source/absence plus exact live membership only; no entire population proof'
NUMS=('ExpectedCatalogVersion','Term','Index','Coverage','Revision','Matched','Affected','Ordinal','AppliedCommandLSN')
LIMITATIONS=[
 'Attachment accounting only; authenticated peer transport, live acquisition before shutdown, current-FSM identity/ACTIVE fences and actual execution of source/live/WAL proof require independently retained outer evidence.',
 'Scope digest preimage includes internal lifecycle Identity, ReadySetDigest and router ModelDigest; these are not in the retained public mutation/audit attachments. Exact scope/token/outcome consistency is checked, not independent scope-preimage reconstruction.',
 'Final receipts contain expected-document hashes and live status, not actual source document bytes or per-domain membership evidence. They bind the product prepared-owner canonical source/absence and exact membership attestation; ANN results alone are never source membership authority.',
 'One physical StateToken and WAL next-LSN are retained per voter; internal before/after equality, manager-quiescence and current-FSM rechecks are product attestation, not independently serialized fence history.',
 'Six known IDs/witnesses only; no whole-population source membership, general history, resource, capacity or campaign acceptance.'
]

def u64(x,label,positive=False):
 need(type(x)is int and (0<x if positive else 0<=x) and x<1<<64,label+' uint64');return x

def octets(x,label):
 need(isinstance(x,list) and len(x)==32 and all(type(v)is int and 0<=v<=255 for v in x) and any(x),label+' exact nonzero 32 bytes');return bytes(x)

def b64(x,label,cap=65536):
 need(isinstance(x,str),label+' base64 string');v=base64.b64decode(x,validate=True)
 need(base64.b64encode(v).decode()==x and 0<len(v)<=cap,label+' canonical bounded base64');return v

def gojson(x):
 # Here only integer/string/bool/null objects are reencoded; FP32 request JSON
 # is copied from retained raw pieces rather than Python float serialization.
 return json.dumps(x,ensure_ascii=False,separators=(',',':')).replace('<','\\u003c').replace('>','\\u003e').replace('&','\\u0026').replace('\u2028','\\u2028').replace('\u2029','\\u2029')

def scope_bytes(s):
 need(set(s)=={'Version','Index','OwnerGroup','Generation','Digest'},'exact scope schema')
 need(s['Version']==1 and type(s['Version'])is int,'scope version1');u64(s['Generation'],'scope generation',True)
 for key in ('Index','OwnerGroup'):
  need(isinstance(s[key],str) and 0<len(s[key].encode())<=1024,'bounded UTF8 scope '+key)
 octets(s['Digest'],'scope digest')
 return gojson({k:s[k] for k in ('Version','Index','OwnerGroup','Generation','Digest')}).encode()

def outcome(o,physical):
 need(set(o)==set(NUMS)|{'ScopeDigest','CommandDigest'},'exact outcome schema')
 octets(o['ScopeDigest'],'outcome scope digest');octets(o['CommandDigest'],'outcome command digest')
 for k in NUMS:u64(o[k],k)
 need(all(o[k]>0 for k in ('Term','Index','Coverage','Ordinal')) and o['Ordinal']<=65536,'outcome authority/ordinal')
 need(o['Matched']<=1 and o['Affected']<=1 and (o['Affected']==0 or o['Revision']>0),'bounded original counts/revision')
 need((o['AppliedCommandLSN']>0) if physical else o['AppliedCommandLSN']==0,'physical witness/original token LSN')
 return struct.pack('<Q',1)+bytes(o['ScopeDigest'])+bytes(o['CommandDigest'])+b''.join(struct.pack('<Q',o[k]) for k in NUMS)

def token(z,req,deletion,ordinal):
 raw=b64(z['VisibilityToken'],'visibility token',8192);need(raw[:4]==b'CVM1','exact CVM1 token magic')
 t=c.strict(raw[4:]);need(set(t)=={'Scope','Attempt','Outcome'},'exact token schema')
 scope_bytes(t['Scope']);outcome(t['Outcome'],False)
 need(b64(t['Attempt'],'attempt',1024)==b64(req['IdempotencyKey'],'request attempt',1024),'token original attempt')
 s,o=t['Scope'],t['Outcome'];g=req['Generation']
 need(s['Index']==g['Index'] and s['Generation']==g['Generation'] and z['Generation']==g and s['OwnerGroup']==z['OwnerGroup']=='group-a','exact scope/generation/owner binding')
 need(o['ScopeDigest']==s['Digest'] and o['Ordinal']==ordinal,'scope digest and ordinal')
 for ok,zk in [('Term','CommitTerm'),('Index','CommitIndex'),('Coverage','Coverage'),('Revision','LiveRevision'),('Matched','Matched'),('Affected','Deleted' if deletion else 'Modified')]:
  need(o[ok]==z[zk],'token original outcome '+ok)
 need(z['ProductionConsensus'] is True and z['AppliedIndex']>=z['CommitIndex'],'actual original consensus/applied')
 need(z['Modified']==0 if deletion else z['Deleted']==0,'operation-kind original counts');c.mutation_counters(z)
 return t

def varint(n):
 u64(n,'uvarint');out=bytearray()
 while n>=128:out.append((n&127)|128);n>>=7
 out.append(n);return bytes(out)

def bytevector(v):return b'\x01'+varint(len(v))+v

def entry(collection,req,deletion,s,catalog):
 scope=scope_bytes(s);name=collection.encode();need(0<len(name)<=1024,'bounded collection')
 ids=b64(req['ID'],'exact ID',1024);ids.decode('utf-8');attempt=b64(req['IdempotencyKey'],'attempt',1024)
 sections={8:attempt,100:b'\x01'+name,102:bytevector(ids),105:varint(catalog),151:scope}
 if not deletion:
  sections.update({101:b'\x01',103:bytevector(b64(req['Document'],'document',65536)),106:b'\x01'})
 return b'TDC1'+b''.join(varint(v) for v in (1,32 if deletion else 31,1,0,len(sections)))+b''.join(varint(k)+varint(len(v))+v for k,v in sorted(sections.items()))

def command_digest(raw):
 # Exact raftentry.writeDigestField/writeDigestU64, including named BE lengths.
 def integer(name,v):return name.encode()+struct.pack('>Q',v)
 def field(name,v):return integer('field-name-len',len(name))+name.encode()+integer('field-value-len',len(v))+v
 framed=field('domain',b'TreeDB/R3a/CommandDigestV1\0')+integer('entry-version',1)
 for name,value in [('scope-rule',b'single-group-v1'),('database-scope',b'database/default'),('catalog-scope',b'catalog/default'),('nativewire-deterministic-entry',raw)]:framed+=field(name,value)
 return hashlib.sha256(framed).digest()

def chain(collection,witnesses,attempts):
 prefix='vector_colocated_outcome_v1:'+c.sha(collection.encode())+':'
 previous=bytes(32);retained=len((prefix+'usage').encode())+56
 for i,(w,attempt) in enumerate(zip(witnesses,attempts),1):
  o=w['Outcome'];raw=outcome(o,True);need(o['Ordinal']==i,'ordered/gap-free publication ordinal')
  ah=c.sha(attempt);need(w['AttemptSHA256']==ah,'exact witness attempt hash')
  key=(prefix+'attempt:'+ah).encode();retained+=len(key)+len(raw)
  record=hashlib.sha256(b'gomap:colocated-outcome-record:v1\0'+struct.pack('<Q',len(key))+key+raw[:-8]+bytes(8)).digest()
  previous=hashlib.sha256(b'gomap:colocated-outcome-chain:v1\0'+previous+record).digest()
 need(retained<=32<<20,'bounded collection retained metadata')
 return previous.hex(),retained

def field(raw,key,value):
 v,s=c.field(raw,key);need(v==value,'exact raw '+key);return s

def rawarray(raw,key,values):
 pairs=c.pieces(raw,key);need([v for v,s in pairs]==values,'exact retained '+key+' array');return pairs

def verify_payload(r,text,config,states):
 need(c.strict(text)==r,'exact retained report object')
 count=original_count(r);need(len(states)==count+1,'seven independently reconstructed populations supplied')
 init=config['VectorInitialization'];collection=init['Collection']['Collection'];epoch=init['CatalogEpoch']
 need(len(config['Nodes'])==4 and [n['ID'] for n in config['Nodes']]==list(NODES) and len(config['Groups'])==1 and config['Groups'][0]['ID']=='group-a','actual fixed RF4 config')
 writes=r['Writes'];need(len(writes)==count,'six original writes');pairs=rawarray(text,'Writes',writes)
 tokens=[];attempts=[];final={};planwrites=[];previous=0
 for i,(w,raw) in enumerate(pairs):
  need(w['Ordinal']==i and w['Outcome']=='succeeded' and w['Invoked'] is True and w['Error']==w['ErrorCode']=='','complete original attempt ledger')
  kind='Delete' if 'Delete' in w else 'Replace';need((kind=='Delete')==('Delete' in w) and ('Replace' in w)!=('Delete' in w),'one original request kind')
  req=w[kind];need(req['Deadline']=='0001-01-01T00:00:00Z','zero logical deadline')
  rawreq=field(raw,kind,req);rawresponse=field(raw,'Response',w['Response'])
  need(w['LogicalSHA256']==c.sha(rawreq.encode()),'original exact logical request SHA')
  wire=c.request_wire(rawreq,w['Deadline']);need(w['RequestSHA256']==c.sha(wire.encode()),'actual original serialized deadline request SHA')
  need(w['ResponseSHA256']==c.sha(rawresponse.encode()) and w['ResponseBytes']==len(rawresponse.encode()),'original retained response SHA/bytes')
  z=w['Response'];need(previous<z['CommitIndex'] and z['AppliedIndex']<=r['RequiredAppliedIndex'],'ordered original commit/floor');previous=z['CommitIndex']
  t=token(z,req,kind=='Delete',i+1);tokens.append(t);attempts.append(b64(req['IdempotencyKey'],'attempt',1024))
  rawcmd=entry(collection,req,kind=='Delete',t['Scope'],t['Outcome']['ExpectedCatalogVersion'])
  need(command_digest(rawcmd)==bytes(t['Outcome']['CommandDigest']),'independent actual deterministic command-entry digest')
  id=b64(req['ID'],'ID',1024);final[id]={'ID':req['ID'],'Document':req.get('Document'),'Absent':kind=='Delete'}
  planwrites.append('{'+gojson(kind)+':'+rawreq+',"Response":'+rawresponse+'}')
 need(len(set(attempts))==count and all(t['Scope']==tokens[0]['Scope'] for t in tokens),'six unique attempts/exact shared scope')
 need(tokens[0]['Scope']['Generation']==init['Generation'] and tokens[0]['Scope']['Index']==init['IndexDefinition']['name'],'actual configured generation/index binding')
 need(previous==r['HighestNewCommitIndex'] and r['RequiredAppliedIndex']>=previous,'highest new original/applied floor')
 finals=[final[id] for id in sorted(final)];need(len(finals)==4 and sum(not f['Absent'] for f in finals)==1,'four final IDs/one present')
 plan={'Version':1,'RunID':RUN,'HighestNewCommitIndex':previous,'RequiredAppliedIndex':r['RequiredAppliedIndex'],'Writes':[{('Delete' if 'Delete' in w else 'Replace'):w.get('Delete',w.get('Replace')),'Response':w['Response']} for w in writes],'Final':finals}
 planraw='{"Version":1,"RunID":'+gojson(RUN)+',"HighestNewCommitIndex":'+str(previous)+',"RequiredAppliedIndex":'+str(r['RequiredAppliedIndex'])+',"Writes":['+','.join(planwrites)+'],"Final":'+gojson(finals)+'}'
 need(len(planraw.encode())<=512<<10 and r['AuditPlan']==plan and field(text,'AuditPlan',plan)==planraw,'exact Go raw six-write audit plan')
 for f in finals:
  id=b64(f['ID'],'final ID',1024).decode();need((id not in states[-1])==f['Absent'],'final reconstructed population presence')
  if not f['Absent']:
   doc=c.strict(b64(f['Document'],'final document'));need(c.vecbits(doc['embedding'])==c.vecbits(states[-1][id]),'final canonical FP32 vector ledger binding')
 retries=r['Retries'];need(len(retries)==2 and [w['Ordinal'] for w in retries]==[0,2],'two original-attempt retries')
 for w in retries:
  original=writes[w['Ordinal']];kind='Delete' if 'Delete' in original else 'Replace'
  need(w['Outcome']=='succeeded' and w['Invoked'] is True and w['Error']==w['ErrorCode']=='' and w[kind]==original[kind],'exact retained original request retry')
  c.outcome_same(original['Response'],w['Response'])
 need(r['RequiredAppliedIndex']==max(w['Response']['AppliedIndex'] for w in writes+retries),'highest observed original/retry applied floor')
 history=r['PostRecall']['ReadinessAfter'];need(len(history)>=4 and len(history)%4==0,'retained complete final readiness round')
 last=history[-4:];need([h['RequestedNode'] for h in last]==list(NODES),'ordered four-voter final readiness round')
 for h in last:
  v=h['State'];need(h['Error']==h['ErrorCode']==v.get('Error','')=='' and v['NodeID']==h['RequestedNode'] and v['Live'] is True and v['Ready'] is True and v['Draining'] is False and v['VectorPhase']=='active' and v['CatalogEpoch']==epoch,'actual final ACTIVE readiness identity')
  need(len(v['Groups'])==1 and v['Groups'][0]['GroupID']=='group-a' and v['Groups'][0]['Ready'] is True and v['Groups'][0].get('Error','')=='' and v['Groups'][0]['LocalAppliedIndex']>=r['RequiredAppliedIndex'],'actual final readiness applied floor')
 audits=r['Audits'];need(len(audits)==4,'four actual retained attachments');auditpairs=rawarray(text,'Audits',audits)
 auditnodes=[];proofs=[];logical=[]
 for d,draw in auditpairs:
  v=d['ColocatedAudit'];node=v['NodeID'];auditnodes.append(node)
  need(v['Version']==1 and v['RunID']==RUN and v['PlanSHA256']==c.sha(planraw.encode()) and v['OwnerGroup']==tokens[0]['Scope']['OwnerGroup'] and v['Scope']==(SCOPE_LABEL if count==6 else 'all declared retained original outcomes and known-ID canonical source/absence plus exact live membership only; no entire population proof'),'exact bounded audit plan/scope identity')
  u64(v['AppliedTerm'],'audit applied term',True);u64(v['AppliedIndex'],'audit applied index',True)
  need(v['AppliedIndex']>=r['RequiredAppliedIndex'],'audit applied floor')
  state=v['PhysicalState'];need(isinstance(state,dict) and set(state)=={'CommitSeq','RootPageID','SystemRootPageID','AppliedCommandLSN','MaxEntryRevision','LeafGenerationStateVersion'} and all(type(n)is int and 0<=n<1<<64 for n in state.values()) and state['RootPageID']>0 and state['SystemRootPageID']>0,'exact retained six-scalar physical StateToken/source and SystemRoot')
  lsn=u64(state['AppliedCommandLSN'],'covered command LSN',True);need(v['CommandWALNextLSN']==lsn+1 and lsn<(1<<64)-1,'exact physical WAL next/coverage')
  ws=v['Witnesses'];need(len(ws)==count and v['RetainedCount']==count,'six exact local witnesses')
  previouslsn=0
  for i,(w,t) in enumerate(zip(ws,tokens)):
   o=w['Outcome'];outcome(o,True);cmp=copy.deepcopy(o);cmp['AppliedCommandLSN']=0
   need(cmp==t['Outcome'],'local covered original outcome equals original token')
   need(previouslsn<o['AppliedCommandLSN']<=lsn and o['Index']<=v['AppliedIndex'] and o['Term']<=v['AppliedTerm'],'ordered physically covered witness');previouslsn=o['AppliedCommandLSN']
  ch,nb=chain(collection,ws,attempts);need(v['RetainedChain']==ch and v['RetainedBytes']==nb,'independent exact logical chain/retained bytes (LSN excluded)');logical.append((ch,nb))
  status=d['Status'];need(status['NodeID']==node and status['VectorPhase']=='active' and status['Catalog']['Epoch']==epoch and len(status['Groups'])==1,'local diagnostics identity/ACTIVE/catalog')
  group=status['Groups'][0];need(group['GroupID']=='group-a' and group['NodeID']==node and group['Applied']['Index']>=r['RequiredAppliedIndex'],'local diagnostics applied-floor group identity')
  fs=v['Final'];need(len(fs)==4,'four final local exact-ID attestations')
  for f,want in zip(fs,finals):
   need(f['ID']==want['ID'] and f['Absent'] is want['Absent'],'ordered exact final ID/absence')
   expected='' if want['Absent'] else c.sha(b64(want['Document'],'final document'))
   need(f.get('ExpectedDocumentSHA256','')==expected,'final exact expected-document digest')
   live=f['Live'];need(set(live)=={'Generation','Revision','Coverage','MutatedIDs','LiveIDs','Cutovers'} and all(type(n)is int and n>=0 for n in live.values()),'exact bounded live-status schema')
   need(live['Generation']==tokens[0]['Scope']['Generation'] and live['Revision']==writes[-1]['Response']['LiveRevision'] and live['Coverage']==writes[-1]['Response']['Coverage'],'final local live generation/revision/coverage')
  need(all(f['Live']==fs[0]['Live'] for f in fs),'stable local live status across final proofs');proofs.append(fs)
 need(auditnodes==list(NODES) and len(set(logical))==1 and all(fs==proofs[0] for fs in proofs),'ordered four-voter logical summary/final proof agreement')
 return {'status':'AUDIT_ATTACHMENT_ACCOUNTING_VERIFIED_PENDING_OUTER_AUTHORITY','plan_sha256':c.sha(planraw.encode()),'retained_chain':logical[0][0],'retained_bytes':logical[0][1],'audits_sha256':[c.sha(raw.encode()) for d,raw in auditpairs],'scope_preimage_independently_recomputed':False,'command_digests_independently_recomputed':True,'replica_local_lsn_excluded_from_chain':True,'limitations':LIMITATIONS}

def original_count(report):
 n=report.get('Originals',0)
 need(type(n) is int and (n==0 or 6<=n<=63),'declared original count')
 return n or 6

def verifier(a,p,r,ptext,text,states):
 # a is root-approved manifest; this gate rechecks actual config pin only.
 raw=c.bounded(a['receipts']['config'],1<<20)
 need(c.sha(raw)==a['config_sha256']==a['local_pins'][a['receipts']['config']],'actual approved config bytes/hash')
 need(c.strict(ptext)==p,'exact supplied planned report bytes')
 planned=rawarray(ptext,'Writes',p['Writes']);actual=rawarray(text,'Writes',r['Writes'])
 need(len(planned)==len(actual)==original_count(r) and original_count(p)==original_count(r),'complete planned/actual six-slot ledger')
 for (want,wraw),(got,graw) in zip(planned,actual):
  key='Delete' if 'Delete' in want else 'Replace'
  need(want['Ordinal']==got['Ordinal'] and want['Kind']==got['Kind'] and want[key]==got[key] and want['LogicalSHA256']==got['LogicalSHA256'] and field(wraw,key,want[key])==field(graw,key,got[key]),'exact planned original request/ordinal/raw-byte binding')
 return verify_payload(r,text,c.strict(raw),states)

def self_check():
 checks=[]
 def yes(label,fn):fn();checks.append({'name':label,'outcome':'PASS'})
 def no(label,fn):
  try:fn()
  except (AssertionError,ValueError,KeyError,TypeError,UnicodeError):checks.append({'name':label,'outcome':'PASS_REJECTED'});return
  raise AssertionError('tamper admitted: '+label)
 enc=lambda v:base64.b64encode(v).decode()
 s={'Version':1,'Index':'embedding','OwnerGroup':'group-a','Generation':7,'Digest':[1]+[0]*31}
 golden_scope={'Version':1,'Index':'embedding','OwnerGroup':'owner','Generation':7,'Digest':[1]+[0]*31}
 req={'ID':enc(b'id'),'IdempotencyKey':enc(b'colocated/attempt'),'Document':enc(b'{"embedding":[1,0]}')}
 # Frozen nativewire fixtures, not synthetic expected encoder outputs.
 GOLDEN_REPLACE='54444331011f0100080811636f6c6f63617465642f617474656d7074640501646f6373650101660401026964671501137b22656d62656464696e67223a5b312c305d7d6901076a0101970190017b2256657273696f6e223a312c22496e646578223a22656d62656464696e67222c224f776e657247726f7570223a226f776e6572222c2247656e65726174696f6e223a372c22446967657374223a5b312c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c305d7d'
 GOLDEN_DELETE='5444433101200100050811636f6c6f63617465642f617474656d7074640501646f6373660401026964690107970190017b2256657273696f6e223a312c22496e646578223a22656d62656464696e67222c224f776e657247726f7570223a226f776e6572222c2247656e65726174696f6e223a372c22446967657374223a5b312c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c302c305d7d'
 yes('replace deterministic existing Go golden',lambda:need(entry('docs',req,False,golden_scope,7).hex()==GOLDEN_REPLACE,'golden replace'))
 yes('delete deterministic existing Go golden',lambda:need(entry('docs',req,True,golden_scope,7).hex()==GOLDEN_DELETE,'golden delete'))
 # Transparent synthetic accounting fixture; NOT runtime evidence.
 writes=[];states=[{} for _ in range(7)]
 for i,(id,delete) in enumerate([('A',False),('A',False),('B',True),('C',False),('C',True),('D',True)]):
  q={'Version':1,'Generation':{'Index':'embedding','Generation':7},'IdempotencyKey':enc(('attempt%d'%i).encode()),'ID':enc(id.encode())}
  if not delete:q.update(Vector=[0.5,0.5],Document=enc(('{"embedding":[0.5,0.5],"kind":"%d"}'%i).encode()))
  q['Deadline']='0001-01-01T00:00:00Z';key='Delete' if delete else 'Replace'
  o={'ScopeDigest':s['Digest'],'CommandDigest':list(command_digest(entry('docs',q,delete,s,10+i))),'ExpectedCatalogVersion':10+i,'Term':2,'Index':20+i,'Coverage':30+i,'Revision':i+1,'Matched':0 if delete else 1,'Affected':1,'Ordinal':i+1,'AppliedCommandLSN':0}
  t={'Scope':s,'Attempt':q['IdempotencyKey'],'Outcome':o}
  z={'VisibilityToken':enc(b'CVM1'+gojson(t).encode()),'Generation':q['Generation'],'OwnerGroup':'group-a','CommitTerm':2,'CommitIndex':20+i,'AppliedIndex':25,'ProductionConsensus':True,'Coverage':30+i,'LiveRevision':i+1,'Matched':o['Matched'],'Modified':0 if delete else 1,'Deleted':1 if delete else 0,'Counters':{'Routes':1,'Commits':1,'Replications':1,'Applies':1,'VisibilityProofs':1,'Forwards':0}}
  deadline='2026-10-04T10:00:00Z';qr=gojson(q);zr=gojson(z)
  writes.append({'Ordinal':i,'Kind':'delete' if delete else 'replace','Phase':'mixed-measured','LogicalSHA256':c.sha(qr.encode()),'RequestSHA256':c.sha(c.request_wire(qr,deadline).encode()),'ResponseSHA256':c.sha(zr.encode()),'Outcome':'succeeded','ErrorCode':'','Error':'',key:q,'Response':z,'Invoked':True,'Deadline':deadline,'ResponseBytes':len(zr.encode())})
 final=[{'ID':enc(id.encode()),'Document':writes[1]['Replace']['Document'] if id=='A' else None,'Absent':id!='A'} for id in 'ABCD'];states[-1]={'A':[0.5,0.5]}
 plan={'Version':1,'RunID':RUN,'HighestNewCommitIndex':25,'RequiredAppliedIndex':25,'Writes':[{('Delete' if 'Delete'in w else 'Replace'):w.get('Delete',w.get('Replace')),'Response':w['Response']} for w in writes],'Final':final}
 config={'VectorInitialization':{'Collection':{'Collection':'docs'},'CatalogEpoch':9,'Generation':7,'IndexDefinition':{'name':'embedding'}},'Nodes':[{'ID':n} for n in NODES],'Groups':[{'ID':'group-a'}]}
 audits=[]
 for v,node in enumerate(NODES):
  ws=[]
  for w in writes:
   t=c.strict(b64(w['Response']['VisibilityToken'],'token')[4:]);o=copy.deepcopy(t['Outcome']);o['AppliedCommandLSN']=100+v*100+o['Ordinal'];ws.append({'AttemptSHA256':c.sha(b64(t['Attempt'],'attempt')),'Outcome':o})
  ch,nb=chain('docs',ws,[b64(w.get('Replace',w.get('Delete'))['IdempotencyKey'],'attempt') for w in writes])
  live={'Generation':7,'Revision':6,'Coverage':35,'MutatedIDs':4,'LiveIDs':0,'Cutovers':0}
  fs=[dict(ID=f['ID'],Absent=f['Absent'],Live=live,**({} if f['Absent'] else {'ExpectedDocumentSHA256':c.sha(b64(f['Document'],'doc'))})) for f in final]
  audits.append({'Status':{'NodeID':node,'VectorPhase':'active','Catalog':{'Epoch':9},'Groups':[{'NodeID':node,'GroupID':'group-a','Applied':{'Index':25,'Term':2}}]},'ColocatedAudit':{'Version':1,'RunID':RUN,'PlanSHA256':c.sha(gojson(plan).encode()),'NodeID':node,'OwnerGroup':'group-a','Scope':SCOPE_LABEL,'AppliedTerm':2,'AppliedIndex':25,'PhysicalState':{'AppliedCommandLSN':106+v*100,'RootPageID':8,'SystemRootPageID':9,'CommitSeq':1,'MaxEntryRevision':0,'LeafGenerationStateVersion':0},'CommandWALNextLSN':107+v*100,'RetainedCount':6,'RetainedBytes':nb,'RetainedChain':ch,'Witnesses':ws,'Final':fs}})
 ready=[{'RequestedNode':n,'Error':'','ErrorCode':'','State':{'NodeID':n,'Live':True,'Ready':True,'Draining':False,'VectorPhase':'active','CatalogEpoch':9,'Groups':[{'GroupID':'group-a','Ready':True,'LocalAppliedIndex':25}]}} for n in NODES]
 r={'Writes':writes,'Retries':[copy.deepcopy(writes[0]),copy.deepcopy(writes[2])],'AuditPlan':plan,'Audits':audits,'HighestNewCommitIndex':25,'RequiredAppliedIndex':25,'PostRecall':{'ReadinessAfter':ready}}
 verify=lambda z:verify_payload(z,gojson(z),config,states)
 yes('synthetic four-voter complete accounting with distinct physical LSN',lambda:verify(r))
 no('reject wrong configured lowercase index name',lambda:verify_payload(r,gojson(r),dict(config,VectorInitialization=dict(config['VectorInitialization'],IndexDefinition={'name':'other'})),states))
 no('reject invented uppercase configured index Name',lambda:verify_payload(r,gojson(r),dict(config,VectorInitialization=dict(config['VectorInitialization'],IndexDefinition={'Name':'embedding'})),states))
 def tamper(label,fn):
  z=copy.deepcopy(r);fn(z);no(label,lambda:verify(z))
 tamper('missing audit',lambda z:z['Audits'].pop())
 tamper('duplicate voter',lambda z:z['Audits'][1]['ColocatedAudit'].update(NodeID='node-a'))
 tamper('wrong audit plan',lambda z:z['Audits'][0]['ColocatedAudit'].update(PlanSHA256='1'*64))
 tamper('wrong owner',lambda z:z['Audits'][0]['ColocatedAudit'].update(OwnerGroup='other'))
 tamper('uncovered local witness',lambda z:z['Audits'][0]['ColocatedAudit']['Witnesses'][0]['Outcome'].update(AppliedCommandLSN=1000))
 tamper('zero physical witness',lambda z:z['Audits'][0]['ColocatedAudit']['Witnesses'][0]['Outcome'].update(AppliedCommandLSN=0))
 tamper('missing physical root field',lambda z:z['Audits'][0]['ColocatedAudit']['PhysicalState'].pop('SystemRootPageID'))
 tamper('zero source root',lambda z:z['Audits'][0]['ColocatedAudit']['PhysicalState'].update(RootPageID=0))
 tamper('WAL coverage gap',lambda z:z['Audits'][0]['ColocatedAudit'].update(CommandWALNextLSN=999))
 tamper('ordinal gap',lambda z:z['Audits'][0]['ColocatedAudit']['Witnesses'][3]['Outcome'].update(Ordinal=6))
 tamper('changed original outcome',lambda z:z['Audits'][0]['ColocatedAudit']['Witnesses'][0]['Outcome'].update(Revision=999))
 tamper('retained chain mismatch',lambda z:z['Audits'][0]['ColocatedAudit'].update(RetainedChain='1'*64))
 tamper('retained bytes mismatch',lambda z:z['Audits'][0]['ColocatedAudit'].update(RetainedBytes=1))
 tamper('wrong local applied floor',lambda z:z['Audits'][0]['ColocatedAudit'].update(AppliedIndex=24))
 tamper('final present becomes absent',lambda z:z['Audits'][0]['ColocatedAudit']['Final'][0].update(Absent=True))
 tamper('final document mismatch',lambda z:z['Audits'][0]['ColocatedAudit']['Final'][0].update(ExpectedDocumentSHA256='1'*64))
 tamper('wrong final generation',lambda z:z['Audits'][0]['ColocatedAudit']['Final'][0]['Live'].update(Generation=8))
 tamper('stale final live coverage',lambda z:z['Audits'][0]['ColocatedAudit']['Final'][0]['Live'].update(Coverage=34))
 tamper('wrong final readiness catalog',lambda z:z['PostRecall']['ReadinessAfter'][0]['State'].update(CatalogEpoch=8))
 tamper('wrong retry original ordinal',lambda z:z['Retries'][1].update(Ordinal=1))
 tamper('retry original token changed',lambda z:z['Retries'][0]['Response'].update(VisibilityToken=enc(b'wrong')))
 tamper('incomplete original',lambda z:z['Writes'][0].update(Outcome='UNKNOWN'))
 no('missing raw report',lambda:verify_payload(r,'{}',config,states))
 no('missing reconstructed population',lambda:verify_payload(r,gojson(r),config,[]))
 no('unknown scope authority field',lambda:scope_bytes(dict(s,Term=99)))
 no('wrong scope version',lambda:scope_bytes(dict(s,Version=2)))
 no('zero scope digest',lambda:scope_bytes(dict(s,Digest=[0]*32)))
 no('malformed token magic',lambda:token(dict(writes[0]['Response'],VisibilityToken=enc(b'BAD1'+gojson({'Scope':s}).encode())),writes[0]['Replace'],False,1))
 no('token unknown field',lambda:token(dict(writes[0]['Response'],VisibilityToken=enc(b'CVM1'+gojson(dict(c.strict(b64(writes[0]['Response']['VisibilityToken'],'token')[4:]),Extra=1)).encode())),writes[0]['Replace'],False,1))
 # Independent chain property: vary covered physical LSN only, preserve logical chain.
 ws=copy.deepcopy(audits[0]['ColocatedAudit']['Witnesses']);at=[b64(w.get('Replace',w.get('Delete'))['IdempotencyKey'],'attempt') for w in writes]
 before=chain('docs',ws,at)
 for w in ws:w['Outcome']['AppliedCommandLSN']+=500
 yes('replica LSN excluded exactly',lambda:need(chain('docs',ws,at)==before,'LSN-independent chain'))
 ws[0]['Outcome']['Revision']+=1
 yes('logical outcome changes chain',lambda:need(chain('docs',ws,at)!=before,'logical chain sensitivity'))
 yes('Go HTML/special escape',lambda:need(gojson('<>&\u2028\u2029')=='"\\u003c\\u003e\\u0026\\u2028\\u2029"','Go escapes'))
 original=command_digest(entry('docs',req,False,golden_scope,7))
 for label,changed,sc,cv in [
  ('changed exact document',dict(req,Document=enc(b'{"embedding":[1,0],"kind":"changed"}')),golden_scope,7),
  ('changed exact attempt',dict(req,IdempotencyKey=enc(b'other')),golden_scope,7),
  ('changed exact scope',req,dict(golden_scope,Generation=8),7),
  ('changed expected catalog',req,golden_scope,8)]:
  yes(label+' changes command digest',lambda changed=changed,sc=sc,cv=cv:need(command_digest(entry('docs',changed,False,sc,cv))!=original,'exact command identity sensitivity'))
 z=copy.deepcopy(r);z['Retries'][0]['Response']['Counters']['Forwards']=1
 yes('legitimate per-call retry forward can vary',lambda:verify(z))
 return {'status':'PURE_SELF_CHECK_PASS','checks':checks,'check_count':len(checks),'actual_runtime_evidence_validated':False,'limitations':LIMITATIONS}

if __name__=='__main__':
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--self-check',action='store_true');args=parser.parse_args()
 if not args.self_check:parser.error('only pure --self-check CLI; import verifier for root composition')
 print(json.dumps(self_check(),indent=2))
