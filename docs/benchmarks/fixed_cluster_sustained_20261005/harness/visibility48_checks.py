"""Complete actual adapted read-consumer fixtures; synthetic and source-only."""
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
def visibility_fixture(consumer,originals):
 source=(R/'shared_read.py').read_text()
 node=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef) and n.name=='self_check')
 body=[]
 for statement in node.body:
  if isinstance(statement,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='tests' for t in statement.targets):break
  body.append(statement)
 text=ast.unparse(ast.Module(body=body,type_ignores=[]))
 # Adapt only the retained fixture builder, never the verifier being tested.
 substitutions={"c.causal_range(":"causal_range(","range(7)":"range(originals+1)","range(6)":"range(originals)","account([], 6)":"account([], originals)","account(r['Writes'], 6)":"account(r['Writes'], originals)","[20000000] * 6":"[20000000] * originals","'doc%02d'":"'doc-%06d'","5000000000":"6000000000","HighestNewCommitIndex=16":"HighestNewCommitIndex=10+originals","RequiredAppliedIndex=16":"RequiredAppliedIndex=10+originals","AppliedIndex=16":"AppliedIndex=10+originals","ready(0, 16)":"ready(0, 10+originals)","utc(61000000000)":"utc(DURATION+1000000000)","16 / 60":"16 / (DURATION/1e9)","('PostRecall', 'quiescent-after-mixed', 16)":"('PostRecall', 'quiescent-after-mixed', 10+originals)","ACCEPT_MIXED_INVARIANT_RECALL_WINDOW_OBSERVATION_PENDING_ROOT_SHUTDOWN_VERIFICATION":"ACCEPT_MIXED_CHANGING_TOP10_RECALL_WINDOW_OBSERVATION_PENDING_ROOT_SHUTDOWN_VERIFICATION"}
 for before,after in substitutions.items():
  assert before in text,('fixture builder seam',before)
  text=text.replace(before,after)
 ns=dict(consumer.__dict__);ns.update(originals=originals)
 exec(compile(text,'retained-fixture-construction-only','exec'),ns)
 p,r,states=ns['p'],ns['r'],ns['states']
 for report in (p,r):report['Originals']=originals
 changed=[dict(ID='synthetic-absent-%d'%i,Present=False,ScoreBits=0) for i in range(4)]
 for report in (p,r):
  for i,prefix in enumerate(report['Prefixes']):prefix.update(Prefix=i,Changed=[copy.deepcopy(changed) for _ in range(16)])
 for claim in r['ReadPrefixes']:
  lo,hi=claim['Lower'],claim['Upper']
  claim.update(CompatibleMask=sum(1<<i for i in range(lo,hi+1)),RecallAt10=1.0)
 ns['ptext']=ns['encode'](p)
 r['PlannedEventBytes']=len(('{"Event":"planned","Report":'+ns['ptext']+'}\n').encode())
 def run(candidate=None):
  value=r if candidate is None else candidate
  old=consumer.c.bounded
  try:
   consumer.c.bounded=lambda path,cap=consumer.CAP:ns['cfg'] if path==ns['configpath'] else (_ for _ in ()).throw(AssertionError('only synthetic config read'))
   return consumer.verifier(ns['a'],p,value,ns['ptext'],ns['encode'](value),states,.9)
  finally:consumer.c.bounded=old
 return ns,run

def visibility_controls():
 receipts=[]
 for count in (6,48):
  ns,run=visibility_fixture(ra.rd,count)
  proof=run();assert len(proof['visibility_wire_response_hash_pairs'])==count
  checks.append('visibility_actual_adapted_complete_%d_final_slot_pass'%count)
  folder=fixtures/('visibility-actual-consumer-%d'%count);folder.mkdir()
  for name,value in [('planned',ns['p']),('result',ns['r'])]:(folder/(name+'.json')).write_text(ns['encode'](value))
  receipts.append(dict(originals=count,final_slot=count-1,proof=proof))
  if count==48:
   for slot in (0,5,46):
    z=copy.deepcopy(ns['r']);next_start=z['Writes'][slot+1]['StartNS']
    z['VisibilityOriginUTC'][slot]=ns['utc'](next_start-500000)
    z['Visibility'][slot]['SearchRequest']['Deadline']=ns['utc'](next_start-500000+ra.rd.RPC)
    z['Visibility'][slot]['RequestSHA256']=sha(ns['encode'](z['Visibility'][slot]['SearchRequest']).encode())
    label='visibility_actual_adapted_48_slot%d_next_mutation_crossing'%slot
    try:run(z)
    except AssertionError as error:assert str(error)=='post-ACK probe completes before next mutation/drain'
    else:raise AssertionError('accepted '+label)
    checks.append(label)
    (folder/('rejected-slot%d.json'%slot)).write_text(ns['encode'](z))
 (fixtures/'visibility48-consumer-integration.json').write_text(json.dumps({'state':'SYNTHETIC_COMPLETE_ACTUAL_ADAPTED_READ_CONSUMER_ONLY','consumers':receipts,'source_sha256':sha((R/'shared_read.py').read_bytes()),'adapted_verifier_sha256':ra.DERIVATION['read_verifier']['adapted_sha256'],'runtime_started':False,'network_calls':0,'limitations':['Synthetic unchanged population/native-table rows test accounting and visibility boundaries; no native ranking or campaign acceptance.','Six-write positive preserves legacy count compatibility in the actual Trial24 adapter; campaign qualification still requires48.']},indent=2)+'\n')
