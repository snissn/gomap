"""Inert contracts only: never invokes main(), Go, network or a benchmark."""
import ast,copy,hashlib,json,pathlib,tempfile,unittest,time
from unittest.mock import patch
import hosted,science
P=pathlib.Path(__file__).resolve().parent

def authority():
 e=dict(GITHUB_ACTIONS='true',GITHUB_REPOSITORY='snissn/gomap',GITHUB_EVENT_NAME='pull_request',GITHUB_RUN_NUMBER='1',GITHUB_RUN_ATTEMPT='1',RUNNER_ENVIRONMENT='github-hosted',RUNNER_OS='Linux',RUNNER_ARCH='X64',GITHUB_ACTOR='snissn',GITHUB_RUN_ID='123456',GITHUB_SHA='a'*40,GITHUB_WORKFLOW_SHA='a'*40,ImageOS='ubuntu24')
 auth=dict(head_sha='b'*40,base_sha='c'*40,review_sha256='d'*64,expires_epoch=int(time.time()+3600),nonce=hosted.NONCE,authorization='AUTHORIZE_ONE_REVIEWED_HOSTED_DIAGNOSTIC')
 event={'action':'opened','sender':{'login':'snissn'},'repository':{'full_name':'snissn/gomap'},'pull_request':{'draft':True,'number':99999,'user':{'login':'snissn'},'head':{'sha':'b'*40,'ref':'codex/3992-hosted-engineering-successor-20261029','repo':{'full_name':'snissn/gomap'}},'base':{'sha':'c'*40,'repo':{'full_name':'snissn/gomap'}},'body':'<!-- GOMAP3992-AUTHORITY\n'+json.dumps(auth)+'\n-->'}}
 return e,event

def raw(mode='raw'):
 r=dict(n=128,t_ns=128000000,mem_bytes=12801,mem_allocs=2561,mode=mode,activation_count=0,calibration=None,timed=None)
 if mode!='raw':
  rec=dict(operation_ns=900000,requested_ns=72000 if mode=='positive' else 0,realized_ns=72001 if mode=='positive' else 0)
  r.update(activation_count=129,calibration=rec,timed=[rec.copy() for _ in range(128)])
 return r
def text(r):return 'BenchmarkCommitAtPrimaryOnlyAfterDataGeneration 128 1000000 ns/op 100 B/op 20 allocs/op 0 generation_advances/op\nDETECTOR_RAW_SAMPLE '+json.dumps(r)+'\nPASS\n'

class Contracts(unittest.TestCase):
 def test_authority(self):
  e,event=authority();hosted.authorize(e,event)
  for key in e:
   bad=e.copy();bad[key]='wrong'
   with self.subTest(key=key),self.assertRaises((ValueError,KeyError)):hosted.authorize(bad,event)
  for action in ['synchronize','edited','reopened','ready_for_review']:
   bad=copy.deepcopy(event);bad['action']=action
   with self.assertRaises(ValueError):hosted.authorize(e,bad)
  for attr in ['GITHUB_RUN_NUMBER','GITHUB_RUN_ATTEMPT']:
   bad=e.copy();bad[attr]='2'
   with self.assertRaises(ValueError):hosted.authorize(bad,event)
 def test_disarmed_by_default(self):
  e,event=authority()
  with self.assertRaises(ValueError):hosted.authorize({},event)
  for body in ['',event['pull_request']['body']*2,event['pull_request']['body'].replace('AUTHORIZE_ONE_REVIEWED_HOSTED_DIAGNOSTIC','NO')]:
   bad=copy.deepcopy(event);bad['pull_request']['body']=body
   with self.assertRaises(ValueError):hosted.authorize(e,bad)
 def test_successor_rejects_original_authority(self):
  e,event=authority()
  bad=copy.deepcopy(event);bad['pull_request']['head']['ref']='codex/3992-hosted-detector-once-20261029'
  with self.assertRaises(ValueError):hosted.authorize(e,bad)
  bad=copy.deepcopy(event);bad['pull_request']['body']=bad['pull_request']['body'].replace(hosted.NONCE,'3992-hosted-resolution-once-20261029-v1')
  with self.assertRaises(ValueError):hosted.authorize(e,bad)
 def test_successor_workflow_contract(self):
  workflow=P.parents[2]/'.github/workflows/3992-hosted-detector-successor.yml'
  s=workflow.read_text()
  self.assertNotIn('workflow_dispatch:',s);self.assertNotIn('push:',s)
  self.assertIn('types: [opened]',s)
  job,steps=s.split('    steps:',1)
  self.assertNotIn('runner.temp',job)
  self.assertEqual(steps.count('${{ runner.temp }}'),1)
  self.assertIn('"$GITHUB_ENV"',steps)
  self.assertIn("github.run_number == '1' && github.run_attempt == '1'",job)
  self.assertIn('codex/3992-hosted-engineering-successor-20261029',job)
  self.assertIn("workflow='.github/workflows/3992-hosted-detector-successor.yml'",(P/'hosted.py').read_text())
 def test_first_sample_pending_precedes_spawn(self):
  tree=ast.parse((P/'hosted.py').read_text())
  main=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main')
  loop=next(n for n in ast.walk(main) if isinstance(n,ast.For) and isinstance(n.iter,ast.Call) and isinstance(n.iter.func,ast.Name) and n.iter.func.id=='schedule')
  pending=next(i for i,n in enumerate(loop.body) if 'trial-start-pending.json' in ast.unparse(n))
  spawn=next(i for i,n in enumerate(loop.body) if "owner('sample-" in ast.unparse(n))
  self.assertLess(pending,spawn)
 def test_schedule(self):
  s=science.schedule();self.assertEqual(len(s),160)
  for arm in science.ARMS:
   for block in range(2):
    r=[v for v in s if v['arm']==arm and v['block']==block]
    self.assertEqual(len(r),16);self.assertEqual([x['revision'] for x in r[::2]].count('baseline'),4)
  for arm in ['AA','CC']:
   self.assertEqual(len({(x['binary'],x['mode']) for x in s if x['arm']==arm}),1)
 def test_exact_raw(self):
  for mode in ['raw','off','positive']:science.parse_raw(text(raw(mode)),mode)
  for key,bad in [('n',127),('t_ns',0),('mem_bytes',12928),('activation_count',1)]:
   r=raw();r[key]=bad
   with self.assertRaises(ValueError):science.parse_raw(text(r),'raw')
 def test_impossible_ledgers(self):
  for key,bad in [('operation_ns',1100000),('realized_ns',10000000),('requested_ns',0)]:
   r=raw('positive');r['timed'][0][key]=bad
   with self.assertRaises(ValueError):science.parse_raw(text(r),'positive')
  r=raw('positive')
  for x in r['timed']:x['operation_ns']=1100000;x['requested_ns']=88000;x['realized_ns']=88000
  with self.assertRaises(ValueError):science.parse_raw(text(r),'positive')
 def test_no_missing_or_skipped_lifecycle(self):
  n=science.gate.LIFECYCLE;good='--- PASS: '+n+' (0.1s)\nPASS\n'
  science.verify_test(good,n)
  for bad in ['', 'PASS\n',good.replace('PASS:','SKIP:'),good.replace(n,'Wrong'),good+'panic: failure\n']:
   with self.assertRaises(ValueError):science.verify_test(bad,n)
  with self.assertRaises(ValueError):science.verify_discovery('',[n])
 def test_unresolved_and_red_retained(self):
  rows=[dict(item,metrics={'ns/op':1000000.,'B/op':100.,'allocs/op':20.,'generation_advances/op':0.},raw=raw()) for item in science.schedule()]
  result=science.analyze(rows);self.assertEqual(result['status'],'DETECTOR_RESOLUTION_UNRESOLVED');self.assertFalse(result['product_acceptance'])
  for r in rows:
   if r['revision']=='candidate':r['metrics'].update({'ns/op':1083000.,'B/op':101.})
  result=science.analyze(rows)
  for v in result['blocks'].values():
   for b in v:self.assertFalse(b['gates']['timing']);self.assertFalse(b['gates']['bytes'])
  with self.assertRaises(ValueError):science.analyze(rows[:-1])
 def test_exact_reused_sources(self):
  self.assertEqual(hosted.sha(P/'frozen/mvcc_primary_cost_gate.py'),'b57bc93ce46900357052b07a796158eaef71d931bcfb59990992cb97b4f706a3')
  self.assertEqual(hosted.sha(P/'frozen/primary_generation_routing_v6_test.go'),'2642bedc0ed6edbea8d7a599c66642c1d2107d93f1faf03355fb8cb071c56ab9')
  d=json.loads((P/'REUSE.json').read_text())['exact_copied_files_except_ast_extracted_science']
  for n in ['supervisor.py','leader_exec.py','detector_test.go','guard_regression_test.go','instrumented_mvcc.go']:self.assertEqual(hosted.sha(P/n),d[n])
 def test_seal(self):hosted.verify_seal()
 def test_no_product_or_network_mutation(self):
  s=(P/'hosted.py').read_text();tree=ast.parse(s)
  self.assertNotIn('subprocess',s);self.assertNotIn('192.168.',s);self.assertNotIn('sudo',s)
  calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='owner']
  self.assertGreater(len(calls),15)

if __name__=='__main__':unittest.main()
