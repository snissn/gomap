import copy,json,pathlib,struct,tempfile,unittest
from unittest import mock
import ycsb_public_sentinel as s
import ycsb_mongodb_reference as r
import ycsb_mongodb_reference_prepare as p

class Controls(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=pathlib.Path(self.tmp.name);self.d=self.root/'mongodb';self.d.mkdir();files={}
  for name in ('source','result','review'):
   q=self.root/name;q.write_text(name);files[str(q)]=s.c.digest(q)
  self.m=dict(schema_version=1,campaign_id='a'*32,targets=['mongodb'],profile='mongodb_w1_journaled',address='127.0.0.1:27018',client_binary='/exact-client',write_concern=dict(w=1,journal_requested=True),deadlines=dict(helper_seconds=180),common_load=dict(comparison_class='same-request-jtrue',address='127.0.0.1:27018',write_concern=dict(w=1,journal_requested=None),reviewed_control_a=dict(scope='bson-binary-and-pointer-bearing-prefix',reviewed=True,files=files)))
  self.owner=dict(pid=123,start_ticks=456,exe_sha256='b'*64,gomaxprocs_unset=True)
  self.t=lambda i:f'2026-10-30T10:00:0{i}.000000Z'
  (self.root/'manifest.json').write_text(json.dumps(self.m))
  for name,a,b in [('load',0,1),('snapshot_loaded',1,2),('run_1',5,6)]:(self.d/(name+'.launch.json')).write_text(json.dumps(dict(start_utc=self.t(a),end_utc=self.t(b))))
 def fake(self):
  class Fake:
   def __init__(obj,address,out):obj.out=out;obj.sock=mock.Mock()
   def close(obj):pass
   def call(obj,command):
    for kind,rid,to,value in [('request',1,0,command),('response',2,1,dict(ok=1,n=1))]:
     body=b'\0'*5+s.wire.document(value);(obj.out/('01-'+kind+'.bin')).write_bytes(struct.pack('<iiii',len(body)+16,rid,to,2013)+body)
    return dict(ok=1,n=1)
  with mock.patch.object(s.wire,'Client',Fake),mock.patch.object(s.c,'observe',return_value=self.owner),mock.patch.object(s.c,'now',side_effect=[self.t(3),self.t(4)]):s.run(self.d,self.m,self.owner,self.m['address'])
 def test_positive_raw_owner_chronology(self):
  self.fake();x=s.validate(self.d,self.m,self.owner,self.m['address']);self.assertEqual(x['request']['writeConcern'],dict(w=1,j=True));self.assertNotEqual(x['request']['insert'],'usertable')
 def test_response_failures(self):
  for x in [dict(ok=0,n=1),dict(ok=1,n=0),dict(ok=1,n=True),dict(ok=1),dict(ok=1,n=1,writeErrors=[]),dict(ok=1,n=1,writeConcernError={})]:
   with self.subTest(x=x),self.assertRaises(Exception):s.accepted(x)
 def test_review_or_proof_changed_rejected(self):
  for key,val in [('reviewed',False),('scope','old-string-only')]:
   m=copy.deepcopy(self.m);m['common_load']['reviewed_control_a'][key]=val
   with self.assertRaises(Exception):s.policy(m)
  pathlib.Path(next(iter(self.m['common_load']['reviewed_control_a']['files']))).write_text('changed')
  with self.assertRaises(Exception):s.policy(self.m)
 def test_phase_concern_bindings(self):
  for stem,suffix in [('load',''),('run_1','&journal=true')]:
   argv=['/exact-client','load' if stem=='load' else 'run','mongodb','-p','mongodb.url=mongodb://'+self.m['address']+'/ycsb?w=1'+suffix]
   s.phase_command(self.d,stem,self.m,argv);s.validate_phase(self.d,stem,self.m)
  x=json.loads((self.d/'load.actual-command.json').read_text());x['argv'][-1]+='&journal=true';(self.d/'load.actual-command.json').write_text(json.dumps(x))
  with self.assertRaises(Exception):s.validate_phase(self.d,'load',self.m)
 def test_duplicate_uri_rejected(self):
  argv=['/exact-client','load','mongodb','-p','mongodb.url=mongodb://'+self.m['address']+'/ycsb?w=1','-p','mongodb.url=mongodb://elsewhere/ycsb?w=1']
  with self.assertRaises(Exception):s.phase_command(self.d,'load',self.m,argv)
 def test_wrong_owner_raw_or_time_rejected(self):
  self.fake();original=(self.d/'public-sentinel.json').read_bytes()
  for field,value in [('campaign_id','c'*32),('address','127.0.0.1:27130'),('start_utc',self.t(0)),('end_utc',self.t(6)),('request_sha256','0'*64)]:
   x=json.loads(original);x[field]=value;(self.d/'public-sentinel.json').write_text(json.dumps(x))
   with self.subTest(field=field),self.assertRaises(Exception):s.validate(self.d,self.m,self.owner,self.m['address'])
  (self.d/'public-sentinel.json').write_bytes(original);bad=dict(self.owner,pid=124)
  with self.assertRaises(Exception):s.validate(self.d,self.m,bad,self.m['address'])
 def test_phase_argv_and_legacy_source_contract(self):
  m=dict(self.m,recordcount=100000,operationcount=10000,threadcount=16)
  self.assertIn('mongodb.url=mongodb://'+m['address']+'/ycsb?w=1',r.phase_argv(m,'load'));self.assertIn('mongodb.url=mongodb://'+m['address']+'/ycsb?w=1&journal=true',r.phase_argv(m,'run_1'))
  legacy=dict(m);legacy.pop('common_load');self.assertIn('mongodb.url=mongodb://'+m['address']+'/ycsb?w=1&journal=true',r.phase_argv(legacy,'load'))
  self.assertNotIn('ycsb_public_sentinel.py',p.source_names(legacy));self.assertIn('ycsb_public_sentinel.py',p.source_names(m))
 def test_differing_durable_ack_requests_require_reviewed_b_and_exact_concerns(self):
  m=copy.deepcopy(self.m);m['common_load']['comparison_class']='durable-ack-differing-requests'
  m['common_load']['reviewed_control_b']=dict(scope='public-bson-command-wal-ack-crash-replay',reviewed=True,files=dict(m['common_load']['reviewed_control_a']['files']))
  s.policy(m)
  g=copy.deepcopy(m);g['targets']=['treedb_mongo'];g['profile']='command_wal_durable';g['write_concern']['journal_requested']=None;s.policy(g)
  for base,key,value in [(m,'journal_requested',None),(m,'journal_requested',False),(g,'journal_requested',True),(g,'journal_requested',False)]:
   bad=copy.deepcopy(base);bad['write_concern'][key]=value
   with self.subTest(target=base['targets'],value=value),self.assertRaises(Exception):s.policy(bad)
  for key,value in [('reviewed',False),('scope','unreviewed broader claim')]:
   bad=copy.deepcopy(g);bad['common_load']['reviewed_control_b'][key]=value
   with self.subTest(key=key),self.assertRaises(Exception):s.policy(bad)
  bad=copy.deepcopy(g);bad['profile']='command_wal_relaxed'
  with self.assertRaises(Exception):s.policy(bad)
if __name__=='__main__':unittest.main()
