"""Offline synthetic bounded-reference controls; no Docker/product execution."""

import sys,pathlib,json,struct,tempfile,copy,unittest
from unittest import mock
import ycsb_mongodb_reference as r
import ycsb_reference_wire as w
import ycsb_compare_validate as c
class BoundedObservationControls(unittest.TestCase):
 def test_valid_and_corrupt_retained_resource_and_query_controls(self):
  with tempfile.TemporaryDirectory() as tmp:
   d=pathlib.Path(tmp);role='server';owner=dict(process={'pid':123},container_id='a'*64);m=dict(campaign_id='b'*32)
   common=dict(schema_version=1,campaign_id=m['campaign_id'],role=role,process=owner['process'])
   resource=dict(common,limits='Max cpu time              449                  450                  seconds\n',members=[123],cgroup='/sys/fs/cgroup/docker/'+owner['container_id'],cgroup_device=27,cgroup_inode=11,memory_max='12884901888',memory_swap_max='0',cpu_policy={})
   default=dict(common,start_utc='2026-10-30T00:00:02Z',end_utc='2026-10-30T00:00:03Z',reply={'ok':1})
   frames=d/'server.default-concern-frames';frames.mkdir()
   for name,doc in [('request',{'getDefaultRWConcern':1,'$db':'admin'}),('response',{'ok':1})]:
    body=b'\0'*5+w.document(doc);(frames/('01-'+name+'.bin')).write_bytes(struct.pack('<iiii',len(body)+16,1,0 if name=='request' else 1,2013)+body)
   def check(a,b):
    (d/'server.resource-observation.json').write_text(json.dumps(a));(d/'server.default-concern.json').write_text(json.dumps(b))
    with mock.patch.object(c,'validate_cpu_policy'):r.bounded_observations(d,role,m,owner,(c.timestamp('2026-10-30T00:00:01Z'),c.timestamp('2026-10-30T00:00:04Z')))
   check(resource,default)
   for key,value in [('memory_max','1'),('memory_swap_max','1'),('cgroup_inode',0),('cgroup_device',True),('members',[124]),('role','reopened'),('campaign_id','wrong'),('process',{'pid':124}),('cgroup','/wrong'),('limits','Max cpu time 450 451 seconds')]:
    with self.subTest(key=key),self.assertRaises(ValueError):check(dict(resource,**{key:value}),default)
   for patch in ({'end_utc':'2026-10-30T00:00:05Z'},{'start_utc':'2026-10-30T00:00:00Z'},{'start_utc':'2026-10-30T00:00:04Z'}):
    with self.subTest(interval=patch),self.assertRaises(ValueError):check(resource,dict(default,**patch))
   with mock.patch.object(c,'validate_cpu_policy',side_effect=ValueError('policy')),self.assertRaises(ValueError):
    (d/'server.resource-observation.json').write_text(json.dumps(resource));(d/'server.default-concern.json').write_text(json.dumps(default));r.bounded_observations(d,role,m,owner,(c.timestamp('2026-10-30T00:00:01Z'),c.timestamp('2026-10-30T00:00:04Z')))
   with self.assertRaises(ValueError):check(resource,dict(default,reply={'ok':0}))
   with self.assertRaises(ValueError):check(resource,dict(default,reply={'ok':0,'code':51300,'codeName':'Other'}))


 def test_raw_default_concern_frames_and_standalone_unknown(self):
  # Exercise the unmocked wire decoder and exact standalone diagnostic policy.
  with tempfile.TemporaryDirectory() as tmp:
   d=pathlib.Path(tmp);cid='a'*64;owner=dict(process=dict(pid=123,start_ticks=1,exe_sha256='b'*64,gomaxprocs_unset=True),container_id=cid);m=dict(campaign_id='b'*32)
   common=dict(schema_version=1,campaign_id=m['campaign_id'],role='server',process=owner['process'])
   policy=dict(process=owner['process'],leader_affinity=[0],cgroup=dict(membership='0::/docker/'+cid,ancestors=[{}]))
   resource=dict(common,limits='Max cpu time 449 450 seconds   \n',members=[123],cgroup='/sys/fs/cgroup/docker/'+cid,cgroup_device=27,cgroup_inode=11,memory_max='12884901888',memory_swap_max='0',cpu_policy=policy)
   (d/'server.resource-observation.json').write_text(json.dumps(resource));frames=d/'server.default-concern-frames';frames.mkdir()
   def frame(doc,to):
    body=b'\0'*5+w.document(doc);return struct.pack('<iiii',len(body)+16,1,to,2013)+body
   request=frame({'getDefaultRWConcern':1,'$db':'admin'},0)
   unknown=dict(ok=0,code=51300,codeName='Location51300',errmsg="'getDefaultRWConcern' is not supported on standalone nodes.")
   def check(reply,raw=None,req=request):
    (frames/'01-request.bin').write_bytes(req);(frames/'01-response.bin').write_bytes(frame(reply,1) if raw is None else raw)
    (d/'server.default-concern.json').write_text(json.dumps(dict(common,start_utc='2026-10-30T00:00:02Z',end_utc='2026-10-30T00:00:03Z',reply=reply)))
    r.bounded_observations(d,'server',m,owner,(c.timestamp('2026-10-30T00:00:01Z'),c.timestamp('2026-10-30T00:00:04Z')))
   check({'ok':1});check(unknown)
   for bad_policy in ({},dict(policy,leader_affinity=[]),dict(policy,process=dict(owner['process'],start_ticks=2)),dict(policy,cgroup={})):
    (d/'server.resource-observation.json').write_text(json.dumps(dict(resource,cpu_policy=bad_policy)))
    with self.subTest(policy=bad_policy),self.assertRaises(ValueError):check({'ok':1})
   (d/'server.resource-observation.json').write_text(json.dumps(resource))
   for raw,req in [(frame({'ok':0},1),request),(frame({'ok':1},2),request),(frame({'ok':1},1)+b'x',request),(frame({'ok':1},1),frame({'ping':1,'$db':'admin'},0))]:
    with self.subTest(raw=raw,req=req),self.assertRaises(ValueError):check({'ok':1},raw,req)
   for key,value in [('code',51301),('codeName','Other'),('errmsg','other')]:
    with self.subTest(key=key),self.assertRaises(ValueError):check(dict(unknown,**{key:value}))

import sys,pathlib,unittest
from types import SimpleNamespace
from unittest import mock
import ycsb_mongodb_reference as r
class ContainerPolicyControls(unittest.TestCase):
 def test_host_adapter_and_rejections_preserve_native_guard(self):
  cid='a'*64;group=pathlib.Path('/sys/fs/cgroup/docker/'+cid);identity={'pid':123}
  data={'/proc/self/mountinfo':'1 0 0:27 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n','/proc/123/cgroup':'0::/docker/'+cid+'\n',str(group/'cgroup.procs'):'123\n','/proc/1/cgroup':'0::/init.scope\n','/sys/fs/cgroup/init.scope/cgroup.procs':'1\n'}
  def links(p):return 'cg:[host]' if str(p) in ('/proc/self/ns/cgroup','/proc/1/ns/cgroup') else 'ns:[container]'
  with mock.patch.object(r.c,'observe',return_value=identity),mock.patch.object(pathlib.Path,'read_text',lambda p,*a,**kw:data[str(p)]),mock.patch.object(pathlib.Path,'stat',return_value=SimpleNamespace(st_dev=27,st_ino=42)),mock.patch.object(r.os,'readlink',side_effect=links),mock.patch.object(r.os,'sched_getaffinity',return_value={0,1}),mock.patch.object(r.c.runtime_observer,'ancestor_policy',return_value=[{'path':'/sys/fs/cgroup'}]):
   result=r.observe_container_cpu_policy(123,identity,group,cid);self.assertEqual(result['leader_affinity'],[0,1])
   with self.assertRaisesRegex(RuntimeError,'namespace differs'):r.c.runtime_observer.quota_snapshot(123)
   for key,bad in [(str(group/'cgroup.procs'),'123\n124\n'),('/proc/123/cgroup','0::/docker/other\n'),('/proc/self/mountinfo','1 0 0:27 /hidden /sys/fs/cgroup rw - cgroup2 cgroup rw\n')]:
    original=data[key];data[key]=bad
    with self.subTest(key=key),self.assertRaises(ValueError):r.observe_container_cpu_policy(123,identity,group,cid)
    data[key]=original
   data['/sys/fs/cgroup/init.scope/cgroup.procs']='2\n'
   with self.assertRaisesRegex(ValueError,'contain PID1'):r.observe_container_cpu_policy(123,identity,group,cid)
   data['/sys/fs/cgroup/init.scope/cgroup.procs']='1\n'
   calls=iter((42,43))
   with mock.patch.object(pathlib.Path,'stat',lambda p,*a,**k:SimpleNamespace(st_dev=27,st_ino=next(calls) if p==group else 55)),self.assertRaisesRegex(ValueError,'identity changed'):r.observe_container_cpu_policy(123,identity,group,cid)


if __name__ == "__main__": unittest.main()
