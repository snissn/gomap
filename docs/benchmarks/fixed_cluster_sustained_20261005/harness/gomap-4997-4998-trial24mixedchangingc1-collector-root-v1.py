"""Root-approved rf4trial24mixedchangingc1 collector/lifecycle. No automatic workload retry.
Provisional constructor: final landed pins and independent prereview are mandatory.
Observations only: independent artifact acceptance remains root-owned.
The manifest pins fresh receipts and exact owned CIDs; no prior population/image pins apply.
Import is inert. Root alone may execute --approved after exact-source prereview.
"""
import argparse
import base64
import datetime
import hashlib
import json
import pathlib
import re
import shlex
import secrets
import signal
import struct
import math
import copy
import decimal
import subprocess
import sys
import time

if not __debug__:
    raise RuntimeError('assertion checks require ordinary Python, not -O')
RUN = 'rf4trial24mixedchangingc1'
QUERY_RUN = RUN + 'mixedc1v1'
NAME = 'treedb-4250-' + RUN + '-mixed-window-c1-v1'
HOST = 'mikers@192.168.0.185'
ROOT = '/home/mikers/gomap-4250-twohost-' + RUN
INPUT_ROOT = '/home/mikers/gomap-4997-4998-' + RUN + '-inputs-root-v1'
LOCAL_INPUT_ROOT = '/tmp/gomap-4997-4998-' + RUN + '-inputs-root-v1'
OUTPUT = pathlib.Path('/tmp/gomap-4997-4998-trial24mixedchangingc1-window-root-v1')
GATE = '/home/mikers/gomap-4997-4998-trial24mixedchangingc1-window-resource-root-v1'
GATE_DIR = GATE + '/gate'
MOUNT_GATE = '/run-resource-gate'
OUTER_SECONDS = 480
TOKEN_BYTES = 2048
FIELDS = ('memory.current', 'memory.peak', 'memory.max', 'memory.events',
          'memory.swap.current', 'memory.swap.max', 'memory.swap.events',
          'cpu.max', 'cpu.stat', 'io.stat')
FLAGS = ('RootAccepted', 'SourcePrereviewAccepted', 'MixedWindowModeValidated',
         'ResourceGateModeValidated', 'DriverUserReadAccessValidated',
         'LandedSourceValidated', 'PopulationAuditModeValidated',
         'CollectorPrereviewAccepted', 'ExclusiveCampaignOwner')
RECEIPTS = ('source_inventory', 'build', 'source_prereview', 'bootstrap',
            'config', 'plan', 'baseline', 'landed_source', 'collector_prereview',
            'run_authorization', 'predeclaration', 'initial_oracle', 'prefix_oracles')
HOSTS = {'node-a':'192.168.0.111', 'node-b':'192.168.0.111',
         'node-c':'192.168.0.185', 'node-d':'192.168.0.185'}


def strict_json(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate JSON key: ' + key)
            result[key] = value
        return result
    # Go encoding/json writes FP32 negative zero as the integer token -0.
    # Preserve its sign before vector hashing/reencoding; every other integer
    # token remains int so ordinary protocol counters keep their exact type.
    return json.loads(raw, object_pairs_hook=unique, parse_int=lambda token: -0.0 if token=='-0' else int(token), parse_constant=lambda s: (_ for _ in ()).throw(ValueError('nonfinite JSON '+s)))


def digest_valid(value, size=64):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{%d}' % size, value) is not None and value != '0'*size


def validate_manifest(a, collector_sha):
    keys = set(FLAGS) | {'Version','RunID','remote_root','source_head','source_tree',
        'source_inventory_sha256','collector_sha256','query_image','driver_sha256',
        'driver_uid_gid','bootstrap_sha256','config_sha256','plan_sha256',
        'input_inventory_sha256','baseline','nodes','receipts','local_pins',
        'population_limits','server_cli_path'}
    assert isinstance(a, dict) and set(a) == keys
    assert type(a['Version']) is int and a['Version'] == 1 and a['RunID'] == RUN
    assert all(a[k] is True for k in FLAGS)
    assert a['remote_root'] == ROOT
    assert a['server_cli_path'] == '/treedb-fixed-peer'
    validate_population_limits(a['population_limits'], a['baseline']['PopulationRows'], 128)
    for k in ('source_head','source_tree'):
        assert digest_valid(a[k],40), k
    for k in keys:
        if k.endswith('sha256'):
            assert digest_valid(a[k]), k
    assert a['collector_sha256'] == collector_sha
    assert re.fullmatch('sha256:[0-9a-f]{64}', a['query_image']) and a['query_image'] != 'sha256:'+'0'*64
    assert re.fullmatch('[1-9][0-9]*:[1-9][0-9]*', a['driver_uid_gid'])
    b = a['baseline']
    assert set(b) == {'PopulationRows','PopulationSHA256','HighestCommitIndex','CorpusRows','AnchorRows','Dimensions','Queries'}
    assert all(type(b[k]) is int for k in b if k != 'PopulationSHA256')
    assert b['CorpusRows'] == 10000 and b['Dimensions'] == 128 and b['Queries'] == 16
    assert b['AnchorRows'] > 0 and b['PopulationRows'] == b['CorpusRows'] + b['AnchorRows']
    assert b['HighestCommitIndex'] > 0 and digest_valid(b['PopulationSHA256'])
    assert isinstance(a['nodes'],list) and len(a['nodes']) == 4
    assert [n['node'] for n in a['nodes']] == list(HOSTS)
    for n in a['nodes']:
        assert set(n) == {'node','host','image','server_sha256','cid','inspect_path','inspect_sha256'}
        assert n['host'] == HOSTS[n['node']] and digest_valid(n['cid'])
        assert re.fullmatch('sha256:[0-9a-f]{64}', n['image']) and n['image'] != 'sha256:'+'0'*64
        assert digest_valid(n['server_sha256']) and digest_valid(n['inspect_sha256'])
    assert len({n['cid'] for n in a['nodes']}) == 4
    pins = a['local_pins']
    assert isinstance(pins,dict) and 11 <= len(pins) <= 64
    assert all(isinstance(p,str) and p.startswith('/') and digest_valid(h) for p,h in pins.items())
    assert set(a['receipts']) == set(RECEIPTS)
    for key, p in a['receipts'].items():
        assert p in pins, key
    for n in a['nodes']:
        assert pins[n['inspect_path']] == n['inspect_sha256']
    for key in ('source_inventory','bootstrap','config','plan'):
        assert pins[a['receipts'][key]] == a[key+'_sha256']
    assert pins[LOCAL_INPUT_ROOT+'/input-inventory.json'] == a['input_inventory_sha256']
    return a


def input_name(name):
    p = pathlib.PurePosixPath(name)
    assert isinstance(name,str) and name and not p.is_absolute()
    assert p.as_posix() == name and all(v not in ('.','..') for v in p.parts)
    return name


def validate_input_inventory(inv):
    assert isinstance(inv,dict) and 1 <= len(inv) <= 512
    assert all(input_name(p) and digest_valid(h) for p,h in inv.items())
    assert {'probe.jsonl','provenance.json','dataset/manifest.json'} <= set(inv)
    return inv


def expected_binds(n):
    p = ROOT+'/'+n['node']
    binds = {p+'/config.json:/config.json:ro',p+'/credentials:/credentials:ro',p+'/data:/data',p+'/raft:/raft'}
    if n['node'] == 'node-c':
        binds.add(p+'/dataset:/dataset:ro')
    return binds


def validate_voter(x, n):
    assert x['Id'] == n['cid'] and x['Name'] == '/'+n['name'] and x['Image'] == n['image']
    assert x['Config']['Labels']['treedb.fixed-cluster.run'] == RUN
    h = x['HostConfig']
    assert h['Memory'] == h['MemorySwap'] == 2147483648 and h['NanoCpus'] == 2000000000
    assert h['RestartPolicy']['Name'] == 'no' and h['NetworkMode'] == 'host'
    assert len(h['Binds']) == len(expected_binds(n)) and set(h['Binds']) == expected_binds(n)
    assert not x['State']['OOMKilled']
    return x


def owned(x, cid=None):
    assert x['Image'] == image and x['Name'] == '/'+NAME
    assert x['Config']['Labels']['treedb.fixed-cluster.run'] == RUN
    assert isinstance(launch_nonce,str) and re.fullmatch('[0-9a-f]{32}',launch_nonce)
    assert x['Config']['Labels']['treedb.fixed-cluster.invocation'] == launch_nonce
    if cid:
        assert x['Id'] == cid
    else:
        assert digest_valid(x['Id'])
    h = x['HostConfig']
    assert h['Memory'] == h['MemorySwap'] == 2147483648 and h['NanoCpus'] == 2000000000
    assert h['RestartPolicy']['Name'] == 'no'
    assert h['NetworkMode'] == 'host' and x['Config']['User'] == APPROVED['driver_uid_gid']
    expected = {root+'/node-c/config.json:/config.json:ro',root+'/node-c/credentials:/credentials:ro',
        root+'/bootstrap-qualify.json:/bootstrap.json:ro',INPUT_ROOT+':/recall:ro',GATE_DIR+':'+MOUNT_GATE+':rw'}
    assert len(h['Binds']) == len(expected) and set(h['Binds']) == expected


def driver_probe_target(cid, launch_attempted):
    # An earlier preparation refusal never establishes ownership by name.
    if not launch_attempted:
        assert cid is None
        return None
    assert cid is None or digest_valid(cid)
    # Lost reply may recover ONLY through owned()'s fresh invocation nonce.
    return cid or NAME


def sample_all(cid=None, boundary=None):
    batch = []
    for ip in ('192.168.0.111','192.168.0.185'):
        targets = [dict(name=n['cid'],node=n['node'],role='voter',previous_path=paths.get(n['node'])) for n in NODES if n['host']==ip]
        if cid and ip=='192.168.0.185':
            targets.append(dict(name=cid,node='client',role='client',previous_path=paths.get('client')))
        s = json.loads(call('samples-'+ip,['python3','-c',SAMPLER,json.dumps(targets),json.dumps(FIELDS)],host='mikers@'+ip))
        s['boundary'] = boundary
        s['host'] = ip
        s['local_started_unix'] = last_record['started_unix']
        s['local_finished_unix'] = last_record['finished_unix']
        s['clock_offset_low'] = s['started_unix']-s['local_finished_unix']
        s['clock_offset_high'] = s['finished_unix']-s['local_started_unix']
        for target in s['targets']:
            x = target['inspect']
            paths[target['node']] = target['cgroup_path']
            if target['role']=='client':
                owned(x,cid)
                if boundary is not None:
                    assert x['State']['Running'] and x['State']['Pid']>0 and not x['State']['OOMKilled']
            else:
                validate_voter(x,next(n for n in NODES if n['node']==target['node']))
                assert x['State']['Running']
        batch.append(s)
    return batch


def validate_mixed_report(planned, report):
    # Structural/identity observation checks only; root independently verifies
    # canonical score bits, causal prefixes, ledger, audits and acceptance recall.
    sustained_shape(planned,True);sustained_shape(report)
    a, pa, b = report['Admission'], planned['Admission'], APPROVED['baseline']
    for admission in (a, pa):
        assert admission['RunID']==QUERY_RUN and admission['Phase']=='post-only'
        assert len(admission['Queries'])==16
        assert admission['BinarySHA256']==APPROVED['driver_sha256']
        assert admission['ConfigSHA256']==APPROVED['config_sha256'] and admission['BootstrapSHA256']==APPROVED['bootstrap_sha256']
        assert admission['Timeout']==420000000000 and admission['RPCTimeout']==3000000000
        assert admission['PopulationRows']==b['PopulationRows'] and admission['PopulationSHA256']==b['PopulationSHA256']
        assert admission['HighestCommitIndex']==b['HighestCommitIndex']
    assert len(planned['Anchors'])==len(report['Anchors'])==b['AnchorRows'] and planned['Anchors']==report['Anchors']
    assert len(report['Prefixes'])==59 and planned['Prefixes']==report['Prefixes']==prefix_oracles['Prefixes']
    assert encode_go_json(original_requests(report['Writes']))==encode_go_json(prefix_oracles['OriginalRequests']), 'original request byte identity including signed zero'
    assert report['Kind']=='fixed_cluster_mixed_window_v1' and report['PaceInterval']==5000000000
    assert report['Concurrency']==1 and report['WarmupPlanned']==64 and report['MaxAttempts']==65536
    assert report['OutputBytes']==134217728 and report['RequestedDuration']==300000000000
    assert report['ResourceGateDir']==MOUNT_GATE and len(report['ResourceBoundaries'])==len(boundary_evidence)==2
    for phase,boundary in zip(('ready','done'),report['ResourceBoundaries']):
        raw=validate_gate_receipt(gate_expected[phase+'.json'],phase,QUERY_RUN,gate_nonce)
        validate_ack_binding(gate_expected[phase+'.json'],gate_expected[phase+'.ack'],phase,QUERY_RUN,gate_nonce)
        assert all(boundary[k]==raw[k] for k in raw if k not in ('AcknowledgedUTC','WaitNS'))
        assert boundary['AcknowledgedUTC']!='0001-01-01T00:00:00Z' and boundary['WaitNS']>=0
    assert report['MeasuredOriginUTC']==boundary_evidence[1]['receipt']['MeasuredOriginUTC']
    assert report['ActualDurationNS']==boundary_evidence[1]['receipt']['ActualDurationNS']>=300000000000
    c,w=report['Counts'],report['WarmupCounts']
    assert c['Planned']==c['Attempted']+c['Unissued'] and c['Attempted']==sum(c[k] for k in ('Succeeded','Failed','Canceled','Unknown'))
    assert report['Completions']==c['Attempted'] and report['WarmupCompletions']==w['Attempted']
    assert w['Attempted']==w['Succeeded']==64 and w['Failed']==w['Canceled']==w['Unknown']==0
    assert c['Attempted']==c['Succeeded'] and c['Failed']==c['Canceled']==c['Unknown']==0
    assert all(v>0 for v in report['MeasuredQuerySucceeded'])
    assert len(report['Writes'])==58 and all(v['Invoked'] and v['Outcome']=='succeeded' for v in report['Writes'])
    assert len(report['Retries'])==2 and all(v['Invoked'] and v['Outcome']=='succeeded' for v in report['Retries'])
    assert len(report['Visibility'])==58 and len(report['VisibilityEvidence'])==58
    assert report['HighestNewCommitIndex']>b['HighestCommitIndex'] and report['RequiredAppliedIndex']>=report['HighestNewCommitIndex']
    assert report['AuditPlan']['HighestNewCommitIndex']==report['HighestNewCommitIndex']
    assert report['AuditPlan']['RequiredAppliedIndex']==report['RequiredAppliedIndex']
    audits=report['Audits']
    assert len(audits)==4 and {v['ColocatedAudit']['NodeID'] for v in audits}==set(HOSTS)
    for v in audits:
        audit=v['ColocatedAudit']
        assert audit['RunID']==QUERY_RUN and audit['AppliedIndex']>=report['RequiredAppliedIndex']
        assert audit['RetainedCount']==58 and len(audit['Witnesses'])==58 and len(audit['Final'])==4
    assert not report['Truncated'] and report['StopReason']=='window_elapsed'
    assert planned['Profile']==report['Profile']=='changing-top10'
    assert a['Verdict']=='ACCEPTED_INPUTS_CHANGING_TOP10_PENDING_RUNTIME'
    assert report['Verdict']=='ACCEPT_MIXED_CHANGING_TOP10_RECALL_WINDOW_OBSERVATION_PENDING_ROOT_SHUTDOWN_VERIFICATION'
    validate_causal_report(report, initial_vectors)
    return c


def call(label, args, timeout=30, host=HOST, input_bytes=None):
    global serial, last_record
    serial += 1
    if deadline is not None:
        remaining=deadline-time.monotonic()
        if remaining<=0:raise TimeoutError("collector deadline; no retry")
        timeout=min(timeout,remaining)
    argv = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", host, shlex.join(args)]
    record = dict(argv=argv, started_unix=time.time())
    if input_bytes is not None:
        assert isinstance(input_bytes,bytes) and len(input_bytes)<=512<<10
        record['stdin_sha256']=hashlib.sha256(input_bytes).hexdigest()
        record['stdin_bytes']=len(input_bytes)
    try:
        v = subprocess.run(argv, input=input_bytes, capture_output=True, timeout=timeout)
        (OUTPUT / ("%04d-%s.stdout" % (serial,label))).write_bytes(v.stdout)
        (OUTPUT / ("%04d-%s.stderr" % (serial,label))).write_bytes(v.stderr)
        v.stdout=v.stdout.decode("utf-8",errors="surrogateescape");v.stderr=v.stderr.decode("utf-8",errors="surrogateescape")
        record.update(exit_code=v.returncode, stdout=v.stdout, stderr=v.stderr)
    except subprocess.TimeoutExpired as e:
        def decoded(x):
            return x.decode(errors="replace") if isinstance(x, bytes) else (x or "")
        (OUTPUT / ("%04d-%s.stdout" % (serial,label))).write_bytes(e.stdout or b"")
        (OUTPUT / ("%04d-%s.stderr" % (serial,label))).write_bytes(e.stderr or b"")
        record.update(exit_code=None, timed_out=True, stdout=decoded(e.stdout), stderr=decoded(e.stderr))
        raise
    finally:
        record["finished_unix"] = time.time()
        last_record = record
        (OUTPUT / ("%04d-%s.json" % (serial, label))).write_text(json.dumps(record, indent=2) + "\n")
    if v.returncode:
        raise RuntimeError((label, v.returncode, v.stderr))
    return v.stdout


SAMPLER = r'''
import json,pathlib,subprocess,sys,time
started=time.time(); result=[]
for target in json.loads(sys.argv[1]):
 begin=time.time(); x=json.loads(subprocess.check_output(['docker','inspect',target['name']],text=True))[0]
 path=None; discovery_error=None
 if x['State']['Running'] and x['State']['Pid']:
  try:
   lines=pathlib.Path('/proc/%d/cgroup'%x['State']['Pid']).read_text().splitlines()
   path=next(line[3:] for line in lines if line.startswith('0::'))
  except Exception as e: discovery_error=repr(e)
 if path is None: path=target.get('previous_path')
 fields={}
 for field in json.loads(sys.argv[2]):
  field_begin=time.time()
  try:
   if path is None: raise RuntimeError('no discovered cgroup-v3 path')
   value=(pathlib.Path('/sys/fs/cgroup')/path.lstrip('/')/field).read_text()
   fields[field]={'status':'available','raw':value}
  except Exception as e: fields[field]={'status':'missing','reason':repr(e)}
  fields[field].update(started_unix=field_begin,finished_unix=time.time())
 status_begin=time.time()
 try:
  if not x['State']['Running'] or not x['State']['Pid']: raise RuntimeError('process stopped; RSS/HWM unavailable')
  raw=pathlib.Path('/proc/%d/status'%x['State']['Pid']).read_text()
  assert 'VmRSS:' in raw and 'VmHWM:' in raw
  fields['process.status']={'status':'available','raw':raw}
 except Exception as e: fields['process.status']={'status':'missing','reason':repr(e)}
 fields['process.status'].update(started_unix=status_begin,finished_unix=time.time())
 result.append({'role':target['role'],'node':target['node'],'inspect':x,'cgroup_path':path,'path_discovery_error':discovery_error,'fields':fields,'network':{'status':'unsupported','reason':'host-network shared namespace; per-container network byte counters unavailable; no host totals substituted'},'started_unix':begin,'finished_unix':time.time()})
print(json.dumps({'started_unix':started,'finished_unix':time.time(),'targets':result,'scope':'periodic samples include setup/warmup/drain; missing fields retained; no exact measured-only or whole-lifetime peak claim'}))
'''


def validate_gate_receipt(raw, phase, run_id, nonce=None):
    if not isinstance(raw, bytes) or not 0<len(raw)<=TOKEN_BYTES or not raw.endswith(b'\n'):
        raise ValueError('incomplete or oversized gate receipt')
    def unique(pairs):
        result={}
        for key,value in pairs:
            if key in result:raise ValueError('duplicate receipt key')
            result[key]=value
        return result
    v=json.loads(raw,object_pairs_hook=unique)
    assert set(v)=={'Version','RunID','Phase','Nonce','PublishedUTC','MeasuredOriginUTC','ActualDurationNS','StopReason','AcknowledgedUTC','WaitNS'}
    assert type(v['Version']) is int and v['Version']==1 and v['RunID']==run_id and v['Phase']==phase
    assert isinstance(v['Nonce'],str) and re.fullmatch(r'[0-9a-f]{32}',v['Nonce']) and (nonce is None or v['Nonce']==nonce)
    assert type(v['ActualDurationNS']) is int and v['ActualDurationNS']>=0 and type(v['WaitNS']) is int and v['WaitNS']==0
    assert v['AcknowledgedUTC']=='0001-01-01T00:00:00Z' and isinstance(v['StopReason'],str)
    published=datetime.datetime.fromisoformat(v['PublishedUTC'].replace('Z','+00:00'));assert published.utcoffset()==datetime.timedelta(0) and published.year>=2026
    if phase in ('claim','ready'):
        assert v['MeasuredOriginUTC']=='0001-01-01T00:00:00Z' and v['ActualDurationNS']==0 and v['StopReason']==''
    else:
        assert phase=='done' and v['ActualDurationNS']>0 and v['StopReason']
        origin=datetime.datetime.fromisoformat(v['MeasuredOriginUTC'].replace('Z','+00:00'));assert origin.utcoffset()==datetime.timedelta(0) and published.timestamp()>=origin.timestamp()+v['ActualDurationNS']/1e9
    return v


GATE_PROBE = r"""
import base64,json,os,pathlib,stat,subprocess,sys,time
p=pathlib.Path(sys.argv[1]);before=os.lstat(p);assert stat.S_ISDIR(before.st_mode)
entries=[]
with os.scandir(p) as scan:
 for entry in scan:
  entries.append(entry.name)
  assert len(entries)<=5
files={}
for name in entries:
 assert name in ('claim.json','ready.json','ready.ack','done.json','done.ack')
 q=p/name;info=os.lstat(q);assert stat.S_ISREG(info.st_mode) and info.st_size<=2048
 with q.open('rb') as f:
  opened=os.fstat(f.fileno());assert (opened.st_dev,opened.st_ino)==(info.st_dev,info.st_ino)
  raw=f.read(2049);assert len(raw)<=2048
 files[name]=base64.b64encode(raw).decode()
x=json.loads(subprocess.check_output(['docker','inspect',sys.argv[2]],timeout=5))[0]
print(json.dumps({'files':files,'directory_identity':[before.st_dev,before.st_ino],'inspect':x,'remote_sample_unix':time.time()}),file=sys.stderr,flush=True)
assert x['State']['Running'] and x['State']['Pid']>0 and not x['State']['OOMKilled'], 'client exited before gate acknowledgment'
# Accepted driver has only planned/result events. A final result while a gate
# remains unacknowledged is premature; retain the log prefix in this raw proof.
r=subprocess.run(['docker','logs','--tail','1',sys.argv[2]],capture_output=True,timeout=5)
assert r.returncode==0 and len(r.stdout)<=134217728
assert not r.stdout.startswith(b'{"Event":"result"'), 'premature result before acknowledgment'
after=os.lstat(p);assert (before.st_dev,before.st_ino)==(after.st_dev,after.st_ino)
print(json.dumps({'files':files,'directory_identity':[before.st_dev,before.st_ino],'inspect':x,'log_prefix_b64':base64.b64encode(r.stdout[:128]).decode(),'log_stderr_b64':base64.b64encode(r.stderr).decode(),'remote_finished_unix':time.time()}))
"""


GATE_SNAPSHOT = r"""
import base64,json,os,pathlib,stat,sys,time
p=pathlib.Path(sys.argv[1]);result={'path':str(p),'time':time.time(),'tokens':{}}
try:
 st=os.lstat(p);assert stat.S_ISDIR(st.st_mode)
 entries=[]
 with os.scandir(p) as scan:
  for entry in scan:
   entries.append(entry.name)
   if len(entries)>5:break
 result['entries']=entries
 for name in entries:
  value={}
  try:
   assert name in ('claim.json','ready.json','ready.ack','done.json','done.ack')
   q=p/name;info=os.lstat(q);value['size']=info.st_size;assert stat.S_ISREG(info.st_mode)
   with q.open('rb') as f:
    opened=os.fstat(f.fileno());assert (opened.st_dev,opened.st_ino)==(info.st_dev,info.st_ino)
    raw=f.read(2049)
   value['raw_b64']=base64.b64encode(raw).decode();value['complete']=info.st_size<=2048 and len(raw)<=2048
  except Exception as e:value['error']=repr(e)
  result['tokens'][name]=value
except Exception as e:result['error']=repr(e)
print(json.dumps(result))
"""


GATE_ACK = r"""
import base64,json,os,pathlib,stat,sys,time
p=pathlib.Path(sys.argv[1]);phase=sys.argv[2];raw=base64.b64decode(sys.argv[3],validate=True);identity=json.loads(sys.argv[4]);assert phase in ('ready','done') and 0<len(raw)<=2048 and raw.endswith(b'\n')
st=os.lstat(p);assert stat.S_ISDIR(st.st_mode) and [st.st_dev,st.st_ino]==identity
q=p/(phase+'.json');info=os.lstat(q);assert stat.S_ISREG(info.st_mode) and info.st_size<=2048
with q.open('rb') as f:
 opened=os.fstat(f.fileno());assert (opened.st_dev,opened.st_ino)==(info.st_dev,info.st_ino) and f.read(2049)==raw
ack=p/(phase+'.ack');assert not os.path.lexists(ack)
tmp=p.parent/(phase+'.ack-exact-bytes.tmp')
with tmp.open('xb') as f:f.write(raw)
assert not os.path.lexists(ack)
# Single trusted collector owns acknowledgments; temporary file is outside gate.
os.rename(tmp,ack)
with ack.open('rb') as f:assert f.read(2049)==raw
print(json.dumps({'phase':phase,'sha256':__import__('hashlib').sha256(raw).hexdigest(),'bytes':len(raw),'installed_unix':time.time(),'directory_identity':identity}))
"""


def wait_gate(phase,cid):
    global gate_identity,gate_nonce
    target=phase+'.json'
    allowed={'claim.json','ready.json'} if phase=='ready' else {'claim.json','ready.json','ready.ack','done.json'}
    while True:
        proof=json.loads(call('gate-poll-'+phase,['python3','-c',GATE_PROBE,GATE_DIR,cid]))
        owned(proof['inspect'],cid)
        if gate_identity is None:gate_identity=proof['directory_identity']
        assert gate_identity==proof['directory_identity']
        files={name:base64.b64decode(raw,validate=True) for name,raw in proof['files'].items()}
        assert set(files)<=allowed, 'unexpected/premature gate token'
        assert all(name in files and files[name]==raw for name,raw in gate_expected.items()), 'changed or missing prior gate token'
        complete=True
        for name in ('claim.json',target):
            if name not in files or not files[name].endswith(b'\n'):
                complete=False;continue # driver writes directly after exclusive create
            token_phase='claim' if name=='claim.json' else phase
            value=validate_gate_receipt(files[name],token_phase,QUERY_RUN,gate_nonce)
            if gate_nonce is None:gate_nonce=value['Nonce']
            if name not in gate_expected:
                gate_expected[name]=files[name]
                (OUTPUT/('gate-'+name)).write_bytes(files[name])
        if complete:
            assert 'claim.json' in gate_expected and target in gate_expected
            return validate_gate_receipt(files[target],phase,QUERY_RUN,gate_nonce)
        time.sleep(min(0.5,max(0,deadline-time.monotonic())))


def validate_ack_binding(raw, ack_raw, phase, run_id, nonce):
    value=validate_gate_receipt(raw,phase,run_id,nonce)
    assert isinstance(ack_raw,bytes) and ack_raw==raw, 'ack must copy exact receipt bytes'
    return value


def validate_boundary_samples(batch,phase):
    roles=[]
    for sample in batch:
        assert sample['boundary']==phase and sample['started_unix']<=sample['finished_unix']
        for target in sample['targets']:
            roles.append(target['node'])
            state=target['inspect']['State'];assert state['Running'] and state['Pid']>0 and not state['OOMKilled']
            assert target['network']['status']=='unsupported'
            for field in FIELDS+('process.status',):
                value=target['fields'][field]
                assert value['status'] in ('available','missing')
                assert sample['started_unix']<=value['started_unix']<=value['finished_unix']<=sample['finished_unix']
                if value['status']=='available':assert isinstance(value['raw'],str) # empty io.stat is an actual available read
                else:assert value['reason']
    assert len(roles)==5 and set(roles)=={'node-a','node-b','node-c','node-d','client'}


def boundary_capture(phase,cid):
    receipt=wait_gate(phase,cid)
    indices=list(range(len(samples),len(samples)+2))
    batch=sample_all(cid,boundary=phase);samples.extend(batch)
    validate_boundary_samples(batch,phase)
    (OUTPUT/'resource-samples.json').write_text(json.dumps(samples,indent=2)+'\n')
    # Recheck immutable receipts/client liveness after actual reads and before ack.
    assert wait_gate(phase,cid)==receipt
    raw=gate_expected[phase+'.json']
    ack=json.loads(call('gate-ack-'+phase,['python3','-c',GATE_ACK,GATE_DIR,phase,base64.b64encode(raw).decode(),json.dumps(gate_identity)]))
    assert ack['sha256']==hashlib.sha256(raw).hexdigest() and ack['bytes']==len(raw) and ack['directory_identity']==gate_identity
    validate_ack_binding(raw,raw,phase,QUERY_RUN,gate_nonce)
    gate_expected[phase+'.ack']=raw
    (OUTPUT/('gate-'+phase+'.ack')).write_bytes(raw)
    boundary_evidence.append({'phase':phase,'receipt':receipt,'sample_indices':indices,'ack':ack,'ack_local_started_unix':last_record['started_unix'],'ack_local_finished_unix':last_record['finished_unix']})
    (OUTPUT/'resource-boundaries.json').write_text(json.dumps(boundary_evidence,indent=2)+'\n')


def measurement_resource_brackets(samples, start, end):
    roles=('node-a','node-b','node-c','node-d','client')
    fields=FIELDS+('process.status',)
    driver_samples=[s for s in samples if s['host']=='192.168.0.185']
    if not driver_samples:
        return {role:{'status':'unavailable','reason':'no driver-host clock bounds'} for role in roles}
    dlo=min(s['clock_offset_low'] for s in driver_samples)
    dhi=max(s['clock_offset_high'] for s in driver_samples)
    brackets={}
    for role in roles:
        per_field={}
        for field in fields:
            entries=[]
            for index,s in enumerate(samples):
                for target in s['targets']:
                    if target['node']!=role:continue
                    v=target['fields'].get(field,{})
                    if v.get('status')!='available':continue
                    if s['host']=='192.168.0.185':low=high=0
                    else:low=dlo-s['clock_offset_high'];high=dhi-s['clock_offset_low']
                    entries.append((v['started_unix']+low,v['finished_unix']+high,index))
            before=[v for v in entries if v[1]<=start]
            after=[v for v in entries if v[0]>=end]
            if not before or not after:
                per_field[field]={'status':'unavailable','reason':'no actual available field reads conservatively enclosing measurement; gate presence is not a substitute for field availability'}
                continue
            left=max(before,key=lambda v:v[1]);right=min(after,key=lambda v:v[0])
            per_field[field]={'status':'available','before_sample_index':left[2],'after_sample_index':right[2],'before_started_driver_clock_lower':left[0],'before_finished_driver_clock_upper':left[1],'after_started_driver_clock_lower':right[0],'after_finished_driver_clock_upper':right[1],'pre_overscan_seconds_range':[start-left[1],start-left[0]],'post_overscan_seconds_range':[right[0]-end,right[1]-end],'scope':'enclosing sampled interval includes overscan; neither exact measured-only counter deltas nor measurement-only peaks'}
        brackets[role]={'status':'available' if all(v['status']=='available' for v in per_field.values()) else 'incomplete','fields':per_field,'network':{'status':'unsupported','reason':'host-network shared namespace; no per-container bytes'}}
    return brackets


def read_bounded(path, cap=64<<20):
    p=pathlib.Path(path)
    assert p.is_file() and not p.is_symlink() and p.stat().st_size<=cap
    with p.open('rb') as f:
        raw=f.read(cap+1)
    assert len(raw)<=cap
    return raw


def validate_source_bindings(a, pinned):
    source = strict_json(pinned[a['receipts']['source_inventory']])
    assert set(source) == {'head','tree','rows','overlays'}
    assert source['head'] == a['source_head'] and source['tree'] == a['source_tree']
    assert isinstance(source['rows'],list) and 0 < len(source['rows']) <= 100000
    paths=set()
    for row in source['rows']:
        assert set(row) == {'path','mode','git_blob'}
        path=input_name(row['path'])
        assert path not in paths and row['mode'] in ('100644','100755')
        assert digest_valid(row['git_blob'],40)
        paths.add(path)
    assert isinstance(source['overlays'],dict)
    assert all(input_name(path) in paths and digest_valid(sha) for path,sha in source['overlays'].items())
    review = strict_json(pinned[a['receipts']['source_prereview']])
    assert review['outcome'] == 'ACCEPT'
    assert review['candidate_head'] == a['source_head'] and review['candidate_tree'] == a['source_tree']
    build = strict_json(pinned[a['receipts']['build']])
    assert set(build) == {'state','head','tree','source_inventory_sha256','server_sha256',
        'driver_sha256','server_images','driver_image','source_verified_before_after'}
    assert build['state'] == 'VERIFIED_BUILD_FROM_FROZEN_SOURCE' and build['source_verified_before_after'] is True
    assert build['head'] == source['head'] and build['tree'] == source['tree']
    assert build['source_inventory_sha256'] == a['source_inventory_sha256']
    assert hashlib.sha256(pinned[a['receipts']['source_inventory']]).hexdigest() == build['source_inventory_sha256']
    assert digest_valid(build['server_sha256']) and build['driver_sha256'] == a['driver_sha256']
    assert build['driver_image'] == a['query_image']
    assert isinstance(build['server_images'],dict) and set(build['server_images']) == set(HOSTS)
    for n in a['nodes']:
        assert build['server_sha256'] == n['server_sha256']
        assert build['server_images'][n['node']] == n['image']
    validate_finalization(a,pinned)
    return source,review,build


def prepare_local(a):
    pinned={}
    total=0
    for path,h in a['local_pins'].items():
        raw=read_bounded(path)
        total+=len(raw)
        assert total<=256<<20 and hashlib.sha256(raw).hexdigest()==h, path
        pinned[path]=raw
    validate_source_bindings(a,pinned)  # semantic source/review/ELF/image admission before ANY external call
    assert strict_json(pinned[a['receipts']['baseline']])==a['baseline']
    inv=validate_input_inventory(strict_json(pinned[LOCAL_INPUT_ROOT+'/input-inventory.json']))
    input_files={}
    for name,h in inv.items():
        raw=read_bounded(pathlib.Path(LOCAL_INPUT_ROOT)/name)
        total+=len(raw)
        assert total<=256<<20 and hashlib.sha256(raw).hexdigest()==h,name
        input_files[name]=raw
    vectors=build_initial_population(input_files, strict_json(pinned[a['receipts']['bootstrap']]), a['baseline'])
    oracle=strict_json(pinned[a['receipts']['initial_oracle']])
    assert oracle==population_identity(vectors,128), 'independently frozen initial full oracle disagrees'
    validate_prefix_oracles(strict_json(pinned[a['receipts']['prefix_oracles']]),a,vectors)
    nodes=[]
    for original in a['nodes']:
        n=dict(original,root=ROOT+'/'+original['node'],name='treedb-4250-'+RUN+'-'+original['node'])
        x=strict_json(pinned[n['inspect_path']])
        assert isinstance(x,list) and len(x)==1  # direct inspect, no historical log wrapper
        validate_voter(x[0],n)
        assert x[0]['State']['Running'] or x[0]['State']['ExitCode']==0
        nodes.append(n)
    return pinned,inv,nodes,vectors


def inspect_voter(n,label):
    x=json.loads(call(label,['docker','inspect',n['cid']],host='mikers@'+n['host']))
    assert isinstance(x,list) and len(x)==1
    return validate_voter(x[0],n)


def driver_arguments():
    return ['docker','run','--pull=never','-d','--restart=no','--name',NAME,
        '--label','treedb.fixed-cluster.run='+RUN,
        '--label','treedb.fixed-cluster.invocation='+launch_nonce,'--user',APPROVED['driver_uid_gid'],
        '--network=host','--memory=2g','--memory-swap=2g','--cpus=2',
        '--entrypoint','/treedb-query-under-write',
        '-v',root+'/node-c/config.json:/config.json:ro',
        '-v',root+'/node-c/credentials:/credentials:ro',
        '-v',root+'/bootstrap-qualify.json:/bootstrap.json:ro',
        '-v',INPUT_ROOT+':/recall:ro','-v',GATE_DIR+':'+MOUNT_GATE+':rw',image,
        '-config','/config.json','-bootstrap-receipt','/bootstrap.json',
        '-mode','mixed-window','-mixed-profile','changing-top10','-mixed-originals','58','-mixed-interval','5s','-phase','post-only',
        '-probe-receipt','/recall/probe.jsonl','-dataset','/recall/dataset',
        '-provenance','/recall/provenance.json','-run-id',QUERY_RUN,
        '-read-resource-gate-dir',MOUNT_GATE,'-timeout','420s','-rpc-timeout','3s',
        '-read-concurrency','1','-read-window','300s','-read-warmup','64',
        '-read-max-attempts','65536','-read-output-bytes','134217728']


def json_file(name,value):
    (OUTPUT/name).write_text(json.dumps(value,indent=2)+'\n')



PREDECLARATION = '/tmp/gomap-fixed-next-preflight-20261004/changing-result-trial24-campaign-predeclaration-root-v1.json'
POPULATION_ENCODING = 'id-le32-fp32-le-v1'
POPULATION_LIMIT_KEYS = {'MaxRows','MaxIDBytes','MaxSourceRecordBytes','MaxTotalBytes','MaxInspected'}


def validate_population_limits(l, rows, dimensions):
    assert isinstance(l,dict) and set(l)==POPULATION_LIMIT_KEYS
    assert all(type(v) is int and v>0 for v in l.values())
    assert rows<=l['MaxRows']<=65536 and 1<=l['MaxIDBytes']<=65536
    assert dimensions*4+16<=l['MaxSourceRecordBytes']<=1<<20
    assert l['MaxTotalBytes']<=512<<20 and l['MaxInspected']<=1<<20
    return l


def fp32(value):
    assert type(value) in (int,float) and math.isfinite(value)
    value=struct.unpack('<f',struct.pack('<f',value))[0]
    assert math.isfinite(value)
    return value


def checked_vector(values, dimensions):
    assert isinstance(values,(list,tuple)) and len(values)==dimensions
    v=tuple(fp32(x) for x in values)
    assert sum(float(x)*float(x) for x in v)>0, 'zero cosine vector'
    return v


def decoded_id(encoded):
    assert isinstance(encoded,str)
    raw=base64.b64decode(encoded,validate=True)
    assert 0<len(raw)<=1024
    return raw.decode('utf-8',errors='strict')


def population_identity(vectors, dimensions):
    assert isinstance(vectors,dict) and len(vectors)<=65536
    h=hashlib.sha256();hashed=0
    for id in sorted(vectors,key=lambda s:s.encode('utf-8')):
        raw=id.encode('utf-8');assert 0<len(raw)<=1024
        v=checked_vector(vectors[id],dimensions)
        word=struct.pack('<I',len(raw))+raw+struct.pack('<%df'%dimensions,*v)
        h.update(word);hashed+=len(word)
    return {'Rows':len(vectors),'Dimensions':dimensions,'Encoding':POPULATION_ENCODING,
            'SHA256':h.hexdigest(),'HashedBytes':hashed}


def build_initial_population(files, bootstrap, baseline):
    # Independent frozen FP32 input, fixture anchors and actual original growth
    # request form the initial oracle. No prefix/report digest supplies vectors.
    m=strict_json(files['dataset/manifest.json'])
    assert m['docs']==10000 and m['dimensions']==128 and m['document_id_pattern']=='doc-%06d'
    raw=files['dataset/documents.f32'];ident=m['files']['documents.f32']
    assert len(raw)==10000*128*4==ident['bytes']
    assert hashlib.sha256(raw).hexdigest()==ident['sha256']
    vectors={'doc-%06d'%i:checked_vector(v,128) for i,v in enumerate(struct.iter_unpack('<128f',raw))}
    def anchor(id,x,y):
        assert id not in vectors;vectors[id]=checked_vector([x,y]+[0]*126,128)
    for id,x,y in [('seed-x',1,0),('seed-minus-x',-1,0),('seed-minus-y',0,-1)]:anchor(id,x,y)
    anchor(bootstrap['Insert']['VisibleID'],0,1)
    events=[strict_json(line) for line in files['probe.jsonl'].splitlines() if line.strip()]
    planned=[e['Report'] for e in events if e.get('Event')=='planned']
    results=[e['Report'] for e in events if e.get('Event')=='result']
    assert len(planned)==len(results)==1
    result=results[0];assert result['Verdict']=='ACCEPT_BOUNDED_FUNCTIONAL_CLIENT_OVERLAP' and result['Error']==''
    assert result['Counts']['Failed']==result['Counts']['Canceled']==result['Counts']['Unknown']==0
    writes=[op for op in result['Operations'] if op['Kind']=='insert']
    assert len(writes)==2 and writes[0]['Phase']=='concurrent' and writes[1]['Phase']=='explicit-retry'
    original,retry=writes
    assert original['Outcome']==retry['Outcome']=='succeeded'
    id=decoded_id(original['InsertRequest']['ID']);assert id not in vectors
    assert original['InsertResponse']['VisibleID']==retry['InsertResponse']['VisibleID']==id
    assert original['InsertRequest']['Vector']==retry['InsertRequest']['Vector']
    assert 0<original['InsertResponse']['CommitIndex']<retry['InsertResponse']['CommitIndex']==baseline['HighestCommitIndex']
    vectors[id]=checked_vector(original['InsertRequest']['Vector'],128)
    identity=population_identity(vectors,128)
    assert identity['Rows']==baseline['PopulationRows']==10000+baseline['AnchorRows']
    assert identity['SHA256']==baseline['PopulationSHA256']
    return vectors


def apply_originals(initial, writes):
    assert len(writes)==58
    out=dict(initial);previous=0;keys=set()
    for ordinal,w in enumerate(writes):
        assert w['Ordinal']==ordinal and w['Invoked'] is True and w['Outcome']=='succeeded'
        kind,req=operation_request(w)
        id=decoded_id(req['ID']);assert id in out and id.startswith('doc-')
        key=base64.b64decode(req['IdempotencyKey'],validate=True);assert key and key not in keys;keys.add(key)
        response=w['Response'];assert response['ProductionConsensus'] is True
        assert previous<response['CommitIndex']<=response['AppliedIndex'];previous=response['CommitIndex']
        assert response['Matched']==(1 if kind=='replace' else 0)
        if kind=='replace':
            assert response['Modified']==1 and response['Deleted']==0
            vector=checked_vector(req['Vector'],128)
            document=strict_json(base64.b64decode(req['Document'],validate=True))
            assert struct.pack('<128f',*checked_vector(document['embedding'],128))==struct.pack('<128f',*vector)
            out[id]=vector
        else:
            assert response['Deleted']==1 and response['Modified']==0
            del out[id]
    return out


def population_plan(vectors, floor, known_plan=None):
    assert type(floor) is int and floor>0
    identity=population_identity(vectors,128)
    l={k:APPROVED['population_limits'][k] for k in ('MaxRows','MaxIDBytes','MaxSourceRecordBytes','MaxTotalBytes','MaxInspected')}
    validate_population_limits(l,identity['Rows'],128)
    expectation={k:identity[k] for k in ('Rows','Dimensions','SHA256')};expectation['Limits']=l
    if known_plan is None:
        p={'Version':1,'RunID':QUERY_RUN+'initial','HighestNewCommitIndex':0,
           'RequiredAppliedIndex':floor,'Writes':[],'Final':[]}
    else:
        p=copy.deepcopy(known_plan)
        assert p['RunID']==QUERY_RUN and len(p['Writes'])==58 and len(p['Final'])==4
        assert p['RequiredAppliedIndex']==floor>=p['HighestNewCommitIndex']>0
        assert 'Population' not in p
    p['Population']=expectation
    assert len(encode_go_json(p))<=512<<10
    return p


def go_fp32_json(value):
    # Plan floats are the public []float32 vector fields. Find the shortest
    # round-tripping significant decimal, then apply encoding/json's FP32
    # exponent thresholds. This is confined to audit plan bindings; it is NOT
    # used as a replacement for Go's final attempt byte accounting.
    v=fp32(value);bits=struct.pack('<f',v)
    if v==0:return '-0' if math.copysign(1,v)<0 else '0'
    for precision in range(1,10):
        token=format(v,'.%dg'%precision)
        if struct.pack('<f',float(token))==bits:break
    else:raise AssertionError('no FP32 roundtrip')
    d=decimal.Decimal(token)
    if abs(v)<fp32(1e-6) or abs(v)>=fp32(1e21):
        mantissa,exponent=format(d.normalize(),'e').split('e')
        return mantissa+'e'+('+' if int(exponent)>=0 else '-')+str(abs(int(exponent)))
    return format(d,'f')


def encode_go_json(value):
    # Struct field order is retained from the Go report or built explicitly.
    # No unordered maps are present in an audit plan. Match HTML/line escapes.
    if isinstance(value,dict):return b'{'+b','.join(encode_go_json(k)+b':'+encode_go_json(v) for k,v in value.items())+b'}'
    if isinstance(value,list):return b'['+b','.join(encode_go_json(v) for v in value)+b']'
    if type(value) is float:return go_fp32_json(value).encode()
    raw=json.dumps(value,ensure_ascii=False,separators=(',',':'),allow_nan=False)
    return raw.replace('&',r'\u0026').replace('<',r'\u003c').replace('>',r'\u003e').replace('\u2028',r'\u2028').replace('\u2029',r'\u2029').encode()


def validate_population_diagnostic(raw, node, plan, identity):
    assert isinstance(raw,bytes) and 0<len(raw)<=8<<20
    d=strict_json(raw);audit=d['ColocatedAudit']
    assert audit['Version']==1 and audit['NodeID']==node['node']
    assert audit['RunID']==plan['RunID']
    assert audit['PlanSHA256']==hashlib.sha256(encode_go_json(plan)).hexdigest()
    assert audit['AppliedIndex']>=plan['RequiredAppliedIndex'] and audit['AppliedTerm']>0
    proof=audit['Population'];assert isinstance(proof,dict)
    assert set(proof)=={'Rows','Dimensions','Encoding','SHA256','SourceRecordBytes','HashedBytes','AssetBytes','TotalBytes','Inspected'}
    for k,v in identity.items():assert proof[k]==v,k
    assert all(type(proof[k]) is int and proof[k]>=0 for k in ('SourceRecordBytes','HashedBytes','AssetBytes','TotalBytes','Inspected'))
    l=plan['Population']['Limits']
    assert proof['Rows']<=l['MaxRows'] and proof['Inspected']<=l['MaxInspected'] and proof['TotalBytes']<=l['MaxTotalBytes']
    assert proof['TotalBytes']>=proof['SourceRecordBytes']+proof['HashedBytes']+proof['AssetBytes']
    assert len(audit.get('Witnesses') or [])==len(plan['Writes']) and len(audit.get('Final') or [])==len(plan['Final'])
    if plan['Writes']:assert audit['RetainedCount']==len(plan['Writes'])
    return d


def population_audits(phase, plan):
    assert phase in ('initial','final')
    identity=population_identity(initial_vectors if phase=='initial' else apply_originals(initial_vectors,audit_originals(plan['Writes'])),128)
    # Raw plans, request hashes, stdout/stderr and liveness brackets are retained
    # before the first stop. Any refusal is consumed; no diagnostic retry loop.
    raw=encode_go_json(plan);(OUTPUT/(phase+'-population-plan.json')).write_bytes(raw+b'\n')
    records=[]
    for n in NODES:
        before=sample_all();samples.extend(before)
        assert all(t['inspect']['State']['Running'] for s in before for t in s['targets'])
        response=call(phase+'-population-'+n['node'],['docker','exec','-i',n['cid'],APPROVED['server_cli_path'],
            '-mode','diagnostics','-config','/config.json','-expected-binary-sha256',n['server_sha256'],
            '-colocated-audit-plan','/dev/stdin'],host='mikers@'+n['host'],timeout=120,input_bytes=raw+b'\n')
        decoded=validate_population_diagnostic(response.encode('utf-8',errors='surrogateescape'),n,plan,identity)
        after=sample_all();samples.extend(after)
        assert all(t['inspect']['State']['Running'] for s in after for t in s['targets'])
        records.append({'node':n['node'],'cid':n['cid'],'before_samples':before,'after_samples':after,'diagnostics':decoded})
        json_file(phase+'-population-audits.json',{'state':'PARTIAL_NOT_ACCEPTED','observations':records,'oracle':identity})
        json_file('resource-samples.json',samples)
    assert len(records)==4
    json_file(phase+'-population-audits.json',{'state':'ALL4_SOURCE_POPULATION_OBSERVED_PENDING_ROOT_ACCEPTANCE','observations':records,'oracle':identity,
        'scope':'four separate current-FSM local observations at actual applied positions; not global snapshot or live ANN equality'})


def validate_finalization(a,pinned):
    # Fresh real bindings, not historical candidate attestation, before SSH.
    pre=strict_json(pinned[a['receipts']['predeclaration']])
    assert a['receipts']['predeclaration']==PREDECLARATION
    assert pre['campaign']==RUN and pre['runtime_started'] is False
    assert pre['topology']['voters']==4 and pre['topology']['voters_per_host']==2
    assert pre['workload']['profile']=='changing-top10' and pre['workload']['duration_seconds']==300
    assert pre['source_head'] in (None,a['source_head']) and pre['source_tree'] in (None,a['source_tree'])
    landed=strict_json(pinned[a['receipts']['landed_source']])
    assert landed['state']=='LANDED_SOURCE_TREE_VERIFIED'
    assert landed['runtime_head']==a['source_head'] and landed['runtime_tree']==a['source_tree']
    assert landed['source_inventory_sha256']==a['source_inventory_sha256']
    assert set(landed['issues'])=={'5021'}
    for issue in landed['issues'].values():
        assert issue['merged'] is True and digest_valid(issue['landed_commit'],40)
    assert isinstance(landed['runtime_blobs'],dict) and len(landed['runtime_blobs'])>=8
    assert all(input_name(k) and digest_valid(v,40) for k,v in landed['runtime_blobs'].items())
    source=strict_json(pinned[a['receipts']['source_inventory']])
    blobs={r['path']:r['git_blob'] for r in source['rows']}
    required={'cmd/treedb-query-under-write/main.go','cmd/treedb-query-under-write/mixed_v1.go',
        'cmd/treedb-query-under-write/recall_v1.go','cmd/treedb-query-under-write/window_v1.go',
        'cmd/treedb-fixed-peer/main.go','TreeDB/collections/vector_source_population_proof_v1.go',
        'TreeDB/nativewire/colocated_audit_v1.go','TreeDB/nativewire/peer_request_admission_v1.go'}
    assert required<=set(landed['runtime_blobs'])
    assert all(blobs[k]==v for k,v in landed['runtime_blobs'].items())
    review=strict_json(pinned[a['receipts']['collector_prereview']])
    assert review['decision']=='ACCEPT' and review['collector_sha256']==a['collector_sha256']
    assert review['source_head']==a['source_head'] and review['source_tree']==a['source_tree']
    assert review['findings']==[]
    assert review['source_inventory_sha256']==a['source_inventory_sha256']
    assert review['predeclaration_sha256']==a['local_pins'][PREDECLARATION]
    auth=strict_json(pinned[a['receipts']['run_authorization']])
    assert auth['RunID']==RUN and auth['state']=='AUTHORIZED_SINGLE_FRESH_CAMPAIGN'
    assert auth['source_head']==a['source_head'] and auth['source_tree']==a['source_tree']
    assert auth['collector_sha256']==a['collector_sha256']
    assert all(auth[k] is True for k in ('all_cleanup_io_stopped','no_other_campaign_or_writer','fresh_owned_stores_verified'))
    # Operator attestation, not an invented independent process detector.


def validate_bootstrap_prefix(bootstrap, highest):
    original,retry=bootstrap['Insert'],bootstrap['Retry']
    assert original['VisibleID']==retry['VisibleID']
    assert 0<original['CommitIndex']<retry['CommitIndex']<=highest
    for value in (original,retry):assert value['ProductionConsensus'] is True and value['AppliedIndex']>=value['CommitIndex']


def compatible_prefixes(response, prefixes, query_index, lower, upper):
    assert type(lower) is int and type(upper) is int and 0<=lower<=upper<len(prefixes)<=64
    neighbors=response['Neighbors'];assert len(neighbors)==10
    ids=[n['ID'] for n in neighbors];assert len(set(ids))==10
    scores=[fp32(n['Score']) for n in neighbors]
    assert all((-scores[i-1],ids[i-1].encode())<(-scores[i],ids[i].encode()) for i in range(1,10))
    c=response['Counters'];assert c['SelectedPartitions']>0 and c['HNSWServedPartitions']==c['SelectedPartitions']
    assert c['ExactScanPartitions']==c['Retries']==c['Redirects']==0 and c['ReadProofs']>0
    mask=0;recalls=[]
    for prefix in range(lower,upper+1):
        p=prefixes[prefix];assert p['Prefix']==prefix
        changed=p['Changed'][query_index];assert len(changed)==4 and len({v['ID'] for v in changed})==4
        valid=True
        for n,score in zip(neighbors,scores):
            for v in changed:
                if v['ID']==n['ID'] and (not v['Present'] or v['ScoreBits']!=struct.unpack('<I',struct.pack('<f',score))[0]):valid=False
        if valid:
            truth=p['Truth'][query_index];assert len(truth)==10 and len({n['ID'] for n in truth})==10
            mask|=1<<prefix;recalls.append(len(set(ids)&{v['ID'] for v in truth})/10)
    assert mask, 'whole response has no single compatible causal prefix'
    return mask,min(recalls)


def validate_causal_report(report, initial):
    count=original_count(report);assert count==58
    prefixes=report['Prefixes'];assert len(prefixes)==count+1
    states=[dict(initial)]
    for i in range(1,count+1):states.append(apply_prefix(initial,report['Writes'][:i]))
    for i,p in enumerate(prefixes):
        identity=population_identity(states[i],128)
        assert p['Prefix']==i and p['PopulationRows']==identity['Rows'] and p['PopulationSHA256']==identity['SHA256']
        assert len(p['Truth'])==len(p['Changed'])==16
    assert any({n['ID'] for n in prefixes[0]['Truth'][qi]}!={n['ID'] for n in p['Truth'][qi]}
               for p in prefixes[1:] for qi in range(16)), 'no top10 membership changed'
    for i,w in enumerate(report['Writes']):
        assert w['StartNS']>0 and w['EndNS']>=w['StartNS']
        if i:assert w['StartNS']>=report['Writes'][i-1]['EndNS'] and w['StartNS']>=report['Writes'][i-1]['StartNS']+5000000000
    reads=report['ReadPrefixes'];attempts=report['Attempts']
    measured=[a for a in attempts if a['Phase']=='measured'];assert len(reads)==len(measured)
    by_ordinal={p['Ordinal']:p for p in reads};assert len(by_ordinal)==len(reads)
    query_attempts=[0]*16;query_success=[0]*16;sum_recall=0.0
    for a in attempts:
        assert a['Outcome']=='succeeded' and a['Response'] is not None
        assert a['Ordinal']>=0 and a['Worker']==0 and a['StartNS']<=a['EndNS']
        qi=a['Ordinal']%16;assert a['QueryID']==report['Admission']['Queries'][qi]['QueryID']
        assert a['Response']['Generation']==report['Admission']['Generation']
        if a['Phase']=='measured':
            lower=max([i+1 for i,w in enumerate(report['Writes']) if w['Outcome']=='succeeded' and w['EndNS']<=a['StartNS']]+[0])
            upper=max([i+1 for i,w in enumerate(report['Writes']) if w['Invoked'] and w['StartNS']<=a['EndNS']]+[0])
            mask,recall=compatible_prefixes(a['Response'],prefixes,qi,lower,upper)
            proof=by_ordinal[a['Ordinal']]
            assert proof['Lower']==lower and proof['Upper']==upper and uint64_mask(proof['CompatibleMask'])==mask
            assert proof['Matched']==min(i for i in range(count+1) if mask&(1<<i))
            assert proof['RecallAt10']==a['RecallAt10']==recall
            query_attempts[qi]+=1;query_success[qi]+=1;sum_recall+=recall
        else:
            mask,recall=compatible_prefixes(a['Response'],prefixes,qi,0,0)
            assert mask==1 and a['RecallAt10']==recall
    assert query_attempts==report['MeasuredQueryAttempts'] and query_success==report['MeasuredQuerySucceeded']
    assert report['MeanRecallAt10']==sum_recall/len(measured)
    assert len({(a['Phase'],a['Ordinal']) for a in attempts})==len(attempts)
    assert [a['Ordinal'] for a in measured]==list(range(len(measured)))
    assert all(a['Phase'] in ('warmup','measured') for a in attempts)
    for retry,ordinal in zip(report['Retries'],(0,2)):
        original=report['Writes'][ordinal]
        assert retry['Ordinal']==ordinal and retry['Kind']==original['Kind']
        field='Replace' if original['Kind']=='replace' else 'Delete'
        assert retry[field]==original[field] and retry['LogicalSHA256']==original['LogicalSHA256']
        for key in ('Generation','OwnerGroup','CommitTerm','CommitIndex','Coverage','LiveRevision','Matched','Modified','Deleted','VisibilityToken'):
            assert retry['Response'][key]==original['Response'][key],key
        assert retry['Response']['ProductionConsensus'] is True
        assert retry['Response']['AppliedIndex']>=original['Response']['CommitIndex']
    assert report['Counts']['Attempted']==len(measured)
    assert report['WarmupCounts']['Attempted']==len(attempts)-len(measured)
    # Final scalar accounting is checked by root using literal Go encoding:
    # Python shortest-float formatting is not Go encoding/json for FP32 scores.
    assert 0<report['RetainedAttemptBytes']<=report['OutputBytes']


def apply_prefix(initial,writes):
    out=dict(initial)
    for w in writes:
        kind,req=operation_request(w)
        if kind=='replace':out[decoded_id(req['ID'])]=checked_vector(req['Vector'],128)
        else:del out[decoded_id(req['ID'])]
    return out


def operation_request(w):
    # mixedWrite and ColocatedAuditWriteV1 omit the inactive pointer. Preserve
    # that canonical wire shape instead of inventing a null inactive operation.
    assert isinstance(w,dict)
    active=[field for field in ('Replace','Delete') if field in w]
    assert len(active)==1, 'exactly one omitted-field operation required'
    field=active[0];req=w[field];assert isinstance(req,dict)
    kind=field.lower()
    if 'Kind' in w:assert w['Kind']==kind, 'operation kind/pointer mismatch'
    return kind,req


def original_requests(writes):
    assert isinstance(writes,list)
    requests=[]
    for w in writes:
        kind,req=operation_request(w)
        assert w['Kind']==kind
        assert type(w['Ordinal']) is int and w['Ordinal']>=0
        requests.append({'Ordinal':w['Ordinal'],'Kind':kind,kind.capitalize():req})
    return requests


def audit_originals(writes):
    # Audit-ledger rows contain no Ordinal/Kind/Invoked/Outcome; add only those
    # collector observations, preserving the actual active request and response.
    assert isinstance(writes,list)
    return [dict(w,Ordinal=i,Kind=operation_request(w)[0],Invoked=True,Outcome='succeeded')
            for i,w in enumerate(writes)]


def validate_prefix_oracles(oracle,a,initial):
    # Root independently computes canonical FP32 scores/top10 before execution.
    # This constructor binds that frozen authority; it does not fake exported truth.
    assert oracle['state']=='INDEPENDENT_CANONICAL_PREFIX_ORACLES_VERIFIED'
    assert oracle['RunID']==QUERY_RUN and oracle['Profile']=='changing-top10'
    assert oracle['source_head']==a['source_head'] and oracle['source_tree']==a['source_tree']
    assert oracle['InitialPopulationSHA256']==population_identity(initial,128)['SHA256']
    writes=oracle['OriginalRequests'];prefixes=oracle['Prefixes']
    assert len(writes)==58 and len(prefixes)==59
    assert writes==original_requests(writes), 'frozen OriginalRequests must use canonical omitted inactive fields'
    sustained_originals(writes)
    assert all(w['Ordinal']==i and w['Kind'] in ('replace','delete') for i,w in enumerate(writes))
    assert any({n['ID'] for n in prefixes[0]['Truth'][q]}!={n['ID'] for n in p['Truth'][q]}
               for p in prefixes[1:] for q in range(16))
    for i,p in enumerate(prefixes):
        identity=population_identity(apply_prefix(initial,writes[:i]),128)
        assert p['Prefix']==i and p['PopulationRows']==identity['Rows'] and p['PopulationSHA256']==identity['SHA256']
        assert len(p['Truth'])==len(p['Changed'])==16
        for truth,changed in zip(p['Truth'],p['Changed']):
            assert len(truth)==10 and len({n['ID'] for n in truth})==10
            assert all(math.isfinite(fp32(n['Score'])) for n in truth)
            assert len(changed)==4 and len({n['ID'] for n in changed})==4


def manifest_template():
    # No hashes, CIDs, images or source pins are fabricated. All activation flags
    # are false. This cannot pass validate_manifest or produce external activity.
    return {'state':'PROVISIONAL_CONSTRUCTOR_NOT_EXECUTABLE','RunID':RUN,'source_head':None,'source_tree':None,
            'collector_sha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
            'server_cli_path':'/treedb-fixed-peer',
            'required_receipts':list(RECEIPTS),'activation_flags':dict.fromkeys(FLAGS,False),
            'population_limits':{'MaxRows':16384,'MaxIDBytes':1024,'MaxSourceRecordBytes':1048576,
              'MaxTotalBytes':536870912,'MaxInspected':1048576},
            'integration_owner':'root; final landed source/build/blobs, images/CIDs, accepted probe/full oracle and independent collector prereview are required'}


def self_check():
    # Fabricated inputs only; this does not establish product/runtime behavior.
    checks=[]
    def refuses(name,fn):
        try:fn()
        except (AssertionError,KeyError,ValueError,TypeError,OverflowError):checks.append(name);return
        raise AssertionError('synthetic refusal missed: '+name)
    assert encode_go_json([1.0,-0.0,.0000001,.000001,1e21])==b'[1,-0,1e-7,0.000001,1e+21]'
    checks.append('audit_FP32_JSON_roundtrip_and_thresholds')
    identity=population_identity({'b':[1,-0.0],'a':[0,1]},2)
    raw=struct.pack('<I',1)+b'a'+struct.pack('<2f',0,1)+struct.pack('<I',1)+b'b'+struct.pack('<2f',1,-0.0)
    assert identity['SHA256']==hashlib.sha256(raw).hexdigest();checks.append('raw_ID_sort_FP32_signedzero')
    assert identity['SHA256']!=population_identity({'b':[1,0.0],'a':[0,1]},2)['SHA256']
    refuses('nonfinite_vector',lambda:population_identity({'a':[float('nan'),1]},2))
    refuses('placeholder_manifest',lambda:validate_manifest(manifest_template(),'a'*64))
    refuses('duplicate_JSON',lambda:strict_json('{"x":1,"x":2}'))
    refuses('nonfinite_JSON',lambda:strict_json('{"x":NaN}'))
    c={'SelectedPartitions':1,'HNSWServedPartitions':1,'ExactScanPartitions':0,'Retries':0,'Redirects':0,'ReadProofs':1}
    neighbors=[{'ID':'n%02d'%i,'Score':1-i/16} for i in range(10)]
    truth=list(neighbors);other=list(neighbors[:9])+[{'ID':'other','Score':0}]
    changed=[{'ID':'c%d'%i,'Present':True,'ScoreBits':0} for i in range(4)]
    prefixes=[{'Prefix':i,'Changed':[changed],'Truth':[truth if i==0 else other]} for i in range(2)]
    response={'Neighbors':neighbors,'Counters':c}
    mask,recall=compatible_prefixes(response,prefixes,0,0,1)
    assert mask==3 and recall==.9;checks.append('all_compatible_prefixes_minimum_not_favorable')
    assert compatible_prefixes(response,prefixes,0,1,1)==(2,.9);checks.append('narrowed_bounds')
    hybrid=copy.deepcopy(prefixes)
    for i,p in enumerate(hybrid):
        p['Changed'][0][0]={'ID':'n00','Present':True,'ScoreBits':0x3f800000 if i==0 else 0}
        p['Changed'][0][1]={'ID':'n01','Present':True,'ScoreBits':0x3f700000 if i==1 else 0}
    refuses('no_single_prefix_hybrid',lambda:compatible_prefixes(response,hybrid,0,0,1))
    bad=copy.deepcopy(response);bad['Counters']['ExactScanPartitions']=1
    refuses('exact_fallback',lambda:compatible_prefixes(bad,prefixes,0,0,1))
    bad=copy.deepcopy(response);bad['Neighbors'][1]['ID']=bad['Neighbors'][0]['ID']
    refuses('duplicate_response',lambda:compatible_prefixes(bad,prefixes,0,0,1))
    plan={'Version':1,'RunID':'synthetic','HighestNewCommitIndex':0,'RequiredAppliedIndex':7,'Writes':[],'Final':[],
          'Population':{'Rows':identity['Rows'],'Dimensions':2,'SHA256':identity['SHA256'],'Limits':
          {'MaxRows':4,'MaxIDBytes':16,'MaxSourceRecordBytes':64,'MaxTotalBytes':2048,'MaxInspected':32}}}
    proof=dict(identity,SourceRecordBytes=48,AssetBytes=0,TotalBytes=identity['HashedBytes']+48,Inspected=2)
    diag={'ColocatedAudit':{'Version':1,'NodeID':'synthetic','RunID':'synthetic',
        'PlanSHA256':hashlib.sha256(encode_go_json(plan)).hexdigest(),'AppliedIndex':7,'AppliedTerm':1,
        'Witnesses':[],'Final':[],'Population':proof}}
    validate_population_diagnostic(encode_go_json(diag),{'node':'synthetic'},plan,identity);checks.append('population_decode_binding_and_accounting')
    bad=copy.deepcopy(diag);bad['ColocatedAudit']['PlanSHA256']='f'*64
    refuses('population_plan_binding',lambda:validate_population_diagnostic(encode_go_json(bad),{'node':'synthetic'},plan,identity))
    bad=copy.deepcopy(diag);bad['ColocatedAudit']['NodeID']='other'
    refuses('population_node_binding',lambda:validate_population_diagnostic(encode_go_json(bad),{'node':'synthetic'},plan,identity))
    bad=copy.deepcopy(diag);bad['ColocatedAudit']['Population']['SHA256']='f'*64
    refuses('population_digest_mismatch',lambda:validate_population_diagnostic(encode_go_json(bad),{'node':'synthetic'},plan,identity))
    bad=copy.deepcopy(diag);bad['ColocatedAudit']['Population']['TotalBytes']=4096
    refuses('population_total_cap',lambda:validate_population_diagnostic(encode_go_json(bad),{'node':'synthetic'},plan,identity))
    bad=copy.deepcopy(diag);bad['ColocatedAudit']['AppliedIndex']=6
    refuses('population_applied_floor',lambda:validate_population_diagnostic(encode_go_json(bad),{'node':'synthetic'},plan,identity))
    # Literal Go-compatible JSON, fabricated locally from declared struct order.
    # This exercises ingestion, not a preconstructed Python -0.0 shortcut.
    literal=b'{"Version":1,"RunID":"synthetic-go-zero","HighestNewCommitIndex":0,"RequiredAppliedIndex":7,"Writes":[{"Replace":{"Version":1,"Generation":{"Index":"synthetic","Generation":2},"IdempotencyKey":"aw==","ID":"YQ==","Vector":[1,-0],"Document":"e30=","Deadline":"0001-01-01T00:00:00Z"},"Response":{"CommitIndex":7,"AppliedIndex":7}}],"Final":[]}'
    decoded=strict_json(literal);vector=decoded['Writes'][0]['Replace']['Vector']
    assert type(decoded['Version']) is int and type(decoded['RequiredAppliedIndex']) is int
    assert type(decoded['HighestNewCommitIndex']) is int and decoded['HighestNewCommitIndex']==0
    assert type(vector[1]) is float and struct.pack('<f',vector[1])==b'\x00\x00\x00\x80'
    actual=population_identity({'a':vector},2)
    expected=struct.pack('<I',1)+b'a'+struct.pack('<II',0x3f800000,0x80000000)
    assert actual['SHA256']==hashlib.sha256(expected).hexdigest()
    assert actual['SHA256']!=population_identity({'a':[1,0]},2)['SHA256']
    assert encode_go_json(decoded)==literal
    assert hashlib.sha256(encode_go_json(decoded)).hexdigest()==hashlib.sha256(literal).hexdigest()
    checks.append('literal_Go_JSON_decode_FP32_population_and_plan_hash_signedzero')
    integer_tokens=strict_json('[0,1,-1,65536]');assert all(type(v) is int for v in integer_tokens)
    checks.append('ordinary_protocol_integer_tokens_preserved')
    # Actual mixedWrite/ColocatedAuditWriteV1 omitempty shapes: no inactive key.
    initial={'doc-%06d'%i:checked_vector(v+[0]*126,128) for i,v in enumerate(([0,1],[1,0],[-1,0],[0,-1]))}
    pattern=[('replace',0,[1,-0.0]),('replace',0,[-1,-0.0]),('delete',1,None),('replace',2,[0,1]),('delete',2,None),('delete',3,None)]+[('replace',0,[1,-0.0] if i%2==0 else [-1,-0.0]) for i in range(6,58)]
    writes=[]
    for i,(kind,index,xy) in enumerate(pattern):
        req={'Version':1,'Generation':{'Index':'synthetic','Generation':2},
             'IdempotencyKey':base64.b64encode(('key-%d'%i).encode()).decode(),
             'ID':base64.b64encode(('doc-%06d'%index).encode()).decode()}
        if kind=='replace':
            req['Vector']=xy+[0]*126
            req['Document']=base64.b64encode(encode_go_json({'embedding':req['Vector']})).decode()
        req['Deadline']='0001-01-01T00:00:00Z'
        response={'ProductionConsensus':True,'CommitIndex':10+i,'AppliedIndex':10+i,
                  'Matched':int(kind=='replace'),'Modified':int(kind=='replace'),'Deleted':int(kind=='delete')}
        row={'Ordinal':i,'Kind':kind,kind.capitalize():req,'Response':response,'Invoked':True,'Outcome':'succeeded'}
        writes.append(strict_json(encode_go_json(row)))
    requests=original_requests(writes)
    assert all(set(w)=={'Ordinal','Kind',w['Kind'].capitalize()} for w in requests)
    assert all(requests[i][w['Kind'].capitalize()] is w[w['Kind'].capitalize()] for i,w in enumerate(writes))
    final=apply_originals(initial,writes)
    assert population_identity(final,128)==population_identity(apply_prefix(initial,requests),128)
    audit_rows=[{w['Kind'].capitalize():w[w['Kind'].capitalize()],'Response':w['Response']} for w in writes]
    adapted=audit_originals(strict_json(encode_go_json(audit_rows)))
    assert encode_go_json(original_requests(adapted))==encode_go_json(requests)
    assert population_identity(apply_originals(initial,adapted),128)==population_identity(final,128)
    checks.append('real_omitempty_six_mixed_and_audit_ledgers_population_pipeline')
    normalized_roundtrip=strict_json(encode_go_json(requests))
    assert encode_go_json(normalized_roundtrip)==encode_go_json(requests)
    checks.append('frozen_original_requests_omitted_fields_and_signedzero_binding')
    for name,bad in (
        ('missing_operation',{'Ordinal':0,'Kind':'replace'}),
        ('missing_operation_kind',{'Ordinal':0,'Replace':writes[0]['Replace']}),
        ('dual_operation',dict(writes[0],Delete={'ID':'YQ=='})),
        ('null_active_operation',{'Ordinal':0,'Kind':'replace','Replace':None}),
        ('null_inactive_operation',dict(writes[0],Delete=None)),
        ('kind_pointer_mismatch',dict(writes[0],Kind='delete'))):
        refuses(name,lambda bad=bad:original_requests([bad]))
    bad=dict(writes[0],Delete=None)
    refuses('invalid_audit_operation',lambda:audit_originals([bad]))
    refuses('invalid_prefix_operation',lambda:apply_prefix(initial,[bad]))
    bad_writes=copy.deepcopy(writes);bad_writes[0]['Delete']=None
    refuses('invalid_final_population_operation',lambda:apply_originals(initial,bad_writes))
    return {'state':'PASS_SYNTHETIC_CONSTRUCTOR_SELF_CHECK_ONLY','checks':checks,
            'runtime_started':False,'network_calls':0,'limits':'no source qualification, runtime/performance proof, independent review or campaign acceptance'}


def main():
    global APPROVED,root,image,NODES,serial,last_record,deadline,paths
    global gate_expected,gate_identity,gate_nonce,boundary_evidence,samples,launch_nonce,initial_vectors,prefix_oracles
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--approved')
    parser.add_argument('--self-check',action='store_true')
    parser.add_argument('--manifest-template',action='store_true')
    cli=parser.parse_args()
    if cli.self_check or cli.manifest_template:
        assert not cli.approved and not (cli.self_check and cli.manifest_template)
        print(json.dumps(self_check() if cli.self_check else manifest_template(),indent=2))
        return 0
    assert cli.approved, 'only root may execute with fully activated --approved manifest'
    source=pathlib.Path(__file__).read_bytes()
    approved_bytes=read_bounded(cli.approved,1<<20)
    APPROVED=validate_manifest(strict_json(approved_bytes),hashlib.sha256(source).hexdigest())
    pinned,inv,NODES,initial_vectors=prepare_local(APPROVED)  # ALL local pins before any SSH/process
    prefix_oracles=strict_json(pinned[APPROVED['receipts']['prefix_oracles']])
    assert not OUTPUT.exists(), 'consumed campaign output must never be reused'
    OUTPUT.mkdir()
    (OUTPUT/'approved-inputs.json').write_bytes(approved_bytes)
    (OUTPUT/'collector-source.py').write_bytes(source)
    (OUTPUT/'input-inventory.json').write_bytes(pinned[LOCAL_INPUT_ROOT+'/input-inventory.json'])
    pin_dir=OUTPUT/'pins';pin_dir.mkdir()
    retained={}
    for i,(path,raw) in enumerate(pinned.items()):
        name='%03d.bin'%i
        (pin_dir/name).write_bytes(raw)
        retained[path]={'sha256':hashlib.sha256(raw).hexdigest(),'retained_path':'pins/'+name}
    json_file('retained-input-pins.json',retained)
    root,image=ROOT,APPROVED['query_image']
    serial,last_record,deadline=0,None,None
    paths={};gate_expected={};gate_identity=None;gate_nonce=None;boundary_evidence=[]
    samples=[];errors=[];cid=None;final=None;stdout='';outcomes=None
    launch_nonce=secrets.token_hex(16);launch_attempted=False
    workload_passed=False;resource_complete=False;audits_before_stop=False
    initial_population_passed=False;final_population_passed=False
    def interrupt(signum,frame):
        raise KeyboardInterrupt('collector interrupted; consumed run, no retry')
    signal.signal(signal.SIGTERM,interrupt)
    args=driver_arguments()
    json_file('command.json',args)
    try:
        # Current exact ownership survives any subsequent plan/probe/launch refusal.
        for n in NODES:
            x=inspect_voter(n,'initial-'+n['node'])
            baseline=strict_json(pinned[n['inspect_path']])[0]
            assert x['State']['Running']==baseline['State']['Running']
            assert x['State']['StartedAt']==baseline['State']['StartedAt']
            assert x['State']['Running'] or x['State']['ExitCode']==0
        verify="import pathlib,json,hashlib; p=pathlib.Path("+repr(INPUT_ROOT)+"); b=(p/'input-inventory.json').read_bytes(); assert hashlib.sha256(b).hexdigest()=="+repr(APPROVED['input_inventory_sha256'])+"; i=json.loads(b); assert i=="+repr(inv)+"; assert all(hashlib.sha256((p/f).read_bytes()).hexdigest()==h for f,h in i.items()); print('PASS_EXACT_FRESH_INPUT_BUNDLE')"
        call('verify-input-bundle',['python3','-c',verify])
        for filename,key in (('bootstrap-qualify.json','bootstrap_sha256'),('node-c/config.json','config_sha256'),('plan.json','plan_sha256')):
            raw=call('snapshot-'+key,['cat',root+'/'+filename]).encode('utf-8',errors='surrogateescape')
            assert hashlib.sha256(raw).hexdigest()==APPROVED[key],key
            (OUTPUT/filename.replace('/','-')).write_bytes(raw)
        plan=strict_json((OUTPUT/'plan.json').read_bytes())
        assert plan['driver_node']=='node-c' and len(plan['nodes'])==4
        assert {v['node'] for v in plan['nodes']}==set(HOSTS)
        for n in NODES:
            v=next(v for v in plan['nodes'] if v['node']==n['node'])
            assert all(v[k]==n[k] for k in ('host','image','node','root','name'))
            assert plan['binary_sha256']==n['server_sha256']
        bootstrap=strict_json((OUTPUT/'bootstrap-qualify.json').read_bytes())
        assert bootstrap['Dataset']['Rows']==10000 and bootstrap['Dataset']['Dimensions']==128
        assert bootstrap['Insert']['ProductionConsensus'] and bootstrap['Retry']['ProductionConsensus']
        validate_bootstrap_prefix(bootstrap,APPROVED['baseline']['HighestCommitIndex'])
        assert bootstrap['Insert']['VisibleID'].startswith(RUN+'/dataset-')
        assert {v['NodeID'] for v in bootstrap['Readiness']}==set(HOSTS)
        assert all(v['Ready'] and v['Live'] and not v['Draining'] and v['VectorPhase']=='active' for v in bootstrap['Readiness'])
        # Verify the approved query image's actual ELF with a stopped retained container.
        hash_name=NAME+'-hash'
        hash_cid=call('hash-create',['docker','create','--pull=never','--name',hash_name,
            '--memory=2g','--memory-swap=2g','--cpus=2','--entrypoint','/treedb-query-under-write',image]).strip()
        assert digest_valid(hash_cid)
        hash_path=GATE+'-driver-elf'
        call('hash-path-exclusive',['python3','-c','import pathlib; assert not pathlib.Path('+repr(hash_path)+').exists()'])
        call('hash-copy',['docker','cp',hash_cid+':/treedb-query-under-write',hash_path])
        assert call('driver-hash',['sha256sum',hash_path]).split()[0]==APPROVED['driver_sha256']
        x=json.loads(call('hash-inspect',['docker','inspect',hash_cid]))[0]
        assert x['Id']==hash_cid and x['Image']==image and x['Name']=='/'+hash_name and not x['State']['Running']
        call('query-name-exclusive',['python3','-c',"import subprocess; r=subprocess.run(['docker','inspect',"+repr(NAME)+"],capture_output=True,text=True); assert r.returncode!=0 and 'no such' in r.stderr.lower(), (r.returncode,r.stderr)"])
        account=call('gate-account',['python3','-c',"import os; print('%d:%d'%(os.getuid(),os.getgid()))"]).strip()
        assert account==APPROVED['driver_uid_gid']
        create_gate="import pathlib,json,os; p=pathlib.Path("+repr(GATE)+"); p.mkdir(mode=0o700); q=p/'gate'; q.mkdir(mode=0o700); st=os.lstat(q); assert not list(q.iterdir()); print(json.dumps({'path':str(q),'directory_identity':[st.st_dev,st.st_ino]}))"
        gate_creation=json.loads(call('gate-create-exclusive',['python3','-c',create_gate]))
        gate_identity=gate_creation['directory_identity']
        for n in NODES:
            x=inspect_voter(n,'before-start-'+n['node'])
            if not x['State']['Running']:
                call('start-'+n['node'],['docker','start',n['cid']],host='mikers@'+n['host'])
            assert inspect_voter(n,'started-'+n['node'])['State']['Running']
        samples.extend(sample_all())
        json_file('resource-samples.json',samples)
        # Untimed complete-current-source audit AFTER growth and BEFORE window.
        population_audits('initial', population_plan(initial_vectors,APPROVED['baseline']['HighestCommitIndex']))
        initial_population_passed=True
        deadline=time.monotonic()+OUTER_SECONDS
        launch_attempted=True  # immediately before the sole launch; never repeat
        reply=call('launch',args).strip()
        assert digest_valid(reply)
        cid=reply
        boundary_capture('ready',cid)
        boundary_capture('done',cid)
        while True:
            batch=sample_all(cid);samples.extend(batch)
            json_file('resource-samples.json',samples)
            client=next(t for s in batch for t in s['targets'] if t['role']=='client')
            if not client['inspect']['State']['Running']:
                break
            time.sleep(min(.5,max(0,deadline-time.monotonic())))
    except BaseException as e:
        errors.append(repr(e))
    finally:
        deadline=None  # independent bounded cleanup, no workload retry
        try:
            snapshot=json.loads(call('gate-final-snapshot',['python3','-c',GATE_SNAPSHOT,GATE_DIR]))
            json_file('gate-final-snapshot.json',snapshot)
            for name,value in snapshot.get('tokens',{}).items():
                if 'raw_b64' in value:
                    (OUTPUT/('gate-final-'+name)).write_bytes(base64.b64decode(value['raw_b64'],validate=True))
        except BaseException as e:
            errors.append('gate retention: '+repr(e))
        try:
            target=driver_probe_target(cid,launch_attempted)
            if target is not None:
                probe="import subprocess; r=subprocess.run(['docker','inspect',"+repr(target)+"],capture_output=True,text=True); assert r.returncode==0 or 'no such' in r.stderr.lower(); print(r.stdout if r.returncode==0 else 'null')"
                inspected=json.loads(call('driver-final-inspect',['python3','-c',probe]))
                if inspected is not None:
                    assert isinstance(inspected,list) and len(inspected)==1
                    final=inspected[0];owned(final,cid)
                    cid=final['Id']  # pin recovered exact CID only after full nonce/ownership proof
                    if final['State']['Running']:
                        call('driver-failstop',['docker','stop','--time','10',final['Id']])
                    final=json.loads(call('driver-stopped',['docker','inspect',final['Id']]))[0];owned(final,cid)
                    assert not final['State']['Running']
                    stdout=call('driver-logs',['docker','logs',final['Id']])
                    (OUTPUT/'stdout.jsonl').write_bytes(stdout.encode('utf-8',errors='surrogateescape'))
                    (OUTPUT/'stderr.log').write_bytes(last_record['stderr'].encode('utf-8',errors='surrogateescape'))
                    batch=sample_all(final['Id']);samples.extend(batch)
                    json_file('resource-samples.json',samples)
                    assert len(stdout.encode('utf-8',errors='surrogateescape'))<=134217728
                    reports=[strict_json(line) for line in stdout.splitlines() if line.strip()]
                    planned=[v['Report'] for v in reports if v.get('Event')=='planned']
                    results=[v['Report'] for v in reports if v.get('Event')=='result']
                    assert len(planned)==len(results)==1
                    report=results[0]
                    # This is BEFORE the first voter stop, while final samples prove
                    # all four live owned voters. Failed rows remain raw, never retried.
                    outcomes=validate_mixed_report(planned[0],report)
                    # Writer has joined; retries and known-ID audits succeeded.
                    # Independently apply original requests to the retained full input.
                    final_vectors=apply_originals(initial_vectors,report['Writes'])
                    final_identity=population_identity(final_vectors,128)
                    assert final_identity['Rows']==report['Prefixes'][-1]['PopulationRows']
                    assert final_identity['SHA256']==report['Prefixes'][-1]['PopulationSHA256']
                    final_plan=population_plan(final_vectors,report['RequiredAppliedIndex'],report['AuditPlan'])
                    population_audits('final',final_plan)
                    final_population_passed=True
                    audits_before_stop=True
                    json_file('audits-before-voter-stop.json',{'observed_unix':time.time(),
                        'stdout_sha256':hashlib.sha256(stdout.encode('utf-8',errors='surrogateescape')).hexdigest(),
                        'AuditPlan':report['AuditPlan'],'Audits':report['Audits'],
                        'scope':'derived driver attachments before ANY voter stop; independent authority verification pending root'})
                    workload_passed=not errors and final['State']['ExitCode']==0 and not final['State']['OOMKilled']
                    start=datetime.datetime.fromisoformat(report['MeasuredOriginUTC'].replace('Z','+00:00')).timestamp()
                    end=start+report['ActualDurationNS']/1e9
                    brackets=measurement_resource_brackets(samples,start,end)
                    resource_complete=all(v['status']=='available' for v in brackets.values())
                    json_file('measurement-resource-brackets.json',{'measured_origin_unix':start,'measured_end_unix':end,
                        'brackets':brackets,'clock_limit':'observed offset intervals assume wall clocks do not step; no clock-step monitor retained'})
                    if not resource_complete:
                        errors.append('incomplete actual field-level resource brackets; native observations retained separately')
        except BaseException as e:
            errors.append('driver retention/report observation: '+repr(e))
        json_file('inspect.json',final)
        # Exact manifest-owned CIDs only. An ownership mismatch refuses that stop;
        # never replace name/CID, recreate, delete stores, restart, or retry workload.
        stop_receipts=[]
        for n in NODES:
            try:
                x=inspect_voter(n,'stop-inspect-'+n['node'])
                if x['State']['Running']:
                    call('stop-'+n['node'],['docker','stop','--time','30',n['cid']],host='mikers@'+n['host'],timeout=45)
                x=inspect_voter(n,'final-'+n['node'])
                assert not x['State']['Running'] and x['State']['ExitCode']==0
                stop_receipts.append({'node':n['node'],'cid':n['cid'],'observed_unix':time.time(),'inspect':x})
            except BaseException as e:
                errors.append(n['node']+' stop: '+repr(e))
        json_file('voter-stop-receipts.json',stop_receipts)
    passed=workload_passed and resource_complete and audits_before_stop and initial_population_passed and final_population_passed and not errors
    summary=dict(status='OBSERVATIONS_PENDING_ROOT_INDEPENDENT_VALIDATION' if passed else 'FAILED_OR_UNKNOWN_CONSUMED',
        native_window_passed_pending_root=workload_passed,resource_brackets_complete=resource_complete,
        all_four_audits_observed_before_voter_stop=audits_before_stop,
        all_four_initial_population_passed=initial_population_passed,all_four_final_population_passed=final_population_passed,outcomes=outcomes,errors=errors,
        driver_exit=final['State']['ExitCode'] if final else None,oom_killed=final['State']['OOMKilled'] if final else None,
        consumed=True,stores_preserved=True,collector_deadline_seconds=OUTER_SECONDS,
        network_scope='host network: per-container byte counters unsupported; no host totals substituted',
        peak_scope='memory.peak/VmHWM include setup/warmup/drain; no measured-only or whole-lifetime peak claim',
        root_action='independently verify canonical declared-prefix/causal/ledger/original-retry/initial-final-population/audit/resource/source/clean-stop evidence; never rerun failed/UNKNOWN window')
    json_file('exit.json',summary)
    print(json.dumps(summary))
    return 0 if passed else 1




# Generic schema scalars are parsed without binary64 conversion.
def original_count(report):
    n=report.get('Originals',0)
    assert type(n) is int and (n==0 or 6<=n<=63)
    return n or 6
def uint64_mask(n):
    assert type(n) is int and 0<n<1<<64
    return n
def sustained_originals(writes):
    assert len(writes)==58
    assert [w['Ordinal'] for w in writes]==list(range(58)) and all(type(w['Ordinal']) is int for w in writes)
    assert [w['Kind'] for w in writes[:6]]==['replace','replace','delete','replace','delete','delete']
    ids=[operation_request(w)[1]['ID'] for w in writes[:6]]
    assert ids[0]==ids[1] and ids[3]==ids[4] and len({ids[i] for i in (0,2,3,5)})==4
    keys=set()
    for w in writes:
        kind,req=operation_request(w)
        key=base64.b64decode(req['IdempotencyKey'],validate=True)
        assert key and key not in keys;keys.add(key)
    a=writes[0]['Replace']['ID'];v=[writes[i]['Replace']['Vector'] for i in (0,1)]
    assert [struct.pack('<f',x) for x in v[0]]!=[struct.pack('<f',x) for x in v[1]]
    for i,w in enumerate(writes[6:],6):
        kind,req=operation_request(w)
        assert kind=='replace' and req['ID']==a and len(req['Vector'])==128
        assert [struct.pack('<f',x) for x in req['Vector']]==[struct.pack('<f',x) for x in v[i%2]]

def sustained_shape(report,planned=False):
    assert original_count(report)==58
    writes=report['Writes'];prefixes=report['Prefixes']
    assert len(writes)==58 and len(prefixes)==59
    sustained_originals(writes)
    assert [p['Prefix'] for p in prefixes]==list(range(59))
    assert all(type(p['Prefix']) is int and type(p['PopulationRows']) is int and len(p['Truth'])==len(p['Changed'])==16 for p in prefixes)
    assert report['Concurrency']==1 and report['WarmupPlanned']==64 and report['MaxAttempts']==65536 and report['OutputBytes']==134217728
    assert report['RequestedDuration']==300000000000 and report['PaceInterval']==5000000000 and report['Truncated'] is False
    for i,w in enumerate(writes):
        assert type(w['Ordinal']) is int and w['Ordinal']==i and w['IntendedOffsetNS']==i*5000000000
        assert w['Outcome']==('unissued' if planned else 'succeeded') and w['Invoked'] is (not planned)
    if planned:
        assert report['Retries'] in (None,[]) and report['Attempts'] in (None,[]) and report['ReadPrefixes'] in (None,[])
    else:
        assert len(report['Retries'])==2 and [w['Ordinal'] for w in report['Retries']]==[0,2]
        assert all(type(w['Ordinal']) is int and w['Outcome']=='succeeded' and w['Invoked'] is True for w in report['Retries'])
        for p in report['ReadPrefixes']:uint64_mask(p['CompatibleMask'])

if __name__=='__main__':
    sys.exit(main())
