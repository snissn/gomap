if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import sys
sys.dont_write_bytecode=True
from source_paths import source_path, W
"""Pure bounded resource/gate/ownership accounting; never whole-campaign acceptance.
No subprocess/network calls. Historical top-level is never imported/executed.
verifier(..., permission_path=..., permission_sha256=..., collector_exit_path=..., collector_exit_sha256=...)
requires retained raw evidence, not a collector status as authority.
"""
import ast, base64, copy, datetime, hashlib, importlib.util, json, math, re, shlex, sys
from pathlib import Path
CORE=source_path('/tmp/gomap-4994-mixed-window-artifact-verify-root-v7.py')
CORE_SHA='2af4fdcf0a78a1607ddaa199ef18e26d6fa69b04c4b10fa0917496cb1c2083f5'
OLD=source_path('/tmp/gomap-4975-trial13c1-paced-window-artifact-verify-root-v1.py')
OLD_SHA='60eaf880f98ab35a1886b01f9f2dd89738c927a2bdcc25dfd52444353fe7fcc9'
COLLECTOR=source_path('/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24mixedchangingc1-collector-root-v1.py')
COLLECTOR_SHA='e8c7e4fb3fbfab3c1001c7a86f36b36b6e89caddc50c90a755fdc795a6a4c9e5'
RUN=W.campaign; QUERY_RUN=W.query_run
C=Path(W.output)
ROOT='/home/mikers/gomap-4250-twohost-'+RUN
INPUT_ROOT='/home/mikers/gomap-4997-4998-'+RUN+'-inputs-root-v1'
GATE=W.gate;GATE_DIR=GATE+'/gate'
NAME=W.name;MOUNT_GATE='/run-resource-gate'
HOSTS={'node-a':'192.168.0.111','node-b':'192.168.0.111','node-c':'192.168.0.185','node-d':'192.168.0.185'}
FIELDS=('memory.current','memory.peak','memory.max','memory.events','memory.swap.current','memory.swap.max','memory.swap.events','cpu.max','cpu.stat','io.stat','process.status')
PROVENANCE={}
def pinned(path,digest):
 raw=Path(path).read_bytes()
 if hashlib.sha256(raw).hexdigest()!=digest:raise ValueError('source pin '+str(path))
 return raw
spec=importlib.util.spec_from_file_location('mixed_resource_core_v4',CORE)
pinned(CORE,CORE_SHA);core=importlib.util.module_from_spec(spec);spec.loader.exec_module(core)
need,sha,strict,bounded,stamp=core.need,core.sha,core.strict,core.bounded,core.stamp

def source_parts(path,digest):
 raw=pinned(path,digest);s=raw.decode();t=ast.parse(s)
 funcs={n.name:ast.get_source_segment(s,n) for n in t.body if isinstance(n,ast.FunctionDef)}
 const={}
 for n in t.body:
  if isinstance(n,ast.Assign):
   try:v=ast.literal_eval(n.value)
   except (ValueError,TypeError):continue
   for k in n.targets:
    if isinstance(k,ast.Name):const[k.id]=v
 return funcs,const
CF,CONSTANTS=source_parts(COLLECTOR,COLLECTOR_SHA)
OF,_=source_parts(OLD,OLD_SHA)

def pure_collector(a,nonce):
 ns=dict(W=W,re=re,json=json,datetime=datetime,TOKEN_BYTES=2048,ROOT=ROOT,RUN=RUN,QUERY_RUN=QUERY_RUN,NAME=NAME,
   INPUT_ROOT=INPUT_ROOT,GATE_DIR=GATE_DIR,MOUNT_GATE=MOUNT_GATE,
   APPROVED=a,root=ROOT,image=a['query_image'],launch_nonce=nonce)
 allow=('digest_valid','expected_binds','validate_voter','owned','driver_arguments','validate_gate_receipt','validate_ack_binding')
 for k in allow:
  PROVENANCE[k]={'file':COLLECTOR,'source_sha256':sha(CF[k].encode())}
 exec(compile('\n\n'.join(CF[k] for k in allow),'pinned-pure-ownership','exec'),ns)
 return ns

def ack_observation_order(proof,ack,db,batch,phase):
 # installed_unix is captured after atomic rename and reread. Go may consume
 # the exact ACK earlier; only the prior livepoll bounds consumption in time.
 prior=proof['remote_finished_unix'];post=ack['installed_unix'];consumed=stamp(db['AcknowledgedUTC'])/1e9
 need(finite(prior) and finite(post) and phase+'.ack' not in proof['files'],'finite prior poll without phase ACK')
 ends=[v['finished_unix'] for s in batch if s['host']=='192.168.0.185' for t in s['targets'] for v in t['fields'].values()]
 need(ends and all(finite(x) and x<=prior and x<=consumed for x in ends),'same-host boundary fields precede livepoll and Go ACK')
 need(prior<=consumed and prior<=post,'prior livepoll before Go ACK and post-install observation')

def gate(C,r,samples,ssh,driver,a):
 # Reuse the old gate semantic helper with frozen constants, separate
 # permission validation, and the available ACK timing observation contract.
 s=OF['verify_gate'];start=s.index(' constants={}');end=s.index(' prior_serial=0',start)
 s=s[:start]+' constants=CONSTANTS\n'+s[end:]
 start=s.index(' permission=Path(')
 s=s[:start]+" return dict(directory_identity=identity,nonce=nonce,tokens_sha256={k:sha(v) for k,v in tokens.items()},boundaries=gate_details)\n"
 old_order="need(ack['installed_unix']<=stamp(db['AcknowledgedUTC'])/1e9,'Go acknowledged after installation')"
 need(s.count(old_order)==1,'one historical post-install timestamp assumption')
 s=s.replace(old_order,'ack_observation_order(proof,ack,db,batch,phase)')
 PROVENANCE['verify_gate']={'file':OLD,'source_sha256':sha(OF['verify_gate'].encode()),'adapted_source_sha256':sha(s.encode()),'changes':'direct frozen collector constants; separate permission validation; ACK consumption bound by actual prior livepoll, not post-install observation'}
 ns=dict(load=lambda p:strict(bounded(p)),need=need,sha=sha,stamp=stamp,json=json,re=re,
   base64=base64,shlex=shlex,CONSTANTS=CONSTANTS,ack_observation_order=ack_observation_order)
 exec(compile(s,'pinned-adapted-gate','exec'),ns)
 return ns['verify_gate'](C,r,samples,ssh,driver,GATE_DIR,a)

def finite(v):return isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v)
def interval(start,end):need(finite(start) and finite(end) and start<=end,'finite ordered clock interval')
def kv(raw):
 out={}
 for line in raw.splitlines():
  k,v=line.split();need(k not in out and re.fullmatch('[0-9]+',v),'counter syntax');out[k]=int(v)
 return out

def field_value(field,raw):
 need(isinstance(raw,str),'raw available field')
 if field.startswith('memory.') and field not in ('memory.events','memory.swap.events'):
  need(re.fullmatch(r'[0-9]+\s*',raw) is not None,'finite memory counter');n=int(raw)
  if field=='memory.max':need(n==2147483648,'actual 2GiB cap')
  if field in ('memory.swap.current','memory.swap.max'):need(n==0,'zero swap')
 elif field=='memory.events':
  z=kv(raw);need({'oom','oom_kill'}<=set(z) and all(z.get(k,0)==0 for k in ('oom','oom_kill','oom_group_kill')),'actual zero OOM events')
 elif field=='memory.swap.events':
  z=kv(raw);need({'high','max','fail'}<=set(z) and all(z[k]==0 for k in ('high','max','fail')),'zero swap events')
 elif field=='cpu.max':
  q=raw.split();need(len(q)==2 and all(re.fullmatch('[0-9]+',x) for x in q) and int(q[1])>0 and int(q[0])==2*int(q[1]),'actual 2CPU quota')
 elif field=='cpu.stat':need({'usage_usec','user_usec','system_usec'}<=set(kv(raw)),'CPU accounting')
 elif field=='io.stat':
  # Empty is an available actual read; nonempty must retain real counters.
  for line in raw.splitlines():
   dev,*values=line.split();need(re.fullmatch('[0-9]+:[0-9]+',dev) and values,'IO device')
   need(all(re.fullmatch('[a-z_]+=[0-9]+',v) for v in values),'IO counters')
 elif field=='process.status':
  for k in ('VmRSS','VmHWM'):
   need(len(re.findall(r'^'+k+r':\s+[0-9]+\s+kB$',raw,re.M))==1,'actual raw process '+k)
 else:raise ValueError('unknown resource field')

def enclosing(entries,start,end):
 before=[v for v in entries if v[1]<=start];after=[v for v in entries if v[0]>=end]
 need(before and after,'missing enclosing actual resource read')
 left=max(before,key=lambda v:v[1]);right=min(after,key=lambda v:v[0])
 return dict(status='available',before_sample_index=left[2],after_sample_index=right[2],
   before_started_driver_clock_lower=left[0],before_finished_driver_clock_upper=left[1],
   after_started_driver_clock_lower=right[0],after_finished_driver_clock_upper=right[1],
   pre_overscan_seconds_range=[start-left[1],start-left[0]],post_overscan_seconds_range=[right[0]-end,right[1]-end])

PERMISSION_PATH_KEYS=('inspect_path','command_path','stdout_path','stderr_path','launch_path','inspect_receipt_path','logs_path','exit_path')

def bind_report(path,p,r,ptext,text):
 pp,rr,pt,rt,digest=core.raw_report(path)
 need(pp==p and rr==r and pt==ptext and rt==text,'actual retained planned/result Report callback binding')
 need(len([v for v in bounded(path).splitlines() if v.strip()])==2,'exact two raw Event wrappers')
 return digest

def permission_proof(pr,permission_raw,a):
 need(pr['NetworkMode']=='none' and pr['DBMounts']==[] and pr['CredentialsReadable'] is True and pr['DriverSHA256']==a['driver_sha256'] and pr['Image']==a['query_image'] and pr['User']==a['driver_uid_gid'] and pr['RunID']==RUN and pr['MutationInvocations']==0,'isolated permission claim')
 need(isinstance(pr.get('raw_evidence'),dict) and pr['raw_evidence'],'raw permission evidence')
 # This small module cannot infer credential construction from generic receipt shapes.
 # Root must provide explicit bounded raw inspection, launch argv and planned stdout.
 need(set(PERMISSION_PATH_KEYS)<=set(pr),'permission raw contract missing')
 px=strict(permission_raw[pr['inspect_path']]);need(isinstance(px,list) and len(px)==1,'one raw isolated inspect');px=px[0]
 pc=strict(permission_raw[pr['command_path']]);ps=permission_raw[pr['stdout_path']]
 expected_name=NAME+('-trial24-checkpoint-window-v1' if W.issue=='5021' and a.get('receipts',{}).get('predeclaration','/tmp/gomap-fixed-next-preflight-20261004/changing-result-trial24-campaign-predeclaration-root-v1.json')!='/tmp/gomap-fixed-next-preflight-20261004/changing-result-trial24-campaign-predeclaration-root-v1.json' else '')+'-isolated-permission'
 need(pc.count('--name')==1 and pc[pc.index('--name')+1]==expected_name and px['Name']=='/'+expected_name,'exact isolated phase name')
 need(px['Image']==a['query_image'] and px['HostConfig']['NetworkMode']=='none' and px['Config']['User']==a['driver_uid_gid'],'raw isolated image/network/user')
 binds=px['HostConfig']['Binds'];need(isinstance(binds,list) and len(binds)==len(set(binds)),'unique isolated mounts')
 required={ROOT+'/node-c/config.json:/config.json:ro',ROOT+'/node-c/credentials:/credentials:ro',ROOT+'/bootstrap-qualify.json:/bootstrap.json:ro'}
 need(required<=set(binds),'actual config/bootstrap/credential mounts')
 for b in binds:
  need(b.endswith(':ro') and len(b.split(':'))==3,'isolated readonly mount')
  source,dest,_=b.split(':');need(dest in ('/config.json','/credentials','/bootstrap.json','/recall') and (source+':'+dest+':ro' in required or source==INPUT_ROOT and dest=='/recall'),'no DB/runtime/gate mounts')
 need(isinstance(pc,list) and all(isinstance(v,str) for v in pc) and pc[:2]==['docker','run'] and pc.count(a['query_image'])==1 and pc==pr['ApprovedArgv'],'one full root-approved isolated argv')
 def option(flag):
  values=[]
  for i,v in enumerate(pc):
   if v==flag:need(i+1<len(pc),'option operand');values.append(pc[i+1])
   elif v.startswith(flag+'='):values.append(v[len(flag)+1:])
  need(len(values)==1,'one isolated option '+flag);return values[0]
 need(option('--network')=='none' and option('--user')==a['driver_uid_gid'] and option('--entrypoint')=='/treedb-query-under-write','isolated actual nonroot/no-network ELF invocation')
 need(option('-config')=='/config.json' and option('-bootstrap-receipt')=='/bootstrap.json' and option('-run-id')==QUERY_RUN and option('-phase')=='post-only' and option('-mode')=='read-window','actual bounded planned driver mode')
 need(pc.count('-v')==len(binds) and {pc[i+1] for i,v in enumerate(pc) if v=='-v'}==set(binds),'raw argv/inspect exact mounts')
 need(px['State']['Running'] is False and px['State']['ExitCode']!=0 and px['State']['OOMKilled'] is False,'actual isolated expected readiness refusal')
 need(sha(ps)==pr['PlannedSHA256'],'raw permission planned SHA')
 ev=[strict(line) for line in ps.splitlines() if line.strip()];need(len(ev)==2 and [e['Event'] for e in ev]==['planned','result'],'complete planned/failed offline output')
 for e in ev:
  z=e['Report'];need(z['Kind']=='fixed_cluster_read_window_v1' and z.get('Attempts') in (None,[]) and z['ActualDurationNS']==z['Completions']==z['WarmupCompletions']==0 and z['MeasuredOriginUTC']=='0001-01-01T00:00:00Z' and not z.get('Writes') and not z.get('Retries'),'offline read-only zero attempts')
  for k in ('Counts','WarmupCounts'):
   ct=z[k];need(set(ct)=={'Planned','Attempted','Succeeded','Failed','Canceled','Unknown','Unissued'} and all(type(v)is int and v>=0 for v in ct.values()) and ct['Planned']==ct['Unissued'] and all(ct[n]==0 for n in ('Attempted','Succeeded','Failed','Canceled','Unknown')),'offline complete unissued population')
 need(ev[1]['Report']['Verdict']=='FAILED' and ev[1]['Report']['Error'] and ev[1]['Report']['Admission']['RunID']==QUERY_RUN and ev[1]['Report']['Admission']['BinarySHA256']==a['driver_sha256'],'actual failed read-only receipt')
 need(ev[0]['Report']['Admission']['RunID']==QUERY_RUN and ev[0]['Report']['Admission']['BinarySHA256']==a['driver_sha256'],'actual admitted ELF constructor planned event')

 image_at=pc.index(a['query_image']);docker_args=pc[2:image_at];app_args=pc[image_at+1:]
 need('--mount' not in docker_args and '--volumes-from' not in docker_args and not any(v.startswith(('--mount=','--volumes-from=')) for v in docker_args),'no structured/inherited launch mounts')
 # No unknown Docker switch or extra positional operand may alter execution.
 allowed={'--pull','--restart','--name','--label','--user','--network','--memory','--memory-swap','--cpus','--entrypoint','-v'}
 i=0
 while i<len(docker_args):
  v=docker_args[i]
  if v=='-d':i+=1;continue
  flag=v.split('=',1)[0];need(flag in allowed,'bounded Docker preflight option')
  if '=' not in v:need(i+1<len(docker_args),'Docker option operand');i+=1
  i+=1
 need(docker_args.count('-d')==1,'detached isolated launch CID')
 need(px['Path']=='/treedb-query-under-write' and px['Args']==app_args and px['Config']['Entrypoint']==['/treedb-query-under-write'] and px['Config']['Cmd']==app_args,'actual inspected Path/Args/Entrypoint/Cmd match approved argv')
 h=px['HostConfig'];need(h.get('Mounts') in (None,[]) and h.get('VolumesFrom') in (None,[]),'no structured/inherited inspected mounts')
 effective=px['Mounts'];need(isinstance(effective,list) and len(effective)==len(binds),'exact effective readonly mount count')
 expected={(b.split(':')[0],b.split(':')[1]) for b in binds};seen=set()
 for m in effective:
  pair=(m['Source'],m['Destination']);need(pair in expected and pair not in seen and m['Type']=='bind' and m['RW'] is False and m['Mode']=='ro','only exact effective readonly binds');seen.add(pair)
 need(seen==expected,'all permitted mounts effective, no DB/anonymous volume')
 def observation(key,args):
  rec=strict(permission_raw[pr[key]]);interval(rec['started_unix'],rec['finished_unix']);av=rec['argv']
  need(rec['exit_code']==0 and not rec.get('timed_out') and len(av)==7 and av[:6]==['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','mikers@192.168.0.185'] and shlex.split(av[-1])==args,'raw isolated '+key+' command')
  need(isinstance(rec['stdout'],str) and isinstance(rec['stderr'],str),'raw observation stdout/stderr');return rec
 launch=observation('launch_path',pc);cid=launch['stdout'].strip();need(core.digest(cid) and cid==px['Id'],'actual isolated returned/inspected CID')
 inspection=observation('inspect_receipt_path',['docker','inspect',cid]);need(inspection['stdout'].encode('utf-8','surrogateescape')==permission_raw[pr['inspect_path']],'raw inspect bytes for launched CID')
 logs=observation('logs_path',['docker','logs',cid]);need(logs['stdout'].encode('utf-8','surrogateescape')==ps and logs['stderr'].encode('utf-8','surrogateescape')==permission_raw[pr['stderr_path']],'raw same-CID planned-only logs and stderr')
 exited=observation('exit_path',['docker','wait',cid]);need(exited['stdout'].strip()==str(px['State']['ExitCode']),'actual same-CID stopped exit')
 need(launch['finished_unix']<=exited['started_unix'] and exited['finished_unix']<=inspection['started_unix'] and inspection['finished_unix']<=logs['started_unix'],'launch wait inspect logs observation order')
 return dict(cid=cid,planned_sha256=sha(ps),exit_code=px['State']['ExitCode'])


def verifier(a,p,r,ptext,text,states,*,permission_path,permission_sha256,collector_exit_path,collector_exit_sha256):
 """Partial gate only. All callback inputs are independently validated elsewhere.
 Requires exact raw collected output and separate root preflight/process-exit pins.
 """
 load=lambda p:strict(bounded(p))
 need(a['RunID']==RUN and a['collector_sha256']==COLLECTOR_SHA,'fixed approved campaign')
 need(bounded(C/'collector-source.py')==pinned(COLLECTOR,COLLECTOR_SHA),'actual collector source')
 need(load(C/'approved-inputs.json')==a,'exact approved manifest')
 stdout_sha256=bind_report(C/'stdout.jsonl',p,r,ptext,text)
 retained=load(C/'retained-input-pins.json')
 def pin_input(path,digest):
  need(a['local_pins'].get(str(path))==digest,'declared local pin')
  z=retained[str(path)];need(z['sha256']==digest,'retained pin digest');core.path_safe(z['retained_path'])
  raw=bounded(C/z['retained_path']);need(sha(raw)==digest,'actual retained local input');return raw
 pr=strict(pin_input(permission_path,permission_sha256))
 permission_raw={path:pin_input(path,digest) for path,digest in pr['raw_evidence'].items()}
 permission=permission_proof(pr,permission_raw,a)
 # Process exit is externally retained and hash-pinned, distinct from exit.json.
 ce=bounded(collector_exit_path,64);need(ce.strip()==b'0','actual collector process exit0')
 need(core.digest(collector_exit_sha256) and sha(ce)==collector_exit_sha256,'external collector exit raw pin')
 ssh=[]
 for f in sorted(C.glob('[0-9][0-9][0-9][0-9]-*.json')):
  x=load(f);interval(x['started_unix'],x['finished_unix']);need(x['exit_code']==0 and not x.get('timed_out'),'raw SSH successful/no timeout '+f.name)
  argv=x['argv'];need(len(argv)==7 and argv[:5]==['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10'] and argv[5] in ('mikers@192.168.0.111','mikers@192.168.0.185'),'exact serial SSH transport argv')
  need(bounded(f.with_suffix('.stdout'))==x['stdout'].encode('utf-8','surrogateescape') and bounded(f.with_suffix('.stderr'))==x['stderr'].encode('utf-8','surrogateescape'),'raw SSH stdout/stderr retained')
  ssh.append((f,x))
 need(ssh and [int(f.name[:4]) for f,x in ssh]==list(range(1,len(ssh)+1)),'complete serial ledger')
 for (_,left),(_,right) in zip(ssh,ssh[1:]):need(left['finished_unix']<=right['started_unix'],'serial SSH order')
 def one(suffix,args=None,host=None):
  rows=[(f,x) for f,x in ssh if f.name[5:]==suffix+'.json'];need(len(rows)==1,'unique raw '+suffix)
  f,x=rows[0]
  if args is not None:need(shlex.split(x['argv'][-1])==args,'exact raw command '+suffix)
  if host is not None:need(x['argv'][5]=='mikers@'+host,'actual command host')
  return f,x
 launchf,launch=one('launch',host='192.168.0.185');cmd=load(C/'command.json');nonce=[v.split('=',1)[1] for v in cmd if v.startswith('treedb.fixed-cluster.invocation=')]
 need(len(nonce)==1 and re.fullmatch('[0-9a-f]{32}',nonce[0]),'fresh invocation nonce')
 ns=pure_collector(a,nonce[0]);need(cmd==ns['driver_arguments']() and shlex.split(launch['argv'][-1])==cmd,'one full exact launch argv')
 need(sum(shlex.split(x['argv'][-1])[:2]==['docker','run'] for f,x in ssh)==1,'one workload launch')
 driver=load(C/'inspect.json');cid=launch['stdout'].strip();need(core.digest(cid) and driver['Id']==cid,'actual returned launch CID');ns['owned'](driver,cid)
 need(not driver['State']['Running'] and driver['State']['ExitCode']==0 and not driver['State']['OOMKilled'],'driver clean0')
 _,stopped=one('driver-stopped',['docker','inspect',cid]);need(strict(stopped['stdout'])==[driver],'raw clean driver inspect')
 _,logs=one('driver-logs',['docker','logs',cid]);need(logs['stdout'].encode('utf-8','surrogateescape')==bounded(C/'stdout.jsonl') and logs['stderr'].encode('utf-8','surrogateescape')==bounded(C/'stderr.log'),'exact CID raw logs')
 events=[strict(v) for v in bounded(C/'stdout.jsonl').splitlines() if v.strip()];need(len(events)==2 and [v['Event'] for v in events]==['planned','result'] and events[0]['Report']==p and events[1]['Report']==r,'exact two retained events')
 _,dh=one('driver-hash',['sha256sum',GATE+'-driver-elf']);need(dh['stdout'].split()==[a['driver_sha256'],GATE+'-driver-elf'],'actual image ELF hash')
 nodes={n['node']:dict(n,name='treedb-4250-'+RUN+'-'+n['node']) for n in a['nodes']}
 def owner(x,role):
  need(x['State']['OOMKilled'] is False,'every sampled/owned role no OOM')
  if role=='client':ns['owned'](x,cid)
  else:ns['validate_voter'](x,nodes[role])
 for role,n in nodes.items():
  initial=[]
  for label in ('initial','before-start','started','stop-inspect','final'):
   f,x=one(label+'-'+role,['docker','inspect',n['cid']],n['host']);v=strict(x['stdout']);need(len(v)==1,'single owned voter');owner(v[0],role);initial.append(v[0])
  baseline=strict(pin_input(n['inspect_path'],n['inspect_sha256']))[0]
  need(initial[0]['State']['Running']==baseline['State']['Running'] and initial[0]['State']['StartedAt']==baseline['State']['StartedAt'],'fresh initial direct inspect authority')
  need(initial[2]['State']['Running'] and initial[2]['State']['Pid']>0,'started voter live')
  need(not initial[-1]['State']['Running'] and initial[-1]['State']['ExitCode']==0,'four clean stopped0')
  if initial[1]['State']['Running'] is False:one('start-'+role,['docker','start',n['cid']],n['host'])
  if initial[3]['State']['Running']:one('stop-'+role,['docker','stop','--time','30',n['cid']],n['host'])
 samples=load(C/'resource-samples.json');need(samples,'actual samples');paths={};peaks={};hwm={}
 for s in samples:
  interval(s['started_unix'],s['finished_unix']);interval(s['local_started_unix'],s['local_finished_unix'])
  need(s['clock_offset_low']==s['started_unix']-s['local_finished_unix'] and s['clock_offset_high']==s['finished_unix']-s['local_started_unix'],'clock uncertainty formula')
  matches=[x for f,x in ssh if x['started_unix']==s['local_started_unix'] and x['finished_unix']==s['local_finished_unix']];need(len(matches)==1,'raw sampler SSH binding');x=matches[0];raw=strict(x['stdout'])
  need(raw['targets']==s['targets'] and raw['started_unix']==s['started_unix'] and raw['finished_unix']==s['finished_unix'],'actual sampler data')
  args=shlex.split(x['argv'][-1]);need(len(args)==5 and args[:3]==['python3','-c',CONSTANTS['SAMPLER']] and x['argv'][5]=='mikers@'+s['host'] and strict(args[4])==list(FIELDS[:-1]),'exact sampler code/host/fields')
  targets=strict(args[3]);need({t['node'] for t in targets}>= {role for role,host in HOSTS.items() if host==s['host']},'both host voters sampled');need(len(targets)==len(s['targets']) and len({t['node'] for t in targets})==len(targets),'unique sample roles')
  for req,t in zip(targets,s['targets']):
   role=t['node'];need(role in set(HOSTS)|{'client'} and (s['host']=='192.168.0.185' if role=='client' else HOSTS[role]==s['host']),'sample role host')
   need(req=={'name':cid if role=='client' else nodes[role]['cid'],'node':role,'role':'client' if role=='client' else 'voter','previous_path':paths.get(role)},'actual exact sample targets/history')
   need(t['role']==req['role'],'sampler retained role identity');owner(t['inspect'],role);paths[role]=t['cgroup_path'];need(isinstance(paths[role],str) and paths[role].startswith('/') and '..' not in Path(paths[role]).parts,'real cgroup path')
   need(t['network']['status']=='unsupported' and set(t['fields'])==set(FIELDS),'no invented network and exact eleven fields')
   for field,v in t['fields'].items():
    need(v['status'] in ('available','missing') and s['started_unix']<=v['started_unix']<=v['finished_unix']<=s['finished_unix'],'per-field interval')
    if v['status']=='missing':need(bool(v.get('reason')),'missing reason');continue
    field_value(field,v['raw'])
    if field=='memory.peak':peaks[role]=max(peaks.get(role,0),int(v['raw']))
    if field=='process.status':hwm[role]=max(hwm.get(role,0),int(re.search(r'^VmHWM:\s+([0-9]+)\s+kB$',v['raw'],re.M)[1])*1024)
 # Collector uses Python's microsecond timestamp conversion for resource clock
 # bounds; preserve it exactly, while token comparisons retain nanoseconds.
 start=datetime.datetime.fromisoformat(r['MeasuredOriginUTC'].replace('Z','+00:00')).timestamp();end=start+r['ActualDurationNS']/1e9
 resource=load(C/'measurement-resource-brackets.json');need(resource['measured_origin_unix']==start and resource['measured_end_unix']==end,'actual measured interval')
 dlo=min(s['clock_offset_low'] for s in samples if s['host']=='192.168.0.185');dhi=max(s['clock_offset_high'] for s in samples if s['host']=='192.168.0.185')
 for role in (*HOSTS,'client'):
  z=resource['brackets'][role];need(z['status']=='available' and z['network']['status']=='unsupported' and set(z['fields'])==set(FIELDS),'complete role bracket')
  for field in FIELDS:
   entries=[]
   for i,s in enumerate(samples):
    for t in s['targets']:
     if t['node']!=role or t['fields'][field]['status']!='available':continue
     v=t['fields'][field];lo,hi=(0,0) if s['host']=='192.168.0.185' else (dlo-s['clock_offset_high'],dhi-s['clock_offset_low'])
     entries.append((v['started_unix']+lo,v['finished_unix']+hi,i))
   expected=enclosing(entries,start,end);need(all(z['fields'][field][k]==v for k,v in expected.items()),'independent enclosing bracket '+role+'/'+field)
 create_gate="if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')\nimport pathlib,json,os; p=pathlib.Path("+repr(GATE)+"); p.mkdir(mode=0o700); q=p/'gate'; q.mkdir(mode=0o700); st=os.lstat(q); assert not list(q.iterdir()); print(json.dumps({'path':str(q),'directory_identity':[st.st_dev,st.st_ino]}))"
 _,created=one('gate-create-exclusive',['python3','-c',create_gate],host='192.168.0.185')
 identity=strict(created['stdout'])['directory_identity'];need(len(identity)==2 and all(type(v)is int and v>0 for v in identity),'actual created gate device/inode')
 gp=gate(C,r,samples,ssh,driver,a)
 need(gp['directory_identity']==identity,'same created gate identity')
 for phase in ('ready','done'):
  one('gate-ack-'+phase,host='192.168.0.185')
 for f,x in ssh:
  if '-gate-poll-' in f.name:need(x['argv'][5]=='mikers@192.168.0.185' and shlex.split(x['argv'][-1])==['python3','-c',CONSTANTS['GATE_PROBE'],GATE_DIR,cid],'actual poll code/CID')
 _,snapshot=one('gate-final-snapshot',['python3','-c',CONSTANTS['GATE_SNAPSHOT'],GATE_DIR],host='192.168.0.185');need(strict(snapshot['stdout'])==load(C/'gate-final-snapshot.json'),'raw final snapshot')
 # Actual audit acquisition precedes any stop, with all four live post-drain.
 audit=load(C/'audits-before-voter-stop.json');need(audit['stdout_sha256']==sha(bounded(C/'stdout.jsonl')) and audit['AuditPlan']==r['AuditPlan'] and audit['Audits']==r['Audits'] and len(audit['Audits'])==4,'actual four attached audits')
 stops=[x for f,x in ssh if re.fullmatch('[0-9]{4}-stop-node-[abcd][.]json',f.name)]
 need(stops and logs['finished_unix']<=audit['observed_unix']<=min(x['started_unix'] for x in stops),'audit attachments before first voter stop')
 stopped_rows=load(C/'voter-stop-receipts.json');need(len(stopped_rows)==4 and {z['node'] for z in stopped_rows}==set(HOSTS),'exact four stop receipts')
 for z in stopped_rows:
  role=z['node'];_,final=one('final-'+role,['docker','inspect',nodes[role]['cid']],HOSTS[role])
  need(z['cid']==nodes[role]['cid'] and z['inspect']==strict(final['stdout'])[0] and z['observed_unix']>=final['finished_unix'],'raw final clean stop binding')
 post=[s for s in samples if logs['finished_unix']<=s['local_started_unix'] and s['local_finished_unix']<=audit['observed_unix']]
 live=set()
 for s in post:
  for t in s['targets']:
   if t['node'] in HOSTS and t['inspect']['State']['Running'] and t['inspect']['State']['Pid']>0:live.add(t['node'])
 need(live==set(HOSTS),'all four voters live after logs/before audit observation')
 summary=load(C/'exit.json');need(summary['status']=='OBSERVATIONS_PENDING_ROOT_INDEPENDENT_VALIDATION' and summary['errors']==[] and summary['driver_exit']==0 and summary['oom_killed'] is False and summary['consumed'] is True and summary['stores_preserved'] is True and all(summary[k] is True for k in ('native_window_passed_pending_root','resource_brackets_complete','all_four_audits_observed_before_voter_stop')),'honest consumed successful collector disposition')
 return dict(disposition='RESOURCE_GATE_OWNERSHIP_ACCOUNTING_ONLY',campaign_acceptance=False,stdout_sha256=stdout_sha256,permission=permission,gate=gp,observed_memory_peak_bytes=peaks,observed_process_hwm_bytes=hwm,source_pins={CORE:CORE_SHA,OLD:OLD_SHA,COLLECTOR:COLLECTOR_SHA},reused_functions=PROVENANCE,limits=['SSH ledger binds retained command observations; authentication/execution authority remains root-owned.','Audit attachment acquisition while live is proved; canonical documents/current-FSM/WAL authority requires separate audit verifier.','Source/build/bootstrap/TLS permission authority remain root outer gates.','Peaks include setup/warmup/drain/history; no measured-only/whole-lifetime peaks or capacity claim.','All eleven read intervals enclose measurement with overscan; clock translation assumes no wall-clock steps.','ACK installed_unix is a post-install observation; atomic install-to-Go-read causality is bound by frozen source and exact tokens, while timestamps bound prior livepoll/fields, not the exact rename event.','Host-network per-container bytes unsupported; no host totals substituted.'])

def self_check():
 checks=[]
 def good(name,fn):fn();checks.append(name)
 def bad(name,fn):
  try:fn()
  except (ValueError,AssertionError,KeyError,TypeError):checks.append(name);return
  raise ValueError('tamper accepted '+name)
 good('empty available io',lambda:field_value('io.stat',''))
 good('bounded 2CPU',lambda:field_value('cpu.max','200000 100000\n'))
 good('zero swap',lambda:field_value('memory.swap.current','0\n'))
 good('zero oom',lambda:field_value('memory.events','low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n'))
 good('raw RSS HWM',lambda:field_value('process.status','VmRSS:\t20 kB\nVmHWM:\t40 kB\n'))
 for name,field,raw in [('nonzero swap','memory.swap.max','1'),('OOM','memory.events','oom 1\noom_kill 0'),('quota','cpu.max','max 100000'),('cap','memory.max','2147483649'),('missing HWM','process.status','VmRSS: 1 kB'),('nonfinite','memory.current','nan')]:bad(name,lambda f=field,r=raw:field_value(f,r))
 z=enclosing([(1,2,0),(3,4,1),(12,13,2),(15,16,3)],5,10)
 need(z['before_sample_index']==1 and z['after_sample_index']==2 and z['pre_overscan_seconds_range']==[1,2] and z['post_overscan_seconds_range']==[2,3],'independent bracket arithmetic');checks.append('exact enclosing and overscan')
 bad('missing pre bracket',lambda:enclosing([(12,13,0)],5,10))
 bad('NaN clock',lambda:interval(float('nan'),2))
 bad('reverse clock',lambda:interval(2,1))
 a={'query_image':'sha256:'+'1'*64,'driver_uid_gid':'1000:1000'};ns=pure_collector(a,'a'*32)
 cmd=ns['driver_arguments']();need(cmd.count('docker')==1 and '-mode' in cmd and cmd[cmd.index('-mode')+1]=='mixed-window' and 'treedb.fixed-cluster.invocation='+'a'*32 in cmd,'actual pure launch');checks.append('exact pure driver argv')
 x={'Id':'2'*64,'Image':a['query_image'],'Name':'/'+NAME,'Config':{'User':'1000:1000','Labels':{'treedb.fixed-cluster.run':RUN,'treedb.fixed-cluster.invocation':'a'*32}},'HostConfig':{'Memory':2147483648,'MemorySwap':2147483648,'NanoCpus':2000000000,'RestartPolicy':{'Name':'no'},'NetworkMode':'host','Binds':[ROOT+'/node-c/config.json:/config.json:ro',ROOT+'/node-c/credentials:/credentials:ro',ROOT+'/bootstrap-qualify.json:/bootstrap.json:ro',INPUT_ROOT+':/recall:ro',GATE_DIR+':'+MOUNT_GATE+':rw']}}
 good('nonce owned exact CID',lambda:ns['owned'](x,'2'*64))
 for name,edit in [('foreign nonce',lambda z:z['Config']['Labels'].__setitem__('treedb.fixed-cluster.invocation','b'*32)),('foreign CID',lambda z:z.__setitem__('Id','3'*64)),('DB mount injected',lambda z:z['HostConfig']['Binds'].append('/foreign:/data'))]:
  z=copy.deepcopy(x);edit(z);bad(name,lambda z=z:ns['owned'](z,'2'*64))
 token=dict(Version=1,RunID=QUERY_RUN,Phase='ready',Nonce='b'*32,PublishedUTC='2026-10-04T01:00:00Z',MeasuredOriginUTC='0001-01-01T00:00:00Z',ActualDurationNS=0,StopReason='',AcknowledgedUTC='0001-01-01T00:00:00Z',WaitNS=0)
 raw=(json.dumps(token,separators=(',',':'))+'\n').encode()
 good('source pure exact token ACK',lambda:ns['validate_ack_binding'](raw,raw,'ready',QUERY_RUN,'b'*32))
 bad('ACK byte tamper',lambda:ns['validate_ack_binding'](raw,raw+b' ','ready',QUERY_RUN,'b'*32))
 bad('token nonce mismatch',lambda:ns['validate_gate_receipt'](raw,'ready',QUERY_RUN,'c'*32))
 bad('token truncated newline',lambda:ns['validate_gate_receipt'](raw[:-1],'ready',QUERY_RUN,'b'*32))
 bad('token oversized',lambda:ns['validate_gate_receipt'](raw+b' '*2048+b'\n','ready',QUERY_RUN,'b'*32))
 need(re.fullmatch('[0-9]{4}-stop-node-[abcd][.]json','0042-stop-node-a.json') is not None,'shutdown ledger label');checks.append('shutdown raw label pattern')
 # Tiny composition fixture uses the actual pinned core.raw_report, replacing
 # only file reads in memory. No fixture files/runtime/subprocess are created.
 planned={'Example':1};result={'Example':2}
 jsonl=(json.dumps({'Event':'planned','Report':planned},separators=(',',':'))+'\n'+json.dumps({'Event':'result','Report':result},separators=(',',':'))+'\n').encode()
 saved_core=core.bounded;saved_local=globals()['bounded']
 try:
  core.bounded=lambda path:jsonl;globals()['bounded']=lambda path:jsonl
  pp,rr,pt,rt,d=core.raw_report('memory-only')
  good('real core raw_report two-event composition',lambda:need(bind_report('memory-only',pp,rr,pt,rt)==d,'composition SHA'))
  bad('full JSONL substituted for result Report',lambda:bind_report('memory-only',pp,rr,pt,jsonl.decode()))
 finally:core.bounded=saved_core;globals()['bounded']=saved_local
 # Only a tiny authority-binding fixture; not a fabricated campaign receipt.
 a['driver_sha256']='4'*64;pc=ns['driver_arguments']()
 pc[pc.index('--network=host')]='--network=none'
 gate_bind=GATE_DIR+':'+MOUNT_GATE+':rw';j=pc.index(gate_bind);del pc[j-1:j+1]
 j=pc.index('-read-resource-gate-dir');del pc[j:j+2]
 name_at=pc.index('--name')+1;pc[name_at]=NAME+'-isolated-permission'
 pc[pc.index('-mode')+1]='read-window';j=pc.index('-mixed-interval');del pc[j:j+2];j=pc.index('-mixed-profile');del pc[j:j+2];j=pc.index('-mixed-originals');del pc[j:j+2]
 image_at=pc.index(a['query_image']);app=pc[image_at+1:]
 binds=[pc[i+1] for i,v in enumerate(pc) if v=='-v']
 px={'Id':'5'*64,'Name':'/'+NAME+'-isolated-permission','Image':a['query_image'],'Path':'/treedb-query-under-write','Args':app,
  'Config':{'User':a['driver_uid_gid'],'Entrypoint':['/treedb-query-under-write'],'Cmd':app},
  'HostConfig':{'NetworkMode':'none','Binds':binds,'Mounts':[],'VolumesFrom':[]},
  'Mounts':[{'Source':b.split(':')[0],'Destination':b.split(':')[1],'Type':'bind','Mode':'ro','RW':False} for b in binds],
  'State':{'Running':False,'ExitCode':1,'OOMKilled':False}}
 offline=dict(Kind='fixed_cluster_read_window_v1',Admission={'RunID':QUERY_RUN,'BinarySHA256':a['driver_sha256']},Attempts=None,ActualDurationNS=0,Completions=0,WarmupCompletions=0,MeasuredOriginUTC='0001-01-01T00:00:00Z',Counts=dict(Planned=65536,Attempted=0,Succeeded=0,Failed=0,Canceled=0,Unknown=0,Unissued=65536),WarmupCounts=dict(Planned=64,Attempted=0,Succeeded=0,Failed=0,Canceled=0,Unknown=0,Unissued=64),Verdict='FAILED',Error='isolated readiness refused')
 ps=('\n'.join(json.dumps({'Event':event,'Report':offline}) for event in ('planned','result'))+'\n').encode()
 pr=dict(NetworkMode='none',DBMounts=[],CredentialsReadable=True,DriverSHA256=a['driver_sha256'],Image=a['query_image'],User=a['driver_uid_gid'],RunID=RUN,MutationInvocations=0,PlannedSHA256=sha(ps),ApprovedArgv=pc)
 for key in PERMISSION_PATH_KEYS:pr[key]='/pure/'+key
 def obs(args,out,start,end,err=''):
  return json.dumps(dict(argv=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','mikers@192.168.0.185',shlex.join(args)],stdout=out,stderr=err,started_unix=start,finished_unix=end,exit_code=0)).encode()
 cid=px['Id'];inspect_bytes=json.dumps([px]).encode();err=b'expected isolated readiness refusal\n'
 vals={'command_path':json.dumps(pc).encode(),'inspect_path':inspect_bytes,'stdout_path':ps,'stderr_path':err,
  'launch_path':obs(pc,cid+'\n',1,2),'exit_path':obs(['docker','wait',cid],'1\n',3,4),
  'inspect_receipt_path':obs(['docker','inspect',cid],inspect_bytes.decode(),5,6),
  'logs_path':obs(['docker','logs',cid],ps.decode(),7,8,err.decode())}
 raw={pr[k]:v for k,v in vals.items()};pr['raw_evidence']={k:sha(v) for k,v in raw.items()}
 good('one CID approved argv inspect wait logs ELF proof',lambda:permission_proof(pr,raw,a))
 def altered_inspect(change):
  z=copy.deepcopy(px);change(z);v=json.dumps([z]).encode();rr=dict(raw);rr[pr['inspect_path']]=v
  rr[pr['inspect_receipt_path']]=obs(['docker','inspect',cid],v.decode(),5,6);return rr
 for name,edit in [('substituted entrypoint',lambda z:z.__setitem__('Path','/bin/false')),('substituted inspected args',lambda z:z.__setitem__('Args',['-mode','insert'])),('foreign preflight CID',lambda z:z.__setitem__('Id','6'*64)),('extra effective DB mount',lambda z:z['Mounts'].append({'Type':'bind','Source':'/stores','Destination':'/data','RW':True,'Mode':'rw'})),('structured mount',lambda z:z['HostConfig'].__setitem__('Mounts',[{'Type':'bind','Target':'/data'}])),('inherited mount',lambda z:z['HostConfig'].__setitem__('VolumesFrom',['foreign']))]:
  rr=altered_inspect(edit);bad(name,lambda rr=rr:permission_proof(pr,rr,a))
 rr=dict(raw);rr[pr['launch_path']]=obs(['docker','run','--network=none','foreign-image'],'5'*64+'\n',1,2)
 bad('substituted actual launch argv',lambda:permission_proof(pr,rr,a))
 rr=dict(raw);rr[pr['logs_path']]=obs(['docker','logs','6'*64],ps.decode(),7,8,err.decode())
 bad('foreign logs CID',lambda:permission_proof(pr,rr,a))
 z=copy.deepcopy(pr);rr=dict(raw);v=[strict(line) for line in ps.splitlines()];v[0]['Report']['Admission']['BinarySHA256']='7'*64;ss=('\n'.join(json.dumps(x) for x in v)+'\n').encode();z['PlannedSHA256']=sha(ss);rr[z['stdout_path']]=ss
 bad('planned ELF hash substitution',lambda:permission_proof(z,rr,a))
 for key,value in [('Completions',1),('ActualDurationNS',1),('Attempts',[{}]),('Verdict','ACCEPTED')]:
  vv=[strict(line) for line in ps.splitlines()];vv[1]['Report'][key]=value;ss=('\n'.join(json.dumps(x) for x in vv)+'\n').encode();z=copy.deepcopy(pr);rr=dict(raw);z['PlannedSHA256']=sha(ss);rr[z['stdout_path']]=ss
  bad('offline '+key+' tamper',lambda z=z,rr=rr:permission_proof(z,rr,a))
 # Meaningful timing regression: a real consumer can beat the installer's
 # post-install timestamp; the exact prior livepoll remains a strict bound.
 consumed='2026-10-04T20:21:26.603498205Z';ct=stamp(consumed)/1e9
 poll={'remote_finished_unix':ct-.3,'files':{'ready.json':'retained'}};ack={'installed_unix':ct+.001};db={'AcknowledgedUTC':consumed}
 batch=[{'host':'192.168.0.185','targets':[{'fields':{'memory.current':{'finished_unix':ct-.4}}}]}]
 good('Go ACK precedes installer postcheck observation',lambda:ack_observation_order(poll,ack,db,batch,'ready'))
 for label,value in [('late poll',ct+1),('nonfinite poll',float('nan')),('bool poll',True)]:
  z=copy.deepcopy(poll);z['remote_finished_unix']=value;bad(label,lambda z=z:ack_observation_order(z,ack,db,batch,'ready'))
 z=copy.deepcopy(poll);z['files']['ready.ack']='stale';bad('ACK present before installation dispatch',lambda:ack_observation_order(z,ack,db,batch,'ready'))
 z=copy.deepcopy(batch);z[0]['targets'][0]['fields']['memory.current']['finished_unix']=ct+1;bad('late same-host boundary field',lambda:ack_observation_order(poll,ack,db,z,'ready'))
 bad('post-install observation precedes prior poll',lambda:ack_observation_order(poll,{'installed_unix':ct-1},db,batch,'ready'))
 return dict(disposition='PURE_SELFCHECK_ONLY' ,checks=checks,count=len(checks),source_sha256=sha(Path(__file__).read_bytes()),source_pins={CORE:CORE_SHA,OLD:OLD_SHA,COLLECTOR:COLLECTOR_SHA},limits=['No campaign evidence exists/was fabricated; full callback not executed. Gate adaptation and permission acquisition require independent review before use.','Permission requires all named raw paths in raw_evidence plus ApprovedArgv; actual detached SSH launch/wait/inspect/log observations for one CID, Path/Args/Entrypoint/Cmd, effective readonly mounts, planned Admission.BinarySHA256 and raw planned-event SHA.','No subprocess/network/runtime calls.'],required_permission_keys=['NetworkMode','DBMounts','CredentialsReadable','DriverSHA256','Image','User','RunID','MutationInvocations','PlannedSHA256','raw_evidence','inspect_path','command_path','stdout_path','stderr_path','launch_path','inspect_receipt_path','logs_path','exit_path','ApprovedArgv'],collector_exit_contract='Separate actual process-exit path plus SHA; future exit cannot be preexecution manifest-local pin.')
if __name__=='__main__':
 if sys.argv[1:]!=['--self-check']:raise SystemExit('only pure --self-check; root invokes verifier callback')
 print(json.dumps(self_check(),sort_keys=True))
