import ctypes,datetime,hashlib,json,os,re,signal,subprocess,tarfile,time
from pathlib import Path
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');N=D/'native-integrated';B=D/'native-main-integration/private-hash-capacity/source-correction01';O=B/'controls';O.mkdir(exist_ok=False)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def dump(p,v):p.write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
def utc():return datetime.datetime.now(datetime.timezone.utc).isoformat()
grantpath=D/'native-decision/root-private-ledger-correction-and-controls.md';assert sha(grantpath)=='cac20b5e0ca11c9f2fe9d6e255b178654ac383d93918f25ac76cb3fc7be84202'
plan=json.loads((B/'proposed-commands.json').read_text());proposals=plan['commands'];assert len(proposals)==12
assert [('-race' in c['argv']) for c in proposals]==[False]*6+[True]*6
assert sum(len(c['required_top_level_tests']) for c in proposals[:6])==28
for c in proposals:
 assert c['argv'][c['argv'].index('-run')+1]=='^('+'|'.join(c['required_top_level_tests'])+')$'
 assert '-p=1' in c['argv'] and '-count=1' in c['argv'] and '-v' in c['argv'] and '-timeout=3m' in c['argv']
 assert all(x not in ' '.join(c['argv']) for x in ['Actual33','RealBackendRoute','FinalDriver'])
manifestpath=B/'source-frozen.json';expected=json.loads(manifestpath.read_text());assert len(expected)==10389
for p,h in plan['selected_test_files'].items(): assert sha(N/p)==expected[p]==h
receipt=json.loads((B/'freeze-receipt.json').read_text());assert all(sha(D/'native-decision'/name)==h for name,h in receipt['selections'].items());archive=B/'source-frozen.tar.gz';assert sha(archive)==receipt['source_archive_sha256'];assert sha(manifestpath)==receipt['source_manifest_sha256']
with tarfile.open(archive,'r:gz') as tf:assert {m.name:hashlib.sha256(tf.extractfile(m).read()).hexdigest() for m in tf if m.isfile()}==expected
libc=ctypes.CDLL(None,use_errno=True);assert libc.prctl(36,1,0,0,0)==0
subreaper=ctypes.c_int();assert libc.prctl(37,ctypes.byref(subreaper),0,0,0)==0 and subreaper.value==1
compiler=Path(proposals[0]['argv'][0]);goroot=Path(proposals[0]['env']['GOROOT']);identity={'selection_bindings':receipt['selections'],'go_binary':str(compiler),'go_binary_sha256':sha(compiler),'goroot_VERSION':(goroot/'VERSION').read_text(),'compile_binary_sha256':sha(goroot/'pkg/tool/linux_amd64/compile'),'grant_sha256':sha(grantpath),'archive':str(archive),'archive_sha256':sha(archive),'archive_verified_files':len(expected),'runner_pid':os.getpid(),'runner_pgid':os.getpgrp(),'subreaper_verified':True,'runner_sha256':sha(Path(__file__)),'proposed_commands_sha256':sha(B/'proposed-commands.json'),'selected_test_files':plan['selected_test_files']};assert identity['go_binary_sha256']=='d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65';assert identity['compile_binary_sha256']=='3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1';dump(O/'identity.json',identity)
def source():
 paths=[]
 for base,ds,fs in os.walk(N):
  ds[:]=[x for x in ds if x!='.git']
  for name in fs:
   p=Path(base,name);q=str(p.relative_to(N))
   if q!='.git' and p.is_file():paths.append(q)
 return {p:sha(N/p) for p in sorted(paths)}
def processes():
 rows={}
 for p in Path('/proc').iterdir():
  if not p.name.isdigit():continue
  try:
   t=(p/'stat').read_text();a=t.rfind(')');fields=t[a+2:].split();rows[int(p.name)]={'pid':int(p.name),'state':fields[0],'ppid':int(fields[1]),'pgid':int(fields[2]),'session':int(fields[3]),'comm':t[t.find('(')+1:a]}
  except (OSError,ValueError,IndexError):pass
 return rows
def reap():
 result=[]
 while True:
  try:p,s=os.waitpid(-1,os.WNOHANG)
  except ChildProcessError:break
  if p==0:break
  result.append({'pid':p,'status':s})
 return result
results=[]
for command in proposals:
 name=command['name'];out=O/name;out.mkdir();Path(command['env']['TMPDIR']).mkdir(parents=True,exist_ok=False)
 before=source();dump(out/'source-before.json',before);assert before==expected,'source drift before '+name
 dump(out/'command.json',dict(command,executed=True,grant_sha256=sha(grantpath),identity=str(O/'identity.json'),source_archive=str(archive),source_archive_sha256=sha(archive)))
 env=os.environ.copy();env.update(command['env']);dump(out/'environment.json',{'variables':{k:v for k,v in env.items() if not any(term in k.upper() for term in ['TOKEN','PASSWORD','SECRET','CREDENTIAL','AUTH','PRIVATE_KEY'])},'redacted_variable_names':[k for k in env if any(term in k.upper() for term in ['TOKEN','PASSWORD','SECRET','CREDENTIAL','AUTH','PRIVATE_KEY'])]});started=utc();start=time.monotonic();timedout=False;signals=[];observed={};events=[]
 with (out/'run.log').open('wb') as log:
  p=subprocess.Popen(command['argv'],cwd=command['cwd'],env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True);pgid=os.getpgid(p.pid);dump(out/'process.json',{'pid':p.pid,'pgid':pgid,'start_utc':started,'runner_pid':os.getpid(),'runner_subreaper':True})
  while p.poll() is None:
   rows=processes();observed.update({pid:r for pid,r in rows.items() if r['pgid']==pgid})
   if time.monotonic()-start>command['outer_timeout_seconds']:
    timedout=True
    for sig in (signal.SIGTERM,signal.SIGKILL):
     try:os.killpg(pgid,sig);signals.append({'signal':sig.name,'utc':utc()})
     except ProcessLookupError:pass
     if sig==signal.SIGTERM:
      deadline=time.monotonic()+2
      while time.monotonic()<deadline and p.poll() is None:time.sleep(.05)
    break
   time.sleep(.1)
  rc=p.wait();events+=reap()
 deadline=time.monotonic()+5
 while time.monotonic()<deadline:
  events+=reap();rows=processes();group=[r for r in rows.values() if r['pgid']==pgid];children=[r for r in rows.values() if r['ppid']==os.getpid()]
  if not group and not children:break
  time.sleep(.05)
 residual_observed=bool(group or children)
 if group or children:
  for g in {r['pgid'] for r in group+children}:
   if g==os.getpgrp():continue
   try:os.killpg(g,signal.SIGKILL);signals.append({'signal':'SIGKILL residual cleanup','pgid':g,'utc':utc()})
   except ProcessLookupError:pass
  deadline=time.monotonic()+3
  while time.monotonic()<deadline:
   events+=reap();rows=processes();group=[r for r in rows.values() if r['pgid']==pgid];children=[r for r in rows.values() if r['ppid']==os.getpid()]
   if not group and not children:break
   time.sleep(.05)
 after=source();dump(out/'source-after.json',after);text=(out/'run.log').read_text(errors='replace');run_names=re.findall(r'^=== RUN\s+(\S+)',text,re.M);pass_names=re.findall(r'^\s*--- PASS: (\S+)',text,re.M);fail_names=re.findall(r'^\s*--- FAIL: (\S+)',text,re.M);skip_names=re.findall(r'^\s*--- SKIP: (\S+)',text,re.M)
 top=[x for x in run_names if '/' not in x];required=command['required_top_level_tests'];missing=[x for x in required if x not in run_names or x not in pass_names];unexpected=[x for x in top if x not in required];reasons=[]
 if rc:reasons.append('nonzero exit')
 if timedout:reasons.append('outer timeout')
 if before!=after or after!=expected:reasons.append('source drift')
 if group or children or residual_observed:reasons.append('process leak')
 if signals:reasons.append('process intervention')
 if missing or unexpected or len(top)!=len(required) or len(set(top))!=len(top):reasons.append('top RUN inventory')
 if skip_names:reasons.append('unexpected skip')
 if fail_names or any(x not in pass_names for x in run_names):reasons.append('RUN without PASS or FAIL')
 if any(x in text for x in ['WARNING: DATA RACE','panic:','goleak:']):reasons.append('race/panic/leak')
 result={'name':name,'status':'PASS' if not reasons else 'STOP','stop_reasons':reasons,'exit_code':rc,'timeout':timedout,'started_utc':started,'ended_utc':utc(),'elapsed_seconds':time.monotonic()-start,'leader_joined':True,'pid':p.pid,'pgid':pgid,'subreaper_enabled':True,'observed_group_processes':list(observed.values()),'adopted_children_reaped':events,'signals':signals,'remaining_group_members':group,'remaining_direct_or_adopted_children':children,'group_and_children_empty':not group and not children,'residual_observed_before_cleanup':residual_observed,'source_before_equals_frozen':before==expected,'source_after_equals_frozen':after==expected,'source_before_after_equal':before==after,'test_RUN_names':run_names,'test_PASS_names':pass_names,'test_FAIL_names':fail_names,'test_SKIP_names':skip_names,'top_level_RUN_names':top,'required_top_level_names':required,'missing_named_top_RUN_PASS':missing,'unexpected_top_RUN':unexpected,'top_level_pass_count':len([x for x in pass_names if '/' not in x]),'child_RUN_count':len([x for x in run_names if '/' in x]),'raw_log_sha256':sha(out/'run.log')};dump(out/'result.json',result);results.append(result);print(json.dumps({'name':name,'status':result['status'],'exit_code':rc,'elapsed_seconds':result['elapsed_seconds'],'top_level_pass':result['top_level_pass_count'],'child_RUN':result['child_RUN_count'],'source_equal':before==after==expected,'group_empty':result['group_and_children_empty'],'stop_reasons':reasons}),flush=True)
 if reasons:break
summary={'results':[{'name':x['name'],'result':str(O/x['name']/'result.json'),'status':x['status'],'exit_code':x['exit_code']} for x in results],'commands_executed':len(results),'commands_not_executed':[x['name'] for x in proposals[len(results):]],'stopped_at_first_failure':any(x['status']!='PASS' for x in results),'source_manifest_sha256':sha(manifestpath),'source_files':len(expected),'source_still_frozen':source()==expected,'all_groups_empty':all(x['group_and_children_empty'] for x in results),'all_authorized_pass':len(results)==12 and all(x['status']=='PASS' for x in results),'top_pass_normal':sum(x['top_level_pass_count'] for x in results if not x['name'].startswith('race-')),'top_pass_race':sum(x['top_level_pass_count'] for x in results if x['name'].startswith('race-')),'actual33_executed':False,'native_publication_runtime':False,'public_status':'Q32 RED/ACK0; no allowance','completed_utc':utc()};dump(O/'summary.json',summary);print(json.dumps(summary,indent=2),flush=True)
