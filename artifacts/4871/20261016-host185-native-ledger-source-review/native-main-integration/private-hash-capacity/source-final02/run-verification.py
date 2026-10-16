import ctypes,datetime,hashlib,json,os,re,signal,subprocess,tarfile,time
from pathlib import Path
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');N=D/'native-integrated';B=D/'native-main-integration/private-hash-capacity/source-final02';O=B/'verification';O.mkdir(exist_ok=False)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def dump(p,v):p.write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
def utc():return datetime.datetime.now(datetime.timezone.utc).isoformat()
grantpath=D/'native-decision/root-private-hash-capacity-selection.md';assert sha(grantpath)=='063f85d21e071fb4bf20d79c00af6d1a601dc6c6db15aa0baa04e358bef7cb32';proposals=json.loads((B/'proposed-commands.json').read_text())['commands'];assert len(proposals)==1 and proposals[0]['argv'][proposals[0]['argv'].index('-run')+1]=='^$';assert all('actual33' not in ' '.join(p['argv']) for p in proposals);manifestpath=B/'source-frozen.json';expected=json.loads(manifestpath.read_text());assert len(expected)==json.loads((B/'freeze-receipt.json').read_text())['source_files']
receipt=json.loads((B/'freeze-receipt.json').read_text());assert all(sha(D/'native-decision'/name)==h for name,h in receipt['selections'].items());archive=B/'source-frozen.tar.gz';assert sha(archive)==receipt['source_archive_sha256'];assert sha(manifestpath)==receipt['source_manifest_sha256']
with tarfile.open(archive,'r:gz') as tf:assert {m.name:hashlib.sha256(tf.extractfile(m).read()).hexdigest() for m in tf if m.isfile()}==expected
libc=ctypes.CDLL(None,use_errno=True);assert libc.prctl(36,1,0,0,0)==0
subreaper=ctypes.c_int();assert libc.prctl(37,ctypes.byref(subreaper),0,0,0)==0 and subreaper.value==1
compiler=Path(proposals[0]['argv'][0]);goroot=Path(proposals[0]['env']['GOROOT']);identity={'selection_bindings':receipt['selections'],'go_binary':str(compiler),'go_binary_sha256':sha(compiler),'goroot_VERSION':(goroot/'VERSION').read_text(),'compile_binary_sha256':sha(goroot/'pkg/tool/linux_amd64/compile'),'grant_sha256':sha(grantpath),'archive':str(archive),'archive_sha256':sha(archive),'archive_verified_files':len(expected),'runner_pid':os.getpid(),'runner_pgid':os.getpgrp(),'subreaper_verified':True};assert identity['go_binary_sha256']=='d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65';assert identity['compile_binary_sha256']=='3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1';dump(O/'identity.json',identity)
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
 name=command['name'];out=O/name;out.mkdir();Path(command['env']['TMPDIR']).mkdir(parents=True,exist_ok=True)
 before=source();dump(out/'source-before.json',before);assert before==expected,'source drift before '+name
 dump(out/'command.json',dict(command,executed=True,grant_sha256=sha(grantpath),identity=str(O/'identity.json'),source_archive=str(archive),source_archive_sha256=sha(archive)))
 env=os.environ.copy();env.update(command['env']);started=utc();start=time.monotonic();timedout=False;signals=[];observed={};events=[]
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
 after=source();dump(out/'source-after.json',after);text=(out/'run.log').read_text(errors='replace');run_names=re.findall(r'^=== RUN\s+(.*)$',text,re.M);pass_names=re.findall(r'^--- PASS: ([^ ]+)',text,re.M);fail_names=re.findall(r'^--- FAIL: ([^ ]+)',text,re.M);skip_names=re.findall(r'^--- SKIP: ([^ ]+)',text,re.M)
 result={'name':name,'exit_code':rc,'timeout':timedout,'started_utc':started,'ended_utc':utc(),'elapsed_seconds':time.monotonic()-start,'leader_joined':True,'pid':p.pid,'pgid':pgid,'subreaper_enabled':True,'observed_group_processes':list(observed.values()),'adopted_children_reaped':events,'signals':signals,'remaining_group_members':group,'remaining_direct_or_adopted_children':children,'group_and_children_empty':not group and not children,'residual_observed_before_cleanup':residual_observed,'source_before_equals_frozen':before==expected,'source_after_equals_frozen':after==expected,'source_before_after_equal':before==after,'test_RUN_names':run_names,'top_level_pass_names':pass_names,'top_level_fail_names':fail_names,'top_level_skip_names':skip_names,'test_RUN_count':len(run_names),'top_level_pass_count':len(pass_names),'top_level_fail_count':len(fail_names),'top_level_skip_count':len(skip_names)};dump(out/'result.json',result);results.append(result);print(json.dumps({'name':name,'exit_code':rc,'elapsed_seconds':result['elapsed_seconds'],'tests_started':len(run_names),'top_level_pass':len(pass_names),'top_level_fail':len(fail_names),'source_equal':before==after==expected,'group_empty':result['group_and_children_empty']}),flush=True)
 if rc or timedout or before!=after or after!=expected or group or children or residual_observed or run_names or skip_names or signals:break
summary={'results':[{'name':x['name'],'result':str(O/x['name']/'result.json'),'exit_code':x['exit_code']} for x in results],'commands_executed':len(results),'commands_not_executed':[x['name'] for x in proposals[len(results):]],'stopped_at_first_failure':any(x['exit_code'] or x['timeout'] for x in results),'source_manifest_sha256':sha(manifestpath),'source_files':len(expected),'source_still_frozen':source()==expected,'all_groups_empty':all(x['group_and_children_empty'] for x in results),'test_bodies_executed':False,'zero_test_RUN_verified':all(x['test_RUN_count']==0 for x in results),'all_requested_packages_reported':all(all(any(line.startswith('ok\tgithub.com/snissn/gomap/'+pkg+'\t') or line.startswith('ok  \tgithub.com/snissn/gomap/'+pkg+'\t') for line in (O/x['name']/'run.log').read_text().splitlines()) for pkg in ['TreeDB/freelist', 'TreeDB/db', 'TreeDB/internal/rootpublication', 'TreeDB/internal/retainedalloc', 'TreeDB/caching', 'TreeDB/internal/primaryarena', 'TreeDB/mvcc', 'TreeDB/zipper']) for x in results),'native_publication_runtime':False,'public_status':'Q32 RED/ACK0','completed_utc':utc()};dump(O/'summary.json',summary);print(json.dumps(summary,indent=2),flush=True)
