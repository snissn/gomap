#!/usr/bin/env python3
"""One hosted diagnostic, never product acceptance. No GitHub writes/host access."""
import argparse,fcntl,hashlib,json,os,pathlib,re,shutil,signal,stat,sys,time
from science import ARMS,gate,schedule,parse_raw,analyze,verify_test,verify_discovery
from supervisor import Supervisor
from census import enforce as enforce_capacity

P=pathlib.Path(__file__).resolve().parent
A='49ad410daf0cabdc9af1c5ca8efca85d5de3154e'
C='7553a2cbc88fa18d402142113c57c25cd8e66092'
TREES={A:'1adca861aafad06315f2e7e42cc14e4f22f4627c',C:'619bbd50a70c0aa554462d012f0984b0bfba3f8f'}
NONCE='3992-hosted-engineering-successor-20261029-v1'
CAP=2*1024**3
FLOOR=20*1024**3

def sha(path):return gate.digest(pathlib.Path(path))
def save(path,value):
 with pathlib.Path(path).open('x') as f:json.dump(value,f,indent=2,sort_keys=True);f.write('\n')

def authorize(e,event):
 required={'GITHUB_ACTIONS':'true','GITHUB_REPOSITORY':'snissn/gomap','GITHUB_EVENT_NAME':'pull_request','GITHUB_RUN_NUMBER':'1','GITHUB_RUN_ATTEMPT':'1','RUNNER_ENVIRONMENT':'github-hosted','RUNNER_OS':'Linux','RUNNER_ARCH':'X64','GITHUB_ACTOR':'snissn'}
 if any(e.get(k)!=v for k,v in required.items()):raise ValueError('wrong opened PR/first run/attempt/owner/hosted class')
 if not re.fullmatch(r'[1-9][0-9]*',e.get('GITHUB_RUN_ID','')):raise ValueError('missing run identity')
 if not re.fullmatch('[a-f0-9]{40}',e.get('GITHUB_SHA','')) or e.get('GITHUB_WORKFLOW_SHA')!=e['GITHUB_SHA']:raise ValueError('exact event merge/workflow SHA required')
 pr=event['pull_request']
 if event['action']!='opened' or pr['draft'] is not True or event['sender']['login']!='snissn' or pr['user']['login']!='snissn':raise ValueError('one authorized owner-opened draft PR required')
 if event['repository']['full_name']!='snissn/gomap' or pr['head']['repo']['full_name']!='snissn/gomap' or pr['base']['repo']['full_name']!='snissn/gomap' or pr['head']['ref']!='codex/3992-hosted-engineering-successor-20261029':raise ValueError('exact same-repository one-use draft branch required')
 matches=re.findall(r'<!-- GOMAP3992-AUTHORITY\n(.*?)\n-->',pr.get('body') or '',re.S)
 if len(matches)!=1:raise ValueError('one explicit pre-opening authority object required')
 auth=json.loads(matches[0])
 if set(auth)!={'head_sha','base_sha','review_sha256','expires_epoch','nonce','authorization'}:raise ValueError('exact public authority schema')
 if auth['nonce']!=NONCE or auth['authorization']!='AUTHORIZE_ONE_REVIEWED_HOSTED_DIAGNOSTIC':raise ValueError('not armed by explicit owner authority')
 if auth['head_sha']!=pr['head']['sha'] or auth['base_sha']!=pr['base']['sha'] or not all(re.fullmatch('[a-f0-9]{40}',auth[k]) for k in ['head_sha','base_sha']):raise ValueError('exact reviewed head/base required')
 if not re.fullmatch('[a-f0-9]{64}',auth['review_sha256']):raise ValueError('accepted independent review digest required')
 if type(auth['expires_epoch']) is not int or not time.time()<auth['expires_epoch']<=time.time()+5400:raise ValueError('fresh <=90 minute authority expiry required')
 if e.get('ImageOS')!='ubuntu24':raise ValueError('original ubuntu24 runner image class required')
 return dict(auth,**{k:e[k] for k in [*required,'GITHUB_RUN_ID','GITHUB_SHA','GITHUB_WORKFLOW_SHA','ImageOS']},pr_number=pr['number'])

def verify_seal():
 expected=json.loads((P/'INPUTS.json').read_text())
 for n,h in expected.items():
  path=P/n
  if path.is_symlink() or not path.is_file() or sha(path)!=h:raise ValueError('sealed diagnostic input drift: '+n)
 return sha(P/'INPUTS.json')

def census(root):
 return enforce_capacity(root,"hosted")

def snapshot(tmp,out):
 paths={}
 for n,p in {'database_tmp':tmp,'output':out,'workspace':pathlib.Path(os.environ['GITHUB_WORKSPACE'])}.items():
  s=p.stat();v=os.statvfs(p);paths[n]={'path':str(p),'device':s.st_dev,'fsid':v.f_fsid,'free_bytes':v.f_bavail*v.f_frsize}
 data={'uname':list(os.uname()),'boot_id':pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'affinity':sorted(os.sched_getaffinity(0)),'paths':paths,'runner_image':{k:os.environ.get(k) for k in ['ImageOS','ImageVersion','RUNNER_ENVIRONMENT','RUNNER_ARCH','RUNNER_OS']},'proc':{}}
 for n in ['diskstats','meminfo','vmstat','stat','cpuinfo','self/mountinfo','pressure/cpu','pressure/io','pressure/memory']:
  p=pathlib.Path('/proc')/n;data['proc'][n]=p.read_text() if p.exists() else None
 return data

class Commands:
 def __init__(self,out,env,lease):
  self.out=out;self.env=env;self.lease=lease;self.serial=0;self.rows=[];self.deadline=time.monotonic()+4500
  self.owner=Supervisor(event=self.event,allow_nested_sessions=True);self.owner.install_handlers()
 def event(self,**row):
  with (self.out/'process-events.jsonl').open('a') as f:f.write(json.dumps(dict(row,monotonic=time.monotonic()))+'\n')
 def guard(self):
  if time.monotonic()>self.deadline:raise RuntimeError('75 minute controller limit')
  if getattr(self,'expires',None) is not None and time.time()>=self.expires:raise RuntimeError('one-use authority expired')
  census(self.out)
  if shutil.disk_usage(self.env['TMPDIR']).free<FLOOR:raise RuntimeError('database volume free floor')
  st=os.fstat(self.lease);target=(self.out/'job.lock').lstat()
  if not stat.S_ISREG(target.st_mode) or (st.st_dev,st.st_ino)!=(target.st_dev,target.st_ino) or target.st_uid!=os.getuid() or stat.S_IMODE(target.st_mode)!=0o600:raise RuntimeError('owned per-job lease identity changed')
  fcntl.flock(self.lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
 def __call__(self,label,argv,cwd=None,timeout=90,quiet=False):
  self.guard()
  self.serial+=1;name='%03d-%s'%(self.serial,label);log=self.out/(name+'.log');observer=None;started=time.monotonic();row={'label':label,'argv':list(map(str,argv)),'cwd':str(cwd or self.out),'status':'INVALID'}
  if quiet:observer=gate.ProcessObservation(self.out/(name+'-processes.jsonl'))
  def tick():
   self.guard()
   if log.exists() and log.stat().st_size>16*1024**2:raise RuntimeError('16MiB command log cap')
   if observer is not None:observer.observe()
  try:
   self.owner.capture(argv,self.env,cwd or self.out,log,pass_fds=(self.lease,),tick=tick,timeout=timeout,poll_interval=.25)
   row['status']=0
  finally:
   row['seconds']=time.monotonic()-started;row['drained']=self.owner.active is None
   if log.exists():row['sha256']=sha(log)
   if observer is not None:row['cpu_observation']=observer.finish()
   self.rows.append(row);(self.out/'commands.json').write_text(json.dumps(self.rows,indent=2)+'\n')
  if observer is not None and row['cpu_observation']['busy']:raise RuntimeError('original external CPU policy failed')
  return log.read_text()
 def finish(self,restore=True):
  # An unresolved owner remains live, holding the inherited per-job lease. GitHub
  # may subsequently hard-kill/reclaim this disposable VM; that is INCOMPLETE,
  # never a successful drain. No post-reap numeric PID signaling is introduced.
  while self.owner.active is not None:
   try:self.owner.finalize()
   except BaseException as exc:
    (self.out/'custody-unresolved.json').write_text(json.dumps({'status':'CUSTODY_UNRESOLVED','error':repr(exc),'lease_retained':True})+'\n')
    time.sleep(1)
  if restore:self.owner.restore_handlers()

def main():
 authority=authorize(os.environ,json.loads(pathlib.Path(os.environ['GITHUB_EVENT_PATH']).read_text()));seal=verify_seal()
 out=pathlib.Path(os.environ['DIAGNOSTIC_OUTPUT']);parent=pathlib.Path(os.environ['RUNNER_TEMP']).resolve()
 if out.parent.resolve()!=parent or out.name!='gomap3992-'+authority['GITHUB_RUN_ID'] or out.exists():raise ValueError('exclusive exact runner output required')
 out.mkdir(mode=0o700);save(out/'authority.json',dict(authority,input_seal=seal,diagnostic_only=True,product_acceptance=False))
 lease=os.open(out/'job.lock',os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW,0o600);fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
 lst=os.fstat(lease);save(out/'lease.json',{'scope':'this disposable hosted job only; not physical storage exclusion','device':lst.st_dev,'inode':lst.st_ino,'uid':lst.st_uid})
 tmp=out/'tmp';tmp.mkdir(mode=0o700)
 env={k:v for k,v in os.environ.items() if k in {'PATH','HOME','LANG','LC_ALL','TZ','GOROOT','GOCACHE','GOMODCACHE','GOPATH'}}
 env.update(GOMAXPROCS='1',GOGC='100',GOMEMLIMIT='off',GODEBUG='',GOAMD64='v1',GOTOOLCHAIN='local',GOWORK='off',GOFLAGS='',CGO_ENABLED='1',CC='/usr/bin/gcc',CXX='/usr/bin/g++',CGO_CFLAGS='-O2 -g',CGO_CXXFLAGS='-O2 -g',CGO_CPPFLAGS='',CGO_LDFLAGS='-O2 -g',LD_PRELOAD='',LD_LIBRARY_PATH='',LD_AUDIT='',TMPDIR=str(tmp),PYTHONDONTWRITEBYTECODE='1')
 owner=Commands(out,env,lease);owner.expires=authority['expires_epoch'];terminal={'status':'ENGINEERING_INVALID','diagnostic_only':True,'product_acceptance':False,'original_red_preserved':True};sources=[];rows=[];cleaned=False
 try:
  before=snapshot(tmp,out);save(out/'host-before.json',before)
  if 0 not in before['affinity'] or os.cpu_count()!=4 or 'AMD EPYC 7763' not in before['proc']['cpuinfo']:raise RuntimeError('historical four-vCPU EPYC7763 class unavailable; no timed sample')
  owner('mounts',['findmnt','--json','--output','TARGET,SOURCE,FSTYPE,OPTIONS,MAJ:MIN']);owner('lscpu',['lscpu','--json']);owner('block-devices',['lsblk','--json','--bytes','--output','NAME,MAJ:MIN,TYPE,SIZE,FSTYPE,MOUNTPOINTS,MODEL'])
  tool=owner('toolchain',['go','env','-json','GOVERSION','GOROOT','GOTOOLDIR','GOOS','GOARCH','CGO_ENABLED']);tc=json.loads(tool)
  if tc['GOVERSION']!='go1.26.10' or tc['GOOS']!='linux' or tc['GOARCH']!='amd64':raise RuntimeError('wrong pinned toolchain')
  compile_version=owner('compiler',['go','tool','compile','-V'])
  if 'go1.26.10' not in compile_version:raise RuntimeError('compiler/driver mismatch')
  save(out/'toolchain.json',{'env':tc,'driver':gate.file_identity(pathlib.Path(shutil.which('go'))),'compiler':gate.file_identity(pathlib.Path(tc['GOTOOLDIR'])/'compile'),'cc':gate.file_identity('/usr/bin/gcc')})
  repo=pathlib.Path(os.environ['GITHUB_WORKSPACE']);bins={}
  original_mvcc=owner('original-wrapper',['git','show',C+':TreeDB/mvcc/mvcc.go'],repo)
  needle='\treturn s.CommitGroupAt([]CommitGroup{{Timestamp: timestamp, Mutations: mutations}}, mode)'
  replacement='\tstarted := detectorStart(timestamp, mutations, mode)\n\terr := s.CommitGroupAt([]CommitGroup{{Timestamp: timestamp, Mutations: mutations}}, mode)\n\tif err == nil { detectorFinish(started) }\n\treturn err'
  if original_mvcc.count(needle)!=1 or (P/'instrumented_mvcc.go').read_text()!=original_mvcc.replace(needle,replacement):raise RuntimeError('instrumentation differs from exact public-return-only intervention')
  changed=owner('original-source-diff',['git','diff','--name-only',A,C],repo).splitlines()
  if changed!=['TreeDB/command_wal_group_commit.go','TreeDB/mvcc/ordered_completion_progress_test.go']:raise RuntimeError('historical pair source scope differs')
  owner('original-source-patch',['git','diff',A,C],repo)
  if owner('controller-head',['git','rev-parse','HEAD'],repo).strip()!=authority['head_sha'] or owner('controller-clean',['git','status','--porcelain','--untracked-files=all'],repo):raise RuntimeError('dirty/wrong reviewed PR source')
  owner('event-merge-fetch',['git','fetch','--no-tags','origin',authority['GITHUB_WORKFLOW_SHA']],repo)
  parents=owner('event-merge-parents',['git','show','-s','--format=%P',authority['GITHUB_WORKFLOW_SHA']],repo).strip().split()
  if parents!=[authority['base_sha'],authority['head_sha']]:raise RuntimeError('workflow merge parents differ from reviewed event pair')
  workflow='.github/workflows/3992-hosted-detector-successor.yml'
  workflow_blob=owner('workflow-from-merge',['git','show',authority['GITHUB_WORKFLOW_SHA']+':'+workflow],repo)
  if workflow_blob!=(repo/workflow).read_text():raise RuntimeError('event merge workflow differs from reviewed head workflow')
  for label,commit in [('A',A),('C',C)]:
   src=parent/('gomap3992-source-'+label+'-'+authority['GITHUB_RUN_ID']);owner('worktree-'+label,['git','worktree','add','--detach',src,commit],repo);sources.append(src)
   if owner('tree-'+label,['git','rev-parse','HEAD^{tree}'],src).strip()!=TREES[commit]:raise RuntimeError('wrong source tree')
   bins[label]=src
  for label,src in [('A',bins['A']),('C',bins['C']),('I',bins['C']),('guards',bins['C'])]:
   replacements={str(src/gate.HARNESS):str(P/'frozen/primary_generation_routing_v6_test.go'),str(src/'TreeDB/mvcc/detector_resolution_test.go'):str(P/'detector_test.go')}
   if label in ('I','guards'):replacements[str(src/'TreeDB/mvcc/mvcc.go')]=str(P/'instrumented_mvcc.go')
   if label=='guards':replacements[str(src/'TreeDB/mvcc/detector_guard_test.go')]=str(P/'guard_regression_test.go')
   overlay=out/('overlay-'+label+'.json');save(overlay,{'Replace':replacements})
   owner('build-'+label,['go','test','-c','-p=1','-tags=treedb_test','-trimpath','-buildvcs=false','-overlay',overlay,'-o',out/(label+'.test'),'./TreeDB/mvcc'],src,timeout=600)
  env.update(GOPROXY='off',GOSUMDB='off');save(out/'measured-environment.json',env)
  bindings={x:gate.runtime_binding(out/(x+'.test'),owner,'runtime-before-'+x,env) for x in ['A','C','I']};save(out/'runtime-before.json',bindings)
  owner('build-info',['go','version','-m',out/'A.test',out/'C.test',out/'I.test'])
  guard_names=['TestDetectorArithmetic','TestDetectorActivationPredicate','TestDetectorNegativeShapeSuccessor','TestDetectorErrorPreservedSuccessor']
  verify_discovery(owner('guard-discovery',[out/'guards.test','-test.list=^TestDetector']),['TestDetectorRawSample',*guard_names])
  for n in guard_names:verify_test(owner(n,[out/'guards.test','-test.run=^'+n+'$','-test.v']),n)
  for v in ['A','C','I']:
   verify_discovery(owner('discovery-'+v,[out/(v+'.test'),'-test.list=^('+gate.LIFECYCLE+'|TestDetectorRawSample)$']),[gate.LIFECYCLE,'TestDetectorRawSample'])
   verify_test(owner('lifecycle-before-'+v,['taskset','-c','0',out/(v+'.test'),'-test.run=^'+gate.LIFECYCLE+'$','-test.v','-detector-mode='+('off' if v=='I' else 'raw')]),gate.LIFECYCLE)
  save(out/'schedule.json',schedule());gate.acquire_quiet_window(out);campaign=time.monotonic()+1800
  for item in schedule():
   if time.monotonic()>campaign:raise RuntimeError('30 minute measured campaign limit')
   save(out/('sample-%03d-before.json'%item['index']),snapshot(tmp,out))
   if item['index']==0:save(out/'trial-start-pending.json',{'status':'FIRST_COMPARATOR_START_PENDING','first_item':item,'replay_authorized':False})
   text=owner('sample-%03d'%item['index'],['taskset','-c','0',out/(item['binary']+'.test'),'-test.run=^TestDetectorRawSample$','-test.v','-test.benchtime=128x','-test.count=1','-test.benchmem','-test.timeout=90s','-detector-mode='+item['mode']],cwd=repo,quiet=True)
   verify_test(text,'TestDetectorRawSample');metrics,raw=parse_raw(text,item['mode']);rows.append(dict(item,metrics=metrics,raw=raw));(out/'rows.json').write_text(json.dumps(rows,indent=2)+'\n')
   save(out/('sample-%03d-after.json'%item['index']),snapshot(tmp,out))
  for v in ['A','C','I']:verify_test(owner('lifecycle-after-'+v,['taskset','-c','0',out/(v+'.test'),'-test.run=^'+gate.LIFECYCLE+'$','-test.v','-detector-mode='+('off' if v=='I' else 'raw')]),gate.LIFECYCLE)
  after={x:gate.runtime_binding(out/(x+'.test'),owner,'runtime-after-'+x,env) for x in ['A','C','I']};save(out/'runtime-after.json',after)
  if after!=bindings or verify_seal()!=seal:raise RuntimeError('runtime/input drift')
  for src in sources:
   if owner('source-final-'+src.name,['git','status','--porcelain','--untracked-files=all'],src):raise RuntimeError('source drift')
  terminal=analyze(rows);terminal['historical_attribution']=False;terminal['environment_scope']='observed hosted VM; historical image/storage equivalence unproved'
 except BaseException as exc:
  terminal.update(error=repr(exc),completed_rows=len(rows));raise
 finally:
  owner.finish(restore=False)
  # Commands are drained before any removal. Remove only paths created by this
  # invocation; do not prune unrelated worktrees, caches, services or processes.
  try:
   for src in sources:
    owner('remove-'+src.name,['git','worktree','remove',src],pathlib.Path(os.environ['GITHUB_WORKSPACE']))
   shutil.rmtree(tmp);cleaned=True
  except BaseException as exc:terminal.update(status='ENGINEERING_INVALID',cleanup_error=repr(exc))
  terminal.update(session_drained=owner.owner.active is None,cleanup_complete=cleaned,lease_held_through_audit=True,completed_rows=len(rows));save(out/'host-after.json',snapshot(out,out));save(out/'terminal.json',terminal)
  owner.finish()
  os.close(lease)
  save(out/'ARTIFACT-SHA256.json',{str(p.relative_to(out)):sha(p) for p in out.rglob('*') if p.is_file() and not p.is_symlink()})

if __name__=='__main__':main()
