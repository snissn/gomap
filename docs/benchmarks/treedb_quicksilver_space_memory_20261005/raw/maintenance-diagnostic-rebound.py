"""Read-only failed-original oracle; bounded CPU diagnostic on a physical copy."""
import hashlib,json,os,pathlib,signal,subprocess,time
root=pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005'); packet=root/'baseline-3m-profile/1-treedb-durable-profile'
meta=json.load((packet/'run.json').open()); report=json.load((packet/'stdout.json').open())[0]; original=pathlib.Path(report['data_dir'])
assert original.parent==root/'working-dbs' and not original.is_symlink()
out=root/'maintenance-3m-diagnostic-rebound';assert not out.exists();out.mkdir();records=[]
env={k:v for k,v in os.environ.items() if not k.startswith('TREEDB_')};env.update(meta['env'])
manifest=json.load((root/'qualified-manifest-baseline.json').open());env.update(manifest['build_env'])
def sha(path):
 h=hashlib.sha256()
 with pathlib.Path(path).open('rb') as f:
  for data in iter(lambda:f.read(1048576),b''):h.update(data)
 return h.hexdigest()
def record(): (out/'commands.json').write_text(json.dumps(records,indent=2)+'\n')
def run(label,argv,timeout=1800,cwd=None):
 start=time.time()
 with (out/(label+'-stdout.txt')).open('w') as stdout,(out/(label+'-stderr.txt')).open('w') as stderr:
  p=subprocess.Popen(argv,env=env,cwd=cwd,stdout=stdout,stderr=stderr,start_new_session=True)
  try:rc=p.wait(timeout=timeout);timed=False
  except subprocess.TimeoutExpired:
   timed=True;os.killpg(p.pid,signal.SIGTERM)
   try:rc=p.wait(timeout=10)
   except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);rc=p.wait()
 records.append(dict(label=label,command=argv,started=start,finished=time.time(),rc=rc,timed_out=timed,pid=p.pid));record()
 assert rc==0 and not timed,label
 print('PASS',label,round(time.time()-start,2),flush=True)
def census():
 rows=[]
 for f in sorted(original.rglob('*')):
  assert not f.is_symlink()
  if f.is_file():
   s=f.stat();rows.append((str(f.relative_to(original)),s.st_size,s.st_blocks,s.st_mtime_ns))
 return rows
source=root/'source-baseline'; overlay=root/'maintenance-diagnostic-overlay'; receipt=json.load((overlay/'receipt.json').open())
prior=json.load((root/'maintenance-3m-diagnostic/commands.json').open())
for label in ('treemap','readonly-oracle'):
 row=next(x for x in prior if x['label']=='build-'+label)
 assert sha(root/'bin'/('diagnostic-'+label))==row['binary_sha256']
run('build-rebind-copy',['go','build','-p','1','-buildvcs=false','-o',str(root/'bin/rebind-owned-copy'),str(overlay/'rebind-copy.go')],cwd=source)
records[-1]['binary_sha256']=sha(root/'bin/rebind-owned-copy');records[-1]['helper_sha256']=sha(overlay/'rebind-copy.go');record()
args=iter(meta['command']);oracle=[str(root/'bin/diagnostic-readonly-oracle')]
next(args)
for value in args:
 if value=='-profile-dir':next(args)
 elif value!='-keep':oracle.append(value)
oracle+=['-quicksilver-verify-dir',str(original)]
before=census()
assert before==[tuple(x) for x in json.load((root/'maintenance-3m-diagnostic/original-unchanged.json').open())['after']]
copy=root/'working-dbs/maintenance-3m-rebound-profile-copy';assert not copy.exists()
run('physical-copy',['cp','-a','--reflink=auto',str(original),str(copy)])
run('rebind-copied-snapshot',[str(root/'bin/rebind-owned-copy'),str(copy)])
oracle[-1]=str(copy);run('rebound-copy-readonly-full-oracle',oracle)
proof=json.load((out/'rebound-copy-readonly-full-oracle-stdout.txt').open())
assert proof['verification_only'] and proof['verified_keys']==report['verified_keys'] and proof['verified_misses']==report['verified_misses']
assert before==census(),'ORIGINAL CENSUS CHANGED'

cpu=out/'rewrite-cpu.pprof';env['GOMAP_QS_COMPACT_CPU_PROFILE']=str(cpu)
argv=[str(root/'bin/diagnostic-treemap'),'compact',str(copy),'-rw','-json','-mode','exhaustive','-sync-each-phase','-leaf-pack-max-passes','64']
start=time.time();stderr=out/'profile-stderr.txt'
with (out/'profile-stdout.json').open('w') as stdout,stderr.open('w') as err:
 p=subprocess.Popen(['/usr/bin/time','-v']+argv,env=env,stdout=stdout,stderr=err,start_new_session=True);complete=False;intentional=False
 while p.poll() is None:
  complete='DIAGNOSTIC_CPU_PROFILE_COMPLETE' in stderr.read_text()
  if complete or time.time()-start>240:
   intentional=True;os.killpg(p.pid,signal.SIGTERM)
   try:p.wait(timeout=10)
   except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
   break
  time.sleep(1)
complete='DIAGNOSTIC_CPU_PROFILE_COMPLETE' in stderr.read_text()
records.append(dict(label='bounded-copy-cpu-diagnostic',command=argv,started=start,finished=time.time(),rc=p.returncode,intentionally_terminated=intentional,profile_complete=complete,copy=str(copy),cpu_sha256=sha(cpu) if cpu.exists() else None));record()
assert cpu.exists() and 'DIAGNOSTIC_CPU_PROFILE_COMPLETE' in stderr.read_text(),'NO COMPLETE PROFILE'
assert before==census(),'ORIGINAL CENSUS CHANGED'
run('cpu-top',['go','tool','pprof','-top','-nodecount=40',str(root/'bin/diagnostic-treemap'),str(cpu)])
run('cpu-cumulative',['go','tool','pprof','-top','-cum','-nodecount=40',str(root/'bin/diagnostic-treemap'),str(cpu)])
(out/'identity.json').write_text(json.dumps(dict(source=meta['source'],environment=env,method_sha256=sha(__file__),overlay_receipt_sha256=sha(overlay/'receipt.json'),original=str(original),copy=str(copy),interpretation='bounded diagnostic of active copy; not throughput qualification'),indent=2)+'\n')
print('PASS_COMPLETE_CPU_DIAGNOSTIC_ORIGINAL_PRESERVED',flush=True)
