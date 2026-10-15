from pathlib import Path
import datetime,hashlib,json,os,signal,subprocess,time
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');O=D/'ordinary-review/get-profiles-5027d'
assert not O.exists();O.mkdir();(O/'tmp').mkdir()
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
arms={
 'B':(D/'performance-185/5027d-hosted/packet/measured-binaries/baseline-db.test','943733dcfb972d319198a05d4ae13bcfc857e243339b106212421442e89193fe'),
 'C50':(D/'performance-185/5027d-hosted/packet/measured-binaries/candidate-db.test','f31f04240cd874be28fb1fa22db5449217dd578dddbdbc5507d72af4fc53416d'),
 'C72':(D/'performance-185/72e01-hosted/raw-primary/measured-binaries/candidate-db.test','6ec1c9db444d065992808eee2b5c482263832aa4f71a2940d1ed3a3168ecda13')}
for p,h in arms.values():assert sha(p)==h;p.chmod(p.stat().st_mode|0o100)
env=os.environ.copy();env.update(GOMAXPROCS='1',GOGC='100',TMPDIR=str(O/'tmp'),GOTOOLCHAIN='local',GOWORK='off');env.pop('GODEBUG',None);env.pop('GOMEMLIMIT',None)
plan={'at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'authorization':'Root accepts independent exact-Sol six-profile causal proposal; sole runtime, no qualification or rerun','order':['B','C50','C72','C72','C50','B'],'per_process_outer_seconds':30,'total_outer_seconds':180,'cpu':0,'GOMAXPROCS':1,'GOGC':100,'GODEBUG':'unset','GOMEMLIMIT':'unset','binary_compiler':'Go1.26.9','purpose':'CPU attribution only; unchanged original benchmark and retained ELFs; profiling overhead and different local host preclude hosted gate disposition','limitations':['Original benchmark seeds random key generator with wall clock; no reproducible seed hook','Whole-process profiles include setup and teardown','No local timing acceptance or hosted failure waiver'],'binaries':{k:{'path':str(p),'sha256':h} for k,(p,h) in arms.items()}}
(O/'root-authorization.json').write_text(json.dumps(plan,indent=2)+'\n')
(O/'host.txt').write_text(subprocess.check_output(['uname','-a'],text=True)+subprocess.check_output(['lscpu'],text=True))
def observation():
 d={'monotonic':time.monotonic(),'loadavg':Path('/proc/loadavg').read_text().strip(),'cpu0':[x for x in Path('/proc/stat').read_text().splitlines() if x.startswith('cpu0 ')]}
 for n in ['scaling_cur_freq','cpuinfo_cur_freq']:
  p=Path('/sys/devices/system/cpu/cpu0/cpufreq')/n
  try:d[n]=p.read_text().strip()
  except OSError:pass
 return d
(O/'processes-before.txt').write_text(subprocess.check_output(['ps','-eo','pid,ppid,stat,pcpu,pmem,comm'],text=True))
started=time.monotonic();receipts=[]
for i,arm in enumerate(plan['order']):
 p,h=arms[arm];assert sha(p)==h
 name=f'{i:02d}-{arm}';profile=O/(name+'.pprof');args=['taskset','-c','0',str(p),'-test.run=^$','-test.bench=^BenchmarkGetVersioned$','-test.benchmem','-test.benchtime=8s','-test.count=1','-test.timeout=120s','-test.cpuprofile='+str(profile)]
 assert time.monotonic()-started<180
 observations=[];begin=time.monotonic();timed_out=False
 with (O/(name+'.log')).open('wb') as out:
  proc=subprocess.Popen(args,cwd=D/'ordinary',env=env,stdout=out,stderr=subprocess.STDOUT,start_new_session=True)
  while proc.poll() is None:
   observations.append(observation())
   if time.monotonic()-begin>30 or time.monotonic()-started>180:
    timed_out=True;os.killpg(proc.pid,signal.SIGKILL);proc.wait();break
   time.sleep(.25)
 elapsed=time.monotonic()-begin;assert sha(p)==h
 receipt={'arm':arm,'argv':args,'pid':proc.pid,'elapsed_seconds':elapsed,'exit_code':proc.returncode,'timed_out':timed_out,'binary_sha256':h,'log_sha256':sha(O/(name+'.log')),'profile_sha256':sha(profile) if profile.exists() else None,'observations':observations}
 (O/(name+'.json')).write_text(json.dumps(receipt,indent=2)+'\n');receipts.append({k:v for k,v in receipt.items() if k!='observations'});(O/'receipts.json').write_text(json.dumps(receipts,indent=2)+'\n')
 print(json.dumps({'arm':arm,'exit_code':proc.returncode,'elapsed_seconds':elapsed}),flush=True)
 if proc.returncode or timed_out:raise SystemExit('Retained failure; stop; no automatic retry')
(O/'processes-after.txt').write_text(subprocess.check_output(['ps','-eo','pid,ppid,stat,pcpu,pmem,comm'],text=True))
(O/'completion.json').write_text(json.dumps({'status':'ALL_SIX_DIAGNOSTIC_PROFILES_COMPLETE','elapsed_seconds':time.monotonic()-started,'at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'gate_acceptance':False},indent=2)+'\n')
