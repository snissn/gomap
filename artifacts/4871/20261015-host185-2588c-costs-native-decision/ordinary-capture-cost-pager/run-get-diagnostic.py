from pathlib import Path
import os,subprocess,time,threading,json,hashlib,statistics,signal,datetime
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');A=D/'ordinary-capture-cost-pager';W=D/'ordinary-repair';O=A/'get-diagnostic';assert not O.exists();O.mkdir();(O/'tmp').mkdir()
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
validation=json.loads((A/'validation.json').read_text());assert validation['complete'] and validation['all_pass']
arms={'B':(D/'performance-185/baseline-db.test','fa655eef121154e0f0b55235594c240842ec3acbe67d0e3a4f223297d8f6aa0b'),'C':(A/'candidate-db.test','ee4d7a8c4892292c796c351a9878613abc59292fc602ab2ce9597c65c540545d')}
frozen=json.loads((A/'freeze-after.json').read_text())
def check_source():
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=W,text=True).strip()==frozen['head']
 assert all(sha(W/p)==v for p,v in frozen['files'].items())
check_source()
for p,h in arms.values():assert sha(p)==h
allowed={os.getpid()};hz=os.sysconf('SC_CLK_TCK')
def snap():
 d={}
 for p in Path('/proc').iterdir():
  if not p.name.isdigit():continue
  try:
   s=(p/'stat').read_text();i=s.rindex(')');f=s[i+2:].split();d[(int(p.name),f[19])]={'pid':int(p.name),'comm':s[s.index('(')+1:i],'ticks':int(f[11])+int(f[12])}
  except (OSError,ValueError,IndexError):pass
 return d
def delta(a,b,dt):
 return [{**v,'cpu_pct':(v['ticks']-a[k]['ticks'])*100/hz/dt} for k,v in b.items() if k in a and v['pid'] not in allowed and (v['ticks']-a[k]['ticks'])*100/hz/dt>=2]
plan={'at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'one bounded local direction diagnostic; no qualification or hosted failure waiver','source_head':frozen['head'],'source_patch_sha256':validation['source_patch_sha256'],'compiler':'go1.26.3','pairs':[['B','C'],['C','B'],['B','C']],'original_benchmark':'BenchmarkGetVersioned','benchtime':'2s','cpu':0,'GOMAXPROCS':1,'GOGC':100,'quiet_threshold_percent':2,'preflight_seconds':5,'per_call_timeout_seconds':20,'total_timeout_seconds':120,'all_samples_retained':True,'binaries':{k:{'path':str(p),'sha256':h} for k,(p,h) in arms.items()}}
(O/'plan.json').write_text(json.dumps(plan,indent=2)+'\n')
a=snap();t=time.monotonic();time.sleep(5);b=snap();dt=time.monotonic()-t;busy=delta(a,b,dt)
(O/'preflight.json').write_text(json.dumps({'duration':dt,'before':list(a.values()),'after':list(b.values()),'busy':busy,'loadavg':Path('/proc/loadavg').read_text()},indent=2)+'\n')
if busy:
 print('NOT_QUIET_NO_SAMPLES',json.dumps(busy),flush=True);raise SystemExit(2)
env=os.environ.copy();env.update(GOMAXPROCS='1',GOGC='100',TMPDIR=str(O/'tmp'),GOTOOLCHAIN='local',GOWORK='off');env.pop('GODEBUG',None);env.pop('GOMEMLIMIT',None)
stop=threading.Event();samples=[]
def monitor():
 a=snap();last=time.monotonic()
 while not stop.wait(1):
  b=snap();now=time.monotonic();samples.append({'time':now,'duration':now-last,'busy':delta(a,b,now-last),'before':list(a.values()),'after':list(b.values())});a,last=b,now
thread=threading.Thread(target=monitor,daemon=True);thread.start();rows=[];started=time.monotonic()
try:
 for n,order in enumerate(plan['pairs']):
  for arm in order:
   p,h=arms[arm];assert sha(p)==h;name=f'{n}-{arm}';argv=['taskset','-c','0',str(p),'-test.run=^$','-test.bench=^BenchmarkGetVersioned$','-test.benchtime=2s','-test.count=1','-test.benchmem','-test.timeout=15s'];assert time.monotonic()-started<120
   begin=time.monotonic()
   with (O/(name+'.txt')).open('w') as f:
    proc=subprocess.Popen(argv,cwd=W,env=env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True);allowed.add(proc.pid)
    try:code=proc.wait(timeout=20)
    except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);code=proc.wait();raise
   raw=(O/(name+'.txt')).read_text();parsed=[l.split() for l in raw.splitlines() if l.startswith('Benchmark')];assert code==0 and len(parsed)==1,(code,parsed)
   fields=parsed[0];metrics={fields[i+1]:float(fields[i]) for i in range(2,len(fields)-1,2)};row={'pair':n,'arm':arm,'argv':argv,'pid':proc.pid,'binary_sha256':h,'exit':code,'elapsed':time.monotonic()-begin,'metrics':metrics,'log_sha256':sha(O/(name+'.txt'))};rows.append(row);(O/'rows.json').write_text(json.dumps(rows,indent=2)+'\n');print(json.dumps(row),flush=True)
finally:
 stop.set();thread.join();(O/'host-during.json').write_text(json.dumps(samples,indent=2)+'\n')
check_source()
paired=[]
for n in range(3):
 vals={r['arm']:r['metrics']['ns/op'] for r in rows if r['pair']==n};paired.append((vals['C']/vals['B']-1)*100)
summary={'scope':plan['scope'],'complete':len(rows)==6,'quiet_during':not any(x['busy'] for x in samples),'source_equal':True,'paired_ns_delta_percent':paired,'median_paired_ns_delta_percent':statistics.median(paired),'medians':{k:{m:statistics.median(r['metrics'][m] for r in rows if r['arm']==k) for m in ['ns/op','B/op','allocs/op']} for k in arms},'elapsed_seconds':time.monotonic()-started};(O/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary),flush=True)
