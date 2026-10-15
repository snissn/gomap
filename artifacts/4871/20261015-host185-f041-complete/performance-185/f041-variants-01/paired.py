import pathlib,os,time,json,subprocess,threading,hashlib,statistics
D=pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');O=D/'performance-185/f041-variants-01';R=O/'paired';R.mkdir();G=D/'toolchains/go1.26.3'
assert json.loads((O/'combined-controls.json').read_text())['exit']==0
E=os.environ.copy();E.update(GOMAXPROCS='1',TMPDIR='/mnt/fast4tb/mvcc4871tmp',GOROOT=str(G),GOTOOLCHAIN='local',GOWORK='off')
allowed={os.getpid()};hz=os.sysconf('SC_CLK_TCK')
def snap():
 d={}
 for p in pathlib.Path('/proc').iterdir():
  if not p.name.isdigit():continue
  try:
   s=(p/'stat').read_text();i=s.rindex(')');f=s[i+2:].split();d[(int(p.name),f[19])]={'pid':int(p.name),'comm':s[s.index('(')+1:i],'ticks':int(f[11])+int(f[12])}
  except (OSError,ValueError,IndexError):pass
 return d
def delta(a,b,dt):
 return [{**v,'cpu_pct':(v['ticks']-a[k]['ticks'])*100/hz/dt} for k,v in b.items() if k in a and v['pid'] not in allowed and (v['ticks']-a[k]['ticks'])*100/hz/dt>=2]
a=snap();t=time.monotonic();time.sleep(5);busy=delta(a,snap(),time.monotonic()-t)
(R/'preflight.json').write_text(json.dumps({'busy':busy,'threshold_pct':2,'loadavg':pathlib.Path('/proc/loadavg').read_text()},indent=2)+'\n')
if busy:print('NOT_QUIET_NO_SAMPLES',json.dumps(busy),flush=True);raise SystemExit(2)
plan=[]
for label,pkg,rx in [('get','db','^BenchmarkGetVersioned$'),('seek','treedb','^BenchmarkSnapshotIteratorSeekNext$/^keys=1024$/^snapshot_seek$')]:
 for n,order in enumerate([['baseline','control','combined'],['combined','control','baseline'],['baseline','control','combined']],1):
  for variant in order:
   b=O/(variant+'-'+pkg+'.test');name=f'{label}-{n}-{variant}';argv=['taskset','-c','0',str(b),'-test.run=^$','-test.bench='+rx,'-test.benchtime=1s','-test.count=1','-test.benchmem','-test.timeout=30s'];plan.append({'name':name,'label':label,'variant':variant,'repeat':n,'argv':argv,'binary_sha256':hashlib.sha256(b.read_bytes()).hexdigest()})
(R/'plan.json').write_text(json.dumps({'scope':'bounded viability only; never final qualification','plan':plan},indent=2)+'\n')
stop=threading.Event();samples=[]
def monitor():
 a=snap();last=time.monotonic()
 while not stop.wait(1):
  b=snap();now=time.monotonic();samples.append({'time':now,'busy':delta(a,b,now-last)});a,last=b,now
thread=threading.Thread(target=monitor,daemon=True);thread.start();rows=[]
try:
 for p in plan:
  with (R/(p['name']+'.txt')).open('w') as f:
   proc=subprocess.Popen(p['argv'],cwd=O,env=E,stdout=f,stderr=subprocess.STDOUT);allowed.add(proc.pid);code=proc.wait(timeout=40)
  raw=(R/(p['name']+'.txt')).read_text();m=[l.split() for l in raw.splitlines() if l.startswith('Benchmark')];assert code==0 and len(m)==1,(code,m)
  fields=m[0];metrics={fields[i+1]:float(fields[i]) for i in range(2,len(fields)-1,2)};rows.append({**p,'exit':code,'metrics':metrics});(R/'rows.json').write_text(json.dumps(rows,indent=2)+'\n');print(p['name'],metrics,flush=True)
finally:stop.set();thread.join();(R/'host-during.json').write_text(json.dumps(samples,indent=2)+'\n')
summary={'all_samples_retained':len(rows)==18,'quiet_during':not any(x['busy'] for x in samples),'claim':'direction only if quiet; no final acceptance','metrics':{}}
for label in ['get','seek']:
 summary['metrics'][label]={v:statistics.median(r['metrics']['ns/op'] for r in rows if r['label']==label and r['variant']==v) for v in ['baseline','control','combined']}
(R/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary),flush=True)
