from pathlib import Path
import subprocess,os,json,time,hashlib,re,statistics,math
base=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013/sprint-performance-20261014')
e=base/'evidence';bins=base/'binaries';tmp=base/'timed-temp';tmp.mkdir(exist_ok=False)
def sha(b):return hashlib.sha256(b).hexdigest()
env=dict(os.environ,GOROOT=str(base.parent/'toolchains/go1.26.3-from-185-20261014'),GOWORK='off',GOTOOLCHAIN='local')
receipts=[];timed_wall=0
allbins={p.name:sha(p.read_bytes()) for p in bins.glob('*.test')}
(e/'premeasurement-binaries.json').write_text(json.dumps(allbins,indent=1)+'\n')
for p in bins.glob('*.test'):p.chmod(0o755)
processes=subprocess.check_output(['ps','-eo','pid,ppid,etime,pcpu,pmem,args','--sort=-pcpu'],text=True);(e/'timing-start-processes.txt').write_text(processes)
# Explicit own workers are quiescent. Fail on another Go/compiler/test/known
# benchmark producer rather than disturbing any foreign process.
for line in processes.splitlines()[1:]:
 command=' '.join(line.split()[5:])
 if re.match(r'.*(?:/bin/go (?:test|build)|/pkg/tool/[^ ]+/(?:compile|link)|\.test(?: |$)|unified-bench(?: |$))',command):raise SystemExit('producer still active: '+line)
def run(kind,label,sample,group,package,regex,duration,mode=None):
 global timed_wall
 executable=bins/(label+'-'+('mvcc' if package=='mvcc' else 'raw-'+package)+'.test')
 assert sha(executable.read_bytes())==allbins[executable.name]
 datadir=tmp/(kind+'-'+label+'-'+str(sample)+'-'+group);datadir.mkdir()
 local=dict(env,TMPDIR=str(datadir),GOMAXPROCS='4' if package=='mvcc' else '1')
 if mode:local['SPRINT_MEMTABLE']=mode
 cpu='0-3' if package=='mvcc' else '0'
 argv=['taskset','-c',cpu,str(executable),'-test.run','^$','-test.bench',regex,'-test.benchmem','-test.benchtime='+duration,'-test.count=1','-test.v','-test.timeout=30s']
 log=e/(kind+'-'+label+'-'+str(sample)+'-'+group+'.log');assert not log.exists()
 started=time.monotonic();stamp=time.time()
 with log.open('wb') as f:
  try:r=subprocess.run(argv,env=local,stdout=f,stderr=subprocess.STDOUT,timeout=35).returncode
  except subprocess.TimeoutExpired:r=124
 wall=time.monotonic()-started
 if kind!='smoke':timed_wall+=wall
 rows=[line for line in log.read_text().splitlines() if re.match(r'^Benchmark\S+\s+\d+\s+',line)]
 receipt={'kind':kind,'label':label,'sample':sample,'group':group,'argv':argv,'env':{k:local[k] for k in ['GOROOT','GOWORK','GOTOOLCHAIN','TMPDIR','GOMAXPROCS']+(['SPRINT_MEMTABLE'] if mode else [])},'unix_start':stamp,'wall_seconds':wall,'exit':r,'log':log.name,'raw_sha256':sha(log.read_bytes()),'binary_sha256':sha(executable.read_bytes()),'rows':rows}
 receipts.append(receipt);(e/'sprint-measurement-receipts.json').write_text(json.dumps({'diagnostic_only':True,'timed_wall_seconds':timed_wall,'receipts':receipts},indent=1)+'\n')
 print(json.dumps({'kind':kind,'label':label,'sample':sample,'group':group,'exit':r,'wall_seconds':round(wall,3),'rows':len(rows)}),flush=True)
 if r or not rows:print(log.read_text()[-4000:],flush=True);raise SystemExit('FAILED_OR_UNMATCHED; all attempted raw retained')
 assert timed_wall<120,'bounded diagnostic measured wall exceeded'
for label,mode in [('before-sprint','adaptive'),('recent-main','cow_btree'),('coherent-candidate','cow_btree')]:run('smoke',label,0,'all-public-cells','mvcc','^BenchmarkSprintMVCC$','1x',mode)
groups=[('get_versioned','db','^BenchmarkGetVersioned$','200ms'),('batch_write','db','^BenchmarkConditionalTxnBaselineBatchWrite$','100x'),('snapshot_seek','treedb','^BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek$','200ms'),('repeated_iterator','caching','^BenchmarkRepeatedIterator$','200ms'),('durable_sync','treedb','^BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1$','200ms')]
for sample in [1,2]:
 for group,package,regex,duration in groups:
  for label in (['before-sprint','recent-main','coherent-candidate'] if sample==1 else ['coherent-candidate','recent-main','before-sprint']):run('micro',label,sample,group,package,regex,duration)
for sample in [1,2]:
 for profile in ['command_wal_durable','command_wal_relaxed']:
  for placement in ['inline','forced_pointer']:
   for operation in ['point','group_versions','concurrent']:
    for label in (['before-sprint','recent-main','coherent-candidate'] if sample==1 else ['coherent-candidate','recent-main','before-sprint']):
     mode='adaptive' if label=='before-sprint' else 'cow_btree'
     regex='^BenchmarkSprintMVCC$/^'+profile+'$/^'+mode+'$/^'+placement+'$/^'+operation+'$'
     run('public',label,sample,profile+'-'+placement+'-'+operation,'mvcc',regex,'64x',mode)
assert allbins=={p.name:sha(p.read_bytes()) for p in bins.glob('*.test')}
(e/'postmeasurement-binaries.json').write_text(json.dumps(allbins,indent=1)+'\n')
(e/'timing-end-processes.txt').write_text(subprocess.check_output(['ps','-eo','pid,ppid,etime,pcpu,pmem,args','--sort=-pcpu'],text=True))
print(json.dumps({'COMPLETE':True,'invocations':len(receipts),'timed_wall_seconds':timed_wall}),flush=True)
