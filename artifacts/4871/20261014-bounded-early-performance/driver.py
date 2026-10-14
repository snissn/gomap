import pathlib, subprocess, os, json, time, hashlib, re, sys
root=pathlib.Path('/mnt/fast4tb/mvcc-4871-earlyperf-20261014-sol61')
e=root/'evidence'; bins=root/'measured-binaries'
assert not (e/'measurement-receipts.json').exists()
base='3f87d74b98e8b1e809d151611be03785f85e2283'; cand='c3ba82a5ef6d6590f6c70734045c8d640c4c1255'
env=os.environ.copy();env.update(GOMAXPROCS='1',GOWORK='off',GOTOOLCHAIN='local',GOROOT='/home/mikers/.gvm/gos/go1.26.3')
started=time.monotonic();receipts=[]
groups=[('get_versioned','db','^BenchmarkGetVersioned$','200ms'),('batch_write','db','^BenchmarkConditionalTxnBaselineBatchWrite$','100x'),('snapshot_seek','treedb','^BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek$','200ms'),('repeated_iterator','caching','^BenchmarkRepeatedIterator$','200ms'),('durable_sync','treedb','^BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1$','200ms')]
def measure(revision,sample,group,package,regex,duration):
 assert time.monotonic()-started<600,'bounded measured budget'
 executable=bins/f'{revision}-{package}.test'; datadir=root/'tmp'/f'{revision}-{sample}-{group}';datadir.mkdir()
 env['TMPDIR']=str(datadir)
 argv=['taskset','-c','0',str(executable),'-test.run','^$','-test.bench',regex,'-test.benchmem','-test.benchtime='+duration,'-test.count=1','-test.v','-test.timeout=30s']
 stamp=time.time();wall=time.monotonic()
 processes=subprocess.check_output(['ps','-eo','pid,ppid,etime,pcpu,pmem,args','--sort=-pcpu'],text=True)
 (e/f'processes-{revision}-{sample}-{group}.txt').write_text(processes)
 log=e/f'{revision}-{sample}-{group}.log'
 with log.open('w') as output:
  try: result=subprocess.run(argv,env=env,stdout=output,stderr=subprocess.STDOUT,timeout=35).returncode
  except subprocess.TimeoutExpired:result=124
 content=log.read_text()
 receipt={'revision':revision,'sample':sample,'group':group,'argv':argv,'environment':{k:env[k] for k in ['TMPDIR','GOMAXPROCS','GOWORK','GOTOOLCHAIN','GOROOT']},'unix_start':stamp,'elapsed_seconds':time.monotonic()-wall,'exit':result,'log':log.name,'log_sha256':hashlib.sha256(log.read_bytes()).hexdigest(),'binary_sha256':hashlib.sha256(executable.read_bytes()).hexdigest(),'benchmark_rows':[x for x in content.splitlines() if re.match(r'^Benchmark\S+\s+\d+\s+',x)],'remaining_temp_files':[str(p.relative_to(datadir)) for p in datadir.rglob('*')]}
 receipts.append(receipt);(e/'measurement-receipts.json').write_text(json.dumps({'kind':'BOUNDED_EARLY_SMOKE_NOT_ACCEPTANCE','baseline':base,'candidate':cand,'elapsed_seconds':time.monotonic()-started,'receipts':receipts},indent=2))
 print(revision,sample,group,'exit',result,'wall',round(receipt['elapsed_seconds'],2),flush=True)
 for row in receipt['benchmark_rows']:print(row,flush=True)
 if result or not receipt['benchmark_rows']:raise SystemExit('failed or unmatched benchmark; remaining measurements stopped')
for sample in [1,2]:
 for group,package,regex,duration in groups:
  for revision in (['baseline','candidate'] if sample==1 else ['candidate','baseline']):measure(revision,sample,group,package,regex,duration)
regex='^BenchmarkCOWPublicDirtyCost$/^command_wal_durable$/^cow_btree$/^(Inline64|Pointer4096)$/^N=1024$/^(CaptureReadRelease|IncrementalWriteSync)$'
measure('candidate',1,'cow_current_cost','treedb',regex,'16x')
print('measured_total_seconds',time.monotonic()-started,flush=True)
