import os,pathlib,subprocess,json,time,hashlib,statistics,re
out=pathlib.Path('/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/default-path-repair')
env=os.environ.copy();env['GOMAXPROCS']='1'
cases=[('get','db','^BenchmarkGetVersioned$'),('tiny','public','^BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1$')]
receipts=[]
for repeat in range(1,9):
 for label,kind,regex in cases:
  for version in (['base','repaired'] if repeat%2 else ['repaired','base']):
   stem=f'repairpair-{version}-{label}-r{repeat}'
   args=['taskset','-c','0',str(out/'bin'/f'{version}-{kind}.test'),'-test.run','^$','-test.bench',regex,'-test.benchmem','-test.benchtime',('2s' if label=='get' else '1000x'),'-test.count','1']
   ps=subprocess.run(['ps','-eo','pid,ppid,etime,%cpu,args'],capture_output=True,text=True).stdout
   (out/(stem+'.processes.private.txt')).write_text(ps)
   load_before=pathlib.Path('/proc/loadavg').read_text()
   started=time.time()
   with (out/(stem+'.stdout')).open('wb') as stdout,(out/(stem+'.stderr')).open('wb') as stderr:
    p=subprocess.run(args,env=env,stdout=stdout,stderr=stderr)
   receipt={'name':stem,'args':args,'start_unix':started,'elapsed_seconds':time.time()-started,'exit':p.returncode,'load_before':load_before,'load_after':pathlib.Path('/proc/loadavg').read_text(),'qualification':'shared185 diagnostic; foreign owner preserved; Go1.26.3 differs hosted1.26.8'}
   for stream in ['stdout','stderr']:
    receipt[stream+'_sha256']=hashlib.sha256((out/(stem+'.'+stream)).read_bytes()).hexdigest()
   receipts.append(receipt);(out/'repaired-paired-receipts.json').write_text(json.dumps(receipts,indent=2)+'\n')
   (out/(stem+'.after-processes.private.txt')).write_text(subprocess.run(['ps','-eo','pid,ppid,etime,%cpu,args'],capture_output=True,text=True).stdout)
   if p.returncode:raise SystemExit(p.returncode)
rows={}
for receipt in receipts:
 matches=re.findall(r'^(Benchmark\S+)\s+\d+\s+([\d.]+) ns/op.*?\s+(\d+) B/op\s+(\d+) allocs/op$',(out/(receipt['name']+'.stdout')).read_text(),re.M)
 if len(matches)!=1:raise ValueError((receipt['name'],matches))
 version,label,repeat=receipt['name'].removeprefix('repairpair-').split('-');rows[(version,label,repeat)]=[float(matches[0][1]),int(matches[0][2]),int(matches[0][3])]
summary={}
for label,_,_ in cases:
 summary[label]={m:{'base':[rows[('base',label,f'r{i}')][j] for i in range(1,9)],'candidate':[rows[('repaired',label,f'r{i}')][j] for i in range(1,9)],'paired_ratios':[rows[('repaired',label,f'r{i}')][j]/rows[('base',label,f'r{i}')][j] if rows[('base',label,f'r{i}')][j] else None for i in range(1,9)]} for j,m in enumerate(['ns','bytes','allocs'])}
(out/'repaired-paired-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
