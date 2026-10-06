import os,pathlib,subprocess,json,time,hashlib,statistics,re
out=pathlib.Path('/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/default-path-repair')
env=os.environ.copy();env['GOMAXPROCS']='1'
cases=[('get','db','^BenchmarkGetVersioned$'),('tiny','public','^BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1$')]
receipts=[]
for repeat in range(1,9):
 for label,kind,regex in cases:
  for version in (['base','original'] if repeat%2 else ['original','base']):
   stem=f'{version}-{label}-r{repeat}'
   args=['taskset','-c','0',str(out/'bin'/f'{version}-{kind}.test'),'-test.run','^$','-test.bench',regex,'-test.benchmem','-test.benchtime','2s','-test.count','1']
   ps=subprocess.run(['ps','-eo','pid,ppid,etime,%cpu,args'],capture_output=True,text=True).stdout
   (out/(stem+'.processes.private.txt')).write_text(ps)
   started=time.time()
   with (out/(stem+'.stdout')).open('wb') as stdout,(out/(stem+'.stderr')).open('wb') as stderr:
    p=subprocess.run(args,env=env,stdout=stdout,stderr=stderr)
   receipt={'name':stem,'args':args,'start_unix':started,'elapsed_seconds':time.time()-started,'exit':p.returncode}
   for stream in ['stdout','stderr']:
    receipt[stream+'_sha256']=hashlib.sha256((out/(stem+'.'+stream)).read_bytes()).hexdigest()
   receipts.append(receipt);(out/'original-paired-receipts.json').write_text(json.dumps(receipts,indent=2)+'\n')
   if p.returncode:raise SystemExit(p.returncode)
rows={}
for receipt in receipts:
 matches=re.findall(r'^(Benchmark\S+)\s+\d+\s+([\d.]+) ns/op.*?\s+(\d+) B/op\s+(\d+) allocs/op$',(out/(receipt['name']+'.stdout')).read_text(),re.M)
 if len(matches)!=1:raise ValueError((receipt['name'],matches))
 version,label,repeat=receipt['name'].split('-');rows[(version,label,repeat)]=[float(matches[0][1]),int(matches[0][2]),int(matches[0][3])]
summary={}
for label,_,_ in cases:
 summary[label]={m:{'base':[rows[('base',label,f'r{i}')][j] for i in range(1,9)],'candidate':[rows[('original',label,f'r{i}')][j] for i in range(1,9)],'paired_ratios':[rows[('original',label,f'r{i}')][j]/rows[('base',label,f'r{i}')][j] if rows[('base',label,f'r{i}')][j] else None for i in range(1,9)]} for j,m in enumerate(['ns','bytes','allocs'])}
(out/'original-paired-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
