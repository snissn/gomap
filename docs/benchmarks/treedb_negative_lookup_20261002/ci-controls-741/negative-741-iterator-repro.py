import subprocess,json,hashlib,pathlib,os,time,re,statistics
ROOT=pathlib.Path('/mnt/fast4tb/gomap-negative-741-iterator-repro-20261002')
GO='/home/mikers/.gvm/gos/go1.26.3/bin/go'
ENV={k:v for k,v in os.environ.items()if k in ('HOME','PATH','TMPDIR','GOPATH','GOCACHE')}
ENV.update(GOROOT='/home/mikers/.gvm/gos/go1.26.3',GOTOOLCHAIN='local',GOWORK='off',GOFLAGS='',GOMAXPROCS='1',GOMEMLIMIT='1GiB',GOGC='100',GODEBUG='')
def sha(path):
 h=hashlib.sha256()
 with pathlib.Path(path).open('rb')as f:
  for b in iter(lambda:f.read(1<<20),b''):h.update(b)
 return h.hexdigest()
def write(name,data):
 (ROOT/name).write_text(json.dumps(data,indent=2)+'\n')
ROWS=[]
GRANT=os.environ.get('REPRO_TIMING_GRANT')
def run(phase,cmd,timeout=120,**kwargs):
 try:
  return subprocess.run(cmd,timeout=timeout,**kwargs)
 except subprocess.TimeoutExpired as error:
  streams={}
  for stream,data in (('stdout',error.stdout),('stderr',error.stderr)):
   target=kwargs.get(stream)
   if hasattr(target,'name'):
    target.flush();path=pathlib.Path(target.name)
   else:
    path=ROOT/('timeout-'+phase+'.'+stream);path.write_bytes(data or b'')
   streams[stream+'_sha256']=sha(path)
  write('capture-incomplete.json',{'grant':GRANT,'complete':False,'failed_phase':phase,'rows':ROWS,'completed_rows':len(ROWS),'argv':cmd,'timeout_seconds':timeout,**streams})
  raise
def output(phase,cmd,**kwargs):
 return run(phase,cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True,**kwargs).stdout.decode()
def validate_archives(archives):
 if len(archives)!=2 or {item['name']for item in archives}!={'base','candidate'}:
  raise ValueError('archives must contain exactly base and candidate')
def validate_records(records,archives):
 validate_archives(archives)
 if set(records)!={'base','candidate'}:
  raise ValueError('preparation must contain exactly base and candidate')
 for archive in archives:
  name=archive['name'];item=records[name];source=ROOT/name
  if (item['original_head'],item['archive_sha256'],item['source'])!=(archive['head'],archive['sha256'],str(source)):
   raise ValueError('preparation archive identity mismatch: '+name)
  if sha(ROOT/(name+'.tar.gz'))!=archive['sha256']:
   raise ValueError('archive hash mismatch: '+name)
  if inventory(source)!=item['identity'] or sha(ROOT/(name+'-caching.test'))!=item['binary_sha256']:
   raise ValueError('input or binary identity mismatch: '+name)
def inventory(source):
 raw=output(source.name+'-inventory-list',[GO,'list','-deps','-test','-json','./TreeDB/caching'],cwd=source,env=ENV);d=json.JSONDecoder();files={};modules={}
 while raw.strip():
  pkg,end=d.raw_decode(raw.lstrip());raw=raw.lstrip()[end:]
  if pkg.get('Module'):
   m=pkg['Module'];modules[m['Path']]=m
   for n in (m,m.get('Replace',{})):
    if n.get('GoMod'):files[n['GoMod']]=sha(n['GoMod'])
  for field in ('GoFiles','CgoFiles','CFiles','CXXFiles','MFiles','HFiles','FFiles','SFiles','SysoFiles','EmbedFiles'):
   for name in pkg.get(field,[]):
    p=pathlib.Path(pkg['Dir'])/name;files[str(p.resolve())]=sha(p)
 settings=json.loads(output(source.name+'-inventory-env',[GO,'env','-json'],cwd=source,env=ENV));settings['GOGCCFLAGS']=re.sub(r'(-f(?:file|debug)-prefix-map=)/tmp/go-build[0-9]+=',r'\1/tmp/go-build<generated>=',settings['GOGCCFLAGS']);tools=pathlib.Path(ENV['GOROOT'])/'pkg/tool/linux_amd64'
 for p in [pathlib.Path(GO),tools/'compile',tools/'link',tools/'asm',source/'go.mod',source/'go.sum',*pathlib.Path(ENV['GOROOT']).joinpath('pkg/include').glob('*')]:
  if p.is_file():files[str(p.resolve())]=sha(p)
 return {'inputs':files,'modules':modules,'go_environment':settings}
def main():
 archives=json.loads((ROOT/'archives.json').read_text());validate_archives(archives)
 if (ROOT/'preparation.json').exists():
  records=json.loads((ROOT/'preparation.json').read_text())
  validate_records(records,archives)
 else:
  records={}
  for item in archives:
   name=item['name'];source=ROOT/name
   assert sha(ROOT/(name+'.tar.gz'))==item['sha256'];before=inventory(source);binary=ROOT/(name+'-caching.test');cmd=[GO,'test','-c','-trimpath','-buildvcs=false','-o',str(binary),'./TreeDB/caching'];beg=time.time()
   with (ROOT/(name+'-build-v2.stdout')).open('x')as out,(ROOT/(name+'-build-v2.stderr')).open('x')as err:r=run(name+'-build',cmd,timeout=600,cwd=source,env=dict(ENV,GOMAXPROCS='2'),stdout=out,stderr=err)
   assert r.returncode==0;after=inventory(source);assert before==after
   records[name]={'original_head':item['head'],'archive_sha256':item['sha256'],'source':str(source),'identity':before,'binary_sha256':sha(binary),'build_command':cmd,'build_environment':dict(ENV,GOMAXPROCS='2'),'build_exit':r.returncode,'build_elapsed_seconds':time.time()-beg,'build_stdout_sha256':sha(ROOT/(name+'-build-v2.stdout')),'build_stderr_sha256':sha(ROOT/(name+'-build-v2.stderr'))}
 write('preparation.json',records)
 print('PREPARATION PASS',json.dumps({k:{'head':v['original_head'],'binary':v['binary_sha256'],'inputs':len(v['identity']['inputs'])}for k,v in records.items()}),flush=True)
 if os.environ.get('REPRO_TIMING_GRANT'):
  grant=GRANT;rows=ROWS;validate_records(records,archives);preflight=output('vmstat-before',['vmstat','1','3']);(ROOT/'vmstat-before.txt').write_text(preflight);assert int(preflight.splitlines()[-1].split()[14])>=90,'host not quiet';(ROOT/'processes-before.txt').write_text(output('processes-before',['ps','-eo','pid,ppid,etime,pcpu,args','--sort=-pcpu']))
  for pair in range(8):
   for name in (('base','candidate')if pair%2==0 else('candidate','base')):
    binary=ROOT/(name+'-caching.test');cmd=['taskset','-c','4',str(binary),'-test.run=^$','-test.bench=^BenchmarkRepeatedIterator$','-test.benchtime=2s','-test.count=1','-test.benchmem'];stem=f'v2-{pair+1}-{name}';start=time.time()
    with (ROOT/(stem+'.stdout')).open('x')as out,(ROOT/(stem+'.stderr')).open('x')as err:r=run(stem,cmd,timeout=60,cwd=ROOT/name,env=ENV,stdout=out,stderr=err)
    assert r.returncode==0;raw=(ROOT/(stem+'.stdout')).read_text();assert 'PASS'in raw.splitlines();matches=re.findall(r'^BenchmarkRepeatedIterator(?:-\d+)?\s+(\d+)\s+([\d.]+) ns/op\s+([\d.]+) B/op\s+([\d.]+) allocs/op$',raw,re.M);assert len(matches)==1;iterations,ns,bytes_,allocs=matches[0];rows.append({'pair':pair+1,'name':name,'iterations':int(iterations),'ns_op':float(ns),'B_op':float(bytes_),'allocs_op':float(allocs),'argv':cmd,'environment':ENV,'started_unix':start,'elapsed_seconds':time.time()-start,'exit':r.returncode,'stdout_sha256':sha(ROOT/(stem+'.stdout')),'stderr_sha256':sha(ROOT/(stem+'.stderr'))});write('capture-incomplete.json',{'grant':grant,'complete':False,'rows':rows});print(stem,ns,bytes_,allocs,flush=True)
  validate_records(records,archives)
  ratios=[next(x['ns_op']for x in rows if x['pair']==p and x['name']=='candidate')/next(x['ns_op']for x in rows if x['pair']==p and x['name']=='base')for p in range(1,9)];summary={'grant':grant,'complete':True,'rows':rows,'base_median_ns':statistics.median(x['ns_op']for x in rows if x['name']=='base'),'candidate_median_ns':statistics.median(x['ns_op']for x in rows if x['name']=='candidate'),'paired_median_relative_delta_percent':(statistics.median(ratios)-1)*100,'paired_ratios':ratios,'input_and_binary_identities_unchanged':True};write('capture.json',summary);print('FINAL',json.dumps({k:v for k,v in summary.items()if k!='rows'}),flush=True)
if __name__=='__main__':main()
