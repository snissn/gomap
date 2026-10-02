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
def inventory(source):
 raw=subprocess.check_output([GO,'list','-deps','-test','-json','./TreeDB/caching'],cwd=source,env=ENV,text=True);d=json.JSONDecoder();files={};modules={}
 while raw.strip():
  pkg,end=d.raw_decode(raw.lstrip());raw=raw.lstrip()[end:]
  if pkg.get('Module'):
   m=pkg['Module'];modules[m['Path']]=m
   for n in (m,m.get('Replace',{})):
    if n.get('GoMod'):files[n['GoMod']]=sha(n['GoMod'])
  for field in ('GoFiles','CgoFiles','CFiles','CXXFiles','MFiles','HFiles','FFiles','SFiles','SysoFiles','EmbedFiles'):
   for name in pkg.get(field,[]):
    p=pathlib.Path(pkg['Dir'])/name;files[str(p.resolve())]=sha(p)
 settings=json.loads(subprocess.check_output([GO,'env','-json'],cwd=source,env=ENV,text=True));settings['GOGCCFLAGS']=re.sub(r'(-f(?:file|debug)-prefix-map=)/tmp/go-build[0-9]+=',r'\1/tmp/go-build<generated>=',settings['GOGCCFLAGS']);tools=pathlib.Path(ENV['GOROOT'])/'pkg/tool/linux_amd64'
 for p in [pathlib.Path(GO),tools/'compile',tools/'link',tools/'asm',source/'go.mod',source/'go.sum',*pathlib.Path(ENV['GOROOT']).joinpath('pkg/include').glob('*')]:
  if p.is_file():files[str(p.resolve())]=sha(p)
 return {'inputs':files,'modules':modules,'go_environment':settings}
if (ROOT/'preparation.json').exists():
 records=json.loads((ROOT/'preparation.json').read_text())
 for name,item in records.items():
  assert inventory(ROOT/name)==item['identity'];assert sha(ROOT/(name+'-caching.test'))==item['binary_sha256']
else:
 records={}
 for item in json.loads((ROOT/'archives.json').read_text()):
  name=item['name'];source=ROOT/name
  assert sha(ROOT/(name+'.tar.gz'))==item['sha256'];before=inventory(source);binary=ROOT/(name+'-caching.test');cmd=[GO,'test','-c','-trimpath','-buildvcs=false','-o',str(binary),'./TreeDB/caching'];beg=time.time()
  with (ROOT/(name+'-build-v2.stdout')).open('x')as out,(ROOT/(name+'-build-v2.stderr')).open('x')as err:r=subprocess.run(cmd,cwd=source,env=dict(ENV,GOMAXPROCS='2'),stdout=out,stderr=err)
  assert r.returncode==0;after=inventory(source);assert before==after
  records[name]={'original_head':item['head'],'archive_sha256':item['sha256'],'source':str(source),'identity':before,'binary_sha256':sha(binary),'build_command':cmd,'build_environment':dict(ENV,GOMAXPROCS='2'),'build_exit':r.returncode,'build_elapsed_seconds':time.time()-beg,'build_stdout_sha256':sha(ROOT/(name+'-build-v2.stdout')),'build_stderr_sha256':sha(ROOT/(name+'-build-v2.stderr'))}
write('preparation.json',records)
print('PREPARATION PASS',json.dumps({k:{'head':v['original_head'],'binary':v['binary_sha256'],'inputs':len(v['identity']['inputs'])}for k,v in records.items()}),flush=True)
if os.environ.get('REPRO_TIMING_GRANT'):
 grant=os.environ['REPRO_TIMING_GRANT'];rows=[];preflight=subprocess.check_output(['vmstat','1','3'],text=True);(ROOT/'vmstat-before.txt').write_text(preflight);assert int(preflight.splitlines()[-1].split()[14])>=90,'host not quiet';(ROOT/'processes-before.txt').write_text(subprocess.check_output(['ps','-eo','pid,ppid,etime,pcpu,args','--sort=-pcpu'],text=True))
 for pair in range(8):
  for name in (('base','candidate')if pair%2==0 else('candidate','base')):
   binary=ROOT/(name+'-caching.test');cmd=['taskset','-c','4',str(binary),'-test.run=^$','-test.bench=^BenchmarkRepeatedIterator$','-test.benchtime=2s','-test.count=1','-test.benchmem'];stem=f'v2-{pair+1}-{name}';start=time.time()
   with (ROOT/(stem+'.stdout')).open('x')as out,(ROOT/(stem+'.stderr')).open('x')as err:r=subprocess.run(cmd,cwd=ROOT/name,env=ENV,stdout=out,stderr=err)
   assert r.returncode==0;raw=(ROOT/(stem+'.stdout')).read_text();assert 'PASS'in raw.splitlines();matches=re.findall(r'^BenchmarkRepeatedIterator(?:-\d+)?\s+(\d+)\s+([\d.]+) ns/op\s+([\d.]+) B/op\s+([\d.]+) allocs/op$',raw,re.M);assert len(matches)==1;iterations,ns,bytes_,allocs=matches[0];rows.append({'pair':pair+1,'name':name,'iterations':int(iterations),'ns_op':float(ns),'B_op':float(bytes_),'allocs_op':float(allocs),'argv':cmd,'environment':ENV,'started_unix':start,'elapsed_seconds':time.time()-start,'exit':r.returncode,'stdout_sha256':sha(ROOT/(stem+'.stdout')),'stderr_sha256':sha(ROOT/(stem+'.stderr'))});write('capture-incomplete.json',{'grant':grant,'complete':False,'rows':rows});print(stem,ns,bytes_,allocs,flush=True)
 for name,item in records.items():
  assert inventory(ROOT/name)==item['identity'];assert sha(ROOT/(name+'-caching.test'))==item['binary_sha256']
 ratios=[next(x['ns_op']for x in rows if x['pair']==p and x['name']=='candidate')/next(x['ns_op']for x in rows if x['pair']==p and x['name']=='base')for p in range(1,9)];summary={'grant':grant,'complete':True,'rows':rows,'base_median_ns':statistics.median(x['ns_op']for x in rows if x['name']=='base'),'candidate_median_ns':statistics.median(x['ns_op']for x in rows if x['name']=='candidate'),'paired_median_relative_delta_percent':(statistics.median(ratios)-1)*100,'paired_ratios':ratios,'input_and_binary_identities_unchanged':True};write('capture.json',summary);print('FINAL',json.dumps({k:v for k,v in summary.items()if k!='rows'}),flush=True)
