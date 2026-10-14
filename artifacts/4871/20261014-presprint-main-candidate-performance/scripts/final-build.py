from pathlib import Path
import subprocess,hashlib,json,os,shutil,time
base=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013/sprint-performance-20261014')
ordinary=base.parent/'ordinary-5111'
goRoot=base.parent/'toolchains/go1.26.3-from-185-20261014'
def sha(b):return hashlib.sha256(b).hexdigest()
def inventory(root):
 return {str(p.relative_to(root)):sha(p.read_bytes()) for prefix in ['TreeDB','HashDB','internal'] for p in (root/prefix).rglob('*') if p.is_file() and p.suffix in {'.go','.s','.S','.h','.c','.cpp','.inc','.syso'} and not p.name.endswith('_test.go')} | {x:sha((root/x).read_bytes()) for x in ['go.mod','go.sum']}
before=inventory(ordinary)
candidate=base/'sources'/'coherent-candidate';candidate.mkdir(exist_ok=False)
rows=[]
for name,h in before.items():
 b=(ordinary/name).read_bytes();assert sha(b)==h
 p=candidate/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
 rows.append({'path':name,'sha256':h,'bytes':len(b)})
embed='TreeDB/mvcc/mvcctest/testdata/public_trace_v1.json'
if (ordinary/embed).exists():
 p=candidate/embed;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes((ordinary/embed).read_bytes());rows.append({'path':embed,'sha256':sha(p.read_bytes()),'bytes':len(p.read_bytes())})
assert inventory(ordinary)==before
receipt={'label':'coherent-candidate','ordinary_head':subprocess.check_output(['git','-C',str(ordinary),'rev-parse','HEAD'],text=True).strip(),'source_kind':'Actual uncommitted coherent ordinary source; exact projection not a merged commit','production_files':rows,'production_bytes':sum(x['bytes'] for x in rows),'shared_head':'62253f2dc9a5e417e37c16e2dfae524daa70cbef','qualification':'Focused32normal/race only; full ordinary/native acceptance OPEN'}
(base/'evidence/coherent-candidate-projection.json').write_text(json.dumps(receipt,indent=1)+'\n')
env=dict(os.environ,GOROOT=str(goRoot),GOWORK='off',GOTOOLCHAIN='local',CGO_ENABLED='1',GOMAXPROCS='4')
for label in ['before-sprint','recent-main','coherent-candidate']:
 out=base/'sources'/label;harness=out/'TreeDB/mvcc/sprintperf';harness.mkdir(exist_ok=True)
 for source in (base/'harness').glob('*.go'):shutil.copyfile(source,harness/source.name)
 commands=[('deps',[str(goRoot/'bin/go'),'list','-deps','-test','-json','./TreeDB/mvcc/sprintperf']),('build',[str(goRoot/'bin/go'),'test','-c','-trimpath','-buildvcs=false','-o',str(base/'binaries'/(label+'-mvcc.test')),'./TreeDB/mvcc/sprintperf'])]
 for step,argv in commands:
  log=base/'evidence'/(label+'-final-'+step+'.log');assert not log.exists();started=time.time()
  with log.open('wb') as f:r=subprocess.run(argv,cwd=out,env=env,stdout=f,stderr=subprocess.STDOUT)
  result={'label':label,'argv':argv,'env':{k:env[k] for k in ['GOROOT','GOWORK','GOTOOLCHAIN','CGO_ENABLED','GOMAXPROCS']},'exit':r.returncode,'wall_seconds':time.time()-started,'raw_sha256':sha(log.read_bytes())}
  if step=='deps' and not r.returncode:
   content=log.read_text();decoder=json.JSONDecoder();packages=[];position=0
   while position<len(content):
    while position<len(content) and content[position].isspace():position+=1
    if position==len(content):break
    p,length=decoder.raw_decode(content,position);position=length;packages.append(p)
   inputs=[]
   for p in packages:
    d=Path(p.get('Dir',''))
    for field in ['GoFiles','CgoFiles','CFiles','CXXFiles','MFiles','HFiles','FFiles','SFiles','SysoFiles','EmbedFiles']:
     for name in p.get(field,[]):
      file=Path(name) if Path(name).is_absolute() else d/name
      if not file.is_file():continue
      b=file.read_bytes()
      if file.is_relative_to(out):
       relative=str(file.relative_to(out))
       if not relative.startswith('TreeDB/mvcc/sprintperf/'):
        lookup={x['path']:x for x in json.loads((base/'evidence'/(label+'-projection.json')).read_text())['production_files']}
        assert relative in lookup and sha(b)==lookup[relative]['sha256'],relative
       origin='projected_source'
      elif file.is_relative_to(goRoot):origin='exact_verified_toolchain'
      else:origin='external_module_or_generated_test_driver'
      inputs.append({'import':p['ImportPath'],'field':field,'path':str(file),'origin':origin,'bytes':len(b),'sha256':sha(b)})
   binding=base/'evidence'/(label+'-compiled-inputs.json');binding.write_text(json.dumps({'packages':len(packages),'inputs':inputs},indent=1)+'\n');result.update(packages=len(packages),inputs=len(inputs),compiled_inputs_sha256=sha(binding.read_bytes()))
  if step=='build' and not r.returncode:result['binary_sha256']=sha((base/'binaries'/(label+'-mvcc.test')).read_bytes())
  log.with_suffix('.receipt.json').write_text(json.dumps(result,indent=1)+'\n');print(json.dumps(result),flush=True)
  if r.returncode:print(log.read_text()[-3500:],flush=True);raise SystemExit(r.returncode)
assert inventory(ordinary)==before
argv=[str(goRoot/'bin/go'),'test','-c','-trimpath','-buildvcs=false','-o',str(base/'binaries/coherent-candidate-raw-db.test'),'./TreeDB/db']
log=base/'evidence/coherent-candidate-raw-build.log';started=time.time()
with log.open('wb') as f:r=subprocess.run(argv,cwd=ordinary,env=env,stdout=f,stderr=subprocess.STDOUT)
log.with_suffix('.receipt.json').write_text(json.dumps({'argv':argv,'exit':r.returncode,'wall_seconds':time.time()-started,'raw_sha256':sha(log.read_bytes()),'production_source_unchanged':inventory(ordinary)==before},indent=1)+'\n');print(json.dumps({'raw_build_exit':r.returncode}),flush=True)
if r.returncode:print(log.read_text()[-3500:]);raise SystemExit(r.returncode)
