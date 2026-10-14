import pathlib,hashlib,json,subprocess,os,sys,datetime
wt=pathlib.Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013/native-4878-traversal')
packet=wt/'artifacts/4878/20261014-native-shared-physical-cleanup'
packet.mkdir(parents=True,exist_ok=True)
files=['TreeDB/internal/retainedalloc/'+x for x in ['owner.go','owner_work.go','owner_test.go','owner_work_test.go']]+['TreeDB/pager/'+x for x in ['pager.go','grow_work.go','primary_read_root_v6.go','mapping_owner.go','mapping_owner_unix.go','mapping_owner_windows.go','physical_close_work.go','physical_close_work_test.go','physical_close_work_unix_test.go','primary_read_root_close_work.go']]+['TreeDB/internal/primaryarena/'+x for x in ['arena.go','physical_close_work.go','physical_close_work_test.go']]
rows=[]
for f in files:
 b=(wt/f).read_bytes();rows.append({'path':f,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()})
manifest={'state':'PARTIAL_4878_GENERIC_SHARED_TRANCHE','head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=wt,text=True).strip(),'source':rows,'production_paths':sum(not f.endswith('_test.go') for f in files),'test_paths':sum(f.endswith('_test.go') for f in files),'fork_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=wt.parent/'compress-5129',text=True).strip(),'protected_original_whole_fixture_sha256':'77f14c80a007ce9b918e007e5cce24e5d78ffa022c6a92386cfb58bda246d55f','public_33_80_gate':'OPEN_NOT_RUN_ABI_INCOMPLETE','generated_fixture_roots_released':[]}
(packet/'source.json').write_text(json.dumps(manifest,indent=2)+'\n')
for row in rows:
 dest=packet/'frozen-source'/row['path'];dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes((wt/row['path']).read_bytes())
go='/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go'
env=os.environ.copy();env.update(GOWORK='off',GOMAXPROCS='2',CGO_ENABLED='1',GOPROXY='off',GOSUMDB='off',GOTOOLCHAIN='local')
mod='/home/mikers/benchmarks/mvcc-4871-repair-20261013/scratch/native-codec-worker.mod'
base=[go,'test','-modfile='+mod,'-p','2']
packages=['./TreeDB/internal/retainedalloc','./TreeDB/pager','./TreeDB/internal/primaryarena']
listcmd=[go,'list','-modfile='+mod,'-deps','-json']+packages
result=subprocess.run(listcmd,cwd=wt,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
(packet/'dependency-list.stderr.txt').write_text(result.stderr)
if result.returncode:sys.exit(result.returncode)
decoder=json.JSONDecoder();blob=result.stdout;pos=0;deps=[]
while pos<len(blob):
 while pos<len(blob) and blob[pos].isspace():pos+=1
 if pos==len(blob):break
 value,end=decoder.raw_decode(blob,pos);pos=end
 directory=value.get('Dir','')
 if directory.startswith(str(wt)) or directory.startswith(str(wt.parent/'compress-5129')):
  for name in value.get('GoFiles',[])+value.get('CgoFiles',[])+value.get('CFiles',[])+value.get('SFiles',[]):
   file=pathlib.Path(directory)/name;b=file.read_bytes();deps.append({'path':str(file),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()})
(packet/'runtime-source.json').write_text(json.dumps(sorted(deps,key=lambda r:r['path']),indent=2)+'\n')
results=[]
commands=[('normal',base+packages+['-count=1'],{}),('race',base+['-race']+packages+['-run','Test(RemovalWork|EnrollmentCloseWork|OrdinaryMetadata|PhysicalCloseWork|GrowthPrivate|Owner|ArenaPhysicalCloseWork)','-count=1'],{}),('386',base+packages+['-run','Test(RemovalWork|EnrollmentCloseWork|OrdinaryMetadata|PhysicalCloseWork|GrowthPrivate|Owner|ArenaPhysicalCloseWork)','-count=1'],{'GOARCH':'386'}),('windows-compile',base+['-c','./TreeDB/pager','-o',str(wt.parent/'scratch/native-shared-physical-20261014/pager-windows-amd64.test.exe')],{'GOOS':'windows','GOARCH':'amd64','CGO_ENABLED':'0'})]
for label,cmd,extra in commands:
 if label=='windows-compile':(wt.parent/'scratch/native-shared-physical-20261014').mkdir(exist_ok=True)
 runenv=env.copy();runenv.update(extra)
 started=datetime.datetime.now(datetime.timezone.utc).isoformat()
 with (packet/(label+'.log')).open('w') as log:
  log.write(json.dumps({'command':cmd,'environment':{k:runenv[k] for k in ['GOWORK','GOMAXPROCS','CGO_ENABLED','GOPROXY','GOSUMDB','GOTOOLCHAIN']+list(extra)},'start':started})+'\n');log.flush()
  ret=subprocess.run(cmd,cwd=wt,env=runenv,stdout=log,stderr=subprocess.STDOUT).returncode
 results.append({'label':label,'exit_code':ret,'command':cmd,'env_delta':extra})
 (packet/'checks.json').write_text(json.dumps(results,indent=2)+'\n')
 print(label,ret,flush=True)
 if ret:sys.exit(ret)
for row in rows:
 if hashlib.sha256((wt/row['path']).read_bytes()).hexdigest()!=row['sha256']:raise RuntimeError('source changed: '+row['path'])
for row in deps:
 if hashlib.sha256(pathlib.Path(row['path']).read_bytes()).hexdigest()!=row['sha256']:raise RuntimeError('dependency changed: '+row['path'])
output=wt.parent/'scratch/native-shared-physical-20261014'
release={'root':str(output),'producer':'shared_packet.py windows-compile','consumers':[],'source_retained_in':str(packet),'files':[{'path':str(p),'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in output.iterdir() if p.is_file()]}
(packet/'released-build-output.json').write_text(json.dumps(release,indent=2)+'\n')
print('PACKET',packet,'MANIFEST',hashlib.sha256((packet/'source.json').read_bytes()).hexdigest(),flush=True)
