import os,pathlib,json,subprocess,hashlib,time,signal,datetime
D=pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015'); W=D/'ordinary-repair'; A=D/'ordinary-capture-cost'; G=D/'toolchains/go1.26.3'; (A/'tmp').mkdir(exist_ok=True); (A/'logs').mkdir(exist_ok=True)
def digest(b): return hashlib.sha256(b).hexdigest()
def manifest():
 names=subprocess.check_output(['git','ls-files','-z'],cwd=W).split(b'\0'); files={}
 for name in names:
  if not name: continue
  p=W/os.fsdecode(name)
  if p.is_file(): files[os.fsdecode(name)]=digest(p.read_bytes())
 return {'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=W,text=True).strip(),'files':files,'status':subprocess.check_output(['git','status','--short'],cwd=W,text=True)}
initial=manifest(); (A/'freeze-before.json').write_text(json.dumps(initial,indent=2)+'\n'); patch=subprocess.check_output(['git','diff','--binary'],cwd=W); (A/'source.patch').write_bytes(patch)
env=os.environ.copy(); selected={'GOROOT':str(G),'GOWORK':'off','GOTOOLCHAIN':'local','GOMAXPROCS':'2','TMPDIR':str(A/'tmp'),'GOCACHE':str(D/'ordinary-work/cache'),'GOMODCACHE':str(D/'ordinary-work/modcache'),'GOPROXY':'off','GOSUMDB':'off','CGO_ENABLED':'1','GOENV':'off','GOFLAGS':'','GOAMD64':'v1','GOMEMLIMIT':'8GiB','GOGC':'100','GODEBUG':''}; env.update(selected); (A/'environment.json').write_text(json.dumps(selected,indent=2)+'\n')
db_re='^Test(OneShot.*|AcquireSnapshot.*|MinPinnedSnapshot.*|Snapshot.*|OriginalDATAOnlySnapshot.*|OriginalGenerationWrapperCloseRacesLastSnapshot|PrimarySnapshot.*|PrimaryOneShot.*|PrimaryMetadataCapture.*)$'
calls=[]
for race in (False,True):
 for name,pkg,regex in [('db','./TreeDB/db',db_re),('rootpublication','./TreeDB/internal/rootpublication',None)]:
  argv=[str(G/'bin/go'),'test','-p=1']+(['-race'] if race else [])+[pkg,'-count=1','-timeout=120s','-json']+(['-run',regex] if regex else [])
  calls.append((name+('-race' if race else '-normal'),argv,150))
calls += [('db-build',[str(G/'bin/go'),'test','-p=1','-c','-o',str(A/'candidate-db.test'),'./TreeDB/db'],180),('db-buildinfo',[str(G/'bin/go'),'version','-m',str(A/'candidate-db.test')],30),('compiler-version',[str(G/'bin/go'),'version'],30)]
results=[]
for name,argv,limit in calls:
 before=manifest(); path=A/'logs'/name; path.mkdir(exist_ok=True); command={'argv':argv,'cwd':str(W),'env':selected,'outer_timeout_seconds':limit,'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}; (path/'command.json').write_text(json.dumps(command,indent=2)+'\n'); (path/'source-before.json').write_text(json.dumps(before,indent=2)+'\n'); start=time.monotonic(); terminated=False
 with (path/'stdout.txt').open('wb') as out,(path/'stderr.txt').open('wb') as err:
  proc=subprocess.Popen(argv,cwd=W,env=env,stdout=out,stderr=err,start_new_session=True)
  try: code=proc.wait(timeout=limit)
  except subprocess.TimeoutExpired:
   terminated=True; os.killpg(proc.pid,signal.SIGTERM)
   try: code=proc.wait(timeout=5)
   except subprocess.TimeoutExpired: os.killpg(proc.pid,signal.SIGKILL); code=proc.wait(timeout=5)
 after=manifest(); (path/'source-after.json').write_text(json.dumps(after,indent=2)+'\n'); events=[]
 for line in (path/'stdout.txt').read_text(errors='replace').splitlines():
  try: events.append(json.loads(line))
  except json.JSONDecodeError: pass
 summary={'name':name,'exit':code,'elapsed_seconds':time.monotonic()-start,'terminated_group':terminated,'source_equal':before==after==initial,'top_pass':[e['Test'] for e in events if e.get('Action')=='pass' and 'Test' in e and '/' not in e['Test']],'sub_pass':sum(e.get('Action')=='pass' and '/' in e.get('Test','') for e in events),'skips':[e.get('Test') for e in events if e.get('Action')=='skip'],'failures':[e.get('Test',e.get('Package')) for e in events if e.get('Action')=='fail'],'data_race_reports':(path/'stdout.txt').read_text(errors='replace').count('WARNING: DATA RACE'),'stdout_sha256':digest((path/'stdout.txt').read_bytes()),'stderr_sha256':digest((path/'stderr.txt').read_bytes()),'command':command}; results.append(summary); (path/'result.json').write_text(json.dumps(summary,indent=2)+'\n'); (A/'validation.json').write_text(json.dumps({'checks':results,'complete':False},indent=2)+'\n'); print(json.dumps({k:summary[k] for k in ['name','exit','elapsed_seconds','source_equal','failures','skips']}),flush=True)
 if code!=0 or not summary['source_equal']:
  print('STOPPED_ON_FAILURE_OR_SOURCE_DRIFT',flush=True); break
final=manifest(); (A/'freeze-after.json').write_text(json.dumps(final,indent=2)+'\n'); payload={'head':initial['head'],'all_source_equal':initial==final,'source_patch_sha256':digest(patch),'source_file_count':len(initial['files']),'checks':results,'complete':len(results)==len(calls),'all_pass':len(results)==len(calls) and all(x['exit']==0 and x['source_equal'] for x in results),'compiler_sha256':digest((G/'bin/go').read_bytes())}
if (A/'candidate-db.test').exists(): payload['db_elf_sha256']=digest((A/'candidate-db.test').read_bytes())
(A/'validation.json').write_text(json.dumps(payload,indent=2)+'\n'); print(json.dumps({k:v for k,v in payload.items() if k!='checks'}),flush=True)
