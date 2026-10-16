import ctypes, datetime, hashlib, json, os, re, signal, subprocess, tarfile, time
from pathlib import Path
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015'); W=D/'native-integrated'; O=D/'native-main-integration/actual33-observation'
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1048576),b''):h.update(b)
 return h.hexdigest()
def dump(p,v):p.write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
def utc():return datetime.datetime.now(datetime.timezone.utc).isoformat()
js=D/'native-decision/root-integrated-actual33-observation-grant.json';md=js
assert sha(js)=='8ed1d4596b0d2f83d80a281105c5386664276fcba47833decb9ef2faa3281859'
grant=json.loads(js.read_text());original=grant['command'];mp=Path(original['source_manifest']);archive=Path(original['source_archive'])
assert grant['maximum_commands']==1 and grant['retries']==0 and not grant['publication_runtime_authorized'] and not grant['benchmark_authorized']
assert original['cwd']==str(W) and original['argv'][0]==str(D/'toolchains/go1.26.3/bin/go')
assert sha(mp)==original['source_manifest_sha256'] and sha(archive)==original['source_archive_sha256']
expected=json.loads(mp.read_text());assert len(expected)==10375
with tarfile.open(archive,'r:gz') as tf:
 actual={}
 for m in tf:
  if m.isfile():
   assert m.name not in actual
   actual[m.name]=hashlib.sha256(tf.extractfile(m).read()).hexdigest()
 assert actual==expected
fixture='TreeDB/mvcc/native_prune_backend_route_v6_test.go';assert sha(W/fixture)==expected[fixture]
test='TestNativeMVCCRealBackendRouteRefusalPreservesCustodyV6'
assert re.search(r'^func '+test+r'\(t \*testing\.T\)',(W/fixture).read_text(),re.M)
assert original['argv'][original['argv'].index('-run')+1]=='^'+test+'$'
assert original['argv']==[str(D/'toolchains/go1.26.3/bin/go'),'test','-p=1','./TreeDB/mvcc','-run','^'+test+'$','-count=1','-v','-timeout=3m']
assert original['outer_timeout_seconds']==240
c=dict(original,id=original['name'],top_level_tests=[test],selector='^'+test+'$',race=False)
g={'source_manifest_sha256':original['source_manifest_sha256'],'source_archive_sha256':original['source_archive_sha256'],'source_tree':original['source_tree'],'selected_test_files':{fixture:expected[fixture]},'commands':[c],'required_child_tests':{}}
dump(O/'grant.json',grant)
inventory=D/'native-main-integration/fixture-inventory-final'
receipt=json.loads((inventory/'receipt.json').read_text())
assert receipt['source_manifest_sha256']==g['source_manifest_sha256'] and receipt['source_archive_sha256']==g['source_archive_sha256']
assert receipt['tree_after']==g['source_tree'] and receipt['staged_path_count']==202
assert (inventory/'tree-after.log').read_text().strip()==g['source_tree']
for name in ['tree-after','complete-staged-check','staged-paths']:
 r=json.loads((inventory/(name+'.json')).read_text());assert r['exit_code']==0 and r['log_sha256']==sha(inventory/(name+'.log'))
gitdirtext=(W/'.git').read_text().strip();assert gitdirtext.startswith('gitdir: ')
gitdir=Path(gitdirtext[len('gitdir: '):]);gitdir=gitdir if gitdir.is_absolute() else W/gitdir
indexpath=gitdir/'index';indexhash=sha(indexpath)
indexbinding={'root_recorded_tree':g['source_tree'],'root_receipt_sha256':sha(inventory/'receipt.json'),'index_path':str(indexpath),'index_sha256_before':indexhash,'binding_method':'Read-only root staged-tree/check receipts plus unchanged live index hash; no Git command executed','root_inventory_files':{x.name:sha(x) for x in inventory.iterdir() if x.is_file() and x.suffix in ['.json','.log']}}
dump(O/'index-binding.json',indexbinding)
dump(O/'selection.json',g)
(O/'grant-original.json').write_bytes(js.read_bytes())
root=D/'toolchains/go1.26.3'; go=root/'bin/go'; compiler=root/'pkg/tool/linux_amd64/compile'
assert sha(go)=='d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65'
assert sha(compiler)=='3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1'
libc=ctypes.CDLL(None,use_errno=True);assert libc.prctl(36,1,0,0,0)==0
sr=ctypes.c_int();assert libc.prctl(37,ctypes.byref(sr),0,0,0)==0 and sr.value==1
identity={'started_utc':utc(),'selection_md_sha256':sha(md),'selection_json_sha256':sha(js),'source_manifest_sha256':sha(mp),'source_archive_sha256':sha(archive),'archive_verified_files':len(expected),'selected_test_files':g['selected_test_files'],'go_binary':str(go),'go_sha256':sha(go),'compiler_sha256':sha(compiler),'VERSION':(root/'VERSION').read_text(),'runner_pid':os.getpid(),'runner_pgid':os.getpgrp(),'subreaper_verified':True,'runner_sha256':sha(Path(__file__))};dump(O/'identity.json',identity)
baseenv={'GOROOT':str(root),'GOTOOLCHAIN':'local','GOWORK':'off','GOENV':'off','GOMAXPROCS':'2','GOMEMLIMIT':'8GiB','CGO_ENABLED':'1','GOGC':'100','GOPROXY':'off','GOSUMDB':'off','GOFLAGS':'','GODEBUG':'','GOCACHE':str(D/'native-build/cache'),'GOMODCACHE':str(D/'ordinary-work/modcache')}
def source():
 result={}
 for base,ds,fs in os.walk(W):
  ds[:]=[d for d in ds if d!='.git']
  for f in fs:
   p=Path(base,f);q=str(p.relative_to(W))
   if q!='.git' and p.is_file():result[q]=sha(p)
 return dict(sorted(result.items()))
def processes():
 rows={}
 for p in Path('/proc').iterdir():
  if not p.name.isdigit():continue
  try:
   t=(p/'stat').read_text();a=t.rfind(')');v=t[a+2:].split();pid=int(p.name)
   rows[pid]={'pid':pid,'state':v[0],'ppid':int(v[1]),'pgid':int(v[2]),'session':int(v[3]),'comm':t[t.find('(')+1:a]}
  except (OSError,ValueError,IndexError):pass
 return rows
def reap():
 events=[]
 while True:
  try:p,s=os.waitpid(-1,os.WNOHANG)
  except ChildProcessError:break
  if p==0:break
  events.append({'pid':p,'status':s,'utc':utc()})
 return events
required=g['required_child_tests']
dump(O/'required-child-tests.json',required)
print(json.dumps({'event':'inputs_verified','source_files':len(expected),'archive_verified':True,'subreaper':True}),flush=True)
results=[]
for c in g['commands']:
 out=O/c['id'];out.mkdir(exist_ok=False);tmp=out/'tmp';tmp.mkdir()
 before=source();dump(out/'source-before.json',before)
 if before!=expected or sha(indexpath)!=indexhash:
  dump(out/'result.json',{'status':'STOP','reason':'source drift before launch','executed':False});break
 assert c['argv'][0]==str(go) and c['argv'][c['argv'].index('-run')+1]==c['selector']
 ev=dict(original['env']);assert all(ev[k]==v for k,v in baseenv.items());env=os.environ.copy();env.update(ev)
 dump(out/'command.json',dict(c,cwd=str(W),env=ev,selection_json_sha256=sha(js),source_manifest_sha256=sha(mp),source_archive_sha256=sha(archive)))
 start=time.monotonic();started=utc();timedout=False;signals=[];observed={};events=[]
 with (out/'run.log').open('wb') as log:
  p=subprocess.Popen(c['argv'],cwd=W,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True);pgid=os.getpgid(p.pid)
  dump(out/'process.json',{'pid':p.pid,'pgid':pgid,'runner_pid':os.getpid(),'subreaper_verified':True,'started_utc':started})
  while p.poll() is None:
   rows=processes();observed.update({pid:r for pid,r in rows.items() if r['pgid']==pgid})
   if time.monotonic()-start>c['outer_timeout_seconds']:
    timedout=True
    for sig in [signal.SIGTERM,signal.SIGKILL]:
     try:os.killpg(pgid,sig);signals.append({'signal':sig.name,'pgid':pgid,'utc':utc()})
     except ProcessLookupError:pass
     if sig==signal.SIGTERM:
      deadline=time.monotonic()+2
      while time.monotonic()<deadline and p.poll() is None:time.sleep(.05)
    break
   time.sleep(.1)
  rc=p.wait();events+=reap()
 deadline=time.monotonic()+5
 while True:
  events+=reap();rows=processes();group=[r for r in rows.values() if r['pgid']==pgid];children=[r for r in rows.values() if r['ppid']==os.getpid()]
  if not group and not children or time.monotonic()>=deadline:break
  time.sleep(.05)
 leaked=bool(group or children);residual_before_cleanup={'group':group,'children':children}
 if leaked:
  for member in group+children:
   try:os.kill(member['pid'],signal.SIGKILL);signals.append({'signal':'SIGKILL residual','pid':member['pid'],'utc':utc()})
   except ProcessLookupError:pass
  deadline=time.monotonic()+3
  while time.monotonic()<deadline:
   events+=reap();rows=processes();group=[r for r in rows.values() if r['pgid']==pgid];children=[r for r in rows.values() if r['ppid']==os.getpid()]
   if not group and not children:break
   time.sleep(.05)
 after=source();dump(out/'source-after.json',after)
 text=(out/'run.log').read_text(errors='replace');runs=re.findall(r'^=== RUN\s+(\S+)',text,re.M);passes=re.findall(r'^\s*--- PASS: (\S+)',text,re.M);fails=re.findall(r'^\s*--- FAIL: (\S+)',text,re.M);skips=re.findall(r'^\s*--- SKIP: (\S+)',text,re.M)
 top=[n for n in runs if '/' not in n];expected_top=c['top_level_tests'];missing=[n for n in expected_top if n not in runs or n not in passes];unexpected_top=[n for n in top if n not in expected_top]
 required_children=[parent+'/'+child for parent,names in required.items() if parent in expected_top for child in names]
 if c.get('required_leaf'):required_children.append(c['required_leaf'])
 missing_children=[n for n in required_children if n not in runs or n not in passes]
 m4leaves=[n for n in runs if n.startswith('TestIndexVacuumM4CertificationMatrix/') and n.count('/')==2]
 reasons=[]
 if rc:reasons.append('nonzero exit')
 if timedout:reasons.append('outer timeout')
 if leaked or group or children:reasons.append('process leak')
 if signals:reasons.append('process intervention')
 if after!=expected or before!=after:reasons.append('source drift')
 if sha(indexpath)!=indexhash:reasons.append('index drift')
 if missing:reasons.append('missing named top RUN/PASS')
 if unexpected_top:reasons.append('unexpected top RUN')
 if missing_children:reasons.append('missing required child RUN/PASS')
 if skips:reasons.append('unexpected SKIP')
 if fails:reasons.append('FAIL test')
 if any(n not in passes for n in runs):reasons.append('observed RUN without PASS')
 if len(top)!=len(expected_top) or len(set(top))!=len(top):reasons.append('top inventory mismatch')
 if c.get('required_selected_cells') and m4leaves!=[c['required_leaf']]:reasons.append('M4 selected cell mismatch')
 if 'WARNING: DATA RACE' in text or 'panic:' in text or 'goleak:' in text:reasons.append('race/panic/leak diagnostic')
 result={'id':c['id'],'status':'PASS' if not reasons else 'STOP','stop_reasons':reasons,'exit_code':rc,'timeout':timedout,'started_utc':started,'ended_utc':utc(),'elapsed_seconds':time.monotonic()-start,'pid':p.pid,'pgid':pgid,'leader_joined':True,'subreaper_verified':True,'observed_group_processes':list(observed.values()),'adopted_children_reaped':events,'signals':signals,'residual_before_cleanup':residual_before_cleanup,'remaining_group_members':group,'remaining_direct_or_adopted_children':children,'group_and_children_empty':not group and not children,'source_before_after_frozen':before==after==expected,'test_RUN_names':runs,'test_PASS_names':passes,'test_FAIL_names':fails,'test_SKIP_names':skips,'expected_top_level_names':expected_top,'top_level_RUN_names':top,'top_level_pass_count':len([n for n in passes if '/' not in n]),'missing_top_names':missing,'unexpected_top_names':unexpected_top,'required_child_names':required_children,'missing_child_names':missing_children,'M4_selected_leaf_names':m4leaves,'raw_log_sha256':sha(out/'run.log')};dump(out/'result.json',result);results.append(result)
 print(json.dumps({'event':'completion','id':c['id'],'status':result['status'],'exit_code':rc,'elapsed_seconds':result['elapsed_seconds'],'top_pass':result['top_level_pass_count'],'all_pass':len(passes),'source_frozen':result['source_before_after_frozen'],'group_empty':result['group_and_children_empty'],'stop_reasons':reasons}),flush=True)
 if reasons:break
dump(O/'index-after.json',{'index_sha256_after':sha(indexpath),'unchanged':sha(indexpath)==indexhash,'root_recorded_tree':g['source_tree']})
summary={'results':[{'id':r['id'],'status':r['status'],'result':str(O/r['id']/'result.json')} for r in results],'commands_executed':len(results),'commands_not_executed':[c['id'] for c in g['commands'][len(results):]],'all_authorized_pass':len(results)==len(g['commands']) and all(r['status']=='PASS' for r in results),'source_still_frozen':source()==expected,'source_manifest_sha256':sha(mp),'source_files':len(expected),'all_groups_empty':all(r['group_and_children_empty'] for r in results),'actual33_executed':True,'native_publication_executed':False,'public_status':'Q32 RED/ACK0; no allowance','completed_utc':utc()};dump(O/'summary.json',summary);print(json.dumps({'event':'finished',**summary}),flush=True)
