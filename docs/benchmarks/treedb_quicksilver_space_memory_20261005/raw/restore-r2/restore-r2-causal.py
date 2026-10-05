import json,hashlib,pathlib,subprocess,time,os
r=pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005');source=r/'source-restore-r2';old=r/'source-baseline';out=r/'restore-r2-causal';out.mkdir(exist_ok=False)
identity=json.loads((r/'restore-r2-source-inputs.json').read_text())
def sha(p):return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
assert all(sha(source/f)==h for f,h in identity['files'].items())
env={k:v for k,v in os.environ.items() if not k.startswith('TREEDB_')};env.update(GOROOT='/home/mikers/.gvm/gos/go1.26.3',PATH='/home/mikers/.gvm/gos/go1.26.3/bin:/usr/local/bin:/usr/bin:/bin',GOWORK='off',GOFLAGS='',GOENV='off',GOTOOLCHAIN='local',GOCACHE='/mnt/fast4tb/go-build-cache',GOMODCACHE='/mnt/fast4tb/go-mod-cache',GOMAXPROCS='12',TMPDIR=str(r/'tmp'),CGO_ENABLED='1')
paths=['TreeDB/db/durable_root_side_store_recovery_supported_test.go','TreeDB/db/durable_root_snapshot_rebind_test.go']
overlay={'Replace':{str(old/f):str(source/f) for f in paths}}
(out/'red-overlay.json').write_text(json.dumps(overlay,indent=2)+'\n')
def run(label,cwd,argv,expected_green):
 start=time.time()
 with (out/(label+'-stdout.txt')).open('w') as stdout,(out/(label+'-stderr.txt')).open('w') as stderr:p=subprocess.run(argv,cwd=cwd,env=env,stdout=stdout,stderr=stderr)
 text=(out/(label+'-stdout.txt')).read_text()+(out/(label+'-stderr.txt')).read_text()
 receipt=dict(head=identity['head'],command=argv,cwd=str(cwd),started=start,finished=time.time(),seconds=time.time()-start,rc=p.returncode,environment={k:env[k] for k in ('GOROOT','PATH','GOWORK','GOFLAGS','GOENV','GOTOOLCHAIN','GOCACHE','GOMODCACHE','GOMAXPROCS','TMPDIR','CGO_ENABLED')},method_sha256=sha(__file__),overlay=overlay if not expected_green else None,production_sha256=sha(cwd/'TreeDB/db/durable_root_snapshot_rebind.go'),test_sha256={f:sha(source/f) for f in paths},stdout_sha256=sha(out/(label+'-stdout.txt')),stderr_sha256=sha(out/(label+'-stderr.txt')))
 (out/(label+'-receipt.json')).write_text(json.dumps(receipt,indent=2)+'\n');print(label,p.returncode,round(receipt['seconds'],2),flush=True)
 if expected_green:assert p.returncode==0,text[-12000:]
 else:
  assert p.returncode!=0 and 'cannot merge with fresh destination authority' in text,text[-12000:]
  for kind in ('dictionary','template'):
   for layout in ('manifest-v1','directory-v2'):assert 'FAIL: TestRebindDurableRootSnapshotSideStoreNamespaceMatchesFreshAuthority/'+kind+'/'+layout in text,text[-12000:]
run('red',old,['go','test','-p','1','-overlay',str(out/'red-overlay.json'),'./TreeDB/db','-run','^TestRebindDurableRootSnapshotSideStoreNamespaceMatchesFreshAuthority$','-count','1','-v'],False)
run('green',source,['go','test','-p','1','./TreeDB/db','-run','TestRebindDurableRootSnapshot|TestDurableRootPublicLayoutDictionary','-count','1','-v'],True)
print('PASS_CURRENT_DICTIONARY_TEMPLATE_CAUSAL_MATRIX',flush=True)
