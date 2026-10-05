"""Native red/green and relevant maintenance/race checks on frozen L sources."""
import hashlib,json,os,pathlib,subprocess,time
r=pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005');source=r/'source-manifest-l';out=r/'manifest-l-native-tests';out.mkdir()
manifest=json.loads((r/'provisional-manifest-manifest-l.json').read_text());env={k:v for k,v in os.environ.items() if not k.startswith(('TREEDB_','LD_'))};env.update(manifest['build_env'],GOMAXPROCS='12',TMPDIR=str(r/'tmp'))
overlay=out/'red-overlay.json';overlay.write_text(json.dumps({'Replace':{str(source/'TreeDB/db/leaf_generation_gc.go'):str(r/'source-restore-r2/TreeDB/db/leaf_generation_gc.go')}})+'\n')
checks=[('red',1,['-overlay',str(overlay),'-run','^TestLeafGenerationGC_(PrunePreservesPublishedManifest|DeletesFullyDeadGeneration|RetriesDeletedGenerationFileAfterReopen)$']),('green',0,['-run','^(TestLeafGeneration(GC|Pack)|TestCompactStorage|TestStableResourceManifest|TestValueLogGC|TestDurableRootSnapshotRebind)']),('race',0,['-race','-run','^TestLeafGenerationGC_(PrunePreservesPublishedManifest|DeletesFullyDeadGeneration|RetriesDeletedGenerationFileAfterReopen|ProtectedRootIDsKeepDetachedRootLive|RecoverableRootsKeepOldGenerationLive|SnapshotPinsKeepDetachedGenerationLive)'])]
records=[]
for name,expected,flags in checks:
    command=['go','test','-p','1',*flags,'./TreeDB/db','-count=1'];started=time.time()
    with (out/(name+'.txt')).open('w') as log:p=subprocess.run(command,cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=600)
    records.append(dict(name=name,command=command,expected_rc=expected,rc=p.returncode,started=started,finished=time.time(),output_sha256=hashlib.sha256((out/(name+'.txt')).read_bytes()).hexdigest(),source_head='f5a82a6f7ac81e6a2959b730e030e6468e75444b',manifest_sha256=hashlib.sha256((r/'provisional-manifest-manifest-l.json').read_bytes()).hexdigest()))
    (out/'commands.json').write_text(json.dumps(records,indent=2)+'\n');print(name,p.returncode,round(time.time()-started,2),flush=True);assert p.returncode==expected,(out/(name+'.txt')).read_text()[-5000:]
assert 'generation_id 2 appears more than once' in (out/'red.txt').read_text()
print('PASS_NATIVE_RED_GREEN_RACE',flush=True)
